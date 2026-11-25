package melta

// The baseline standard tools: the five owner-constructed Tool
// implementations — read_file, write_file, and edit_file over the shared file
// operations, snapshot bookkeeping, and artifact-derived code groups, plus
// the foreground/background command and process tools.
// They are ordinary composition members compiled directly into every
// Melta: they are never optional plugin exports, and file tools carry no
// process dependency.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/cmdoutput"
	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/editpreview"
	"github.com/Syntheresis/melta/internal/jobs"
	"github.com/Syntheresis/melta/internal/snapshot"
	"github.com/Syntheresis/melta/internal/tool"
	"github.com/Syntheresis/melta/internal/toolargs"
	"github.com/Syntheresis/melta/model"
)

// maxDiagnosticBytes is the durable diagnostic bound for the
// validation-class diagnostics these tools produce, matching the Harness's
// own tool-boundary bound.
const maxDiagnosticBytes = 512

// toolSettings is the tools' private configuration, decoded with json.Unmarshal
// from the top-level tools section. Nil input and missing or null-valued
// members keep the defaults; every consumed limit must be positive, and
// command_timeout must fit its seconds-to-time.Duration conversion. Every
// other member is ignored, including max_background_processes regardless of
// its JSON value.
type toolSettings struct {
	MaxOutputBytes   int `json:"max_output_bytes"`
	ReadMaxLines     int `json:"read_max_lines"`
	ReadLineMaxChars int `json:"read_line_max_chars"`
	CommandTimeout   int `json:"command_timeout"`
}

func defaultToolSettings() toolSettings {
	return toolSettings{MaxOutputBytes: 15360, ReadMaxLines: 500, ReadLineMaxChars: 5000, CommandTimeout: 120}
}

// decodeToolSettings decodes one supplied top-level tools section on top of
// the defaults and validates every consumed limit.
func decodeToolSettings(raw json.RawMessage) (toolSettings, error) {
	s := defaultToolSettings()
	if len(bytes.TrimSpace(raw)) != 0 {
		if err := json.Unmarshal(raw, &s); err != nil {
			return toolSettings{}, fmt.Errorf("tools: invalid settings: %w", err)
		}
	}
	switch {
	case s.MaxOutputBytes <= 0:
		return toolSettings{}, errors.New("tools: max_output_bytes must be positive")
	case s.ReadMaxLines <= 0:
		return toolSettings{}, errors.New("tools: read_max_lines must be positive")
	case s.ReadLineMaxChars <= 0:
		return toolSettings{}, errors.New("tools: read_line_max_chars must be positive")
	case s.CommandTimeout <= 0:
		return toolSettings{}, errors.New("tools: command_timeout must be positive")
	case int64(s.CommandTimeout) > math.MaxInt64/int64(time.Second):
		return toolSettings{}, errors.New("tools: command_timeout overflows the seconds-to-duration conversion")
	}
	return s, nil
}

// validateToolsSettings validates one captured top-level tools section; the
// configuration candidate construction calls it directly for every
// publication.
func validateToolsSettings(raw json.RawMessage) error {
	_, err := decodeToolSettings(raw)
	return err
}

// toolsConfig projects the decoded settings onto the shared file-tool
// configuration.
func (s toolSettings) toolsConfig() config.ToolsConfig {
	return config.ToolsConfig{
		MaxOutputBytes:   s.MaxOutputBytes,
		ReadMaxLines:     s.ReadMaxLines,
		ReadLineMaxChars: s.ReadLineMaxChars,
	}
}

// standardTools is the constructed file-tool state: the owner data root the
// mutating calls derive their code groups from and the command spills derive
// their output directory from. It keeps no Session-indexed state, no handle
// tags, no cached read records and no reset or eviction hooks, and no process
// dependency: every mutating call opens its code group fresh from the calling
// Operation's admitted-input identity and drops it afterward. No environment
// key set is captured here — the command paths resolve the live subprocess
// environment through the ToolContext's call-time producer, and the jobs
// service is a field of the two tools that call it.
type standardTools struct{ dataDir string }

func (in *standardTools) settings(inv Invocation) (toolSettings, error) {
	return decodeToolSettings(inv.toolsSettings())
}

// openCodeGroup opens the calling Operation's one disk code group, keyed by
// the admitted-input identity from the ToolContext — a validated Harness
// identity, never model-supplied data. One Operation has one group keyed by
// its one admitted input; every delivered message owns exactly one Operation,
// so every normally delivered head — steering included — opens its own group.
// The handle is opened per call, shared by that call's sub-operations, and
// not retained.
func (in *standardTools) openCodeGroup(tc ToolContext) (*snapshot.CodeStore, error) {
	directory := filepath.Join(in.dataDir, "code", tc.AdmittedEntry.SessionID, tc.AdmittedEntry.EntryID)
	return snapshot.OpenCodeStore(directory)
}

// immediateError is the normalization-class immediate outcome: status error
// carrying the bounded validation diagnostic, per the tool-boundary
// validation contract.
func immediateError(callID string, cause error) toolPlan {
	return toolPlan{Immediate: &harness.ToolOutcome{
		Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: boundedDiagnostic(cause)},
	}}
}

// boundedDiagnostic renders one tool-produced validation diagnostic within
// the durable diagnostic bound.
func boundedDiagnostic(cause error) string {
	msg := cause.Error()
	if len(msg) > maxDiagnosticBytes {
		msg = strings.ToValidUTF8(msg[:maxDiagnosticBytes], "")
	}
	return msg
}

// runForegroundCommandFn is the one seam over the shared foreground runner:
// commandOutcome routes through it, so tests drive the runner's settled
// ExitError shapes with a context cancelled after the runner returns.
// Nil-free in production; it simply delegates to tool.RunForegroundCommand.
var runForegroundCommandFn = tool.RunForegroundCommand

func toolError(callID string, err error) harness.ToolOutcome {
	return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: boundedDiagnostic(err)}}
}

func mutationOutcome(callID, output string, outcome tool.MutationOutcome, err error) harness.ToolOutcome {
	if err != nil {
		result := toolError(callID, err)
		if outcome == tool.MutationUncertain {
			result.Evidence = harness.ExecutionUnknown
		}
		return result
	}
	return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: output}}
}

func (in *standardTools) read(_ context.Context, tc ToolContext, call model.ToolCall) harness.ToolOutcome {
	settings, err := in.settings(tc.Invocation)
	if err != nil {
		return toolError(call.ID, err)
	}
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return toolError(call.ID, err)
	}
	path, offset, limit, err := tool.ReadArgs(args, settings.ReadMaxLines)
	if err != nil {
		return toolError(call.ID, err)
	}
	output, err := tool.ReadFile(tc.Workspace, settings.toolsConfig(), path, offset, limit)
	if err != nil {
		return toolError(call.ID, err)
	}
	return harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: output}}
}

func (in *standardTools) write(_ context.Context, tc ToolContext, call model.ToolCall) harness.ToolOutcome {
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return toolError(call.ID, err)
	}
	path, content, err := tool.WriteArgs(args)
	if err != nil {
		return toolError(call.ID, err)
	}
	group, err := in.openCodeGroup(tc)
	if err != nil {
		return toolError(call.ID, err)
	}
	output, outcome, err := tool.WriteFile(tc.Workspace, group, path, content)
	return mutationOutcome(call.ID, output, outcome, err)
}

func (in *standardTools) edit(_ context.Context, tc ToolContext, call model.ToolCall) harness.ToolOutcome {
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return toolError(call.ID, err)
	}
	path, oldString, newString, replaceAll, err := tool.EditArgs(args)
	if err != nil {
		return toolError(call.ID, err)
	}
	group, err := in.openCodeGroup(tc)
	if err != nil {
		return toolError(call.ID, err)
	}
	output, outcome, err := tool.EditFile(tc.Workspace, group, path, oldString, newString, replaceAll)
	result := mutationOutcome(call.ID, output, outcome, err)
	if err == nil {
		if m := editpreview.MetadataFromParams(args, output); m != nil {
			if raw, err := json.Marshal(m); err == nil {
				result.Metadata = raw
			}
		}
	}
	return result
}

// runCommandTool is the run_command tool: always available and executed
// through the shell. background=true starts a job through the
// background bridge and returns the retained immediate text; every start
// failure renders through the one wrapper. Its own fields are the file-tool
// state (for the spill directory and settings), the jobs service the
// background start calls, and the live-output registry that tracks the
// starting call's output preview and publishes its `tool_output_changed`
// notices for both foreground and background commands.
type runCommandTool struct {
	inst       *standardTools
	jobs       jobs.Jobs
	toolOutput *liveToolOutput
}

func (t runCommandTool) run(ctx context.Context, tc ToolContext, call model.ToolCall) harness.ToolOutcome {
	s, err := t.inst.settings(tc.Invocation)
	if err != nil {
		return toolError(call.ID, err)
	}
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return toolError(call.ID, err)
	}
	command, timeout, background, err := tool.RunCommandArgs(args, s.CommandTimeout)
	if err != nil {
		return toolError(call.ID, err)
	}
	if background {
		return t.backgroundOutcome(ctx, call.ID, command, timeout, tc)
	}
	env, err := commandEnvironment(ctx, tc)
	if err != nil {
		return toolError(call.ID, err)
	}
	spillDir := filepath.Join(t.inst.dataDir, "code", tc.AdmittedEntry.SessionID, "output")
	return t.foregroundOutcome(ctx, call.ID, command, tc, timeout, s, spillDir, env)
}

// errNoSubprocessEnv is the one missing-producer diagnostic shared by the
// foreground resolution and the background pre-reservation check.
var errNoSubprocessEnv = errors.New("run_command: no subprocess environment producer")

// commandEnvironment resolves one cooperative command's environment through
// the ToolContext's call-time producer. A missing callback — or a producer
// reporting no environment — is an ordinary error the Caller renders with no
// process start; there is no ambient-environment fallback.
func commandEnvironment(ctx context.Context, tc ToolContext) ([]string, error) {
	if tc.SubprocessEnv == nil {
		return nil, errNoSubprocessEnv
	}
	env, err := tc.SubprocessEnv(ctx)
	if err != nil {
		return nil, err
	}
	if env == nil {
		return nil, errors.New("run_command: subprocess environment producer returned no environment")
	}
	return env, nil
}

// backgroundOutcome starts one background job for the call: it
// refuses a missing subprocess-environment producer before any reservation,
// reserves the identity, registers the starting call's live output preview,
// hands the spawn to the background bridge, resolves the job's exact
// environment at the admitted Jobs start handoff, aborts the reservation and
// removes the preview when the bridge or the start rejects, and returns the
// retained immediate template on success. Its output is the same registry
// entry and the same `tool_output_changed` notice a foreground command
// publishes, addressed by the call that started the job. Every start failure —
// reserve, handoff, or spawn — renders through the one legacy wrapper.
func (t runCommandTool) backgroundOutcome(ctx context.Context, callID, command string, timeoutSec int, tc ToolContext) harness.ToolOutcome {
	if tc.SubprocessEnv == nil {
		return toolError(callID, errNoSubprocessEnv)
	}
	jobsInst := t.jobs
	sessionID := tc.AdmittedEntry.SessionID
	jobID, err := jobsInst.Reserve()
	if err != nil {
		return backgroundStartOutcome(callID, err)
	}
	t.toolOutput.register(sessionID, tc.OperationID, callID, func() string {
		return jobsInst.Preview(sessionID, jobID)
	})
	err = tc.Background.StartJob(ctx, jobID, func(ctx context.Context, deliver func(string) error) error {
		// The environment resolves at the admitted start handoff, and the
		// exact slice is the job process's environment; a
		// nil snapshot fails the start instead of inheriting ambient secrets.
		env, envErr := commandEnvironment(ctx, tc)
		if envErr != nil {
			return envErr
		}
		return jobsInst.Start(jobs.StartRequest{
			JobID:      jobID,
			SessionID:  sessionID,
			Workspace:  tc.Workspace,
			Command:    command,
			TimeoutSec: timeoutSec,
			Env:        env,
			Config:     tc.Invocation.jobsSettings(),
			OnExit: func(er jobs.ExitResult) {
				_ = deliver(legacyCompletionText(er)) // terminal for the job's completion; the exit callback has nowhere to propagate
				t.toolOutput.remove(sessionID, tc.OperationID, callID)
			},
			OnChange: func() {
				t.toolOutput.publishChanged(tc.Workspace, sessionID, tc.OperationID, callID)
			},
		})
	})
	if err != nil {
		jobsInst.Abort(jobID) // idempotent: removes the reservation if it still exists
		t.toolOutput.remove(sessionID, tc.OperationID, callID)
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultInterrupted, Content: "command cancelled"}}
		}
		return backgroundStartOutcome(callID, err)
	}
	return harness.ToolOutcome{Result: model.ToolResult{
		CallID: callID,
		Status: model.ResultSuccess,
		Content: fmt.Sprintf("Command running in the background with ID: `%s`. You will be notified when it finishes. If your next steps do not depend on its output, continue with them; otherwise use process to read the output.\nRunning in the background: `%s`.",
			jobID, strings.Join(jobsInst.Live(sessionID), ", ")),
	}}
}

// backgroundStartOutcome renders one background start failure through the
// single legacy wrapper.
func backgroundStartOutcome(callID string, err error) harness.ToolOutcome {
	return toolError(callID, fmt.Errorf("run_command: background start: %w", err))
}

// legacyCompletionText composes one job's terminal completion in the
// retained legacy form: `completed` for an empty reason, `(No output)` for
// empty output, and the whole string capped at the job's own
// max_output_bytes — the same bound capture uses — keeping the longest
// complete UTF-8 prefix that fits the trailing marker.
func legacyCompletionText(er jobs.ExitResult) string {
	reason := er.Reason
	if reason == "" {
		reason = "completed"
	}
	output := er.Output
	if output == "" {
		output = "(No output)"
	}
	return truncateCompletionText(
		fmt.Sprintf("Background process %s (%q) finished: %s, exit code %d.\nOutput:\n%s", er.ID, er.Command, reason, er.ExitCode, output),
		er.MaxOutputBytes)
}

// truncateCompletionText caps one rendered completion at the limit in UTF-8
// bytes, retaining the longest complete-character prefix that fits the
// trailing marker.
func truncateCompletionText(text string, limit int) string {
	const marker = "\n[truncated]"
	if limit <= 0 || len(text) <= limit {
		return text
	}
	max := limit - len(marker)
	if max < 0 {
		max = 0
	}
	for max > 0 && text[max]&0xC0 == 0x80 { // back off to a complete-character boundary
		max--
	}
	return text[:max] + marker
}

// foregroundOutcome constructs and registers the foreground call's
// output preview for live reads, publishes its throttled change notices on the
// Operation scope, and removes it after the runner returns.
func (t runCommandTool) foregroundOutcome(ctx context.Context, callID, command string, tc ToolContext, timeoutSec int, s toolSettings, spillDir string, env []string) harness.ToolOutcome {
	sessionID := tc.AdmittedEntry.SessionID
	capture := cmdoutput.NewCapture(cmdoutput.Options{
		Directory:    spillDir,
		SpillPrefix:  "cmd_output_",
		MaxBytes:     s.MaxOutputBytes,
		MaxLineChars: s.ReadLineMaxChars,
		OnChange: func() {
			t.toolOutput.publishChanged(tc.Workspace, sessionID, tc.OperationID, callID)
		},
	})
	t.toolOutput.register(sessionID, tc.OperationID, callID, capture.Preview)
	defer t.toolOutput.remove(sessionID, tc.OperationID, callID)
	return commandOutcome(ctx, callID, command, tc.Workspace, timeoutSec, capture, env)
}

// commandOutcome runs one foreground command through the shared
// runner over the resolved subprocess environment and maps the retained
// outcomes onto the model-visible result: a
// completed run settles as success or — for a nonzero exit — the same
// ExitError output the legacy engine reports as an error; a configured
// timeout settles as an error result with the retained timeout text; the
// runner's delivered-cause cancellation settles as an interrupted result with
// the retained cancellation text. The classification reads the cause the
// runner flagged on the error, never the context after the runner has settled
// the real result.
func commandOutcome(ctx context.Context, callID, command, dir string, timeoutSec int, capture *cmdoutput.Capture, env []string) harness.ToolOutcome {
	result, err := runForegroundCommandFn(ctx, command, dir, timeoutSec, capture, env)
	var exitErr *tool.ExitError
	if errors.As(err, &exitErr) {
		status := model.ResultError
		if exitErr.Cancelled {
			status = model.ResultInterrupted
		}
		return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: status, Content: exitErr.Output}}
	}
	if err != nil {
		return toolError(callID, err)
	}
	return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: result}}
}

// processTool is the process tool: always available; it maps the retained
// read/kill/list actions onto the jobs service under the owning session.
type processTool struct{ jobs jobs.Jobs }

func (t processTool) run(_ context.Context, tc ToolContext, call model.ToolCall) harness.ToolOutcome {
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return toolError(call.ID, err)
	}
	action, id, err := tool.ProcessArgs(args)
	if err != nil {
		return toolError(call.ID, err)
	}
	sessionID := tc.AdmittedEntry.SessionID
	var output string
	switch action {
	case "read":
		output, err = t.jobs.Read(sessionID, id)
	case "kill":
		err = t.jobs.Kill(sessionID, id)
		output = fmt.Sprintf("Process %s terminated.", id)
	case "list":
		output = t.jobs.List(sessionID)
	}
	if err != nil {
		return toolError(call.ID, err)
	}
	return harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: output}}
}

// standardToolDeclaration is one owner-constructed standard tool declaration:
// its model-visible ID and baseline definition, built from the literal
// description constants with no constructed state.
type standardToolDeclaration struct {
	id         string
	definition model.ToolDefinition
}

// standardToolDeclarations returns the five standard tool declarations in
// declaration order.
func standardToolDeclarations() []standardToolDeclaration {
	return []standardToolDeclaration{
		{id: "read_file", definition: model.ToolDefinition{Name: "read_file", Description: readFileDescription, Parameters: json.RawMessage(readFileParameters)}},
		{id: "write_file", definition: model.ToolDefinition{Name: "write_file", Description: writeFileDescription, Parameters: json.RawMessage(writeFileParameters)}},
		{id: "edit_file", definition: model.ToolDefinition{Name: "edit_file", Description: editFileDescription, Parameters: json.RawMessage(editFileParameters)}},
		{id: "run_command", definition: model.ToolDefinition{Name: "run_command", Description: runCommandDescription, Parameters: json.RawMessage(runCommandParameters)}},
		{id: "process", definition: model.ToolDefinition{Name: "process", Description: processDescription, Parameters: json.RawMessage(processParameters)}},
	}
}

// newStandardTools constructs the five standard tool values over the owner
// data root, the jobs service and the owner's live-output registry: the file
// tools share one state that carries no process dependency, run_command
// carries the jobs service and registry beside that state, and process
// carries only the jobs service; no per-Session state is created here and no
// environment key set is captured — the command paths resolve the live
// subprocess environment through the ToolContext's call-time producer.
func newStandardTools(dataDir string, jobsService jobs.Jobs, toolOutput *liveToolOutput) map[string]Tool {
	inst := &standardTools{dataDir: dataDir}
	return map[string]Tool{
		"read_file":   inst.read,
		"write_file":  inst.write,
		"edit_file":   inst.edit,
		"run_command": (runCommandTool{inst, jobsService, toolOutput}).run,
		"process":     (processTool{jobsService}).run,
	}
}
