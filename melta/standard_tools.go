package melta

// The baseline standard tools: the five owner-constructed Tool
// implementations — read_file, write_file, and edit_file over the shared file
// preparation, snapshot bookkeeping, and artifact-derived code groups, plus
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
	"github.com/Syntheresis/melta/internal/pathutil"
	"github.com/Syntheresis/melta/internal/snapshot"
	"github.com/Syntheresis/melta/internal/tool"
	"github.com/Syntheresis/melta/internal/toolargs"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/shellparse"
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
func immediateError(callID string, cause error) harness.PreparedTool {
	return harness.PreparedTool{Immediate: &harness.ToolOutcome{
		Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: boundedDiagnostic(cause)},
	}}
}

// immediateDenied is the failed-canonical-preparation immediate outcome: the
// fixed denial, per the tool-boundary contract.
func immediateDenied(callID string) harness.PreparedTool {
	return harness.PreparedTool{Immediate: &harness.ToolOutcome{
		Result: model.ToolResult{CallID: callID, Status: model.ResultDenied, Content: harness.PermissionDeniedContent},
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

// canonicalPaths computes the canonical Workspace root with the same
// pathutil canonicalization the shared preparation binds internally, so
// Harness policy containment and the closures' revalidation agree.
func canonicalPaths(root string) (string, error) {
	resolved, err := pathutil.ResolveFilePathFrom("", root)
	if err != nil {
		return "", err
	}
	return resolved.CanonicalPath, nil
}

// canonicalPathsFn is the one seam over preparation canonicalization: both
// containment inputs route through it, so tests flip a symlink between the
// tool's resolutions and the shared preparation's binding. Nil-free in
// production; it simply delegates to canonicalPaths.
var canonicalPathsFn = canonicalPaths

// runForegroundCommandFn is the one seam over the shared foreground runner:
// commandOutcome routes through it, so tests drive the runner's settled
// ExitError shapes with a context cancelled after the runner returns.
// Nil-free in production; it simply delegates to tool.RunForegroundCommand.
var runForegroundCommandFn = tool.RunForegroundCommand

func permissionPairs(permission string, targets []tool.PreparedTarget) []harness.PermissionRequest {
	pairs := make([]harness.PermissionRequest, 0, len(targets))
	for _, target := range targets {
		pairs = append(pairs, harness.PermissionRequest{Permission: permission, Target: target.CanonicalPath})
	}
	return pairs
}

// stringOutcome runs one prepared string-result call and wraps the outcome:
// the model-visible content is the shared body's own bounded output, and an
// optional metadata converter turns the committed normalized arguments plus
// the result into the owned metadata shape. A nil metadata map or a failed
// well-formedness marshal drops the optional metadata; the result itself
// always commits.
func stringOutcome(ctx context.Context, callID string, call *tool.PreparedCall, args string, metadata func(args, result string) map[string]any) harness.ToolOutcome {
	result, err := call.Execute(ctx, harness.LocationRecheckFromContext(ctx))
	if err != nil {
		return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: err.Error()}}
	}
	outcome := harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: result}}
	if metadata != nil {
		if m := metadata(args, result); m != nil {
			if raw, err := json.Marshal(m); err == nil {
				outcome.Metadata = raw
			}
		}
	}
	return outcome
}

// normalizeCallArguments is the one shared Normalize body for every tool:
// the strict decode of the call arguments, the tool's single argument
// normalization, and the marshaled normalized object.
func normalizeCallArguments(call model.ToolCall, normalize func(map[string]any) (map[string]any, error)) (json.RawMessage, error) {
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return nil, err
	}
	normalized, err := normalize(args)
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

// readTool is the read_file tool: always available, no metadata, the
// nil-tracker read path that always returns the bounded requested content.
type readTool struct{ inst *standardTools }

func (t readTool) Normalize(_ context.Context, tc ToolContext, call model.ToolCall) (json.RawMessage, error) {
	s, err := t.inst.settings(tc.Invocation)
	if err != nil {
		return nil, err
	}
	return normalizeCallArguments(call, func(args map[string]any) (map[string]any, error) {
		return tool.NormalizeReadArgs(args, s.ReadMaxLines)
	})
}

func (t readTool) Prepare(_ context.Context, tc ToolContext, call model.ToolCall) harness.PreparedTool {
	s, err := t.inst.settings(tc.Invocation)
	if err != nil {
		return immediateError(call.ID, err)
	}
	// Preparation accepts only the committed normalized arguments: one
	// strict decode-and-pass, never a second normalization.
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return immediateError(call.ID, err)
	}
	workspace, err := canonicalPathsFn(tc.Workspace)
	if err != nil {
		return immediateDenied(call.ID)
	}
	prepared, err := tool.PrepareReadCall(tc.Workspace, workspace, s.toolsConfig(), args)
	if err != nil {
		return immediateDenied(call.ID)
	}
	return harness.PreparedTool{
		Permissions:        permissionPairs(harness.PermissionFileRead, prepared.Targets),
		CanonicalWorkspace: workspace,
		Execute: func(ctx context.Context) harness.ToolOutcome {
			return stringOutcome(ctx, call.ID, prepared, string(call.Arguments), nil)
		},
	}
}

// mutationPrepared is one mutation tool's prepared call pieces: the
// declared canonical targets and the outcome-wrapping executor.
type mutationPrepared struct {
	targets []tool.PreparedTarget
	execute func(context.Context) harness.ToolOutcome
}

// prepareMutation is the one shared mutation-Prepare body: the strict
// decode-and-pass of the committed normalized arguments, the canonical root
// binding, the per-call code group, and the assembled executor plan with its
// file.write pairs. The per-tool callback runs the shared preparation and
// wraps the outcome; its error is a failed canonical preparation.
func (in *standardTools) prepareMutation(tc ToolContext, call model.ToolCall, run func(root, rootCanonical string, group *snapshot.CodeStore, args map[string]any) (mutationPrepared, error)) harness.PreparedTool {
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return immediateError(call.ID, err)
	}
	workspace, err := canonicalPathsFn(tc.Workspace)
	if err != nil {
		return immediateDenied(call.ID)
	}
	group, err := in.openCodeGroup(tc)
	if err != nil {
		return immediateDenied(call.ID)
	}
	prepared, err := run(tc.Workspace, workspace, group, args)
	if err != nil {
		return immediateDenied(call.ID)
	}
	return harness.PreparedTool{
		Permissions:        permissionPairs(harness.PermissionFileWrite, prepared.targets),
		CanonicalWorkspace: workspace,
		Execute:            prepared.execute,
	}
}

// writeTool is the write_file tool.
type writeTool struct{ inst *standardTools }

func (writeTool) Normalize(_ context.Context, _ ToolContext, call model.ToolCall) (json.RawMessage, error) {
	return normalizeCallArguments(call, tool.NormalizeWriteArgs)
}

func (t writeTool) Prepare(_ context.Context, tc ToolContext, call model.ToolCall) harness.PreparedTool {
	return t.inst.prepareMutation(tc, call, func(root, rootCanonical string, group *snapshot.CodeStore, args map[string]any) (mutationPrepared, error) {
		prepared, err := tool.PrepareWriteCall(root, rootCanonical, group, args)
		if err != nil {
			return mutationPrepared{}, err
		}
		return mutationPrepared{
			targets: prepared.Targets,
			execute: func(ctx context.Context) harness.ToolOutcome {
				return stringOutcome(ctx, call.ID, prepared, string(call.Arguments), nil)
			},
		}, nil
	})
}

// editTool is the edit_file tool; its metadata carries the edit_preview diff
// built from the committed normalized arguments and the result.
type editTool struct{ inst *standardTools }

func (editTool) Normalize(_ context.Context, _ ToolContext, call model.ToolCall) (json.RawMessage, error) {
	return normalizeCallArguments(call, tool.NormalizeEditArgs)
}

func (t editTool) Prepare(_ context.Context, tc ToolContext, call model.ToolCall) harness.PreparedTool {
	return t.inst.prepareMutation(tc, call, func(root, rootCanonical string, group *snapshot.CodeStore, args map[string]any) (mutationPrepared, error) {
		prepared, err := tool.PrepareEditCall(root, rootCanonical, group, args)
		if err != nil {
			return mutationPrepared{}, err
		}
		return mutationPrepared{
			targets: prepared.Targets,
			execute: func(ctx context.Context) harness.ToolOutcome {
				return stringOutcome(ctx, call.ID, prepared, string(call.Arguments), editpreview.MetadataFromArgs)
			},
		}, nil
	})
}

// runCommandTool is the run_command tool: always available. It decomposes
// conclusive simple commands into one command.run target per complete
// segment, falls back to one complete target for everything else, and always
// executes through the shell. background=true starts a job through the
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

func (t runCommandTool) Normalize(_ context.Context, tc ToolContext, call model.ToolCall) (json.RawMessage, error) {
	s, err := t.inst.settings(tc.Invocation)
	if err != nil {
		return nil, err
	}
	return normalizeCallArguments(call, func(args map[string]any) (map[string]any, error) {
		return tool.NormalizeRunCommandArgs(args, s.CommandTimeout)
	})
}

func (t runCommandTool) Prepare(_ context.Context, tc ToolContext, call model.ToolCall) harness.PreparedTool {
	s, err := t.inst.settings(tc.Invocation)
	if err != nil {
		return immediateError(call.ID, err)
	}
	// Preparation accepts only the committed normalized arguments: one
	// strict decode-and-pass, never a second normalization.
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return immediateError(call.ID, err)
	}
	command, _ := args["command"].(string)
	timeoutSec := s.CommandTimeout
	var targets []string
	if command == "" {
		return immediateError(call.ID, errors.New("run_command: command is required"))
	}
	if v, ok := args["timeout"].(json.Number); ok {
		if n, err := v.Int64(); err == nil {
			timeoutSec = int(n)
		}
	}
	if segments, ok := shellparse.ParseSimple(command); ok {
		for _, segment := range segments {
			targets = append(targets, segment.Text)
		}
	}
	if len(targets) == 0 {
		// One complete-command fallback: the input with only leading
		// ASCII blanks/newlines removed, or the complete original input
		// when nothing remains. The exact text is both target and the
		// script the shell runs.
		fallback := strings.TrimLeft(command, " \t\n")
		if fallback == "" {
			fallback = command
		}
		targets = []string{fallback}
		command = fallback
	}
	// The converged tail: the resolved command and targets declare the one
	// shared permission set, and background=true swaps the execute body for
	// the job-start path (the model timeout forwards when >= 1; absent or
	// sub-one becomes 0).
	background, _ := args["background"].(bool)
	backgroundTimeoutSec := 0
	if v, ok := args["timeout"].(json.Number); ok {
		if n, err := v.Int64(); err == nil && n >= 1 {
			backgroundTimeoutSec = int(n)
		}
	}
	pairs := make([]harness.PermissionRequest, 0, len(targets))
	for _, target := range targets {
		pairs = append(pairs, harness.PermissionRequest{Permission: harness.PermissionCommandRun, Target: target})
	}
	spillDir := filepath.Join(t.inst.dataDir, "code", tc.AdmittedEntry.SessionID, "output")
	return harness.PreparedTool{
		Permissions: pairs,
		Execute: func(ctx context.Context) harness.ToolOutcome {
			if background {
				return t.backgroundOutcome(ctx, call.ID, command, backgroundTimeoutSec, tc)
			}
			// The environment resolves immediately at this process start,
			// never at Prepare: a key connected after the call was prepared
			// is still scrubbed, and a shell sibling stays visible.
			env, envErr := commandEnvironment(ctx, tc)
			if envErr != nil {
				return commandEnvironmentFailure(call.ID, envErr)
			}
			return t.foregroundOutcome(ctx, call.ID, command, tc, timeoutSec, s, spillDir, env)
		},
	}
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

// commandEnvironmentFailure renders one environment-resolution failure as the
// ordinary model-visible error outcome.
func commandEnvironmentFailure(callID string, cause error) harness.ToolOutcome {
	return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: boundedDiagnostic(cause)}}
}

// backgroundOutcome starts one background job for the prepared call: it
// refuses a missing subprocess-environment producer before any reservation,
// reserves the identity, registers the starting call's live output preview,
// hands the spawn to the background bridge, resolves the job's exact
// environment at the admitted Jobs start handoff, aborts the reservation and
// removes the preview when the bridge or the start rejects, and returns the
// retained immediate template on success. Its output is the same registry
// entry and the same `tool_output_changed` notice a foreground command
// publishes, addressed by the call that started the job. Every start failure —
// reserve, handoff, or spawn — renders through the one legacy wrapper.
func (t runCommandTool) backgroundOutcome(_ context.Context, callID, command string, timeoutSec int, tc ToolContext) harness.ToolOutcome {
	if tc.SubprocessEnv == nil {
		return commandEnvironmentFailure(callID, errNoSubprocessEnv)
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
	err = tc.Background.StartJob(jobID, func(ctx context.Context, deliver func(string) error) error {
		// The environment resolves at the admitted start handoff — never at
		// Prepare — and the exact slice is the job process's environment; a
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
	return harness.ToolOutcome{Result: model.ToolResult{
		CallID:  callID,
		Status:  model.ResultError,
		Content: fmt.Errorf("run_command: background start: %w", err).Error(),
	}}
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

// foregroundOutcome constructs and registers the prepared foreground call's
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

// commandOutcome runs one prepared foreground command through the shared
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
		return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: err.Error()}}
	}
	return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: result}}
}

// processTool is the process tool: always available; it maps the retained
// read/kill/list actions onto the jobs service under the owner-scoped
// canonical target <session-id>/<job-id> (read/kill) or the fixed target *
// (list). The jobs service is its own field.
type processTool struct{ jobs jobs.Jobs }

func (processTool) Normalize(_ context.Context, _ ToolContext, call model.ToolCall) (json.RawMessage, error) {
	return normalizeCallArguments(call, normalizeProcessArgs)
}

func (t processTool) Prepare(_ context.Context, tc ToolContext, call model.ToolCall) harness.PreparedTool {
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return immediateError(call.ID, err)
	}
	action, _ := args["action"].(string)
	sessionID := tc.AdmittedEntry.SessionID
	switch action {
	case "read", "kill":
		id, _ := args["id"].(string)
		if id == "" {
			return immediateError(call.ID, fmt.Errorf("process: id is required for %s", action))
		}
		if !isJobID(id) {
			return immediateError(call.ID, fmt.Errorf("process: no process with ID %q", id))
		}
		target := sessionID + "/" + id
		return harness.PreparedTool{
			Permissions: []harness.PermissionRequest{{Permission: harness.PermissionProcessOwn, Target: target}},
			Execute: func(_ context.Context) harness.ToolOutcome {
				if action == "read" {
					output, err := t.jobs.Read(sessionID, id)
					if err != nil {
						return harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultError, Content: err.Error()}}
					}
					return harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: output}}
				}
				if err := t.jobs.Kill(sessionID, id); err != nil {
					return harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultError, Content: err.Error()}}
				}
				return harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: fmt.Sprintf("Process %s terminated.", id)}}
			},
		}
	case "list":
		return harness.PreparedTool{
			Permissions: []harness.PermissionRequest{{Permission: harness.PermissionProcessOwn, Target: "*"}},
			Execute: func(_ context.Context) harness.ToolOutcome {
				return harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: t.jobs.List(sessionID)}}
			},
		}
	default:
		return immediateError(call.ID, fmt.Errorf("process: unknown action %q", action))
	}
}

// normalizeProcessArgs is the process tool's member validator: private
// `_melta_` fields are stripped, a present action or id must be a
// string, and every other member is preserved; Prepare owns the semantics.
func normalizeProcessArgs(args map[string]any) (map[string]any, error) {
	clean := make(map[string]any, len(args))
	for key, value := range args {
		if strings.HasPrefix(key, "_melta_") {
			continue
		}
		if key == "action" || key == "id" {
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("process: %s must be a string", key)
			}
		}
		clean[key] = value
	}
	return clean, nil
}

// isJobID reports whether id is inside the jobs service's
// 8-lowercase-hex identity namespace.
func isJobID(id string) bool {
	if len(id) != 8 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
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
		"read_file":   readTool{inst},
		"write_file":  writeTool{inst},
		"edit_file":   editTool{inst},
		"run_command": runCommandTool{inst, jobsService, toolOutput},
		"process":     processTool{jobsService},
	}
}
