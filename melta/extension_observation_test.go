package melta

// Independent-command and committed-observation transport suite: the same
// observer, timer submit, saved-callback reuse, in-callback command and target
// containment run through the in-process native author and the installed
// worker fixture at the owner boundary. Retained-work delivery and the
// bounded-queue isolation rules are exercised once at their owner.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// reportLog reads the fixture's JSON-line report file in arrival order.
type reportLog struct {
	path string
}

func newReportLog(path string) *reportLog {
	return &reportLog{path: path}
}

func (l *reportLog) snapshot() []map[string]string {
	data, err := os.ReadFile(l.path)
	if err != nil {
		return nil
	}
	var reports []map[string]string
	for _, line := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		var report map[string]string
		if json.Unmarshal([]byte(line), &report) != nil {
			continue
		}
		reports = append(reports, report)
	}
	return reports
}

// awaitKind waits for one report of the wanted kind and returns it.
func (l *reportLog) awaitKind(t *testing.T, kind string) map[string]string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for _, report := range l.snapshot() {
			if report["kind"] == kind {
				return report
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q report arrived: %+v", kind, l.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// countKind counts reports of one kind whose operation matches; an empty
// operation matches every report.
func (l *reportLog) countKind(kind, operation string) int {
	count := 0
	for _, report := range l.snapshot() {
		if report["kind"] == kind && (operation == "" || report["operation"] == operation) {
			count++
		}
	}
	return count
}

var independentReportMu sync.Mutex

// appendIndependentReport appends one JSON report line to the fixture's report
// file under one process-wide lock, so concurrent observers and timers never
// interleave a line.
func appendIndependentReport(path, kind, class, value, session, operation, status, version string) {
	if path == "" {
		return
	}
	line, err := json.Marshal(map[string]string{
		"kind": kind, "class": class, "value": value, "session": session,
		"operation": operation, "status": status, "version": version,
	})
	if err != nil {
		return
	}
	independentReportMu.Lock()
	defer independentReportMu.Unlock()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) // #nosec G304 -- The test harness supplies its private temporary report path through settings.
	if err != nil {
		return
	}
	_, _ = file.Write(append(line, '\n'))
	_ = file.Close()
}

// independentSettings is the fixture's own settings section: the private test
// report path and the activation version tag.
type independentSettings struct {
	Enabled bool   `json:"enabled"`
	Report  string `json:"report"`
	Version string `json:"version"`
	Observe string `json:"observe"`
}

func independentSettingsOf(api extension.API) independentSettings {
	var settings independentSettings
	_ = json.Unmarshal(api.Settings(), &settings)
	return settings
}

func independentNativeClass(err error) string {
	if err == nil {
		return ""
	}
	return string(ErrorCode(err))
}

func nativeIndependentReport(api extension.API, kind string, result extension.Result, err error) {
	settings := independentSettingsOf(api)
	appendIndependentReport(settings.Report, kind, independentNativeClass(err), string(result.Value), "", "", "", settings.Version)
}

func nativeIndependentReportFact(api extension.API, fact extension.Fact) {
	settings := independentSettingsOf(api)
	appendIndependentReport(settings.Report, "fact", "", "", fact.SessionID, fact.OperationID, fact.Status, settings.Version)
}

func nativeSessionTarget(sessionID string) extension.CommandTarget {
	return extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: sessionID}
}

// independentToolArgs reads one tool fixture argument object.
type independentToolArgs struct {
	Phase       string `json:"phase"`
	OperationID string `json:"operation_id"`
	Parent      string `json:"parent"`
}

func independentToolPhase(raw json.RawMessage) independentToolArgs {
	var args independentToolArgs
	_ = json.Unmarshal(raw, &args)
	return args
}

// independentCommandAfterSettled issues one command after the named parent's
// settled record, retrying only while an unrelated conflict class clears.
func independentCommandAfterSettled(api extension.API, target extension.CommandTarget, operationID string) (extension.Result, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		result, err := api.Command(context.Background(), target, "submit", independentSubmitRequest(operationID))
		if err == nil || independentNativeClass(err) != "conflict" || time.Now().After(deadline) {
			return result, err
		}
		time.Sleep(time.Millisecond)
	}
}

// waitSettled waits for the named parent Operation's recorded settlement.
func waitSettled(settled <-chan string, parent string) bool {
	deadline := time.After(10 * time.Second)
	for {
		select {
		case operationID := <-settled:
			if operationID == parent {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// independentSubmitRequest builds one fixture submit request.
func independentSubmitRequest(operationID string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{
		"operation_id": operationID,
		"mode":         "regular",
		"content":      []map[string]string{{"kind": "text", "text": "tool"}},
	})
	return raw
}

// independentCommandTool issues API.Command during and after its own Prepare
// and Execute callbacks, reporting the class to the fixture report file.
type independentCommandTool struct {
	api     extension.API
	settled chan string
}

func (t independentCommandTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func independentToolOutcome(callID, content string) harness.ToolOutcome {
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: content}}
}

func (t independentCommandTool) Prepare(ctx context.Context, call extension.AuthorContext, input model.ToolCall) extension.ToolPlan {
	args := independentToolPhase(input.Arguments)
	phase, operationID := args.Phase, args.OperationID
	switch phase {
	case "prepare":
		result, err := t.api.Command(ctx, nativeSessionTarget(call.Session.SessionID), "submit", independentSubmitRequest(operationID))
		nativeIndependentReport(t.api, "tool-prepare", result, err)
		return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: input.ID, Status: model.ResultSuccess, Content: "prepared"}}}
	case "prepare_after":
		settings := independentSettingsOf(t.api)
		sessionID := call.Session.SessionID
		go func() {
			if !waitSettled(t.settled, args.Parent) {
				return
			}
			result, err := independentCommandAfterSettled(t.api, nativeSessionTarget(sessionID), operationID)
			appendIndependentReport(settings.Report, "tool-prepare-after", independentNativeClass(err), string(result.Value), "", operationID, "", settings.Version)
		}()
		return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: input.ID, Status: model.ResultSuccess, Content: "prepared"}}}
	}
	payload, _ := json.Marshal(map[string]json.RawMessage{
		"id":        json.RawMessage(`"` + input.ID + `"`),
		"arguments": append(json.RawMessage(nil), input.Arguments...),
	})
	return extension.ToolPlan{Data: payload, Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "fixture"}}}
}

func (t independentCommandTool) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	var payload struct {
		ID        string          `json:"id"`
		Arguments json.RawMessage `json:"arguments"`
	}
	_ = json.Unmarshal(plan.Data, &payload)
	args := independentToolPhase(payload.Arguments)
	phase, operationID := args.Phase, args.OperationID
	switch phase {
	case "execute":
		result, err := t.api.Command(ctx, nativeSessionTarget(call.Session.SessionID), "submit", independentSubmitRequest(operationID))
		nativeIndependentReport(t.api, "tool-execute", result, err)
	case "execute_after":
		settings := independentSettingsOf(t.api)
		sessionID := call.Session.SessionID
		go func() {
			if !waitSettled(t.settled, args.Parent) {
				return
			}
			result, err := independentCommandAfterSettled(t.api, nativeSessionTarget(sessionID), operationID)
			appendIndependentReport(settings.Report, "tool-execute-after", independentNativeClass(err), string(result.Value), "", operationID, "", settings.Version)
		}()
	}
	return independentToolOutcome(payload.ID, "executed")
}

func nativeIndependentOperation(name string, handler extension.AuthorHandler) extension.Operation {
	return extension.Operation{
		Target:  extension.ExposedTargetSession,
		Method:  extension.Method{Name: name, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{}`)},
		Handler: handler,
	}
}

// nativeIndependentFixture is the in-process counterpart of the worker
// fixture's independent mode.
func nativeIndependentFixture() extension.Extension {
	return extension.Extension{Manifest: authorFixtureManifest(), Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		_, activateErr := api.Command(ctx, extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "interrupt", json.RawMessage(`{}`))
		nativeIndependentReport(api, "activate", extension.Result{}, activateErr)

		settled := make(chan string)
		issuedCommand := false
		observerHandle, err := api.AttachObserver(func(ctx context.Context, fact extension.Fact) error {
			settings := independentSettingsOf(api)
			if settings.Observe == "block" {
				appendIndependentReport(settings.Report, "observe-started", "", "", fact.SessionID, fact.OperationID, fact.Status, settings.Version)
				<-ctx.Done()
				appendIndependentReport(settings.Report, "observe-canceled", "", "", fact.SessionID, fact.OperationID, fact.Status, settings.Version)
				return ctx.Err()
			}
			nativeIndependentReportFact(api, fact)
			if settings.Observe == "command" && fact.Kind == extension.FactOperationAdmitted && !issuedCommand {
				issuedCommand = true
				arguments, _ := json.Marshal(map[string]string{"workspace": filepath.Dir(settings.Report)})
				result, err := api.Command(ctx, extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "create_session", arguments)
				appendIndependentReport(settings.Report, "observer-command", independentNativeClass(err), string(result.Value), fact.SessionID, fact.OperationID, fact.Status, settings.Version)
			}
			if fact.Kind == extension.FactOperationSettled {
				select {
				case settled <- fact.OperationID:
				default:
				}
			}
			return nil
		}, nil)
		if err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(nativeIndependentOperation("remove_observer", func(_ context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			nativeIndependentReport(api, "observer-remove", extension.Result{}, api.Remove(observerHandle))
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterTool(testDescribeTool("command-tool"), independentCommandTool{api: api, settled: settled}, nil); err != nil {
			return nil, err
		}

		if _, err := api.RegisterOperation(nativeIndependentOperation("nil_command", func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			sessionID := call.Session.SessionID
			settings := independentSettingsOf(api)
			go func() {
				result, err := api.Command(nil, nativeSessionTarget(sessionID), "submit", json.RawMessage(`{"operation_id":"op-nil-context","mode":"regular","content":[{"kind":"text","text":"nil context"}]}`))
				appendIndependentReport(settings.Report, "nil-command", independentNativeClass(err), string(result.Value), sessionID, "", "", settings.Version)
			}()
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachRequest(func(_ context.Context, call extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			settings := independentSettingsOf(api)
			if settings.Observe != "request-block" {
				return harness.RequestConsequence{}, nil
			}
			appendIndependentReport(settings.Report, "request-entered", "", "", call.Session.SessionID, "", "", settings.Version)
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(settings.Report + ".release"); err == nil {
					break
				}
				if time.Now().After(deadline) {
					return harness.RequestConsequence{}, errors.New("request hook release timed out")
				}
				time.Sleep(5 * time.Millisecond)
			}
			return harness.RequestConsequence{}, nil
		}, nil); err != nil {
			return nil, err
		}
		var saved func(context.Context, string, string, json.RawMessage) (extension.Result, error)
		if _, err := api.RegisterOperation(nativeIndependentOperation("timer", func(_ context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			sessionID := call.Session.SessionID
			payload := append(json.RawMessage(nil), args...)
			settings := independentSettingsOf(api)
			go func() {
				time.Sleep(20 * time.Millisecond)
				result, err := api.Command(context.Background(), nativeSessionTarget(sessionID), "submit", payload)
				appendIndependentReport(settings.Report, "timer", independentNativeClass(err), string(result.Value), sessionID, "", "", settings.Version)
			}()
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(nativeIndependentOperation("in_callback", func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			var request struct {
				Independent bool            `json:"independent"`
				Arguments   json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(args, &request); err != nil {
				return extension.Result{}, err
			}
			sessionID := call.Session.SessionID
			settings := independentSettingsOf(api)
			if request.Independent {
				go func() {
					result, err := api.Command(context.Background(), nativeSessionTarget(sessionID), "submit", request.Arguments)
					appendIndependentReport(settings.Report, "in-callback", independentNativeClass(err), string(result.Value), sessionID, "", "", settings.Version)
				}()
				return extension.Result{Value: json.RawMessage(`{}`)}, nil
			}
			result, err := api.Command(ctx, nativeSessionTarget(sessionID), "submit", request.Arguments)
			nativeIndependentReport(api, "in-callback", result, err)
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(nativeIndependentOperation("save", func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			saved = call.Call
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(nativeIndependentOperation("replay", func(ctx context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			var replayArgs struct {
				Method    string          `json:"method"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(args, &replayArgs); err != nil {
				return extension.Result{}, err
			}
			if saved == nil {
				settings := independentSettingsOf(api)
				appendIndependentReport(settings.Report, "replay", "internal", "no saved callback", "", "", "", settings.Version)
				return extension.Result{Value: json.RawMessage(`{}`)}, nil
			}
			result, err := saved(ctx, extension.ContractCommands, replayArgs.Method, replayArgs.Arguments)
			nativeIndependentReport(api, "replay", result, err)
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(nativeIndependentOperation("cross", func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			var request struct {
				SessionID string          `json:"session_id"`
				Method    string          `json:"method"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(args, &request); err != nil {
				return extension.Result{}, err
			}
			settings := independentSettingsOf(api)
			go func() {
				result, err := api.Command(context.Background(), nativeSessionTarget(request.SessionID), request.Method, request.Arguments)
				appendIndependentReport(settings.Report, "cross", independentNativeClass(err), string(result.Value), request.SessionID, "", "", settings.Version)
			}()
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		return nil, nil
	}}
}

// setExtensionSection replaces one extension's complete configuration section.
func setExtensionSection(t *testing.T, configPath, id, section string) {
	t.Helper()
	data, err := os.ReadFile(configPath)
	testassert.NoError(t, err)
	var root map[string]json.RawMessage
	testassert.NoError(t, json.Unmarshal(data, &root))
	var extensions map[string]json.RawMessage
	if len(root["extensions"]) != 0 {
		testassert.NoError(t, json.Unmarshal(root["extensions"], &extensions))
	}
	if extensions == nil {
		extensions = map[string]json.RawMessage{}
	}
	extensions[id] = json.RawMessage(section)
	root["extensions"], err = json.Marshal(extensions)
	testassert.NoError(t, err)
	data, err = json.Marshal(root)
	testassert.NoError(t, err)
	writeServiceFile(t, configPath, string(data))
}

// independentOwner opens one real owner over the requested transport with the
// author fixture enabled and its reports collected at path.
func independentOwnerAt(t *testing.T, workerMode bool, access map[string]ExtensionAccess, reportPath, version, observe string) (*Melta, *reportLog, *ownerEnv) {
	t.Helper()
	e := newOwnerEnv(t)
	section := fmt.Sprintf(`{"enabled":true,"report":%q`, reportPath)
	if version != "" {
		section += fmt.Sprintf(`,"version":%q`, version)
	}
	if observe != "" {
		section += fmt.Sprintf(`,"observe":%q`, observe)
	}
	section += "}"
	setExtensionSection(t, e.configPath, authorFixtureManifest().ID, section)
	e.fixtureStore(storage.NewMemory())
	opts := e.options()
	if workerMode {
		spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
			return nativeIndependentFixture(), nil
		}}
		spawner.install()
		opts.Extensions = append(opts.Extensions, executableAuthorExtension(authorFixtureManifest(), "author-worker", adapterEnvironment))
	} else {
		opts.Extensions = append(opts.Extensions, nativeIndependentFixture())
	}
	opts.Access = access
	r, err := open(context.Background(), opts)
	testassert.Require(t, err == nil, "open: %v", err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r, newReportLog(reportPath), e
}

func independentOwner(t *testing.T, workerMode bool, access map[string]ExtensionAccess) (*Melta, *reportLog) {
	t.Helper()
	r, log, _ := independentOwnerAt(t, workerMode, access, filepath.Join(t.TempDir(), "reports.jsonl"), "", "")
	return r, log
}

func invokeFixtureOperation(t *testing.T, r *Melta, sessionID, operation, arguments, requestID string) {
	t.Helper()
	_, err := r.InvokeExtension(context.Background(), testCaller(r), fixtureRequest(authorFixtureManifest().ID, operation, sessionTarget(sessionID), arguments, requestID))
	testassert.NoError(t, err)
}

// requireNoOperation proves a refused independent command performed no
// admission.
func requireNoOperation(t *testing.T, r *Melta, sessionID, operationID string) {
	t.Helper()
	err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
		_, err := h.ReadOperation(ctx, sessionID, operationID)
		return err
	})
	testassert.Require(t, errors.Is(err, harness.ErrNotFound), "operation %q exists after a refused command: %v", operationID, err)
}

// requireAttributedInput proves one admitted input entry carries the fixture's
// plugin origin and extension attribution.
func requireAttributedInput(t *testing.T, r *Melta, sessionID, operationID string) {
	t.Helper()
	facts, _ := historyThroughRuntime(t, r, sessionID)
	for _, fact := range facts {
		if fact.Input != nil && fact.Input.OperationID == operationID {
			testassert.Require(t, fact.Input.Origin == harness.InputOriginPlugin && fact.Input.ExtensionID == authorFixtureManifest().ID,
				"admitted input attribution = (%s, %q), want plugin/%s", fact.Input.Origin, fact.Input.ExtensionID, authorFixtureManifest().ID)
			return
		}
	}
	t.Fatalf("no admitted input entry for operation %q in %+v", operationID, facts)
}

func awaitSettledReport(t *testing.T, log *reportLog, operationID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for log.countKind("fact", operationID) < 2 {
		testassert.Require(t, !time.Now().After(deadline), "observer facts for %q never arrived: %+v", operationID, log.snapshot())
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAuthorIndependentCommand submits from a timer after the callback
// returned, proves the attributed admission and returned receipt, and refuses
// the in-callback command, saved callback reuse and closed activation.
func TestAuthorIndependentCommand(t *testing.T) {
	for _, mode := range []struct {
		name   string
		worker bool
	}{{name: "native"}, {name: "worker", worker: true}} {
		t.Run(mode.name, func(t *testing.T) {
			r, log := independentOwner(t, mode.worker, nil)
			activate := log.awaitKind(t, "activate")
			testassert.Require(t, activate["class"] == "closed", "pre-publication command class = %q, want closed", activate["class"])

			session := projectionSession(t, r, filepath.Join(t.TempDir(), "ws"), "solo")
			sessionID := session.Identity.SessionID

			submit := commandSubmitRequest(t, "op-timer", "hello")
			rawSubmit, err := json.Marshal(submit)
			testassert.NoError(t, err)
			invokeFixtureOperation(t, r, sessionID, "timer", string(rawSubmit), "req-timer")

			timer := log.awaitKind(t, "timer")
			testassert.Require(t, timer["class"] == "", "timer command failed: %+v", timer)
			testassert.Require(t, strings.Contains(timer["value"], `"operation_id":"op-timer"`), "timer receipt = %q, want the admission receipt", timer["value"])
			awaitOperation(t, r, sessionID, "op-timer", harness.OperationSuccess)
			requireAttributedInput(t, r, sessionID, "op-timer")

			// The observer receives the committed admission and settlement
			// exactly once for the operation the timer submitted.
			awaitSettledReport(t, log, "op-timer")
			admitted, settled := 0, 0
			for _, report := range log.snapshot() {
				if report["kind"] != "fact" || report["operation"] != "op-timer" {
					continue
				}
				switch report["status"] {
				case "":
					admitted++
				case string(harness.OperationSuccess):
					settled++
					testassert.Require(t, report["session"] == sessionID, "settled fact = %+v", report)
				}
			}
			testassert.Require(t, admitted == 1 && settled == 1, "observer facts for op-timer = admitted %d settled %d", admitted, settled)

			// A command issued with its own operation callback's context is
			// denied and performs no admission.
			inCallbackSubmit := commandSubmitRequest(t, "op-in-callback", "inside")
			rawInCallback, err := json.Marshal(inCallbackSubmit)
			testassert.NoError(t, err)
			inCallbackArguments, err := json.Marshal(map[string]json.RawMessage{"arguments": rawInCallback})
			testassert.NoError(t, err)
			invokeFixtureOperation(t, r, sessionID, "in_callback", string(inCallbackArguments), "req-in-callback")
			inCallback := log.awaitKind(t, "in-callback")
			testassert.Require(t, inCallback["class"] == "denied", "in-callback command = %+v, want denied", inCallback)
			requireNoOperation(t, r, sessionID, "op-in-callback")

			// A saved callback belongs to the returned call and stays closed.
			invokeFixtureOperation(t, r, sessionID, "save", `{}`, "req-save")
			replaySubmit := commandSubmitRequest(t, "op-replay", "after")
			rawReplay, err := json.Marshal(replaySubmit)
			testassert.NoError(t, err)
			replayArguments, err := json.Marshal(map[string]json.RawMessage{"method": json.RawMessage(`"submit"`), "arguments": rawReplay})
			testassert.NoError(t, err)
			invokeFixtureOperation(t, r, sessionID, "replay", string(replayArguments), "req-replay")
			replay := log.awaitKind(t, "replay")
			testassert.Require(t, replay["class"] == "closed", "saved callback reuse = %+v, want closed", replay)
			requireNoOperation(t, r, sessionID, "op-replay")
		})
	}
}

// TestAuthorObserverCommand proves a command issued from an observer's
// delivery context is independent and succeeds.
func TestAuthorObserverCommand(t *testing.T) {
	for _, mode := range []struct {
		name   string
		worker bool
	}{{name: "native"}, {name: "worker", worker: true}} {
		t.Run(mode.name, func(t *testing.T) {
			r, log, _ := independentOwnerAt(t, mode.worker, nil, filepath.Join(t.TempDir(), "reports.jsonl"), "", "command")
			caller := testCaller(r)
			session := projectionSession(t, r, filepath.Join(t.TempDir(), "ws"), "")
			_, err := r.SubmitSession(context.Background(), caller, session.Identity.SessionID, protocol.SubmitRequest{
				OperationId: "op-observer-command", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "observed", nil)},
			})
			testassert.NoError(t, err)
			report := log.awaitKind(t, "observer-command")
			testassert.Require(t, report["class"] == "", "observer command = %+v, want an independent success", report)
			testassert.Require(t, strings.Contains(report["value"], `"session_id"`), "observer command value = %q, want a newly created Session", report["value"])
			awaitOperation(t, r, session.Identity.SessionID, "op-observer-command", harness.OperationSuccess)
		})
	}
}

// TestAuthorCommandIndependentOfRequestHook proves an independent command for
// one Session succeeds while a request hook of the same activation is parked
// in another Session. The hook issues no command itself.
func TestAuthorCommandIndependentOfRequestHook(t *testing.T) {
	for _, mode := range []struct {
		name   string
		worker bool
	}{{name: "native"}, {name: "worker", worker: true}} {
		t.Run(mode.name, func(t *testing.T) {
			reportPath := filepath.Join(t.TempDir(), "reports.jsonl")
			r, log, _ := independentOwnerAt(t, mode.worker, nil, reportPath, "", "request-block")
			caller := testCaller(r)
			parked := projectionSession(t, r, filepath.Join(t.TempDir(), "ws"), "")
			_, err := r.SubmitSession(context.Background(), caller, parked.Identity.SessionID, protocol.SubmitRequest{
				OperationId: "op-request-hook", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "park", nil)},
			})
			testassert.NoError(t, err)
			log.awaitKind(t, "request-entered")

			// The independent command runs for another Session while the
			// first Session's request hook is still in flight.
			other := projectionSession(t, r, filepath.Join(t.TempDir(), "ws"), "")
			submit := commandSubmitRequest(t, "op-independent", "independent")
			rawSubmit, err := json.Marshal(submit)
			testassert.NoError(t, err)
			invokeFixtureOperation(t, r, other.Identity.SessionID, "timer", string(rawSubmit), "req-independent")
			report := log.awaitKind(t, "timer")
			testassert.Require(t, report["class"] == "", "independent command = %+v, want success", report)

			testassert.NoError(t, os.WriteFile(reportPath+".release", []byte("release"), 0o600))
			awaitOperation(t, r, other.Identity.SessionID, "op-independent", harness.OperationSuccess)
			awaitOperation(t, r, parked.Identity.SessionID, "op-request-hook", harness.OperationSuccess)
		})
	}
}

// independentLastUserText reads the last user message's text through string
// and content-part encodings: system-prompt and history text never selects a
// fixture turn.
func independentLastUserText(body string) string {
	var doc struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal([]byte(body), &doc) != nil {
		return ""
	}
	for i := len(doc.Messages) - 1; i >= 0; i-- {
		if doc.Messages[i].Role != "user" {
			continue
		}
		var text string
		if json.Unmarshal(doc.Messages[i].Content, &text) == nil {
			return text
		}
		var parts []struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(doc.Messages[i].Content, &parts) == nil {
			for _, part := range parts {
				text += part.Text
			}
			return text
		}
	}
	return ""
}

// heldSessionReadStore holds one Session-register read inside its
// transaction after arming; other reads pass through.
type heldSessionReadStore struct {
	harness.Storage
	target  string
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu    sync.Mutex
	armed bool
}

func (s *heldSessionReadStore) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

func (s *heldSessionReadStore) ReadRegisters(ctx context.Context, sessionID string) ([]harness.Register, error) {
	s.mu.Lock()
	held := s.armed && sessionID == s.target
	s.mu.Unlock()
	if held {
		s.once.Do(func() {
			close(s.entered)
			<-s.release
		})
	}
	return s.Storage.ReadRegisters(ctx, sessionID)
}

// TestCommandSessionReadDoesNotHoldNativeMu proves a Command blocked in its
// pre-guard Session header read does not block a concurrent registration on the
// same activation.
func TestCommandSessionReadDoesNotHoldNativeMu(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "ws")
	store := storage.NewMemory()

	// Create the target Session outside the owner's Harness so the first owner
	// read must reach storage.
	tempCtx, cancelTemp := context.WithCancel(context.Background())
	temp, err := harness.New(tempCtx, harness.Dependencies{Storage: store, Prepare: func(context.Context, harness.PreparationRequest) (harness.PreparedExecution, error) {
		return harness.PreparedExecution{}, errors.New("no preparation")
	}})
	testassert.NoError(t, err)
	created, err := temp.CreateSession(context.Background(), harness.CreateSessionRequest{Workspace: workspace, Inputs: harness.SessionInputs{}})
	testassert.NoError(t, err)
	cancelTemp()
	testassert.NoError(t, temp.Wait(context.Background()))
	target := created.Identity.SessionID

	held := &heldSessionReadStore{Storage: store, target: target, entered: make(chan struct{}), release: make(chan struct{})}
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "read-hold-fixture")
	e.fixtureStore(held)
	apiCh := make(chan extension.API, 1)
	manifest := registrarManifest("read-hold-fixture")
	manifest.Services = []string{extension.ContractCommands}
	capture := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		apiCh <- api
		return nil, nil
	}}
	opts := e.options(capture)
	opts.Access = map[string]ExtensionAccess{manifest.ID: {Scope: &CallerScope{Kind: CallerScopeWorkspace, Workspace: workspace}}}
	r, err := open(context.Background(), opts)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	api := <-apiCh

	held.arm()
	commandDone := make(chan error, 1)
	go func() {
		_, err := api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: target}, "interrupt", json.RawMessage(`{}`))
		commandDone <- err
	}()
	select {
	case <-held.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the Command never reached its Session header read")
	}

	registrationDone := make(chan error, 1)
	go func() {
		_, err := api.RegisterTool(testDescribeTool("held-tool"), testRegisteredTool{}, nil)
		registrationDone <- err
	}()
	select {
	case err := <-registrationDone:
		testassert.NoError(t, err)
	case <-time.After(2 * time.Second):
		close(held.release)
		t.Fatal("a concurrent registration blocked while Command's Session read was held")
	}
	close(held.release)
	select {
	case err := <-commandDone:
		testassert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the Command never finished after the Session read released")
	}
}

// TestAuthorToolCallbackCommand proves a command issued with a live tool
// callback's context is denied and performs no admission; the same issued from
// a timer after the callback returned succeeds.
func TestAuthorToolCallbackCommand(t *testing.T) {
	for _, mode := range []struct {
		name   string
		worker bool
	}{{name: "native"}, {name: "worker", worker: true}} {
		t.Run(mode.name, func(t *testing.T) {
			reportPath := filepath.Join(t.TempDir(), "reports.jsonl")
			r, log, e := independentOwnerAt(t, mode.worker, nil, reportPath, "", "")
			e.server.setScript(func(_ context.Context, body string) []string {
				if lastMessageRole(body) == "tool" {
					return textTurnEvents("done")
				}
				var arguments string
				switch independentLastUserText(body) {
				case "prepare":
					arguments = `{"phase":"prepare","operation_id":"op-tool-prepare","parent":"op-parent-prepare"}`
				case "prepare_after":
					arguments = `{"phase":"prepare_after","operation_id":"op-tool-prepare-after","parent":"op-parent-prepare-after"}`
				case "execute":
					arguments = `{"phase":"execute","operation_id":"op-tool-execute","parent":"op-parent-execute"}`
				case "execute_after":
					arguments = `{"phase":"execute_after","operation_id":"op-tool-execute-after","parent":"op-parent-execute-after"}`
				default:
					return textTurnEvents("done")
				}
				return toolCallTurnEvents("call-1", "command-tool", arguments)
			})
			caller := testCaller(r)
			session := projectionSession(t, r, filepath.Join(t.TempDir(), "ws"), "")
			sessionID := session.Identity.SessionID
			submit := func(operationID, text string) {
				t.Helper()
				_, err := r.SubmitSession(context.Background(), caller, sessionID, protocol.SubmitRequest{
					OperationId: operationID, Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, text, nil)},
				})
				testassert.NoError(t, err)
				awaitOperation(t, r, sessionID, operationID, harness.OperationSuccess)
			}

			submit("op-parent-prepare", "prepare")
			report := log.awaitKind(t, "tool-prepare")
			testassert.Require(t, report["class"] == "denied", "Prepare command = %+v, want denied", report)
			requireNoOperation(t, r, sessionID, "op-tool-prepare")

			submit("op-parent-prepare-after", "prepare_after")
			report = log.awaitKind(t, "tool-prepare-after")
			testassert.Require(t, report["class"] == "" && (strings.Contains(report["value"], `"operation_id":"op-tool-prepare-after"`) || strings.Contains(report["value"], `"disposition":"steering"`)), "post-Prepare timer command = %+v, want a successful receipt", report)
			awaitOperation(t, r, sessionID, "op-tool-prepare-after", harness.OperationSuccess)

			submit("op-parent-execute", "execute")
			report = log.awaitKind(t, "tool-execute")
			testassert.Require(t, report["class"] == "denied", "Execute command = %+v, want denied", report)
			requireNoOperation(t, r, sessionID, "op-tool-execute")

			submit("op-parent-execute-after", "execute_after")
			report = log.awaitKind(t, "tool-execute-after")
			testassert.Require(t, report["class"] == "" && (strings.Contains(report["value"], `"operation_id":"op-tool-execute-after"`) || strings.Contains(report["value"], `"disposition":"steering"`)), "post-Execute timer command = %+v, want a successful receipt", report)
			awaitOperation(t, r, sessionID, "op-tool-execute-after", harness.OperationSuccess)
		})
	}
}

// TestAuthorIndependentCommandScope proves a Workspace grant bounds the
// command target and observer delivery: a cross-Workspace target is denied and
// unrelated traffic never reaches the scoped observer.
func TestAuthorIndependentCommandScope(t *testing.T) {
	for _, mode := range []struct {
		name   string
		worker bool
	}{{name: "native"}, {name: "worker", worker: true}} {
		t.Run(mode.name, func(t *testing.T) {
			home := t.TempDir()
			workspaceA := filepath.Join(home, "workspace-a")
			workspaceB := filepath.Join(home, "workspace-b")
			access := map[string]ExtensionAccess{
				authorFixtureManifest().ID: {Scope: &CallerScope{Kind: CallerScopeWorkspace, Workspace: workspaceA}},
			}
			r, log := independentOwner(t, mode.worker, access)
			caller := testCaller(r)
			sessionA, err := r.CreateSession(context.Background(), caller, workspaceA, nil)
			testassert.NoError(t, err)
			sessionB, err := r.CreateSession(context.Background(), caller, workspaceB, nil)
			testassert.NoError(t, err)

			// One unrelated Session is filtered before any enqueue: the
			// observer never sees B's admitted or settled facts and stays
			// attached for A. The queue-capacity flood belongs to the owner
			// rule test in observation_test.go.
			_, err = r.SubmitSession(context.Background(), caller, sessionB.SessionId, protocol.SubmitRequest{
				OperationId: "b-op", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "noise", nil)},
			})
			testassert.NoError(t, err)
			awaitOperation(t, r, sessionB.SessionId, "b-op", harness.OperationSuccess)
			_, err = r.SubmitSession(context.Background(), caller, sessionA.SessionId, protocol.SubmitRequest{
				OperationId: "a-op", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "kept", nil)},
			})
			testassert.NoError(t, err)
			awaitOperation(t, r, sessionA.SessionId, "a-op", harness.OperationSuccess)
			awaitSettledReport(t, log, "a-op")

			for _, report := range log.snapshot() {
				if report["kind"] == "fact" {
					testassert.Require(t, report["session"] == sessionA.SessionId, "scoped observer received unrelated Session fact: %+v", report)
				}
			}
			admitted, settled := 0, 0
			for _, report := range log.snapshot() {
				if report["kind"] == "fact" && report["operation"] == "a-op" {
					if report["status"] == "" {
						admitted++
					} else {
						settled++
					}
				}
			}
			testassert.Require(t, admitted == 1 && settled == 1, "scoped observer A facts = admitted %d settled %d", admitted, settled)

			// A target in another Workspace is outside the granted scope.
			submit := commandSubmitRequest(t, "op-cross", "cross")
			rawSubmit, err := json.Marshal(submit)
			testassert.NoError(t, err)
			crossArguments, err := json.Marshal(map[string]json.RawMessage{
				"session_id": json.RawMessage(`"` + sessionB.SessionId + `"`),
				"method":     json.RawMessage(`"submit"`),
				"arguments":  rawSubmit,
			})
			testassert.NoError(t, err)
			invokeFixtureOperation(t, r, sessionA.SessionId, "cross", string(crossArguments), "req-cross")
			cross := log.awaitKind(t, "cross")
			testassert.Require(t, cross["class"] == "denied", "cross-Workspace command = %+v, want denied", cross)
			requireNoOperation(t, r, sessionB.SessionId, "op-cross")
		})
	}
}

// TestAuthorIndependentCommandUnapproved proves a grant that withholds
// melta.commands denies the independent command while observation continues.
func TestAuthorIndependentCommandUnapproved(t *testing.T) {
	for _, mode := range []struct {
		name   string
		worker bool
	}{{name: "native"}, {name: "worker", worker: true}} {
		t.Run(mode.name, func(t *testing.T) {
			access := map[string]ExtensionAccess{authorFixtureManifest().ID: {Services: []string{}}}
			r, log := independentOwner(t, mode.worker, access)
			caller := testCaller(r)
			session, err := r.CreateSession(context.Background(), caller, filepath.Join(t.TempDir(), "ws"), nil)
			testassert.NoError(t, err)
			submit := commandSubmitRequest(t, "op-denied", "denied")
			rawSubmit, err := json.Marshal(submit)
			testassert.NoError(t, err)
			arguments, err := json.Marshal(map[string]json.RawMessage{"independent": json.RawMessage(`true`), "arguments": rawSubmit})
			testassert.NoError(t, err)
			invokeFixtureOperation(t, r, session.SessionId, "in_callback", string(arguments), "req-denied")
			denied := log.awaitKind(t, "in-callback")
			testassert.Require(t, denied["class"] == "denied", "unapproved command = %+v, want denied", denied)
			requireNoOperation(t, r, session.SessionId, "op-denied")

			// The ordinary user admission still publishes its fact to the
			// observer: the refused command grants nothing and detaches nothing.
			_, err = r.SubmitSession(context.Background(), caller, session.SessionId, protocol.SubmitRequest{
				OperationId: "op-observe", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "observed", nil)},
			})
			testassert.NoError(t, err)
			awaitOperation(t, r, session.SessionId, "op-observe", harness.OperationSuccess)
			awaitSettledReport(t, log, "op-observe")
		})
	}
}

// TestExtensionAccessCloneAndValidation proves the embedder grants are
// validated once at Open, owned against caller mutation, and applied to the
// effective activation services and scope.
func TestExtensionAccessCloneAndValidation(t *testing.T) {
	if cloned, err := cloneExtensionAccess(nil); err != nil || cloned != nil {
		t.Fatalf("nil access = (%v, %v), want (nil, nil)", cloned, err)
	}
	services := []string{extension.ContractCommands}
	scope := CallerScope{Kind: CallerScopeWorkspace, Workspace: "/tmp/ws-one"}
	cloned, err := cloneExtensionAccess(map[string]ExtensionAccess{
		"ext": {Services: services, Scope: &scope},
	})
	testassert.NoError(t, err)
	services[0] = extension.ContractState
	scope.Workspace = "/tmp/ws-two"
	testassert.Require(t, cloned["ext"].Services[0] == extension.ContractCommands && cloned["ext"].Scope.Workspace == "/tmp/ws-one", "cloned access shares caller state: %+v", cloned["ext"])

	invalid := CallerScope{Kind: CallerScopeWorkspace, Workspace: "relative"}
	_, err = cloneExtensionAccess(map[string]ExtensionAccess{"ext": {Scope: &invalid}})
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "invalid scope = %v, want invalid", err)

	spec := nativeActivationSpec{extension: extension.Extension{Manifest: registrarManifest("ext")}}
	missing := applyExtensionAccess(spec, map[string]ExtensionAccess{"other": {}})
	testassert.Require(t, missing.approvedServices == nil && missing.scope == (CallerScope{}), "missing entry changed the spec: %+v", missing)
	restricted := applyExtensionAccess(spec, cloned)
	testassert.Require(t, slices.Equal(restricted.approvedServices, []string{extension.ContractCommands}) && restricted.scope == (CallerScope{Kind: CallerScopeWorkspace, Workspace: "/tmp/ws-one"}), "restricted entry = %+v", restricted)
}

// TestActivationCommandRefusedAfterRetirement proves an API handle retained by
// an extension cannot issue an independent command once its activation is
// retired, even while captured work keeps that activation open.
func TestActivationCommandRefusedAfterRetirement(t *testing.T) {
	owner := &configurationService{composition: newComposition()}
	owner.composition.hosts.owner = &Melta{}
	manifest := registrarManifest("independent")
	manifest.Services = []string{extension.ContractCommands}
	var api extension.API
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, got extension.API) (func(context.Context) error, error) {
		if api == nil {
			api = got
		}
		return nil, nil
	}}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{"v":1}`)}})
	testassert.NoError(t, err)
	defer first.release()
	testassert.NoError(t, first.publishForTest())

	// The published activation reaches the ordinary producer: its refusal is
	// the missing Session target, never the closed activation.
	_, err = api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "interrupt", json.RawMessage(`{}`))
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "published activation command = %v, want the ordinary producer refusal", err)

	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{"v":2}`)}})
	testassert.NoError(t, err)
	defer second.release()
	testassert.NoError(t, second.publishForTest())

	_, err = api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "interrupt", json.RawMessage(`{}`))
	testassert.Require(t, errors.Is(err, extension.ErrClosed), "retired activation command = %v, want closed", err)
}

// TestCallbackContextCommandDeniedBeforeActivationAndTarget proves any context
// Melta handed to a host callback is denied before publication and target
// checks: a retired activation and an unknown Session both answer denied.
func TestCallbackContextCommandDeniedBeforeActivationAndTarget(t *testing.T) {
	e := newOwnerEnv(t)
	const (
		idA     = "callback-context-a"
		idB     = "callback-context-b"
		unknown = "0123456789abcdef0123456789abcdef"
	)
	enableFixturePlugin(t, e.configPath, idA)
	enableFixturePlugin(t, e.configPath, idB)
	manifestA := registrarManifest(idA)
	manifestA.Services = []string{extension.ContractCommands}
	manifestB := registrarManifest(idB)
	manifestB.Services = []string{extension.ContractCommands}
	var apiA extension.API
	a := extension.Extension{Manifest: manifestA, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		apiA = api
		return nil, nil
	}}
	b := extension.Extension{Manifest: manifestB, Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }}
	opts := e.options(a, b)
	sessionScope := CallerScope{Kind: CallerScopeSession, SessionID: unknown}
	opts.Access = map[string]ExtensionAccess{
		idA: {Services: []string{extension.ContractCommands}, Scope: &sessionScope},
		idB: {Services: []string{extension.ContractCommands}},
	}
	r, err := open(context.Background(), opts)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	testassert.Require(t, apiA != nil, "activation A captured no API")
	var activationB *nativeActivation
	for _, activation := range r.config.current().native.activations {
		if activation != nil && activation.id == idB {
			activationB = activation
		}
	}
	testassert.Require(t, activationB != nil, "activation B was not published")
	callCtx, cancel := context.WithCancel(context.Background())
	marked := callbackScope(&nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: callCtx, cancel: cancel}, activation: activationB})
	defer marked.end()

	_, err = apiA.Command(marked.ctx, extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: unknown}, "interrupt", json.RawMessage(`{}`))
	testassert.Require(t, errors.Is(err, ErrDenied), "unknown-Session callback command = %v, want denied before the target", err)

	setExtensionSection(t, e.configPath, idA, `{"enabled":true,"v":2}`)
	_, err = r.Reload(context.Background(), testCaller(r))
	testassert.NoError(t, err)
	_, err = apiA.Command(marked.ctx, extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "interrupt", json.RawMessage(`{}`))
	testassert.Require(t, errors.Is(err, ErrDenied), "retired-activation callback command = %v, want denied before publication", err)
}

// TestNilContextCommandParity proves a nil command context is independent by
// the native API and the worker request path alike.
func TestNilContextCommandParity(t *testing.T) {
	classes := make(map[string]string, 2)
	for _, mode := range []struct {
		name   string
		worker bool
	}{{name: "native"}, {name: "worker", worker: true}} {
		t.Run(mode.name, func(t *testing.T) {
			workspace := filepath.Join(t.TempDir(), "ws")
			access := map[string]ExtensionAccess{
				authorFixtureManifest().ID: {Scope: &CallerScope{Kind: CallerScopeWorkspace, Workspace: workspace}},
			}
			r, log := independentOwner(t, mode.worker, access)
			session := projectionSession(t, r, workspace, "")
			invokeFixtureOperation(t, r, session.Identity.SessionID, "nil_command", `{}`, "req-nil-command")
			report := log.awaitKind(t, "nil-command")
			testassert.Require(t, report["class"] == "", "nil-context command = %+v, want an independent success", report)
			awaitOperation(t, r, session.Identity.SessionID, "op-nil-context", harness.OperationSuccess)
			classes[mode.name] = report["class"]
		})
	}
	testassert.Equal(t, classes["native"], classes["worker"])
}

// TestWorkerObserverRemoveCancelsInFlightObserve proves Remove of a worker
// observer whose observe call is in flight cancels that call: the host request
// sends its cancel frame and the worker observer returns.
func TestWorkerObserverRemoveCancelsInFlightObserve(t *testing.T) {
	reportPath := filepath.Join(t.TempDir(), "reports.jsonl")
	r, log, _ := independentOwnerAt(t, true, nil, reportPath, "", "block")
	caller := testCaller(r)
	session := projectionSession(t, r, filepath.Join(t.TempDir(), "ws"), "")
	_, err := r.SubmitSession(context.Background(), caller, session.Identity.SessionID, protocol.SubmitRequest{
		OperationId: "op-observed", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "observed", nil)},
	})
	testassert.NoError(t, err)
	started := log.awaitKind(t, "observe-started")
	testassert.Require(t, started["operation"] == "op-observed", "observe-started = %+v", started)

	invokeFixtureOperation(t, r, session.Identity.SessionID, "remove_observer", `{}`, "req-remove-observer")
	removed := log.awaitKind(t, "observer-remove")
	testassert.Require(t, removed["class"] == "", "observer remove = %+v, want success", removed)
	canceled := log.awaitKind(t, "observe-canceled")
	testassert.Require(t, canceled["operation"] == "op-observed", "observe-canceled = %+v, want the in-flight fact", canceled)
	awaitOperation(t, r, session.Identity.SessionID, "op-observed", harness.OperationSuccess)
}

// TestExtensionObservationRetainedCapture proves a retired activation's
// captured work delivers only to the old activation's observer, never the
// successor, and that the closed activation ends that registration.
func TestExtensionObservationRetainedCapture(t *testing.T) {
	reportPath := filepath.Join(t.TempDir(), "reports.jsonl")
	r, log, e := independentOwnerAt(t, false, nil, reportPath, "old", "")

	hold := make(chan struct{})
	e.server.setHold(hold)
	caller := testCaller(r)
	session, err := r.CreateSession(context.Background(), caller, filepath.Join(t.TempDir(), "ws"), nil)
	testassert.NoError(t, err)
	_, err = r.SubmitSession(context.Background(), caller, session.SessionId, protocol.SubmitRequest{
		OperationId: "op-retained", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "parked", nil)},
	})
	testassert.NoError(t, err)
	select {
	case <-e.server.arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("parked model request never arrived")
	}

	// Retire the activation while the Operation is still captured.
	previous := r.config.current().native
	var retired *nativeActivation
	for _, activation := range previous.activations {
		if activation != nil && activation.id == authorFixtureManifest().ID {
			retired = activation
		}
	}
	testassert.Require(t, retired != nil, "the retained activation is not current")
	setExtensionSection(t, e.configPath, authorFixtureManifest().ID, fmt.Sprintf(`{"enabled":true,"report":%q,"version":"new"}`, reportPath))
	_, err = r.Reload(context.Background(), testCaller(r))
	testassert.NoError(t, err)
	close(hold)
	awaitOperation(t, r, session.SessionId, "op-retained", harness.OperationSuccess)

	// The captured activation's observer receives the retained work's admitted
	// fact; nothing about the retained Operation reaches the successor.
	deadline := time.Now().Add(10 * time.Second)
	for log.countKind("fact", "op-retained") == 0 {
		testassert.Require(t, !time.Now().After(deadline), "the retained activation's observer received no fact: %+v", log.snapshot())
		time.Sleep(5 * time.Millisecond)
	}
	for _, report := range log.snapshot() {
		if report["operation"] != "op-retained" {
			continue
		}
		testassert.Require(t, report["version"] == "old", "retained fact reached the successor: %+v", report)
	}

	// The captured activation closes after its final release: its observer
	// registration is ended through the one end path, never left listed live.
	deadline = time.Now().Add(10 * time.Second)
	for {
		r.config.nativeMu.Lock()
		remaining := 0
		for _, entry := range retired.entries {
			if entry.kind == "observer" {
				remaining++
			}
		}
		r.config.nativeMu.Unlock()
		if remaining == 0 {
			break
		}
		testassert.Require(t, !time.Now().After(deadline), "the closed activation still lists %d observer registrations", remaining)
		time.Sleep(5 * time.Millisecond)
	}

	// New work after the retirement publishes to the successor only.
	_, err = r.SubmitSession(context.Background(), caller, session.SessionId, protocol.SubmitRequest{
		OperationId: "op-current", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textContentPart(t, "after", nil)},
	})
	testassert.NoError(t, err)
	awaitOperation(t, r, session.SessionId, "op-current", harness.OperationSuccess)
	awaitSettledReport(t, log, "op-current")
	for _, report := range log.snapshot() {
		if report["operation"] == "op-current" {
			testassert.Require(t, report["version"] == "new", "new work reached the retired activation: %+v", report)
		}
	}
}
