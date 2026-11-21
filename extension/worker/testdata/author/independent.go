package main

// Independent-command fixture mode: one observer, one timer submit after the
// callback returns, one command attempted inside its own callback, and one
// saved callback reuse. Every semantic outcome is appended as one JSON line to
// the report file named by the activation settings, so the transport-boundary
// tests can compare what the extension actually received.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

var independentReportMu sync.Mutex

func independentClass(err error) string {
	if err == nil {
		return ""
	}
	var protocolError *worker.Error
	if errors.As(err, &protocolError) {
		return string(protocolError.Class)
	}
	switch {
	case errors.Is(err, extension.ErrClosed):
		return "closed"
	case errors.Is(err, harness.ErrConflict):
		return "conflict"
	case errors.Is(err, harness.ErrInvalid):
		return "invalid"
	case strings.Contains(err.Error(), "denied"):
		return "denied"
	}
	return "internal"
}

// independentReportFile appends one JSON report line to the settings-named
// file under one process-wide lock, so concurrent observers and timers never
// interleave a line.
func independentSettingsOf(settings json.RawMessage) struct {
	Report  string `json:"report"`
	Version string `json:"version"`
	Observe string `json:"observe"`
} {
	var parsed struct {
		Report  string `json:"report"`
		Version string `json:"version"`
		Observe string `json:"observe"`
	}
	_ = json.Unmarshal(settings, &parsed)
	return parsed
}

func independentReportFile(settings json.RawMessage, kind, class, value, session, operation, status string) {
	reportSettings := independentSettingsOf(settings)
	if reportSettings.Report == "" {
		return
	}
	line, err := json.Marshal(map[string]string{
		"kind": kind, "class": class, "value": value, "session": session,
		"operation": operation, "status": status, "version": reportSettings.Version,
	})
	if err != nil {
		return
	}
	independentReportMu.Lock()
	defer independentReportMu.Unlock()
	file, err := os.OpenFile(reportSettings.Report, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) // #nosec G304 -- The test harness supplies its private temporary report path through settings.
	if err != nil {
		return
	}
	_, _ = file.Write(append(line, '\n'))
	_ = file.Close()
}

func independentReportOutcome(api extension.API, kind string, result extension.Result, err error) {
	independentReportFile(api.Settings(), kind, independentClass(err), string(result.Value), "", "", "")
}

func independentSessionTarget(sessionID string) extension.CommandTarget {
	return extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: sessionID}
}

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

func independentCommandAfterSettled(api extension.API, target extension.CommandTarget, operationID string) (extension.Result, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		result, err := api.Command(context.Background(), target, "submit", independentSubmitRequest(operationID))
		if err == nil || independentClass(err) != "conflict" || time.Now().After(deadline) {
			return result, err
		}
		time.Sleep(time.Millisecond)
	}
}

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
		result, err := t.api.Command(ctx, independentSessionTarget(call.Session.SessionID), "submit", independentSubmitRequest(operationID))
		independentReportOutcome(t.api, "tool-prepare", result, err)
		return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: input.ID, Status: model.ResultSuccess, Content: "prepared"}}}
	case "prepare_after":
		settings := t.api.Settings()
		sessionID := call.Session.SessionID
		go func() {
			if !waitSettled(t.settled, args.Parent) {
				return
			}
			result, err := independentCommandAfterSettled(t.api, independentSessionTarget(sessionID), operationID)
			independentReportFile(settings, "tool-prepare-after", independentClass(err), string(result.Value), "", operationID, "")
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
		result, err := t.api.Command(ctx, independentSessionTarget(call.Session.SessionID), "submit", independentSubmitRequest(operationID))
		independentReportOutcome(t.api, "tool-execute", result, err)
	case "execute_after":
		settings := t.api.Settings()
		sessionID := call.Session.SessionID
		go func() {
			if !waitSettled(t.settled, args.Parent) {
				return
			}
			result, err := independentCommandAfterSettled(t.api, independentSessionTarget(sessionID), operationID)
			independentReportFile(settings, "tool-execute-after", independentClass(err), string(result.Value), "", operationID, "")
		}()
	}
	return independentToolOutcome(payload.ID, "executed")
}

func independentOperation(name string, handler extension.AuthorHandler) extension.Operation {
	return extension.Operation{
		Target:  extension.ExposedTargetSession,
		Method:  extension.Method{Name: name, Mode: extension.ModeServiceIO, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{}`)},
		Handler: handler,
	}
}

func independentFixture() extension.Extension {
	manifest := extension.Manifest{
		ID: "author-fixture", Version: "1", APIVersion: extension.APIVersion,
		SettingsSchema: json.RawMessage(`{"type":"object"}`), Services: []string{extension.ContractCommands},
	}
	return extension.Extension{Manifest: manifest, Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		_, activateErr := api.Command(ctx, extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "interrupt", json.RawMessage(`{}`))
		independentReportOutcome(api, "activate", extension.Result{}, activateErr)

		settled := make(chan string)
		issuedCommand := false
		observerHandle, err := api.AttachObserver(func(ctx context.Context, fact extension.Fact) error {
			settings := api.Settings()
			if independentSettingsOf(settings).Observe == "block" {
				independentReportFile(settings, "observe-started", "", "", fact.SessionID, fact.OperationID, fact.Status)
				<-ctx.Done()
				independentReportFile(settings, "observe-canceled", "", "", fact.SessionID, fact.OperationID, fact.Status)
				return ctx.Err()
			}
			independentReportFile(settings, "fact", "", "", fact.SessionID, fact.OperationID, fact.Status)
			if independentSettingsOf(settings).Observe == "command" && fact.Kind == extension.FactOperationAdmitted && !issuedCommand {
				issuedCommand = true
				arguments, _ := json.Marshal(map[string]string{"workspace": filepath.Dir(independentSettingsOf(settings).Report)})
				result, err := api.Command(ctx, extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "create_session", arguments)
				independentReportFile(settings, "observer-command", independentClass(err), string(result.Value), fact.SessionID, fact.OperationID, fact.Status)
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
		if _, err := api.RegisterOperation(independentOperation("remove_observer", func(_ context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			independentReportOutcome(api, "observer-remove", extension.Result{}, api.Remove(observerHandle))
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterTool(model.ToolDefinition{Name: "command-tool", Description: "command-tool", Parameters: json.RawMessage(`{"type":"object"}`)}, independentCommandTool{api: api, settled: settled}, nil); err != nil {
			return nil, err
		}

		if _, err := api.RegisterOperation(independentOperation("nil_command", func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			sessionID := call.Session.SessionID
			settings := api.Settings()
			go func() { // #nosec G118 -- the nil-context command is independent of the callback.
				result, err := api.Command(nil, independentSessionTarget(sessionID), "submit", independentSubmitRequest("op-nil-context"))
				independentReportFile(settings, "nil-command", independentClass(err), string(result.Value), sessionID, "", "")
			}()
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachRequest(extension.HookModePure, func(_ context.Context, call extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			settings := independentSettingsOf(api.Settings())
			if settings.Observe != "request-block" {
				return harness.RequestConsequence{}, nil
			}
			independentReportFile(api.Settings(), "request-entered", "", "", call.Session.SessionID, "", "")
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
		if _, err := api.RegisterOperation(independentOperation("timer", func(_ context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			sessionID := call.Session.SessionID
			payload := append(json.RawMessage(nil), args...)
			settings := api.Settings()
			go func() { // #nosec G118 -- the timer must outlive its callback: it issues an independent command after the callback returned.
				time.Sleep(20 * time.Millisecond)
				result, err := api.Command(context.Background(), independentSessionTarget(sessionID), "submit", payload)
				independentReportFile(settings, "timer", independentClass(err), string(result.Value), sessionID, "", "")
			}()
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(independentOperation("in_callback", func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			var request struct {
				Independent bool            `json:"independent"`
				Arguments   json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(args, &request); err != nil {
				return extension.Result{}, err
			}
			sessionID := call.Session.SessionID
			settings := api.Settings()
			if request.Independent {
				go func() { // #nosec G118 -- the independent command must outlive its callback.
					result, err := api.Command(context.Background(), independentSessionTarget(sessionID), "submit", request.Arguments)
					independentReportFile(settings, "in-callback", independentClass(err), string(result.Value), sessionID, "", "")
				}()
				return extension.Result{Value: json.RawMessage(`{}`)}, nil
			}
			result, err := api.Command(ctx, independentSessionTarget(sessionID), "submit", request.Arguments)
			independentReportOutcome(api, "in-callback", result, err)
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(independentOperation("save", func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			saved = call.Call
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(independentOperation("replay", func(ctx context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			var replayArgs struct {
				Method    string          `json:"method"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(args, &replayArgs); err != nil {
				return extension.Result{}, err
			}
			if saved == nil {
				independentReportFile(api.Settings(), "replay", "internal", "no saved callback", "", "", "")
				return extension.Result{Value: json.RawMessage(`{}`)}, nil
			}
			result, err := saved(ctx, extension.ContractCommands, replayArgs.Method, replayArgs.Arguments)
			independentReportOutcome(api, "replay", result, err)
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(independentOperation("cross", func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			var request struct {
				SessionID string          `json:"session_id"`
				Method    string          `json:"method"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(args, &request); err != nil {
				return extension.Result{}, err
			}
			settings := api.Settings()
			go func() { // #nosec G118 -- the independent command must outlive its callback.
				result, err := api.Command(context.Background(), independentSessionTarget(request.SessionID), request.Method, request.Arguments)
				independentReportFile(settings, "cross", independentClass(err), string(result.Value), request.SessionID, "", "")
			}()
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}), nil); err != nil {
			return nil, err
		}
		return nil, nil
	}}
}
