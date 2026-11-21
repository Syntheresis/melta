package melta

// Public extension-operation suite: the registered catalog, the single
// InvokeExtension address path, the fixed melta.commands bridge, and the
// declared-failure/partial round trip are exercised through the real owner
// over the registered activation. Transport-boundary native/worker parity
// belongs to executable_parity_test.go; these client-path cases run once, at
// their owner.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/protocol"
	"golang.org/x/sys/unix"
)

const (
	fixtureExtensionID = "fixture"

	// validCallErrorPartial is one multiline, large-number, escaped partial
	// the call-error forwarding must preserve byte-exactly.
	validCallErrorPartial = "{\n  \"big\": 9007199254740993,\n  \"text\": \"line\\\\nbreak\"\n}"
)

func fixtureHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func fixtureCaller(t *testing.T, r *Melta, scope CallerScope) Caller {
	t.Helper()
	caller, err := r.Caller(CallerContext{Origin: harness.InputOriginUser, Scope: scope})
	testassert.NoError(t, err)
	return caller
}

func fixtureRequest(extensionID, operation string, target protocol.ExtensionInvocationTarget, arguments, requestID string) protocol.ExtensionInvocationRequest {
	rawArguments := json.RawMessage(arguments)
	return protocol.ExtensionInvocationRequest{
		ExtensionId: extensionID,
		Operation:   operation,
		Target:      target,
		Arguments:   &rawArguments,
		RequestId:   requestID,
	}
}

func workspaceTarget(workspace string) protocol.ExtensionInvocationTarget {
	return protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace}
}

func sessionTarget(sessionID string) protocol.ExtensionInvocationTarget {
	return protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindSession, SessionId: &sessionID}
}

func harnessTarget() protocol.ExtensionInvocationTarget {
	return protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}
}

func countEvent(events []string, prefix string) int {
	count := 0
	for _, event := range events {
		if strings.HasPrefix(event, prefix) {
			count++
		}
	}
	return count
}

// commandEnvelope is the fixture operation's fixed bridge envelope: the
// command method name plus that method's ordinary generated request value.
type commandEnvelope struct {
	Method    string          `json:"method"`
	Arguments json.RawMessage `json:"arguments"`
}

func decodeEnvelope(arguments json.RawMessage) (commandEnvelope, error) {
	var envelope commandEnvelope
	if err := json.Unmarshal(arguments, &envelope); err != nil || envelope.Method == "" {
		return commandEnvelope{}, fmt.Errorf("command envelope: %w", harness.ErrInvalid)
	}
	return envelope, nil
}

// operationFixtureExtension registers the suite's fixture: one advertised
// tool, two same-contract services (only their operations are client-visible,
// a service method like "retain" is never one), and the harness-, Workspace-,
// and Session-target operations the rows address. Every handler records one
// attribution line carrying the host-issued Session and request identity.
func operationFixtureExtension(events *traceLog) extension.Extension {
	return operationFixtureExtensionDeferred(events, nil)
}

// operationFixtureExtensionDeferred is the fixture with an optional sink for
// an independent command issued from a timer after its callback returned.
func operationFixtureExtensionDeferred(events *traceLog, deferred chan<- extension.Result) extension.Extension {
	manifest := registrarManifest(fixtureExtensionID)
	manifest.Services = []string{extension.ContractCommands}
	commandMethod := func(name string) extension.Method {
		return extension.Method{Name: name, Mode: extension.ModeServiceIO, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{}`), Error: json.RawMessage(`{}`), Partial: json.RawMessage(`{}`)}
	}
	runCommand := func(mapped bool) extension.AuthorHandler {
		return func(ctx context.Context, call extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
			envelope, err := decodeEnvelope(arguments)
			if err != nil {
				return extension.Result{}, err
			}
			events.add("command:" + envelope.Method + ";session=\"" + call.Session.SessionID + "\";request=\"" + call.RequestID + "\"")
			result, err := call.Call(ctx, extension.ContractCommands, envelope.Method, envelope.Arguments)
			var callErr *extension.CallError
			if mapped && errors.As(err, &callErr) && len(callErr.Partial) != 0 {
				code := string(ErrorCode(err))
				events.add("command:revert-call-error;class=" + code + ";partial=" + fixtureHash(string(callErr.Partial)))
				failure := &extension.Failure{Code: code, Message: err.Error(), Partial: append(json.RawMessage(nil), callErr.Partial...)}
				return extension.Result{Failure: failure}, nil
			}
			return result, err
		}
	}
	return extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterTool(testDescribeTool("fixture.tool"), testRegisteredTool{}, nil); err != nil {
			return nil, err
		}
		echoService := func(name string) extension.Service {
			return extension.Service{Name: name, Methods: []extension.RegisteredMethod{{
				Method: extension.Method{Name: "echo", Mode: extension.ModePure, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{"type":"object"}`)},
				Handler: func(_ context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					events.add("service:" + name + ":request=" + call.RequestID)
					return extension.Result{Value: append(json.RawMessage(nil), args...)}, nil
				},
			}}}
		}
		if _, err := api.RegisterService(extension.Service{Name: "fixture.service", Methods: append(echoService("fixture.service").Methods, extension.RegisteredMethod{
			Method: extension.Method{Name: "retain", Mode: extension.ModePure, Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: json.RawMessage(`{}`)}, nil
			},
		})}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterService(echoService("fixture.service.alias"), nil); err != nil {
			return nil, err
		}
		exposedEcho := func(name, service string) extension.Operation {
			return extension.Operation{
				Target: extension.ExposedTargetHarness,
				Method: extension.Method{Name: name, Mode: extension.ModePure, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{"type":"object"}`)},
				Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return call.Call(ctx, service, "echo", args)
				},
			}
		}
		if _, err := api.RegisterOperation(exposedEcho("echo", "fixture.service"), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(exposedEcho("alias_echo", "fixture.service.alias"), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetWorkspace,
			Method: extension.Method{Name: "probe", Mode: extension.ModePure, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{"type":"object"}`)},
			Handler: func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				value, _ := json.Marshal(map[string]string{"workspace": call.Session.Workspace})
				return extension.Result{Value: value}, nil
			},
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{Target: extension.ExposedTargetSession, Method: commandMethod("command"), Handler: runCommand(true)}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{Target: extension.ExposedTargetSession, Method: commandMethod("command_unmapped"), Handler: runCommand(false)}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{Target: extension.ExposedTargetHarness, Method: commandMethod("command_harness"), Handler: runCommand(true)}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{Target: extension.ExposedTargetSession, Method: commandMethod("independent_command"), Handler: func(ctx context.Context, call extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
			envelope, err := decodeEnvelope(arguments)
			if err != nil {
				return extension.Result{}, err
			}
			sessionID := call.Session.SessionID
			payload := append(json.RawMessage(nil), envelope.Arguments...)
			go func() {
				time.Sleep(20 * time.Millisecond)
				result, err := api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: sessionID}, envelope.Method, payload)
				var callErr *extension.CallError
				if errors.As(err, &callErr) && len(callErr.Partial) != 0 {
					events.add("api-command:revert-call-error;class=" + string(ErrorCode(err)) + ";partial=" + fixtureHash(string(callErr.Partial)))
					result = extension.Result{Failure: &extension.Failure{Code: string(ErrorCode(err)), Message: err.Error(), Partial: append(json.RawMessage(nil), callErr.Partial...)}}
					err = nil
				}
				if err != nil {
					result = extension.Result{Failure: &extension.Failure{Code: string(ErrorCode(err)), Message: err.Error()}}
				}
				if deferred != nil {
					deferred <- result
				}
			}()
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "pure_command", Mode: extension.ModePure, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
				envelope, err := decodeEnvelope(arguments)
				if err != nil {
					return extension.Result{}, err
				}
				events.add("pure:" + envelope.Method + ":attempt")
				return call.Call(ctx, extension.ContractCommands, envelope.Method, envelope.Arguments)
			},
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "query_hold", Mode: extension.ModePure, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{"type":"object"}`)},
			Handler: func(_ context.Context, _ extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
				var hold struct{ Hold, Release, Block string }
				if err := json.Unmarshal(arguments, &hold); err != nil {
					return extension.Result{}, err
				}
				if err := os.WriteFile(hold.Hold, []byte("hold"), 0o600); err != nil {
					return extension.Result{}, err
				}
				await := func(path string) bool {
					deadline := time.Now().Add(30 * time.Second)
					for time.Now().Before(deadline) {
						if _, err := os.Stat(path); err == nil {
							return true
						}
						time.Sleep(5 * time.Millisecond)
					}
					return false
				}
				if !await(hold.Release) {
					return extension.Result{}, errors.New("query_hold never released")
				}
				if hold.Block != "" {
					if err := os.WriteFile(hold.Block+".produced", []byte("produced"), 0o600); err != nil {
						return extension.Result{}, err
					}
					if !await(hold.Block) {
						return extension.Result{}, errors.New("query_hold never unblocked")
					}
				}
				return extension.Result{Value: json.RawMessage(`{"held":true}`)}, nil
			},
		}, nil); err != nil {
			return nil, err
		}
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: commandMethod("call_error"),
			Handler: func(_ context.Context, _ extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
				var mode struct{ Mode string }
				if err := json.Unmarshal(arguments, &mode); err != nil {
					return extension.Result{}, err
				}
				switch mode.Mode {
				case "valid":
					return extension.Result{}, &extension.CallError{Err: harness.ErrStorage, Partial: json.RawMessage(validCallErrorPartial)}
				case "null":
					return extension.Result{}, &extension.CallError{Err: harness.ErrStorage, Partial: json.RawMessage("null")}
				case "empty":
					return extension.Result{}, &extension.CallError{Err: harness.ErrStorage}
				case "wrapped":
					return extension.Result{}, &extension.CallError{Err: fmt.Errorf("wrapped: %w", harness.ErrStorage), Partial: json.RawMessage(validCallErrorPartial)}
				case "bad":
					return extension.Result{}, &extension.CallError{Err: harness.ErrStorage, Partial: json.RawMessage("{")}
				}
				return extension.Result{}, errors.New("unknown call_error mode")
			},
		}, nil)
		return nil, err
	}}
}

// openOperationFixture opens one real owner with the registered fixture
// extension enabled, over the memory store.
func openOperationFixture(t *testing.T) (*Melta, *ownerEnv, *traceLog) {
	t.Helper()
	return openOperationFixtureDeferred(t, nil)
}

// openOperationFixtureDeferred is openOperationFixture with the deferred
// independent-command result sink.
func openOperationFixtureDeferred(t *testing.T, deferred chan<- extension.Result) (*Melta, *ownerEnv, *traceLog) {
	t.Helper()
	e := newOwnerEnv(t)
	writeAgentDefinitions(t, e.configPath, projectionAgentsDocument)
	enableFixturePlugin(t, e.configPath, fixtureExtensionID)
	e.fixtureStore(storage.NewMemory())
	events := &traceLog{}
	r, err := e.open(context.Background(), operationFixtureExtensionDeferred(events, deferred))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r, e, events
}

// TestExtensionCatalogAndAddressing proves the public catalog and the single
// invocation path resolve the concrete registration and target before
// binding, keep two same-contract services separately addressable, carry the
// resolved target attribution, and refuse every out-of-scope or unexposed
// address without calling a handler.
func TestExtensionCatalogAndAddressing(t *testing.T) {
	r, e, events := openOperationFixture(t)
	ctx := context.Background()
	harnessCaller := fixtureCaller(t, r, harnessScope)
	workspaceA, workspaceB := filepath.Join(e.home, "catalog-a"), filepath.Join(e.home, "catalog-b")
	sessionA, err := r.CreateSession(ctx, harnessCaller, workspaceA, nil)
	testassert.NoError(t, err)
	sessionB, err := r.CreateSession(ctx, harnessCaller, workspaceB, nil)
	testassert.NoError(t, err)

	catalog, err := r.ListExtensions(ctx, harnessCaller)
	testassert.NoError(t, err)
	var entry *protocol.RegisteredExtension
	for i := range catalog {
		if catalog[i].ExtensionId == fixtureExtensionID {
			entry = &catalog[i]
		}
	}
	testassert.Require(t, entry != nil, "catalog omitted the enabled fixture extension: %+v", catalog)
	testassert.Require(t, entry.ConfigurationSchema != nil && *entry.ConfigurationSchema != "" && entry.Settings != "" && strings.Contains(entry.Settings, `"enabled":true`), "catalog schema/settings = %q/%q", *entry.ConfigurationSchema, entry.Settings)
	testassert.Require(t, slices.Contains(entry.Tools, "fixture.tool"), "catalog tools = %+v", entry.Tools)
	var sawEcho, sawAlias, sawWorkspaceProbe, sawHidden bool
	for _, exposed := range entry.Operations {
		sawEcho = sawEcho || exposed.Method.Name == "echo" && exposed.Target == protocol.RegisteredExtensionOperationTargetHarness
		sawAlias = sawAlias || exposed.Method.Name == "alias_echo" && exposed.Target == protocol.RegisteredExtensionOperationTargetHarness
		sawWorkspaceProbe = sawWorkspaceProbe || exposed.Method.Name == "probe" && exposed.Target == protocol.RegisteredExtensionOperationTargetWorkspace
		sawHidden = sawHidden || exposed.Method.Name == "retain"
	}
	testassert.Require(t, sawEcho && sawAlias && sawWorkspaceProbe && !sawHidden, "catalog operations = %+v", entry.Operations)

	// Two same-contract services stay separately addressable and each call
	// reaches its own concrete handler even though the method shape matches.
	body := "{\n  \"big\": 9007199254740993,\n  \"escaped\": \"line\\n\\t<>&\"\n}"
	mainResult, err := r.InvokeExtension(ctx, harnessCaller, fixtureRequest(fixtureExtensionID, "echo", harnessTarget(), body, "req-main"))
	testassert.NoError(t, err)
	testassert.Require(t, mainResult.Failure == nil && string(mainResult.Value) == body, "main echo result = %+v, want the exact raw bytes", mainResult)
	aliasResult, err := r.InvokeExtension(ctx, harnessCaller, fixtureRequest(fixtureExtensionID, "alias_echo", harnessTarget(), body, "req-alias"))
	testassert.NoError(t, err)
	testassert.Require(t, aliasResult.Failure == nil && string(aliasResult.Value) == body, "alias echo result = %+v, want the exact raw bytes", aliasResult)
	testassert.Require(t, countEvent(events.all(), "service:fixture.service:") == 1 && countEvent(events.all(), "service:fixture.service.alias:") == 1, "same-contract calls did not reach their own handlers: %v", events.all())

	// The Workspace-target call carries the resolved Workspace attribution.
	workspaceCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeWorkspace, Workspace: workspaceA})
	probe, err := r.InvokeExtension(ctx, workspaceCaller, fixtureRequest(fixtureExtensionID, "probe", workspaceTarget(workspaceA), `{}`, "req-probe"))
	testassert.NoError(t, err)
	testassert.Require(t, probe.Failure == nil, "workspace probe failure = %+v", probe.Failure)
	var probeValue map[string]string
	testassert.NoError(t, json.Unmarshal(probe.Value, &probeValue))
	testassert.Require(t, probeValue["workspace"] == workspaceA, "workspace attribution = %q, want %q", probeValue["workspace"], workspaceA)

	// A Workspace grant cannot reach another Workspace or its Sessions.
	_, err = r.InvokeExtension(ctx, workspaceCaller, fixtureRequest(fixtureExtensionID, "probe", workspaceTarget(workspaceB), `{}`, "req-cross-workspace"))
	testassert.Require(t, errors.Is(err, ErrDenied), "cross-Workspace call = %v, want denied", err)
	_, err = r.InvokeExtension(ctx, workspaceCaller, fixtureRequest(fixtureExtensionID, "command", sessionTarget(sessionB.SessionId), `{"method":"submit","arguments":{"operation_id":"x","mode":"regular","content":[{"kind":"text","text":"x"}]}}`, "req-cross-session"))
	testassert.Require(t, errors.Is(err, ErrDenied), "cross-Session call = %v, want denied", err)
	// A Session grant cannot select another Session for A's call.
	sessionCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeSession, SessionID: sessionA.SessionId})
	_, err = r.InvokeExtension(ctx, sessionCaller, fixtureRequest(fixtureExtensionID, "command", sessionTarget(sessionB.SessionId), `{"method":"submit","arguments":{"operation_id":"y","mode":"regular","content":[{"kind":"text","text":"y"}]}}`, "req-session-b"))
	testassert.Require(t, errors.Is(err, ErrDenied), "Session B selected for A's call = %v, want denied", err)
	// An unexposed service method, a forged target, and an unknown operation
	// never dispatch.
	_, err = r.InvokeExtension(ctx, harnessCaller, fixtureRequest(fixtureExtensionID, "retain", harnessTarget(), `{}`, "req-hidden"))
	testassert.Require(t, errors.Is(err, harness.ErrNotFound), "unexposed method = %v, want not found", err)
	_, err = r.InvokeExtension(ctx, harnessCaller, fixtureRequest(fixtureExtensionID, "echo", workspaceTarget(workspaceA), `{}`, "req-forged-target"))
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "forged target = %v, want invalid", err)
	_, err = r.InvokeExtension(ctx, harnessCaller, fixtureRequest(fixtureExtensionID, "unknown", harnessTarget(), `{}`, "req-unknown"))
	testassert.Require(t, errors.Is(err, harness.ErrNotFound), "unknown operation = %v, want not found", err)
	testassert.Require(t, countEvent(events.all(), "command:") == 0, "refused addresses executed a command handler: %v", events.all())
}

// TestExtensionSessionCommands proves the fixed melta.commands bridge uses
// ordinary admission and attribution for every command route, derives the
// Session only from the host-issued context, rejects malformed generated
// arguments before mutation, and gives a pure callback no command grant.
func TestExtensionSessionCommands(t *testing.T) {
	r, e, events := openOperationFixture(t)
	ctx := context.Background()
	harnessCaller := fixtureCaller(t, r, harnessScope)
	workspace := filepath.Join(e.home, "commands")
	session := projectionSession(t, r, workspace, "solo")
	other := projectionSession(t, r, workspace, "solo")
	var err error
	sessionCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeSession, SessionID: session.Identity.SessionID})

	command := func(method, arguments, requestID string) (extension.Result, error) {
		return r.InvokeExtension(ctx, sessionCaller, fixtureRequest(fixtureExtensionID, "command", sessionTarget(session.Identity.SessionID), `{"method":"`+method+`","arguments":`+arguments+`}`, requestID))
	}
	submitArgs, err := json.Marshal(commandSubmitRequest(t, "op-submit", "hello"))
	testassert.NoError(t, err)
	result, err := command("submit", string(submitArgs), "req-submit")
	testassert.NoError(t, err)
	testassert.Require(t, result.Failure == nil, "submit failure = %+v", result.Failure)
	awaitOperation(t, r, session.Identity.SessionID, "op-submit", harness.OperationSuccess)
	// Repeating the stable admission ID observes no second Operation.
	before := e.server.requests()
	_, _ = command("submit", string(submitArgs), "req-submit-repeat")
	testassert.Require(t, e.server.requests() == before, "repeat admission ID started a second model execution")
	testassert.Require(t, countEvent(events.all(), "command:submit") == 2, "fixed command bridge ran %d submit calls, want two invocations over one admission", countEvent(events.all(), "command:submit"))
	submitReached := 2

	// The bridge attribution carries the host-issued Session and request.
	var commandEvent string
	for _, line := range events.all() {
		if strings.HasPrefix(line, "command:submit") && commandEvent == "" {
			commandEvent = line
		}
	}
	want := `session="` + session.Identity.SessionID + `"`
	testassert.Require(t, strings.Contains(commandEvent, want) && strings.Contains(commandEvent, `request="req-submit"`), "command attribution = %q", commandEvent)

	// Session-addressed arguments can never claim another Session: an
	// unknown generated member refuses before any mutation.
	_, err = command("submit", `{"session_id":"`+other.Identity.SessionID+`","operation_id":"op-wrong","mode":"regular","content":[{"kind":"text","text":"x"}]}`, "req-wrong-session")
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "Session-claiming submit = %v, want invalid", err)
	testassert.Require(t, errors.Is(readOperationError(ctx, r, other.Identity.SessionID, "op-wrong"), harness.ErrNotFound), "refused Session-claiming arguments mutated the other Session")
	testassert.Require(t, errors.Is(readOperationError(ctx, r, session.Identity.SessionID, "op-wrong"), harness.ErrNotFound), "refused Session-claiming arguments mutated the bound Session")
	_, err = command("submit", `{"operation_id":"op-case","Operation_id":"op-case-other","mode":"regular","content":[{"kind":"text","text":"x"}]}`, "req-case")
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "case-variant generated request member = %v, want invalid", err)
	testassert.Require(t, errors.Is(readOperationError(ctx, r, session.Identity.SessionID, "op-case"), harness.ErrNotFound) && errors.Is(readOperationError(ctx, r, session.Identity.SessionID, "op-case-other"), harness.ErrNotFound), "case-variant generated request member mutated Session state")
	testassert.Require(t, countEvent(events.all(), "command:submit") == submitReached+2, "malformed generated values refused before reaching the bridge: %v", events.all())

	// Every fixed command route reaches its ordinary producer through the
	// same bridge.
	_, err = command("compact", `{"operation_id":"op-compact"}`, "req-compact")
	testassert.NoError(t, err)
	awaitOperation(t, r, session.Identity.SessionID, "op-compact", harness.OperationSuccess)
	_, err = command("fork", `{"operation_id":"op-fork","boundary_item_id":"`+firstItemID(t, r, session.Identity.SessionID)+`","content":[{"kind":"text","text":"fork"}]}`, "req-fork")
	testassert.NoError(t, err)
	_, err = command("set_inputs", `{"inputs":{"model":"prov/m","instructions":"","tools":[]}}`, "req-inputs")
	testassert.NoError(t, err)
	_, err = command("set_model", `{"model":"prov/m"}`, "req-model")
	testassert.NoError(t, err)
	reverted, err := command("revert_code", `{"boundary_operation_id":"op-submit"}`, "req-revert-success")
	testassert.NoError(t, err)
	testassert.Require(t, reverted.Failure == nil, "revert success failure = %+v", reverted.Failure)
	var revertResult protocol.CodeRevertResult
	testassert.NoError(t, json.Unmarshal(reverted.Value, &revertResult))
	testassert.Require(t, revertResult.Restored != nil && revertResult.Skipped != nil && revertResult.Error == nil, "revert success receipt = %+v", revertResult)
	preTraversal, err := command("revert_code", `{"boundary_operation_id":"missing"}`, "req-revert-invalid")
	testassert.Require(t, errors.Is(err, harness.ErrInvalid) && preTraversal.Failure == nil, "pre-traversal revert = (%+v, %v), want the original invalid error", preTraversal, err)
	_, err = command("interrupt", `{}`, "req-interrupt")
	testassert.NoError(t, err)
	_, err = command("stop", `{}`, "req-stop")
	testassert.NoError(t, err)
	archive, err := command("archive", `{}`, "req-archive")
	testassert.NoError(t, err)
	testassert.Require(t, archive.Failure == nil, "archive failure = %+v", archive.Failure)
	_, err = command("reopen", `{}`, "req-reopen")
	testassert.NoError(t, err)
	_, err = command("archive", `{}`, "req-rearchive")
	testassert.NoError(t, err)
	deleted, err := command("delete", `{}`, "req-delete")
	testassert.NoError(t, err)
	testassert.Require(t, deleted.Failure == nil, "delete failure = %+v", deleted.Failure)
	var deletion protocol.DeletionResult
	testassert.NoError(t, json.Unmarshal(deleted.Value, &deletion))
	testassert.Require(t, deletion.CleanupFailures != nil, "delete receipt lost its cleanup_failures array")
	testassert.Require(t, countEvent(events.all(), "command:revert-call-error") == 0, "pre-traversal revert produced a CallError partial")

	// Create uses its ordinary request value and Caller authorization on a
	// Harness-target command route.
	created, err := r.InvokeExtension(ctx, harnessCaller, fixtureRequest(fixtureExtensionID, "command_harness", harnessTarget(), `{"method":"create_session","arguments":{"workspace":"`+workspace+`"}}`, "req-create"))
	testassert.NoError(t, err)
	testassert.Require(t, created.Failure == nil, "create_session failure = %+v", created.Failure)
	var createdSession struct {
		SessionID string `json:"session_id"`
	}
	testassert.NoError(t, json.Unmarshal(created.Value, &createdSession))
	testassert.Require(t, createdSession.SessionID != "", "create_session returned no Session: %s", created.Value)

	// A pure callback cannot dispatch the command bridge, and the refusal is
	// the composition denial, not a partial.
	pure, err := r.InvokeExtension(ctx, harnessCaller, fixtureRequest(fixtureExtensionID, "pure_command", harnessTarget(), `{"method":"submit","arguments":{}}`, "req-pure"))
	testassert.Require(t, err != nil && pure.Failure == nil && errors.Is(err, ErrDenied), "pure command = (%+v, %v), want the composition refusal", pure, err)
	testassert.Require(t, countEvent(events.all(), "pure:submit:attempt") == 1, "pure callback did not attempt the refused bridge")
	testassert.Require(t, countEvent(events.all(), "command:submit") == submitReached+2, "the refused pure attempt reached the command bridge: %v", events.all())
}

func firstItemID(t *testing.T, r *Melta, sessionID string) string {
	t.Helper()
	page, err := r.GetHistory(context.Background(), fixtureCaller(t, r, harnessScope), sessionID, nil)
	testassert.NoError(t, err)
	testassert.Require(t, len(page.Items) != 0, "session %q has no history", sessionID)
	for _, item := range page.Items {
		raw, err := json.Marshal(item)
		testassert.NoError(t, err)
		var members map[string]json.RawMessage
		testassert.NoError(t, json.Unmarshal(raw, &members))
		if string(bytes.TrimSpace(members["kind"])) != `"input"` {
			continue
		}
		var value struct {
			ItemId string `json:"item_id"`
		}
		testassert.NoError(t, json.Unmarshal(raw, &value))
		if value.ItemId != "" {
			return value.ItemId
		}
	}
	t.Fatalf("session %q has no input item", sessionID)
	return ""
}

// TestExtensionQueryDeletionAndCancellation proves the authoritative
// Session-target completion: a parked read racing a successful fork receipt
// cannot outrank it — after the source Session is deleted only the read
// returns not-found while the committed fork remains — and a late request
// cancellation cannot replace a produced result after its Session remains
// available.
func TestExtensionQueryDeletionAndCancellation(t *testing.T) {
	r, e, _ := openOperationFixture(t)
	ctx := context.Background()
	harnessCaller := fixtureCaller(t, r, harnessScope)
	workspace := filepath.Join(e.home, "query")
	session := projectionSession(t, r, workspace, "solo")
	sessionID := session.Identity.SessionID
	submitCodeOperation(t, r, sessionID, "source", "hello")
	boundary := firstItemID(t, r, sessionID)
	sessionCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeSession, SessionID: sessionID})

	invokeHold := func(ctx context.Context, caller Caller, hold, release, block, requestID, targetSession string) (extension.Result, error) {
		arguments, _ := json.Marshal(map[string]string{"hold": hold, "release": release, "block": block})
		return r.InvokeExtension(ctx, caller, fixtureRequest(fixtureExtensionID, "query_hold", sessionTarget(targetSession), string(arguments), requestID))
	}
	hold, release := filepath.Join(t.TempDir(), "hold"), filepath.Join(t.TempDir(), "release")
	outcomes := make(chan extensionOutcome, 1)
	go func() {
		result, err := invokeHold(ctx, sessionCaller, hold, release, "", "req-deleted", sessionID)
		outcomes <- extensionOutcome{result: result, err: err}
	}()
	awaitFile(t, hold)

	// The fork completes while the read is parked: its committed receipt
	// survives the source deletion that follows.
	forkArguments, _ := json.Marshal(map[string]any{"method": "fork", "arguments": map[string]any{
		"operation_id": "fork-op", "boundary_item_id": boundary,
		"content": []map[string]string{{"kind": "text", "text": "fork"}},
	}})
	forked, err := r.InvokeExtension(ctx, sessionCaller, fixtureRequest(fixtureExtensionID, "command", sessionTarget(sessionID), string(forkArguments), "req-fork"))
	testassert.NoError(t, err)
	testassert.Require(t, forked.Failure == nil, "fork failure = %+v", forked.Failure)
	var receipt protocol.ForkResult
	testassert.NoError(t, json.Unmarshal(forked.Value, &receipt))
	testassert.Require(t, receipt.Session.SessionId != "" && receipt.Operation.OperationId == "fork-op", "fork receipt = %+v", receipt)

	_, err = r.ArchiveSession(ctx, harnessCaller, sessionID)
	testassert.NoError(t, err)
	_, err = r.DeleteSession(ctx, harnessCaller, sessionID)
	testassert.NoError(t, err)
	testassert.NoError(t, os.WriteFile(release, []byte("release"), 0o600))
	var deletion extensionOutcome
	select {
	case deletion = <-outcomes:
	case <-time.After(10 * time.Second):
		t.Fatal("parked query never returned")
	}
	testassert.Require(t, errors.Is(deletion.err, harness.ErrNotFound), "query after Session deletion = %v, want not-found", deletion.err)
	awaitOperation(t, r, receipt.Session.SessionId, "fork-op", harness.OperationSuccess)
	header := headerThroughRuntime(t, r, receipt.Session.SessionId)
	testassert.Equal(t, header.Identity.SessionID, receipt.Session.SessionId)

	// A late cancellation after the handler produced its success leaves the
	// produced result in place.
	second := projectionSession(t, r, workspace, "solo")
	secondCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeSession, SessionID: second.Identity.SessionID})
	hold2, release2, block2 := filepath.Join(t.TempDir(), "hold"), filepath.Join(t.TempDir(), "release"), filepath.Join(t.TempDir(), "block")
	cancelCtx, cancel := context.WithCancel(ctx)
	outcomes = make(chan extensionOutcome, 1)
	go func() {
		result, err := invokeHold(cancelCtx, secondCaller, hold2, release2, block2, "req-late-cancel", second.Identity.SessionID)
		outcomes <- extensionOutcome{result: result, err: err}
	}()
	awaitFile(t, hold2)
	testassert.NoError(t, os.WriteFile(release2, []byte("release"), 0o600))
	awaitFile(t, block2+".produced")
	cancel()
	testassert.NoError(t, os.WriteFile(block2, []byte("block"), 0o600))
	var late extensionOutcome
	select {
	case late = <-outcomes:
	case <-time.After(10 * time.Second):
		t.Fatal("late-cancelled query never returned")
	}
	testassert.Require(t, late.err == nil && late.result.Failure == nil && string(late.result.Value) == `{"held":true}`, "late-cancelled query = (%+v, %v), want the produced result", late.result, late.err)
}

// TestExtensionRevertPartial proves the actual produced reversion partial
// through the public extension path: the fixed revert command preserves the
// complete accumulated result as the declared failure's partial, not a
// fabricated empty error or a second restore.
func TestExtensionRevertPartial(t *testing.T) {
	r, e, events := openOperationFixture(t)
	ctx := context.Background()
	workspace := filepath.Join(e.home, "revert")
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))
	session := projectionSession(t, r, workspace, "solo")
	sessionID := session.Identity.SessionID
	first := submitCodeOperation(t, r, sessionID, "op-first", "first")
	second := submitCodeOperation(t, r, sessionID, "op-second", "second")
	awaitRestorableSession(t, r, sessionID)

	// The later-visited (earlier-admitted) group fails on a corrupt meta
	// after the earlier-visited group restores.
	badEntryDir := filepath.Join(codeGroupRoot(r, sessionID, first.Admission.AdmittedEntry.EntryID), "snapshots", "1", "aaaaaaaaaaaaaaaa")
	testassert.NoError(t, os.MkdirAll(badEntryDir, 0o700))
	testassert.NoError(t, os.WriteFile(filepath.Join(badEntryDir, "meta.json"), []byte("{"), 0o600))
	target := filepath.Join(workspace, "parked.txt")
	testassert.NoError(t, os.WriteFile(target, []byte("current"), 0o600))
	fifo := seedFIFOCodeEntry(t, codeGroupRoot(r, sessionID, second.Admission.AdmittedEntry.EntryID), target, "current")

	sessionCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeSession, SessionID: sessionID})
	outcomes := make(chan extensionOutcome, 1)
	go func() {
		result, err := r.InvokeExtension(ctx, sessionCaller, fixtureRequest(fixtureExtensionID, "command", sessionTarget(sessionID), `{"method":"revert_code","arguments":{"boundary_operation_id":"op-first"}}`, "req-revert"))
		outcomes <- extensionOutcome{result: result, err: err}
	}()
	park := parkOrOutcome(t, fifo, outcomes)
	defer park.flush()
	park.release("restored")
	var out extensionOutcome
	select {
	case out = <-outcomes:
	case <-time.After(10 * time.Second):
		t.Fatal("revert command never returned")
	}
	testassert.Require(t, out.err == nil, "revert extension call = %v, want a declared failure with nil Go error", out.err)
	testassert.Require(t, out.result.Failure != nil && out.result.Failure.Code == "internal" && out.result.Failure.Message != "" && out.result.Value == nil, "revert result = %+v, want the declared internal failure", out.result)
	var partial protocol.CodeRevertResult
	testassert.NoError(t, json.Unmarshal(out.result.Failure.Partial, &partial))
	testassert.Require(t, len(partial.Restored) == 1 && partial.Restored[0] == target && partial.Skipped != nil && partial.Error != nil && partial.Error.Code == protocol.ErrorCodeInternal, "revert partial = %+v, want the parked group's accumulated restore and the traversal error", partial)
	wantPartial := "command:revert-call-error;class=internal;partial=" + fixtureHash(string(out.result.Failure.Partial))
	testassert.Require(t, slices.Contains(events.all(), wantPartial), "fixture handler did not observe a nonnil CallError with the exact partial: %v", events.all())
}

// TestExtensionRevertPartialThroughAPICommand proves a revert_code CallError
// partial survives the independent API.Command route as the declared failure's
// exact partial.
func TestExtensionRevertPartialThroughAPICommand(t *testing.T) {
	deferred := make(chan extension.Result, 1)
	r, e, events := openOperationFixtureDeferred(t, deferred)
	ctx := context.Background()
	workspace := filepath.Join(e.home, "revert-independent")
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))
	session := projectionSession(t, r, workspace, "solo")
	sessionID := session.Identity.SessionID
	first := submitCodeOperation(t, r, sessionID, "op-first", "first")
	second := submitCodeOperation(t, r, sessionID, "op-second", "second")
	awaitRestorableSession(t, r, sessionID)

	badEntryDir := filepath.Join(codeGroupRoot(r, sessionID, first.Admission.AdmittedEntry.EntryID), "snapshots", "1", "aaaaaaaaaaaaaaaa")
	testassert.NoError(t, os.MkdirAll(badEntryDir, 0o700))
	testassert.NoError(t, os.WriteFile(filepath.Join(badEntryDir, "meta.json"), []byte("{"), 0o600))
	target := filepath.Join(workspace, "parked.txt")
	testassert.NoError(t, os.WriteFile(target, []byte("current"), 0o600))
	fifo := seedFIFOCodeEntry(t, codeGroupRoot(r, sessionID, second.Admission.AdmittedEntry.EntryID), target, "current")

	sessionCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeSession, SessionID: sessionID})
	invoked, err := r.InvokeExtension(ctx, sessionCaller, fixtureRequest(fixtureExtensionID, "independent_command", sessionTarget(sessionID), `{"method":"revert_code","arguments":{"boundary_operation_id":"op-first"}}`, "req-independent-revert"))
	testassert.Require(t, err == nil && invoked.Failure == nil, "independent command invocation = (%+v, %v), want the deferred success", invoked, err)
	outcomes := make(chan extensionOutcome, 1)
	go func() { outcomes <- extensionOutcome{result: <-deferred} }()
	park := parkOrOutcome(t, fifo, outcomes)
	defer park.flush()
	park.release("restored")
	var out extensionOutcome
	select {
	case out = <-outcomes:
	case <-time.After(10 * time.Second):
		t.Fatal("independent revert command never returned")
	}
	testassert.Require(t, out.err == nil, "independent revert call = %v, want a declared failure", out.err)
	testassert.Require(t, out.result.Failure != nil && out.result.Failure.Code == "internal" && out.result.Value == nil, "independent revert result = %+v, want the declared internal failure", out.result)
	var partial protocol.CodeRevertResult
	testassert.NoError(t, json.Unmarshal(out.result.Failure.Partial, &partial))
	testassert.Require(t, len(partial.Restored) == 1 && partial.Restored[0] == target && partial.Skipped != nil && partial.Error != nil && partial.Error.Code == protocol.ErrorCodeInternal, "independent revert partial = %+v, want the accumulated restore and traversal error", partial)
	wantPartial := "api-command:revert-call-error;class=internal;partial=" + fixtureHash(string(out.result.Failure.Partial))
	testassert.Require(t, slices.Contains(events.all(), wantPartial), "API.Command did not preserve the CallError partial: %v", events.all())
}

// TestExtensionRevertUnmappedCallError proves a handler that forwards the
// bridge error directly keeps the public command class and the exact produced
// partial as extension.CallError.
func TestExtensionRevertUnmappedCallError(t *testing.T) {
	r, e, _ := openOperationFixture(t)
	ctx := context.Background()
	workspace := filepath.Join(e.home, "revert-unmapped")
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))
	session := projectionSession(t, r, workspace, "solo")
	sessionID := session.Identity.SessionID
	first := submitCodeOperation(t, r, sessionID, "op-first", "first")
	second := submitCodeOperation(t, r, sessionID, "op-second", "second")
	awaitRestorableSession(t, r, sessionID)

	badEntryDir := filepath.Join(codeGroupRoot(r, sessionID, first.Admission.AdmittedEntry.EntryID), "snapshots", "1", "aaaaaaaaaaaaaaaa")
	testassert.NoError(t, os.MkdirAll(badEntryDir, 0o700))
	testassert.NoError(t, os.WriteFile(filepath.Join(badEntryDir, "meta.json"), []byte("{"), 0o600))
	target := filepath.Join(workspace, "parked.txt")
	testassert.NoError(t, os.WriteFile(target, []byte("current"), 0o600))
	fifo := seedFIFOCodeEntry(t, codeGroupRoot(r, sessionID, second.Admission.AdmittedEntry.EntryID), target, "current")

	sessionCaller := fixtureCaller(t, r, CallerScope{Kind: CallerScopeSession, SessionID: sessionID})
	outcomes := make(chan extensionOutcome, 1)
	go func() {
		result, err := r.InvokeExtension(ctx, sessionCaller, fixtureRequest(fixtureExtensionID, "command_unmapped", sessionTarget(sessionID), `{"method":"revert_code","arguments":{"boundary_operation_id":"op-first"}}`, "req-revert-unmapped"))
		outcomes <- extensionOutcome{result: result, err: err}
	}()
	park := parkOrOutcome(t, fifo, outcomes)
	defer park.flush()
	park.release("restored")
	var out extensionOutcome
	select {
	case out = <-outcomes:
	case <-time.After(10 * time.Second):
		t.Fatal("unmapped revert command never returned")
	}
	var callErr *extension.CallError
	testassert.Require(t, errors.As(out.err, &callErr) && callErr.Partial != nil && out.result.Failure == nil, "unmapped revert = (%+v, %v), want extension.CallError with a partial", out.result, out.err)
	testassert.Require(t, ErrorCode(callErr) == protocol.ErrorCodeInternal && callErr.Err != nil, "unmapped revert class = %q cause = %v, want the original internal class and cause", ErrorCode(callErr), callErr.Err)
	var partial protocol.CodeRevertResult
	testassert.NoError(t, json.Unmarshal(callErr.Partial, &partial))
	testassert.Require(t, len(partial.Restored) == 1 && partial.Restored[0] == target && partial.Skipped != nil && partial.Error != nil && partial.Error.Code == protocol.ErrorCodeInternal, "unmapped partial = %+v", partial)
	remarshaled, err := json.Marshal(partial)
	testassert.NoError(t, err)
	testassert.Require(t, bytes.Equal(callErr.Partial, remarshaled), "unmapped partial bytes changed: %q vs %q", callErr.Partial, remarshaled)
}

type extensionOutcome struct {
	result extension.Result
	err    error
}

// parkOrOutcome waits for the restore reader to park, but reports an early
// restore result instead of hiding it behind the FIFO deadline.
func parkOrOutcome(t *testing.T, fifo string, outcomes <-chan extensionOutcome) *fifoPark {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		fd, err := unix.Open(fifo, unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if err == nil {
			return &fifoPark{writerFD: fd, fifo: fifo}
		}
		select {
		case out := <-outcomes:
			if out.result.Failure != nil {
				t.Fatalf("restore returned before parking: failure=%+v partial=%s err=%v", *out.result.Failure, out.result.Failure.Partial, out.err)
			}
			t.Fatalf("restore returned before parking: result=%+v err=%v", out.result, out.err)
		default:
		}
		testassert.Require(t, errors.Is(err, unix.ENXIO), "FIFO writer open: %v", err)
		testassert.Require(t, !time.Now().After(deadline), "the restore never parked on the FIFO saved original")
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitFile waits for one fixture marker file.
func awaitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		testassert.Require(t, !time.Now().After(deadline), "marker %s never appeared", path)
		time.Sleep(5 * time.Millisecond)
	}
}

// TestExtensionInvocationCallerOwnership proves the public invocation path
// uses the same owner-bound Caller check as every other producer: a Caller
// issued by another real owner, an unissued Caller with a valid-looking
// scope, and the zero Caller all deny a valid exposed address before any
// handler or command dispatch. The same-owner grant remains the positive
// control.
func TestExtensionInvocationCallerOwnership(t *testing.T) {
	r, _, events := openOperationFixture(t)
	ctx := context.Background()
	harnessCaller := fixtureCaller(t, r, harnessScope)
	request := func(requestID string) protocol.ExtensionInvocationRequest {
		return fixtureRequest(fixtureExtensionID, "echo", harnessTarget(), `{"ok":true}`, requestID)
	}

	before := countEvent(events.all(), "service:fixture.service:")
	result, err := r.InvokeExtension(ctx, harnessCaller, request("req-owner"))
	testassert.NoError(t, err)
	testassert.Require(t, result.Failure == nil && string(result.Value) == `{"ok":true}`, "same-owner result = %+v", result)
	testassert.Require(t, countEvent(events.all(), "service:fixture.service:") == before+1, "same-owner grant did not dispatch")

	foreign, _, _ := openOperationFixture(t)
	foreignCaller := fixtureCaller(t, foreign, harnessScope)
	unissued := Caller{origin: harness.InputOriginUser, scope: harnessScope}
	for _, tc := range []struct {
		name   string
		caller Caller
	}{
		{"foreign owner", foreignCaller},
		{"unissued caller", unissued},
		{"zero caller", Caller{}},
	} {
		before = countEvent(events.all(), "service:fixture.service:")
		_, err := r.InvokeExtension(ctx, tc.caller, request("req-"+strings.ReplaceAll(tc.name, " ", "-")))
		testassert.Require(t, errors.Is(err, ErrDenied), "%s = %v, want denied", tc.name, err)
		testassert.Require(t, countEvent(events.all(), "service:fixture.service:") == before, "%s dispatched a handler", tc.name)
	}
}

func TestCallErrorOwnershipPreservesErrorChain(t *testing.T) {
	for _, joined := range []bool{false, true} {
		partial := json.RawMessage(`{"restored":["a.go"]}`)
		call := &extension.CallError{Err: harness.ErrStorage, Partial: partial}
		var original error = fmt.Errorf("command failed: %w", call)
		if joined {
			original = errors.Join(harness.ErrConflict, original)
		}
		owned := ownCallError(original)
		if owned.Error() != original.Error() || ErrorCode(owned) != ErrorCode(original) || !errors.Is(owned, harness.ErrStorage) || errors.Is(owned, harness.ErrConflict) != joined {
			t.Fatalf("owned error lost its enclosing chain: got %v (%s), want %v (%s)", owned, ErrorCode(owned), original, ErrorCode(original))
		}
		var result *extension.CallError
		if !errors.As(owned, &result) || !bytes.Equal(result.Partial, partial) {
			t.Fatalf("owned error lost its partial: %v", owned)
		}
		partial[0] = '['
		if string(result.Partial) != `{"restored":["a.go"]}` {
			t.Fatalf("owned partial retained caller bytes: %s", result.Partial)
		}
	}
	plain := errors.New("ordinary failure")
	if ownCallError(plain) != plain {
		t.Fatal("ordinary error was replaced")
	}
}

// TestExtensionCallErrorPartialValidation proves call-error forwarding
// preserves valid multiline/large-number, null, empty and wrapped causes
// exactly; only actual malformed output is refused as the existing extension
// invalid-output sentinel.
func TestExtensionCallErrorPartialValidation(t *testing.T) {
	r, _, _ := openOperationFixture(t)
	invoke := func(mode string) (extension.Result, error) {
		return r.InvokeExtension(context.Background(), fixtureCaller(t, r, harnessScope), fixtureRequest(fixtureExtensionID, "call_error", harnessTarget(), `{"mode":"`+mode+`"}`, "req-call-error-"+mode))
	}
	for _, tc := range []struct {
		mode    string
		partial string
		typed   bool
		wrapped bool
	}{
		{"valid", validCallErrorPartial, true, false},
		{"null", "null", true, false},
		{"empty", "", false, false},
		{"wrapped", validCallErrorPartial, true, true},
	} {
		result, err := invoke(tc.mode)
		var callErr *extension.CallError
		testassert.Require(t, result.Failure == nil && err != nil, "%s result = (%+v, %v)", tc.mode, result, err)
		testassert.Require(t, errors.Is(err, harness.ErrStorage) && ErrorCode(err) == protocol.ErrorCodeStorage, "%s class = %q", tc.mode, ErrorCode(err))
		if !tc.typed {
			if errors.As(err, &callErr) {
				testassert.Require(t, callErr.Partial == nil, "%s partial = %q, want absent", tc.mode, callErr.Partial)
			}
			continue
		}
		testassert.Require(t, errors.As(err, &callErr), "%s error = %v, want CallError", tc.mode, err)
		testassert.Require(t, string(callErr.Partial) == tc.partial, "%s partial = %q, want %q", tc.mode, callErr.Partial, tc.partial)
		if tc.wrapped {
			testassert.Require(t, strings.Contains(callErr.Err.Error(), "wrapped:"), "wrapped cause = %v", callErr.Err)
		}
	}
}

// TestExtensionMalformedCallErrorPartialRefused proves malformed call output
// is rejected at acceptance as the extension invalid-output sentinel, never
// an exposed CallError carrying invalid bytes.
func TestExtensionMalformedCallErrorPartialRefused(t *testing.T) {
	r, _, _ := openOperationFixture(t)
	result, err := r.InvokeExtension(context.Background(), fixtureCaller(t, r, harnessScope), fixtureRequest(fixtureExtensionID, "call_error", harnessTarget(), `{"mode":"bad"}`, "req-bad-call-error"))
	var callErr *extension.CallError
	testassert.Require(t, result.Failure == nil && err != nil && !errors.As(err, &callErr), "malformed call error = (%+v, %v), want no CallError", result, err)
	testassert.Require(t, errors.Is(err, extension.ErrInvalid), "malformed call error = %v, want the extension invalid-output sentinel", err)
}
