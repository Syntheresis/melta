package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestToolBindingSettlement(t *testing.T) {
	for _, tc := range []struct {
		name       string
		callName   string
		missing    bool
		broken     string
		invalid    bool
		denied     bool
		executor   bool
		wantStatus model.ToolResultStatus
		wantBound  bool
		wantArgs   bool
	}{
		{name: "unadvertised", callName: "ghost", wantStatus: model.ResultError},
		{name: "missing", callName: "echo", missing: true, wantStatus: model.ResultError},
		{name: "invalid binding without callbacks", callName: "echo", broken: "both", wantStatus: model.ResultError},
		{name: "invalid binding without normalizer", callName: "echo", broken: "normalizer", wantStatus: model.ResultError},
		{name: "invalid binding without preparer", callName: "echo", broken: "preparer", wantStatus: model.ResultError},
		{name: "invalid binding without identity", callName: "echo", broken: "identity", wantStatus: model.ResultError},
		{name: "normalization failure", callName: "echo", invalid: true, wantStatus: model.ResultError, wantBound: true},
		{name: "denied", callName: "echo", denied: true, wantStatus: model.ResultDenied, wantBound: true, wantArgs: true},
		{name: "immediate", callName: "echo", wantStatus: model.ResultSuccess, wantBound: true, wantArgs: true},
		{name: "executor", callName: "echo", executor: true, wantStatus: model.ResultSuccess, wantBound: true, wantArgs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookups, normalizations, prepares, executions, releases := 0, 0, 0, 0, 0
			identity := ToolImplementation{ID: "actual-capability", ExtensionID: "actual-extension"}
			inv := InvocationExecution{LookupTool: func(name string) (ToolBinding, bool) {
				lookups++
				if tc.missing {
					return ToolBinding{}, false
				}
				binding := ToolBinding{
					Identity: identity,
					Release:  func() { releases++ },
					Normalize: func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
						normalizations++
						if tc.invalid {
							return nil, errors.New("bad arguments")
						}
						return objectNormalize(ctx, call)
					},
					Prepare: func(ctx context.Context, call model.ToolCall) PreparedTool {
						prepares++
						result := ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
						if tc.denied {
							return PreparedTool{Permissions: []PermissionRequest{{Permission: PermissionFileRead, Target: "/outside"}}, CanonicalWorkspace: "/w", Execute: func(context.Context) ToolOutcome {
								executions++
								return result
							}}
						}
						if tc.executor {
							return PreparedTool{Permissions: fixturePermission, Execute: func(context.Context) ToolOutcome {
								executions++
								return result
							}}
						}
						return PreparedTool{Permissions: fixturePermission, Immediate: &result}
					},
				}
				switch tc.broken {
				case "both":
					binding.Normalize, binding.Prepare = nil, nil
				case "normalizer":
					binding.Normalize = nil
				case "preparer":
					binding.Prepare = nil
				case "identity":
					binding.Identity.ID = ""
				}
				return binding, true
			}}
			_, _, _, result, err := freshToolBoundary(t, testCapture(), inv, model.ToolCall{ID: "call-1", Name: tc.callName, Arguments: json.RawMessage(`{"x":1}`)})
			testassert.NoError(t, err)
			testassert.Equal(t, result.Status, tc.wantStatus)
			wantEvidence := ExecutionNotStarted
			if tc.wantStatus == model.ResultSuccess {
				wantEvidence = ExecutionObserved
			}
			testassert.Equal(t, result.Evidence, wantEvidence)
			testassert.Require(t, (result.Implementation != nil) == tc.wantBound, "implementation = %+v", result.Implementation)
			if tc.wantBound {
				testassert.Equal(t, *result.Implementation, identity)
			}
			testassert.Require(t, (len(result.NormalizedArguments) > 0) == tc.wantArgs, "arguments = %q", result.NormalizedArguments)
			if tc.callName == "ghost" {
				testassert.Equal(t, lookups, 0)
			} else {
				testassert.Equal(t, lookups, 1)
			}
			if tc.missing || tc.broken != "" || tc.callName == "ghost" {
				testassert.Equal(t, normalizations, 0)
			} else {
				testassert.Equal(t, normalizations, 1)
			}
			if !tc.wantArgs {
				testassert.Equal(t, prepares, 0)
			}
			if !tc.executor {
				testassert.Equal(t, executions, 0)
			}
			if tc.callName == "ghost" || tc.missing {
				testassert.Equal(t, releases, 0)
			} else {
				testassert.Equal(t, releases, 1)
			}
		})
	}
}

func TestNormalizationFailureInterruption(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cause       error
		cancel      bool
		deadline    bool
		interrupted bool
	}{
		{name: "operation cancellation", cause: context.Canceled, cancel: true, interrupted: true},
		{name: "wrapped operation cancellation", cause: fmt.Errorf("normalize: %w", context.Canceled), cancel: true, interrupted: true},
		{name: "deadline error with canceled operation", cause: context.DeadlineExceeded, cancel: true, interrupted: true},
		{name: "operation deadline", cause: context.DeadlineExceeded, deadline: true, interrupted: true},
		{name: "declared interruption", cause: &EffectError{Err: errors.New("stopped"), Interrupted: true}, interrupted: true},
		{name: "cancellation with live operation", cause: context.Canceled},
		{name: "deadline with live operation", cause: context.DeadlineExceeded},
		{name: "cancellation wrapped by effect error with canceled operation", cause: &EffectError{Err: context.Canceled}, cancel: true, interrupted: true},
		{name: "cancellation wrapped by effect error with live operation", cause: &EffectError{Err: context.Canceled}},
		{name: "validation wrapped by effect error with canceled operation", cause: &EffectError{Err: errors.New("invalid arguments")}, cancel: true},
		{name: "validation failure with canceled operation", cause: errors.New("invalid arguments"), cancel: true},
		{name: "validation failure with live operation", cause: errors.New("invalid arguments")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tc.deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			defer cancel()
			prepares := 0
			exec := hookFixtureExecution(func(context.Context, model.ToolCall) PreparedTool {
				prepares++
				return PreparedTool{}
			}, func(normalizeCtx context.Context, _ model.ToolCall) (json.RawMessage, error) {
				if tc.cancel {
					cancel()
				}
				if tc.deadline {
					<-normalizeCtx.Done()
				}
				return nil, tc.cause
			})
			_, _, _, _, results, err := freshExecutionToolCallsCtx(t, ctx, testCapture(), exec, testToolCall("call-1"))
			testassert.NoError(t, err)
			testassert.Equal(t, prepares, 0)
			wantStatus, wantContent := model.ResultError, tc.cause.Error()
			if tc.interrupted {
				wantStatus, wantContent = model.ResultInterrupted, interruptedToolResultContent
			}
			result := results[0]
			testassert.Require(t, result.Status == wantStatus && result.Content == wantContent && result.Evidence == ExecutionNotStarted &&
				result.Implementation != nil && result.Implementation.ID == "echo" && len(result.NormalizedArguments) == 0,
				"result = %+v, want %s %q with binding and no effective arguments", result, wantStatus, wantContent)
		})
	}
}

func TestOpaqueArgumentHookConsequences(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte("not json"), {0xff, 0xfe}, []byte(`[1,2]`)} {
		t.Run(base64.StdEncoding.EncodeToString(raw), func(t *testing.T) {
			hooks := []ToolArgumentsHook{
				{ID: "keep", ExtensionID: "ext", Mode: HookModeEffectful, Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
					return ToolArgumentsResult{Arguments: model.CloneRaw(call.Arguments)}, nil
				}},
				{ID: "repair", ExtensionID: "ext", Mode: HookModePure, Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
					testassert.Require(t, bytes.Equal(call.Arguments, raw), "repair input = %q, want %q", call.Arguments, raw)
					return ToolArgumentsResult{Arguments: json.RawMessage(`{"fixed":true}`)}, nil
				}},
			}
			spy := &hookToolSpy{}
			exec := hookFixtureExecution(spy.tool, objectNormalize, hooks...)
			_, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, model.ToolCall{ID: "call-1", Name: "echo", Arguments: raw})
			testassert.NoError(t, err)
			testassert.Equal(t, results[0].Status, model.ResultSuccess)
			history := hookGraphHookResults(t, store, sessionID)
			testassert.Require(t, len(history) == 1 && bytes.Equal(history[0].Arguments, raw), "retained consequence = %+v, want %q", history, raw)
			testassert.Require(t, string(results[0].NormalizedArguments) == `{"fixed":true}`, "effective arguments = %q", results[0].NormalizedArguments)
		})
	}
}

func TestToolLookupAfterHooksKeepsBoundImplementation(t *testing.T) {
	current := "before"
	var prepared, executed string
	hook := ToolArgumentsHook{ID: "replace", ExtensionID: "ext", Mode: HookModePure, Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		current = "after"
		return ToolArgumentsResult{Arguments: call.Arguments}, nil
	}}
	inv := InvocationExecution{
		ToolHooks: []ToolArgumentsHook{hook},
		LookupTool: func(string) (ToolBinding, bool) {
			bound := current
			return ToolBinding{
				Identity: ToolImplementation{ID: bound},
				Normalize: func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
					current = "later"
					return objectNormalize(ctx, call)
				},
				Prepare: func(_ context.Context, call model.ToolCall) PreparedTool {
					prepared = bound
					current = "after preparation"
					return PreparedTool{Permissions: fixturePermission, Execute: func(context.Context) ToolOutcome {
						executed = bound
						return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
					}}
				},
			}, true
		},
	}
	_, _, _, result, err := freshToolBoundary(t, testCapture(), inv, testToolCall("call-1"))
	testassert.NoError(t, err)
	testassert.Require(t, prepared == "after" && executed == "after" && result.Implementation != nil && result.Implementation.ID == "after", "prepared %q executed %q result %+v", prepared, executed, result)
}

func TestToolResultArgumentsRequireBinding(t *testing.T) {
	for _, evidence := range []ExecutionEvidence{ExecutionNotStarted, ExecutionObserved, ExecutionUnknown} {
		result := validToolResultEntry(testOpID)
		result.Evidence = evidence
		result.NormalizedArguments = json.RawMessage(`{}`)
		result.Implementation = nil
		if err := validateToolResultEntry(result); err == nil {
			t.Fatalf("%s effective arguments without a bound implementation were accepted", evidence)
		}
		result.Implementation = &ToolImplementation{ID: "echo"}
		testassert.NoError(t, validateToolResultEntry(result))
	}
}

func TestObservedToolResultRequiresArguments(t *testing.T) {
	for _, evidence := range []ExecutionEvidence{ExecutionObserved, ExecutionUnknown} {
		result := validToolResultEntry(testOpID)
		result.Evidence = evidence
		result.NormalizedArguments = nil
		if err := validateToolResultEntry(result); err == nil {
			t.Fatalf("%s result without effective arguments was accepted", evidence)
		}
		result.NormalizedArguments = json.RawMessage(`{}`)
		testassert.NoError(t, validateToolResultEntry(result))
	}
}

func TestCancellationDuringNormalizationStopsPreparation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepares := 0
	normalize := func(context.Context, model.ToolCall) (json.RawMessage, error) {
		cancel()
		return json.RawMessage(`{"x":1}`), nil
	}
	prepare := func(context.Context, model.ToolCall) PreparedTool {
		prepares++
		return PreparedTool{}
	}
	exec := hookFixtureExecution(prepare, normalize)
	_, _, _, _, results, err := freshExecutionToolCallsCtx(t, ctx, testCapture(), exec, testToolCall("call-1"))
	testassert.NoError(t, err)
	testassert.Equal(t, prepares, 0)
	testassert.Require(t, results[0].Status == model.ResultInterrupted && results[0].Implementation != nil && string(results[0].NormalizedArguments) == `{"x":1}`, "settlement = %+v", results[0])
}

func TestToolImplementationCodec(t *testing.T) {
	result := validToolResultEntry(testOpID)
	result.Implementation = &ToolImplementation{ID: "capability", ExtensionID: "extension"}
	raw, err := encodeToolResultEntry(result)
	testassert.NoError(t, err)
	env := Entry{SessionID: result.SessionID, ID: result.EntryID, OperationID: result.OperationID, Kind: EntryToolResult, Payload: raw}
	decoded, err := decodeToolResultEntry(env)
	testassert.NoError(t, err)
	testassert.Equal(t, *decoded.Implementation, *result.Implementation)
	for _, value := range []string{`null`, `{}`, `{"id":""}`, `{"id":"tool","extension_id":""}`, `{"id":"tool","extra":1}`} {
		env.Payload = setKey(raw, "implementation", json.RawMessage(value))
		if _, err := decodeToolResultEntry(env); err == nil {
			t.Errorf("implementation %s accepted", value)
		}
	}
}
