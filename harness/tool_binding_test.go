package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestToolBindingSettlement(t *testing.T) {
	for _, tc := range []struct {
		name, call, broken       string
		missing, invalid, denied bool
		status                   model.ToolResultStatus
		bound, args              bool
	}{
		{name: "unadvertised", call: "ghost", status: model.ResultError},
		{name: "missing", call: "echo", missing: true, status: model.ResultError},
		{name: "invalid binding without validator", call: "echo", broken: "validate", status: model.ResultError},
		{name: "invalid binding without run", call: "echo", broken: "run", status: model.ResultError},
		{name: "invalid binding without identity", call: "echo", broken: "identity", status: model.ResultError},
		{name: "validation failure", call: "echo", invalid: true, status: model.ResultError},
		{name: "denied", call: "echo", denied: true, status: model.ResultDenied, bound: true, args: true},
		{name: "success", call: "echo", status: model.ResultSuccess, bound: true, args: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookups, validations, runs, releases := 0, 0, 0, 0
			identity := ToolImplementation{ID: "actual-capability", ExtensionID: "actual-extension"}
			inv := InvocationExecution{LookupTool: func(string) (ToolBinding, bool, error) {
				lookups++
				if tc.missing {
					return ToolBinding{}, false, nil
				}
				binding := ToolBinding{Identity: identity, Release: func() { releases++ }, Validate: func(json.RawMessage) error {
					validations++
					if tc.invalid {
						return errors.New("bad arguments")
					}
					return nil
				}, Run: func(_ context.Context, call model.ToolCall) ToolOutcome {
					runs++
					return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
				}}
				switch tc.broken {
				case "validate":
					binding.Validate = nil
				case "run":
					binding.Run = nil
				case "identity":
					binding.Identity.ID = ""
				}
				return binding, true, nil
			}}
			if tc.denied {
				inv.CheckStage = func() CheckStage {
					return CheckStage{Hooks: []ToolCheckHook{{ID: "check", ExtensionID: "actual-extension", Run: func(context.Context, model.ToolCall) (*string, error) { reason := "restricted"; return &reason, nil }}}}
				}
			}
			_, _, _, result, err := freshToolBoundary(t, testCapture(), inv, model.ToolCall{ID: "call-1", Name: tc.call, Arguments: json.RawMessage(`{"x":1}`)})
			testassert.NoError(t, err)
			testassert.Equal(t, result.Status, tc.status)
			evidence := ExecutionNotStarted
			if tc.status == model.ResultSuccess {
				evidence = ExecutionObserved
			}
			testassert.Equal(t, result.Evidence, evidence)
			testassert.Require(t, (result.Implementation != nil) == tc.bound, "implementation=%+v", result.Implementation)
			if tc.bound {
				testassert.Equal(t, *result.Implementation, identity)
			}
			testassert.Require(t, (len(result.NormalizedArguments) > 0) == tc.args, "arguments=%q", result.NormalizedArguments)
			if tc.call == "ghost" {
				testassert.Equal(t, lookups, 0)
			} else {
				testassert.Equal(t, lookups, 1)
			}
			if tc.missing || tc.broken != "" || tc.call == "ghost" {
				testassert.Equal(t, validations, 0)
			} else {
				testassert.Equal(t, validations, 1)
			}
			if tc.status == model.ResultSuccess {
				testassert.Equal(t, runs, 1)
			} else {
				testassert.Equal(t, runs, 0)
			}
			if tc.call == "ghost" || tc.missing {
				testassert.Equal(t, releases, 0)
			} else {
				testassert.Equal(t, releases, 1)
			}
		})
	}
}

func TestToolBindingCaptureErrors(t *testing.T) {
	lost := errors.New("captured activation lost")
	rewrite := ToolArgumentsHook{ID: "rewrite", ExtensionID: "ext", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
		return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":2}`)}, nil
	}}
	for _, afterValidation := range []bool{false, true} {
		t.Run(map[bool]string{false: "lookup", true: "check"}[afterValidation], func(t *testing.T) {
			runs := 0
			inv := InvocationExecution{ToolHooks: []ToolArgumentsHook{rewrite}}
			inv.LookupTool = func(string) (ToolBinding, bool, error) {
				if !afterValidation {
					return ToolBinding{}, false, lost
				}
				return ToolBinding{Identity: ToolImplementation{ID: "echo"}, Validate: func(json.RawMessage) error { return nil }, Run: func(context.Context, model.ToolCall) ToolOutcome {
					runs++
					return ToolOutcome{}
				}}, true, nil
			}
			if afterValidation {
				inv.CheckStage = func() CheckStage { return CheckStage{Error: lost} }
			}
			_, _, _, result, err := freshToolBoundary(t, testCapture(), inv, testToolCall("call-1"))
			testassert.NoError(t, err)
			testassert.Equal(t, result.Status, model.ResultError)
			testassert.Equal(t, result.Content, lost.Error())
			testassert.Equal(t, result.Evidence, ExecutionNotStarted)
			testassert.Equal(t, runs, 0)
			if afterValidation {
				testassert.Require(t, result.Implementation != nil && result.Implementation.ID == "echo" && string(result.NormalizedArguments) == `{"x":2}` && len(result.HookIDs) == 1 && result.HookIDs[0].HookID == "rewrite", "result = %+v", result)
			} else {
				testassert.Require(t, result.Implementation == nil && len(result.NormalizedArguments) == 0 && len(result.HookIDs) == 0, "result = %+v", result)
			}
		})
	}
}

func TestToolStageErrorsRemainValidationFailures(t *testing.T) {
	for _, stage := range []string{"lookup", "arguments", "check", "malformed check", "hook"} {
		t.Run(stage, func(t *testing.T) {
			failure := &EffectError{Err: errors.New("stage failed"), Interrupted: true}
			runs := 0
			inv := InvocationExecution{LookupTool: fixtureToolLookup(func(context.Context, model.ToolCall) ToolOutcome {
				runs++
				return ToolOutcome{}
			})}
			switch stage {
			case "lookup":
				inv.LookupTool = func(string) (ToolBinding, bool, error) { return ToolBinding{}, false, failure }
			case "arguments":
				inv.ArgumentStage = func() ArgumentStage { return ArgumentStage{Error: failure} }
			case "check":
				inv.CheckStage = func() CheckStage { return CheckStage{Error: failure} }
			case "malformed check":
				inv.CheckStage = func() CheckStage { return CheckStage{Hooks: []ToolCheckHook{{ID: "broken", ExtensionID: "ext"}}} }
			case "hook":
				inv.CheckStage = func() CheckStage {
					return CheckStage{Hooks: []ToolCheckHook{{ID: "check", ExtensionID: "ext", Run: func(context.Context, model.ToolCall) (*string, error) { return nil, failure }}}}
				}
			}
			_, _, _, result, err := freshToolBoundary(t, testCapture(), inv, testToolCall("call-1"))
			testassert.NoError(t, err)
			want := model.ResultError
			if stage == "hook" {
				want = model.ResultInterrupted
			}
			testassert.Equal(t, result.Status, want)
			testassert.Equal(t, result.Evidence, ExecutionNotStarted)
			testassert.Equal(t, runs, 0)
			testassert.Require(t, (result.Implementation != nil) == (stage == "check" || stage == "malformed check" || stage == "hook"), "result = %+v", result)
		})
	}
}

func TestOpaqueArgumentHookConsequences(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte("not json"), {0xff, 0xfe}, []byte(`[1,2]`)} {
		t.Run(base64.StdEncoding.EncodeToString(raw), func(t *testing.T) {
			hooks := []ToolArgumentsHook{
				{ID: "keep", ExtensionID: "ext", Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
					return ToolArgumentsResult{Arguments: model.CloneRaw(call.Arguments)}, nil
				}},
				{ID: "repair", ExtensionID: "ext", Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
					testassert.Require(t, bytes.Equal(call.Arguments, raw), "repair input = %q, want %q", call.Arguments, raw)
					return ToolArgumentsResult{Arguments: json.RawMessage(`{"fixed":true}`)}, nil
				}},
			}
			spy := &hookToolSpy{}
			exec := hookFixtureExecution(spy.tool, hooks...)
			_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, model.ToolCall{ID: "call-1", Name: "echo", Arguments: raw})
			testassert.NoError(t, err)
			testassert.Equal(t, results[0].Status, model.ResultSuccess)
			testassert.Require(t, string(results[0].NormalizedArguments) == `{"fixed":true}`, "effective arguments = %q", results[0].NormalizedArguments)
		})
	}
}

func TestToolLookupAfterHooksKeepsBoundImplementation(t *testing.T) {
	current := "before"
	var executed string
	hook := ToolArgumentsHook{ID: "replace", ExtensionID: "ext", Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		current = "after"
		return ToolArgumentsResult{Arguments: call.Arguments}, nil
	}}
	inv := InvocationExecution{
		ToolHooks: []ToolArgumentsHook{hook},
		LookupTool: func(string) (ToolBinding, bool, error) {
			bound := current
			return ToolBinding{
				Identity: ToolImplementation{ID: bound},
				Validate: func(json.RawMessage) error { return nil },
				Run: func(_ context.Context, call model.ToolCall) ToolOutcome {
					executed = bound
					return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
				},
			}, true, nil
		},
	}
	_, _, _, result, err := freshToolBoundary(t, testCapture(), inv, testToolCall("call-1"))
	testassert.NoError(t, err)
	testassert.Require(t, executed == "before" && result.Implementation != nil && result.Implementation.ID == "before", "executed %q result %+v", executed, result)

	hookCalls := 0
	inv.ToolHooks[0].Run = func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		hookCalls++
		return ToolArgumentsResult{Arguments: call.Arguments}, nil
	}
	inv.LookupTool = func(string) (ToolBinding, bool, error) { return ToolBinding{}, false, nil }
	_, _, _, result, err = freshToolBoundary(t, testCapture(), inv, testToolCall("call-1"))
	testassert.NoError(t, err)
	testassert.Equal(t, hookCalls, 0)
	testassert.Equal(t, result.Status, model.ResultError)
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
