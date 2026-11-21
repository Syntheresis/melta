package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/model"
)

func TestAuthorizationRunsAfterRewriteAndPreparation(t *testing.T) {
	var steps []string
	inv := InvocationExecution{
		ToolHooks: []ToolArgumentsHook{{ID: "rewrite", ExtensionID: "ext", Mode: HookModePure, Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
			steps = append(steps, "rewrite")
			return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":2}`)}, nil
		}}},
		LookupTool: fixtureToolLookup(func(_ context.Context, call model.ToolCall) PreparedTool {
			steps = append(steps, "prepare")
			if string(call.Arguments) != `{"x":2}` {
				t.Fatalf("prepared %s", call.Arguments)
			}
			return PreparedTool{Permissions: fixturePermission, Execute: func(context.Context) ToolOutcome {
				steps = append(steps, "execute")
				return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ran"}}
			}}
		}, objectNormalize),
		BindLocation: func(string) (string, func() error, error) { return "/w", func() error { return nil }, nil },
		AuthorizationHooks: []AuthorizationHook{{Run: func(_ context.Context, action PreparedAction, _ func(string) (string, error)) (*string, error) {
			steps = append(steps, "hook")
			if string(action.Arguments) != `{"x":2}` {
				t.Fatalf("authorized %s", action.Arguments)
			}
			return nil, nil
		}}},
	}
	exec := executionForInvocation(inv)
	exec.Permissions = permissionEvaluatorFunc(func(_ context.Context, action PreparedAction) (PermissionDecision, error) {
		steps = append(steps, "policy")
		if string(action.Arguments) != `{"x":2}` {
			t.Fatalf("policy %s", action.Arguments)
		}
		return PermissionAllow, nil
	})
	_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
	if err != nil || results[0].Status != model.ResultSuccess || strings.Join(steps, ",") != "rewrite,prepare,policy,hook,execute" {
		t.Fatalf("steps=%v results=%+v err=%v", steps, results, err)
	}
}

func TestPreparedAuthorizationFrozenAndMixed(t *testing.T) {
	var hooks, executions int
	capture := testCapture()
	admission := preparedExecutionFor(capture, func(context.Context, OperationAdmission) (Execution, error) { return Execution{}, nil })
	h, store, c, sessionID := newAdmittedHarness(t, context.Background(), admission)
	prepare := func(_ context.Context, call model.ToolCall) PreparedTool {
		if string(call.Arguments) == `{"x":3}` {
			return PreparedTool{Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultError, Content: "failed"}}}
		}
		outcome := ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "immediate"}}
		p := PreparedTool{Permissions: []PermissionRequest{{Permission: "custom", Target: "one"}}}
		if string(call.Arguments) == `{"x":4}` || string(call.Arguments) == `{"x":5}` {
			p.Immediate = &outcome
		} else {
			p.Execute = func(context.Context) ToolOutcome {
				executions++
				return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ran"}}
			}
		}
		return p
	}
	check := func(action PreparedAction) {
		t.Helper()
		if action.ToolName != "echo" || action.CanonicalWorkspace != "/actual" || len(action.Permissions) != 1 || action.Permissions[0].Target != "one" {
			t.Fatalf("unexpected action: %+v", action)
		}
	}
	inv := InvocationExecution{
		LookupTool:   fixtureToolLookup(prepare, objectNormalize),
		BindLocation: func(string) (string, func() error, error) { return "/actual", func() error { return nil }, nil },
		AuthorizationHooks: []AuthorizationHook{
			{Run: func(_ context.Context, action PreparedAction, _ func(string) (string, error)) (*string, error) {
				check(action)
				hooks++
				if string(action.Arguments) == `{"x":1}` || string(action.Arguments) == `{"x":5}` {
					reason := "restricted"
					return &reason, nil
				}
				if string(action.Arguments) == `{"x":6}` {
					return nil, errors.New("callback failed")
				}
				action.Arguments[0] = '!'
				action.Permissions[0].Target = "mutated"
				return nil, nil
			}},
			{Run: func(_ context.Context, action PreparedAction, _ func(string) (string, error)) (*string, error) {
				check(action)
				hooks++
				if string(action.Arguments) != `{"x":2}` && string(action.Arguments) != `{"x":4}` && string(action.Arguments) != `{"x":7}` {
					t.Fatalf("mutated args: %s", action.Arguments)
				}
				return nil, nil
			}},
		},
	}
	exec := executionForInvocation(inv)
	exec.Permissions = permissionEvaluatorFunc(func(_ context.Context, action PreparedAction) (PermissionDecision, error) {
		if action.CanonicalWorkspace != "" || action.ToolName != "echo" || len(action.Permissions) != 1 || action.Permissions[0].Target != "one" {
			t.Fatalf("unexpected policy action: %+v", action)
		}
		action.Arguments[0] = '!'
		action.Permissions[0].Target = "mutated"
		return PermissionAllow, nil
	})
	var calls []model.ToolCall
	for i, args := range []string{`{"x":1}`, `{"x":2}`, `{"x":3}`, `{"x":4}`, `{"x":5}`, `{"x":6}`, `{"x":7}`} {
		calls = append(calls, model.ToolCall{ID: string(rune('a' + i)), Name: "echo", Arguments: json.RawMessage(args)})
	}
	results, err := runToolCalls(t, h, c, store, sessionID, context.Background(), testOpID, exec, capture, calls)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.ToolResultStatus{model.ResultDenied, model.ResultSuccess, model.ResultError, model.ResultSuccess, model.ResultDenied, model.ResultError, model.ResultSuccess}
	for i, result := range results {
		if result.Status != want[i] || string(result.NormalizedArguments) != string(calls[i].Arguments) {
			t.Fatalf("result %d = %+v", i, result)
		}
	}
	if hooks != 9 || executions != 2 {
		t.Fatalf("hooks=%d executions=%d", hooks, executions)
	}
}
