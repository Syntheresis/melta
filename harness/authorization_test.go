package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestAuthorizationRunsAfterRewriteAndPreparation proves the stage order of
// one tool call: the rewrite hooks run first, the final arguments pass the
// bound implementation's schema, the check hooks see those final arguments,
// and only then does the implementation run.
func TestAuthorizationRunsAfterRewriteAndPreparation(t *testing.T) {
	var steps []string
	inv := InvocationExecution{
		ToolHooks: []ToolArgumentsHook{{ID: "rewrite", ExtensionID: "ext", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
			steps = append(steps, "rewrite")
			return ToolArgumentsResult{Arguments: json.RawMessage(`{ "x" : 2 }`)}, nil
		}}},
		LookupTool: func(string) (ToolBinding, bool, error) {
			return ToolBinding{
				Identity: ToolImplementation{ID: "echo"},
				Validate: func(json.RawMessage) error {
					steps = append(steps, "validate")
					return nil
				},
				Run: func(_ context.Context, call model.ToolCall) ToolOutcome {
					steps = append(steps, "run")
					if string(call.Arguments) != `{ "x" : 2 }` {
						t.Fatalf("run arguments %s", call.Arguments)
					}
					return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ran"}}
				},
			}, true, nil
		},
		CheckStage: func() CheckStage {
			return CheckStage{Hooks: []ToolCheckHook{{ID: "check", ExtensionID: "ext", Run: func(_ context.Context, call model.ToolCall) (*string, error) {
				steps = append(steps, "check")
				if string(call.Arguments) != `{ "x" : 2 }` {
					t.Fatalf("checked %s", call.Arguments)
				}
				return nil, nil
			}}}}
		},
	}
	exec := executionForInvocation(inv)
	_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
	if err != nil || results[0].Status != model.ResultSuccess || strings.Join(steps, ",") != "rewrite,validate,check,run" {
		t.Fatalf("steps=%v results=%+v err=%v", steps, results, err)
	}
	if string(results[0].NormalizedArguments) != `{ "x" : 2 }` {
		t.Fatalf("recorded arguments %s", results[0].NormalizedArguments)
	}
}

// TestInvalidArgumentsStopBeforeCheck proves that final arguments the bound
// implementation's schema refuses settle as an error result before any check
// hook runs, and that a non-object settles the same way.
func TestInvalidArgumentsStopBeforeCheck(t *testing.T) {
	for _, tc := range []struct {
		name string
		args json.RawMessage
		want string
	}{
		{name: "schema refusal", args: json.RawMessage(`{"x":2}`), want: "arguments outside the schema"},
		{name: "non-object", args: json.RawMessage(`[1]`), want: "not a JSON object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var checked int
			inv := InvocationExecution{
				LookupTool: func(string) (ToolBinding, bool, error) {
					return ToolBinding{
						Identity: ToolImplementation{ID: "echo"},
						Validate: func(json.RawMessage) error {
							if tc.name == "schema refusal" {
								return errors.New("arguments outside the schema")
							}
							return nil
						},
						Run: func(context.Context, model.ToolCall) ToolOutcome {
							t.Fatal("implementation ran before validation")
							return ToolOutcome{}
						},
					}, true, nil
				},
				CheckStage: func() CheckStage {
					return CheckStage{Hooks: []ToolCheckHook{{ID: "check", ExtensionID: "ext", Run: func(context.Context, model.ToolCall) (*string, error) {
						checked++
						return nil, nil
					}}}}
				},
			}
			call := testToolCall("call-1")
			call.Arguments = tc.args
			_, _, _, result, err := freshToolBoundary(t, testCapture(), inv, call)
			testassert.NoError(t, err)
			testassert.Equal(t, result.Status, model.ResultError)
			testassert.Check(t, strings.Contains(result.Content, tc.want), "content = %q, want %q", result.Content, tc.want)
			testassert.Equal(t, result.Evidence, ExecutionNotStarted)
			testassert.Equal(t, result.NormalizedArguments == nil, true)
			testassert.Equal(t, checked, 0)
		})
	}
}

// TestPreparedAuthorizationFrozenAndMixed proves the check hooks' owned copies
// and the outcomes of one mixed chain: a refusal settles only its call, a
// check error is an error result, the passing calls run, and mutating a hook's
// argument copy changes neither what runs nor what is recorded.
func TestPreparedAuthorizationFrozenAndMixed(t *testing.T) {
	var hooks, executions int
	var executedArguments []string
	capture := testCapture()
	admission := preparedExecutionFor(capture, func(context.Context, OperationAdmission) (Execution, error) { return Execution{}, nil })
	h, store, c, sessionID := newAdmittedHarness(t, context.Background(), admission)
	run := func(_ context.Context, call model.ToolCall) ToolOutcome {
		executedArguments = append(executedArguments, string(call.Arguments))
		if string(call.Arguments) == `{"x":3}` {
			return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultError, Content: "failed"}}
		}
		if string(call.Arguments) == `{"x":4}` || string(call.Arguments) == `{"x":5}` {
			return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "immediate"}}
		}
		executions++
		return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ran"}}
	}
	check := func(call model.ToolCall) {
		t.Helper()
		if call.Name != "echo" {
			t.Fatalf("unexpected call: %+v", call)
		}
	}
	inv := InvocationExecution{
		LookupTool: fixtureToolLookup(run),
		CheckStage: func() CheckStage {
			return CheckStage{Hooks: []ToolCheckHook{
				{ID: "one", ExtensionID: "ext", Run: func(_ context.Context, call model.ToolCall) (*string, error) {
					check(call)
					hooks++
					if string(call.Arguments) == `{"x":1}` || string(call.Arguments) == `{"x":5}` {
						reason := "restricted"
						return &reason, nil
					}
					if string(call.Arguments) == `{"x":6}` {
						return nil, errors.New("callback failed")
					}
					call.Arguments[0] = '!'
					return nil, nil
				}},
				{ID: "two", ExtensionID: "ext", Run: func(_ context.Context, call model.ToolCall) (*string, error) {
					check(call)
					hooks++
					if string(call.Arguments) != `{"x":2}` && string(call.Arguments) != `{"x":3}` && string(call.Arguments) != `{"x":4}` && string(call.Arguments) != `{"x":7}` {
						t.Fatalf("mutated args: %s", call.Arguments)
					}
					return nil, nil
				}},
			}}
		},
	}
	exec := executionForInvocation(inv)
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
	if hooks != 11 || executions != 2 {
		t.Fatalf("hooks=%d executions=%d", hooks, executions)
	}
	if strings.Join(executedArguments, ",") != `{"x":2},{"x":3},{"x":4},{"x":7}` {
		t.Fatalf("executed arguments = %v", executedArguments)
	}
}
