package harness

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// byteNormalize is the byte-preserving fixture normalizer: it accepts exactly
// one non-null JSON object and returns the caller's exact argument bytes, so
// every compaction or escaping visible downstream is attributable to the
// durable codec rather than the normalizer.
func byteNormalize(_ context.Context, call model.ToolCall) (json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(call.Arguments, &obj); err != nil || obj == nil {
		return nil, errors.New("arguments must be one non-null JSON object")
	}
	return model.CloneRaw(call.Arguments), nil
}

// hookToolSpy records each prepared call's identity and argument bytes.
type hookToolSpy struct {
	mu    sync.Mutex
	calls []model.ToolCall
	plan  func(model.ToolCall) PreparedTool
}

func (s *hookToolSpy) tool(_ context.Context, call model.ToolCall) PreparedTool {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
	if s.plan != nil {
		return s.plan(call)
	}
	return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}}
}

func (s *hookToolSpy) received() []model.ToolCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.ToolCall{}, s.calls...)
}

// newHookHarness admits one running Operation ("op-1") whose prepared
// execution is exactly the given value.
func newHookHarness(t *testing.T, exec Execution) (*Harness, *graphStorage, *coordinator, string) {
	t.Helper()
	store := emptyStore(t)
	prepared := PreparedExecution{Capture: testCapture(), Open: func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	}}
	h := newTestHarness(t, store, func(context.Context, PreparationRequest) (PreparedExecution, error) {
		return prepared, nil
	})
	session, err := h.CreateSession(context.Background(), CreateSessionRequest{Workspace: "/tmp/works", Inputs: testInputs("coder")})
	testassert.NoError(t, err)
	{
		_, disposition := mustAdmitWithoutExecution(t, h, session.Identity.SessionID, testOpID, admissionContent("hello"))
		testassert.Equal(t, disposition, DispositionAdmitted)
	}
	c := leaseCoordinator(t, h, session.Identity.SessionID)
	return h, store, c, session.Identity.SessionID
}

// hookFixtureExecution returns the hook fixtures' execution: its opener answers
// every Engine request with an invocation carrying the given preparation
// callback, normalizer, and selected argument hooks.
func hookFixtureExecution(toolFn func(context.Context, model.ToolCall) PreparedTool, normalize func(context.Context, model.ToolCall) (json.RawMessage, error), hooks ...ToolArgumentsHook) Execution {
	return Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
		return InvocationExecution{
			Model: func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				return completedTurnStream(), nil
			},
			LookupTool: fixtureToolLookup(toolFn, normalize), ToolHooks: hooks,
		}, nil
	}}
}

// TestToolArgumentHookChainSettlement proves the complete ordered hook chain:
// the first hook receives the original raw call arguments and every later one
// the preceding replacement bytes. The final value is normalized once before
// preparation; call identity and tool name stay bound, and the assistant entry
// keeps its original raw bytes. No hook entry commits and the Operation names
// no active effect while a hook runs.
func TestToolArgumentHookChainSettlement(t *testing.T) {
	var (
		mu             sync.Mutex
		normalizations int
		received       []string // hook-received argument bytes in chain order
		activeEffects  []string
	)
	normalize := func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
		mu.Lock()
		normalizations++
		mu.Unlock()
		return byteNormalize(ctx, call)
	}
	var (
		h         *Harness
		c         *coordinator
		sessionID string
	)
	observe := func() {
		rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
		if err != nil {
			t.Fatalf("read operation inside hook: %v", err)
		}
		if rec.State.ActiveEffect == nil {
			activeEffects = append(activeEffects, "")
		} else {
			activeEffects = append(activeEffects, string(rec.State.ActiveEffect.Kind))
		}
	}
	hooks := []ToolArgumentsHook{
		{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
			mu.Lock()
			received = append(received, string(call.Arguments))
			mu.Unlock()
			observe()
			return ToolArgumentsResult{Arguments: json.RawMessage(`{ "x": "<" }`)}, nil
		}},
		{ID: "cap.hook.two", ExtensionID: "cap-a", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
			mu.Lock()
			received = append(received, string(call.Arguments))
			mu.Unlock()
			observe()
			return ToolArgumentsResult{Arguments: json.RawMessage(`{"y":"&>"}`)}, nil
		}},
	}
	spy := &hookToolSpy{}
	exec := hookFixtureExecution(spy.tool, normalize, hooks...)
	call := model.ToolCall{ID: "call-1", Name: "echo", Arguments: json.RawMessage(` {"x": 1} `)}
	admission := preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return Execution{}, nil
	})
	h, store, c, sessionID := newAdmittedHarness(t, context.Background(), admission)
	open := exec.OpenInvocation
	exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
		inv, err := open(ctx, request)
		if err != nil {
			return inv, err
		}
		inv.Model = firstResponseModel(inv.Model, []model.ToolCall{call})
		return inv, nil
	}
	prepared := preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})
	testassert.NoError(t, executeDirect(h, c, h.ctx, testOpID, prepared))
	results := toolCallResults(t, store, sessionID, []model.ToolCall{call})
	got := model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}
	testassert.Equal(t, got, (model.ToolResult{CallID: "call-1", Status: model.ResultSuccess, Content: "done"}))
	// The bound tool normalizes the final hook consequence exactly once.
	mu.Lock()
	count, chain := normalizations, append([]string{}, received...)
	mu.Unlock()
	testassert.Equal(t, count, 1)
	testassert.Require(t, len(chain) == 2 && chain[0] == ` {"x": 1} ` && chain[1] == `{ "x": "<" }`, "hook chain received %q, want the raw arguments then the replacement bytes", chain)
	preparedCalls := spy.received()
	testassert.Require(t, len(preparedCalls) == 1 && preparedCalls[0].ID == "call-1" && preparedCalls[0].Name == "echo" && string(preparedCalls[0].Arguments) == `{"y":"&>"}`, "preparer received %+v, want the final replacement bytes under the bound identity", preparedCalls)
	testassert.Require(t, len(activeEffects) == 2 && activeEffects[0] == "" && activeEffects[1] == "", "active effects inside the hooks = %v, want none", activeEffects)

	// The assistant entry retains the original raw bytes.
	graph, verr := validateFixture(t, store, sessionID)
	testassert.NoError(t, verr)
	testassert.Equal(t, len(graph.Entries), 5)
	for _, entry := range graph.Entries {
		if entry.Assistant == nil || len(entry.Assistant.ToolCalls) == 0 {
			continue
		}
		record := entry.Assistant.ToolCalls[0]
		raw, derr := base64.StdEncoding.DecodeString(record.ArgumentsBase64)
		testassert.Require(t, derr == nil && string(raw) == ` {"x": 1} `, "assistant raw arguments = %q err %v, want the original bytes retained", raw, derr)
	}

	rec, rerr := h.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, rerr)
	testassert.Require(t, rec.State.ActiveEffect == nil && len(rec.State.PendingToolCalls) == 0 && rec.State.Terminal != nil, "operation state = %+v, want the effect cleared, the call settled and the run retired", rec.State)
	messages, merr := fixtureProjection(h, c)(context.Background())
	testassert.NoError(t, merr)
	for _, msg := range messages {
		for _, part := range msg.Content {
			testassert.Require(t, part.Text != `{"y":"\u0026\u003e"}`, "hook tool arguments projected as conversation content")
		}
	}

}

// TestToolArgumentHookInterruptionRules proves the interruption rows fixed
// for argument hooks: an explicitly interrupted callback and an untyped
// cancellation under a done call context interrupt, cancellation stops the
// chain before its next hook, and a cancellation-shaped error under a live
// context is an ordinary hook error.
func TestToolArgumentHookInterruptionRules(t *testing.T) {
	t.Run("explicit interruption during run interrupts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		hook := ToolArgumentsHook{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(ctx context.Context, _ model.ToolCall) (ToolArgumentsResult, error) {
			cancel() // the execution context dies during Run
			return ToolArgumentsResult{}, &EffectError{Err: ctx.Err(), Interrupted: true}
		}}
		exec := hookFixtureExecution(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize, hook)
		_, store, _, sessionID, results, err := freshExecutionToolCallsCtx(t, ctx, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		got := model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}
		testassert.Equal(t, got, (model.ToolResult{CallID: "call-1", Status: model.ResultInterrupted, Content: interruptedToolResultContent}))
		graph, verr := validateFixture(t, store, sessionID)
		testassert.NoError(t, verr)
		testassert.Equal(t, len(graph.Entries), 5)
	})

	t.Run("cancellation stops the chain before the next hook", func(t *testing.T) {
		secondRan := 0
		ctx, cancel := context.WithCancel(context.Background())
		exec := hookFixtureExecution(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize,
			ToolArgumentsHook{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				cancel() // the execution context dies during the successful hook's Run
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"first":true}`)}, nil
			}},
			ToolArgumentsHook{ID: "cap.hook.two", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				secondRan++
				return ToolArgumentsResult{Arguments: json.RawMessage(`{}`)}, nil
			}},
		)
		_, store, _, sessionID, results, err := freshExecutionToolCallsCtx(t, ctx, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		got := model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}
		testassert.Equal(t, got, (model.ToolResult{CallID: "call-1", Status: model.ResultInterrupted, Content: interruptedToolResultContent}))
		testassert.Equal(t, secondRan, 0)
		graph, verr := validateFixture(t, store, sessionID)
		testassert.NoError(t, verr)
		testassert.Equal(t, len(graph.Entries), 5)
	})

	t.Run("cancellation-shaped error without a done context is a hook error", func(t *testing.T) {
		hook := ToolArgumentsHook{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
			return ToolArgumentsResult{}, context.Canceled // no done execution context: an ordinary hook error
		}}
		exec := hookFixtureExecution(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize, hook)
		_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		got := model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}
		testassert.Require(t, got.Status == model.ResultError && got.Content == "context canceled", "tool result = %+v, want the validation error with the hook's diagnostic", got)
	})

	t.Run("cancellation-shaped error after a done call context interrupts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		hook := ToolArgumentsHook{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(ctx context.Context, _ model.ToolCall) (ToolArgumentsResult, error) {
			cancel() // the execution context dies, then the hook returns its cancellation
			return ToolArgumentsResult{}, ctx.Err()
		}}
		exec := hookFixtureExecution(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize, hook)
		_, store, _, sessionID, results, err := freshExecutionToolCallsCtx(t, ctx, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		got := model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}
		testassert.Equal(t, got, (model.ToolResult{CallID: "call-1", Status: model.ResultInterrupted, Content: interruptedToolResultContent}))
		graph, verr := validateFixture(t, store, sessionID)
		testassert.NoError(t, verr)
		testassert.Equal(t, len(graph.Entries), 5)
	})
}

// TestToolArgumentHookFailureRules checks hook errors and invalid replacements
// stop the chain, while normalizer failures happen after successful hooks.
// Both settle only the affected call and leave later calls eligible.
func TestToolArgumentHookFailureRules(t *testing.T) {
	cases := []struct {
		name      string
		run       func(context.Context, model.ToolCall) (ToolArgumentsResult, error)
		normalize func(context.Context, model.ToolCall) (json.RawMessage, error)
		want      model.ToolResult
		wantLater int
	}{
		{
			name: "returned error carries the bounded diagnostic",
			run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{}, errors.New("hook broke")
			},
			normalize: objectNormalize,
			want:      model.ToolResult{CallID: "call-1", Status: model.ResultError, Content: "hook broke"},
			wantLater: 0,
		},
		{
			name: "cancellation-shaped failure leaves later sibling eligible",
			run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{}, &EffectError{Err: context.Canceled}
			},
			normalize: objectNormalize,
			want:      model.ToolResult{CallID: "call-1", Status: model.ResultError, Content: "context canceled"},
			wantLater: 0,
		},
		{
			name: "an empty-text error keeps valid evidence and settles the call",
			run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{}, errors.New("")
			},
			normalize: objectNormalize,
			want:      model.ToolResult{CallID: "call-1", Status: model.ResultError, Content: invalidToolResultContent},
			wantLater: 0,
		},
		{
			name: "invalid returned value is the internal-validation error",
			run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`[1,2]`)}, nil
			},
			normalize: objectNormalize,
			want:      model.ToolResult{CallID: "call-1", Status: model.ResultError, Content: invalidToolResultContent},
			wantLater: 0,
		},
		{
			name: "normalizer rejection keeps its useful diagnostic",
			run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"a":1}`)}, nil
			},
			normalize: func(context.Context, model.ToolCall) (json.RawMessage, error) {
				return nil, errors.New("no default for a")
			},
			want:      model.ToolResult{CallID: "call-1", Status: model.ResultError, Content: "no default for a"},
			wantLater: 1,
		},
		{
			name: "malformed normalizer outcome is the internal-validation error",
			run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"a":1}`)}, nil
			},
			normalize: func(context.Context, model.ToolCall) (json.RawMessage, error) { return json.RawMessage(`[1,2]`), nil },
			want:      model.ToolResult{CallID: "call-1", Status: model.ResultError, Content: invalidToolResultContent},
			wantLater: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var later int
			hooks := []ToolArgumentsHook{
				{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
					if call.ID == "call-1" { // only the first call's chain fails; a later call runs its own chain
						return tc.run(ctx, call)
					}
					return ToolArgumentsResult{Arguments: json.RawMessage(`{}`)}, nil
				}},
				{ID: "cap.hook.two", ExtensionID: "cap-a", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
					if call.ID == "call-1" {
						later++
					}
					return ToolArgumentsResult{Arguments: json.RawMessage(`{}`)}, nil
				}},
			}
			spy := &hookToolSpy{}
			normalize := tc.normalize
			normalizeFor := func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) { // the case's normalizer only ever judges the failing call
				if call.ID == "call-1" {
					return normalize(ctx, call)
				}
				return objectNormalize(ctx, call)
			}
			exec := hookFixtureExecution(spy.tool, normalizeFor, hooks...)
			_, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"), testToolCall("call-2"))
			testassert.NoError(t, err)
			first := model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}
			testassert.Equal(t, first, tc.want)
			testassert.Equal(t, later, tc.wantLater)
			// Later calls still run: call-2 executes after the failed chain.
			laterResult := model.ToolResult{CallID: results[1].ToolCallID, Status: results[1].Status, Content: results[1].Content}
			testassert.Require(t, laterResult.Status == model.ResultSuccess, "later call result = %+v, want success", laterResult)
			prepareCalls := spy.received()
			testassert.Require(t, len(prepareCalls) == 1 && prepareCalls[0].ID == "call-2", "preparer calls = %+v, want only the later call", prepareCalls)
			{
				_, err := validateFixture(t, store, sessionID)
				testassert.NoError(t, err)
			}
		})
	}
}

// TestToolArgumentHookDenialSettlesAfterHooks proves argument hooks may have
// settled before a final tool permission denial: the denial forbids the
// concrete tool and never rolls back the separate hook effects, while the
// allowed sibling executes with the final committed value.
func TestToolArgumentHookDenialSettlesAfterHooks(t *testing.T) {
	hook := ToolArgumentsHook{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
		return ToolArgumentsResult{Arguments: json.RawMessage(`{"repaired":true}`)}, nil
	}}
	buildExec := func(plan func(context.Context, model.ToolCall) PreparedTool) Execution {
		return hookFixtureExecution(plan, objectNormalize, hook)
	}

	t.Run("denial forbids the concrete start", func(t *testing.T) {
		var executed int
		exec := buildExec(func(ctx context.Context, call model.ToolCall) PreparedTool {
			return PreparedTool{ // an executor plan the authorization hook refuses
				Permissions: fixturePermission,
				Execute: func(context.Context) ToolOutcome {
					executed++
					return ToolOutcome{Evidence: ExecutionObserved, Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ran"}}
				},
			}
		})
		open := exec.OpenInvocation
		exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
			inv, err := open(ctx, request)
			if err != nil {
				return inv, err
			}
			inv.BindLocation = func(string) (string, func() error, error) { return "/w", func() error { return nil }, nil }
			inv.AuthorizationHooks = []AuthorizationHook{{Run: func(context.Context, PreparedAction, func(string) (string, error)) (*string, error) {
				reason := "restricted"
				return &reason, nil
			}}}
			return inv, nil
		}
		_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		testassert.Equal(t, model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}, (model.ToolResult{CallID: "call-1", Status: model.ResultDenied, Content: "restricted"}))
		testassert.Equal(t, executed, 0)
	})

	t.Run("allowed sibling executes with the final committed value", func(t *testing.T) {
		spy := &hookToolSpy{}
		exec := buildExec(spy.tool)
		_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		testassert.Require(t, results[0].Status == model.ResultSuccess, "tool result = %+v, want success", results[0])
		prepared := spy.received()
		testassert.Require(t, len(prepared) == 1 && string(prepared[0].Arguments) == `{"repaired":true}`, "preparer received %+v, want the final committed replacement", prepared)
	})
}

// TestToolArgumentHookAdvertisementGate proves the one advertisement gate
// precedes every hook: a committed call whose tool name is absent from the
// Engine-supplied per-call names of the producing request settles the
// unavailable-tool error and never runs a hook — a capture advertising that
// name grants it nothing — while a names-complete call proceeds past the gate
// into its hook chain and plan.
func TestToolArgumentHookAdvertisementGate(t *testing.T) {
	ran := 0
	hook := ToolArgumentsHook{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
		ran++
		return ToolArgumentsResult{Arguments: json.RawMessage(`{}`)}, nil
	}}
	prepared := 0
	exec := hookFixtureExecution(func(ctx context.Context, call model.ToolCall) PreparedTool {
		prepared++
		return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ran"}}}
	}, objectNormalize, hook)
	ghost := model.ToolCall{ID: "call-1", Name: "ghost", Arguments: json.RawMessage(`{}`)}

	// The producing request advertises only echo: the ghost call settles the
	// unavailable-tool error and never runs a hook or preparer.
	_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, ghost)
	testassert.NoError(t, err)
	testassert.Equal(t, model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}, (model.ToolResult{CallID: "call-1", Status: model.ResultError, Content: `Tool "ghost" is not available.`}))
	testassert.Equal(t, ran, 0)
	testassert.Equal(t, prepared, 0)

	// A capture whose request advertises ghost lets the same call past the gate
	// into its hook chain and plan.
	ghostCapture := testCapture()
	ghostCapture.Tools = append(ghostCapture.Tools, model.ToolDefinition{Name: "ghost", Description: "ghost tool", Parameters: json.RawMessage(`{"type":"object"}`)})
	_, _, _, _, results, err = freshExecutionToolCalls(t, ghostCapture, exec, ghost)
	testassert.NoError(t, err)
	testassert.Require(t, results[0].Status == model.ResultSuccess && results[0].Content == "ran", "names-complete call = %+v, want it past the gate into its hook chain and plan", results[0])
	testassert.Equal(t, ran, 1)
	testassert.Equal(t, prepared, 1)
}

// TestToolArgumentHookInvalidOpenedExecutionRejectsHooks proves the opened
// invocation requires a non-empty unique ID and a non-nil run function for
// every argument hook, closing before rejection and settling the ordinary
// failure.
func TestToolArgumentHookInvalidOpenedExecutionRejectsHooks(t *testing.T) {
	closed := 0
	for _, tc := range []struct {
		name  string
		hooks []ToolArgumentsHook
	}{
		{"empty id", []ToolArgumentsHook{{Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) { return ToolArgumentsResult{}, nil }}}},
		{"nil run", []ToolArgumentsHook{{ID: "cap.hook.one"}}},
		{"duplicate id", []ToolArgumentsHook{
			{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) { return ToolArgumentsResult{}, nil }},
			{ID: "cap.hook.one", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) { return ToolArgumentsResult{}, nil }},
		}},
		{"empty extension identity", []ToolArgumentsHook{
			{ID: "cap.hook.one", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) { return ToolArgumentsResult{}, nil }},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, c, sessionID, prepared, _ := newOpenerHarness(t, func(context.Context, OperationAdmission) (Execution, error) {
				modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
					return completedTurnStream(), nil
				}
				return Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
					return InvocationExecution{
						Model:      modelFn,
						LookupTool: fixtureToolLookup(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize),
						ToolHooks:  tc.hooks,
						Close: func() error {
							closed++
							return nil
						},
					}, nil
				}}, nil
			})
			{
				err := executeDirect(h, c, h.ctx, testOpID, prepared)
				testassert.Require(t, err != nil, "execute = nil, want the invalid-hook rejection")
			}
			testassert.Equal(t, closed, 1)
			closed = 0
			rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
			testassert.NoError(t, err)
			testassert.Equal(t, rec.State.Status, OperationFailure)
		})
	}
}

// requestHookFixtureExecution supplies one opened conversation invocation
// carrying the given request hooks over the fixture model and tool callbacks.
func requestHookFixtureExecution(modelFn func(context.Context, model.Request, model.Extra) (model.Stream, error), toolFn func(context.Context, model.ToolCall) PreparedTool, hooks ...RequestHook) Execution {
	if modelFn == nil {
		modelFn = func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return completedTurnStream(), nil
		}
	}
	if toolFn == nil {
		toolFn = func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }
	}
	return Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
		return InvocationExecution{Model: modelFn, LookupTool: fixtureToolLookup(toolFn, objectNormalize), RequestStage: fixtureRequestStage(hooks...)}, nil
	}}
}

// runRequestHookRound drives one real pipeline round over the already-admitted
// Operation and returns the first conversation model request it dispatched.
func runRequestHookRound(t *testing.T, h *Harness, c *coordinator, exec Execution, capture ExecutionCapture) (model.Request, error) {
	t.Helper()
	return runAdmittedExecution(t, h, c, context.Background(), testOpID, capture, exec)
}

func graphSettlementOf(t *testing.T, store *graphStorage, sessionID string) *operationSettlementEntry {
	t.Helper()
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var out *operationSettlementEntry
	count := 0
	for _, entry := range graph.Entries {
		if entry.Settlement != nil {
			count++
			out = entry.Settlement
		}
	}
	testassert.Equal(t, count, 1)
	return out
}

// TestRequestHookChainSettlement proves the request-boundary positive
// pipeline row: the earlier hook's contribution feeds the next hook, both
// accepted identities land in request evidence in hook order, the dispatch
// consumes the final contribution, and no hook entry commits or active effect
// exists while a hook runs.
func TestRequestHookChainSettlement(t *testing.T) {
	var saw RequestHookInput
	changed := "final prompt"
	addition, aerr := model.NewMessage(model.Message{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "contributed"}}})
	testassert.NoError(t, aerr)
	var (
		h         *Harness
		sessionID string
		active    []string
	)
	observe := func() {
		rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
		if err != nil {
			t.Fatalf("read operation inside hook: %v", err)
		}
		if rec.State.ActiveEffect == nil {
			active = append(active, "")
		} else {
			active = append(active, string(rec.State.ActiveEffect.Kind))
		}
	}
	first := RequestHook{ID: "req.pure", ExtensionID: "req-ext", Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		observe()
		prompt := "pure prompt " + in.Prompt
		return RequestConsequence{Prompt: &prompt}, nil
	}}
	second := RequestHook{ID: "req.effectful", ExtensionID: "req-ext", Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		observe()
		saw = in
		return RequestConsequence{Prompt: &changed, Edits: appendRequestFixture(in, addition)}, nil
	}}
	exec := requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		return completedTurnStream(), nil
	}, nil, first, second)
	var store *graphStorage
	var c *coordinator
	h, store, c, sessionID = newHookHarness(t, exec)
	sent, err := runRequestHookRound(t, h, c, exec, testCapture())
	testassert.NoError(t, err)

	// The hook input carried the effective view the earlier contribution
	// produced, never a reread projection.
	testassert.Require(t, saw.RequestID != "" && saw.BoundaryEntryID != "", "hook input = %+v, want the bound request identity and canonical boundary", saw)
	testassert.Require(t, saw.Prompt == "pure prompt system", "hook input prompt = %q, want the view after the earlier contribution", saw.Prompt)
	testassert.Require(t, len(active) == 2 && active[0] == "" && active[1] == "", "active effects inside the hooks = %v, want none", active)

	// The dispatch request consumed both contributions exactly once.
	testassert.Require(t, len(sent.Messages) >= 2 && sent.Messages[0].TextContent() == "final prompt", "dispatch prompt = %q, want the replacement", sent.Messages[0].TextContent())
	last := sent.Messages[len(sent.Messages)-1]
	testassert.Require(t, last.TextContent() == "contributed", "dispatch tail = %q, want the contribution applied to the rebuilt view", last.TextContent())

	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, assistant.Evidence != nil && len(assistant.Evidence.HookIDs) == 2 &&
		assistant.Evidence.HookIDs[0].HookID == "req.pure" && assistant.Evidence.HookIDs[1].HookID == "req.effectful",
		"assistant evidence hook identities = %+v, want both contributors in hook order", assistant.Evidence.HookIDs)
	testassert.Require(t, assistant.Evidence.Prompt == "final prompt" && len(assistant.Evidence.Edits) == len(saw.Messages)+1,
		"assistant evidence = %+v, want the final effective prompt and contribution", assistant.Evidence)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	testassert.Equal(t, len(graph.Entries), 3)
}

// TestRequestHookFailureSettlesTerminalOnce proves the failed-hook
// settlement owner through the full execution pipeline: the hook error
// settles one failure terminal carrying its nonempty diagnostic, the
// conversation request is never dispatched, and no second terminal write or
// replayed effect survives a recovery pass.
func TestRequestHookFailureSettlesTerminalOnce(t *testing.T) {
	ran := 0
	hookErr := errors.New("request binding broke")
	hook := RequestHook{ID: "req.effectful", ExtensionID: "req-ext", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
		ran++
		return RequestConsequence{}, hookErr
	}}
	h, store, c, sessionID, prepared, cancel := newExecutionHarness(t, func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		t.Fatal("a failed request hook dispatched a conversation request")
		return nil, nil
	}, func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} })
	defer cancel()
	// The opened invocation carries the failing request hook.
	prepared.Open = func(_ context.Context, _ OperationAdmission) (Execution, error) {
		return requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			t.Fatal("a failed request hook dispatched a conversation request")
			return nil, nil
		}, func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, hook), nil
	}
	before, err := store.ReadEntries(context.Background(), sessionID, 0)
	testassert.NoError(t, err)
	{
		err := executeDirect(h, c, h.ctx, testOpID, prepared)
		testassert.Require(t, err != nil && errors.Is(err, hookErr), "execute = %v, want the hook cause returned verbatim", err)
	}
	testassert.Equal(t, ran, 1)
	settlement := graphSettlementOf(t, store, sessionID)
	testassert.Require(t, settlement.Status == OperationFailure && settlement.Detail == "request binding broke" &&
		settlement.RequestID == "" && settlement.Evidence == nil && len(settlement.Facts) == 0,
		"settlement = %+v, want exactly one failure terminal with the hook diagnostic", settlement)
	after, err := store.ReadEntries(context.Background(), sessionID, 0)
	testassert.NoError(t, err)
	testassert.Require(t, len(after) == len(before)+1, "entry tail = %d new entries, want exactly the one failure settlement: %d -> %d", len(after)-len(before), len(before), len(after))
	{
		err := Recover(context.Background(), store)
		testassert.NoError(t, err)
	}
	after2, err := store.ReadEntries(context.Background(), sessionID, 0)
	testassert.NoError(t, err)
	testassert.Equal(t, len(after2), len(after))
}

func TestArgumentHookPureChainRepairRestoreAndRefusal(t *testing.T) {
	t.Run("pure repair then exact restore adds no transactions", func(t *testing.T) {
		spy := &hookToolSpy{}
		exec := hookFixtureExecution(spy.tool, objectNormalize,
			ToolArgumentsHook{ID: "cap.pure.one", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":9}`)}, nil
			}},
			ToolArgumentsHook{ID: "cap.pure.two", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":1}`)}, nil // the exact original bytes
			}},
		)
		_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		testassert.Require(t, results[0].Status == model.ResultSuccess, "tool result = %+v, want the restored chain to run the call", results[0])
		prepared := spy.received()
		testassert.Require(t, len(prepared) == 1 && string(prepared[0].Arguments) == `{"x":1}`, "preparer received %+v, want the restored arguments normalized", prepared)
		testassert.Require(t, string(results[0].NormalizedArguments) == `{"x":1}` &&
			len(results[0].HookIDs) == 2 && results[0].HookIDs[0].HookID == "cap.pure.one" && results[0].HookIDs[1].HookID == "cap.pure.two",
			"tool result provenance = %+v %s, want the original consumed value and every accepted pure identity in order", results[0].NormalizedArguments, results[0].HookIDs)
	})
	t.Run("pure refusal settles the call with no durable effect", func(t *testing.T) {
		refusal := errors.New("pure hook refused")
		spy := &hookToolSpy{}
		exec := hookFixtureExecution(spy.tool, objectNormalize,
			ToolArgumentsHook{ID: "cap.pure.refuse", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{}, refusal
			}},
			ToolArgumentsHook{ID: "cap.pure.after", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				t.Fatal("a hook ran after the chain's refusal")
				return ToolArgumentsResult{}, nil
			}},
		)
		_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		testassert.Require(t, results[0].Status == model.ResultError && strings.Contains(results[0].Content, "pure hook refused"), "tool result = %+v, want the validation failure carrying the refusal", results[0])
		testassert.Require(t, len(spy.received()) == 0, "preparer ran for %+v, want no preparation after the refusal", spy.received())
		testassert.Require(t, results[0].Status == model.ResultError && strings.Contains(results[0].Content, "pure hook refused") && len(results[0].HookIDs) == 0,
			"committed tool result = %+v, want the refusal diagnostic settled once with no hook provenance", results[0])
	})
	t.Run("pure-only repair of an invalid original normalizes once and adds no transactions", func(t *testing.T) {
		var (
			mu            sync.Mutex
			attempts      []string // normalizer argument bytes per attempt in call order
			failures      int
			repairSteps   int
			hooklessSteps int
		)
		normalize := func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
			normalized, err := objectNormalize(ctx, call)
			mu.Lock()
			attempts = append(attempts, string(call.Arguments))
			if err != nil {
				failures++
			}
			mu.Unlock()
			return normalized, err
		}
		broken := testToolCall("call-1")
		broken.Arguments = json.RawMessage("not json")
		var preparedCalls []model.ToolCall
		prepareArrived := make(chan struct{}, 1)
		prepareGate := make(chan struct{})
		plan := func(ctx context.Context, call model.ToolCall) PreparedTool {
			preparedCalls = append(preparedCalls, call)
			prepareArrived <- struct{}{}
			<-prepareGate
			return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}}
		}
		exec := hookFixtureExecution(plan, normalize,
			ToolArgumentsHook{ID: "cap.pure.repair", ExtensionID: "cap-a", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
				testassert.Equal(t, string(call.Arguments), "not json")
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":2}`)}, nil
			}},
		)
		store := emptyStore(t)
		admission := preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return Execution{}, nil
		})
		h := newTestHarness(t, store, func(context.Context, PreparationRequest) (PreparedExecution, error) {
			return admission, nil
		})
		session, serr := h.CreateSession(context.Background(), CreateSessionRequest{Workspace: "/tmp/works", Inputs: testInputs("coder")})
		testassert.NoError(t, serr)
		c := leaseCoordinator(t, h, session.Identity.SessionID)
		sessionID := session.Identity.SessionID
		mustAdmitWithoutExecution(t, h, sessionID, testOpID, admissionContent("hello"))
		store.txHook = func(string) error { mu.Lock(); repairSteps++; mu.Unlock(); return nil }
		open := exec.OpenInvocation
		first := true
		exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
			inv, ierr := open(ctx, request)
			if ierr != nil {
				return inv, ierr
			}
			if request.Purpose == InvocationPurposeConversation {
				inv.Model = func(context.Context, model.Request, model.Extra) (model.Stream, error) {
					if first {
						first = false
						return completedTurnStream(broken), nil
					}
					return completedTurnStream(), nil
				}
			}
			return inv, nil
		}
		done := make(chan error, 1)
		go func() {
			done <- executeDirect(h, c, h.ctx, testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
				return exec, nil
			}))
		}()
		select {
		case <-prepareArrived:
		case err := <-done:
			t.Fatalf("execution ended before preparation: %v", err)
		}
		{
			mu.Lock()
			testassert.Require(t, len(attempts) == 1 && attempts[0] == `{"x":2}` && failures == 0,
				"normalization attempts = %v with %d failures, want one accepted replacement", attempts, failures)
			mu.Unlock()
			assistant := compositionAssistant(t, store, sessionID)
			testassert.Require(t, len(assistant.ToolCalls) == 1, "assistant call record = %+v", assistant.ToolCalls)
			raw, derr := base64.StdEncoding.DecodeString(assistant.ToolCalls[0].ArgumentsBase64)
			testassert.Require(t, derr == nil && string(raw) == "not json", "assistant raw bytes = %q, want the source arguments preserved verbatim", raw)
		}
		close(prepareGate)
		testassert.NoError(t, <-done)
		{
			mu.Lock()
			testassert.Require(t, len(attempts) == 1 && failures == 0 && attempts[0] == `{"x":2}`,
				"normalization attempts = %v with %d failures, want one final normalization", attempts, failures)
			mu.Unlock()
		}
		testassert.Require(t, len(preparedCalls) == 1 && string(preparedCalls[0].Arguments) == `{"x":2}`, "preparer received %+v, want the accepted normalized replacement", preparedCalls)
		graph, gerr := validateFixture(t, store, sessionID)
		testassert.NoError(t, gerr)
		settled := 0
		for _, entry := range graph.Entries {
			if entry.ToolResult == nil {
				continue
			}
			settled++
			testassert.Require(t, string(entry.ToolResult.NormalizedArguments) == `{"x":2}` &&
				len(entry.ToolResult.HookIDs) == 1 && entry.ToolResult.HookIDs[0].HookID == "cap.pure.repair" &&
				entry.ToolResult.Content == "done" && entry.ToolResult.Evidence == ExecutionObserved,
				"tool result = %+v, want the repaired value and the pure contributor identity over the executed outcome", entry.ToolResult)
		}
		testassert.Equal(t, settled, 1)
		// The same fixture without hooks settles the valid original; equal
		// durable mutation-step counts show the pure repair added no transaction.
		baseline := testToolCall("call-1")
		spy2 := &hookToolSpy{}
		exec2 := hookFixtureExecution(spy2.tool, objectNormalize)
		_, _, _, _, bres, berr := freshExecutionToolCallsConfig(t, context.Background(), testCapture(), exec2, func(store *graphStorage) {
			store.txHook = func(string) error { mu.Lock(); hooklessSteps++; mu.Unlock(); return nil }
		}, baseline)
		testassert.NoError(t, berr)
		testassert.Require(t, bres[0].Status == model.ResultSuccess, "hookless tool result = %+v, want the valid call settled", bres[0])
		mu.Lock()
		testassert.Require(t, repairSteps > 0 && repairSteps == hooklessSteps,
			"durable mutation steps: repaired pure chain %d, hookless valid settlement %d, want equal counts", repairSteps, hooklessSteps)
		mu.Unlock()
	})
}

func TestRequestHookInvalidStageRejectsHooks(t *testing.T) {
	closed := 0
	for _, tc := range []struct {
		name  string
		hooks []RequestHook
	}{
		{"empty id", []RequestHook{{Run: func(context.Context, RequestHookInput) (RequestConsequence, error) { return RequestConsequence{}, nil }}}},
		{"nil run", []RequestHook{{ID: "req.hook.one"}}},
		{"duplicate id", []RequestHook{
			{ID: "req.hook.one", ExtensionID: "req-ext", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) { return RequestConsequence{}, nil }},
			{ID: "req.hook.one", ExtensionID: "req-ext", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) { return RequestConsequence{}, nil }},
		}},
		{"empty extension identity", []RequestHook{{ID: "req.hook.one", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) { return RequestConsequence{}, nil }}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, c, sessionID, prepared, _ := newOpenerHarness(t, func(context.Context, OperationAdmission) (Execution, error) {
				modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
					return completedTurnStream(), nil
				}
				return Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
					return InvocationExecution{
						Model:        modelFn,
						LookupTool:   fixtureToolLookup(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize),
						RequestStage: fixtureRequestStage(tc.hooks...),
						Close: func() error {
							closed++
							return nil
						},
					}, nil
				}}, nil
			})
			{
				err := executeDirect(h, c, h.ctx, testOpID, prepared)
				testassert.Require(t, err != nil && strings.Contains(err.Error(), "request stage requires"), "execute = %v, want the invalid-stage rejection", err)
			}
			testassert.Equal(t, closed, 1)
			closed = 0
			rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
			testassert.NoError(t, err)
			testassert.Equal(t, rec.State.Status, OperationFailure)
		})
	}
}

// TestRequestHookEmptyErrorTextSettlesWithGenericDetail proves the terminal
// detail floor through the full execution pipeline: a hook failure whose own
// error text is empty still settles exactly one failure terminal carrying the
// fixed nonempty standard detail, while the pipeline returns the original
// (empty-text) cause verbatim.
func TestRequestHookEmptyErrorTextSettlesWithGenericDetail(t *testing.T) {
	empty := errors.New("")
	hook := RequestHook{ID: "req.effectful", ExtensionID: "req-ext", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
		return RequestConsequence{}, empty
	}}
	h, store, c, sessionID, prepared, cancel := newExecutionHarness(t, func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		t.Fatal("a failed request hook dispatched a conversation request")
		return nil, nil
	}, func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} })
	defer cancel()
	prepared.Open = func(_ context.Context, _ OperationAdmission) (Execution, error) {
		return requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			t.Fatal("a failed request hook dispatched a conversation request")
			return nil, nil
		}, func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, hook), nil
	}
	{
		err := executeDirect(h, c, h.ctx, testOpID, prepared)
		testassert.Require(t, err != nil && err.Error() == "", "execute = %v, want the original empty-text cause verbatim", err)
	}
	settlement := graphSettlementOf(t, store, sessionID)
	testassert.Require(t, settlement.Status == OperationFailure && settlement.Detail == genericExecutionFailureDetail,
		"settlement = %+v, want the failure settled with the fixed nonempty standard detail", settlement)
}

func TestRequestHookOutcomesThroughExecute(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		for _, cancelDuring := range []bool{false, true} {
			t.Run(fmt.Sprintf("interrupted=%t/canceled=%t", interrupted, cancelDuring), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cause := &EffectError{Err: context.Canceled, Interrupted: interrupted}
				calls, models := 0, 0
				h, store, c, sessionID, prepared, stop := newOpenerHarness(t, func(context.Context, OperationAdmission) (Execution, error) {
					hook := RequestHook{ID: "req.hook", ExtensionID: "req-ext", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
						calls++
						if cancelDuring {
							cancel()
						}
						return RequestConsequence{}, cause
					}}
					return requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
						models++
						return completedTurnStream(), nil
					}, func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, hook), nil
				})
				defer stop()
				err := executeDirect(h, c, ctx, testOpID, prepared)
				want := OperationFailure
				if interrupted {
					want = OperationInterruption
				}
				if (!interrupted && err != cause) || (interrupted && err != nil) || calls != 1 || models != 0 {
					t.Fatalf("execute=%v calls=%d model=%d", err, calls, models)
				}
				settlement := graphSettlementOf(t, store, sessionID)
				if settlement.Status != want {
					t.Fatalf("settlement=%+v want %s", settlement, want)
				}
			})
		}
	}
}

// TestPureRequestHookCancellationClassification pins the pure request-hook
// return boundary: an untyped cancellation-shaped error is an execution
// interruption only while its execution context is done. A typed EffectError
// keeps its declared Interrupted flag authoritative even when false and
// wrapping cancellation; ordinary causes stay failures. A done context
// dispatches no model.
func TestPureRequestHookCancellationClassification(t *testing.T) {
	wrapped := func(cause error) error { return fmt.Errorf("hook transport: %w", cause) }
	for _, tc := range []struct {
		name        string
		cause       error
		typed       *EffectError
		cancel      bool
		interrupted bool
	}{
		{name: "bare cancellation under done context", cause: context.Canceled, cancel: true, interrupted: true},
		{name: "wrapped cancellation under done context", cause: wrapped(context.Canceled), cancel: true, interrupted: true},
		{name: "bare deadline under done context", cause: context.DeadlineExceeded, cancel: true, interrupted: true},
		{name: "wrapped deadline under done context", cause: wrapped(context.DeadlineExceeded), cancel: true, interrupted: true},
		{name: "bare cancellation under live context", cause: context.Canceled},
		{name: "wrapped deadline under live context", cause: wrapped(context.DeadlineExceeded)},
		{name: "ordinary failure under done context", cause: errors.New("hook failed"), cancel: true},
		{name: "typed failure wrapping cancellation under done context", typed: &EffectError{Err: context.Canceled}, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			modelCalls := 0
			hook := RequestHook{ID: "req.pure", ExtensionID: "req-ext", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
				if tc.cancel {
					cancel()
				}
				if tc.typed != nil {
					return RequestConsequence{}, tc.typed
				}
				return RequestConsequence{}, tc.cause
			}}
			exec := requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				modelCalls++
				return completedTurnStream(), nil
			}, nil, hook)
			h, store, c, sessionID := newHookHarness(t, exec)
			_, err := runAdmittedExecution(t, h, c, ctx, testOpID, testCapture(), exec)

			want := OperationFailure
			if tc.interrupted {
				want = OperationInterruption
			}
			settlement := graphSettlementOf(t, store, sessionID)
			testassert.Require(t, settlement.Status == want, "settlement = %+v, want %s", settlement, want)
			testassert.Require(t, modelCalls == 0, "model calls = %d, want no dispatch from a failed pure request hook", modelCalls)
			if tc.interrupted {
				testassert.Require(t, err == nil, "execution = %v, want nil after the settled interruption", err)
			} else {
				cause := tc.cause
				if tc.typed != nil {
					cause = tc.typed
				}
				testassert.Require(t, err == cause, "execution = %v, want the original cause %v", err, cause)
			}
		})
	}
}

func TestPureArgumentHookFailureDoesNotCreateUncertainty(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(fmt.Sprintf("interrupted=%t", interrupted), func(t *testing.T) {
			hook := ToolArgumentsHook{ID: "pure", ExtensionID: "ext", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{}, &EffectError{Err: context.Canceled, Interrupted: interrupted}
			}}
			exec := hookFixtureExecution(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize, hook)
			_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
			want := model.ResultError
			if interrupted {
				want = model.ResultInterrupted
			}
			if err != nil || results[0].Status != want || strings.Contains(results[0].Content, unknownToolResultPrefix) {
				t.Fatalf("result=%+v error=%v", results[0], err)
			}
		})
	}
}

func TestArgumentHookPureRestoreMatchesBaselineTransactions(t *testing.T) {
	driveArgs := func(t *testing.T, hooks ...ToolArgumentsHook) int {
		spy := &hookToolSpy{}
		exec := hookFixtureExecution(spy.tool, objectNormalize, hooks...)
		steps := 0
		_, _, _, _, results, err := freshExecutionToolCallsConfig(t, context.Background(), testCapture(), exec, func(store *graphStorage) {
			store.txHook = func(string) error { steps++; return nil }
		}, testToolCall("call-1"))
		testassert.NoError(t, err)
		testassert.Require(t, results[0].Status == model.ResultSuccess, "tool result = %+v, want the call run", results[0])
		return steps
	}
	baseSteps := driveArgs(t)
	var capturedOriginal json.RawMessage
	pureA := ToolArgumentsHook{ID: "cap.pure.one", ExtensionID: "cap-a", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		capturedOriginal = model.CloneRaw(call.Arguments)
		return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":9}`)}, nil
	}}
	pureB := ToolArgumentsHook{ID: "cap.pure.two", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
		return ToolArgumentsResult{Arguments: model.CloneRaw(capturedOriginal)}, nil
	}}
	pureSteps := driveArgs(t, pureA, pureB)
	testassert.Require(t, pureSteps == baseSteps, "pure restore chain performed %d durable mutation steps, want the baseline %d", pureSteps, baseSteps)
}

// TestRequestHookCodecAdoptedValueContinuesTheLiveView proves the continuation
// rule on the request boundary: a pure contribution whose raw prompt bytes are
// not canonical text reaches the effectful hook only through the codec-adopted
// committed evidence — never resurrected raw bytes — and every later pure
// hook, the continuing view, the durable rows, and the actual dispatch all
// carry the same adopted value.
func TestRequestHookEmptyToolSelectionStaysHidden(t *testing.T) {
	var observedTools int
	var nextPureTools int
	hide := RequestHook{ID: "req.pure.hide", ExtensionID: "req-ext", Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
		hidden := []string{}
		return RequestConsequence{Tools: fixtureDefinitions(hidden...)}, nil
	}}
	noop := RequestHook{ID: "req.pure.noop", ExtensionID: "req-ext", Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		observedTools = len(in.Tools)
		return RequestConsequence{}, nil
	}}
	final := RequestHook{ID: "req.pure.final", ExtensionID: "req-ext", Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		nextPureTools = len(in.Tools)
		return RequestConsequence{}, nil
	}}
	exec := requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		return completedTurnStream(), nil
	}, nil, hide, noop, final)
	h, store, c, sessionID := newHookHarness(t, exec)
	{
		sent, err := runRequestHookRound(t, h, c, exec, testCapture())
		testassert.NoError(t, err)
		testassert.Require(t, len(sent.Tools) == 0, "dispatch advertises %d tools, want the recorded no-advertisement selection", len(sent.Tools))
	}
	testassert.Require(t, observedTools == 0, "later hook received %d tools, want none after the hiding", observedTools)
	testassert.Require(t, nextPureTools == 0, "later pure hook received %d tools, want the hiding continuing", nextPureTools)
	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, assistant.Evidence != nil && assistant.Evidence.Tools != nil && len(assistant.Evidence.Tools) == 0,
		"assistant evidence = %v, want the recorded empty view carried to the boundary", assistant.Evidence)
	{
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	}
}

// TestRequestHookPureContributorsFollowEffectiveChange proves the settled
// request provenance policy through the actual pipeline: every successfully
// accepted hook identity is recorded in configured order, including
// reassertions and no-ops; whole-baseline restoration keeps the identities
// while the recorded value deltas return to the frozen capture; and no
// per-dimension last-setter attribution survives anywhere.
func TestRequestHookPureContributorsFollowEffectiveChange(t *testing.T) {
	capture := testCapture()
	basePrompt := capture.SystemPrompt
	fullNames := make([]string, 0, len(capture.Tools))
	for _, tool := range capture.Tools {
		fullNames = append(fullNames, tool.Name)
	}
	drive := func(t *testing.T, hooks ...RequestHook) (*RequestEvidence, model.Request) {
		exec := requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return completedTurnStream(), nil
		}, nil, hooks...)
		h, store, c, sessionID := newHookHarness(t, exec)
		{
			sent, err := runRequestHookRound(t, h, c, exec, capture)
			testassert.NoError(t, err)
			return compositionAssistant(t, store, sessionID).Evidence, sent
		}
	}
	promptOf := func(req model.Request) string {
		for _, m := range req.Messages {
			if m.Role == model.RoleSystem {
				return m.TextContent()
			}
		}
		return ""
	}
	hook := func(id string, run func(context.Context, RequestHookInput) (RequestConsequence, error)) RequestHook {
		return RequestHook{ID: id, ExtensionID: "req-ext", Run: run}
	}
	t.Run("sequential appends both contribute", func(t *testing.T) {
		var first string
		ev, sent := drive(t,
			hook("req.pure.one", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				changed := in.Prompt + " one"
				first = changed
				return RequestConsequence{Prompt: &changed}, nil
			}),
			hook("req.pure.two", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				testassert.Equal(t, in.Prompt, first)
				changed := in.Prompt + " two"
				return RequestConsequence{Prompt: &changed}, nil
			}),
		)
		testassert.Require(t, len(ev.HookIDs) == 2 && ev.HookIDs[0].HookID == "req.pure.one" && ev.HookIDs[1].HookID == "req.pure.two",
			"contributions = %+v, want both appenders in hook order", ev.HookIDs)
		testassert.Require(t, ev.Prompt == basePrompt+" one two" && promptOf(sent) == basePrompt+" one two",
			"prompt delta = %v dispatch = %q, want the whole accumulated replacement", ev.Prompt, promptOf(sent))
	})
	t.Run("reassertion after a change is also a contributor", func(t *testing.T) {
		ev, sent := drive(t,
			hook("req.pure.one", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				changed := in.Prompt + " one"
				return RequestConsequence{Prompt: &changed}, nil
			}),
			hook("req.pure.reassert", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				reasserted := in.Prompt // the exact value the hook received: no effective change
				return RequestConsequence{Prompt: &reasserted}, nil
			}),
		)
		testassert.Require(t, len(ev.HookIDs) == 2 && ev.HookIDs[0].HookID == "req.pure.one" && ev.HookIDs[1].HookID == "req.pure.reassert",
			"contributions = %+v, want both accepted hooks in configured order", ev.HookIDs)
		testassert.Require(t, promptOf(sent) == basePrompt+" one", "dispatch = %q, want the changed value", promptOf(sent))
	})
	t.Run("pure no-op before a changed hook is also a contributor", func(t *testing.T) {
		ev, _ := drive(t,
			hook("req.pure.noop", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				same := in.Prompt
				return RequestConsequence{Prompt: &same}, nil
			}),
			hook("req.pure.change", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				changed := in.Prompt + " tail"
				return RequestConsequence{Prompt: &changed}, nil
			}),
		)
		testassert.Require(t, len(ev.HookIDs) == 2 && ev.HookIDs[0].HookID == "req.pure.noop" && ev.HookIDs[1].HookID == "req.pure.change",
			"contributions = %+v, want every accepted hook in configured order", ev.HookIDs)
	})
	t.Run("change then restore then later change keeps every accepted contributor", func(t *testing.T) {
		var original string
		ev, sent := drive(t,
			hook("req.pure.change", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				original = in.Prompt
				changed := in.Prompt + " mid"
				return RequestConsequence{Prompt: &changed}, nil
			}),
			hook("req.pure.restore", func(context.Context, RequestHookInput) (RequestConsequence, error) {
				restored := original // the entire frozen prompt returns
				return RequestConsequence{Prompt: &restored}, nil
			}),
			hook("req.pure.later", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				changed := in.Prompt + " later"
				return RequestConsequence{Prompt: &changed}, nil
			}),
		)
		testassert.Require(t, len(ev.HookIDs) == 3 && ev.HookIDs[0].HookID == "req.pure.change" && ev.HookIDs[1].HookID == "req.pure.restore" && ev.HookIDs[2].HookID == "req.pure.later",
			"contributions = %+v, want every accepted hook in configured order", ev.HookIDs)
		testassert.Require(t, promptOf(sent) == original+" later", "dispatch = %q, want restoration then the later change", promptOf(sent))
	})
	t.Run("mixed prompt and tool changes keep intermediate contributors", func(t *testing.T) {
		ev, sent := drive(t,
			hook("req.pure.prompt", func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				changed := in.Prompt + " prompt"
				return RequestConsequence{Prompt: &changed}, nil
			}),
			hook("req.pure.hide", func(context.Context, RequestHookInput) (RequestConsequence, error) {
				hidden := []string{}
				return RequestConsequence{Tools: fixtureDefinitions(hidden...)}, nil
			}),
			hook("req.pure.show", func(context.Context, RequestHookInput) (RequestConsequence, error) {
				shown := append([]string(nil), fullNames...) // the full catalog selection: a genuine change from hidden
				return RequestConsequence{Tools: fixtureDefinitions(shown...)}, nil
			}),
		)
		testassert.Require(t, len(ev.HookIDs) == 3 && ev.HookIDs[0].HookID == "req.pure.prompt" &&
			ev.HookIDs[1].HookID == "req.pure.hide" && ev.HookIDs[2].HookID == "req.pure.show",
			"contributions = %+v, want every hook that actually changed the live view, in hook order", ev.HookIDs)
		testassert.Require(t, reflect.DeepEqual(fixtureDefinitionNames(ev.Tools), fullNames), "names = %v, want the full catalog", ev.Tools)
		testassert.Require(t, len(sent.Tools) == len(capture.Tools), "dispatch advertises %d tools, want the restored full catalog", len(sent.Tools))
	})
	t.Run("hide-all then full baseline restoration keeps identities with no deltas", func(t *testing.T) {
		ev, sent := drive(t,
			hook("req.pure.hide", func(context.Context, RequestHookInput) (RequestConsequence, error) {
				hidden := []string{}
				return RequestConsequence{Tools: fixtureDefinitions(hidden...)}, nil
			}),
			hook("req.pure.restore", func(context.Context, RequestHookInput) (RequestConsequence, error) {
				shown := append([]string(nil), fullNames...)
				prompt := basePrompt
				return RequestConsequence{Prompt: &prompt, Tools: fixtureDefinitions(shown...)}, nil // the entire frozen baseline returns
			}),
		)
		testassert.Require(t, len(ev.HookIDs) == 2 && ev.HookIDs[0].HookID == "req.pure.hide" && ev.HookIDs[1].HookID == "req.pure.restore",
			"contributions = %+v, want both accepted hooks kept in configured order", ev.HookIDs)
		testassert.Require(t, ev.Prompt == basePrompt && reflect.DeepEqual(fixtureDefinitionNames(ev.Tools), fullNames), "evidence = prompt %v names %v, want full captured view", ev.Prompt, ev.Tools)
		testassert.Require(t, promptOf(sent) == basePrompt && len(sent.Tools) == len(capture.Tools), "dispatch = %q with %d tools, want the unchanged baseline view", promptOf(sent), len(sent.Tools))
	})

}

// TestRequestHookInputFrozenDomainsCarryHiddenCatalogAndRestoresThroughCatalogDefinitions
// proves the frozen input domains of RequestHookInput through the actual
// ordered pipeline: a pure hook changes the prompt,
// adds a role-indistinguishable user message, and hides every tool; the
// successor identifies exactly the round-frozen canonical messages
// independently of the effective Messages, sees the captured prompt
// independently of the current Prompt, and discovers the hidden eligible
// tools by inspecting the stage catalog, then restores their definitions. A third hook mutates the successor's owned CanonicalMessages content
// and Catalog parameter bytes; the barrier proves neither mutation reaches the
// live view, a later hook, the model dispatch, or the captured catalog, and
// the dispatch still carries the full restored catalog and the addition
// exactly once.
func TestRequestHookInputFrozenDomainsCarryHiddenCatalogAndRestoresThroughCatalogDefinitions(t *testing.T) {
	capture := testCapture()
	basePrompt := capture.SystemPrompt
	fullNames := make([]string, 0, len(capture.Tools))
	for _, tool := range capture.Tools {
		fullNames = append(fullNames, tool.Name)
	}
	parametersOf := func(tools []model.ToolDefinition) []string {
		out := make([]string, 0, len(tools))
		for _, tool := range tools {
			out = append(out, string(tool.Parameters))
		}
		return out
	}
	baseParameters := parametersOf(capture.Tools)
	textsOf := func(messages []model.Message) []string {
		out := make([]string, 0, len(messages))
		for _, message := range messages {
			out = append(out, string(message.Role)+"|"+message.TextContent())
		}
		return out
	}
	namesOf := func(tools []model.ToolDefinition) []string {
		out := make([]string, 0, len(tools))
		for _, tool := range tools {
			out = append(out, tool.Name)
		}
		return out
	}
	addition, aerr := model.NewMessage(model.Message{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "contributed"}}})
	testassert.NoError(t, aerr)
	var base []string
	firstChange := func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		// At the first boundary the frozen domains and the evolving view agree.
		testassert.Require(t, reflect.DeepEqual(textsOf(in.CanonicalMessages), textsOf(in.Messages)),
			"first input frozen canonical = %v effective = %v, want agreement at the opening boundary", textsOf(in.CanonicalMessages), textsOf(in.Messages))
		testassert.Equal(t, in.CapturedPrompt, basePrompt)
		testassert.Require(t, reflect.DeepEqual(namesOf(in.Catalog), namesOf(in.Tools)),
			"first catalog = %v tools = %v, want the unchanged captured view at the opening boundary", namesOf(in.Catalog), namesOf(in.Tools))
		testassert.Require(t, reflect.DeepEqual(parametersOf(in.Catalog), baseParameters), "first catalog parameters mismatch")
		base = textsOf(in.CanonicalMessages)
		prompt := in.Prompt + " modified"
		hidden := []string{}
		return RequestConsequence{Prompt: &prompt, Edits: appendRequestFixture(in, addition), Tools: fixtureDefinitions(hidden...)}, nil
	}
	frozenSuccessor := func() RequestHook {
		return RequestHook{ID: "req.successor", ExtensionID: "req-ext", Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
			// The frozen canonical excludes the pure addition; the effective
			// Messages carry it.
			testassert.Require(t, reflect.DeepEqual(textsOf(in.CanonicalMessages), base), "successor frozen canonical = %v, want the round-frozen base", textsOf(in.CanonicalMessages))
			testassert.Require(t, len(in.Messages) == len(base)+1 && textsOf(in.Messages)[len(base)] == "user|contributed",
				"effective messages = %v, want the frozen canonical plus the addition", textsOf(in.Messages))
			// The captured prompt stands independently of the current prompt.
			testassert.Equal(t, in.CapturedPrompt, basePrompt)
			testassert.Equal(t, in.Prompt, basePrompt+" modified")
			// Hidden tools are discoverable only through the Catalog.
			testassert.Require(t, len(in.Tools) == 0, "visible tools = %v, want every eligible tool hidden", namesOf(in.Tools))
			testassert.Require(t, reflect.DeepEqual(namesOf(in.Catalog), fullNames), "successor catalog = %v, want the full eligible catalog while hidden", namesOf(in.Catalog))
			testassert.Require(t, reflect.DeepEqual(parametersOf(in.Catalog), baseParameters), "successor catalog parameters mismatch")
			restored := cloneToolDefinitions(in.Catalog)
			// Mutation barrier: nested owned values the hook may scribble on.
			if len(in.CanonicalMessages) > 0 && len(in.CanonicalMessages[0].Content) > 0 {
				in.CanonicalMessages[0].Content[0] = model.ContentPart{Kind: model.PartText, Text: "MUTATED"}
			}
			if len(in.Catalog) > 0 {
				in.Catalog[0].Parameters = json.RawMessage(`{"mutated":true}`)
			}
			return RequestConsequence{Tools: restored}, nil
		}}
	}
	barrierCheck := RequestHook{ID: "req.barrier", ExtensionID: "req-ext", Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		testassert.Require(t, reflect.DeepEqual(textsOf(in.CanonicalMessages), base), "barrier canonical = %v, want the mutation to have reached nothing", textsOf(in.CanonicalMessages))
		testassert.Require(t, reflect.DeepEqual(parametersOf(in.Catalog), baseParameters), "barrier catalog parameters mismatch: the mutation reached no later input")
		testassert.Require(t, reflect.DeepEqual(namesOf(in.Tools), fullNames), "barrier tools = %v, want the restored full catalog for the next boundary", namesOf(in.Tools))
		return RequestConsequence{}, nil
	}}
	promptOf := func(req model.Request) string {
		for _, m := range req.Messages {
			if m.Role == model.RoleSystem {
				return m.TextContent()
			}
		}
		return ""
	}
	t.Run("pure successor chain", func(t *testing.T) {
		hooks := []RequestHook{
			{ID: "req.first", ExtensionID: "req-ext", Run: firstChange},
			frozenSuccessor(),
			barrierCheck,
		}
		exec := requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return completedTurnStream(), nil
		}, nil, hooks...)
		h, store, c, sessionID := newHookHarness(t, exec)
		{
			sent, err := runRequestHookRound(t, h, c, exec, capture)
			testassert.NoError(t, err)
			testassert.Equal(t, promptOf(sent), basePrompt+" modified")
			contributed := 0
			conversation := make([]string, 0, len(sent.Messages))
			for _, m := range sent.Messages {
				if m.Role == model.RoleSystem { // the rendered prompt is carried apart from the canonical view
					continue
				}
				conversation = append(conversation, string(m.Role)+"|"+m.TextContent())
				if m.TextContent() == "contributed" {
					contributed++
				}
			}
			testassert.Require(t, contributed == 1 && reflect.DeepEqual(conversation, append(append([]string(nil), base...), "user|contributed")),
				"dispatch conversation = %v, want the frozen canonical with the addition exactly once", conversation)
			testassert.Require(t, reflect.DeepEqual(namesOf(sent.Tools), fullNames), "dispatch tools = %v, want the full restored catalog", namesOf(sent.Tools)) // full restored catalog
			testassert.Require(t, reflect.DeepEqual(parametersOf(sent.Tools), baseParameters), "dispatch catalog parameters mismatch")
		}
		{
			assistant := compositionAssistant(t, store, sessionID)
			testassert.Require(t, assistant.Evidence != nil && len(assistant.Evidence.HookIDs) == 3 &&
				assistant.Evidence.HookIDs[0].HookID == "req.first" && assistant.Evidence.HookIDs[1].HookID == "req.successor" && assistant.Evidence.HookIDs[2].HookID == "req.barrier",
				"assistant contributions = %+v, want every accepted hook in configured order (the no-op barrier included)", assistant.Evidence.HookIDs)
			testassert.Require(t, reflect.DeepEqual(fixtureDefinitionNames(assistant.Evidence.Tools), fullNames), "names = %v, want the full catalog", assistant.Evidence.Tools)
		}
		testassert.Require(t, reflect.DeepEqual(parametersOf(capture.Tools), baseParameters), "the caller's capture must stand untouched") // the caller's capture stands untouched
	})

}

// TestCallbackIdentityCompleteAtEveryBoundary proves each real Harness
// producer binds the actual request/call identity to its context: prepare and
// pure preprocessing carry no effect identity, and the executed call and the
// after-tool shaping carry its reserved result identity.
func TestCallbackIdentityCompleteAtEveryBoundary(t *testing.T) {
	var (
		mu                   sync.Mutex
		normalized, prepared []CallbackIdentity
		pure, shaped         []CallbackIdentity
		executed             []CallbackIdentity
	)
	record := func(list *[]CallbackIdentity, ctx context.Context) {
		mu.Lock()
		*list = append(*list, CallbackIdentityFromContext(ctx))
		mu.Unlock()
	}
	normalize := func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
		record(&normalized, ctx)
		return objectNormalize(ctx, call)
	}
	tool := func(ctx context.Context, call model.ToolCall) PreparedTool {
		record(&prepared, ctx)
		return PreparedTool{Permissions: fixturePermission, Execute: func(ctx context.Context) ToolOutcome {
			record(&executed, ctx)
			return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ok"}}
		}}
	}
	hooks := []ToolArgumentsHook{
		{ID: "cap.hook.pure", ExtensionID: "cap-a", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
			record(&pure, ctx)
			return ToolArgumentsResult{Arguments: json.RawMessage(`{"repaired":true}`)}, nil
		}},
	}
	outputs := []ToolOutputHook{{ID: "cap.hook.output", ExtensionID: "cap-a", Run: func(ctx context.Context, input ToolOutputInput) (string, error) {
		record(&shaped, ctx)
		return input.Content + "!", nil
	}}}
	exec := Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
		return InvocationExecution{
			Model: func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				return completedTurnStream(), nil
			},
			LookupTool: fixtureToolLookup(tool, normalize), ToolHooks: hooks, ToolOutputHooks: outputs,
		}, nil
	}}
	_, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
	testassert.NoError(t, err)

	testassert.Require(t, len(prepared) == 1 && len(executed) == 1 && len(pure) == 1 && len(shaped) == 1 && len(normalized) == 1, "observed identities = prepare %d execute %d pure %d shaped %d normalize %d, want one each", len(prepared), len(executed), len(pure), len(shaped), len(normalized))
	invocationID := prepared[0].RequestID
	testassert.Require(t, invocationID != "", "Prepare identity %+v, want the opened invocation request", prepared[0])
	for name, identity := range map[string]CallbackIdentity{
		"prepare": prepared[0], "execute": executed[0], "pure hook": pure[0],
		"shaped output": shaped[0], "final normalization": normalized[0],
	} {
		testassert.Require(t, identity.RequestID == invocationID && identity.ToolCallID == "call-1", "%s identity = %+v, want invocation %q and call call-1", name, identity, invocationID)
	}
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var callAssistant *assistantEntry
	for i := range graph.Entries {
		if entry := graph.Entries[i].Assistant; entry != nil && len(entry.ToolCalls) != 0 {
			callAssistant = entry
		}
	}
	testassert.Require(t, callAssistant != nil && callAssistant.RequestID != "", "no assistant entry published the tool call")
	testassert.Require(t, normalized[0].RequestID == invocationID && normalized[0].ToolCallID == "call-1", "normalization identity = %+v, want invocation %q and call call-1", normalized[0], invocationID)
	testassert.Require(t, prepared[0].EffectID == "" && normalized[0].EffectID == "" && pure[0].EffectID == "", "pure/preprocessing identities = prepare %+v normalize %+v pure %+v, want no effect identity", prepared[0], normalized[0], pure[0])
	testassert.Require(t, executed[0].EffectID == results[0].EntryID && shaped[0].EffectID == results[0].EntryID, "executed/shaped effect identities = %q/%q, want the committed tool result entry %q", executed[0].EffectID, shaped[0].EffectID, results[0].EntryID)
}

// TestArgumentHookRefusalSettlesDenied proves a deliberate before-tool refusal
// stops that call's chain and settles it denied, not started, with the refusal
// text and the refusing hook's identity, while the batch's later call still
// runs. Cold revalidation and Fork accept the record.
func TestArgumentHookRefusalSettlesDenied(t *testing.T) {
	reason := "the model may not call this"
	hook := ToolArgumentsHook{ID: "cap.hook.refuse", ExtensionID: "cap-a", Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		if call.ID == "call-1" {
			return ToolArgumentsResult{Refusal: &reason}, nil
		}
		return ToolArgumentsResult{Arguments: model.CloneRaw(call.Arguments)}, nil
	}}
	spy := &hookToolSpy{}
	exec := hookFixtureExecution(spy.tool, objectNormalize, hook)
	first, second := testToolCall("call-1"), testToolCall("call-2")
	h, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, first, second)
	testassert.NoError(t, err)
	testassert.Require(t, results[0].Status == model.ResultDenied && results[0].Content == reason, "refused call = %+v, want denied with the refusal text", results[0])
	testassert.Require(t, results[1].Status == model.ResultSuccess && results[1].Content == "done", "later sibling = %+v, want the ordinary success", results[1])
	prepared := spy.received()
	testassert.Require(t, len(prepared) == 1 && prepared[0].ID == "call-2", "prepared calls = %+v, want only the later sibling", prepared)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var refused *toolResultEntry
	for _, entry := range graph.Entries {
		if entry.ToolResult != nil && entry.ToolResult.ToolCallID == "call-1" {
			refused = entry.ToolResult
		}
	}
	testassert.Require(t, refused != nil && refused.Status == model.ResultDenied && refused.Evidence == ExecutionNotStarted &&
		refused.RefusedBy != nil && refused.RefusedBy.ExtensionID == "cap-a" && refused.RefusedBy.HookID == "cap.hook.refuse",
		"refused result = %+v, want the denied not-started result naming its hook", refused)
	testassert.Require(t, refused.Implementation == nil && len(refused.NormalizedArguments) == 0 && len(refused.HookIDs) == 0,
		"refused result = %+v, want no implementation, arguments or hook identities", refused)
	// Cold revalidation over a fresh Harness accepts the record.
	testassert.NoError(t, Recover(context.Background(), store))
	cold := newTestHarness(t, store, nil)
	_, err = cold.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, err)
	// Fork at an input after the refusal copies the denied result, including
	// the refusing hook's identity.
	submitted, err := h.Submit(context.Background(), SubmitRequest{
		SessionID: sessionID, OperationID: "op-2", Origin: InputOriginUser, Mode: MessageModeRegular, Content: admissionContent("after the refusal"),
	})
	testassert.NoError(t, err)
	testassert.Require(t, submitted.Disposition == DispositionAdmitted && submitted.Operation != nil, "second submit = %+v, want the admitted operation", submitted)
	awaitOperationTerminal(t, store, sessionID, "op-2", OperationFailure)
	entries, err := store.ReadEntries(context.Background(), sessionID, 0)
	testassert.NoError(t, err)
	boundary := ""
	for _, entry := range entries {
		if entry.Kind == EntryInput && entry.OperationID == "op-2" {
			boundary = entry.ID
			break
		}
	}
	testassert.Require(t, boundary != "", "the second Operation has no input boundary")
	forked, err := h.Fork(context.Background(), ForkRequest{Origin: InputOriginUser, SourceSessionID: sessionID, BoundaryEntryID: boundary, OperationID: "fork-refusal", Content: []model.ContentPart{{Kind: model.PartText, Text: "fork"}}})
	testassert.NoError(t, err)
	copied, err := store.ReadEntries(context.Background(), forked.Session.Identity.SessionID, 0)
	testassert.NoError(t, err)
	var copiedRefusal *toolResultEntry
	for _, entry := range copied {
		if entry.Kind != EntryToolResult {
			continue
		}
		decoded, derr := decodeToolResultEntry(entry)
		testassert.NoError(t, derr)
		if decoded.ToolCallID == "call-1" {
			copiedRefusal = &decoded
		}
	}
	testassert.Require(t, copiedRefusal != nil && copiedRefusal.Status == model.ResultDenied &&
		copiedRefusal.RefusedBy != nil && copiedRefusal.RefusedBy.ExtensionID == "cap-a" && copiedRefusal.RefusedBy.HookID == "cap.hook.refuse",
		"copied refused result = %+v, want the refusal identity preserved", copiedRefusal)
}

// TestArgumentHookRefusalMatchesAuthorizationDenial proves a before-tool
// refusal settles with the same bounded, repaired content an authorization-hook
// denial produces.
func TestArgumentHookRefusalMatchesAuthorizationDenial(t *testing.T) {
	for _, row := range []struct {
		name   string
		reason string
		want   string
	}{
		{"long", strings.Repeat("é", 300) + "\xff", ""},
		{"invalid utf8", "\xff\xfe", "\uFFFD"},
	} {
		t.Run(row.name, func(t *testing.T) {
			reason := row.reason
			inv := InvocationExecution{
				LookupTool:   fixtureToolLookup((&hookToolSpy{}).tool, objectNormalize),
				BindLocation: func(string) (string, func() error, error) { return "/w", func() error { return nil }, nil },
				AuthorizationHooks: []AuthorizationHook{{Run: func(context.Context, PreparedAction, func(string) (string, error)) (*string, error) {
					return &reason, nil
				}}},
			}
			_, _, _, _, authorized, err := freshExecutionToolCalls(t, testCapture(), executionForInvocation(inv), testToolCall("call-1"))
			testassert.NoError(t, err)

			hook := ToolArgumentsHook{ID: "cap.hook.refuse", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Refusal: &reason}, nil
			}}
			_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), hookFixtureExecution((&hookToolSpy{}).tool, objectNormalize, hook), testToolCall("call-1"))
			testassert.NoError(t, err)
			got := results[0]
			testassert.Require(t, authorized[0].Status == model.ResultDenied, "authorization denial = %+v, want denied", authorized[0])
			testassert.Require(t, got.Status == model.ResultDenied && got.Content == authorized[0].Content, "hook refusal = %+v, want the authorization denial's content %q", got, authorized[0].Content)
			testassert.Require(t, utf8.ValidString(got.Content) && len(got.Content) <= maxToolDiagnosticBytes, "refusal content = %q, want valid UTF-8 within %d bytes", got.Content, maxToolDiagnosticBytes)
			if row.want != "" {
				testassert.Require(t, got.Content == row.want, "repaired refusal content = %q, want %q", got.Content, row.want)
			}
		})
	}
}

// TestArgumentHookAcceptedSelectionSurvivesToolEffectExits proves that the
// chain's accepted normalized value and ordered provenance survive tool-effect
// exits before execution: an invalid prepared plan and a cancellation-aborted
// tool intent each settle the call carrying the accepted selection.
func TestArgumentHookAcceptedSelectionSurvivesToolEffectExits(t *testing.T) {
	chainHooks := func() []ToolArgumentsHook {
		return []ToolArgumentsHook{
			{ID: "cap.pure.repair", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":9}`)}, nil
			}},
			{ID: "cap.pure.replace", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":7}`)}, nil
			}},
		}
	}
	assertAccepted := func(t *testing.T, store *graphStorage, sessionID string) {
		t.Helper()
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		settled := 0
		for _, entry := range graph.Entries {
			if entry.ToolResult == nil {
				continue
			}
			settled++
			testassert.Require(t, string(entry.ToolResult.NormalizedArguments) == `{"x":7}` &&
				len(entry.ToolResult.HookIDs) == 2 && entry.ToolResult.HookIDs[0].HookID == "cap.pure.repair" && entry.ToolResult.HookIDs[1].HookID == "cap.pure.replace",
				"tool result provenance = %+v %s, want the accepted value and ordered chain", entry.ToolResult.NormalizedArguments, entry.ToolResult.HookIDs)
		}
		testassert.Require(t, settled >= 1, "no committed tool result carried the accepted selection")
	}
	t.Run("an invalid prepared plan settles with the accepted selection", func(t *testing.T) {
		spy := &hookToolSpy{plan: func(model.ToolCall) PreparedTool { return PreparedTool{} }} // shape-invalid: neither immediate nor executable
		exec := hookFixtureExecution(spy.tool, objectNormalize, chainHooks()...)
		_, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, testToolCall("call-1"))
		testassert.NoError(t, err)
		testassert.Require(t, results[0].Status == model.ResultError, "tool result = %+v, want the invalid-plan settlement", results[0])
		prepared := spy.received()
		testassert.Require(t, len(prepared) == 1 && string(prepared[0].Arguments) == `{"x":7}`, "preparer received %+v, want the chain's accepted value", prepared)
		assertAccepted(t, store, sessionID)
	})
	t.Run("a cancellation-aborted tool effect intent settles carrying the accepted selection", func(t *testing.T) {
		executed := 0
		spy := &hookToolSpy{plan: func(call model.ToolCall) PreparedTool {
			return PreparedTool{Permissions: fixturePermission, Execute: func(context.Context) ToolOutcome {
				executed++
				return ToolOutcome{Evidence: ExecutionObserved, Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
			}}
		}}
		exec := hookFixtureExecution(spy.tool, objectNormalize, chainHooks()...)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, _, _, _, results, err := freshExecutionToolCallsConfig(t, ctx, testCapture(), exec, func(store *graphStorage) {
			failEffectRegister(store,
				func(ae *ActiveEffect) bool { return ae.Kind == EffectTool && ae.ToolCallID == "call-1" },
				false, nil, cancel) // cancellation lands on the concrete tool effect intent
		}, testToolCall("call-1"), testToolCall("call-2"))
		testassert.NoError(t, err)
		testassert.Require(t, results[0].Status == model.ResultInterrupted && string(results[0].NormalizedArguments) == `{"x":7}` &&
			len(results[0].HookIDs) == 2 && results[0].HookIDs[0].HookID == "cap.pure.repair" && results[0].HookIDs[1].HookID == "cap.pure.replace",
			"tool result = %+v, want the settled interruption carrying the accepted selection and ordered provenance", results[0])
		testassert.Require(t, results[1].Status == model.ResultInterrupted && results[1].Evidence == ExecutionNotStarted,
			"sibling tool result = %+v, want the terminal helper's proven-unstarted interruption", results[1])
		testassert.Equal(t, executed, 0)
		prepared := spy.received()
		testassert.Require(t, len(prepared) == 1 && string(prepared[0].Arguments) == `{"x":7}`,
			"preparer received %+v, want the accepted value for the call that reached preparation", prepared)
	})
}

// TestArgumentHookErrorIsProcessingFailure is the nearest sibling of a
// refusal: a hook error keeps the ordinary processing-failure settlement,
// records no refusing identity, and the later call still runs.
func TestArgumentHookErrorIsProcessingFailure(t *testing.T) {
	hook := ToolArgumentsHook{ID: "cap.hook.fail", ExtensionID: "cap-a", Run: func(_ context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		if call.ID == "call-1" {
			return ToolArgumentsResult{}, errors.New("hook broke")
		}
		return ToolArgumentsResult{Arguments: model.CloneRaw(call.Arguments)}, nil
	}}
	spy := &hookToolSpy{}
	exec := hookFixtureExecution(spy.tool, objectNormalize, hook)
	first, second := testToolCall("call-1"), testToolCall("call-2")
	_, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, first, second)
	testassert.NoError(t, err)
	testassert.Require(t, results[0].Status == model.ResultError && results[0].Content == "hook broke", "hook error = %+v, want the processing failure", results[0])
	testassert.Require(t, results[1].Status == model.ResultSuccess, "later sibling = %+v, want the ordinary success", results[1])
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var failed *toolResultEntry
	for _, entry := range graph.Entries {
		if entry.ToolResult != nil && entry.ToolResult.ToolCallID == "call-1" {
			failed = entry.ToolResult
		}
	}
	testassert.Require(t, failed != nil && failed.Status == model.ResultError && failed.RefusedBy == nil,
		"failed result = %+v, want the error result without a refusing identity", failed)
}
