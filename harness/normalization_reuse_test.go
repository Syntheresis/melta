// Contract coverage for one normalization after the argument-hook chain and
// independent settlement of invalid calls and valid batch siblings.
package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// seqNormalizer is the fixtures' counting, attempt-stamping normalizer: every
// invocation is numbered, a valid non-null object gains a "seq" member naming
// its attempt number, and every rejection carries its attempt in the
// diagnostic, making each normalization distinguishable downstream.
type seqNormalizer struct {
	attempts int
}

func (n *seqNormalizer) normalize(_ context.Context, call model.ToolCall) (json.RawMessage, error) {
	n.attempts++
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(call.Arguments, &obj); err != nil || obj == nil {
		return nil, fmt.Errorf("normalize attempt %d rejected", n.attempts)
	}
	obj["seq"] = json.RawMessage(strconv.Quote(strconv.Itoa(n.attempts)))
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func passthroughHook(id string, received *json.RawMessage) ToolArgumentsHook {
	return ToolArgumentsHook{ID: id, ExtensionID: "reuse-ext", Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		*received = model.CloneRaw(call.Arguments)
		return ToolArgumentsResult{Arguments: model.CloneRaw(call.Arguments)}, nil
	}}
}

func changedHook(id string, replacement string) ToolArgumentsHook {
	return ToolArgumentsHook{ID: id, ExtensionID: "reuse-ext", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
		return ToolArgumentsResult{Arguments: json.RawMessage(replacement)}, nil
	}}
}

// runReuseFixture admits the reuse fixture and returns the pieces the settle
// rows drive; each settle row runs the real admitted execution that publishes
// and settles its calls.
func runReuseFixture(t *testing.T, hooks []ToolArgumentsHook) (*Harness, *graphStorage, *coordinator, string, Execution, *hookToolSpy, *seqNormalizer) {
	t.Helper()
	spy := &hookToolSpy{}
	n := &seqNormalizer{}
	exec := hookFixtureExecution(spy.tool, n.normalize, hooks...)
	h, store, c, sessionID := newHookHarness(t, exec)
	return h, store, c, sessionID, exec, spy, n
}

func settleCall(t *testing.T, h *Harness, c *coordinator, store *graphStorage, sessionID string, exec Execution, call model.ToolCall) model.ToolResult {
	t.Helper()
	results, err := runToolCalls(t, h, c, store, sessionID, context.Background(), testOpID, exec, testCapture(), []model.ToolCall{call})
	testassert.NoError(t, err)
	return model.ToolResult{CallID: results[0].ToolCallID, Status: results[0].Status, Content: results[0].Content}
}

func rawCall(id string, arguments string) model.ToolCall {
	return model.ToolCall{ID: id, Name: "echo", Arguments: json.RawMessage(arguments)}
}

func TestArgumentHookNormalizationOutcomeReuse(t *testing.T) {
	t.Run("a valid original returned unchanged normalizes once", func(t *testing.T) {
		var received json.RawMessage
		h, store, c, sessionID, exec, spy, n := runReuseFixture(t, []ToolArgumentsHook{passthroughHook("reuse.pure", &received)})
		got := settleCall(t, h, c, store, sessionID, exec, rawCall("call-1", `{"x":1}`))
		testassert.Require(t, got.Status == model.ResultSuccess && got.Content == "done", "settled result = %+v, want the call prepared after normalization", got)
		testassert.Require(t, string(received) == `{"x":1}`, "hook input = %q, want the raw original bytes", string(received))
		prepared := spy.received()
		testassert.Equal(t, len(prepared), 1)
		testassert.Require(t, string(prepared[0].Arguments) == `{"seq":"1","x":1}`, "preparer arguments = %q, want the normalized arguments", string(prepared[0].Arguments))
		testassert.Equal(t, n.attempts, 1)
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	})

	t.Run("an invalid original returned unchanged settles with the diagnostic and no preparation", func(t *testing.T) {
		var received json.RawMessage
		h, store, c, sessionID, exec, spy, n := runReuseFixture(t, []ToolArgumentsHook{passthroughHook("reuse.pure", &received)})
		got := settleCall(t, h, c, store, sessionID, exec, rawCall("call-1", `[1,2]`))
		testassert.Require(t, got.Status == model.ResultError && got.Content == "normalize attempt 1 rejected",
			"settled result = %+v, want the normalizer diagnostic", got)
		testassert.Require(t, got.Content != invalidToolResultContent, "settled content = %q, want the normalizer diagnostic, not generic object-parse text", got.Content)
		testassert.Equal(t, len(spy.received()), 0) // the call was never prepared
		testassert.Equal(t, n.attempts, 1)
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	})

	t.Run("a changed replacement runs one final normalization", func(t *testing.T) {
		h, store, c, sessionID, exec, spy, n := runReuseFixture(t, []ToolArgumentsHook{changedHook("reuse.changer", `{"x":2}`)})
		got := settleCall(t, h, c, store, sessionID, exec, rawCall("call-1", `[1,2]`))
		testassert.Require(t, got.Status == model.ResultSuccess, "settled result = %+v, want the repaired call prepared", got)
		testassert.Equal(t, n.attempts, 1)
		prepared := spy.received()
		testassert.Equal(t, len(prepared), 1)
		testassert.Require(t, string(prepared[0].Arguments) == `{"seq":"1","x":2}`, "preparer arguments = %q, want the freshly normalized replacement", string(prepared[0].Arguments))
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	})

	t.Run("an unchanged successor receives the raw replacement", func(t *testing.T) {
		var seen json.RawMessage
		h, store, c, sessionID, exec, spy, n := runReuseFixture(t, []ToolArgumentsHook{
			changedHook("reuse.changer", `{"x":2}`),
			passthroughHook("reuse.successor", &seen),
		})
		got := settleCall(t, h, c, store, sessionID, exec, rawCall("call-1", `{"x":1}`))
		testassert.Require(t, got.Status == model.ResultSuccess, "settled result = %+v, want the repaired call prepared", got)
		testassert.Require(t, string(seen) == `{"x":2}`, "successor input = %q, want the predecessor's raw replacement", string(seen))
		testassert.Equal(t, n.attempts, 1)
		prepared := spy.received()
		testassert.Equal(t, len(prepared), 1)
		testassert.Require(t, string(prepared[0].Arguments) == `{"seq":"1","x":2}`, "preparer arguments = %q, want the chain's accepted value, normalized exactly once", string(prepared[0].Arguments))
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	})

	t.Run("a settled invalid original never suppresses a valid batch sibling", func(t *testing.T) {
		var seen json.RawMessage
		h, store, c, sessionID, exec, spy, n := runReuseFixture(t, []ToolArgumentsHook{passthroughHook("reuse.pure", &seen)})
		results, err := runToolCalls(t, h, c, store, sessionID, context.Background(), testOpID, exec, testCapture(),
			[]model.ToolCall{rawCall("call-1", `[1,2]`), rawCall("call-2", `{"x":1}`)})
		testassert.NoError(t, err)
		testassert.Equal(t, n.attempts, 2)
		first, second := results[0], results[1]
		testassert.Require(t, first.Status == model.ResultError && first.Content == "normalize attempt 1 rejected",
			"call-1 result = %+v, want the normalizer diagnostic for the invalid original", first)
		testassert.Require(t, second.Status == model.ResultSuccess && second.Content == "done", "call-2 result = %+v, want the valid sibling prepared", second)
		prepared := spy.received()
		testassert.Require(t, len(prepared) == 1 && prepared[0].ID == "call-2" && string(prepared[0].Arguments) == `{"seq":"2","x":1}`,
			"prepared calls = %+v, want only the valid sibling over its own accepted normalization", prepared)
		testassert.Equal(t, n.attempts, 2) // neither settled call re-normalized anything
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	})
}
