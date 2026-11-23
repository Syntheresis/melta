package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestAssembleReadErrorLiveContext pins a non-EOF read failure before any finish reason under a live run context: no output, a *StreamError carrying the last reported usage, and exactly-once close.
func TestAssembleReadErrorLiveContext(t *testing.T) {
	streamErr := assembleFailure(t, context.Background(), testRef,
		deltaStep(choiceDelta(txtPos(0, "x"))),
		usageStep(model.Usage{InputTokens: 1, OutputTokens: 2}),
		errStep(errors.New("boom")),
	)
	testassert.Require(t, streamErr.Err != nil && streamErr.Err.Error() == "boom", "StreamError = %v, want the read failure verbatim", streamErr)
	testassert.Require(t, streamErr.Usage != nil && *streamErr.Usage == (model.Usage{InputTokens: 1, OutputTokens: 2}), "StreamError usage = %#v, want the last reported usage", streamErr.Usage)
}

// TestAssembleReadErrorRoleConflictLiveContext pins that a role conflict does not preempt the live-context stream failure.
func TestAssembleReadErrorRoleConflictLiveContext(t *testing.T) {
	streamErr := assembleFailure(t, context.Background(), testRef,
		deltaStep(model.StreamDelta{HasChoice: true, Role: "assistant"}),
		deltaStep(model.StreamDelta{HasChoice: true, Role: "user"}),
		errStep(errors.New("boom")),
	)
	testassert.Require(t, streamErr.Err != nil && streamErr.Err.Error() == "boom", "StreamError = %v, want the read failure verbatim", streamErr)
}

// TestAssembleReadErrorCancelledContext pins the same read failure before any finish reason under a done context: interrupted with a message only when text or refusal started — reasoning, extras and opaque parts alone retain nothing.
func TestAssembleReadErrorCancelledContext(t *testing.T) {
	cases := []struct {
		name     string
		steps    []func() (model.StreamDelta, error)
		text     string
		extra    model.Extra // the message extras the interrupted message keeps
		thinking bool        // whether the thinking opaque part is kept after the text part
	}{
		{"no-payload", []func() (model.StreamDelta, error){errStep(errors.New("boom"))}, "", nil, false},
		{"with-payload", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), errStep(errors.New("boom"))}, "x", nil, false},
		{"text-with-reasoning", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(model.StreamDelta{HasChoice: true, MessageExtra: model.Extra{"reasoning_content": json.RawMessage(`"thinking"`), "annotations": json.RawMessage(`"drop"`)}}), deltaStep(choiceDelta(model.ContentFragment{Position: 1, Kind: model.PartOpaque, OpaqueWireType: "server_tool_use"})), errStep(errors.New("boom"))}, "x", model.Extra{"reasoning_content": json.RawMessage(`"thinking"`)}, false},
		{"text-with-all-reasoning", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(model.StreamDelta{HasChoice: true, MessageExtra: model.Extra{"reasoning_content": json.RawMessage(`"content"`), "reasoning": json.RawMessage(`"reason"`), "reasoning_details": json.RawMessage(`[{"text":"details"}]`)}}), deltaStep(choiceDelta(model.ContentFragment{Position: 1, Kind: model.PartOpaque, OpaqueWireType: "thinking"}, model.ContentFragment{Position: 2, Kind: model.PartOpaque, OpaqueWireType: "redacted_thinking"})), errStep(errors.New("boom"))}, "x", model.Extra{"reasoning_content": json.RawMessage(`"content"`), "reasoning": json.RawMessage(`"reason"`), "reasoning_details": json.RawMessage(`[{"text":"details"}]`)}, true},
		{"non-text-payload", []func() (model.StreamDelta, error){deltaStep(model.StreamDelta{HasChoice: true, MessageExtra: model.Extra{"reasoning_content": json.RawMessage(`"thinking"`), "raw": json.RawMessage(`"keep"`)}}), deltaStep(choiceDelta(model.ContentFragment{Position: 0, Kind: model.PartOpaque, OpaqueWireType: "thinking"})), errStep(errors.New("boom"))}, "", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background()) // build a fresh cancellable run context
			cancel()

			out, s := assemble(t, ctx, testRef, tc.steps...)
			expectStatus(t, out, model.OutputInterrupted) // A cancelled read failure classifies as interrupted, not errored.
			assertSingleClose(t, s)

			if tc.text == "" {
				testassert.Require(t, out.Message == nil, "payload-less interrupted output unexpectedly carried a message: %#v", out.Message)
			} else {
				parts := msgContent(out)
				wantParts := 1
				if tc.thinking {
					wantParts = 2
				}
				// Interrupted classification must not discard the pre-failure payload.
				testassert.Require(t, len(parts) == wantParts && parts[0].Kind == model.PartText && parts[0].Text == tc.text, "interrupted output retained parts = %#v, want %d starting with text part %q", out.Message, wantParts, tc.text)
				if tc.thinking {
					testassert.Require(t, parts[1].Kind == model.PartOpaque && parts[1].OpaqueWireType == "thinking", "interrupted output retained part = %#v, want the thinking part", parts[1])
				}
				if tc.extra != nil {
					testassert.Require(t, len(out.Message.Extra) == len(tc.extra), "interrupted message extras = %#v, want %#v", out.Message.Extra, tc.extra)
					for key, want := range tc.extra {
						testassert.Require(t, string(out.Message.Extra[key]) == string(want), "interrupted message extra %q = %s, want %s", key, out.Message.Extra[key], want)
					}
				}
				testassert.Require(t, len(msgCalls(out)) == 0, "interrupted output retained tool calls: %#v", msgCalls(out))
			}
		})
	}
}

// TestEstablishmentProtection pins that a non-empty finish reason makes later read errors and cancellation use the final-state verdict: a valid stop/text state stays completed.
func TestEstablishmentProtection(t *testing.T) {
	cases := []struct {
		name   string
		cancel bool
	}{
		{"live-ctx", false},     // The run context stays live through the stream.
		{"cancelled-ctx", true}, // Cancellation still loses to an arrived finish reason.
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}

			out, s := assemble(t, ctx, testRef, deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(stopDelta()), errStep(errors.New("late-boom")))
			expectStatus(t, out, model.OutputCompleted)
			assertSingleClose(t, s)

			parts := msgContent(out)
			// Protected completion retains its full accumulated content.
			testassert.Require(t, len(parts) == 1 && parts[0].Text == "x", "protected completion retained parts = %#v, want one text part %q", out.Message, "x")
		})
	}
}

// TestAssembleUsagePropagation pins usage value semantics end to end: last reported chunk wins by value replacement (even when it arrives after the finish reason as a choice-less trailing delta), and streams that never report any usage yield nil on their outputs.
func TestAssembleUsagePropagation(t *testing.T) {
	t.Run("last-reported-wins", func(t *testing.T) {
		out, s := assemble(t, context.Background(), testRef, deltaStep(choiceDelta(txtPos(0, "x"))), usageStep(model.Usage{InputTokens: 1, OutputTokens: 2}), deltaStep(stopDelta()), usageStep(model.Usage{InputTokens: 3, CachedInputTokens: 4, OutputTokens: 5}))
		expectStatus(t, out, model.OutputCompleted)
		assertSingleClose(t, s)

		testassert.Require(t, out.Usage != nil, "completed output carries nil usage despite reported chunks")
		want := model.Usage{InputTokens: 3, CachedInputTokens: 4, OutputTokens: 5}
		testassert.Equal(t, *out.Usage, want)
	})

	t.Run("never-reported-stays-nil", func(t *testing.T) {
		out, s := assemble(t, context.Background(), testRef, deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(stopDelta()))
		expectStatus(t, out, model.OutputCompleted)
		assertSingleClose(t, s)

		if out.Usage != nil { // a NIL usage pointer MUST hold
			t.Fatalf("completed output unexpectedly carried usage: %#v", *out.Usage)
		}
	})
}

// TestAssembleCancelledReadErrorDropsIncompleteCalls pins interruption classification ahead of the incomplete-tool-call final validation: an error before any finish reason under a done context interrupts, retains started assistant content, and discards every tool-call block.
func TestAssembleCancelledReadErrorDropsIncompleteCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled so the cancellation observation at finalization is fully deterministic.

	out, s := assemble(t, ctx, testRef,
		deltaStep(choiceDelta(txtPos(0, "half"))),
		deltaStep(toolDelta(model.ToolCallFragment{ID: "a"})),                   // identity-only arrival: its name never completes.
		deltaStep(toolDelta(model.ToolCallFragment{ArgumentFragment: `{"x":`})), // anonymous continuation keeps that same slot structurally incomplete.
		errStep(errors.New("boom")),                                             // non-EOF read failure observed while the run context is already done.
	)

	expectStatus(t, out, model.OutputInterrupted) // the incomplete call block must not outrank the interrupted classification; discarding it is the contract, erroring is not.
	assertSingleClose(t, s)

	parts := msgContent(out)
	testassert.Require(t, len(parts) == 1 && parts[0].Text == "half", "interrupted output retained parts = %#v, want one text part %q", out.Message, "half")

	if len(msgCalls(out)) != 0 {
		t.Fatalf("interrupted output unexpectedly retained tool calls: %#v", out.Message.ToolCalls)
	}
}

// TestAssembleCleanEOFBeatsPreCancellation pins normal-EOF precedence over the cancellation check: a valid no-finish-reason payload terminated by EOF completes even when the run context was already done by finalization time — only a non-EOF read failure may classify as interrupted.
func TestAssembleCleanEOFBeatsPreCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled BEFORE assembly begins, so EOF processing is the only thing standing between this stream and an interruption.

	out, s := assemble(t, ctx, testRef, deltaStep(choiceDelta(txtPos(0, "x")))) // no finish-reason chunk; script exhaustion reports io.EOF.

	expectStatus(t, out, model.OutputCompleted) // EOF wins over the cancellation observation; no finish reason is needed for this completion shape.
	assertSingleClose(t, s)
}

// TestAssembleStopWithIncompleteCallErrorsAtFinalState pins that a finish reason sends a later read error through the final-state verdict: stop beside an observed incomplete call slot errors rather than interrupting.
func TestAssembleStopWithIncompleteCallErrorsAtFinalState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled so the cancellation observation at finalization is fully deterministic.

	out, s := assemble(t, ctx, testRef,
		deltaStep(choiceDelta(txtPos(0, "half"))),
		deltaStep(toolDelta(model.ToolCallFragment{ID: "a"})), // identity only: the slot is observed but structurally incomplete.
		deltaStep(stopDelta()),                                // stop beside an observed slot is a final-state mismatch.
		errStep(errors.New("boom")),
	)

	expectStatus(t, out, model.OutputErrored) // The final-state verdict decides, not the cancellation.
	assertSingleClose(t, s)

	testassert.Require(t, out.Message == nil, "errored output unexpectedly carried a message: %#v", out.Message)
	testassert.Require(t, strings.Contains(out.Detail, "tool call at position 0 is incomplete"), "detail = %q, want the incomplete tool-call conflict", out.Detail)
}

// TestAssembleCompletedSlotSurvivesCancelledReadError pins the positive sibling: a slot that completes on a later delta makes the tool_calls finish reason valid, and that completion then survives a cancelled read failure.
func TestAssembleCompletedSlotSurvivesCancelledReadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, s := assemble(t, ctx, testRef,
		deltaStep(choiceDelta(txtPos(0, "x"))),
		deltaStep(toolDelta(model.ToolCallFragment{ID: "a"})),            // incomplete at this instant: the finish reason would not be valid yet.
		deltaStep(toolDelta(model.ToolCallFragment{ID: "a", Name: "n"})), // same identity correlates back to the slot and completes it.
		deltaStep(toolCallsDelta()),                                      // the final-state verdict sees every observed slot complete with one valid call.
		errStep(errors.New("late-boom")),
	)

	expectStatus(t, out, model.OutputCompleted) // the valid finish reason protects the completion from the cancelled read failure.
	assertSingleClose(t, s)

	calls := msgCalls(out)
	testassert.Require(t, len(calls) == 1 && calls[0].ID == "a" && calls[0].Name == "n", "calls = %#v, want the single completed call retained", calls)
}

func TestAssembleCleanupCloseError(t *testing.T) {
	s := newFakeStream(deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(stopDelta())).withCloseError(errors.New("close-fail")) // Close deliberately fails; the receive script is complete.
	out, err := Assemble(context.Background(), testRef, s)

	testassert.NoError(t, err)

	expectStatus(t, out, model.OutputCompleted) // A close failure must not alter the classification derived from the consumed wire data.
	assertSingleClose(t, s)
}

// TestAssembleRefusalAccumulationAndRetention pins refusal fragments concatenating in arrival order without separators: a stop-completed response carries the full joined string on its message, and an interrupted stream with no other payload retains it as eligible partial content.
func TestAssembleRefusalAccumulationAndRetention(t *testing.T) {
	t.Run("completed-carries-joined-refusal", func(t *testing.T) {
		out, s := assemble(t, context.Background(), testRef, deltaStep(model.StreamDelta{HasChoice: true, RefusalFragment: "I cannot"}), deltaStep(choiceDelta(txtPos(0, ""))), deltaStep(stopDelta()))
		expectStatus(t, out, model.OutputCompleted) // A refusal alone satisfies the completed-output payload rule.
		assertSingleClose(t, s)

		testassert.Require(t, out.Message != nil && msgRefusal(out) == "I cannot", "completed output refusal = %q, want %q", msgRefusal(out), "I cannot")
	})

	t.Run("interrupted-retains-refusal", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		out, s := assemble(t, ctx, testRef, deltaStep(model.StreamDelta{HasChoice: true, RefusalFragment: "nope"}), errStep(errors.New("boom")))
		expectStatus(t, out, model.OutputInterrupted) // A cancelled read failure classifies as interrupted.
		assertSingleClose(t, s)

		testassert.Require(t, out.Message != nil && msgRefusal(out) == "nope", "interrupted output retained refusal = %q, want %q", msgRefusal(out), "nope")
	})
}

// TestAssembleAcceptedProtocolReadError pins that accepted-stream protocol failures (complete events wrapping model.ErrProtocol) flow through the same read-error classification as every other non-EOF failure: live context is a *StreamError, cancelled context an interruption — no special-casing of this error class anywhere in assembly itself.
func TestAssembleAcceptedProtocolReadError(t *testing.T) {
	protoErr := fmt.Errorf("%w: malformed sse framing on event 3", model.ErrProtocol)
	t.Run("live-context-stream-error", func(t *testing.T) {
		streamErr := assembleFailure(t, context.Background(), testRef, deltaStep(choiceDelta(txtPos(0, "x"))), errStep(protoErr))
		testassert.Require(t, errors.Is(streamErr, model.ErrProtocol), "StreamError = %v, want the protocol class reachable", streamErr)
	})

	t.Run("cancelled-context-interrupted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		out, s := assemble(t, ctx, testRef, deltaStep(choiceDelta(txtPos(0, "x"))), errStep(protoErr))
		expectStatus(t, out, model.OutputInterrupted) // The same failure under an already-cancelled context is an interruption.
		assertSingleClose(t, s)
	})
}

// TestEstablishedCompletionRevalidatedAtFinalState pins the final-state re-validation rule: a later read error or cancellation preserves completion ONLY while the finally accumulated semantic state still matches the finish reason — tool calls landing after a stop are such a mismatch, so both variants flip to errored.
func TestEstablishedCompletionRevalidatedAtFinalState(t *testing.T) {
	cases := []struct {
		name   string
		cancel bool // true pre-cancels the run context before assembly begins — completion must survive cancellation ONLY when final state still agrees with the finish reason
	}{
		{"read-error-after-stop-with-late-call", false},  // The run context stays live through the stream.
		{"cancellation-after-stop-with-late-call", true}, // Cancellation does not rescue a final state that contradicts its own finish reason.
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}

			out, s := assemble(t, ctx, testRef, deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(stopDelta()), deltaStep(toolDelta(toolFrag("a", "fnA"))), errStep(errors.New("late-boom"))) // A tool call lands after the stop finish reason.
			expectStatus(t, out, model.OutputErrored)                                                                                                                                         // The final state contradicts its finish reason, so completion must not be preserved.
			assertSingleClose(t, s)

			testassert.Require(t, out.Message == nil, "errored output unexpectedly carried a message: %#v", out.Message)
			testassert.Require(t, len(msgCalls(out)) == 0, "errored output unexpectedly retained tool calls: %#v", out.Message)
		})
	}
}
