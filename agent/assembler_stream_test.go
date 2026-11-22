package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// roleConflictSteps is the pinned conflicting-role prefix: a completed
// assistant text fragment followed by a user-role delta. Only the final-state
// verdict may consume the pinned conflict.
func roleConflictSteps() []func() (model.StreamDelta, error) {
	return []func() (model.StreamDelta, error){
		deltaStep(model.StreamDelta{HasChoice: true, Role: "assistant", ContentFragments: []model.ContentFragment{txtPos(0, "x")}}),
		deltaStep(model.StreamDelta{HasChoice: true, Role: "user"}),
	}
}

// TestAssembleReadErrorLiveContext pins a non-EOF read failure on an accepted stream under a live run context and before any finish reason: no output is produced, the failure is a *StreamError carrying the last reported usage, and close still lands exactly once.
func TestAssembleReadErrorLiveContext(t *testing.T) {
	t.Run("failed-read", func(t *testing.T) {
		streamErr := assembleFailure(t, context.Background(), testRef,
			deltaStep(choiceDelta(txtPos(0, "x"))),
			usageStep(model.Usage{InputTokens: 1, OutputTokens: 2}),
			errStep(errors.New("boom")),
		)
		testassert.Require(t, streamErr.Err != nil && streamErr.Err.Error() == "boom", "StreamError = %v, want the read failure verbatim", streamErr)
		testassert.Require(t, streamErr.Usage != nil && *streamErr.Usage == (model.Usage{InputTokens: 1, OutputTokens: 2}), "StreamError usage = %#v, want the last reported usage", streamErr.Usage)
	})

	t.Run("broken-terminator", func(t *testing.T) { // a body that ends before [DONE] is the same stream failure at this boundary.
		truncated := fmt.Errorf("%w: stream body ended before [DONE]: %w", model.ErrStreamBroken, io.ErrUnexpectedEOF)
		streamErr := assembleFailure(t, context.Background(), testRef, deltaStep(choiceDelta(txtPos(0, "x"))), errStep(truncated))
		testassert.Require(t, errors.Is(streamErr, model.ErrStreamBroken), "StreamError = %v, want the broken-stream class reachable", streamErr)
	})

	t.Run("after-role-conflict", func(t *testing.T) { // the pinned conflict never preempts the stream failure before a finish reason.
		streamErr := assembleFailure(t, context.Background(), testRef, append(roleConflictSteps(), errStep(errors.New("boom")))...)
		testassert.Require(t, streamErr.Err != nil && streamErr.Err.Error() == "boom", "StreamError = %v, want the read failure verbatim", streamErr)
	})
}

// TestAssembleReadErrorCancelledContext pins the same read failure observed while the run context was already cancelled and no finish reason had arrived: classification flips to interrupted with non-empty detail, usage as reported, and a message only when text content or a refusal had started — reasoning, extras and opaque parts alone retain nothing.
func TestAssembleReadErrorCancelledContext(t *testing.T) {
	reasoning := model.StreamDelta{HasChoice: true, MessageExtra: model.Extra{"reasoning_content": json.RawMessage(`"thinking"`)}}
	rawExtra := model.StreamDelta{HasChoice: true, MessageExtra: model.Extra{"raw": json.RawMessage(`"keep"`)}}
	opaque := choiceDelta(model.ContentFragment{Position: 0, Kind: model.PartOpaque, OpaqueWireType: "thinking"})
	refusal := model.StreamDelta{HasChoice: true, RefusalFragment: "nope"}

	cases := []struct {
		name       string
		steps      []func() (model.StreamDelta, error)
		wantText   string
		wantRefuse string
		wantExtra  bool
		wantMsg    bool
	}{
		{"no-payload", []func() (model.StreamDelta, error){errStep(errors.New("boom"))}, "", "", false, false},
		{"with-payload", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), errStep(errors.New("boom"))}, "x", "", false, true}, // full text payload accumulated BEFORE the read failure fires
		{"text-with-reasoning-and-extras", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(reasoning), deltaStep(rawExtra), errStep(errors.New("boom"))}, "x", "", true, true},
		{"refusal-only", []func() (model.StreamDelta, error){deltaStep(refusal), errStep(errors.New("boom"))}, "", "nope", false, true},
		{"reasoning-only", []func() (model.StreamDelta, error){deltaStep(reasoning), errStep(errors.New("boom"))}, "", "", false, false},
		{"extras-only", []func() (model.StreamDelta, error){deltaStep(rawExtra), errStep(errors.New("boom"))}, "", "", false, false},
		{"opaque-only", []func() (model.StreamDelta, error){deltaStep(opaque), errStep(errors.New("boom"))}, "", "", false, false},
		{"after-role-conflict", append(roleConflictSteps(), errStep(errors.New("boom"))), "x", "", false, true}, // the pinned conflict never preempts the cancelled classification before a finish reason.
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background()) // build a fresh cancellable run context
			cancel()

			out, s := assemble(t, ctx, testRef, tc.steps...)
			expectStatus(t, out, model.OutputInterrupted) // A cancelled read failure classifies as interrupted, not errored.
			assertSingleClose(t, s)

			if !tc.wantMsg {
				testassert.Require(t, out.Message == nil, "payload-less interrupted output unexpectedly carried a message: %#v", out.Message)
				return
			}

			testassert.Require(t, out.Message != nil, "interrupted output dropped its started message")
			testassert.Equal(t, out.Message.TextContent(), tc.wantText)
			testassert.Equal(t, out.Message.Refusal, tc.wantRefuse)
			if tc.wantExtra {
				testassert.Require(t, string(out.Message.Extra["reasoning_content"]) == `"thinking"` && string(out.Message.Extra["raw"]) == `"keep"`, "interrupted message extras = %#v, want the raw reasoning and arbitrary extras", out.Message.Extra)
			}
		})
	}
}

// TestEstablishmentProtection pins the final-state protection rule end to end: once a non-empty finish reason arrived, later read errors, a broken terminator and cancellation no longer downgrade completion — semantic conflicts are the only things that still can.
func TestEstablishmentProtection(t *testing.T) {
	broken := fmt.Errorf("%w: stream body ended before [DONE]: %w", model.ErrStreamBroken, io.ErrUnexpectedEOF)
	cases := []struct {
		name   string
		steps  []func() (model.StreamDelta, error)
		cancel bool
	}{
		{"live-ctx", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(stopDelta()), errStep(errors.New("late-boom"))}, false},     // The run context stays live through the stream.
		{"cancelled-ctx", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(stopDelta()), errStep(errors.New("late-boom"))}, true}, // Cancellation still loses to an arrived finish reason.
		{"broken-terminator", []func() (model.StreamDelta, error){deltaStep(choiceDelta(txtPos(0, "x"))), deltaStep(stopDelta()), errStep(broken)}, false},             // A missing [DONE] after the finish reason changes nothing.
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}

			out, s := assemble(t, ctx, testRef, tc.steps...)
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

// TestAssembleCancelledReadErrorDropsIncompleteCalls pins interruption classification ahead of the incomplete-tool-call final validation: a non-EOF read fails before any finish reason while the run context is done, so the output is interrupted, started assistant content is retained, and every tool-call block is discarded.
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

// TestAssembleStopWithIncompleteCallErrorsAtFinalState pins that a finish reason is what protects an output from a later read error: stop beside an observed but structurally incomplete call slot still fails the final-state matrix, so the late read error and cancellation cannot turn that mismatch into an interruption — the output is errored with no message.
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
		testassert.Require(t, out.Message != nil && out.Message.TextContent() == "x", "interrupted output dropped its started text: %#v", out.Message)
	})
}

// TestEstablishedCompletionRevalidatedAtFinalState pins the final-state re-validation rule: a later read error or cancellation preserves completion ONLY while the finally accumulated semantic state still matches the finish reason — tool calls landing after a stop and a pinned role conflict are exactly such mismatches, so every variant flips to errored.
func TestEstablishedCompletionRevalidatedAtFinalState(t *testing.T) {
	lateCall := []func() (model.StreamDelta, error){
		deltaStep(choiceDelta(txtPos(0, "x"))),
		deltaStep(stopDelta()),
		deltaStep(toolDelta(toolFrag("a", "fnA"))), // A tool call lands after the stop finish reason.
		errStep(errors.New("late-boom")),
	}
	cases := []struct {
		name   string
		cancel bool // true pre-cancels the run context before assembly begins — completion must survive cancellation ONLY when final state still agrees with the finish reason
		steps  []func() (model.StreamDelta, error)
		detail string // the final-state verdict's own wording when the row pins it
	}{
		{"read-error-after-stop-with-late-call", false, lateCall, ""},  // The run context stays live through the stream.
		{"cancellation-after-stop-with-late-call", true, lateCall, ""}, // Cancellation does not rescue a final state that contradicts its own finish reason.
		{"read-error-after-stop-with-role-conflict", false, append(roleConflictSteps(), deltaStep(stopDelta()), errStep(errors.New("late-boom"))), "conflicting response roles"}, // The pinned conflict is consumed only by the final-state verdict.
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}

			out, s := assemble(t, ctx, testRef, tc.steps...) // The final state contradicts its finish reason, so completion must not be preserved.
			expectStatus(t, out, model.OutputErrored)
			assertSingleClose(t, s)

			testassert.Require(t, out.Message == nil, "errored output unexpectedly carried a message: %#v", out.Message)
			testassert.Require(t, len(msgCalls(out)) == 0, "errored output unexpectedly retained tool calls: %#v", out.Message)
			if tc.detail != "" {
				testassert.Require(t, strings.Contains(out.Detail, tc.detail), "detail = %q, want the final-state verdict's own wording %q", out.Detail, tc.detail)
			}
		})
	}
}
