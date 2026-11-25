// Contract coverage for publication-origin classification and origin rules at
// the pending-call effect boundaries: once an intent transaction is invoked it
// runs on a cancellation-free context, so every error from it — including a
// cancellation-shaped store error under a genuinely canceled execution — is a
// real store failure and a marked publication-origin failure that propagates
// with its original cause and the durable state retained for recovery, never
// followed by a guessed compensating write. Only a cancellation observed
// before the transaction was ever attempted may use a caller's ordinary
// interruption settlement. The same marking owns the result publications and
// their ErrNotFound guards.
package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// publicationFailureCauses are the two shapes one invoked publication
// transaction can return: an ordinary storage failure and a cancellation-
// shaped store error under a live execution.
func publicationFailureCauses() []error {
	return []error{
		fmt.Errorf("%w: store down", ErrStorage),
		fmt.Errorf("publication aborted mid-transaction: %w", context.Canceled),
	}
}

// executorTurnFixture builds one executor tool turn: round one publishes
// call-1, round two ends the turn, and the tool plan runs through the
// executor path that commits the pending-call effect intent.
func executorTurnFixture(t *testing.T) (*Harness, *graphStorage, *coordinator, string, PreparedExecution, context.CancelFunc, *int, *int, *int) {
	t.Helper()
	var rounds, preparedCalls, executed int
	modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		rounds++
		if rounds == 1 {
			return completedTurnStream(testToolCall("call-1")), nil
		}
		return completedTurnStream(), nil
	}
	planFn := func(ctx context.Context, call model.ToolCall) PreparedTool {
		preparedCalls++
		return PreparedTool{Permissions: fixturePermission, Execute: func(context.Context) ToolOutcome {
			executed++
			return ToolOutcome{Evidence: ExecutionObserved, Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
		}}
	}
	h, store, c, sessionID, prepared, cancel := newExecutionHarness(t, modelFn, planFn)
	return h, store, c, sessionID, prepared, cancel, &rounds, &preparedCalls, &executed
}

// failEffectRegister arms one replace_register fault on the Operation-register
// write that names the target effect boundary: the intent that makes want the
// active effect, or — with clear — the result publication that clears the
// active effect after that same intent committed.
func failEffectRegister(store *graphStorage, want func(*ActiveEffect) bool, clear bool, cause error, onFire func()) {
	intentSeen := false
	store.replaceHook = func(key RegisterKey, payload json.RawMessage) error {
		if key.Kind != RegisterOperation {
			return nil
		}
		op, err := decodeOperationRegister(Register{Key: key, Payload: payload})
		if err != nil {
			return nil
		}
		fire := false
		if op.State.ActiveEffect != nil && want(op.State.ActiveEffect) {
			intentSeen = true
			fire = !clear
		} else if clear && intentSeen && op.State.ActiveEffect == nil {
			fire = true
		}
		if fire {
			if onFire != nil {
				onFire()
			}
			return cause
		}
		return nil
	}
}

// assertRetainsRunningState proves the failed-publication durable shape: the
// Operation stays exactly Running with the given active effect, and no
// settlement or tool result write was fabricated.
func assertRetainsRunningState(t *testing.T, h *Harness, store *graphStorage, sessionID string, wantActiveEffect func(*ActiveEffect) bool) {
	t.Helper()
	rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, err)
	testassert.Require(t, rec.State.Status == OperationRunning, "operation state = %+v, want the running state retained for recovery", rec.State)
	testassert.Require(t, wantActiveEffect(rec.State.ActiveEffect), "operation active effect = %+v, want the shape the failed publication left committed", rec.State.ActiveEffect)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	for _, entry := range graph.Entries {
		testassert.Require(t, entry.Settlement == nil, "the failed publication fabricated a compensating settlement write")
		testassert.Require(t, entry.ToolResult == nil, "the failed publication fabricated a tool result write")
	}
}

// TestToolEffectIntentPublicationFailureRetainsRunningState proves the tool
// effect intent's origin boundary: an injected failure of the invoked intent
// transaction — plain storage or cancellation-shaped, under a live execution
// or a concurrent execution cancellation — propagates marked with its original
// cause, the executor never runs, no follow-up write lands, and the Operation
// stays Running for recovery.
func TestToolEffectIntentPublicationFailureRetainsRunningState(t *testing.T) {
	for _, concurrentCancel := range []bool{false, true} {
		for _, cause := range publicationFailureCauses() {
			name := cause.Error()
			if concurrentCancel {
				name += " under concurrent execution cancellation"
			}
			t.Run(name, func(t *testing.T) {
				h, store, c, sessionID, prepared, cancel, rounds, preparedCalls, executed := executorTurnFixture(t)
				defer cancel()
				failEffectRegister(store,
					func(ae *ActiveEffect) bool { return ae.Kind == EffectTool && ae.ToolCallID == "call-1" },
					false, cause, func() {
						if concurrentCancel {
							cancel()
						}
					})
				err := executeDirect(h, c, h.ctx, testOpID, prepared)
				store.txHook = nil
				testassert.Require(t, errors.Is(err, cause), "error = %v, want the original publication cause %v preserved", err, cause)
				testassert.Require(t, isPublicationFailure(err), "error = %v, want the publication-origin classification", err)
				testassert.Equal(t, *executed, 0) // no execution was started past the failed intent
				testassert.Equal(t, *preparedCalls, 1)
				testassert.Equal(t, *rounds, 1) // no compensating later dispatch
				assertRetainsRunningState(t, h, store, sessionID, func(ae *ActiveEffect) bool { return ae == nil })
			})
		}
	}
}

// TestToolResultPublicationFailureRetainsActiveEffect proves the tool result
// publication's marking on the executor path: after the committed intent and
// one real execution, a failed result transaction propagates marked with its
// cause, never replays the executor, and leaves the durable Running state
// carrying the tool effect's active effect for recovery.
func TestToolResultPublicationFailureRetainsActiveEffect(t *testing.T) {
	for _, cause := range publicationFailureCauses() {
		t.Run(cause.Error(), func(t *testing.T) {
			h, store, c, sessionID, prepared, cancel, _, _, executed := executorTurnFixture(t)
			defer cancel()
			failEffectRegister(store,
				func(ae *ActiveEffect) bool { return ae.Kind == EffectTool && ae.ToolCallID == "call-1" },
				true, cause, nil)
			err := executeDirect(h, c, h.ctx, testOpID, prepared)
			store.txHook = nil
			testassert.Require(t, errors.Is(err, cause), "error = %v, want the original publication cause %v preserved", err, cause)
			testassert.Require(t, isPublicationFailure(err), "error = %v, want the publication-origin classification", err)
			testassert.Equal(t, *executed, 1) // the executor ran exactly once; a failed result never replays it
			assertRetainsRunningState(t, h, store, sessionID, func(ae *ActiveEffect) bool {
				return ae != nil && ae.Kind == EffectTool && ae.ToolCallID == "call-1"
			})
		})
	}
}

// TestImmediateToolResultPublicationFailureRetainsNoActiveEffect proves the
// same result-publication marking on the immediate plan shape: no effect
// intent was ever committed, so the retained Running state carries no active
// effect, and the failed result transaction fabricates nothing.
func TestImmediateToolResultPublicationFailureRetainsNoActiveEffect(t *testing.T) {
	for _, cause := range publicationFailureCauses() {
		t.Run(cause.Error(), func(t *testing.T) {
			var rounds int
			modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				rounds++
				if rounds == 1 {
					return completedTurnStream(testToolCall("call-1")), nil
				}
				return completedTurnStream(), nil
			}
			planFn := func(ctx context.Context, call model.ToolCall) PreparedTool {
				return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}}
			}
			h, store, c, sessionID, prepared, cancel := newExecutionHarness(t, modelFn, planFn)
			defer cancel()
			store.entryHook = func(draft EntryDraft) error {
				if draft.Kind != EntryToolResult {
					return nil
				}
				entry, derr := decodeToolResultEntry(Entry{SessionID: draft.SessionID, ID: draft.ID, OperationID: draft.OperationID, Kind: draft.Kind, Payload: draft.Payload})
				if derr == nil && entry.ToolCallID == "call-1" {
					return cause
				}
				return nil
			}
			err := executeDirect(h, c, h.ctx, testOpID, prepared)
			store.txHook = nil
			testassert.Require(t, errors.Is(err, cause), "error = %v, want the original publication cause %v preserved", err, cause)
			testassert.Require(t, isPublicationFailure(err), "error = %v, want the publication-origin classification", err)
			testassert.Equal(t, rounds, 1) // no compensating later dispatch
			assertRetainsRunningState(t, h, store, sessionID, func(ae *ActiveEffect) bool { return ae == nil })
		})
	}
}

// TestHookEffectPublicationFailuresAtIntentAndResult proves the argument-hook
// effect's two publication boundaries with the same rule.
func TestCommitResultNotFoundBoundariesAreMarked(t *testing.T) {
	h, _, c, sessionID := newEffectHarness(t, nil)
	_, err := h.commitToolResult(context.Background(), c, "op-foreign", PendingToolCall{CallID: "call-1", AssistantEntry: EntryRef{SessionID: sessionID, EntryID: "00000000000000000000000000000001"}, ResultEntryID: "00000000000000000000000000000002"},
		ToolOutcome{Result: model.ToolResult{CallID: "call-1", Status: model.ResultSuccess, Content: "done"}, Evidence: ExecutionObserved}, nil, false, toolSelection{}, nil)
	testassert.Require(t, errors.Is(err, ErrNotFound), "commitToolResult = %v, want the original ErrNotFound cause", err)
	testassert.Require(t, isPublicationFailure(err), "commitToolResult error = %v, want the publication-origin classification", err)
}

func TestWaitRetainsPublicationFailureAtPendingCallBoundaries(t *testing.T) {
	t.Run("a marked tool-intent publication failure is retained", func(t *testing.T) {
		h, store, c, _, prepared, cancel, _, _, _ := executorTurnFixture(t)
		cause := fmt.Errorf("intent aborted: %w", context.DeadlineExceeded) // cancellation-shaped, live execution
		failEffectRegister(store,
			func(ae *ActiveEffect) bool { return ae.Kind == EffectTool && ae.ToolCallID == "call-1" },
			false, cause, nil)
		err := executeDirect(h, c, h.ctx, testOpID, prepared)
		store.txHook = nil
		testassert.Require(t, err != nil, "executeDirect = %v, want the publication failure", err)
		cancel()
		waitErr := h.Wait(context.Background())
		testassert.Require(t, errors.Is(waitErr, cause), "Wait = %v, want the retained publication cause", waitErr)
	})
	t.Run("a hook domain failure settles the call and never contaminates Wait", func(t *testing.T) {
		var rounds, preparedCalls int
		domainErr := errors.New("the hook could not inspect the call")
		modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			rounds++
			if rounds == 1 {
				return completedTurnStream(testToolCall("call-1")), nil
			}
			return completedTurnStream(), nil
		}
		planFn := func(ctx context.Context, call model.ToolCall) PreparedTool {
			preparedCalls++
			return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}}
		}
		h, store, c, sessionID, prepared, cancel := newOpenerHarness(t, func(context.Context, OperationAdmission) (Execution, error) {
			return Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
				inv := invocationExecution(modelFn, planFn)
				inv.ToolHooks = []ToolArgumentsHook{{ID: "cap.hook.domain", ExtensionID: "cap-a", Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
					return ToolArgumentsResult{}, domainErr
				}}}
				return inv, nil
			}}, nil
		})
		err := executeDirect(h, c, h.ctx, testOpID, prepared)
		testassert.NoError(t, err) // the domain failure settled the call as an ordinary error result
		_ = store
		cancel()
		waitErr := h.Wait(context.Background())
		testassert.Require(t, waitErr == nil, "Wait = %v, want no retained failure after a settled domain hook failure", waitErr)
		_ = sessionID
	})
	t.Run("a synchronous invalid admission never contaminates Wait", func(t *testing.T) {
		h, _, c, sessionID, _, cancel, _, _, _ := executorTurnFixture(t)
		_, err := h.Submit(context.Background(), SubmitRequest{SessionID: sessionID, OperationID: "op-invalid", Origin: InputOriginUser, Content: admissionContent("hello"), Mode: MessageMode("urgent")})
		testassert.Require(t, errors.Is(err, ErrInvalid), "invalid Submit = %v, want the synchronous rejection", err)
		_ = c
		cancel()
		waitErr := h.Wait(context.Background())
		testassert.Require(t, waitErr == nil, "Wait = %v, want no retained failure after a synchronous invalid admission", waitErr)
	})
}
