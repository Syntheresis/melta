package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// Delivery fixture identities: fixed durable session ids for the parent and
// child of the completion fixtures, one 8-hex job identity, and the child
// fixture's Operation identity.
const (
	completionParentID = "000000000000000000000000000000c2"
	completionChildID  = "000000000000000000000000000000c1"
	jobFixtureID       = "deadbeef"
	childFixtureOp     = "child-op"
)

// cachedCoordinator returns the registry coordinator of one Session. It takes
// no use: a fixture that inspects or mutates the coordinator's graph state
// takes its own explicit lease instead.
func cachedCoordinator(t *testing.T, h *Harness, sessionID string) *coordinator {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.sessions[sessionID]
	testassert.Require(t, c != nil, "no cached coordinator for session %q", sessionID)
	return c
}

// closeBackgroundMode forces the not-receivable closed mode on one
// coordinator, the shape a stopping or closed group presents.
func closeBackgroundMode(c *coordinator) {
	c.mu.Lock()
	c.memberStop = &backgroundMember{done: make(chan struct{})}
	c.mu.Unlock()
}

// completionChildGraph builds one child Session fixture whose single
// Operation carries the given state: running, or terminal with the given
// status and detail. assistantText, when non-empty, commits one assistant
// entry with that text belonging to the Operation.
func completionChildGraph(parentID string, running bool, status OperationState, detail, assistantText string) *testGraph {
	session := validSessionRecord()
	session.Identity.SessionID = completionChildID
	session.Identity.OwnerSessionID = parentID
	if running {
		session.State.CurrentOperationID = childFixtureOp
	}
	op := validOperationRecord()
	op.Admission.SessionID = completionChildID
	op.Admission.OperationID = childFixtureOp
	op.Admission.AdmittedEntry = EntryRef{SessionID: completionChildID, EntryID: hexID(1)}
	input := validInputEntry(childFixtureOp)
	input.SessionID = completionChildID
	input.EntryID = hexID(1)
	entries := []testEntry{
		{env: Entry{SessionID: completionChildID, ID: hexID(1), OperationID: childFixtureOp, Kind: EntryInput, Sequence: 1, CommittedAt: testTime}, input: &input},
	}
	if assistantText != "" {
		assistant := validAssistantEntry(childFixtureOp)
		assistant.SessionID = completionChildID
		assistant.EntryID = hexID(2)
		assistant.RequestID = hexID(2)
		assistant.Content = []model.ContentPart{{Kind: model.PartText, Text: assistantText}}
		entries = append(entries, testEntry{env: Entry{SessionID: completionChildID, ID: hexID(2), OperationID: childFixtureOp, Kind: EntryAssistant, Sequence: 2, CommittedAt: testTime}, assistant: &assistant})
	}
	if !running {
		stamped := testTime
		op.State.Status = status
		op.State.SettledAt = &stamped
		op.State.Terminal = &OperationTerminal{SettlementEntry: EntryRef{SessionID: completionChildID, EntryID: hexID(3)}, Detail: detail}
		settlement := validSettlementEntry()
		settlement.SessionID = completionChildID
		settlement.EntryID = hexID(3)
		settlement.OperationID = childFixtureOp
		settlement.Status = status
		settlement.Detail = detail
		entries = append(entries, testEntry{env: Entry{SessionID: completionChildID, ID: hexID(3), OperationID: childFixtureOp, Kind: EntryOperationSettlement, Sequence: 3, CommittedAt: testTime}, settlement: &settlement})
	}
	return &testGraph{session: session, ops: []OperationRecord{op}, entries: entries}
}

// addRootSession inserts one extra root Session register into the store, so
// fixed-identity child fixtures gain a delivery target parent.
func addRootSession(t *testing.T, store *graphStorage, sessionID string) {
	t.Helper()
	rec := validSessionRecord()
	rec.Identity.SessionID = sessionID
	raw, err := encodeSessionRegister(rec)
	testassert.NoError(t, err)
	{
		err := store.Transact(context.Background(), func(tx Transaction) error {
			_, err := tx.InsertRegister(RegisterDraft{Key: RegisterKey{SessionID: sessionID, Kind: RegisterSession}, Payload: raw})
			return err
		})
		testassert.Require(t, err == nil, "insert session %q: %v", sessionID, err)
	}
}

// storedSignals reads every background_completion signal entry of one Session
// straight from durable storage, in committed order.
func storedSignals(t *testing.T, store *graphStorage, sessionID string) []signalEntry {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	var out []signalEntry
	for _, env := range store.entries[sessionID] {
		if env.Kind != EntrySignal {
			continue
		}
		v, err := decodeSignalEntry(env)
		testassert.Require(t, err == nil, "decode signal entry %s: %v", env.ID, err)
		if v.Signal == SignalBackgroundCompletion {
			out = append(out, v)
		}
	}
	return out
}

// storedEntryCount reads one Session's durable entry count.
func storedEntryCount(store *graphStorage, sessionID string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.entries[sessionID])
}

// storedOperationExists reports whether one Operation register is durable.
func storedOperationExists(store *graphStorage, sessionID, operationID string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, reg := range store.registers[sessionID] {
		if reg.Key.Kind == RegisterOperation && reg.Key.OperationID == operationID {
			return true
		}
	}
	return false
}

// waitMemberDone joins one member's terminal transition.
func waitMemberDone(t *testing.T, m *backgroundMember) {
	t.Helper()
	select {
	case <-m.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("member done channel did not close")
	}
}

// runtimeInputText returns the text of one Session's runtime-origin input
// entry, read from the validated graph.
func runtimeInputText(t *testing.T, store *graphStorage, sessionID string) string {
	t.Helper()
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	for _, entry := range graph.Entries {
		if entry.Input != nil && entry.Input.Origin == InputOriginRuntime {
			return entry.Input.Content[0].Text
		}
	}
	t.Fatalf("session %q carries no runtime-origin input entry", sessionID)
	return ""
}

// archiveDuringPrepare returns a preparation callback that archives the
// prepared admission's own Session on the given call, then returns the result.
// The callback may run on the execution's goroutine, so it reports the archive
// failure through its own error instead of the test.
func archiveDuringPrepare(store *graphStorage, call int, result PreparedExecution) func(context.Context, PreparationRequest) (PreparedExecution, error) {
	calls := 0
	return func(_ context.Context, req PreparationRequest) (PreparedExecution, error) {
		calls++
		if calls == call {
			err := store.Transact(context.Background(), func(tx Transaction) error {
				return foreignArchiveChange(tx, req.Session.Identity.SessionID)
			})
			if err != nil {
				return PreparedExecution{}, err
			}
		}
		return result, nil
	}
}

// TestDeliverBackgroundCompletionModes proves the delivery priority: a
// not-receivable parent takes the durable signal entry regardless of
// activity, a receivable active parent steers, a receivable idle parent
// admits under the completion identity, and a semantic refusal of that
// admission becomes the same operationless signal instead of an error. It also
// covers a report-free settlement, a stop waiting on the member, and a signal
// write that fails without a fallback write.
func TestDeliverBackgroundCompletionModes(t *testing.T) {
	t.Run("closed during harness cancellation with a settling run", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript()
		script.gate = make(chan struct{})
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		session := createSession(t, h)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)
		{
			_, err := submitText(t, h, session, "op-1", MessageModeRegular, "hello")
			testassert.NoError(t, err)
		}
		<-script.arrived // the run is parked at its model boundary
		cancel()         // harness loss while the run still settles

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "lost work")
			testassert.NoError(t, err)
		}
		signals := storedSignals(t, store, session)
		testassert.Equal(t, len(signals), 1)
		sig := signals[0]
		testassert.Require(t, sig.OperationID == "" && sig.RelatedMember != nil && sig.RelatedMember.Kind == "session" && sig.RelatedMember.ID == completionChildID && sig.Content == "lost work", "committed signal = %+v, want the operationless child-member completion", sig)
		waitMemberDone(t, member)

		script.releaseGate() // let the parked run converge
		c.mu.Lock()
		run := c.run
		c.mu.Unlock()
		if run != nil {
			select {
			case <-run.done:
			case <-time.After(2 * time.Second):
				t.Fatalf("the settling run did not converge")
			}
		}
	})

	t.Run("idle admits under the completion identity without a signal entry", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		session := createSession(t, h)
		watch := watchSettlements(store)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "background answer")
			testassert.NoError(t, err)
		}
		waitMemberDone(t, member)
		watch.next() // the admitted completion settles

		{
			rec := settledOperation(t, store, session, member.completionID)
			testassert.Equal(t, rec.State.Status, OperationSuccess)
		}
		{
			signals := storedSignals(t, store, session)
			testassert.Equal(t, len(signals), 0)
		}
		{
			got := runtimeInputText(t, store, session)
			testassert.Equal(t, got, "background answer")
		}
	})

	t.Run("steering delivers through the completion identity at the boundary", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript()
		script.gate = make(chan struct{})
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		session := createSession(t, h)
		watch := watchSettlements(store)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)
		{
			_, err := submitText(t, h, session, "op-1", MessageModeRegular, "hello")
			testassert.NoError(t, err)
		}
		<-script.arrived // the operation is parked at its model boundary

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "steered completion")
			testassert.NoError(t, err)
		}
		waitMemberDone(t, member)
		{
			n := storedEntryCount(store, session)
			testassert.Equal(t, n, 1)
		}

		script.releaseGate()
		<-script.arrived // the completion identity's own Operation carries the steered completion
		{
			texts := strings.Join(script.lastTexts(), "|")
			testassert.Require(t, strings.Contains(texts, "steered completion"), "continuation projection = %q, want the steered completion", texts)
		}
		watch.next()
		{
			rec := settledOperation(t, store, session, "op-1")
			testassert.Equal(t, rec.State.Status, OperationSuccess)
		}
		// The delivered completion runs as its own successor Operation: wait
		// for its terminal commit and Session quiet before raw graph reads.
		awaitOperationTerminal(t, store, session, member.completionID, OperationSuccess)
		awaitHarnessQuiet(t, h, session)
		{
			got := entryTexts(t, store, session)
			testassert.Require(t, reflect.DeepEqual(got, []string{"hello", "steered completion"}), "committed inputs = %q, want the steered completion after the first input", got)
		}
		{
			got := runtimeInputText(t, store, session)
			testassert.Equal(t, got, "steered completion")
		}
	})

	t.Run("steering admits under the completion identity when the operation settles first", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript(payloadlessTurn())
		script.gate = make(chan struct{})
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		session := createSession(t, h)
		watch := watchSettlements(store)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)
		{
			_, err := submitText(t, h, session, "op-1", MessageModeRegular, "hello")
			testassert.NoError(t, err)
		}
		<-script.arrived

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "queued completion")
			testassert.NoError(t, err)
		}
		waitMemberDone(t, member)
		script.releaseGate() // op-1 settles failure; the preserved steering drains through admission
		watch.next()         // op-1's settlement
		watch.next()         // the drain-admitted completion's settlement

		{
			rec := settledOperation(t, store, session, member.completionID)
			testassert.Equal(t, rec.State.Status, OperationSuccess)
		}
		{
			got := runtimeInputText(t, store, session)
			testassert.Equal(t, got, "queued completion")
		}
	})

	t.Run("idle refusal becomes the operationless signal and releases the member", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, PreparedExecution{}, archiveDuringPrepare(store, 1, modelPrepared(script.model)))
		defer cancel()
		session := createSession(t, h)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "refused report")
			testassert.NoError(t, err) // the refusal is delivered as the signal, not returned as a failure
		}
		waitMemberDone(t, member)
		signals := storedSignals(t, store, session)
		testassert.Require(t, len(signals) == 1 && signals[0].RelatedMember != nil && signals[0].RelatedMember.Kind == "session" && signals[0].RelatedMember.ID == completionChildID && signals[0].Content == "refused report", "committed signal = %+v, want the refused completion's operationless record", signals)
		testassert.Require(t, storedEntryCount(store, session) == 1, "the refused delivery committed %d entries, want only the signal", storedEntryCount(store, session))
		testassert.Require(t, !storedOperationExists(store, session, member.completionID), "the refused completion admitted an Operation")
		c.mu.Lock()
		live, lifecycle := liveBackground(c), c.graph.Session.State.Lifecycle
		c.mu.Unlock()
		testassert.Require(t, !live, "the refused completion left its member live")
		// The archived record proves the delivery reached the admission
		// decision instead of taking the not-receivable closed path.
		testassert.Equal(t, lifecycle, LifecycleArchived)
	})

	t.Run("preparation refusal becomes the operationless signal and releases the member", func(t *testing.T) {
		store := emptyStore(t)
		h, cancel := newCancelableHarness(t, store, PreparedExecution{}, func(context.Context, PreparationRequest) (PreparedExecution, error) {
			return PreparedExecution{}, fmt.Errorf("no model configured: %w", ErrInvalid) // the production preparation refusal
		})
		defer cancel()
		session := createSession(t, h)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "refused report")
			testassert.NoError(t, err) // the prepublication refusal is delivered as the signal, not returned as a failure
		}
		waitMemberDone(t, member)
		signals := storedSignals(t, store, session)
		testassert.Require(t, len(signals) == 1 && signals[0].RelatedMember != nil && signals[0].RelatedMember.Kind == "session" && signals[0].RelatedMember.ID == completionChildID && signals[0].Content == "refused report", "committed signal = %+v, want the refused completion's operationless record", signals)
		testassert.Require(t, storedEntryCount(store, session) == 1, "the refused delivery committed %d entries, want only the signal", storedEntryCount(store, session))
		testassert.Require(t, !storedOperationExists(store, session, member.completionID), "the refused completion admitted an Operation")
		testassert.Require(t, storedSignals(t, store, session)[0].OperationID == "", "the preparation refusal committed an Operation-bound signal")
		c.mu.Lock()
		live := liveBackground(c)
		c.mu.Unlock()
		testassert.Require(t, !live, "the refused completion left its member live")
	})

	t.Run("untyped preparation refusal becomes the signal and is not latched", func(t *testing.T) {
		store := emptyStore(t)
		h, cancel := newCancelableHarness(t, store, PreparedExecution{}, func(context.Context, PreparationRequest) (PreparedExecution, error) {
			return PreparedExecution{}, errors.New("preparation broke") // ordinary, untyped producer failure before publication
		})
		defer cancel()
		session := createSession(t, h)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "refused report")
			testassert.NoError(t, err)
		}
		waitMemberDone(t, member)
		signals := storedSignals(t, store, session)
		testassert.Require(t, len(signals) == 1 && signals[0].RelatedMember != nil && signals[0].RelatedMember.Kind == "session" && signals[0].RelatedMember.ID == completionChildID && signals[0].Content == "refused report", "committed signal = %+v, want the untyped refusal's operationless record", signals)
		testassert.Require(t, !storedOperationExists(store, session, member.completionID), "the refused completion admitted an Operation")
		h.mu.Lock()
		latched := h.storageFailure
		h.mu.Unlock()
		testassert.Require(t, latched == nil, "known non-admission latched %v, want nothing", latched)
	})

	t.Run("preparation storage failure propagates and latches without a signal", func(t *testing.T) {
		store := emptyStore(t)
		h, cancel := newCancelableHarness(t, store, PreparedExecution{}, func(context.Context, PreparationRequest) (PreparedExecution, error) {
			return PreparedExecution{}, fmt.Errorf("%w: injected preparation storage failure", ErrStorage)
		})
		defer cancel()
		session := createSession(t, h)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)

		err = h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "refused report")
		testassert.Require(t, errors.Is(err, ErrStorage) && strings.Contains(err.Error(), "injected preparation storage failure"), "delivery = %v, want the preparation storage class", err)
		waitMemberDone(t, member)
		testassert.Require(t, len(storedSignals(t, store, session)) == 0, "a preparation storage failure committed a signal")
		testassert.Require(t, storedEntryCount(store, session) == 0, "the preparation storage failure committed %d entries", storedEntryCount(store, session))
		h.mu.Lock()
		latched := h.storageFailure
		h.mu.Unlock()
		testassert.Require(t, errors.Is(latched, ErrStorage), "latched failure = %v, want the preparation storage class", latched)
	})

	t.Run("a report-free child settles its member without a parent entry", func(t *testing.T) {
		store := completionChildGraph(completionParentID, false, OperationSuccess, "", "child text").storage(t)
		addRootSession(t, store, completionParentID)
		h := newTestHarness(t, store, nil)
		cP := leaseCoordinator(t, h, completionParentID)
		cC := leaseCoordinator(t, h, completionChildID)
		member, err := admitLocked(h, cP, memberSession, completionChildID)
		testassert.NoError(t, err)
		cC.mu.Lock()
		cC.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: 1024}
		cC.mu.Unlock()

		h.childCompletionSettled(cC, "")
		waitMemberDone(t, member)
		testassert.Require(t, storedEntryCount(store, completionParentID) == 0, "the report-free child delivered %d parent entries", storedEntryCount(store, completionParentID))
		testassert.Require(t, len(storedSignals(t, store, completionParentID)) == 0, "the report-free child committed a signal")
		cC.mu.Lock()
		pending := cC.pendingCompletion
		cC.mu.Unlock()
		testassert.Require(t, pending == nil, "the report-free obligation survived settlement")
		cP.mu.Lock()
		live := liveBackground(cP)
		cP.mu.Unlock()
		testassert.Require(t, !live, "the report-free member stayed live")
	})

	t.Run("a stop blocked on the member still admits the report", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		session := createSession(t, h)
		watch := watchSettlements(store)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)

		stopDone := make(chan error, 1)
		go func() { stopDone <- h.Stop(context.Background(), session) }()
		awaitStopInterval(t, c, stopDone) // the stop waits on the live member

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "report during the stop")
			testassert.NoError(t, err)
		}
		waitMemberDone(t, member)
		{
			err := stopResult(t, stopDone)
			testassert.NoError(t, err)
		}
		watch.next() // the admitted report's own Operation settles
		{
			got := runtimeInputText(t, store, session)
			testassert.Equal(t, got, "report during the stop")
		}
		testassert.Require(t, len(storedSignals(t, store, session)) == 0, "the receivable parent recorded a signal instead of the input")
	})

	t.Run("refusal signal storage failure is latched with no fallback write", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, PreparedExecution{}, archiveDuringPrepare(store, 1, modelPrepared(script.model)))
		defer cancel()
		session := createSession(t, h)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)
		store.entryHook = func(draft EntryDraft) error {
			if draft.Kind == EntrySignal {
				return fmt.Errorf("%w: injected signal failure", ErrStorage)
			}
			return nil
		}

		err = h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "refused report")
		store.entryHook = nil
		testassert.Require(t, errors.Is(err, ErrStorage), "delivery = %v, want the failed signal write's storage class", err)
		waitMemberDone(t, member)
		testassert.Require(t, storedEntryCount(store, session) == 0, "the failed signal write left %d entries", storedEntryCount(store, session))
		h.mu.Lock()
		latched := h.storageFailure != nil
		h.mu.Unlock()
		testassert.Require(t, latched, "the failed signal write was not retained")
		c.mu.Lock()
		live := liveBackground(c)
		c.mu.Unlock()
		testassert.Require(t, !live, "the failed signal write left its member live")
	})
}

// TestDeliverBackgroundCompletionEmptyContentRejected proves empty completion
// content is rejected before any obligation is claimed.
func TestDeliverBackgroundCompletionEmptyContentRejected(t *testing.T) {
	h := newTestHarness(t, freshSessionStore(t), nil)
	c := leaseCoordinator(t, h, testSessionID)
	member, err := admitLocked(h, c, memberSession, completionChildID)
	testassert.NoError(t, err)
	{
		err := h.DeliverBackgroundCompletion(context.Background(), testSessionID, member.completionID, "")
		testassert.Require(t, errors.Is(err, ErrInvalid), "empty content = %v, want ErrInvalid", err)
	}
	c.mu.Lock()
	claimed := member.claimed
	c.mu.Unlock()
	testassert.Require(t, !claimed, "the rejected delivery claimed the member")
}

// TestDeliverBackgroundCompletionSecondDeliveryIsNoOp proves the first-writer
// claim: after the first delivery's own Operation completes stably, a second
// delivery of the same completion changes no durable state, causes no second
// model call, and commits no parent signal.
func TestDeliverBackgroundCompletionSecondDeliveryIsNoOp(t *testing.T) {
	store := emptyStore(t)
	script := newModelScript()
	h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
	defer cancel()
	session := createSession(t, h)
	watch := watchSettlements(store)
	c := leaseCoordinator(t, h, session)
	member, err := admitLocked(h, c, memberSession, completionChildID)
	testassert.NoError(t, err)

	{
		err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "first")
		testassert.NoError(t, err)
	}
	waitMemberDone(t, member)
	watch.next() // the delivered completion's own Operation settles
	awaitOperationTerminal(t, store, session, member.completionID, OperationSuccess)
	{
		calls := len(script.seen())
		testassert.Equal(t, calls, 1)
	}
	beforeEntries, beforeRegs := storedSessionState(store, session)

	{
		err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "second")
		testassert.NoError(t, err)
	}

	afterEntries, afterRegs := storedSessionState(store, session)
	testassert.Require(t, reflect.DeepEqual(beforeEntries, afterEntries) && reflect.DeepEqual(beforeRegs, afterRegs), "the duplicate delivery changed the durable registers or entries")
	{
		calls := len(script.seen())
		testassert.Equal(t, calls, 1)
	}
	{
		signals := storedSignals(t, store, session)
		testassert.Require(t, len(signals) == 0, "the duplicate committed signal entries %+v, want no parent signal", signals)
	}

	{
		got := entryTexts(t, store, session)
		testassert.Require(t, reflect.DeepEqual(got, []string{"first"}), "committed inputs = %q, want exactly the first delivery", got)
	}
	testassert.Require(t, storedOperationExists(store, session, member.completionID), "the completion identity was not admitted")

	// A refused completion follows the same first-writer rule: its signal is
	// the one durable record, and a duplicate delivery changes nothing.
	t.Run("refused completion duplicate is a no-op", func(t *testing.T) {
		store := emptyStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, PreparedExecution{}, archiveDuringPrepare(store, 1, modelPrepared(script.model)))
		defer cancel()
		session := createSession(t, h)
		c := leaseCoordinator(t, h, session)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)

		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "refused")
			testassert.NoError(t, err)
		}
		waitMemberDone(t, member)
		beforeEntries, beforeRegs := storedSessionState(store, session)
		{
			err := h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "second")
			testassert.NoError(t, err)
		}
		afterEntries, afterRegs := storedSessionState(store, session)
		testassert.Require(t, reflect.DeepEqual(beforeEntries, afterEntries) && reflect.DeepEqual(beforeRegs, afterRegs), "the duplicate delivery changed the durable registers or entries")
		signals := storedSignals(t, store, session)
		testassert.Require(t, len(signals) == 1 && signals[0].Content == "refused", "committed signals = %+v, want the one first-writer record", signals)
	})
}

// TestDeliverBackgroundCompletionCorruptParent proves an unavailable parent
// returns the ordinary corruption error with no durable write, closes the
// claimed member once for both producers, and a duplicate callback is a
// no-op.
func TestDeliverBackgroundCompletionCorruptParent(t *testing.T) {
	store := freshSessionStore(t)
	h := newTestHarness(t, store, nil)
	c := leaseCoordinator(t, h, testSessionID)
	childMember, err := admitLocked(h, c, memberSession, completionChildID)
	testassert.NoError(t, err)
	jobMember, err := admitLocked(h, c, memberJob, jobFixtureID)
	testassert.NoError(t, err)
	h.mu.Lock()
	c.corru = corruptSession(testSessionID, "injected corruption")
	h.mu.Unlock()

	for _, member := range []*backgroundMember{childMember, jobMember} {
		err := h.DeliverBackgroundCompletion(context.Background(), testSessionID, member.completionID, "x")
		var corrupt *CorruptionError
		testassert.Require(t, errors.As(err, &corrupt), "%s delivery = %v, want a corruption error", member.kind, err)
	}
	entries, regs := storedSessionState(store, testSessionID)
	testassert.Require(t, len(entries) == 0 && len(regs) == 1, "corrupt-parent delivery wrote entries %d registers %d, want none beyond the session register", len(entries), len(regs))
	waitMemberDone(t, childMember)
	waitMemberDone(t, jobMember)

	{
		err := h.DeliverBackgroundCompletion(context.Background(), testSessionID, childMember.completionID, "x")
		testassert.NoError(t, err)
	}
}

// TestDeliverBackgroundCompletionIdleRaceReRoutesToSteering proves the idle
// race: a completion that resolved idle takes the steering path when an
// operation becomes active before its reservation is granted.
func TestDeliverBackgroundCompletionIdleRaceReRoutesToSteering(t *testing.T) {
	store := emptyStore(t)
	script := newModelScript()
	script.gate = make(chan struct{})
	stub := newPrepareStub(modelPrepared(script.model))
	stub.gate = make(chan struct{})
	h, cancel := newCancelableHarness(t, store, PreparedExecution{}, stub.prepare)
	defer cancel()
	session := createSession(t, h)
	c := leaseCoordinator(t, h, session)
	member, err := admitLocked(h, c, memberSession, completionChildID)
	testassert.NoError(t, err)

	submitted := make(chan error, 1)
	go func() {
		_, err := h.Submit(context.Background(), SubmitRequest{
			SessionID:   session,
			OperationID: "op-1",
			Origin:      InputOriginUser,
			Content:     admissionContent("hello"),
			Mode:        MessageModeRegular,
		})
		submitted <- err
	}()
	<-stub.arrived // the submission parks in preparation, holding the reservation

	delivered := make(chan error, 1)
	go func() {
		delivered <- h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "raced")
	}()
	close(stub.gate) // the submission publishes, installs its run, releases the reservation
	{
		err := <-submitted
		testassert.NoError(t, err)
	}
	{
		err := <-delivered
		testassert.NoError(t, err)
	}

	// The rerouted completion is steering, never a separate admission while
	// it waits: it is either still buffered or already delivered through its
	// own successor admission after the boundary exit.
	testassert.Require(t, !storedOperationExists(store, session, member.completionID), "the raced completion admitted a new operation")

	script.releaseGate()
	<-script.arrived // the boundary exits; the successor admission delivers the steering
	// The rerouted completion actually reached the model boundary: whether
	// the steering committed before the winning Operation started or during
	// its first boundary, the request that carries it is sent by its own
	// successor admission.
	deadline := time.After(2 * time.Second)
	for {
		reqs := script.seen()
		if len(reqs) > 0 && strings.Contains(strings.Join(script.lastTexts(), "|"), "raced") {
			break
		}
		select {
		case <-script.arrived:
		case <-deadline:
			t.Fatalf("the rerouted completion never reached a model boundary (requests %d)", len(reqs))
		}
	}
	{
		reqs := script.seen()
		testassert.Require(t, len(reqs) <= 2, "model requests = %d, want the boundary plus at most the drained continuation", len(reqs))
	}
	// The completion executes as its own successor Operation: wait for its
	// terminal commit and Session quiet before any raw graph validation.
	awaitOperationTerminal(t, store, session, member.completionID, OperationSuccess)
	awaitHarnessQuiet(t, h, session)
	{
		got := entryTexts(t, store, session)
		testassert.Require(t, reflect.DeepEqual(got, []string{"hello", "raced"}), "committed inputs = %q, want the rerouted completion as steering", got)
	}
	{
		owner, origin := steeringInputOwner(t, store, session, "raced")
		testassert.Require(t, owner == member.completionID && origin == InputOriginRuntime, "raced input owner/origin = %q/%v, want %q/%v", owner, origin, member.completionID, InputOriginRuntime)
	}
	waitMemberDone(t, member)
}

// TestDeliverBackgroundCompletionReserveFailureTakesClosedPath proves the
// reservation-failure fallback: with the Session's admission reservation held
// and the harness context canceled, the idle-delivery path's reserve call
// fails and the completion takes the durable closed path. The public dispatch
// and the re-evaluation modes stay covered by the delivery-mode tests.
func TestDeliverBackgroundCompletionReserveFailureTakesClosedPath(t *testing.T) {
	store := emptyStore(t)
	h, cancel := newCancelableHarness(t, store, PreparedExecution{}, nil)
	defer cancel()
	session := createSession(t, h)
	c := leaseCoordinator(t, h, session)
	member, err := admitLocked(h, c, memberSession, completionChildID)
	testassert.NoError(t, err)

	release, err := h.reserve(context.Background(), c) // the test holds the admission reservation
	testassert.NoError(t, err)
	defer release()
	cancel() // the idle delivery's reserve call fails against the dead context

	{
		err := h.deliverIdleCompletion(c, completionItem(member, admissionContent("closed on failure")))
		testassert.NoError(t, err)
	}
	signals := storedSignals(t, store, session)
	testassert.Require(t, len(signals) == 1 && signals[0].Content == "closed on failure", "durable signals = %+v, want the closed-path record", signals)
	h.finishBackgroundMember(c, member)
	waitMemberDone(t, member)
}

// TestDeliverBackgroundCompletionAdmissionFailuresFinal proves the idle
// admission attempt is final under the one-attempt rule: the original error
// returns, the member finishes once, no replacement Operation is admitted,
// and no retry or fallback signal write happens. A storage-class failure and
// an otherwise unknown transaction failure are both retained as a failed
// publication with their own cause; a known conflict rejection keeps its class
// and is not latched.
func TestDeliverBackgroundCompletionAdmissionFailuresFinal(t *testing.T) {
	plain := errors.New("injected plain failure")
	cases := []struct {
		name        string
		err         error
		wantLatched bool
		wantErr     func(t *testing.T, err error)
	}{
		{"storage failure", fmt.Errorf("%w: injected storage failure", ErrStorage), true, func(t *testing.T, err error) {
			testassert.Require(t, errors.Is(err, ErrStorage), "delivery error = %v, want the storage class", err)
		}},
		{"plain transaction failure", plain, false, func(t *testing.T, err error) {
			testassert.Equal(t, err, plain) // general admission keeps the transaction error's own identity
		}},
		{"conflict failure", fmt.Errorf("%w: injected conflict", ErrConflict), false, func(t *testing.T, err error) {
			testassert.Require(t, errors.Is(err, ErrInvalid) && strings.Contains(err.Error(), "already exists in another session"), "delivery error = %v, want the cross-session reuse rejection", err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := freshSessionStore(t)
			h := newTestHarness(t, store, func(context.Context, PreparationRequest) (PreparedExecution, error) {
				return validPrepared(), nil
			})
			c := leaseCoordinator(t, h, testSessionID)
			member, err := admitLocked(h, c, memberSession, completionChildID)
			testassert.NoError(t, err)

			store.txHook = func(string) error { return tc.err } // the delivery's admission transaction fails once
			err = h.DeliverBackgroundCompletion(context.Background(), testSessionID, member.completionID, "content")
			store.txHook = nil
			tc.wantErr(t, err)

			waitMemberDone(t, member)
			testassert.Require(t, !storedOperationExists(store, testSessionID, member.completionID), "the failed admission published a replacement Operation")
			{
				n := storedEntryCount(store, testSessionID)
				testassert.Equal(t, n, 0)
			}
			{
				signals := storedSignals(t, store, testSessionID)
				testassert.Require(t, len(signals) == 0, "a fallback signal write committed %+v, want none", signals)
			}
			h.mu.Lock()
			latched := h.storageFailure != nil
			h.mu.Unlock()
			testassert.Equal(t, latched, tc.wantLatched)
			{
				err := h.DeliverBackgroundCompletion(context.Background(), testSessionID, member.completionID, "content")
				testassert.NoError(t, err)
			}
		})
	}
}

// TestClosedDeliveryCommitsBeforeDoneCloses proves the ordering contract: the
// member's done channel closes only after the closed record committed.
func TestClosedDeliveryCommitsBeforeDoneCloses(t *testing.T) {
	store := emptyStore(t)
	script := newModelScript()
	h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
	defer cancel()
	session := createSession(t, h)
	c := leaseCoordinator(t, h, session)
	member, err := admitLocked(h, c, memberJob, jobFixtureID)
	testassert.NoError(t, err)
	closeBackgroundMode(c)

	arrived := make(chan struct{}, 1)
	gate := make(chan struct{})
	store.entryHook = func(draft EntryDraft) error {
		if draft.Kind == EntrySignal {
			select {
			case arrived <- struct{}{}:
			default:
			}
			<-gate
		}
		return nil
	}
	delivered := make(chan error, 1)
	go func() {
		delivered <- h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "durable first")
	}()
	<-arrived // the closed transaction is parked inside storage, the coordinator mutex held
	select {
	case <-member.done:
		t.Fatalf("the member finished before the closed record committed")
	default:
	}
	close(gate)
	{
		err := <-delivered
		testassert.NoError(t, err)
	}
	waitMemberDone(t, member)
	signals := storedSignals(t, store, session)
	testassert.Require(t, len(signals) == 1 && signals[0].RelatedMember != nil && signals[0].RelatedMember.Kind == "job" && signals[0].RelatedMember.ID == jobFixtureID && signals[0].Content == "durable first", "committed signal = %+v, want the job-member completion", signals)
}

// TestClosedDeliveryFailureRetainedBeforeMemberFinish proves the shutdown
// ordering contract of a failed closed delivery: the storage failure is
// retained before the member finish can release a joiner. A controlled
// observer barrier parks the producer after the member's done channel has
// closed, and the public Wait must already return the original storage error
// through that interval. Without retention before the finish the same
// interval reports success.
func TestClosedDeliveryFailureRetainedBeforeMemberFinish(t *testing.T) {
	store := emptyStore(t)
	h, cancel := newCancelableHarness(t, store, PreparedExecution{}, nil)
	defer cancel()
	session := createSession(t, h)
	c := leaseCoordinator(t, h, session)
	member, err := admitLocked(h, c, memberJob, jobFixtureID)
	testassert.NoError(t, err)

	// The closed delivery's signal insert fails before any durable write with
	// a storage-class error.
	store.entryHook = func(draft EntryDraft) error {
		if draft.Kind == EntrySignal {
			return fmt.Errorf("%w: injected storage failure", ErrStorage)
		}
		return nil
	}

	// The barrier parks the producer inside the member finish's Job-scoped
	// invalidation — after done closed — without injecting the failure latch
	// or altering state; only the Job's own invalidation is gated.
	parked := make(chan struct{}, 1)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	finished := 0
	h.deps.Observe = func(fact HarnessFact) {
		if fact.Kind != FactInvalidation || fact.JobID != jobFixtureID {
			return
		}
		finished++
		select {
		case parked <- struct{}{}:
		default:
		}
		<-gate
	}

	cancel() // the not-receivable parent selects the closed delivery path
	produced := make(chan error, 1)
	go func() {
		produced <- h.DeliverBackgroundCompletion(context.Background(), session, member.completionID, "job done")
	}()
	<-parked
	select {
	case <-member.done:
	default:
		t.Fatalf("the member finish invalidation fired before done closed")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	{
		err := h.Wait(waitCtx)
		testassert.Require(t, errors.Is(err, ErrStorage) && strings.Contains(err.Error(), "injected storage failure"), "Wait while the failed delivery is parked after member finish = %v, want the retained storage failure", err)
	}

	release()
	{
		err := <-produced
		testassert.Require(t, errors.Is(err, ErrStorage), "closed delivery = %v, want the storage class", err)
	}
	c.mu.Lock()
	_, present := c.group.members[member.completionID]
	c.mu.Unlock()
	testassert.Require(t, !present, "the member survived its finish")
	testassert.Equal(t, finished, 1)
}

// TestConcurrentClosedDeliveriesSerialize proves two closed deliveries
// preserve cached and durable order: the second cannot enter storage while
// the first holds the coordinator, and both commit with agreeing order, each
// member finishing after its own commit.
func TestConcurrentClosedDeliveriesSerialize(t *testing.T) {
	store := emptyStore(t)
	script := newModelScript()
	h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
	defer cancel()
	session := createSession(t, h)
	c := leaseCoordinator(t, h, session)
	memberA, err := admitLocked(h, c, memberSession, completionChildID)
	testassert.NoError(t, err)
	memberB, err := admitLocked(h, c, memberJob, jobFixtureID)
	testassert.NoError(t, err)
	closeBackgroundMode(c)

	arrived := make(chan struct{}, 2)
	gate := make(chan struct{})
	store.entryHook = func(draft EntryDraft) error {
		if draft.Kind == EntrySignal {
			arrived <- struct{}{}
			<-gate
		}
		return nil
	}
	resA := make(chan error, 1)
	resB := make(chan error, 1)
	go func() {
		resA <- h.DeliverBackgroundCompletion(context.Background(), session, memberA.completionID, "from child")
	}()
	go func() {
		resB <- h.DeliverBackgroundCompletion(context.Background(), session, memberB.completionID, "from job")
	}()

	<-arrived           // the first delivery's transaction parks inside storage
	if c.mu.TryLock() { // the serialization proof: the parked transaction holds the coordinator lock the second delivery must take before it can reach storage
		c.mu.Unlock()
		t.Fatalf("the closed delivery released the coordinator before its transaction committed")
	}
	close(gate)
	<-arrived // the second delivery's transaction enters storage only after the release

	{
		err := <-resA
		testassert.NoError(t, err)
	}
	{
		err := <-resB
		testassert.NoError(t, err)
	}
	waitMemberDone(t, memberA)
	waitMemberDone(t, memberB)

	signals := storedSignals(t, store, session)
	testassert.Equal(t, len(signals), 2)
	c.mu.Lock()
	var cached []string
	for _, entry := range c.graph.Entries {
		if entry.Signal != nil && entry.Signal.Signal == SignalBackgroundCompletion {
			cached = append(cached, entry.Signal.RelatedMember.Kind)
		}
	}
	c.mu.Unlock()
	durable := []string{signals[0].RelatedMember.Kind, signals[1].RelatedMember.Kind}
	testassert.Require(t, reflect.DeepEqual(cached, durable), "cached signal order %v disagrees with durable order %v", cached, durable)
}

// TestClosedDeliveryRevisionDrift proves the closed path never retries: a
// revision drift returns the conflict after one rematerialization with no
// committed entry, a failing rematerialization returns its own error, and a
// storage-class failure is latched.
func TestClosedDeliveryRevisionDrift(t *testing.T) {
	advanceRevision := func(t *testing.T, store *graphStorage) {
		t.Helper()
		err := store.Transact(context.Background(), func(tx Transaction) error {
			key := RegisterKey{SessionID: testSessionID, Kind: RegisterSession}
			reg, err := tx.ReadRegister(key)
			if err != nil {
				return err
			}
			_, err = tx.ReplaceRegister(key, reg.Revision, reg.Payload)
			return err
		})
		testassert.NoError(t, err)
	}

	t.Run("drift returns the conflict, rematerializes once, finishes the member", func(t *testing.T) {
		store := freshSessionStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		c := leaseCoordinator(t, h, testSessionID)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)
		closeBackgroundMode(c)
		advanceRevision(t, store)

		err = h.DeliverBackgroundCompletion(context.Background(), testSessionID, member.completionID, "x")
		testassert.Require(t, errors.Is(err, ErrConflict), "drifted delivery = %v, want the conflict class", err)
		waitMemberDone(t, member)
		{
			n := storedEntryCount(store, testSessionID)
			testassert.Equal(t, n, 0)
		}
		c.mu.Lock()
		revision := c.graph.Session.Revision
		c.mu.Unlock()
		testassert.Equal(t, revision, 2)
	})

	t.Run("failing rematerialization returns corruption", func(t *testing.T) {
		store := freshSessionStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		c := leaseCoordinator(t, h, testSessionID)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)
		closeBackgroundMode(c)
		advanceRevision(t, store)
		{
			err := store.Transact(context.Background(), func(tx Transaction) error {
				key := RegisterKey{SessionID: testSessionID, Kind: RegisterSession}
				reg, err := tx.ReadRegister(key)
				if err != nil {
					return err
				}
				_, err = tx.ReplaceRegister(key, reg.Revision, json.RawMessage(`{"bogus":true}`))
				return err
			})
			testassert.NoError(t, err)
		}

		err = h.DeliverBackgroundCompletion(context.Background(), testSessionID, member.completionID, "x")
		var corrupt *CorruptionError
		testassert.Require(t, errors.As(err, &corrupt), "delivery = %v, want the rematerialization corruption", err)
		waitMemberDone(t, member)
		{
			n := storedEntryCount(store, testSessionID)
			testassert.Equal(t, n, 0)
		}
	})

	t.Run("storage-class rematerialization failure is latched", func(t *testing.T) {
		store := freshSessionStore(t)
		script := newModelScript()
		h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
		defer cancel()
		c := leaseCoordinator(t, h, testSessionID)
		member, err := admitLocked(h, c, memberSession, completionChildID)
		testassert.NoError(t, err)
		closeBackgroundMode(c)
		advanceRevision(t, store)
		store.registersErr = fmt.Errorf("%w: storage down", ErrStorage)

		err = h.DeliverBackgroundCompletion(context.Background(), testSessionID, member.completionID, "x")
		testassert.Require(t, errors.Is(err, ErrStorage), "delivery = %v, want the rematerialization storage failure", err)
		h.mu.Lock()
		latched := h.storageFailure != nil
		h.mu.Unlock()
		testassert.Require(t, latched, "the storage-class rematerialization failure was not latched")
		waitMemberDone(t, member)
	})
}

// TestJobCompletionWaitsForChildSuccessorRetirement proves the recursive
// completion flow: a job completion arriving while the child Operation runs is
// queued, the member-finish tail defers, the post-terminal drain admits the
// completion as the successor Operation, the settlement check defers on the
// installed run until the successor retires, and the parent-facing result
// reaches the parent only afterwards — carrying the successor's own text or
// exactly the header alone for a refusal-only successor, never the
// predecessor's earlier text.
func TestJobCompletionWaitsForChildSuccessorRetirement(t *testing.T) {
	refusal := turnDelta("stop", "")
	refusal.RefusalFragment = "no"
	header := "Session " + completionChildID + ": success"
	for _, tc := range []struct {
		name      string
		successor modelAttempt
		want      string
	}{
		{"text-bearing successor", modelAttempt{stream: streamOf(turnDelta("stop", "final"))}, header + "\nfinal"},
		{"refusal-only successor", modelAttempt{stream: streamOf(refusal, usageDelta(model.Usage{InputTokens: 2, OutputTokens: 1}))}, header},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := completionChildGraph(completionParentID, false, OperationSuccess, "", "").storage(t)
			addRootSession(t, store, completionParentID)
			script := newModelScript(modelAttempt{stream: streamOf(turnDelta("stop", "earlier", testToolCall("call-1")))}, tc.successor)
			script.gate = make(chan struct{})
			stub := newPrepareStub(modelPrepared(script.model))
			h, cancel := newCancelableHarness(t, store, PreparedExecution{}, stub.prepare)
			defer cancel()
			watch := watchSettlements(store)
			cP := leaseCoordinator(t, h, completionParentID)
			cC := leaseCoordinator(t, h, completionChildID)
			member, err := admitLocked(h, cP, memberSession, completionChildID)
			testassert.NoError(t, err)
			cC.mu.Lock()
			cC.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: 1024, report: true}
			cC.mu.Unlock()
			jobMember, err := admitLocked(h, cC, memberJob, jobFixtureID)
			testassert.NoError(t, err)

			{
				_, err := h.Submit(context.Background(), SubmitRequest{
					SessionID:   completionChildID,
					OperationID: "op-1",
					Origin:      InputOriginUser,
					Content:     admissionContent("hello"),
					Mode:        MessageModeRegular,
				})
				testassert.NoError(t, err)
			}
			<-script.arrived // the child run is parked at its model boundary

			{
				err := h.DeliverBackgroundCompletion(context.Background(), completionChildID, jobMember.completionID, "job done")
				testassert.NoError(t, err)
			}
			waitMemberDone(t, jobMember)
			cC.mu.Lock()
			pending, steering := cC.pendingCompletion != nil, len(cC.steering)
			cC.mu.Unlock()
			testassert.Require(t, pending && steering == 1, "after the job delivery: pending %v steering %d, want the obligation deferred and the completion queued", pending, steering)
			testassert.Require(t, !storedOperationExists(store, completionChildID, jobMember.completionID), "the job completion admitted an operation while the run was active")

			stub.gate = make(chan struct{}) // the drain's admission of the completion will park here
			script.releaseGate()            // op-1 settles quiet success with the completion still queued
			<-stub.arrived                  // the drain is parked in preparation, the retired run still installed
			watch.next()                    // op-1's settlement committed

			// the retired-but-not-drained state: the settlement check defers on the run
			h.childCompletionSettled(cC, "")
			cC.mu.Lock()
			pendingDeferred, runInstalled := cC.pendingCompletion != nil, cC.run != nil
			cC.mu.Unlock()
			testassert.Require(t, pendingDeferred && runInstalled, "deferred check: pending %v run %v, want the obligation held on the installed run", pendingDeferred, runInstalled)
			testassert.Require(t, !storedOperationExists(store, completionParentID, member.completionID), "the parent received the completion before the successor retired")

			close(stub.gate) // the drain admits the completion as the successor operation
			watch.next()     // the successor's settlement committed
			watch.next()     // the parent completion's settlement committed

			{
				rec := settledOperation(t, store, completionParentID, member.completionID)
				testassert.Equal(t, rec.State.Status, OperationSuccess)
			}
			{
				got := runtimeInputText(t, store, completionParentID)
				testassert.Require(t, got == tc.want && !strings.Contains(got, "earlier"), "delivered completion = %q, want %q and never the predecessor's earlier text", got, tc.want)
			}
			cC.mu.Lock()
			pendingAfter := cC.pendingCompletion
			cC.mu.Unlock()
			testassert.Require(t, pendingAfter == nil, "the pending obligation survived the completed delivery")
			waitMemberDone(t, member)
		})
	}
}

// TestChildCompletionSettledAbandonment proves the settlement check abandons
// only on a non-empty settled operation identity still current — the failed
// terminal commit — and defers on an empty one while the child runs.
func TestChildCompletionSettledAbandonment(t *testing.T) {
	newRunningChild := func(t *testing.T) (*Harness, *graphStorage, *coordinator, *backgroundMember) {
		t.Helper()
		store := completionChildGraph(completionParentID, true, OperationRunning, "", "hi").storage(t)
		addRootSession(t, store, completionParentID)
		h := newTestHarness(t, store, nil)
		cC := leaseCoordinator(t, h, completionChildID)
		cP := leaseCoordinator(t, h, completionParentID)
		member, err := admitLocked(h, cP, memberSession, completionChildID)
		testassert.NoError(t, err)
		cC.mu.Lock()
		cC.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: 1024, report: true}
		cC.mu.Unlock()
		return h, store, cC, member
	}

	t.Run("abandons on the failed settlement identity", func(t *testing.T) {
		h, store, cC, member := newRunningChild(t)
		h.childCompletionSettled(cC, childFixtureOp)
		waitMemberDone(t, member)
		cC.mu.Lock()
		pending := cC.pendingCompletion
		cC.mu.Unlock()
		testassert.Require(t, pending == nil, "the abandoned obligation survived")
		{
			n := storedEntryCount(store, completionParentID)
			testassert.Equal(t, n, 0)
		}
	})

	t.Run("defers on an empty settled identity", func(t *testing.T) {
		h, _, cC, member := newRunningChild(t)
		h.childCompletionSettled(cC, "")
		select {
		case <-member.done:
			t.Fatalf("the deferred obligation finished the member")
		default:
		}
		cC.mu.Lock()
		pending := cC.pendingCompletion != nil
		cC.mu.Unlock()
		testassert.Require(t, pending, "the deferred obligation was taken")
	})
}

// TestChildCompletionSettledWaitsForBuffers proves the quiet predicate's FIFO
// half: a terminal child that still holds a steering or queued item is not
// settled, and its obligation delivers only after both buffers are empty.
func TestChildCompletionSettledWaitsForBuffers(t *testing.T) {
	for _, name := range []string{"steering", "queued"} {
		t.Run(name, func(t *testing.T) {
			store := completionChildGraph(completionParentID, false, OperationSuccess, "", "child text").storage(t)
			addRootSession(t, store, completionParentID)
			h := newTestHarness(t, store, nil)
			cP := leaseCoordinator(t, h, completionParentID)
			cC := leaseCoordinator(t, h, completionChildID)
			member, err := admitLocked(h, cP, memberSession, completionChildID)
			testassert.NoError(t, err)
			closeBackgroundMode(cP) // the closed path records the report without a model
			cC.mu.Lock()
			cC.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: 1024, report: true}
			item := &pendingMessage{operationID: "op-2", origin: InputOriginUser, content: admissionContent("later")}
			if name == "steering" {
				cC.steering = append(cC.steering, item)
			} else {
				cC.queued = append(cC.queued, item)
			}
			cC.mu.Unlock()

			h.childCompletionSettled(cC, "")
			select {
			case <-member.done:
				t.Fatalf("the buffered child settled its member")
			default:
			}
			cC.mu.Lock()
			pending := cC.pendingCompletion != nil
			cC.mu.Unlock()
			testassert.Require(t, pending, "the buffered child's obligation was taken")

			cC.mu.Lock()
			cC.steering, cC.queued = nil, nil // the drain consumed the item
			cC.mu.Unlock()
			h.childCompletionSettled(cC, "")
			waitMemberDone(t, member)
			signals := storedSignals(t, store, completionParentID)
			testassert.Require(t, len(signals) == 1 && signals[0].Content == "Session "+completionChildID+": success\nchild text", "committed signal = %+v, want the quiet child's report", signals)
		})
	}
}

// TestChildCompletionRendersParentFacingText proves the rendered report per
// settlement status: the stable Session-and-status header, followed by the
// last assistant text of the settled Operation on success and interruption,
// the failure detail on failure, and the header alone when the body is empty —
// each delivered to the idle parent under the completion identity.
func TestChildCompletionRendersParentFacingText(t *testing.T) {
	header := "Session " + completionChildID + ": "
	cases := []struct {
		name          string
		status        OperationState
		detail        string
		assistantText string
		want          string
	}{
		{"success assistant text", OperationSuccess, "", "done", header + "success\ndone"},
		{"empty success header", OperationSuccess, "", "", header + "success"},
		{"interruption header", OperationInterruption, "stopped", "", header + "interruption"},
		{"interruption assistant text", OperationInterruption, "stopped", "halted", header + "interruption\nhalted"},
		{"failure detail", OperationFailure, "boom", "ignored", header + "failure\nboom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := completionChildGraph(completionParentID, false, tc.status, tc.detail, tc.assistantText).storage(t)
			addRootSession(t, store, completionParentID)
			script := newModelScript()
			h, cancel := newCancelableHarness(t, store, modelPrepared(script.model), nil)
			defer cancel()
			watch := watchSettlements(store)
			cC := leaseCoordinator(t, h, completionChildID)
			cP := leaseCoordinator(t, h, completionParentID)
			member, err := admitLocked(h, cP, memberSession, completionChildID)
			testassert.NoError(t, err)
			cC.mu.Lock()
			cC.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: 1024, report: true}
			cC.mu.Unlock()

			h.childCompletionSettled(cC, "")
			waitMemberDone(t, member)
			watch.next() // the parent completion's settlement committed
			{
				rec := settledOperation(t, store, completionParentID, member.completionID)
				testassert.Equal(t, rec.State.Status, OperationSuccess)
			}
			{
				got := runtimeInputText(t, store, completionParentID)
				testassert.Equal(t, got, tc.want)
			}
			cC.mu.Lock()
			pending := cC.pendingCompletion
			cC.mu.Unlock()
			testassert.Require(t, pending == nil, "the delivered obligation survived")
		})
	}
}

// TestChildCompletionTruncatesAtOutputLimit proves the rendered report body is
// capped at the requested limit in UTF-8 bytes, retaining the longest
// complete-character prefix that fits the trailing marker, while the header
// stays whole.
func TestChildCompletionTruncatesAtOutputLimit(t *testing.T) {
	text := strings.Repeat("é", 700) // 1400 UTF-8 bytes
	header := "Session " + completionChildID + ": success"
	cases := []struct {
		name  string
		limit int
		body  string
	}{
		{"even byte boundary", 1000, strings.Repeat("é", 494) + "\n[truncated]"},
		{"backed off to a complete character", 999, strings.Repeat("é", 493) + "\n[truncated]"},
		{"under the bound", 10000, text},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := completionChildGraph(completionParentID, false, OperationSuccess, "", text).storage(t)
			addRootSession(t, store, completionParentID)
			h := newTestHarness(t, store, nil)
			cC := leaseCoordinator(t, h, completionChildID)
			cP := leaseCoordinator(t, h, completionParentID)
			member, err := admitLocked(h, cP, memberSession, completionChildID)
			testassert.NoError(t, err)
			closeBackgroundMode(cP) // the closed path records the rendered content verbatim
			cC.mu.Lock()
			cC.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: tc.limit, report: true}
			cC.mu.Unlock()

			h.childCompletionSettled(cC, "")
			waitMemberDone(t, member)
			signals := storedSignals(t, store, completionParentID)
			testassert.Equal(t, len(signals), 1)
			{
				got := signals[0].Content
				testassert.Equal(t, got, header+"\n"+tc.body)
			}
			testassert.Require(t, len(signals[0].Content) <= len(header)+1+tc.limit, "truncated report exceeds the body limit: %d > %d", len(signals[0].Content), len(header)+1+tc.limit)
		})
	}
}

// TestChildCompletionWholeReportBound proves the whole-report bound applies in
// addition to the requested body limit: the body loses exactly the room the
// header and separator take, the marker counts against the bound, and the
// identity-and-status header survives verbatim.
func TestChildCompletionWholeReportBound(t *testing.T) {
	store := completionChildGraph(completionParentID, false, OperationSuccess, "", strings.Repeat("x", maxCompletionReportBytes)).storage(t)
	graph, err := validateFixture(t, store, completionChildID)
	testassert.NoError(t, err)

	header := "Session " + completionChildID + ": success"
	got := childCompletionContent(graph, 2*maxCompletionReportBytes)
	testassert.Require(t, len(got) <= maxCompletionReportBytes, "whole report = %d bytes, want at most %d", len(got), maxCompletionReportBytes)
	testassert.Require(t, strings.HasPrefix(got, header+"\n"), "whole report does not carry the unchanged header")
	testassert.Require(t, strings.HasSuffix(got, "\n[truncated]"), "whole report does not carry the truncation marker")
	body := strings.TrimPrefix(got, header+"\n")
	testassert.Equal(t, len(body), maxCompletionReportBytes-len(header)-1)

	zero := childCompletionContent(graph, 0)
	testassert.Require(t, len(zero) <= maxCompletionReportBytes, "zero-limit report = %d bytes, want at most %d", len(zero), maxCompletionReportBytes)
	testassert.Require(t, strings.HasSuffix(zero, "\n[truncated]"), "zero-limit report does not carry the truncation marker")
}

// TestShutdownContendedRetirementDiscardsBuffers proves the shutdown case of the
// completion owner: when shutdown cancels the drain reservation while another
// holder contends it, the retiring run discards both FIFOs itself, so the
// quiet settlement notification still delivers the Session's own report while
// the discarded buffered report never becomes a signal and Wait joins only
// after the contended holder releases.
func TestShutdownContendedRetirementDiscardsBuffers(t *testing.T) {
	store := emptyStore(t)
	script := newModelScript()
	script.gate = make(chan struct{})
	h, cancel := newCancelableHarness(t, store, PreparedExecution{}, func(context.Context, PreparationRequest) (PreparedExecution, error) {
		return modelPrepared(script.model), nil
	})
	defer cancel()
	root := createSession(t, h)
	res, err := h.LaunchChildSession(context.Background(), launchRequestFor(root))
	testassert.NoError(t, err)
	child := res.ChildSessionID
	childC := leaseCoordinator(t, h, child)

	jobMember, err := admitLocked(h, childC, memberJob, jobFixtureID)
	testassert.NoError(t, err)
	{
		err := h.DeliverBackgroundCompletion(context.Background(), child, jobMember.completionID, "discarded report")
		testassert.NoError(t, err)
	}
	waitMemberDone(t, jobMember) // the handoff buffers the report under the running child
	childC.mu.Lock()
	buffered := len(childC.steering)
	childC.mu.Unlock()
	testassert.Equal(t, buffered, 1)

	held, err := h.reserve(context.Background(), childC) // the contended reservation the drain must not take
	testassert.NoError(t, err)
	defer held()
	cancel()             // shutdown: the draining acquisition now sees contention and the dead context
	script.releaseGate() // the child's parked execution finishes and retires
	childC.mu.Lock()
	run := childC.run
	childC.mu.Unlock()
	if run != nil {
		select {
		case <-run.done:
		case <-time.After(5 * time.Second):
			t.Fatal("the retiring run did not converge")
		}
	}

	waited := make(chan error, 1)
	go func() { waited <- h.Wait(context.Background()) }()
	select {
	case err := <-waited:
		t.Fatalf("Wait returned before the contended reservation released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	held()
	select {
	case err := <-waited:
		testassert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not join after the contended reservation released")
	}

	// The retired run discarded both FIFOs, so its quiet notification settled
	// the child's own report as the parent's closed-path signal; the discarded
	// buffered report never reappeared.
	childC.mu.Lock()
	discarded := len(childC.steering) + len(childC.queued)
	childC.mu.Unlock()
	testassert.Equal(t, discarded, 0)
	testassert.Require(t, len(storedSignals(t, store, child)) == 0, "discarded report signals = %+v, want none", storedSignals(t, store, child))
	rootSignals := storedSignals(t, store, root)
	testassert.Require(t, len(rootSignals) == 1 && rootSignals[0].RelatedMember != nil && rootSignals[0].RelatedMember.Kind == "session" && rootSignals[0].RelatedMember.ID == child && strings.HasPrefix(rootSignals[0].Content, "Session "+child+": ") && !strings.Contains(rootSignals[0].Content, "discarded report"), "root signals = %+v, want the child's own settled report and not the discarded item", rootSignals)
}

// TestBufferedCompletionSignalRemovesItsHead proves the coherent fallback
// adoption: the signal producer removes the selected buffered head under the
// same lock that commits the signal, so no reader can observe the committed
// signal and the pending item together.
func TestBufferedCompletionSignalRemovesItsHead(t *testing.T) {
	store := freshSessionStore(t)
	h := newTestHarness(t, store, nil)
	c := leaseCoordinator(t, h, testSessionID)
	member, err := admitLocked(h, c, memberSession, completionChildID)
	testassert.NoError(t, err)
	item := completionItem(member, admissionContent("buffered report"))
	c.mu.Lock()
	c.steering = append(c.steering, item)
	c.bumpLocalRevision()
	c.mu.Unlock()

	{
		err := h.deliverClosedCompletion(c, completionMember(member), "buffered report", item)
		testassert.NoError(t, err)
	}
	signals := storedSignals(t, store, testSessionID)
	testassert.Require(t, len(signals) == 1 && signals[0].Content == "buffered report", "committed signals = %+v, want the fallback record", signals)
	view, err := h.ReadSessionView(context.Background(), testSessionID)
	testassert.NoError(t, err)
	testassert.Require(t, len(view.Steering)+len(view.Queued) == 0, "the view shows a buffered item beside the committed signal: steering %+v queued %+v", view.Steering, view.Queued)
	testassert.Require(t, storedEntryCount(store, testSessionID) == 1, "the session carries %d entries, want the signal alone", storedEntryCount(store, testSessionID))
	{
		got := entryTexts(t, store, testSessionID)
		testassert.Require(t, len(got) == 0, "the buffered report also committed an input entry: %q", got)
	}
}
