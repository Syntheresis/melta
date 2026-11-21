package harness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Syntheresis/melta/model"
)

// memberKind classifies one background ownership-group member.
type memberKind string // "session", "job"

const (
	memberSession memberKind = "session"
	memberJob     memberKind = "job"
)

// backgroundMember is one registered member of a Session's background
// ownership group.
type backgroundMember struct {
	kind         memberKind
	id           string        // child session ID or job ID
	starter      string        // extension that started a Session member
	completionID string        // reserved, session-unique, first-writer-wins
	claimed      bool          // set by the first-winner claim; a second claim is a no-op
	done         chan struct{} // closed when the member finishes
}

// backgroundGroup is one Session's set of live background members.
type backgroundGroup struct {
	members map[string]*backgroundMember // keyed by completionID; claimed members remain until finished
}

// launchInfo is one child Session's reserved completion identity and the
// start's report choice.
type launchInfo struct {
	completionID string // the child member's reserved completion identity
	outputLimit  int    // the requested report body limit in UTF-8 bytes
	report       bool   // the start requested a completion report
}

// SessionMember is a live member of a Session's ownership group.
type SessionMember struct {
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Starter string `json:"starter"`
}

// SessionMembers returns the currently live ownership group.
func (h *Harness) SessionMembers(ctx context.Context, sessionID string) ([]SessionMember, error) {
	members := []SessionMember{}
	err := h.withSessionRead(ctx, sessionID, func(c *coordinator) error {
		if c.group != nil {
			for _, member := range c.group.members {
				members = append(members, SessionMember{Kind: string(member.kind), ID: member.id, Starter: member.starter})
			}
		}
		return nil
	})
	return members, err
}

// stopInterval is one stop's join handle: err is written before done closes
// (the workspaceAttempt pattern, runtime/composition.go:673-677).
type stopInterval struct {
	done chan struct{}
	err  error
}

// memberStopping reports whether this Session itself is being stopped.
// The caller holds c.mu.
func memberStopping(c *coordinator) bool {
	return c.memberStop != nil
}

// admitBackgroundMember registers one background member on the Session's
// group. It runs under the caller-held c.mu and rejects a non-open Session
// lifecycle, harness cancellation (the raw error, Submit's entry-gate shape),
// and a stopping group or Session member. The returned member is the caller's
// handle — member.completionID is the completion identity every later step
// consumes — and it owns no graph residency: a member keeps only its group
// membership, so its Session's graph is bounded like any other inactive graph
// while the member is live.
func (h *Harness) admitBackgroundMember(c *coordinator, kind memberKind, id string) (*backgroundMember, error) {
	if c.graph.Session.State.Lifecycle != LifecycleOpen {
		return nil, invalidInput("session %q is archived; admission requires an open Session", c.graph.Session.Identity.SessionID)
	}
	if err := h.ctx.Err(); err != nil {
		return nil, err
	}
	if c.stop != nil || memberStopping(c) {
		return nil, invalidInput("background group is closed or stopping")
	}
	if c.group == nil {
		c.group = &backgroundGroup{members: map[string]*backgroundMember{}}
	}
	completionID, err := newHexID()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStorage, err)
	}
	m := &backgroundMember{kind: kind, id: id, completionID: completionID, done: make(chan struct{})}
	c.group.members[completionID] = m
	c.bumpLocalRevision() // the member admission is a coordinator-local publication
	return m, nil
}

// claimBackgroundMember marks one member claimed and returns it; the member
// stays in the map — visible to Stop's snapshot and the archive gate while
// its settlement is in flight. A missing or already-claimed ID returns
// (nil, false). It runs under the caller-held c.mu.
func (h *Harness) claimBackgroundMember(c *coordinator, completionID string) (*backgroundMember, bool) {
	if c.group == nil {
		return nil, false
	}
	m := c.group.members[completionID]
	if m == nil || m.claimed {
		return nil, false
	}
	m.claimed = true
	return m, true
}

// finishBackgroundMember performs the terminal transition: it removes the
// member if still present and closes done. An already-removed member is a
// no-op. Every member-ending path runs claim-then-finish. It takes c.mu
// itself, then — after removal and outside c.mu — emits the member-ending
// facts and runs the recursive completion check: a finishing descendant may
// unblock this session's own pending completion. The check runs in the same
// tail position as the execution retirement's own settlement, so it settles
// while any residency the caller holds is still held and a member never
// releases residency before its Session's completion has had its chance.
func (h *Harness) finishBackgroundMember(c *coordinator, m *backgroundMember) {
	c.mu.Lock()
	removed := false
	if c.group != nil {
		if _, ok := c.group.members[m.completionID]; ok {
			delete(c.group.members, m.completionID)
			close(m.done)
			c.bumpLocalRevision() // the member finish is a coordinator-local publication
			removed = true
		}
	}
	c.mu.Unlock()
	if removed {
		if m.kind == memberJob { // only a job member's transition names its Job
			h.observeMemberInvalidation(c, m.id)
		} else {
			h.observeInvalidation(c)
		}
	}
	_ = h.childCompletionSettled(c, "") // asynchronous notification; settlement latches storage failures before releasing the member
}

// bgDelivery is the resolved delivery mode of one background completion.
type bgDelivery int

const (
	bgDeliverClosed   bgDelivery = iota // the durable signal entry
	bgDeliverSteering                   // the successor admission delivers it after the boundary exit
	bgDeliverIdle                       // normal admission under the completion identity
)

// backgroundDeliveryPriority resolves one completion's delivery mode under
// the caller-held coordinator mutex: a not-receivable parent (harness loss, a
// non-open lifecycle, or a member stop) closes regardless of activity; an
// active Operation steers — the waiting steering item is enqueued here, under
// the same hold — an idle Session admits. Receivability outranks activity.
func backgroundDeliveryPriority(h *Harness, c *coordinator, item *pendingMessage) bgDelivery {
	if h.ctx.Err() != nil || c.graph.Session.State.Lifecycle != LifecycleOpen || memberStopping(c) {
		return bgDeliverClosed
	}
	if c.graph.Session.State.CurrentOperationID != "" || c.run != nil {
		c.steering = append(c.steering, item)
		c.bumpLocalRevision() // the completion enqueue is a coordinator-local publication
		return bgDeliverSteering
	}
	return bgDeliverIdle
}

// DeliverBackgroundCompletion delivers one background member's completion to
// its owning Session. It resolves the process-local completion obligation
// first — an unknown completion is not new work and never materializes a
// Session — then claims the member first-writer and delivers by the resolved
// mode: a not-receivable parent commits the durable background_completion
// signal entry; a receivable active parent steers the completion into the
// running Operation; a receivable idle parent admits it as an Operation under
// the completion identity, and a known semantic refusal of that admission —
// with the Session record readable and writable — becomes the same durable
// signal entry instead. An already admitted completion is success. Only a
// storage-class delivery failure (ErrStorage) is retained through the Harness
// storage latch, and the first retained one is never replaced; the retention
// happens before the claimed member finishes, so a joiner released by the
// member finish observes the retained error; the claimed member still finishes
// only after the delivery attempt, so a successful closed record commits
// before the member is marked terminal, and a second delivery is a no-op.
// Delivery is one attempt with no retry and no fallback write, and an exited
// Job is removed after its callback: a process read cannot recover a failed
// post-exit delivery's completion itself; the child's history is separate
// evidence.
func (h *Harness) DeliverBackgroundCompletion(ctx context.Context, sessionID, completionID, content string) error {
	if content == "" {
		return invalidInput("background completion content must be non-empty")
	}
	h.mu.Lock()
	owner := h.sessions[sessionID]
	h.mu.Unlock()
	if owner == nil { // registration necessarily installed the member's owning coordinator
		return nil
	}
	owner.mu.Lock()
	member, ok := h.claimBackgroundMember(owner, completionID)
	owner.mu.Unlock()
	if !ok {
		return nil
	}
	err := h.deliverBackgroundCompletion(ctx, owner, member, content)
	if err != nil {
		h.recordStorageFailure(err)
	}
	h.finishBackgroundMember(owner, member)
	return err
}

// deliverBackgroundCompletion runs one claimed completion's validated
// delivery attempt. Normal coordinator validation precedes any graph
// interpretation: an unavailable parent returns its ordinary error with no
// durable work, and the claimed member still finishes. The claimed member
// remains in the group through the whole attempt, so recursive readiness and
// Stop cannot treat it as finished before delivery has resolved.
func (h *Harness) deliverBackgroundCompletion(ctx context.Context, c *coordinator, member *backgroundMember, content string) error {
	validated, release, err := h.acquireCoordinator(ctx, c.sessionID)
	if err != nil {
		return err
	}
	defer release()
	part, err := model.NewContentPart(model.ContentPart{Kind: model.PartText, Text: content})
	if err != nil {
		return invalidInput("background completion content: %v", err)
	}
	parts := []model.ContentPart{part}
	item := completionItem(member, parts)

	validated.mu.Lock()
	mode := backgroundDeliveryPriority(h, validated, item)
	validated.mu.Unlock()
	if mode == bgDeliverSteering { // the completion enqueue is a coordinator-local publication
		h.observeInvalidation(validated)
	}

	switch mode {
	case bgDeliverSteering:
		return nil // enqueued at evaluation; the boundary exit and the successor admission deliver it
	case bgDeliverIdle:
		return h.deliverIdleCompletion(validated, item)
	default:
		return h.deliverClosedCompletion(validated, item.completion, content, nil)
	}
}

// completionItem builds one completion's buffered input item: the completion
// identity, the runtime origin, the report parts, and a copy of only the
// related member kind and ID — never the live member the completion was
// claimed from — so a deferred admission can still record the signal the idle
// path would.
func completionItem(member *backgroundMember, parts []model.ContentPart) *pendingMessage {
	return &pendingMessage{
		operationID: member.completionID,
		origin:      InputOriginRuntime,
		content:     parts,
		completion:  completionMember(member),
	}
}

// completionMember copies the related member kind and ID of one claimed
// completion.
func completionMember(member *backgroundMember) *relatedMember {
	return &relatedMember{Kind: string(member.kind), ID: member.id}
}

// deliverIdleCompletion admits one completion through the idle path: it takes
// the Session's admission reservation, re-evaluates the same priority under
// it — a run installed while the reservation waited re-routes to steering,
// lost receivability to the closed path — and admits through the held
// reservation, installing the execution before releasing it. The reserved
// body runs directly because the reservation is already held: a wrapper that
// takes its own reservation would deadlock against itself. A reserve failure resolves to
// the closed path; a semantic refusal of the admission decision becomes the
// operationless signal under the same reservation; every other admission
// failure is final: no retry and no fallback signal write.
func (h *Harness) deliverIdleCompletion(c *coordinator, item *pendingMessage) error {
	text := item.content[0].Text
	release, err := h.reserve(h.ctx, c)
	if err != nil { // the reservation failed under harness loss: re-evaluated receivability is the closed path
		return h.deliverClosedCompletion(c, item.completion, text, nil)
	}
	defer release()
	c.mu.Lock()
	sessionID := c.sessionID
	mode := backgroundDeliveryPriority(h, c, item)
	c.mu.Unlock()
	if mode == bgDeliverSteering { // the completion enqueue is a coordinator-local publication
		h.observeInvalidation(c)
	}
	switch mode {
	case bgDeliverClosed:
		return h.deliverClosedCompletion(c, item.completion, text, nil)
	case bgDeliverSteering:
		return nil // enqueued at the re-evaluation
	}
	rec, prepared, _, err := h.admitReserved(h.ctx, c, admissionRequest{
		SessionID:   sessionID,
		OperationID: item.operationID,
		Kind:        RequestKindMessage,
		Origin:      InputOriginRuntime,
		Content:     item.content,
	})
	if err != nil {
		return h.resolveCompletionFailure(c, item.completion, text, nil, err)
	}
	if prepared != nil { // install the execution before the deferred release runs
		h.startExecution(c, rec.Admission.OperationID, *prepared)
	}
	return nil
}

// resolveCompletionFailure applies the one completion error policy shared by
// the idle and buffered delivery owners: a refusal becomes the operationless
// signal under the caller's reservation, and every remaining failure returns
// with its cause for the owner's existing first-failure retention. Existing
// same-kind admission is success and never reaches this policy.
func (h *Harness) resolveCompletionFailure(c *coordinator, related *relatedMember, content string, buffered *pendingMessage, err error) error {
	if isAdmissionRefusal(err) {
		return h.deliverClosedCompletion(c, related, content, buffered)
	}
	return err
}

// deliverClosedCompletion commits one operationless background_completion
// signal entry through the compare/commit/adopt/rematerialize pattern, with
// the coordinator mutex held across the whole transaction: it re-reads and
// decodes the parent register, compares its revision with the resident view —
// decode failure is corruption and drift the revision-race conflict — inserts
// the entry with the completion's related member kind/ID and the
// producer-bounded content, and replaces the Session register with its
// unchanged payload and advanced revision. A buffered completion verifies
// under the same held mutex that the exact selected head still belongs to its
// buffer: a stop or shutdown that discarded it during preparation leaves no
// record. On transaction error the filtered corruption marker runs; on
// revision drift the view rematerializes once on the settlement context,
// whose failure replaces the original error. Delivery is never retried.
func (h *Harness) deliverClosedCompletion(c *coordinator, related *relatedMember, content string, buffered *pendingMessage) error {
	c.mu.Lock()
	if buffered != nil && !c.holdsBufferedLocked(buffered) {
		c.mu.Unlock()
		return nil // the stop or shutdown discarded the selected item: it never reappears as a signal
	}
	sessionID := c.graph.Session.Identity.SessionID
	view := c.graph.Session
	entryID, err := newHexID()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	entry := signalEntry{
		SessionID:     sessionID,
		EntryID:       entryID,
		Signal:        SignalBackgroundCompletion,
		RelatedMember: related,
		Content:       content,
	}
	payload, err := encodeSignalEntry(entry)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	settleCtx := context.WithoutCancel(h.ctx)
	var (
		committedSession SessionRecord
		adopted          graphEntry
	)
	err = h.deps.Storage.Transact(settleCtx, func(tx Transaction) error {
		key := RegisterKey{SessionID: sessionID, Kind: RegisterSession}
		reg, err := tx.ReadRegister(key)
		if err != nil {
			return err
		}
		current, err := decodeSessionRegister(reg)
		if err != nil {
			return corruptSession(sessionID, "session register: %v", err)
		}
		if reg.Revision != view.Revision {
			return fmt.Errorf("%w: session %q revision %d changed concurrently to %d", errRevisionRace, sessionID, view.Revision, reg.Revision)
		}
		adopted, err = insertAndAdopt(tx, sessionID, EntryDraft{
			SessionID: sessionID,
			ID:        entryID,
			Kind:      EntrySignal,
			Payload:   payload,
		})
		if err != nil {
			return err
		}
		committedSession = SessionRecord{Identity: current.Identity, State: current.State}
		sessionPayload, err := encodeSessionRegister(committedSession)
		if err != nil {
			return err
		}
		replaced, err := tx.ReplaceRegister(key, reg.Revision, sessionPayload)
		if err != nil {
			return err
		}
		committedSession.Revision = replaced.Revision
		return nil
	})
	if err != nil {
		h.markCorrupt(sessionID, err)
		c.mu.Unlock()
		if errors.Is(err, errRevisionRace) { // a foreign writer changed the durable state under the resident view
			if rerr := h.rematerialize(settleCtx, c, sessionID); rerr != nil { // a discovered corruption or storage failure is the current truth
				return rerr
			}
		}
		return err
	}
	c.graph.Entries = append(c.graph.Entries, adopted)
	c.graph.Session = committedSession
	dropped := c.dropBufferedLocked(buffered) // the head leaves in the same adoption hold as its signal
	if dropped {
		c.bumpLocalRevision()
	}
	c.mu.Unlock()
	h.observeInvalidation(c) // the durable operationless completion advance
	return nil
}

// childCompletionSettled is the one settlement check behind both readiness
// triggers — a member finish and an execution's deferred retirement. It
// returns immediately when the child carries no pending completion; otherwise
// it validates the child's coordinator on the settlement context, re-checks
// the pending obligation under the lock (another trigger may have taken it),
// and either abandons the obligation — a validation failure, or a non-empty
// settled operation identity still current because that execution's terminal
// commit failed — or defers while the child is not terminal, retired, and
// quiet: a non-empty current Operation, an installed run, a non-empty group,
// or a non-empty steering or queued buffer. When settled, it takes the pending
// info; a report-free start releases its member here, and a requested report
// is rendered and delivered to the parent. Every parent-side call happens
// after releasing the child lock.
func (h *Harness) childCompletionSettled(c *coordinator, settledOperationID string) error {
	c.mu.Lock()
	if c.pendingCompletion == nil {
		c.mu.Unlock()
		return nil
	}
	parentID := c.identity.OwnerSessionID
	c.mu.Unlock()

	// The pending obligation's own use covers rendering and the parent
	// delivery, and its acquisition is the same validated access every other
	// graph consumer takes — on the settlement context, which outlives Harness
	// cancellation: a completion that became due is settled and delivered even
	// though the Harness is shutting down, so the abandon branch is genuine
	// unavailability only.
	settleCtx := context.WithoutCancel(h.ctx)
	_, release, err := h.acquireCoordinator(settleCtx, c.sessionID)
	if err != nil {
		// The failed acquisition is the settlement's storage-class outcome:
		// it is retained before the obligation is abandoned, so Wait reports
		// the cause that stopped the settlement.
		h.recordStorageFailure(err)
		// validation failure takes the pending obligation for abandonment
		c.mu.Lock()
		info := c.pendingCompletion
		c.pendingCompletion = nil
		c.mu.Unlock()
		if info != nil {
			h.abandonChildCompletion(parentID, info.completionID)
		}
		return err
	}
	defer release()

	c.mu.Lock()
	info := c.pendingCompletion
	if info == nil { // another trigger took the obligation
		c.mu.Unlock()
		return nil
	}
	if settledOperationID != "" && settledOperationID == c.graph.Session.State.CurrentOperationID {
		// that execution's terminal commit failed: the durable Operation stays
		// running for recovery and the completion obligation is abandoned
		c.pendingCompletion = nil
		c.mu.Unlock()
		h.abandonChildCompletion(parentID, info.completionID)
		return nil
	}
	if !c.quiescentLocked() {
		c.mu.Unlock() // not yet terminal, retired and quiet: the obligation waits
		return nil
	}
	c.pendingCompletion = nil
	if !info.report { // a report-free start ends its member at settlement
		c.mu.Unlock()
		h.abandonChildCompletion(parentID, info.completionID)
		return nil
	}
	content := childCompletionContent(c.graph, info.outputLimit)
	c.mu.Unlock()
	// The parent-side delivery runs on the same settlement context, so a
	// canceled Harness routes it to the durable closed path instead of
	// failing the parent's cold load; a live Harness keeps the ordinary
	// priority decision.
	return h.DeliverBackgroundCompletion(settleCtx, parentID, info.completionID, content)
}

// abandonChildCompletion releases one pending completion's process-local
// ownership without delivery: a report-free settlement ends its member here,
// and an aborted obligation releases its claim. It looks up the parent's
// registry member and, on a resolved coordinator, claim-finishes the
// completion — no corruption validation is bypassed and no compensating write
// is made; durable running child Operations remain for recovery.
func (h *Harness) abandonChildCompletion(parentID, completionID string) {
	h.mu.Lock()
	parent := h.sessions[parentID]
	h.mu.Unlock()
	if parent == nil {
		return
	}
	h.claimFinishBackgroundMember(parent, completionID)
}

// childCompletionContent renders one settled child's parent-facing report:
// the stable header "Session <id>: <terminal status>", followed by a newline
// and a body only when the body is non-empty. The body is the latest committed
// Operation settlement's final assistant text — the last assistant entry
// belonging to that Operation with its text parts joined in stored order — on
// success and interruption, and the settlement's diagnostic on failure.
// Earlier assistant turns, tool results, and previous Operations never
// contribute, and a failure's assistant text never replaces its diagnostic.
// The body is capped at the start's requested limit and, separately, at what
// the whole-report bound leaves after the header's newline; both bounds count
// the trailing marker, and an empty body keeps the header alone. A terminal,
// retired and quiet child with a pending completion always owns a settlement
// entry — a pending completion implies a launched child, an empty current
// Operation leaves every Operation terminal, and each terminal Operation owns
// exactly one settlement — so an absent settlement is unreachable for a
// validated graph and renders as empty content, which the delivery's
// non-empty-content guard rejects uniformly.
func childCompletionContent(g *sessionGraph, outputLimit int) string {
	var settlement *operationSettlementEntry
	for i := range g.Entries {
		if g.Entries[i].Settlement != nil {
			settlement = g.Entries[i].Settlement
		}
	}
	if settlement == nil {
		return "" // unreachable for a validated graph
	}
	header := fmt.Sprintf("Session %s: %s", g.Session.Identity.SessionID, settlement.Status)
	var body string
	switch settlement.Status {
	case OperationSuccess, OperationInterruption:
		var assistant *assistantEntry
		for i := range g.Entries {
			e := &g.Entries[i]
			if e.Assistant != nil && e.Envelope.OperationID == settlement.OperationID {
				assistant = e.Assistant
			}
		}
		if assistant != nil {
			for _, part := range assistant.Content {
				body += part.Text
			}
		}
	default:
		body = settlement.Detail
	}
	limit := outputLimit
	if whole := maxCompletionReportBytes - len(header) - 1; limit <= 0 || limit > whole {
		limit = whole
	}
	if body = truncateCompletionText(body, limit); body == "" {
		return header
	}
	return header + "\n" + body
}

// maxCompletionReportBytes bounds one rendered report as a whole, header,
// separator and body together.
const maxCompletionReportBytes = 1048576

// truncateCompletionText caps one rendered report body at limit UTF-8 bytes,
// retaining the longest complete-character prefix that leaves room for the
// trailing marker. A zero limit is no smaller request than the 1 MiB report
// bound; every positive limit includes the marker bytes, and a limit below the
// marker leaves the marker's own prefix.
func truncateCompletionText(text string, limit int) string {
	const marker = "\n[truncated]"
	if limit <= 0 {
		limit = maxCompletionReportBytes
	}
	if len(text) <= limit {
		return text
	}
	max := limit - len(marker)
	if max < 0 {
		max = 0
	}
	for max > 0 && text[max]&0xC0 == 0x80 { // back off to a complete-character boundary
		max--
	}
	truncated := text[:max] + marker
	if len(truncated) > limit {
		truncated = truncated[:limit]
	}
	return truncated
}

// backgroundHandoffFailure re-checks one starter's handoff set under the
// coordinator mutex: the exact member must remain present, the group must be
// open, the Session must not be stopping as a member, and the Harness context
// must be alive. A nil return admits the start; every
// state outside the valid set fails uniformly — the raw Harness error when
// the Harness is closed, the closed-or-stopping rejection otherwise. No new
// sentinel or model-visible error class exists for a lost handoff.
func (h *Harness) backgroundHandoffFailure(c *coordinator, member *backgroundMember) error {
	c.mu.Lock()
	present := c.group != nil && c.group.members[member.completionID] == member
	open := c.stop == nil && !memberStopping(c)
	c.mu.Unlock()
	if present && open && h.ctx.Err() == nil {
		return nil
	}
	if err := h.ctx.Err(); err != nil {
		return err
	}
	return invalidInput("background group is closed or stopping")
}

// claimFinishBackgroundMember releases one member without delivery on a
// resolved coordinator: the first-writer claim runs under the coordinator
// mutex and the finish follows only when the claim succeeded — a second
// ending is a no-op.
func (h *Harness) claimFinishBackgroundMember(c *coordinator, completionID string) {
	c.mu.Lock()
	m, ok := h.claimBackgroundMember(c, completionID)
	c.mu.Unlock()
	if ok {
		h.finishBackgroundMember(c, m)
	}
}

// liveBackground reports whether one Session's background ownership group
// holds live members. Claimed members count — they remain in the group until
// finished — so the gate covers an in-flight settlement exactly like Stop's
// snapshot does.
func liveBackground(c *coordinator) bool {
	return c.group != nil && len(c.group.members) > 0
}

// Stop stops the addressed Session's group without interrupting its own work.
func (h *Harness) Stop(ctx context.Context, sessionID string) error {
	c, release, err := h.acquireCoordinator(ctx, sessionID)
	if err != nil {
		return err
	}
	defer release()
	return h.stopCoordinator(ctx, c)
}

// stopCoordinator closes and joins one Session's background group.
func (h *Harness) stopCoordinator(ctx context.Context, c *coordinator) error {
	sessionID := c.sessionID
	c.mu.Lock()
	if err := h.unavailableLocked(c, sessionID); err != nil { // an established gone or corrupt cell keeps its error
		c.mu.Unlock()
		return err
	}
	if c.stop != nil {
		interval := c.stop
		c.mu.Unlock()
		<-interval.done
		return interval.err
	}
	if !liveBackground(c) {
		c.mu.Unlock()
		return nil
	}
	interval := &stopInterval{done: make(chan struct{})}
	var members []*backgroundMember
	c.bumpLocalRevision()
	if c.group != nil {
		for _, m := range c.group.members {
			members = append(members, m)
		}
	}
	c.stop = interval
	c.mu.Unlock()
	h.observeInvalidation(c)

	var errs []error

	// Stop the captured members without rediscovering their registration.
	for _, m := range members {
		switch m.kind {
		case memberSession:
			child, release, err := h.acquireCoordinator(ctx, m.id)
			if errors.Is(err, ErrNotFound) {
				continue // launch handoff owns settlement and closes m.done
			}
			if err == nil {
				err = h.stopMember(ctx, child, m)
				release()
			}
			if err != nil {
				errs = append(errs, err)
				if isCorruption(err) { // the unavailable coordinator prevents natural delivery
					h.claimFinishBackgroundMember(c, m.completionID)
				}
			}
		case memberJob:
			if h.deps.Jobs == nil { // unreachable: a live job member implies StartJob succeeded with a stopper
				errs = append(errs, invalidInput("no job stopper configured"))
				continue
			}
			h.deps.Jobs.StopJob(sessionID, m.id) // waits for reaping; the ordinary callback owns the completion
		}
	}

	for _, m := range members {
		<-m.done
	}

	c.mu.Lock()
	c.bumpLocalRevision()
	interval.err = errors.Join(errs...)
	close(interval.done)
	c.stop = nil
	c.mu.Unlock()
	h.observeInvalidation(c)
	return interval.err
}

// stopMember stops exactly the registration captured by its owner's group.
// The owning group interval serializes recursive stops of this member.
func (h *Harness) stopMember(ctx context.Context, c *coordinator, member *backgroundMember) error {
	c.mu.Lock()
	select {
	case <-member.done:
		c.mu.Unlock()
		return nil
	default:
	}
	if err := h.unavailableLocked(c, c.sessionID); err != nil {
		c.mu.Unlock()
		return err
	}
	c.memberStop = member
	c.steering, c.queued = nil, nil
	reservation, run := c.reserved, c.run
	c.bumpLocalRevision()
	c.mu.Unlock()
	if run != nil {
		run.cancel()
	}
	h.observeInvalidation(c)
	err := h.stopCoordinator(ctx, c)
	var installed *activeExecution
	if reservation != nil {
		<-reservation
		c.mu.Lock()
		installed = c.run
		c.mu.Unlock()
		if installed != nil && installed != run {
			installed.cancel()
		}
	}
	if run != nil {
		<-run.done
	}
	if installed != nil && installed != run {
		<-installed.done
	}
	<-member.done
	c.mu.Lock()
	c.memberStop = nil
	c.bumpLocalRevision()
	c.mu.Unlock()
	h.observeInvalidation(c)
	return err
}

// StopAll stops every established Session's background ownership group and
// joins the collected errors. It snapshots the established cells — the ones
// whose graph was validated at least once, so they own every local field
// whether or not a graph is resident — and drives each stop body directly with
// its retained identity and local handles: an idle corpus is never loaded to
// be classified. It is idempotent: a stopped root with no live members and a
// former member with no live group both stop again as no-ops.
func (h *Harness) StopAll(ctx context.Context) error {
	var errs []error
	for _, c := range h.snapshotEstablishedCoordinators() {
		if err := h.stopCoordinator(ctx, c); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// StartJob starts one background job on one Session's ownership group: it
// validates the job identity, the required job-stop dependency, and the spawn
// callback before resolution, admits a job member (the Jobs capability owns
// the jobs limit, so no group cap applies), performs the common handoff, and
// hands the reserved completion identity to the spawn outside locks. The
// spawn owns the concrete start: an error means it did not transfer a live
// completion obligation — the member claim-finishes and the error returns
// unchanged; success means it arranged terminal delivery through
// DeliverBackgroundCompletion using the supplied identity. The caller's
// context covers startup only: a context already done before the spawn refuses
// the start, and a spawn that returned success no longer depends on it.
func (h *Harness) StartJob(ctx context.Context, sessionID, jobID string, spawn func(ctx context.Context, completionID string) error) error {
	if err := validateJobID(jobID, "job id"); err != nil {
		return invalidInput("job id: %v", err)
	}
	if h.deps.Jobs == nil {
		return invalidInput("no job stopper configured")
	}
	if spawn == nil {
		return invalidInput("job start requires a non-nil spawn callback")
	}
	c, release, err := h.acquireCoordinator(ctx, sessionID)
	if err != nil {
		return err
	}
	defer release()
	c.mu.Lock()
	member, err := h.admitBackgroundMember(c, memberJob, jobID)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	h.observeMemberInvalidation(c, jobID) // the job member admission
	if err := h.backgroundHandoffFailure(c, member); err != nil {
		h.claimFinishBackgroundMember(c, member.completionID)
		return err
	}
	if err := ctx.Err(); err != nil {
		h.claimFinishBackgroundMember(c, member.completionID)
		return err
	}
	if err := spawn(ctx, member.completionID); err != nil {
		h.claimFinishBackgroundMember(c, member.completionID)
		return err
	}
	return nil
}

// LaunchChildRequest is the input of one child launch: the open root parent
// Session, the requested Session inputs and message, the caller-generated
// globally unique Operation identity, the calling extension's attribution,
// the requested report body limit, and whether the start requests a completion
// report.
type LaunchChildRequest struct {
	ParentSessionID string
	Inputs          SessionInputs
	ExtensionState  map[string]SessionStateValue
	Content         []model.ContentPart
	OperationID     string // caller-generated, globally unique
	ExtensionID     string // the calling extension's attribution
	OutputLimit     int    // maximum report body size in UTF-8 bytes; 0 keeps the 1 MiB report bound
	Report          bool   // whether the settled child hands the parent a report
}

// LaunchChildResult is the outcome of one child launch: the durable child
// Session identity whose execution has started under the reserved completion.
type LaunchChildResult struct {
	ChildSessionID string
}

// LaunchChildSession launches one child Session as an ordinary Session
// through the shared admission path: it validates the request, requires an
// open parent, prepares the child through the one preparation callback with the
// complete child identity, admits the child member, publishes child and
// admission in one transaction against the re-read parent register, performs
// the common handoff, materializes the child, installs the pending completion
// under the reserved identity, and starts the child execution. The caller's context ends at the launch transaction: a
// caller canceled before it publishes nothing, and after it the Harness owns
// the child, so materialization and settlement run under the Harness context.
// A rejected handoff or failed start settles its member — a
// committed child first attempts its durable Operation's terminal
// interruption, a materialization or settlement failure leaves the durable
// running Operation for recovery. A committed Session identity is returned
// even if the start could not be completed.
func (h *Harness) LaunchChildSession(ctx context.Context, req LaunchChildRequest) (LaunchChildResult, error) {
	if _, err := req.Inputs.normalized(); err != nil {
		return LaunchChildResult{}, invalidInput("inputs: %v", err)
	}
	req.Inputs = req.Inputs.Owned()
	req.ExtensionState = ownSessionStateValues(req.ExtensionState)
	if req.OutputLimit < 0 || req.OutputLimit > 1048576 {
		return LaunchChildResult{}, invalidInput("output limit %d is outside 0..1048576 UTF-8 bytes", req.OutputLimit)
	}
	if err := validateHexID(req.ParentSessionID, "parent session id"); err != nil {
		return LaunchChildResult{}, invalidInput("parent session id: %v", err)
	}
	content, err := validateSubmitInput(req.OperationID, InputOriginPlugin, req.ExtensionID, req.Content)
	if err != nil {
		return LaunchChildResult{}, err
	}
	c, release, err := h.acquireCoordinator(ctx, req.ParentSessionID)
	if err != nil {
		return LaunchChildResult{}, err
	}
	defer release()
	c.mu.Lock()
	view := c.graph.Session // the state the launch transaction re-reads
	c.mu.Unlock()

	childID, err := newHexID()
	if err != nil {
		return LaunchChildResult{}, fmt.Errorf("%w: %v", ErrStorage, err)
	}
	now := time.Now().UTC() // one sampled creation time for created_at and last_activity
	childIdentity := SessionIdentity{
		SessionID:      childID,
		Workspace:      view.Identity.Workspace,
		CreatedAt:      now,
		OwnerSessionID: req.ParentSessionID,
	}
	prepared, capture, prepCtx, cleanup, err := h.prepareExecution(ctx, PreparationSession{
		Identity:       childIdentity,
		Inputs:         req.Inputs,
		ExtensionState: req.ExtensionState,
	}, RequestKindMessage, content)
	if err != nil {
		return LaunchChildResult{}, err
	}
	defer cleanup() // the combined context stays live until publication returns

	c.mu.Lock()
	member, err := h.admitBackgroundMember(c, memberSession, childID)
	if err == nil {
		member.starter = req.ExtensionID
	}
	c.mu.Unlock()
	if err != nil {
		h.releasePrepared(&prepared) // the rejection drops the prepared child execution
		return LaunchChildResult{}, err
	}
	h.observeInvalidation(c) // the child member admission carries no Job identity

	child := SessionRecord{
		Identity: childIdentity,
		State: SessionState{
			Lifecycle:      LifecycleOpen,
			Inputs:         req.Inputs.Owned(),
			ExtensionState: ownSessionStateValues(req.ExtensionState),
			Usage:          UsageTotals{},
			LastActivity:   now,
		},
	}
	err = h.deps.Storage.Transact(prepCtx, func(tx Transaction) error {
		return h.launchTransaction(tx, view, req, child, capture, content)
	})
	if err != nil {
		h.releasePrepared(&prepared) // the launch transaction failed: no child execution was adopted
		h.markCorrupt(req.ParentSessionID, err)
		if errors.Is(err, errRevisionRace) { // a foreign writer changed the durable state under the resident view
			if rerr := h.rematerialize(prepCtx, c, req.ParentSessionID); rerr != nil { // a discovered corruption or storage failure is the current truth
				h.claimFinishBackgroundMember(c, member.completionID)
				return LaunchChildResult{}, rerr
			}
		}
		h.claimFinishBackgroundMember(c, member.completionID)
		return LaunchChildResult{}, publicationError(ctx, err)
	}

	if err := h.backgroundHandoffFailure(c, member); err != nil {
		h.releasePrepared(&prepared) // the rejected handoff settles its member and drops the prepared execution
		return LaunchChildResult{ChildSessionID: childID}, h.settleRejectedChildLaunch(c, member, childID, req.OperationID, req.OutputLimit, req.Report)
	}
	// The committed child installs as a stable cell with its validated durable
	// graph and a call-owned pin, so the pending completion and the execution
	// are installed on the real child rather than on a failed-load placeholder.
	childCoordinator, childRelease, err := h.acquireCoordinator(context.WithoutCancel(h.ctx), childID)
	if err != nil {
		// the durable running Operation stays for recovery; no delivery
		h.releasePrepared(&prepared) // the child materialization failed: the launch never adopts
		h.recordStorageFailure(err)
		h.claimFinishBackgroundMember(c, member.completionID)
		return LaunchChildResult{ChildSessionID: childID}, err
	}
	defer childRelease()
	childCoordinator.mu.Lock()
	childCoordinator.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: req.OutputLimit, report: req.Report}
	childCoordinator.mu.Unlock()
	h.observeInvalidation(childCoordinator) // the durable child creation
	h.observeOperationAdmitted(childID, req.OperationID)
	h.startExecution(childCoordinator, req.OperationID, prepared)
	return LaunchChildResult{ChildSessionID: childID}, nil
}

// launchTransaction is the launch's one transaction: it re-reads and decodes
// the parent register and rejects revision and lifecycle drift, inserts the
// prepared child Session record — its identity already carries the parent
// lineage and both fork-lineage fields empty — and runs the shared
// in-transaction admission producer with the plugin origin, which sets the
// child's current Operation and admission-commit last activity in the same
// transaction. The committed child state is not carried out: the post-commit
// materialization reads the durable truth through the graph validator.
func (h *Harness) launchTransaction(tx Transaction, view SessionRecord, req LaunchChildRequest, child SessionRecord, capture ExecutionCapture, content []model.ContentPart) error {
	key := RegisterKey{SessionID: req.ParentSessionID, Kind: RegisterSession}
	reg, err := tx.ReadRegister(key)
	if err != nil {
		return err
	}
	current, err := decodeSessionRegister(reg)
	if err != nil {
		return corruptSession(req.ParentSessionID, "session register: %v", err)
	}
	if reg.Revision != view.Revision {
		return fmt.Errorf("%w: session %q revision %d changed concurrently to %d", errRevisionRace, req.ParentSessionID, view.Revision, reg.Revision)
	}
	if current.State.Lifecycle != LifecycleOpen {
		return invalidInput("session %q is archived; admission requires an open Session", req.ParentSessionID)
	}
	childPayload, err := encodeSessionRegister(child)
	if err != nil {
		return err
	}
	childReg, err := tx.InsertRegister(RegisterDraft{Key: RegisterKey{SessionID: child.Identity.SessionID, Kind: RegisterSession}, Payload: childPayload})
	if err != nil {
		return err
	}
	child.Revision = childReg.Revision // the producer commits against the inserted revision
	_, _, _, perr := produceAdmission(tx, child, capture, admissionRequest{
		SessionID:   child.Identity.SessionID,
		OperationID: req.OperationID,
		Kind:        RequestKindMessage,
		Origin:      InputOriginPlugin,
		ExtensionID: req.ExtensionID,
		Content:     content,
	})
	if perr != nil {
		return perr
	}
	return nil
}

// settleRejectedChildLaunch resolves one committed child whose handoff was
// rejected. A materialization or terminal-settlement failure leaves the
// running Operation for recovery and releases the member without delivery.
// After successful settlement the start's report choice decides: the usual
// completion owner waits for any remaining child work and delivers its report,
// and a report-free start releases the member at settlement.
func (h *Harness) settleRejectedChildLaunch(c *coordinator, member *backgroundMember, childID, operationID string, outputLimit int, report bool) error {
	child, childRelease, err := h.acquireCoordinator(context.WithoutCancel(h.ctx), childID)
	if err == nil {
		defer childRelease()
		settleCtx := context.WithoutCancel(h.ctx)
		err = interruptRunningOperation(settleCtx, h.deps.Storage, childID, operationID, executionInterruptedDetail)
		if err == nil { // the resident child view predates the settlement commit
			err = h.rematerialize(settleCtx, child, childID)
		}
	}
	if err != nil {
		h.recordStorageFailure(err)
		h.claimFinishBackgroundMember(c, member.completionID)
		return err
	}
	child.mu.Lock()
	child.pendingCompletion = &launchInfo{completionID: member.completionID, outputLimit: outputLimit, report: report}
	child.mu.Unlock()
	return h.childCompletionSettled(child, "")
}
