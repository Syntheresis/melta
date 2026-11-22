package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Syntheresis/melta/agent"
	"github.com/Syntheresis/melta/model"
)

// interruptedToolResultContent is the contract-fixed model-visible content of
// every interrupted-before-execution tool result.
const interruptedToolResultContent = "Tool call interrupted."

// invalidToolResultContent is the contract-fixed model-visible content of the
// validation-error result an invalid prepared plan or invalid returned outcome
// maps onto.
const invalidToolResultContent = "Tool call failed internal validation."

// PermissionDeniedContent is the contract-fixed model-visible content of
// every Harness policy denial; plugins that settle their own denial results
// carry the same published string.
const PermissionDeniedContent = "Permission denied."

// unknownToolResultPrefix is the fixed prefix the shared result producer
// bakes into the model-facing content of a call whose concrete execution is
// possibly-executed-but-unverified (unknown evidence).
const unknownToolResultPrefix = "Execution outcome unknown; the action may have occurred."

// unknownHookToolResultPrefix is the fixed prefix the shared result producer
// bakes into the model-facing content of a proven-unstarted call whose
// pre-tool hook outcome is unknown.
const unknownHookToolResultPrefix = "Pre-tool hook outcome unknown; the hook may have taken effect. The tool was not executed."

// buildToolResultContent renders one settled call's final model-facing content
// exactly once from its producer-assigned execution knowledge and base text:
// an unknown concrete execution and an unknown-outcome pre-tool hook each
// carry their fixed uncertainty prefix joined to the unchanged base by a
// blank line, and every other result stores its base verbatim. It never
// alters status, detail, usage or evidence, and because the built string is
// what the producer stores, no later settlement or recovery can remove a
// prefix.
func buildToolResultContent(evidence ExecutionEvidence, hookUnknown bool, base string) string {
	switch {
	case evidence == ExecutionUnknown:
		return unknownToolResultPrefix + "\n\n" + base
	case hookUnknown:
		return unknownHookToolResultPrefix + "\n\n" + base
	default:
		return base
	}
}

// normalizationFailureContent renders the bounded diagnostic of a failed
// bound-tool normalization: fixed internal-validation content for a
// malformed callback outcome or empty diagnostic, otherwise the bounded
// diagnostic.
func normalizationFailureContent(cause error) string {
	if errors.Is(cause, errMalformedNormalization) {
		return invalidToolResultContent
	}
	if content := boundedToolDiagnostic(cause); content != "" {
		return content
	}
	return invalidToolResultContent
}

// shapingFailureDiagnostic renders one aborted shaping chain's bounded
// processing failure: the bounded cause diagnostic, falling back to the fixed
// internal-validation content when the cause carries no usable text, so a
// failed shaper always publishes its processing failure.
func shapingFailureDiagnostic(cause error) string {
	if content := boundedToolDiagnostic(cause); content != "" {
		return content
	}
	return invalidToolResultContent
}

// maxToolDiagnosticBytes bounds one Harness-produced validation diagnostic
// carried in a tool-result content.
const maxToolDiagnosticBytes = 512

// executionInterruptedDetail is the diagnostic detail of the terminal
// interruption the Harness settles when execution cancellation stops a
// callback, mirroring the Agent's own cancellation checkpoints.
const executionInterruptedDetail = "agent interrupted"

// genericExecutionFailureDetail is the non-empty standard terminal detail one
// execution error whose own text is empty still settles with; an actual
// nonempty diagnostic is always carried verbatim.
const genericExecutionFailureDetail = "execution failure"

// failureDetail renders one Go failure cause as the non-empty terminal detail
// the closed settlement validator requires.
func failureDetail(cause error) string {
	if cause == nil {
		return genericExecutionFailureDetail
	}
	if text := cause.Error(); text != "" {
		return text
	}
	return genericExecutionFailureDetail
}

// publicationFailure is the one private publication-origin wrapper: it marks an
// error raised while publishing durable intent, result, checkpoint or
// terminal state, preserving the original cause through errors.Is and
// errors.As. A publication failure is distinguished by the boundary where it
// occurred — never by its error identity — and retains the actual durable
// running state for recovery, with no guessed follow-up terminal write.
type publicationFailure struct{ cause error }

func (e *publicationFailure) Error() string { return e.cause.Error() }
func (e *publicationFailure) Unwrap() error { return e.cause }

// wrapPublication wraps one publication-boundary cause.
func wrapPublication(cause error) error {
	if cause == nil {
		return nil
	}
	return &publicationFailure{cause: cause}
}

// isPublicationFailure reports whether one error originated at a durable
// publication boundary.
func isPublicationFailure(err error) bool {
	var failure *publicationFailure
	return errors.As(err, &failure)
}

var errExecutionInterrupted = errors.New("execution interrupted")

// EffectInterrupted reports whether one callback failure declared interruption.
func EffectInterrupted(err error) bool {
	var effect *EffectError
	return errors.As(err, &effect) && effect.Interrupted
}

// modelEffectIntent is the committed intent of one logical model effect: the
// facts the result transaction needs after the callback returns, including
// the codec-adopted effective input evidence the intent published.
type modelEffectIntent struct {
	sessionID  string
	expected   model.ModelRef
	resultID   string
	compaction bool // true for the compact effect's intent: its usage is summary usage
	requestID  string
	evidence   *RequestEvidence // the adopted evidence the committed intent carries
}

// hookEffectBinding is one settled hook execution's binding: its extension
// and hook identities and exactly one of the tool call or request it executed
// for. The two binding families stay disjoint even for equal opaque strings.
type hookEffectBinding struct {
	hookID    string
	callID    string
	requestID string
}

// pendingCallIntent carries one pending-call effect's intent input: a hook
// effect its extension identity and consumed input bytes, a tool effect its
// bound implementation, normalized selection and contributing hook provenance.
type pendingCallIntent struct {
	hookID               string
	extensionID          string
	inputArgumentsBase64 string
	finalArguments       json.RawMessage
	contributionIDs      []HookIdentity
	implementation       *ToolImplementation
}

// toolSelection is the final effective argument value a settled tool call
// consumed and the ordered hook provenance that produced it. Results that
// never accepted a value carry neither.
type toolSelection struct {
	arguments      json.RawMessage
	hooks          []HookIdentity
	implementation *ToolImplementation
}

// modelResult is the settlement content of one logical model effect: the
// produced assistant entry when the output carried an eligible payload, the
// terminal classification when the effect settles the Operation, the terminal
// no-output attempt usage when a reported usage has no assistant entry to ride,
// and the maintenance facts a maintenance terminal retains. The usage model
// is the intent's expected identity while an intent is live; without one the
// result declares its own usage model. The compaction flag carries that
// usage's provenance: true marks summary usage produced by the compaction
// summarizer, and conversation producers leave it false.
// successContext is the execution context of an outer prospective success
// only; the publisher's shared guard consults it before the transaction.
// Effect-origin terminals, compaction commits and recovery leave it nil.
type modelResult struct {
	assistant      *assistantEntry   // nil when no eligible assistant payload exists
	terminal       OperationState    // empty for ready; failure or interruption otherwise
	detail         string            // terminal detail; empty for ready
	usage          attemptUsage      // the effect's accepted-attempt usage, carried by the settlement only when no assistant entry carries it.
	usageModel     model.ModelRef    // the terminal no-output usage model when no intent is live; zero otherwise
	compaction     bool              // true when the carried terminal usage is summary usage
	facts          *maintenanceFacts // retained maintenance input/consumed evidence a maintenance terminal settles
	maintenanceID  string            // the maintenance producer's own request identity for settlement facts
	successContext context.Context
}

// modelEffect encloses one complete invocation of the opened conversation
// invocation's model function in one logical model-effect intent/result pair:
// it commits the active-effect intent carrying the round's request identity,
// purpose, model, and effective input evidence, dispatches the physical
// attempts against the Engine-accepted request, invokes the assembly callback exactly once
// after a stream is accepted, derives the settlement from the assembled
// output, validates it, and commits the result and complete next Operation
// state before returning the committed settlement. The automatic maintenance
// orchestration and the request contributions belong to the request source;
// this effect consumes the round they built. The assistant entry retains raw
// calls; normalization belongs to dispatch. The retry classifier belongs to
// the enclosing Execution.
func (d *executionDriver) modelEffect() agent.ModelEffect {
	h := d.harness
	inv := d.conversation
	attempt := inv.callbacks.Model
	capture := d.capture
	return func(ctx context.Context, req model.Request, assemble agent.AssemblyCallback) (agent.ModelSettlement, error) {
		// The advertisement of this logical effect is the effective tool view
		// of its frozen round: the Engine derived it from the validated
		// request before the effect runs and hands the dispatch gate a
		// per-call copy.
		intent, err := h.beginModelEffect(ctx, d.coordinator, d.operationID, inv.round.requestID, InvocationPurposeConversation, capture.Model, &inv.round.net, nil)
		if err != nil {
			// A marked publication failure propagates before any cancellation
			// rule, regardless of a concurrent cancellation. Only an unmarked
			// cancellation observed before the intent transaction was ever
			// attempted settles the cancellation outcome: the run never sees a
			// cancellation-shaped error out of an active effect.
			if !isPublicationFailure(err) && isExecutionCancellation(err) && ctx.Err() != nil {
				if _, cerr := d.commitEffectResult(context.WithoutCancel(h.ctx), nil, modelResult{
					terminal: OperationInterruption,
					detail:   executionInterruptedDetail,
				}); cerr != nil {
					return agent.ModelSettlement{}, cerr
				}
				return agent.ModelSettlement{Disposition: agent.DispoInterruption, Detail: executionInterruptedDetail}, nil
			}
			return agent.ModelSettlement{}, err
		}
		// Execution cancellation stops new callbacks: a context that died
		// between the committed intent and the callback settles the Operation
		// as terminal interruption without ever starting the callback.
		if ctx.Err() != nil {
			return d.interruptModelEffect(intent, attemptUsage{}, nil)
		}
		// Result and terminal transactions run without cancellation so an
		// already-produced result or required terminal settlement publishes.
		settleCtx := context.WithoutCancel(h.ctx)
		// The physical attempt receives the producing conversation invocation
		// and this logical effect's own reserved result identity; retries
		// share that logical effect.
		// The one shared attempt loop lives inside the committed intent:
		// failures are classified by the retry policy, waits run behind the
		// cancellation checks, and an accepted stream is assembled once.
		attemptRes := runModelAttempts(ctx, req, intent.evidence.Parameters, attempt, func(stream model.Stream) (model.Output, error) {
			return assemble(intent.expected, h.observeStream(intent.sessionID, d.operationID, stream))
		}, d.execution.Retry, func() {
			h.emitFact(HarnessFact{Kind: FactResponseRestarted, SessionID: intent.sessionID, OperationID: d.operationID})
		})
		if attemptRes.usageErr != nil { // an inexact retried-usage sum aborts before any result publication
			return agent.ModelSettlement{}, attemptRes.usageErr
		}
		settle := func(cause error) (agent.ModelSettlement, error) {
			if _, err := d.commitEffectResult(settleCtx, &intent, modelResult{
				terminal: OperationFailure,
				detail:   failureDetail(cause),
				usage:    attemptRes.usage,
			}); err != nil {
				return agent.ModelSettlement{}, err
			}
			return agent.ModelSettlement{}, cause
		}
		switch attemptRes.kind {
		case attemptInterrupted:
			return d.interruptModelEffect(intent, attemptRes.usage, nil)
		case attemptInvalid:
			return settle(attemptRes.err)
		case attemptFailed:
			committed := agent.ModelSettlement{Disposition: agent.DispoFailure, Detail: failureDetail(attemptRes.err)}
			if _, err := d.commitEffectResult(settleCtx, &intent, modelResult{
				terminal: OperationFailure,
				detail:   failureDetail(attemptRes.err),
				usage:    attemptRes.usage,
			}); err != nil {
				return agent.ModelSettlement{}, err
			}
			return committed, nil
		}
		set := derivedSettlement(attemptRes.output)
		owned, err := agent.ValidateModelSettlement(intent.expected, set)
		if err != nil {
			return settle(err)
		}
		// A completed call whose identity the Operation already published can
		// never commit: one published call is one settlement unit, and a
		// second assistant entry repeating the identity is durable corruption.
		if owned.Output != nil && owned.Output.Message != nil && len(owned.Output.Message.ToolCalls) > 0 {
			published := publishedCallIDs(d.coordinator, d.operationID)
			for _, call := range owned.Output.Message.ToolCalls {
				if published[call.ID] {
					return settle(&agent.ProtocolError{Boundary: "model", Detail: fmt.Sprintf("completed output repeats tool call id %q already published by operation %q", call.ID, d.operationID)})
				}
			}
		}
		assistant, err := newAssistantEntry(intent.sessionID, d.operationID, intent.resultID, owned.Output, attemptRes.usage, intent.requestID, intent.evidence)
		if err != nil {
			return agent.ModelSettlement{}, err
		}
		var res modelResult
		switch owned.Disposition {
		case agent.DispoReady:
			res = modelResult{assistant: assistant}
		case agent.DispoFailure:
			res = modelResult{assistant: assistant, terminal: OperationFailure, detail: owned.Detail, usage: attemptRes.usage}
		case agent.DispoInterruption:
			res = modelResult{assistant: assistant, terminal: OperationInterruption, detail: owned.Detail, usage: attemptRes.usage}
		}
		if _, err := d.commitEffectResult(settleCtx, &intent, res); err != nil {
			return agent.ModelSettlement{}, err
		}
		return owned, nil
	}
}

// contextMeasurement samples the newest reported conversation usage after
// the current checkpoint boundary under a brief coordinator hold — the
// optional conversation measurement the selected strategy plans over. A
// missing report stays missing.
func contextMeasurement(c *coordinator) *UsageCount {
	c.mu.Lock()
	usage := latestContextUsage(c.graph.Entries, c.graph.Session.State.CheckpointEntryID)
	c.mu.Unlock()
	return usage
}

// interruptModelEffect settles one committed model effect intent as the fixed
// terminal interruption with no assembled output; the accepted attempts'
// usage and maintenance facts ride the settlement entry.
func (d *executionDriver) interruptModelEffect(intent modelEffectIntent, usage attemptUsage, facts *maintenanceFacts) (agent.ModelSettlement, error) {
	settleCtx := context.WithoutCancel(d.harness.ctx)
	committed := agent.ModelSettlement{Disposition: agent.DispoInterruption, Detail: executionInterruptedDetail}
	if _, err := d.commitEffectResult(settleCtx, &intent, modelResult{
		terminal:      OperationInterruption,
		detail:        executionInterruptedDetail,
		usage:         usage,
		facts:         facts,
		maintenanceID: facts.producer(),
	}); err != nil {
		return agent.ModelSettlement{}, err
	}
	return committed, nil
}

// derivedSettlement maps one assembled output onto the closed disposition
// table: completed is ready, interrupted interrupts with its own detail, and
// errored fails with its detail and no assistant entry.
func derivedSettlement(out model.Output) agent.ModelSettlement {
	switch out.Status {
	case model.OutputCompleted:
		return agent.ModelSettlement{Disposition: agent.DispoReady, Output: &out}
	case model.OutputInterrupted:
		return agent.ModelSettlement{Disposition: agent.DispoInterruption, Output: &out, Detail: out.Detail}
	default:
		return agent.ModelSettlement{Disposition: agent.DispoFailure, Output: &out, Detail: out.Detail}
	}
}

// standardRetryPolicy is the nil-Retry classifier: HTTP 429 and 5xx failures,
// errors wrapping net.OpError or model.ErrStreamBroken, and pre-response io.EOF
// or io.ErrUnexpectedEOF failures permit retries 1..3 at 2s/4s/8s; every other
// failure stops.
func standardRetryPolicy(cause error, failed int) (time.Duration, bool) {
	if failed > 3 || !retryableTransportFailure(cause) {
		return 0, false
	}
	return 2 * time.Duration(1<<(failed-1)) * time.Second, true
}

// retryableTransportFailure classifies one failed physical attempt under the
// standard policy's fixed transport-failure classes.
func retryableTransportFailure(cause error) bool {
	var status *model.HTTPStatusError
	if errors.As(cause, &status) {
		return status.StatusCode == 429 || (status.StatusCode >= 500 && status.StatusCode <= 599)
	}
	var op *net.OpError
	if errors.As(cause, &op) {
		return true
	}
	return errors.Is(cause, model.ErrStreamBroken) || errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF)
}

// publishedCallIDs returns the tool call IDs the Operation's committed
// assistant entries already publish, read from the coordinator's validated
// view.
func publishedCallIDs(c *coordinator, operationID string) map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := map[string]bool{}
	for _, entry := range c.graph.Entries {
		if entry.Envelope.OperationID != operationID || entry.Assistant == nil {
			continue
		}
		for _, call := range entry.Assistant.ToolCalls {
			ids[call.ID] = true
		}
	}
	return ids
}

// beginModelEffect commits the one active-effect intent of a logical model
// effect: it reserves the effect's result entry identity, records the request
// identity, actual model, purpose, and (for a conversation effect) the
// effective input evidence it consumes, and publishes or preserves the one
// suspended request state in the same transaction — a conversation effect
// publishes its round's state when none is published, and a maintenance effect
// publishes the supplied summary request state under its own producer
// identity. The reserved identity stays free until the model result or the
// consuming settlement commits under it. The caller continues on the
// codec-adopted record the transaction wrote.
// beginModelEffect commits the one active-effect intent of a model effect.
// A cancellation observed before the intent transaction is invoked returns
// unwrapped and may use the caller's ordinary interruption settlement. Once
// the transaction is invoked it runs on a cancellation-free copy of the
// execution context — values only, like the result and terminal publications
// — so every error from it is a real store failure and a publication-origin
// failure at the intent boundary: it propagates with the durable state
// retained for recovery and is never followed by a guessed compensating
// terminal write.
func (h *Harness) beginModelEffect(ctx context.Context, c *coordinator, operationID string, requestID string, purpose InvocationPurpose, mdl model.ModelRef, evidence *RequestEvidence, requestState *RunningRequest) (modelEffectIntent, error) {
	c.mu.Lock()
	op, ok := c.graph.Operation(operationID)
	if !ok {
		sessionID := c.graph.Session.Identity.SessionID
		c.mu.Unlock()
		return modelEffectIntent{}, wrapPublication(fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, sessionID))
	}
	if op.State.Status != OperationRunning {
		c.mu.Unlock()
		return modelEffectIntent{}, wrapPublication(invalidInput("operation %q is %s; a model effect requires a running Operation", operationID, op.State.Status))
	}
	if op.State.ActiveEffect != nil {
		c.mu.Unlock()
		return modelEffectIntent{}, wrapPublication(invalidInput("operation %q already carries an active effect; effects never nest", operationID))
	}
	if ctx.Err() != nil { // cancellation observed before the Transact is ever invoked is not a
		// publication failure: it returns unwrapped and the caller's ordinary
		// interruption settlement applies. An invoked Transact can no longer observe this
		// context, so every error from it is a real store failure marked publication-origin.
		c.mu.Unlock()
		return modelEffectIntent{}, ctx.Err()
	}
	resultID, err := newHexID()
	if err != nil {
		c.mu.Unlock()
		return modelEffectIntent{}, wrapPublication(fmt.Errorf("%w: %v", ErrStorage, err))
	}
	effectModel := mdl
	var updated OperationRecord
	// Once the pre-Transact check admits the transition, the durable intent
	// transaction runs without execution cancellation or deadline — values
	// only, exactly like the result and terminal publications — so a shutdown
	// that lands mid-transaction never aborts the admitted intent. The
	// caller's execution-context gate settles the interruption afterwards.
	err = h.deps.Storage.Transact(context.WithoutCancel(ctx), func(tx Transaction) error {
		key := RegisterKey{SessionID: op.Admission.SessionID, Kind: RegisterOperation, OperationID: operationID}
		reg, err := tx.ReadRegister(key)
		if err != nil {
			return err
		}
		current, err := decodeOperationRegister(reg)
		if err != nil {
			return corruptSession(op.Admission.SessionID, "operation register %q: %v", operationID, err)
		}
		// a violated semantic precondition outranks the conflict class
		if current.State.Status != OperationRunning {
			return invalidInput("operation %q is %s; a model effect requires a running Operation", operationID, current.State.Status)
		}
		if current.State.ActiveEffect != nil {
			return invalidInput("operation %q already carries an active effect; effects never nest", operationID)
		}
		if reg.Revision != op.Revision {
			return fmt.Errorf("%w: operation %q revision %d changed concurrently to %d", errRevisionRace, operationID, op.Revision, reg.Revision)
		}
		next := current
		next.State.ActiveEffect = &ActiveEffect{
			Kind:          EffectModel,
			ResultEntryID: resultID,
			RequestID:     requestID,
			Model:         &effectModel,
			Purpose:       purpose,
			Evidence:      evidence,
		}
		switch purpose {
		case InvocationPurposeConversation:
			if evidence != nil {
				if next.State.Request == nil {
					next.State.Request = &RunningRequest{
						RequestID:         requestID,
						Purpose:           InvocationPurposeConversation,
						CheckpointEntryID: evidence.CheckpointEntryID,
						BoundaryEntryID:   evidence.BoundaryEntryID,
					}
				} else {
					// Refresh the already-published request's canonical references to
					// the rebuilt projection pair — the same checkpoint and tail the
					// incoming evidence names — so the running state always describes
					// the projection actually consumed. Hook-result references stay.
					advanced := *next.State.Request
					advanced.CheckpointEntryID = evidence.CheckpointEntryID
					advanced.BoundaryEntryID = evidence.BoundaryEntryID
					next.State.Request = &advanced
				}
			}
		case InvocationPurposeMaintenance:
			if next.State.Request == nil && requestState != nil {
				published := *requestState
				next.State.Request = &published
			}
		}
		payload, err := encodeOperationRegister(next)
		if err != nil {
			return err
		}
		replaced, err := tx.ReplaceRegister(key, reg.Revision, payload)
		if err != nil {
			return err
		}
		// The transition adopts the codec-owned record it wrote: the caller's
		// cached state, continuation, and dispatch evidence all come from the
		// committed value.
		adopted, err := decodeOperationRegister(replaced)
		if err != nil {
			return corruptSession(op.Admission.SessionID, "committed operation register %q: %v", operationID, err)
		}
		updated = adopted
		return nil
	})
	if err != nil {
		h.markCorrupt(op.Admission.SessionID, err)
		c.mu.Unlock()
		if errors.Is(err, errRevisionRace) { // a foreign writer changed the durable state under the cached view
			if rerr := h.rematerialize(ctx, c, op.Admission.SessionID); rerr != nil { // a discovered corruption or storage failure is the current truth
				return modelEffectIntent{}, wrapPublication(rerr)
			}
		}
		return modelEffectIntent{}, wrapPublication(err)
	}
	c.graph.replaceOperation(operationID, updated)
	c.mu.Unlock()
	committed := updated.State.ActiveEffect
	intent := modelEffectIntent{
		sessionID: op.Admission.SessionID,
		expected:  op.Admission.Execution.Model,
		resultID:  resultID,
		requestID: requestID,
	}
	if purpose == InvocationPurposeMaintenance {
		intent.compaction = true
		intent.expected = mdl
	}
	if committed != nil && committed.Evidence != nil {
		intent.evidence = committed.Evidence
	}
	return intent, nil
}

// commitEffectResult commits the one result transaction of a settled effect:
// the semantic result entries, usage, the complete next Operation state, and
// the required Session state. A running result clears the active effect and
// records the completed calls' reservations; the suspended running request
// state survives a result that produced a pending batch and clears at every
// request's result with no batch — including the completed summary request
// before the strategy folds its output. A
// terminal result runs the common terminal helper — preserving any produced
// assistant payload, interrupting every call that will not execute, appending
// exactly one interruption signal, appending the settlement entry, writing the
// terminal Operation, and clearing the Session current Operation — all
// atomically. A non-nil intent must find its committed model effect intent,
// whose reserved identity the settlement consumes when no assistant payload
// exists; a nil intent (the outer terminal settlement after agent.Run)
// requires a quiet Operation and always assigns a fresh settlement identity. A
// publication failure leaves the committed running/intent state for recovery.
func (d *executionDriver) commitEffectResult(ctx context.Context, intent *modelEffectIntent, res modelResult) (OperationRecord, error) {
	h := d.harness
	c := d.coordinator
	operationID := d.operationID
	c.mu.Lock()
	if res.successContext != nil { // the shared success guard: one hold with the publication, before the transaction
		if res.successContext.Err() != nil || c.interruptOp == operationID || memberStopping(c) {
			// Interrupt winning this hold cannot be missed; a foreign
			// marker or a root stop's stopping state never converts the
			// success, and an Interrupt after the committed success cannot
			// rewrite it. The conversion keeps the result's own facts,
			// evidence and usage: only the prospective success becomes the
			// ordinary interruption.
			res.terminal, res.detail = OperationInterruption, executionInterruptedDetail
		}
	}
	viewOp, ok := c.graph.Operation(operationID)
	if !ok {
		sessionID := c.graph.Session.Identity.SessionID
		c.mu.Unlock()
		return OperationRecord{}, wrapPublication(fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, sessionID))
	}
	sessionID := viewOp.Admission.SessionID
	viewSession := c.graph.Session

	var (
		committedOp   OperationRecord
		committedSess SessionRecord
		newEntries    []graphEntry
	)
	err := h.deps.Storage.Transact(ctx, func(tx Transaction) error {
		sessionKey := RegisterKey{SessionID: sessionID, Kind: RegisterSession}
		sreg, err := tx.ReadRegister(sessionKey)
		if err != nil {
			return err
		}
		currentSession, err := decodeSessionRegister(sreg)
		if err != nil {
			return corruptSession(sessionID, "session register: %v", err)
		}
		opKey := RegisterKey{SessionID: sessionID, Kind: RegisterOperation, OperationID: operationID}
		oreg, err := tx.ReadRegister(opKey)
		if err != nil {
			return err
		}
		currentOp, err := decodeOperationRegister(oreg)
		if err != nil {
			return corruptSession(sessionID, "operation register %q: %v", operationID, err)
		}
		// violated semantic preconditions outrank the conflict class
		if currentOp.State.Status != OperationRunning {
			return invalidInput("operation %q is %s; its model effect requires a running Operation", operationID, currentOp.State.Status)
		}
		if intent != nil {
			if currentOp.State.ActiveEffect == nil || currentOp.State.ActiveEffect.Kind != EffectModel ||
				currentOp.State.ActiveEffect.ResultEntryID != intent.resultID {
				return invalidInput("operation %q carries no committed model effect intent for result identity %q", operationID, intent.resultID)
			}
		} else if currentOp.State.ActiveEffect != nil {
			return invalidInput("operation %q carries an active effect; the settlement requires a quiet Operation", operationID)
		}
		if sreg.Revision != viewSession.Revision {
			return fmt.Errorf("%w: session %q revision %d changed concurrently to %d", errRevisionRace, sessionID, viewSession.Revision, sreg.Revision)
		}
		if oreg.Revision != viewOp.Revision {
			return fmt.Errorf("%w: operation %q revision %d changed concurrently to %d", errRevisionRace, operationID, viewOp.Revision, oreg.Revision)
		}

		// A terminal result runs the common terminal helper on this
		// transaction's own decoded records.
		if res.terminal != "" {
			// The usage model and the usage provenance come from the live
			// intent; a quiet direct settlement declares its own.
			usageModel := res.usageModel
			compactionUsage := res.compaction
			if intent != nil {
				usageModel = intent.expected
				compactionUsage = intent.compaction
			}
			// Every terminal kind accounts the active execution's
			// accumulated uncommitted maintenance facts exactly once,
			// together with the settled effect's own retained facts.
			facts := d.consumePendingFacts(res.facts)
			var err error
			committedOp, committedSess, newEntries, err = commitTerminalSettlement(tx, sessionID, operationID, currentSession, currentOp, sreg.Revision, oreg.Revision, res.terminal, res.detail, res.assistant, usageModel, res.usage, compactionUsage, facts, res.maintenanceID)
			if err != nil {
				return err
			}
			return nil
		}

		// The transaction's usage contribution rides the produced assistant
		// entry: the last accepted and retried attempts' usage all land on the
		// producer entry, the Operation, and the Session.
		contribution := UsageTotals{}
		if res.assistant != nil {
			contribution, err = usageContribution(res.assistant.Source, res.assistant.Usage, res.assistant.retryUsage)
			if err != nil {
				return err
			}
		}
		opUsage, err := addUsageTotals(currentOp.State.Usage, contribution)
		if err != nil {
			return err
		}
		sessionUsage, err := addUsageTotals(currentSession.State.Usage, contribution)
		if err != nil {
			return err
		}

		// The assistant entry consumes the effect's reserved identity; its
		// completed calls become the durable tool-call intent.
		var pending []PendingToolCall
		if res.assistant != nil {
			payload, err := encodeAssistantEntry(*res.assistant)
			if err != nil {
				return err
			}
			adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
				SessionID:   sessionID,
				ID:          res.assistant.EntryID,
				OperationID: operationID,
				Kind:        EntryAssistant,
				Payload:     payload,
			})
			if err != nil {
				return err
			}
			newEntries = append(newEntries, adopted)
			for _, call := range res.assistant.ToolCalls {
				pending = append(pending, PendingToolCall{
					AssistantEntry: EntryRef{SessionID: sessionID, EntryID: res.assistant.EntryID},
					CallID:         call.ID,
					ResultEntryID:  call.ResultEntryID,
				})
			}
		}

		next := currentOp
		next.State.ActiveEffect = nil
		next.State.Usage = opUsage
		next.State.Status = OperationRunning
		next.State.PendingToolCalls = append(next.State.PendingToolCalls, pending...)
		if len(next.State.PendingToolCalls) == 0 {
			// A request with no surviving batch is complete: the conversation
			// result clears its own request, and a completed maintenance model
			// clears the one summary request it was shaping before the pure
			// folding stage consumes its output.
			next.State.Request = nil
		}
		if intent != nil && intent.compaction {
			// The completed maintenance model intent's empty running result leaves its
			// maintenance facts process-local until a successful checkpoint or terminal
			// publication records them: the marker carries the incomplete-maintenance
			// knowledge across that gap.
			next.State.UnreportedMaintenance = true
		}
		opPayload, err := encodeOperationRegister(next)
		if err != nil {
			return err
		}
		replaced, err := tx.ReplaceRegister(opKey, oreg.Revision, opPayload)
		if err != nil {
			return err
		}
		adoptedOp, err := decodeOperationRegister(replaced)
		if err != nil {
			return corruptSession(sessionID, "committed operation register %q: %v", operationID, err)
		}
		committedOp = adoptedOp

		sessionState := currentSession.State
		sessionState.Usage = sessionUsage
		committedSess = SessionRecord{Identity: currentSession.Identity, State: sessionState}
		sessionPayload, err := encodeSessionRegister(committedSess)
		if err != nil {
			return err
		}
		replacedSession, err := tx.ReplaceRegister(sessionKey, sreg.Revision, sessionPayload)
		if err != nil {
			return err
		}
		committedSess.Revision = replacedSession.Revision
		return nil
	})
	if err != nil {
		h.markCorrupt(sessionID, err)
		c.mu.Unlock()
		if errors.Is(err, errRevisionRace) { // a foreign writer changed the durable state under the cached view
			if rerr := h.rematerialize(ctx, c, sessionID); rerr != nil { // a discovered corruption or storage failure is the current truth
				return OperationRecord{}, wrapPublication(rerr)
			}
		}
		return OperationRecord{}, wrapPublication(err)
	}
	if res.terminal != "" {
		d.completePending() // the accumulated facts are consumed only after their successful publication
	}
	c.graph.Entries = append(c.graph.Entries, newEntries...)
	c.graph.replaceOperation(operationID, committedOp)
	c.graph.Session = committedSess
	c.mu.Unlock()
	h.observeInvalidation(c) // every result/terminal Session register replacement advances
	h.emitToolResultFacts(newEntries, sessionID, operationID)
	if res.terminal != "" {
		h.observeOperationSettled(sessionID, operationID, res.terminal)
	}
	return committedOp, nil
}

// commitTerminalSettlement performs the complete terminal transition in one
// transaction against freshly decoded register records: it preserves any
// produced assistant payload with its completed calls' reservations, writes
// model.ResultInterrupted under every remaining committed tool-result identity
// whose call will not execute — a terminal Operation leaves no unresolved
// call — appends exactly one interruption signal for an interruption terminal,
// appends the Operation settlement entry, writes the terminal Operation with
// its usage totals, and clears the Session current Operation and the running
// request state, all atomically. The settlement entry consumes a committed
// model effect's reserved identity when no assistant payload exists; every
// other settlement takes a fresh identity. A no-output settlement that closed
// a request owns that request's evidence exactly once — the committed model
// intent's effective evidence when one is live, otherwise the retained
// maintenance carry — and the settlement never repeats evidence an assistant
// entry already owns. A maintenance settlement's facts (its producer identity,
// actual input, and consumed evidence) settle on the successful checkpoint or
// here, never both. Recovery calls it from its own re-reading transaction
// with no process-local facts. It performs no precondition check of its own:
// the caller passes only records whose state it may settle.
func commitTerminalSettlement(tx Transaction, sessionID, operationID string, currentSession SessionRecord, currentOp OperationRecord, sregRevision, oregRevision int64, terminal OperationState, detail string, assistant *assistantEntry, usageModel model.ModelRef, usage attemptUsage, compactionUsage bool, facts *maintenanceFacts, maintenanceID string) (OperationRecord, SessionRecord, []graphEntry, error) {
	var newEntries []graphEntry

	// The transaction's usage contribution is additive: the ordinary
	// assistant-carried or terminal no-output usage, the retried attempts'
	// usage, and the retained facts' per-producer usage all land when they
	// occur; the same totals land on the producer entry, the Operation, and
	// the Session, and an absent report contributes nothing.
	ref, last, retried := usageModel, usage.last, usage.retryUsage
	if assistant != nil {
		ref, last, retried = assistant.Source, assistant.Usage, assistant.retryUsage
	}
	contribution, err := usageContribution(ref, last, retried)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	factUsage, err := factsUsageContribution(factsList(facts))
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	contribution, err = addUsageTotals(contribution, factUsage)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	opUsage, err := addUsageTotals(currentOp.State.Usage, contribution)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	sessionUsage, err := addUsageTotals(currentSession.State.Usage, contribution)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}

	// The assistant entry consumes the effect's reserved identity; its
	// completed calls become the durable tool-call intent.
	var pending []PendingToolCall
	if assistant != nil {
		payload, err := encodeAssistantEntry(*assistant)
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
			SessionID:   sessionID,
			ID:          assistant.EntryID,
			OperationID: operationID,
			Kind:        EntryAssistant,
			Payload:     payload,
		})
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		newEntries = append(newEntries, adopted)
		for _, call := range assistant.ToolCalls {
			pending = append(pending, PendingToolCall{
				AssistantEntry: EntryRef{SessionID: sessionID, EntryID: assistant.EntryID},
				CallID:         call.ID,
				ResultEntryID:  call.ResultEntryID,
			})
		}
	}

	// One interrupted hook_result under an active hook effect's reserved
	// identity: recovery settles an unfinished hook without replaying or
	// resuming it, with conservative unknown knowledge, the intent's
	// extension identity and binding, and the consumed input the intent
	// committed — before the interrupted tool results commit. Request-bound
	// hooks carry no fabricated tool call.
	if currentOp.State.ActiveEffect != nil && currentOp.State.ActiveEffect.Kind == EffectHook {
		intent := currentOp.State.ActiveEffect
		hook := hookResultEntry{
			SessionID:            sessionID,
			EntryID:              intent.ResultEntryID,
			OperationID:          operationID,
			HookID:               intent.HookID,
			ExtensionID:          intent.ExtensionID,
			ToolCallID:           intent.ToolCallID,
			RequestID:            intent.RequestID,
			Purpose:              intent.Purpose,
			Status:               hookInterrupted,
			Evidence:             ExecutionUnknown,
			Error:                recoveryInterruptedDetail,
			InputArgumentsBase64: intent.InputArgumentsBase64,
		}
		if intent.Evidence != nil {
			consumed := *intent.Evidence
			hook.InputEvidence = &consumed
		}
		payload, err := encodeHookResultEntry(hook)
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
			SessionID:   sessionID,
			ID:          hook.EntryID,
			OperationID: operationID,
			Kind:        EntryHookResult,
			Payload:     payload,
		})
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		newEntries = append(newEntries, adopted)
	}

	// One interrupted result under every remaining committed tool-result
	// identity whose call will not execute. The active tool effect's
	// interrupted result copies the committed intent's accepted arguments and
	// provenance and carries conservative unknown evidence: the executor's
	// external outcome is possibly-executed-but-unverified. A call carries
	// the hook-uncertainty prefix — with proven-unstarted concrete evidence —
	// when an active hook effect addresses it, or when an earlier committed
	// tool-bound hook result already holds unknown evidence (the quiet gap a
	// crash between the hook result's and the tool result's transactions
	// leaves): an unknown pre-tool hook's notice is never dropped. Unstarted
	// pending siblings carry plain evidence neither. The committed hook
	// result scan serves synthetic settlement only: it runs just for the
	// calls actually settled here, and an ordinary completion with no
	// unresolved call never loads the history.
	remaining := append(append([]PendingToolCall{}, currentOp.State.PendingToolCalls...), pending...)
	unknownHookCalls := map[string]bool{}
	if len(remaining) > 0 {
		committedEntries, err := tx.ReadEntries(sessionID, 0)
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		for _, env := range committedEntries {
			if env.Kind != EntryHookResult || env.OperationID != operationID {
				continue
			}
			hook, err := decodeHookResultEntry(env)
			if err != nil {
				return OperationRecord{}, SessionRecord{}, nil, corruptSession(sessionID, "committed hook result %s: %v", env.ID, err)
			}
			if hook.ToolCallID != "" && hook.Evidence == ExecutionUnknown {
				unknownHookCalls[hook.ToolCallID] = true
			}
		}
	}
	for _, call := range remaining {
		evidence := ExecutionNotStarted
		hookUnknown := unknownHookCalls[call.CallID]
		if currentOp.State.ActiveEffect != nil && currentOp.State.ActiveEffect.ToolCallID == call.CallID {
			switch currentOp.State.ActiveEffect.Kind {
			case EffectTool:
				evidence = ExecutionUnknown
			case EffectHook:
				hookUnknown = true
			}
		}
		result := toolResultEntry{
			SessionID:       sessionID,
			EntryID:         call.ResultEntryID,
			OperationID:     operationID,
			AssistantEntry:  call.AssistantEntry,
			ToolCallID:      call.CallID,
			Status:          model.ResultInterrupted,
			Content:         buildToolResultContent(evidence, hookUnknown, interruptedToolResultContent),
			OriginalContent: interruptedToolResultContent, // synthetic settlements are never shaped
			Evidence:        evidence,
		}
		if currentOp.State.ActiveEffect != nil && currentOp.State.ActiveEffect.Kind == EffectTool &&
			currentOp.State.ActiveEffect.ToolCallID == call.CallID {
			result.Implementation = currentOp.State.ActiveEffect.Implementation
			if len(currentOp.State.ActiveEffect.FinalArguments) > 0 {
				result.NormalizedArguments = model.CloneRaw(currentOp.State.ActiveEffect.FinalArguments)
			}
			if len(currentOp.State.ActiveEffect.ContributionIDs) > 0 {
				result.HookIDs = append([]HookIdentity(nil), currentOp.State.ActiveEffect.ContributionIDs...)
			}
		}
		payload, err := encodeToolResultEntry(result)
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
			SessionID:   sessionID,
			ID:          result.EntryID,
			OperationID: operationID,
			Kind:        EntryToolResult,
			Payload:     payload,
		})
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		newEntries = append(newEntries, adopted)
	}

	// The interruption terminal appends exactly one interruption signal.
	if terminal == OperationInterruption {
		entry, err := newSignalEntry(tx, sessionID, operationID, SignalInterruption)
		if err != nil {
			return OperationRecord{}, SessionRecord{}, nil, err
		}
		newEntries = append(newEntries, *entry)
	}

	// The settlement entry consumes the model effect's reserved identity
	// when no assistant payload exists; every other settlement takes a fresh
	// identity. It witnesses the closed request: the published running
	// request when one exists, otherwise a live model intent's or the
	// maintenance producer's identity. A no-output settlement owns the final
	// request evidence exactly once, copied from the committed model intent
	// when one is live or from the retained maintenance carry otherwise.
	settlementID := ""
	activeEffect := currentOp.State.ActiveEffect
	if assistant == nil && activeEffect != nil && activeEffect.Kind == EffectModel {
		settlementID = activeEffect.ResultEntryID
	}
	if settlementID == "" {
		if settlementID, err = newHexID(); err != nil {
			return OperationRecord{}, SessionRecord{}, nil, fmt.Errorf("%w: %v", ErrStorage, err)
		}
	}
	settlement := operationSettlementEntry{
		SessionID:       sessionID,
		EntryID:         settlementID,
		OperationID:     operationID,
		Status:          terminal,
		Detail:          detail,
		CompactionUsage: compactionUsage,
	}
	// Summary settlements name their producer. Other terminals witness the
	// running request or active model intent, if any.
	switch {
	case compactionUsage && facts != nil && (facts.hasInput() || facts.hasConsumed()):
		settlement.RequestID = maintenanceID // a real maintenance request identity, empty when none opened
	case assistant == nil && activeEffect != nil && activeEffect.Kind == EffectModel &&
		activeEffect.Purpose == InvocationPurposeMaintenance:
		settlement.RequestID = activeEffect.RequestID
	case currentOp.State.Request != nil:
		settlement.RequestID = currentOp.State.Request.RequestID
	case assistant == nil && activeEffect != nil && activeEffect.Kind == EffectModel:
		settlement.RequestID = activeEffect.RequestID
	}
	if assistant == nil {
		if activeEffect != nil && activeEffect.Kind == EffectModel && activeEffect.Purpose == InvocationPurposeConversation {
			evidence := *activeEffect.Evidence
			settlement.Evidence = &evidence
		} else if compactionUsage && facts != nil && facts.hasConsumed() && settlement.RequestID != "" {
			consumed := facts.consumed
			settlement.Evidence = &consumed
		}
	}
	if facts.hasInput() {
		// The actual ordered maintenance facts settle on this terminal
		// exactly once beside any ordinary usage; their own reported usage
		// never becomes the settlement's carried conversation usage.
		settlement.Facts = cloneMaintenanceFacts(facts.facts)
	} else if activeEffect != nil && activeEffect.Kind == EffectModel && activeEffect.Purpose == InvocationPurposeMaintenance {
		settlement.Facts = []maintenanceFact{summaryFact(activeEffect.RequestID, *activeEffect.Model, activeEffect.Evidence)}
	}
	if assistant == nil {
		if usage.last != nil || usage.retryUsage.present() { // terminal no-output model attempt usage rides the settlement entry
			modelRef := usageModel
			settlement.Model = &modelRef
			settlement.Usage = usage.last
			settlement.retryUsage = usage.retryUsage
		} else if activeEffect != nil && activeEffect.Kind == EffectModel && activeEffect.Purpose == InvocationPurposeConversation {
			// Summary producers are witnessed by their facts instead.
			modelRef := *activeEffect.Model
			settlement.Model = &modelRef
		}
	}
	payload, err := encodeOperationSettlementEntry(settlement)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
		SessionID:   sessionID,
		ID:          settlement.EntryID,
		OperationID: operationID,
		Kind:        EntryOperationSettlement,
		Payload:     payload,
	})
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	newEntries = append(newEntries, adopted)
	settled := adopted.Envelope.CommittedAt

	next := currentOp
	next.State.ActiveEffect = nil
	next.State.Request = nil
	next.State.Usage = opUsage
	next.State.Status = terminal
	next.State.PendingToolCalls = []PendingToolCall{}
	next.State.SettledAt = &settled
	if facts.hasInput() {
		// This terminal publication records the maintenance facts exactly once and
		// retires the incomplete-maintenance marker; a settlement that records none
		// — a recovery retaining no process-local facts — preserves it on the terminal.
		next.State.UnreportedMaintenance = false
	}
	next.State.Terminal = &OperationTerminal{
		SettlementEntry: EntryRef{SessionID: sessionID, EntryID: settlementID},
		Detail:          detail,
	}
	opPayload, err := encodeOperationRegister(next)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	replaced, err := tx.ReplaceRegister(RegisterKey{SessionID: sessionID, Kind: RegisterOperation, OperationID: operationID}, oregRevision, opPayload)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	next.Revision = replaced.Revision

	sessionState := currentSession.State
	sessionState.Usage = sessionUsage
	sessionState.CurrentOperationID = ""
	committedSess := SessionRecord{Identity: currentSession.Identity, State: sessionState}
	sessionPayload, err := encodeSessionRegister(committedSess)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	replacedSession, err := tx.ReplaceRegister(RegisterKey{SessionID: sessionID, Kind: RegisterSession}, sregRevision, sessionPayload)
	if err != nil {
		return OperationRecord{}, SessionRecord{}, nil, err
	}
	committedSess.Revision = replacedSession.Revision
	return next, committedSess, newEntries, nil
}

// insertAndAdopt inserts one entry draft and returns its codec-cloned view
// entry, so no committed view value aliases the settlement handed upward.
func insertAndAdopt(tx Transaction, sessionID string, draft EntryDraft) (graphEntry, error) {
	inserted, err := tx.InsertEntry(draft)
	if err != nil {
		return graphEntry{}, err
	}
	adopted, err := decodeGraphEntry(sessionID, inserted)
	if err != nil {
		return graphEntry{}, corruptSession(sessionID, "committed entry %s: %v", inserted.ID, err)
	}
	return adopted, nil
}

// newSignalEntry builds and commits one signal entry of the given kind through
// the transaction, returning its codec-cloned view entry.
func newSignalEntry(tx Transaction, sessionID, operationID string, kind SignalKind) (*graphEntry, error) {
	entryID, err := newHexID()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStorage, err)
	}
	entry := signalEntry{
		SessionID:        sessionID,
		EntryID:          entryID,
		OperationID:      operationID,
		Signal:           kind,
		RelatedOperation: &operationRef{SessionID: sessionID, OperationID: operationID},
		Content:          signalContent(kind),
	}
	payload, err := encodeSignalEntry(entry)
	if err != nil {
		return nil, err
	}
	adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
		SessionID:   sessionID,
		ID:          entryID,
		OperationID: operationID,
		Kind:        EntrySignal,
		Payload:     payload,
	})
	if err != nil {
		return nil, err
	}
	return &adopted, nil
}

// normalizeCallArguments runs one bound normalizer invocation at the Harness
// boundary: the callback receives an owned copy of the completed call and must
// return exactly one complete JSON object, whose bytes the Harness returns as a
// fresh owned copy. A callback error or a malformed result is a per-call
// validation failure.
func normalizeCallArguments(ctx context.Context, normalize func(context.Context, model.ToolCall) (json.RawMessage, error), call model.ToolCall) (json.RawMessage, error) {
	owned, err := model.NewToolCall(call)
	if err != nil {
		return nil, err
	}
	raw, err := normalize(ctx, owned)
	if err != nil {
		return nil, err
	}
	if _, err := decodePayloadObject(raw); err != nil {
		return nil, fmt.Errorf("%w: normalization result is not one complete JSON object", errMalformedNormalization)
	}
	return model.CloneRaw(raw), nil
}

// errMalformedNormalization marks a normalizer result whose shape is not
// one complete JSON object: the existing internal-validation tool error
// class, not a plugin-defined status or a cancellation instruction.
var errMalformedNormalization = errors.New("malformed normalization")

// usageCount converts one model usage report into its durable count.
func usageCount(u *model.Usage) *UsageCount {
	if u == nil {
		return nil
	}
	return &UsageCount{
		InputTokens:       int64(u.InputTokens),
		CachedInputTokens: int64(u.CachedInputTokens),
		OutputTokens:      int64(u.OutputTokens),
	}
}

// usageContribution renders one record's last accepted and retried usage under its model.
func usageContribution(ref model.ModelRef, last *UsageCount, retry retryUsage) (UsageTotals, error) {
	var contribution UsageTotals
	for _, counts := range []*UsageCount{last, retry.RetryUsage} {
		if counts == nil {
			continue
		}
		var err error
		contribution, err = addUsageTotals(contribution, UsageTotals{ByModel: []ModelUsage{{Model: ref, Usage: *counts}}})
		if err != nil {
			return UsageTotals{}, err
		}
	}
	return contribution, nil
}

// newAssistantEntry builds one assistant entry from one validated model
// output under the given entry identity, reserving one result identity per
// completed call and recording the producing request identity and the request
// evidence the model effect consumed. An output without an eligible model-visible
// payload writes no assistant entry and carries the accepted attempts' usage
// otherwise. Call arguments remain raw.
func newAssistantEntry(sessionID, operationID, entryID string, out *model.Output, usage attemptUsage, requestID string, evidence *RequestEvidence) (*assistantEntry, error) {
	if out == nil {
		return nil, nil
	}
	entry := &assistantEntry{
		SessionID:   sessionID,
		EntryID:     entryID,
		OperationID: operationID,
		Status:      out.Status,
		Source:      out.Source,
		RequestID:   requestID,
	}
	if evidence != nil {
		owned := copyRequestEvidence(*evidence)
		entry.Evidence = &owned
	}
	if out.Message != nil {
		entry.Content = out.Message.Content
		entry.Refusal = out.Message.Refusal
		entry.Extra = out.Message.Extra
		for i, call := range out.Message.ToolCalls {
			record := toolCallRecord{
				ID:              call.ID,
				Ordinal:         int64(i),
				Name:            call.Name,
				ArgumentsBase64: base64.StdEncoding.EncodeToString(call.Arguments),
				Extra:           call.Extra,
			}
			entry.ToolCalls = append(entry.ToolCalls, record)
		}
	}
	for i := range entry.ToolCalls {
		id, err := newHexID()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrStorage, err)
		}
		entry.ToolCalls[i].ResultEntryID = id
	}
	if !assistantPayloadEligible(*entry) {
		return nil, nil
	}
	entry.Usage = usage.last
	entry.retryUsage = usage.retryUsage
	return entry, nil
}

// copyRequestEvidence returns an independent deep-owned copy of one evidence
// value: canonical references, the prompt, every recorded message's
// nested parts and extras, and the name and provenance lists.
func copyRequestEvidence(v RequestEvidence) RequestEvidence {
	out := v
	out.Edits = cloneRequestEdits(v.Edits)
	if v.Tools != nil {
		out.Tools = append([]model.ToolDefinition{}, cloneToolDefinitions(v.Tools)...)
	}
	out.Parameters = v.Parameters.Clone()
	if v.SummaryBase != nil {
		out.SummaryBase = append([]model.Message{}, cloneRequestMessages(v.SummaryBase)...)
	}
	if v.SummaryInput != nil {
		out.SummaryInput = append([]model.Message{}, cloneRequestMessages(v.SummaryInput)...)
	}
	if v.HookIDs != nil {
		out.HookIDs = append([]HookIdentity(nil), v.HookIDs...)
	}
	return out
}

// copyRequestEvidencePointer deep-copies an evidence value behind its pointer.
func copyRequestEvidencePointer(evidence *RequestEvidence) *RequestEvidence {
	if evidence == nil {
		return nil
	}
	owned := copyRequestEvidence(*evidence)
	return &owned
}

// ownToolOutcome returns one owned tool outcome: its mutable metadata bytes
// are copied verbatim, preserving malformed or opaque values for their real
// validator rather than normalizing them.
func ownToolOutcome(out ToolOutcome) ToolOutcome {
	out.Metadata = model.CloneRaw(out.Metadata)
	return out
}

// ownPreparedTool freezes one acquired plan's mutable data at acceptance: the
// permission slice and the immediate outcome's value/metadata are independent
// copies, while the original Execute function value is kept.
func ownPreparedTool(plan PreparedTool) PreparedTool {
	if plan.Permissions != nil {
		plan.Permissions = append([]PermissionRequest(nil), plan.Permissions...)
	}
	if plan.Immediate != nil {
		immediate := ownToolOutcome(*plan.Immediate)
		plan.Immediate = &immediate
	}
	return plan
}

// validToolPlan reports whether one prepared plan has the contract shape:
// exactly one immediate result or executor.
func validToolPlan(p PreparedTool) bool {
	return (p.Immediate == nil) != (p.Execute == nil)
}

// authorizationRequired reports whether one shape-valid plan may produce an
// effect and therefore requires successfully evaluated declarations: an
// executor-backed plan or an immediate success. Immediate error, denied and
// interrupted outcomes need no target declaration.
func authorizationRequired(p PreparedTool) bool {
	if p.Execute != nil {
		return true
	}
	return p.Immediate.Result.Status == model.ResultSuccess
}

// toolCallAllowed decides one prepared plan at the Harness permission
// boundary: every declared pair must be a nonempty permission/target pair;
// file.read/file.write targets must be canonical and their prepared canonical
// Workspace root must be a required canonical that is nonempty, absolute and
// lexically clean. Target preparation is never delegated. Only then does the
// evaluator decide the complete prepared
// action, and the plan is allowed exactly when it returns allow without
// error: a deny, a decision outside the closed set, or an error denies the
// call alike. A nil evaluator selects the included built-in policy.
func toolCallAllowed(ctx context.Context, evaluator PermissionEvaluator, plan PreparedTool, name string, args json.RawMessage) bool {
	if len(plan.Permissions) == 0 {
		return false
	}
	var hasFile bool
	for _, req := range plan.Permissions {
		if req.Permission == "" || req.Target == "" {
			return false
		}
		switch req.Permission {
		case PermissionFileRead, PermissionFileWrite:
			hasFile = true
		}
		if (req.Permission == PermissionFileRead || req.Permission == PermissionFileWrite) && !canonicalPathValue(req.Target) {
			return false
		}
	}
	if hasFile && !canonicalPathValue(plan.CanonicalWorkspace) {
		return false
	}
	if evaluator == nil {
		evaluator = PermissionPolicy{}
	}
	decision, err := evaluator.Evaluate(ctx, PreparedAction{
		ToolName:           name,
		Arguments:          append(json.RawMessage(nil), args...),
		CanonicalWorkspace: plan.CanonicalWorkspace,
		Permissions:        append([]PermissionRequest(nil), plan.Permissions...),
	})
	return err == nil && decision == PermissionAllow
}

func authorizationFailure(ctx context.Context, callID string, err error) model.ToolResult {
	var declared *EffectError
	if EffectInterrupted(err) || (!errors.As(err, &declared) && ctx.Err() != nil && isExecutionCancellation(err)) {
		return interruptedToolResult(callID)
	}
	return validationToolResult(callID, err)
}

// authorizeHooks runs pure restrictions after policy, retaining only locations
// explicitly bound by the hooks, not the Workspace fact itself.
func authorizeHooks(ctx context.Context, inv InvocationExecution, callID, name string, args json.RawMessage, plan PreparedTool) (func() error, *model.ToolResult) {
	if len(inv.AuthorizationHooks) == 0 {
		return nil, nil
	}
	fail := func(err error) (func() error, *model.ToolResult) {
		result := authorizationFailure(ctx, callID, err)
		return nil, &result
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	if inv.BindLocation == nil {
		return fail(fmt.Errorf("location binding unavailable"))
	}
	root, rootCheck, err := inv.BindLocation(".")
	if err != nil {
		return fail(err)
	}
	if !canonicalPathValue(root) || rootCheck == nil {
		return fail(fmt.Errorf("invalid location binding"))
	}
	var checks []func() error
	for _, hook := range inv.AuthorizationHooks {
		if ctx.Err() != nil {
			return fail(ctx.Err())
		}
		var mu sync.Mutex
		open := true
		bind := func(path string) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			if !open {
				return "", fmt.Errorf("authorization callback has ended")
			}
			canonical, check, err := inv.BindLocation(path)
			if err != nil {
				return "", err
			}
			if !canonicalPathValue(canonical) || check == nil {
				return "", fmt.Errorf("invalid location binding")
			}
			checks = append(checks, check)
			return canonical, nil
		}
		action := PreparedAction{ToolName: name, Arguments: append(json.RawMessage(nil), args...), CanonicalWorkspace: root, Permissions: append([]PermissionRequest(nil), plan.Permissions...)}
		reason, err := hook.Run(ctx, action, bind)
		mu.Lock()
		open = false
		mu.Unlock()
		if err != nil {
			return fail(err)
		}
		if reason != nil {
			if *reason == "" {
				result := invalidToolResult(callID)
				return nil, &result
			}
			content := refusalDiagnostic(*reason)
			result := model.ToolResult{CallID: callID, Status: model.ResultDenied, Content: content}
			return nil, &result
		}
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	if len(checks) == 0 {
		return nil, nil
	}
	frozen := append([]func() error(nil), checks...)
	return func() error {
		for _, check := range frozen {
			if err := check(); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

// canonicalPathValue reports whether one prepared canonical target or root is
// a nonempty, absolute, lexically clean path.
func canonicalPathValue(p string) bool {
	return p != "" && filepath.IsAbs(p) && filepath.Clean(p) == p
}

// invalidToolResult is the ordinary validation-error result an invalid plan or
// invalid returned outcome maps onto for the original call.
func invalidToolResult(callID string) model.ToolResult {
	return model.ToolResult{CallID: callID, Status: model.ResultError, Content: invalidToolResultContent}
}

// interruptedToolResult is the ordinary interrupted-before-execution result.
func interruptedToolResult(callID string) model.ToolResult {
	return model.ToolResult{CallID: callID, Status: model.ResultInterrupted, Content: interruptedToolResultContent}
}

// pendingToolCall is one published call's correlated private state: the
// pending reservation and its committed assistant record. They are resolved
// together so callers never pair independently derived identities.
type pendingToolCall struct {
	pending PendingToolCall
	record  toolCallRecord
}

// pendingToolCallRecord resolves one published call's pending reservation and
// its committed assistant tool-call record from the coordinator's validated
// view: the boundary consumes the durable record, never call data from model
// JSON.
func pendingToolCallRecord(c *coordinator, operationID, callID string) (pendingToolCall, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	op, ok := c.graph.Operation(operationID)
	if !ok {
		return pendingToolCall{}, fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, c.graph.Session.Identity.SessionID)
	}
	var pending PendingToolCall
	found := false
	for _, candidate := range op.State.PendingToolCalls {
		if candidate.CallID == callID {
			pending, found = candidate, true
			break
		}
	}
	if !found {
		return pendingToolCall{}, invalidInput("operation %q has no pending call %q; every call settles exactly once", operationID, callID)
	}
	for _, entry := range c.graph.Entries {
		if entry.Envelope.ID != pending.AssistantEntry.EntryID || entry.Assistant == nil {
			continue
		}
		for _, record := range entry.Assistant.ToolCalls {
			if record.ID == callID {
				return pendingToolCall{pending: pending, record: record}, nil
			}
		}
	}
	return pendingToolCall{}, invalidInput("operation %q pending call %q has no committed assistant record", operationID, callID)
}

// unavailableToolResult is the ordinary unavailable-tool error result the
// advertisement gate settles for a name outside the producing request's
// advertised set.
func unavailableToolResult(callID, name string) model.ToolResult {
	return model.ToolResult{CallID: callID, Status: model.ResultError, Content: fmt.Sprintf("Tool %q is not available.", name)}
}

// permissionDeniedToolResult is the ordinary policy-denial result settled for
// the original call under its reserved identity: no effect began and no tool
// active intent was written.
func permissionDeniedToolResult(callID string) model.ToolResult {
	return model.ToolResult{CallID: callID, Status: model.ResultDenied, Content: PermissionDeniedContent}
}

// refusedToolResult is the deliberate before-tool refusal result: the settled
// call is denied with the refusal produced by the same denied-content rule as
// an authorization-hook denial, and no effect began.
func refusedToolResult(callID, reason string) model.ToolResult {
	return model.ToolResult{CallID: callID, Status: model.ResultDenied, Content: refusalDiagnostic(reason)}
}

// refusalDiagnostic renders one before-tool refusal as the denied content an
// authorization-hook denial produces: invalid UTF-8 is repaired and the text
// is bounded to the durable diagnostic limit. An empty reason is invalid and
// is rejected before this producer.
func refusalDiagnostic(reason string) string {
	return boundedToolDiagnostic(errors.New(strings.ToValidUTF8(reason, "\uFFFD")))
}

// validationToolResult is the immediate validation-error result for an invalid
// model argument: status error carrying the callback's own bounded useful
// diagnostic. An empty diagnostic still carries the internal-validation text.
func validationToolResult(callID string, cause error) model.ToolResult {
	content := boundedToolDiagnostic(cause)
	if content == "" {
		content = invalidToolResultContent
	}
	return model.ToolResult{CallID: callID, Status: model.ResultError, Content: content}
}

// boundedToolDiagnostic renders one Harness-produced validation diagnostic as
// valid UTF-8 within the durable diagnostic bound: invalid bytes are removed
// at any length, and over-long text truncates within the same sanitizing
// pass, so no diagnostic can make its owning entry unencodable.
func boundedToolDiagnostic(cause error) string {
	msg := cause.Error()
	if len(msg) > maxToolDiagnosticBytes {
		msg = msg[:maxToolDiagnosticBytes]
	}
	return strings.ToValidUTF8(msg, "")
}

// toolEffect gates the advertised call, runs argument hooks over raw bytes,
// binds and normalizes one implementation, and prepares and authorizes its
// plan. The executor intent freezes the binding and effective arguments before
// I/O; settlement preserves them and the execution evidence.
func (d *executionDriver) toolEffect() agent.ToolEffect {
	h := d.harness
	c := d.coordinator
	inv := d.conversation
	return func(ctx context.Context, call model.ToolCall, names []string) (model.ToolResult, error) {
		resolved, err := pendingToolCallRecord(c, d.operationID, call.ID)
		if err != nil {
			return model.ToolResult{}, err
		}
		pending, record := resolved.pending, resolved.record
		// The live dispatcher entry: one started fact after the durable pending
		// call resolved, before any validation, hook, permission or execution.
		// A dispatched denial, error, immediate or interrupted call shares it.
		h.emitFact(HarnessFact{
			Kind:        FactToolStarted,
			SessionID:   pending.AssistantEntry.SessionID,
			OperationID: d.operationID,
			CallID:      record.ID,
			Ordinal:     record.Ordinal,
			Name:        record.Name,
		})
		settleCtx := context.WithoutCancel(h.ctx)
		if ctx.Err() != nil { // execution cancellation prevents later preparation: interrupted-before-execution
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: interruptedToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
		}
		advertised := make(map[string]bool, len(names))
		for _, name := range names {
			advertised[name] = true
		}
		if !advertised[record.Name] { // the one advertisement gate, ahead of every hook and preparer: the engine-supplied per-call advertisement of the producing request is the authority
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: unavailableToolResult(call.ID, record.Name), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
		}
		raw, err := base64.StdEncoding.Strict().DecodeString(record.ArgumentsBase64)
		if err != nil {
			return model.ToolResult{}, err
		}
		selection := toolSelection{arguments: raw}
		hooks := inv.callbacks.ToolHooks
		if inv.callbacks.ArgumentStage != nil {
			stage := inv.callbacks.ArgumentStage()
			if stage.Release != nil {
				defer stage.Release()
			}
			if stage.Error != nil {
				return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: validationToolResult(call.ID, stage.Error), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
			}
			check := inv.callbacks
			check.ToolHooks = stage.Hooks
			if err := validateOpenedInvocation(check); err != nil {
				return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: validationToolResult(call.ID, err), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
			}
			hooks = stage.Hooks
		}
		if len(hooks) > 0 {
			chain, settled, herr := d.runArgumentHooks(ctx, settleCtx, resolved, hooks, raw)
			if herr != nil {
				return model.ToolResult{}, herr
			}
			if settled != nil { // the hook chain settled the call itself; remaining hooks are skipped and later calls continue
				return *settled, nil
			}
			selection = chain
		}
		if ctx.Err() != nil { // finish the accepted chain, but start no subsequent preparation
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: interruptedToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
		}
		binding, ok := inv.callbacks.LookupTool(record.Name)
		if !ok {
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: unavailableToolResult(call.ID, record.Name), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
		}
		if binding.Release != nil {
			defer binding.Release()
		}
		if binding.Identity.ID == "" || binding.Normalize == nil || binding.Prepare == nil {
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: invalidToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
		}
		selection.implementation = &binding.Identity
		prepareCtx := WithCallbackIdentity(ctx, CallbackIdentity{RequestID: d.conversation.request.ID, ToolCallID: call.ID})
		normalized, nerr := normalizeCallArguments(prepareCtx, binding.Normalize, model.ToolCall{ID: record.ID, Name: record.Name, Arguments: selection.arguments, Extra: record.Extra})
		if nerr != nil {
			selection.arguments, selection.hooks = nil, nil
			var result model.ToolResult
			if EffectInterrupted(nerr) || (ctx.Err() != nil && isExecutionCancellation(nerr)) {
				result = interruptedToolResult(call.ID)
			} else {
				result = model.ToolResult{CallID: call.ID, Status: model.ResultError, Content: normalizationFailureContent(nerr)}
			}
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: result, Evidence: ExecutionNotStarted}, nil, false, selection, false)
		}
		selection.arguments = normalized
		if ctx.Err() != nil {
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: interruptedToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, selection, false)
		}
		// The preparer receives the bound callback context carrying its actual
		// producing request and pending call, and an owned copy of the selected
		// normalized call — nothing else.
		normalizedCall, err := model.NewToolCall(model.ToolCall{ID: record.ID, Name: record.Name, Arguments: selection.arguments, Extra: record.Extra})
		if err != nil {
			return h.commitToolResult(settleCtx, c, d.operationID, pending, ToolOutcome{Result: invalidToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, selection, false)
		}
		plan := ownPreparedTool(binding.Prepare(prepareCtx, normalizedCall))
		settle := func(outcome ToolOutcome, shaping *toolShaping, fromIntent bool, selection toolSelection, hookUnknown bool) (model.ToolResult, error) {
			return h.commitToolResult(settleCtx, c, d.operationID, pending, outcome, shaping, fromIntent, selection, hookUnknown)
		}
		if !validToolPlan(plan) {
			return settle(ToolOutcome{Result: invalidToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, selection, false)
		}
		var recheck func() error
		if authorizationRequired(plan) {
			if !toolCallAllowed(ctx, d.execution.Permissions, plan, record.Name, selection.arguments) {
				return settle(ToolOutcome{Result: permissionDeniedToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, selection, false)
			}
			callbacks := inv.callbacks
			if callbacks.AuthorizationStage != nil {
				stage := callbacks.AuthorizationStage()
				if stage.Release != nil {
					defer stage.Release()
				}
				if stage.Error != nil {
					return settle(ToolOutcome{Result: authorizationFailure(ctx, call.ID, stage.Error), Evidence: ExecutionNotStarted}, nil, false, selection, false)
				}
				callbacks.AuthorizationHooks = stage.Hooks
				if err := validateOpenedInvocation(callbacks); err != nil {
					return settle(ToolOutcome{Result: authorizationFailure(ctx, call.ID, err), Evidence: ExecutionNotStarted}, nil, false, selection, false)
				}
			}
			var result *model.ToolResult
			recheck, result = authorizeHooks(prepareCtx, callbacks, call.ID, record.Name, selection.arguments, plan)
			if result != nil {
				return settle(ToolOutcome{Result: *result, Evidence: ExecutionNotStarted}, nil, false, selection, false)
			}
		}
		if plan.Immediate != nil {
			if recheck != nil {
				if err := recheck(); err != nil {
					return settle(ToolOutcome{Result: authorizationFailure(ctx, call.ID, err), Evidence: ExecutionNotStarted}, nil, false, selection, false)
				}
			}
			outcome := acceptedImmediateOutcome(call.ID, *plan.Immediate)
			// Pure output shaping over the tool-produced outcome (the
			// invalid-replacement content included), before the final commit.
			shaping := d.shapeToolOutput(ctx, inv, outcome, call.ID, "")
			return settle(outcome, shaping, false, selection, false)
		}
		if _, err := h.beginPendingCallEffect(ctx, c, d.operationID, call.ID, EffectTool, pendingCallIntent{
			finalArguments:  selection.arguments,
			contributionIDs: selection.hooks,
			implementation:  selection.implementation,
		}); err != nil {
			// A marked publication failure propagates before any cancellation
			// rule, regardless of a concurrent cancellation. Only an unmarked
			// cancellation observed before the intent transaction was ever
			// attempted settles the cancellation outcome: interrupted-before-
			// execution, never a cancellation-shaped error out of an active effect.
			if !isPublicationFailure(err) && isExecutionCancellation(err) && ctx.Err() != nil {
				return settle(ToolOutcome{Result: interruptedToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, false, selection, false)
			}
			return model.ToolResult{}, err
		}
		if ctx.Err() != nil { // cancellation before start commits interrupted-before-execution carrying the intent's accepted selection
			return settle(ToolOutcome{Result: interruptedToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, true, toolSelection{}, false)
		}
		if recheck != nil {
			if err := recheck(); err != nil {
				return settle(ToolOutcome{Result: authorizationFailure(ctx, call.ID, err), Evidence: ExecutionNotStarted}, nil, true, toolSelection{}, false)
			}
			if ctx.Err() != nil {
				return settle(ToolOutcome{Result: interruptedToolResult(call.ID), Evidence: ExecutionNotStarted}, nil, true, toolSelection{}, false)
			}
		}
		// Execute receives the committed concrete-tool reservation's context;
		// this is the only callback with action authority, and the reserved
		// result identity is carried immutably for the native adapter.
		executeCtx := WithCallbackIdentity(ctx, CallbackIdentity{RequestID: d.conversation.request.ID, ToolCallID: call.ID, EffectID: pending.ResultEntryID})
		if recheck != nil {
			executeCtx = context.WithValue(executeCtx, locationChecksKey{}, recheck)
		}
		outcome := ownToolOutcome(plan.Execute(executeCtx))
		if outcome.Evidence == "" { // a valid returned result is itself the observation
			outcome.Evidence = ExecutionObserved
		}
		if _, err := model.NewToolResult(outcome.Result); err != nil || outcome.Result.CallID != call.ID ||
			!validExecutionEvidence(outcome.Evidence) ||
			(outcome.Result.Status == model.ResultSuccess && outcome.Evidence != ExecutionObserved) {
			// The executor ran — the action may have occurred — but declared
			// no valid terminal response or an impossible success knowledge:
			// conservative unknown.
			outcome = ToolOutcome{Result: invalidToolResult(call.ID), Evidence: ExecutionUnknown}
		}
		// Pure output shaping over the tool-produced outcome (the
		// invalid-replacement content included), before the final commit.
		shaping := d.shapeToolOutput(ctx, inv, outcome, call.ID, pending.ResultEntryID)
		// the real outcome wins a cancellation race: the settlement transaction
		// runs without cancellation and publishes what the execution produced.
		return settle(outcome, shaping, true, toolSelection{}, false)
	}
}

// acceptedImmediateOutcome validates one immediate plan's result shape and
// assigns its evidence: an immediate success is observed, every other
// immediate status is not_started, and any supplied value is ignored. A
// malformed outcome settles as the ordinary internal-validation result with
// proven-unstarted knowledge.
func acceptedImmediateOutcome(callID string, outcome ToolOutcome) ToolOutcome {
	evidence := ExecutionNotStarted
	if outcome.Result.Status == model.ResultSuccess {
		evidence = ExecutionObserved
	}
	if _, err := model.NewToolResult(outcome.Result); err != nil || outcome.Result.CallID != callID {
		return ToolOutcome{Result: invalidToolResult(callID), Evidence: ExecutionNotStarted}
	}
	outcome.Evidence = evidence
	return outcome
}

// beginPendingCallEffect commits the one active-effect intent of a
// pending-call effect: the intent addresses the first pending call and
// reserves its result identity — the call's reserved identity for a tool
// effect, a fresh allocation for a hook — recording the effect kind, bound
// tool identity, selection and provenance for a tool effect, or the hook
// identity, extension identity, and consumed input bytes before I/O for a hook
// effect. Effects never nest, so any committed intent of another effect rejects.
// The intent owner follows beginModelEffect's origin rule: a cancellation
// observed before the intent transaction is ever invoked returns unwrapped
// and may use the caller's ordinary interruption settlement. Once the
// transaction is invoked it runs on a cancellation-free copy of the execution
// context — values only, like the result and terminal publications — so every
// error from it is a real store failure and a publication-origin failure at
// the intent boundary: it propagates marked with the durable state retained
// for recovery and is never followed by a guessed compensating write.
func (h *Harness) beginPendingCallEffect(ctx context.Context, c *coordinator, operationID, callID string, kind EffectKind, input pendingCallIntent) (string, error) {
	c.mu.Lock()
	op, ok := c.graph.Operation(operationID)
	if !ok {
		sessionID := c.graph.Session.Identity.SessionID
		c.mu.Unlock()
		return "", wrapPublication(fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, sessionID))
	}
	if op.State.Status != OperationRunning {
		c.mu.Unlock()
		return "", wrapPublication(invalidInput("operation %q is %s; a %s effect requires a running Operation", operationID, op.State.Status, kind))
	}
	if op.State.ActiveEffect != nil {
		c.mu.Unlock()
		return "", wrapPublication(invalidInput("operation %q already carries an active effect; effects never nest", operationID))
	}
	if len(op.State.PendingToolCalls) == 0 || op.State.PendingToolCalls[0].CallID != callID {
		c.mu.Unlock()
		return "", wrapPublication(invalidInput("operation %q does not pending-start with call %q; a %s effect addresses the first pending call", operationID, callID, kind))
	}
	resultID := op.State.PendingToolCalls[0].ResultEntryID
	if kind == EffectHook {
		fresh, err := newHexID()
		if err != nil {
			c.mu.Unlock()
			return "", wrapPublication(fmt.Errorf("%w: %v", ErrStorage, err))
		}
		resultID = fresh
	}
	if ctx.Err() != nil { // cancellation observed before the Transact is ever invoked is not a
		// publication failure: it returns unwrapped and the caller's ordinary interruption settlement applies.
		c.mu.Unlock()
		return "", ctx.Err()
	}
	sessionID := op.Admission.SessionID
	var updated OperationRecord
	// Once the pre-Transact check admits the transition, the durable intent
	// transaction runs without execution cancellation or deadline — values
	// only, exactly like the result and terminal publications — so a shutdown
	// that lands mid-transaction never aborts the admitted intent. The
	// caller's execution-context gate settles the interruption afterwards.
	err := h.deps.Storage.Transact(context.WithoutCancel(ctx), func(tx Transaction) error {
		key := RegisterKey{SessionID: sessionID, Kind: RegisterOperation, OperationID: operationID}
		reg, err := tx.ReadRegister(key)
		if err != nil {
			return err
		}
		current, err := decodeOperationRegister(reg)
		if err != nil {
			return corruptSession(sessionID, "operation register %q: %v", operationID, err)
		}
		// a violated semantic precondition outranks the conflict class
		if current.State.Status != OperationRunning {
			return invalidInput("operation %q is %s; a %s effect requires a running Operation", operationID, current.State.Status, kind)
		}
		if current.State.ActiveEffect != nil {
			return invalidInput("operation %q already carries an active effect; effects never nest", operationID)
		}
		if len(current.State.PendingToolCalls) == 0 || current.State.PendingToolCalls[0].CallID != callID {
			return invalidInput("operation %q does not pending-start with call %q; a %s effect addresses the first pending call", operationID, callID, kind)
		}
		if reg.Revision != op.Revision {
			return fmt.Errorf("%w: operation %q revision %d changed concurrently to %d", errRevisionRace, operationID, op.Revision, reg.Revision)
		}
		next := current
		next.State.ActiveEffect = &ActiveEffect{
			Kind:                 kind,
			ResultEntryID:        resultID,
			ToolCallID:           callID,
			HookID:               input.hookID,
			ExtensionID:          input.extensionID,
			InputArgumentsBase64: input.inputArgumentsBase64,
			FinalArguments:       input.finalArguments,
			ContributionIDs:      input.contributionIDs,
			Implementation:       input.implementation,
		}
		payload, err := encodeOperationRegister(next)
		if err != nil {
			return err
		}
		replaced, err := tx.ReplaceRegister(key, reg.Revision, payload)
		if err != nil {
			return err
		}
		adopted, err := decodeOperationRegister(replaced)
		if err != nil {
			return corruptSession(sessionID, "committed operation register %q: %v", operationID, err)
		}
		updated = adopted
		return nil
	})
	if err != nil {
		h.markCorrupt(sessionID, err)
		c.mu.Unlock()
		if errors.Is(err, errRevisionRace) { // a foreign writer changed the durable state under the cached view
			if rerr := h.rematerialize(ctx, c, sessionID); rerr != nil { // a discovered corruption or storage failure is the current truth
				return "", wrapPublication(rerr)
			}
		}
		return "", wrapPublication(err)
	}
	c.graph.replaceOperation(operationID, updated)
	c.mu.Unlock()
	return resultID, nil
}

// beginRequestHookEffect commits the one active-effect intent of a
// request-bound hook effect: it reserves a fresh result identity, records the
// hook identity, extension identity, and the consumed effective-view evidence
// before any callback runs, and publishes the one suspended request state
// inside this same existing effect transition when none is published yet —
// the request state is never written by a pure-only transaction.
// The intent owner follows beginModelEffect's origin rule: a cancellation
// observed before the intent transaction is ever invoked returns unwrapped;
// once the transaction is invoked it runs on a cancellation-free copy of the
// execution context, so every error from it is a real store failure marked
// publication-origin and propagates with the durable state retained for
// recovery.
func (h *Harness) beginRequestHookEffect(ctx context.Context, c *coordinator, operationID, requestID string, purpose InvocationPurpose, hookID, extensionID string, consumed RequestEvidence) (string, error) {
	c.mu.Lock()
	op, ok := c.graph.Operation(operationID)
	if !ok {
		sessionID := c.graph.Session.Identity.SessionID
		c.mu.Unlock()
		return "", wrapPublication(fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, sessionID))
	}
	if op.State.Status != OperationRunning {
		c.mu.Unlock()
		return "", wrapPublication(invalidInput("operation %q is %s; a request hook effect requires a running Operation", operationID, op.State.Status))
	}
	if op.State.ActiveEffect != nil {
		c.mu.Unlock()
		return "", wrapPublication(invalidInput("operation %q already carries an active effect; effects never nest", operationID))
	}
	if ctx.Err() != nil { // cancellation observed before the Transact is ever invoked is not a
		// publication failure: it returns unwrapped and the caller's ordinary interruption settlement applies.
		c.mu.Unlock()
		return "", errExecutionInterrupted
	}
	resultID, err := newHexID()
	if err != nil {
		c.mu.Unlock()
		return "", wrapPublication(fmt.Errorf("%w: %v", ErrStorage, err))
	}
	sessionID := op.Admission.SessionID
	consumedCopy := consumed
	var updated OperationRecord
	err = h.deps.Storage.Transact(context.WithoutCancel(ctx), func(tx Transaction) error {
		key := RegisterKey{SessionID: sessionID, Kind: RegisterOperation, OperationID: operationID}
		reg, err := tx.ReadRegister(key)
		if err != nil {
			return err
		}
		current, err := decodeOperationRegister(reg)
		if err != nil {
			return corruptSession(sessionID, "operation register %q: %v", operationID, err)
		}
		if current.State.Status != OperationRunning {
			return invalidInput("operation %q is %s; a request hook effect requires a running Operation", operationID, current.State.Status)
		}
		if current.State.ActiveEffect != nil {
			return invalidInput("operation %q already carries an active effect; effects never nest", operationID)
		}
		if reg.Revision != op.Revision {
			return fmt.Errorf("%w: operation %q revision %d changed concurrently to %d", errRevisionRace, operationID, op.Revision, reg.Revision)
		}
		next := current
		next.State.ActiveEffect = &ActiveEffect{
			Kind:          EffectHook,
			ResultEntryID: resultID,
			RequestID:     requestID,
			Purpose:       purpose,
			HookID:        hookID,
			ExtensionID:   extensionID,
			Evidence:      &consumedCopy,
		}
		if next.State.Request == nil {
			next.State.Request = &RunningRequest{
				RequestID:         requestID,
				Purpose:           purpose,
				CheckpointEntryID: consumed.CheckpointEntryID,
				BoundaryEntryID:   consumed.BoundaryEntryID,
			}
		}
		payload, err := encodeOperationRegister(next)
		if err != nil {
			return err
		}
		replaced, err := tx.ReplaceRegister(key, reg.Revision, payload)
		if err != nil {
			return err
		}
		adopted, err := decodeOperationRegister(replaced)
		if err != nil {
			return corruptSession(sessionID, "committed operation register %q: %v", operationID, err)
		}
		updated = adopted
		return nil
	})
	if err != nil {
		h.markCorrupt(sessionID, err)
		c.mu.Unlock()
		if errors.Is(err, errRevisionRace) {
			if rerr := h.rematerialize(ctx, c, sessionID); rerr != nil {
				return "", wrapPublication(rerr)
			}
		}
		return "", wrapPublication(err)
	}
	c.graph.replaceOperation(operationID, updated)
	c.mu.Unlock()
	return resultID, nil
}

// toolShaping is one settled call's pure output-shaping product: the shaped
// base text the shared result producer renders, and the bounded processing
// failure of an aborted chain. A nil value means no shaping ran over the
// outcome and the producer's original content is the base.
type toolShaping struct {
	base    string
	failure string
}

// shapeToolOutput runs one settled call's ordered pure output-shaping chain
// over the producer's original content: every hook receives its own owned
// outcome copy and the current content text — the original content for the
// first hook, each preceding hook's return after it. The first hook error or
// return failing the unchanged result-shape rule aborts the whole chain: the
// base becomes the original content, the real outcome preserved with partial
// shaping abandoned, and one bounded processing failure names the cause.
// Shaping is text only and never changes status, evidence, usage or metadata.
func (d *executionDriver) shapeToolOutput(ctx context.Context, inv *openedInvocation, outcome ToolOutcome, callID, effectID string) *toolShaping {
	hooks := inv.callbacks.ToolOutputHooks
	if inv.callbacks.ResultStage != nil {
		stage := inv.callbacks.ResultStage()
		if stage.Release != nil {
			defer stage.Release()
		}
		if stage.Error != nil {
			return &toolShaping{base: outcome.Result.Content, failure: shapingFailureDiagnostic(stage.Error)}
		}
		check := inv.callbacks
		check.ToolOutputHooks = stage.Hooks
		if err := validateOpenedInvocation(check); err != nil {
			return &toolShaping{base: outcome.Result.Content, failure: shapingFailureDiagnostic(err)}
		}
		hooks = stage.Hooks
	}
	if len(hooks) == 0 {
		return nil
	}
	base := outcome.Result.Content
	shapeCtx := WithCallbackIdentity(ctx, CallbackIdentity{RequestID: d.conversation.request.ID, ToolCallID: callID, EffectID: effectID})
	for _, hook := range hooks {
		shaped, err := hook.Run(shapeCtx, ToolOutputInput{
			Outcome: ToolOutcome{Result: outcome.Result, Metadata: model.CloneRaw(outcome.Metadata), Evidence: outcome.Evidence},
			Content: base,
		})
		if err != nil {
			return &toolShaping{base: outcome.Result.Content, failure: shapingFailureDiagnostic(err)}
		}
		if _, verr := model.NewToolResult(model.ToolResult{CallID: callID, Status: outcome.Result.Status, Content: shaped}); verr != nil {
			return &toolShaping{base: outcome.Result.Content, failure: shapingFailureDiagnostic(fmt.Errorf("shaped output violates the unchanged status content rule: %w", verr))}
		}
		base = shaped
	}
	return &toolShaping{base: base}
}

// commitToolResult commits the one terminal result of a settled call through
// the ordinary tool-result transition: the result entry under the call's
// reserved identity, the cleared active effect, and the complete next
// Operation state in one transaction. The producer-assigned execution
// knowledge and hook-uncertainty flag decide the final model-facing content
// once through the shared result producer — over the shaped base when a pure
// output-shaping chain ran, the outcome's original content otherwise — and
// are stored alongside that original content and an aborted chain's bounded
// processing failure; the settled result returned to the caller carries that
// same final content and unchanged status. The outcome's metadata is
// committed only when it is one well-formed JSON value within the durable
// bound; invalid or oversized metadata is dropped while the result still
// commits. Immediate plans commit without an effect intent; executor-backed
// plans commit behind theirs and copy the intent's committed final arguments
// and hook provenance. The suspended running request state clears when this
// was the last pending call's result.
func (h *Harness) commitToolResult(ctx context.Context, c *coordinator, operationID string, pending PendingToolCall, outcome ToolOutcome, shaping *toolShaping, fromIntent bool, selection toolSelection, hookUnknown bool) (model.ToolResult, error) {
	base := outcome.Result.Content
	var failure string
	if shaping != nil {
		base, failure = shaping.base, shaping.failure
	}
	evidence := outcome.Evidence
	content := buildToolResultContent(evidence, hookUnknown, base)
	c.mu.Lock()
	op, ok := c.graph.Operation(operationID)
	if !ok {
		sessionID := c.graph.Session.Identity.SessionID
		c.mu.Unlock()
		return model.ToolResult{}, wrapPublication(fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, sessionID))
	}
	sessionID := op.Admission.SessionID
	viewSession := c.graph.Session
	var (
		updated       OperationRecord
		committedSess SessionRecord
		newEntries    []graphEntry
	)
	err := h.deps.Storage.Transact(ctx, func(tx Transaction) error {
		sessionKey := RegisterKey{SessionID: sessionID, Kind: RegisterSession}
		sreg, err := tx.ReadRegister(sessionKey)
		if err != nil {
			return err
		}
		currentSession, err := decodeSessionRegister(sreg)
		if err != nil {
			return corruptSession(sessionID, "session register: %v", err)
		}
		opKey := RegisterKey{SessionID: sessionID, Kind: RegisterOperation, OperationID: operationID}
		oreg, err := tx.ReadRegister(opKey)
		if err != nil {
			return err
		}
		currentOp, err := decodeOperationRegister(oreg)
		if err != nil {
			return corruptSession(sessionID, "operation register %q: %v", operationID, err)
		}
		// violated semantic preconditions outrank the conflict class
		if currentOp.State.Status != OperationRunning {
			return invalidInput("operation %q is %s; a tool result requires a running Operation", operationID, currentOp.State.Status)
		}
		if fromIntent {
			if currentOp.State.ActiveEffect == nil || currentOp.State.ActiveEffect.Kind != EffectTool ||
				currentOp.State.ActiveEffect.ToolCallID != pending.CallID ||
				currentOp.State.ActiveEffect.ResultEntryID != pending.ResultEntryID {
				return invalidInput("operation %q carries no committed tool effect intent for call %q", operationID, pending.CallID)
			}
			selection = toolSelection{
				arguments:      currentOp.State.ActiveEffect.FinalArguments,
				hooks:          currentOp.State.ActiveEffect.ContributionIDs,
				implementation: currentOp.State.ActiveEffect.Implementation,
			}
		} else if currentOp.State.ActiveEffect != nil {
			return invalidInput("operation %q carries an active effect; an immediate result requires a quiet Operation", operationID)
		}
		index := -1
		for i := range currentOp.State.PendingToolCalls {
			if currentOp.State.PendingToolCalls[i].CallID == pending.CallID {
				index = i
				break
			}
		}
		if index < 0 {
			return invalidInput("operation %q has no pending call %q; every call settles exactly once", operationID, pending.CallID)
		}
		reservation := currentOp.State.PendingToolCalls[index]
		if sreg.Revision != viewSession.Revision {
			return fmt.Errorf("%w: session %q revision %d changed concurrently to %d", errRevisionRace, sessionID, viewSession.Revision, sreg.Revision)
		}
		if oreg.Revision != op.Revision {
			return fmt.Errorf("%w: operation %q revision %d changed concurrently to %d", errRevisionRace, operationID, op.Revision, oreg.Revision)
		}
		entry := toolResultEntry{
			SessionID:           sessionID,
			EntryID:             reservation.ResultEntryID,
			OperationID:         operationID,
			AssistantEntry:      reservation.AssistantEntry,
			ToolCallID:          pending.CallID,
			Status:              outcome.Result.Status,
			Content:             content,
			OriginalContent:     outcome.Result.Content,
			ShapingFailure:      failure,
			Evidence:            evidence,
			Metadata:            ownedToolMetadata(outcome.Metadata),
			NormalizedArguments: model.CloneRaw(selection.arguments),
			HookIDs:             append([]HookIdentity(nil), selection.hooks...),
			Implementation:      selection.implementation,
		}
		payload, err := encodeToolResultEntry(entry)
		if err != nil {
			return err
		}
		adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
			SessionID:   sessionID,
			ID:          entry.EntryID,
			OperationID: operationID,
			Kind:        EntryToolResult,
			Payload:     payload,
		})
		if err != nil {
			return err
		}
		newEntries = append(newEntries, adopted)
		next := currentOp
		next.State.ActiveEffect = nil
		next.State.PendingToolCalls = append(next.State.PendingToolCalls[:index], next.State.PendingToolCalls[index+1:]...)
		if len(next.State.PendingToolCalls) == 0 {
			next.State.Request = nil // the request's final tool settlement closed its batch
		}
		opPayload, err := encodeOperationRegister(next)
		if err != nil {
			return err
		}
		replaced, err := tx.ReplaceRegister(opKey, oreg.Revision, opPayload)
		if err != nil {
			return err
		}
		adoptedOp, err := decodeOperationRegister(replaced)
		if err != nil {
			return corruptSession(sessionID, "committed operation register %q: %v", operationID, err)
		}
		committedSess = SessionRecord{Identity: currentSession.Identity, State: currentSession.State}
		sessionPayload, err := encodeSessionRegister(committedSess)
		if err != nil {
			return err
		}
		replacedSession, err := tx.ReplaceRegister(sessionKey, sreg.Revision, sessionPayload)
		if err != nil {
			return err
		}
		committedSess.Revision = replacedSession.Revision
		updated = adoptedOp
		return nil
	})
	if err != nil {
		h.markCorrupt(sessionID, err)
		c.mu.Unlock()
		if errors.Is(err, errRevisionRace) { // a foreign writer changed the durable state under the cached view
			if rerr := h.rematerialize(ctx, c, sessionID); rerr != nil { // a discovered corruption or storage failure is the current truth
				return model.ToolResult{}, wrapPublication(rerr)
			}
		}
		return model.ToolResult{}, wrapPublication(err)
	}
	c.graph.Entries = append(c.graph.Entries, newEntries...)
	c.graph.replaceOperation(operationID, updated)
	c.graph.Session = committedSess
	c.mu.Unlock()
	h.observeInvalidation(c) // every tool result replaces the Session register
	h.emitToolResultFacts(newEntries, sessionID, operationID)
	settled := outcome.Result
	settled.Content = content
	return settled, nil
}

// runArgumentHooks passes raw or replaced bytes through the ordered chain. A
// deliberate refusal stops this call's chain and settles the call denied with
// the refusal text; later calls in the batch stay eligible. Effectful hooks
// retain their consumed input and consequence before dispatch.
func (d *executionDriver) runArgumentHooks(ctx, settleCtx context.Context, resolved pendingToolCall, hooks []ToolArgumentsHook, raw []byte) (toolSelection, *model.ToolResult, error) {
	pending := resolved.pending
	current := raw
	var contributions []HookIdentity
	accept := func(value json.RawMessage, hook ToolArgumentsHook) {
		contributions = append(contributions, HookIdentity{ExtensionID: hook.ExtensionID, HookID: hook.ID})
		current = value
	}
	for _, hook := range hooks {
		if ctx.Err() != nil { // cancellation stops the next hook before binding
			result := interruptedToolResult(pending.CallID)
			settled, terr := d.harness.commitToolResult(settleCtx, d.coordinator, d.operationID, pending, ToolOutcome{Result: result, Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
			if terr != nil {
				return toolSelection{}, nil, terr
			}
			return toolSelection{}, &settled, nil
		}
		if hook.Mode == HookModePure {
			// A pure hook reserves no effect identity.
			hookCtx := WithCallbackIdentity(ctx, CallbackIdentity{RequestID: d.conversation.request.ID, ToolCallID: pending.CallID})
			consequence, failure, _ := d.evaluateArgumentHook(hookCtx, resolved, hook, current)
			if failure != nil { // a pure callback cannot create external-effect uncertainty
				settled, cerr := d.harness.commitToolResult(settleCtx, d.coordinator, d.operationID, pending, ToolOutcome{Result: *failure, Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
				if cerr != nil {
					return toolSelection{}, nil, cerr
				}
				return toolSelection{}, &settled, nil
			}
			if consequence.Refusal != nil {
				settled, cerr := d.harness.commitToolResult(settleCtx, d.coordinator, d.operationID, pending, ToolOutcome{Result: refusedToolResult(pending.CallID, *consequence.Refusal), Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
				if cerr != nil {
					return toolSelection{}, nil, cerr
				}
				return toolSelection{}, &settled, nil
			}
			accept(consequence.Arguments, hook)
			continue
		}
		committed, settled, hookErr := d.runOneHook(ctx, settleCtx, resolved, hook, current)
		if hookErr != nil || settled != nil {
			return toolSelection{}, settled, hookErr
		}
		accept(committed.Arguments, hook)
	}
	return toolSelection{arguments: current, hooks: contributions}, nil, nil
}

// evaluateArgumentHook accepts unchanged bytes verbatim and a refusal as the
// deliberate denial of this call; a changed value must be a JSON object. A
// refusal without text is not a valid denial under the model result contract
// and stays a processing failure. The bound tool normalizes after the entire
// chain.
func (d *executionDriver) evaluateArgumentHook(ctx context.Context, resolved pendingToolCall, hook ToolArgumentsHook, current json.RawMessage) (ToolArgumentsResult, *model.ToolResult, ExecutionEvidence) {
	record := resolved.record
	owned, err := model.NewToolCall(model.ToolCall{ID: record.ID, Name: record.Name, Arguments: current, Extra: record.Extra})
	if err != nil { // the committed record's identity fields are validated; never reachable for a valid graph
		failure := invalidToolResult(record.ID)
		return ToolArgumentsResult{}, &failure, ExecutionNotStarted
	}
	if ctx.Err() != nil {
		failure := interruptedToolResult(record.ID)
		return ToolArgumentsResult{}, &failure, ExecutionNotStarted
	}
	consequence, runErr := hook.Run(ctx, owned)
	if runErr != nil {
		failure := validationToolResult(record.ID, runErr)
		if EffectInterrupted(runErr) {
			failure = interruptedToolResult(record.ID)
		}
		return ToolArgumentsResult{}, &failure, DeclaredExecutionKnowledge(runErr)
	}
	if consequence.Refusal != nil {
		if *consequence.Refusal == "" {
			failure := invalidToolResult(record.ID)
			return ToolArgumentsResult{}, &failure, ExecutionObserved
		}
		return ToolArgumentsResult{Refusal: consequence.Refusal}, nil, ExecutionObserved
	}
	if bytes.Equal(consequence.Arguments, current) {
		return ToolArgumentsResult{Arguments: model.CloneRaw(consequence.Arguments)}, nil, ExecutionObserved
	}
	if _, err := decodePayloadObject(consequence.Arguments); err != nil { // an invalid returned value is the malformed-class validation failure
		failure := invalidToolResult(record.ID)
		return ToolArgumentsResult{}, &failure, ExecutionObserved
	}
	return ToolArgumentsResult{Arguments: model.CloneRaw(consequence.Arguments)}, nil, ExecutionObserved
}

// runOneHook settles one effectful argument hook as its own Operation effect:
// it commits the hook active-effect intent carrying the extension identity and
// the consumed input bytes before I/O with a fresh reserved result identity,
// runs the hook outside locks and transactions, and commits the validated
// consequence — a successful replacement or refusal (a real result even when
// cancellation arrives before its commit), a hook error, or an interruption
// under the classification fixed for every hook. A refusal settles the call
// itself as denied with the refusal text; later calls in the batch stay
// eligible. The continuation consumes the codec-adopted committed consequence.
func (d *executionDriver) runOneHook(ctx, settleCtx context.Context, resolved pendingToolCall, hook ToolArgumentsHook, current json.RawMessage) (hookResultEntry, *model.ToolResult, error) {
	h := d.harness
	c := d.coordinator
	operationID := d.operationID
	pending := resolved.pending
	resultID, err := h.beginPendingCallEffect(ctx, c, operationID, pending.CallID, EffectHook, pendingCallIntent{
		hookID:               hook.ID,
		extensionID:          hook.ExtensionID,
		inputArgumentsBase64: base64.StdEncoding.EncodeToString(current),
	})
	if err != nil {
		// A marked publication failure propagates before any cancellation
		// rule. Only an unmarked cancellation observed before the intent
		// transaction was ever attempted applies the ordinary interrupted-
		// before-execution settlement and prevents later starts.
		if !isPublicationFailure(err) && isExecutionCancellation(err) && ctx.Err() != nil {
			outcome := ToolOutcome{Result: interruptedToolResult(pending.CallID), Evidence: ExecutionNotStarted}
			settled, terr := h.commitToolResult(settleCtx, c, operationID, pending, outcome, nil, false, toolSelection{}, false)
			if terr != nil {
				return hookResultEntry{}, nil, terr
			}
			return hookResultEntry{}, &settled, nil
		}
		return hookResultEntry{}, nil, err
	}
	if ctx.Err() != nil { // cancellation between the committed intent and Run: the hook provably never ran
		settled, serr := h.settleHookInterruption(settleCtx, c, operationID, pending, hook, resultID, ExecutionNotStarted)
		return hookResultEntry{}, settled, serr
	}
	// The effectful hook receives its actual pending call, producing request
	// attribution, and the reserved result identity of its own effect.
	hookCtx := WithCallbackIdentity(ctx, CallbackIdentity{RequestID: d.conversation.request.ID, ToolCallID: resolved.record.ID, EffectID: resultID})
	consequence, failure, knowledge := d.evaluateArgumentHook(hookCtx, resolved, hook, current)
	if failure != nil {
		if failure.Status == model.ResultInterrupted {
			settled, serr := h.settleHookInterruption(settleCtx, c, operationID, pending, hook, resultID, knowledge)
			return hookResultEntry{}, settled, serr
		}
		settled, serr := h.settleHookError(settleCtx, c, operationID, pending, hook, resultID, *failure, knowledge)
		return hookResultEntry{}, settled, serr
	}
	// The successful output is a real result even if cancellation
	// arrives before its commit: the settlement transaction runs without
	// cancellation. A refusal carries the same bounded denied content in the
	// hook result and in the settled call.
	result := hookResultEntry{Status: hookSucceeded, Evidence: ExecutionObserved, Arguments: consequence.Arguments}
	var refusal *model.ToolResult
	if consequence.Refusal != nil {
		settledRefusal := refusedToolResult(pending.CallID, *consequence.Refusal)
		refusal = &settledRefusal
		result = hookResultEntry{Status: hookSucceeded, Evidence: ExecutionObserved, Refusal: settledRefusal.Content}
	}
	committed, cerr := h.commitHookResult(settleCtx, c, operationID, hookEffectBinding{hookID: hook.ID, callID: pending.CallID}, resultID, result)
	if cerr != nil {
		return hookResultEntry{}, nil, cerr
	}
	if refusal != nil {
		settled, serr := h.commitToolResult(settleCtx, c, operationID, pending, ToolOutcome{Result: *refusal, Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, false)
		if serr != nil {
			return hookResultEntry{}, nil, serr
		}
		return hookResultEntry{}, &settled, nil
	}
	return committed, nil, nil
}

// DeclaredExecutionKnowledge resolves one hook callback failure's execution
// knowledge: a typed EffectError carries its declared knowledge through a
// binding-infrastructure failure unchanged (an invalid declaration reaches the
// shared accepting validator and rejects, never a guessed value), and an
// ordinary native terminal callback failure is an observed execution.
func DeclaredExecutionKnowledge(runErr error) ExecutionEvidence {
	var typed *EffectError
	if errors.As(runErr, &typed) {
		return typed.Evidence
	}
	return ExecutionObserved
}

// isExecutionCancellation classifies pre-Engine boundary errors: the opening
// errors, the existing pre-publication cancellation gates, and the pure
// request-hook return.
func isExecutionCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// settleHookInterruption commits the interrupted hook_result under the
// effect's reserved identity with the intent-retained consumed input and the
// declared knowledge (not_started for a proven pre-Run interruption, observed
// for a cooperative terminal interruption) and settles the pending call
// interrupted through the existing interruption helpers; the Operation's
// terminal interruption settles through the ordinary run settlement. A hook
// that declared unknown knowledge leaves the concrete call's proven-unstarted
// result uncertain. No hook or tool replays.
func (h *Harness) settleHookInterruption(ctx context.Context, c *coordinator, operationID string, pending PendingToolCall, hook ToolArgumentsHook, resultID string, knowledge ExecutionEvidence) (*model.ToolResult, error) {
	if _, err := h.commitHookResult(ctx, c, operationID, hookEffectBinding{hookID: hook.ID, callID: pending.CallID}, resultID, hookResultEntry{
		Status: hookInterrupted, Evidence: knowledge, Error: executionInterruptedDetail,
	}); err != nil {
		return nil, err
	}
	result := interruptedToolResult(pending.CallID)
	settled, err := h.commitToolResult(ctx, c, operationID, pending, ToolOutcome{Result: result, Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, knowledge == ExecutionUnknown)
	if err != nil {
		return nil, err
	}
	return &settled, nil
}

// settleHookError commits the error hook_result under the effect's reserved
// identity with the declared execution knowledge and the intent-retained
// consumed input, and then the ordinary error result for the tool call. Both
// carry the result's already-bounded, nonempty diagnostic; a hook that
// declared unknown knowledge leaves the concrete call's proven-unstarted
// result uncertain.
func (h *Harness) settleHookError(ctx context.Context, c *coordinator, operationID string, pending PendingToolCall, hook ToolArgumentsHook, resultID string, outcome model.ToolResult, knowledge ExecutionEvidence) (*model.ToolResult, error) {
	if _, err := h.commitHookResult(ctx, c, operationID, hookEffectBinding{hookID: hook.ID, callID: pending.CallID}, resultID, hookResultEntry{
		Status: hookFailed, Evidence: knowledge, Error: outcome.Content,
	}); err != nil {
		return nil, err
	}
	settled, err := h.commitToolResult(ctx, c, operationID, pending, ToolOutcome{Result: outcome, Evidence: ExecutionNotStarted}, nil, false, toolSelection{}, knowledge == ExecutionUnknown)
	if err != nil {
		return nil, err
	}
	return &settled, nil
}

// commitHookResult commits one settled effectful hook execution's result
// transaction: the hook_result entry under the effect's reserved identity,
// with the committed intent's extension identity and binding-appropriate
// consumed input stamped in unconditionally, plus the cleared active effect
// atomically — for a request-bound hook, the committed result reference
// extends the published running request state in the same transaction. On
// success it returns the codec-adopted committed entry, whose values the
// synchronous continuation consumes; every failure returns no entry and
// leaves the committed running intent for normal recovery.
func (h *Harness) commitHookResult(ctx context.Context, c *coordinator, operationID string, binding hookEffectBinding, resultID string, res hookResultEntry) (hookResultEntry, error) {
	c.mu.Lock()
	op, ok := c.graph.Operation(operationID)
	if !ok {
		sessionID := c.graph.Session.Identity.SessionID
		c.mu.Unlock()
		return hookResultEntry{}, wrapPublication(fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, sessionID))
	}
	sessionID := op.Admission.SessionID
	viewSession := c.graph.Session
	var (
		updated       OperationRecord
		committedSess SessionRecord
		newEntries    []graphEntry
		committed     hookResultEntry
	)
	err := h.deps.Storage.Transact(ctx, func(tx Transaction) error {
		sessionKey := RegisterKey{SessionID: sessionID, Kind: RegisterSession}
		sreg, err := tx.ReadRegister(sessionKey)
		if err != nil {
			return err
		}
		currentSession, err := decodeSessionRegister(sreg)
		if err != nil {
			return corruptSession(sessionID, "session register: %v", err)
		}
		opKey := RegisterKey{SessionID: sessionID, Kind: RegisterOperation, OperationID: operationID}
		oreg, err := tx.ReadRegister(opKey)
		if err != nil {
			return err
		}
		currentOp, err := decodeOperationRegister(oreg)
		if err != nil {
			return corruptSession(sessionID, "operation register %q: %v", operationID, err)
		}
		// violated semantic preconditions outrank the conflict class
		if currentOp.State.Status != OperationRunning {
			return invalidInput("operation %q is %s; a hook result requires a running Operation", operationID, currentOp.State.Status)
		}
		if currentOp.State.ActiveEffect == nil || currentOp.State.ActiveEffect.Kind != EffectHook ||
			currentOp.State.ActiveEffect.HookID != binding.hookID ||
			currentOp.State.ActiveEffect.ResultEntryID != resultID ||
			(binding.callID != "" && currentOp.State.ActiveEffect.ToolCallID != binding.callID) ||
			(binding.requestID != "" && currentOp.State.ActiveEffect.RequestID != binding.requestID) {
			return invalidInput("operation %q carries no committed hook effect intent for hook %q", operationID, binding.hookID)
		}
		if sreg.Revision != viewSession.Revision {
			return fmt.Errorf("%w: session %q revision %d changed concurrently to %d", errRevisionRace, sessionID, viewSession.Revision, sreg.Revision)
		}
		if oreg.Revision != op.Revision {
			return fmt.Errorf("%w: operation %q revision %d changed concurrently to %d", errRevisionRace, operationID, op.Revision, oreg.Revision)
		}
		intent := currentOp.State.ActiveEffect
		res.SessionID = sessionID
		res.EntryID = resultID
		res.OperationID = operationID
		res.HookID = binding.hookID
		res.ExtensionID = intent.ExtensionID // the committed intent owns the extension identity through the result
		if binding.callID != "" {
			res.ToolCallID = binding.callID
			res.InputArgumentsBase64 = intent.InputArgumentsBase64
		} else {
			res.RequestID = binding.requestID
			res.Purpose = intent.Purpose
			if intent.Evidence != nil {
				consumed := *intent.Evidence
				res.InputEvidence = &consumed
			}
		}
		payload, err := encodeHookResultEntry(res)
		if err != nil {
			return err
		}
		adopted, err := insertAndAdopt(tx, sessionID, EntryDraft{
			SessionID:   sessionID,
			ID:          res.EntryID,
			OperationID: operationID,
			Kind:        EntryHookResult,
			Payload:     payload,
		})
		if err != nil {
			return err
		}
		newEntries = append(newEntries, adopted)
		committed = *adopted.HookResult
		next := currentOp
		next.State.ActiveEffect = nil
		if binding.requestID != "" && next.State.Request != nil {
			requestState := *next.State.Request
			requestState.HookResults = append(requestState.HookResults, EntryRef{SessionID: sessionID, EntryID: resultID})
			next.State.Request = &requestState
		}
		opPayload, err := encodeOperationRegister(next)
		if err != nil {
			return err
		}
		replaced, err := tx.ReplaceRegister(opKey, oreg.Revision, opPayload)
		if err != nil {
			return err
		}
		adoptedOp, err := decodeOperationRegister(replaced)
		if err != nil {
			return corruptSession(sessionID, "committed operation register %q: %v", operationID, err)
		}
		committedSess = SessionRecord{Identity: currentSession.Identity, State: currentSession.State}
		sessionPayload, err := encodeSessionRegister(committedSess)
		if err != nil {
			return err
		}
		replacedSession, err := tx.ReplaceRegister(sessionKey, sreg.Revision, sessionPayload)
		if err != nil {
			return err
		}
		committedSess.Revision = replacedSession.Revision
		updated = adoptedOp
		return nil
	})
	if err != nil {
		h.markCorrupt(sessionID, err)
		c.mu.Unlock()
		if errors.Is(err, errRevisionRace) { // a foreign writer changed the durable state under the cached view
			if rerr := h.rematerialize(ctx, c, sessionID); rerr != nil { // a discovered corruption or storage failure is the current truth
				return hookResultEntry{}, wrapPublication(rerr)
			}
		}
		return hookResultEntry{}, wrapPublication(err)
	}
	c.graph.Entries = append(c.graph.Entries, newEntries...)
	c.graph.replaceOperation(operationID, updated)
	c.graph.Session = committedSess
	c.mu.Unlock()
	h.observeInvalidation(c) // every hook result replaces the Session register; hooks have no progress kind
	return committed, nil
}

// runRequestHooks runs the request stage's selected hooks in
// configured order over the live effective view: every accepted consequence
// is validated before another hook consumes it. A pure consequence validates
// and coalesces in memory with no effect transaction. An effectful hook runs
// as its own settled effect: its intent commits the consumed view before I/O,
// the callback receives the deep-owned input materialized from the committed
// intent's adopted evidence over the round's frozen canonical messages, and
// the continuation applies the codec-adopted committed consequence. A failed
// hook stops this boundary before its next external effect: the request is
// never dispatched, the failure returns for the ordinary terminal settlement,
// and an effectful failure durably settles its error hook_result.
func (d *executionDriver) runRequestHooks(ctx context.Context, inv *openedInvocation, requestID string, view *requestView) error {
	for _, hook := range view.hooks {
		if ctx.Err() != nil { // cancellation stops the next hook before anything commits
			return errExecutionInterrupted
		}
		if hook.Mode == HookModePure {
			consequence, err := d.runOneRequestHookPure(ctx, inv, requestID, view, hook)
			if err != nil {
				return err
			}
			view.recordAccepted(HookIdentity{ExtensionID: hook.ExtensionID, HookID: hook.ID}, consequence)
			continue
		}
		if err := d.runRequestHookEffect(ctx, inv, hook, requestID, view); err != nil {
			return err
		}
	}
	return nil
}

// runOneRequestHookPure validates one pure consequence against the captured
// catalog and shared message rule and returns it owned; nothing durable is
// written for a pure change. The hook input carries the bound request
// identity, purpose and actual model, and the frozen canonical references
// with deep-owned views.
func (d *executionDriver) runOneRequestHookPure(ctx context.Context, inv *openedInvocation, requestID string, view *requestView, hook RequestHook) (RequestConsequence, error) {
	input := view.hookInput(inv.request, requestID)
	consequence, err := hook.Run(WithCallbackIdentity(ctx, CallbackIdentity{RequestID: requestID}), input)
	if err != nil {
		if EffectInterrupted(err) {
			return RequestConsequence{}, errExecutionInterrupted
		}
		// An untyped cancellation-shaped return is the callback's cooperative
		// interruption only while its execution context is done; a typed
		// EffectError remains authoritative even when it declares false.
		var declared *EffectError
		if !errors.As(err, &declared) && ctx.Err() != nil && isExecutionCancellation(err) {
			return RequestConsequence{}, errExecutionInterrupted
		}
		return RequestConsequence{}, err
	}
	return view.acceptConsequence(consequence)
}

// runRequestHookEffect settles one effectful request hook as its own
// Operation effect.
func (d *executionDriver) runRequestHookEffect(ctx context.Context, inv *openedInvocation, hook RequestHook, requestID string, view *requestView) error {
	h := d.harness
	c := d.coordinator
	operationID := d.operationID
	// The consumed evidence the hook will actually receive is rendered from
	// the live view before anything durable commits.
	consumed, err := acceptRequestEvidence(view.evidenceFor())
	if err != nil {
		return err
	}
	resultID, err := h.beginRequestHookEffect(ctx, c, operationID, requestID, inv.request.Purpose, hook.ID, hook.ExtensionID, consumed)
	if err != nil {
		return err
	}
	if ctx.Err() != nil { // cancellation between the committed intent and Run: the hook provably never ran
		if _, cerr := h.commitHookResult(context.WithoutCancel(h.ctx), c, operationID,
			hookEffectBinding{hookID: hook.ID, requestID: requestID}, resultID, hookResultEntry{
				Status: hookInterrupted, Evidence: ExecutionNotStarted, Error: executionInterruptedDetail,
			}); cerr != nil {
			return cerr
		}
		return errExecutionInterrupted
	}
	// The callback consumes the accepted view also published in the intent.
	view.prompt = consumed.Prompt
	view.edits, view.tools, view.parameters = consumed.Edits, consumed.Tools, consumed.Parameters
	input := view.hookInput(inv.request, requestID)
	consequence, runErr := hook.Run(WithCallbackIdentity(ctx, CallbackIdentity{RequestID: requestID, EffectID: resultID}), input)
	settleCtx := context.WithoutCancel(h.ctx)
	if runErr != nil {
		knowledge := DeclaredExecutionKnowledge(runErr)
		status := hookFailed
		if EffectInterrupted(runErr) {
			status = hookInterrupted
		}
		if _, cerr := h.commitHookResult(settleCtx, c, operationID,
			hookEffectBinding{hookID: hook.ID, requestID: requestID}, resultID, hookResultEntry{
				Status: status, Evidence: knowledge, Error: failureDetail(runErr),
			}); cerr != nil {
			return cerr
		}
		if status == hookInterrupted {
			return errExecutionInterrupted
		}
		return runErr
	}
	accepted, verr := view.acceptConsequence(consequence)
	if verr != nil { // an invalid consequence settles the hook effect as its error before any view advances
		if _, cerr := h.commitHookResult(settleCtx, c, operationID,
			hookEffectBinding{hookID: hook.ID, requestID: requestID}, resultID, hookResultEntry{
				Status: hookFailed, Evidence: ExecutionObserved, Error: boundedToolDiagnostic(verr),
			}); cerr != nil {
			return cerr
		}
		return verr
	}
	// The durable commit publishes first; the continuation advances the view
	// from the codec-adopted committed consequence, never the raw callback
	// return.
	entry, cerr := h.commitHookResult(settleCtx, c, operationID,
		hookEffectBinding{hookID: hook.ID, requestID: requestID}, resultID, hookResultEntry{
			Status: hookSucceeded, Evidence: ExecutionObserved, Consequence: &accepted,
		})
	if cerr != nil {
		return cerr
	}
	applied := *entry.Consequence
	view.recordAccepted(HookIdentity{ExtensionID: hook.ExtensionID, HookID: hook.ID}, applied)
	return nil
}

// executionCancellationResult classifies one pre-Engine execution boundary: a
// cancellation-shaped error under a genuinely done execution context is
// deliberate control cancellation and returns the explicit interruption
// terminal result; under a live context the same error shape stays the
// original infrastructure error and this returns nil. It is applied only at
// execute's entry guard, the preparation opener, and the invocation openers —
// never after the Engine has produced its own authoritative outcome.
func executionCancellationResult(execCtx context.Context, err error) *agent.TerminalResult {
	if err != nil && isExecutionCancellation(err) && execCtx.Err() != nil {
		return &agent.TerminalResult{Status: agent.TerminalInterruption, Detail: executionInterruptedDetail}
	}
	return nil
}

// ownOpenedInvocationDeclarations copies an opened invocation's hook element
// arrays into Harness-owned slices immediately after a successful
// OpenInvocation boundary, before validation or retention: from the
// acceptance onward, an opener-retained array or an earlier callback can
// never replace, nil, or rewrite the accepted invocation's hook identities,
// modes, callbacks, or their configured order. A shallow element copy is
// sufficient — every hook entry is a value.
func ownOpenedInvocationDeclarations(inv *InvocationExecution) {
	inv.ModelParameters = inv.ModelParameters.Clone()
	inv.AuthorizationHooks = append([]AuthorizationHook(nil), inv.AuthorizationHooks...)
	if inv.ToolHooks != nil {
		inv.ToolHooks = append(inv.ToolHooks[:0:0], inv.ToolHooks...)
	}
	if inv.ToolOutputHooks != nil {
		inv.ToolOutputHooks = append(inv.ToolOutputHooks[:0:0], inv.ToolOutputHooks...)
	}
}

// validateOpenedInvocation is the opened invocation's contract: a successful
// Model and LookupTool must be non-nil, every argument
// hook requires a non-empty unique ID, a non-empty extension
// identity, a valid mode, and a non-nil Run, while every output hook — pure
// by contract — requires the same identities and Run without a mode. The
// selected strategy binding is accepted once at the maintenance entry, never
// re-gated per invocation. An invalid invocation closes its own opened
// resources before the rejection returns.
func validateOpenedInvocation(inv InvocationExecution) error {
	if inv.Model == nil || inv.LookupTool == nil {
		return invalidInput("opened invocation requires non-nil model and tool lookup functions")
	}
	for _, hook := range inv.AuthorizationHooks {
		if hook.Run == nil {
			return invalidInput("opened invocation requires a non-nil run function for every authorization hook")
		}
	}
	seenHooks := make(map[string]bool, len(inv.ToolHooks))
	for _, hook := range inv.ToolHooks {
		if hook.ID == "" || hook.Run == nil || hook.ExtensionID == "" || !validHookMode(hook.Mode) || seenHooks[hook.ID] {
			return invalidInput("opened invocation requires a non-empty unique ID, a non-empty extension identity, a valid mode, and a non-nil run function for every argument hook")
		}
		seenHooks[hook.ID] = true
	}
	seenOutputHooks := make(map[string]bool, len(inv.ToolOutputHooks))
	for _, hook := range inv.ToolOutputHooks {
		if hook.ID == "" || hook.Run == nil || hook.ExtensionID == "" || seenOutputHooks[hook.ID] {
			return invalidInput("opened invocation requires a non-empty unique ID, a non-empty extension identity, and a non-nil run function for every output hook")
		}
		seenOutputHooks[hook.ID] = true
	}
	return nil
}

func validateRequestHooks(hooks []RequestHook) error {
	seenRequestHooks := make(map[string]bool, len(hooks))
	for _, hook := range hooks {
		if hook.ID == "" || hook.Run == nil || hook.ExtensionID == "" || !validHookMode(hook.Mode) || seenRequestHooks[hook.ID] {
			return invalidInput("request stage requires a non-empty unique ID, a non-empty extension identity, a valid mode, and a non-nil run function for every request hook")
		}
		seenRequestHooks[hook.ID] = true
	}
	return nil
}
