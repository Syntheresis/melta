package harness

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Syntheresis/melta/agent"
	"github.com/Syntheresis/melta/model"
)

// errSteeringBoundary is the one package-private non-cancellation context
// token: a conversation boundary returns it instead of supplying another
// request while steering waits. Agent.Run hands a non-cancellation context
// error back verbatim with a zero terminal, and the outer terminal settlement
// consumes this token by exact identity before its guards. It is produced
// only by the requestSource closure and is never persisted, returned
// publicly, or latched in Wait.
var errSteeringBoundary = errors.New("harness: steering boundary")

// conversationRound is the one request-source-built round an execution's next
// Engine call consumes: the frozen canonical messages and canonical
// references captured at one projection snapshot, the request identity bound
// before contributions, and the frozen contribution view applied to every
// model dispatch of the round. It is process-local composition inside the
// owning pipeline: the same frozen snapshot is consumed throughout the round,
// and no later history reread re-derives it.
type conversationRound struct {
	requestID string
	net       RequestEvidence // final effective contribution evidence of the round
}

// requestView is the live effective request view built by coalescing the
// selected request hooks over the frozen capture in memory.
type requestView struct {
	capture      ExecutionCapture
	canonical    []model.Message // frozen canonical conversation view of the round
	checkpoint   string
	boundary     string
	prompt       string // effective prompt text
	contributors []HookIdentity
	edits        []RequestMessageEdit
	tools        []model.ToolDefinition
	parameters   model.Extra
	hooks        []RequestHook
	release      func()
	summary      bool
}

// newRequestView freezes the captured prompt and the canonical projection
// into one live view.
func newRequestView(capture ExecutionCapture, checkpoint, boundary string, canonical []model.Message) *requestView {
	return &requestView{
		capture:    capture,
		canonical:  canonical,
		checkpoint: checkpoint,
		boundary:   boundary,
		prompt:     capture.SystemPrompt,
		tools:      []model.ToolDefinition{},
	}
}

// newStrategyRequestView freezes one strategy-authored model request into a
// live view for the same request hooks: the request's leading system message,
// when present, is its prompt, the remaining messages are the frozen
// canonical view, and the catalog and advertisement are empty because a
// maintenance request is tool-free. A request with no leading system message
// exposes no prompt.
func newStrategyRequestView(capture ExecutionCapture, checkpoint, boundary string, request model.Request) *requestView {
	prompt := ""
	canonical := request.Messages
	if len(request.Messages) > 0 && request.Messages[0].Role == model.RoleSystem {
		prompt = request.Messages[0].TextContent()
		canonical = request.Messages[1:]
	}
	toolFree := capture
	toolFree.SystemPrompt = prompt
	toolFree.Tools = nil
	return &requestView{
		capture:    toolFree,
		canonical:  canonical,
		checkpoint: checkpoint,
		boundary:   boundary,
		prompt:     prompt,
		tools:      []model.ToolDefinition{},
		summary:    true,
	}
}

// shapeStrategyRequest applies the accepted live view to one frozen
// strategy-authored request: a hook-changed prompt replaces or removes the
// leading system message, edits determine the message list, and every untouched
// message — including its extras and any additional or empty system messages
// — stays byte-for-byte as the strategy authored it. The request stays
// tool-free.
func shapeStrategyRequest(view *requestView, request model.Request) (model.Request, error) {
	messages := cloneRequestMessages(view.currentCanonical())
	originalPrompt := ""
	hasLeadingSystem := len(request.Messages) > 0 && request.Messages[0].Role == model.RoleSystem
	if hasLeadingSystem {
		originalPrompt = request.Messages[0].TextContent()
		if view.prompt == originalPrompt || view.prompt != "" {
			messages = append([]model.Message{cloneMessageValue(request.Messages[0])}, messages...)
		}
	}
	if view.prompt != originalPrompt {
		switch {
		case hasLeadingSystem && view.prompt == "":
		case hasLeadingSystem:
			leading := messages[0]
			leading.Content = []model.ContentPart{{Kind: model.PartText, Text: view.prompt}}
			messages[0] = leading
		case view.prompt != "":
			system, err := model.NewMessage(model.Message{
				Role:    model.RoleSystem,
				Content: []model.ContentPart{{Kind: model.PartText, Text: view.prompt}},
			})
			if err != nil {
				return model.Request{}, err
			}
			messages = append([]model.Message{system}, messages...)
		}
	}
	return model.Request{Messages: messages}, nil
}

func (v *requestView) currentCanonical() []model.Message {
	return materializeRequestEdits(v.canonical, v.edits)
}

// cloneToolDefinitions returns an independent owned copy of one tool
// definition list with raw parameter bytes cloned.
func cloneToolDefinitions(tools []model.ToolDefinition) []model.ToolDefinition {
	if tools == nil {
		return nil
	}
	out := make([]model.ToolDefinition, len(tools))
	for i, tool := range tools {
		out[i] = tool
		out[i].Parameters = model.CloneRaw(tool.Parameters)
	}
	return out
}

func (v *requestView) captureStage(inv InvocationExecution) error {
	v.parameters = inv.ModelParameters.Clone()
	if inv.RequestStage != nil {
		stage := inv.RequestStage()
		v.release = stage.Release
		if stage.Error != nil {
			return stage.Error
		}
		v.hooks = slices.Clone(stage.Hooks)
		if !v.summary {
			v.capture.Tools = cloneToolDefinitions(stage.Catalog)
			v.capture.InitialToolNames = slices.Clone(stage.InitialToolNames)
		}
	}
	if err := validateRequestParameters(v.parameters); err != nil {
		return err
	}
	if err := model.ValidateRequest(model.Request{Tools: v.capture.Tools}); err != nil {
		return err
	}
	if !v.summary {
		if err := v.setInitialAdvertisement(); err != nil {
			return err
		}
	}
	return validateRequestHooks(v.hooks)
}

func (v *requestView) setInitialAdvertisement() error {
	if v.capture.InitialToolNames == nil {
		v.tools = cloneToolDefinitions(v.capture.Tools)
		if v.tools == nil {
			v.tools = []model.ToolDefinition{}
		}
		return nil
	}
	v.tools = make([]model.ToolDefinition, 0, len(v.capture.InitialToolNames))
	for _, name := range v.capture.InitialToolNames {
		i := slices.IndexFunc(v.capture.Tools, func(tool model.ToolDefinition) bool { return tool.Name == name })
		if i < 0 {
			return fmt.Errorf("request stage advertisement names unregistered tool %q", name)
		}
		v.tools = append(v.tools, v.capture.Tools[i])
	}
	return model.ValidateRequest(model.Request{Tools: v.tools})
}

func (v *requestView) hookInput(request InvocationRequest, requestID string) RequestHookInput {
	messages := cloneRequestMessages(v.currentCanonical())
	positions := make([]*int, len(messages))
	if v.edits == nil {
		for i := range v.canonical {
			positions[i] = &i
		}
	} else {
		for i, edit := range v.edits {
			if edit.Position != nil {
				p := *edit.Position
				positions[i] = &p
			}
		}
	}
	return RequestHookInput{
		RequestID: requestID, Purpose: request.Purpose, Model: request.Model,
		CanonicalMessages: cloneRequestMessages(v.canonical), Messages: messages, MessagePositions: positions,
		CheckpointEntryID: v.checkpoint, BoundaryEntryID: v.boundary,
		CapturedPrompt: v.capture.SystemPrompt, Prompt: v.prompt, Catalog: cloneToolDefinitions(v.capture.Tools),
		Tools: cloneToolDefinitions(v.tools), Parameters: v.parameters.Clone(),
	}
}

// applyConsequence replaces each present input and merges body assignments.
func (v *requestView) applyConsequence(con RequestConsequence) {
	if con.Prompt != nil {
		v.prompt = *con.Prompt
	}
	if con.Tools != nil {
		v.tools = con.Tools
	}
	if con.Edits != nil {
		v.edits = con.Edits
	}
	if len(con.Parameters) != 0 {
		v.parameters = mergeRequestParameters(v.parameters, con.Parameters)
	}
}

// recordAccepted applies one accepted consequence and records its identity.
// Every successfully accepted hook identity — no-op, changed or restored
// alike — enters the contributors once, in configured order; a rejected or
// failed consequence never does. Validation and codec adoption stay with the
// caller.
func (v *requestView) recordAccepted(identity HookIdentity, con RequestConsequence) {
	v.applyConsequence(con)
	v.contributors = append(v.contributors, identity)
}

// evidenceFor renders the complete effective view and accepted hook identities.
func (v *requestView) evidenceFor() RequestEvidence {
	evidence := RequestEvidence{
		CheckpointEntryID: v.checkpoint,
		BoundaryEntryID:   v.boundary,
		Prompt:            v.prompt,
		Edits:             v.edits,
		Tools:             v.tools,
		Parameters:        v.parameters,
	}
	if v.summary {
		evidence.SummaryBase = append([]model.Message{}, v.canonical...)
	}
	evidence.HookIDs = append([]HookIdentity(nil), v.contributors...)
	return evidence
}

func acceptRequestEvidence(v RequestEvidence) (RequestEvidence, error) {
	raw, err := encodeRequestEvidence(v)
	if err != nil {
		return RequestEvidence{}, err
	}
	return decodeRequestEvidence(raw)
}

// acceptConsequence validates a contribution against the frozen stage and owns its values.
func (v *requestView) acceptConsequence(con RequestConsequence) (RequestConsequence, error) {
	if err := validateRequestConsequenceShape(con); err != nil {
		return RequestConsequence{}, err
	}
	if con.Edits != nil {
		if err := validateEditedPositions(requestMessageBase{tail: v.canonical}, con.Edits); err != nil {
			return RequestConsequence{}, err
		}
		con.Edits = slices.Clone(con.Edits)
		visible := make(map[int][]model.ContentPart)
		for _, edit := range v.edits {
			if edit.Position != nil && edit.Content != nil {
				visible[*edit.Position] = edit.Content
			}
		}
		for i, edit := range con.Edits {
			if edit.Position != nil && edit.Content == nil {
				con.Edits[i].Content = visible[*edit.Position]
			}
		}
	}
	for _, def := range con.Tools {
		if !slices.ContainsFunc(v.capture.Tools, func(t model.ToolDefinition) bool { return t.Name == def.Name }) {
			return RequestConsequence{}, fmt.Errorf("consequence tool name %q is outside the stage tool catalog", def.Name)
		}
	}
	raw, err := encodeRequestConsequence(con)
	if err != nil {
		return RequestConsequence{}, err
	}
	return decodeRequestConsequence(raw)
}

// buildDispatchRequest composes the accepted evidence over frozen canonical
// messages; its tool entries are the advertisement the Engine gates on.
func buildDispatchRequest(canonical []model.Message, evidence RequestEvidence) model.Request {
	prompt := evidence.Prompt
	messages := make([]model.Message, 0, 1+len(canonical))
	if prompt != "" {
		messages = append(messages, model.Message{
			Role:    model.RoleSystem,
			Content: []model.ContentPart{{Kind: model.PartText, Text: prompt}},
		})
	}
	messages = append(messages, materializeRequestEdits(canonical, evidence.Edits)...)
	return model.Request{Messages: messages, Tools: evidence.Tools}
}

// cloneRequestMessages deep-copies one message list at every ownership
// boundary: content parts, their extras, raw extras values, and the complete
// permitted message fields.
func cloneRequestMessages(messages []model.Message) []model.Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]model.Message, len(messages))
	for i, m := range messages {
		out[i] = cloneMessageValue(m)
	}
	return out
}

// cloneMessageValue returns an independent deep copy of one message value:
// the content parts' extras and the message extras are cloned, and every
// scalar field is carried unchanged.
func cloneMessageValue(m model.Message) model.Message {
	out := m
	out.Content = ownContentParts(m.Content)
	out.Extra = m.Extra.Clone()
	if m.ToolCalls != nil {
		out.ToolCalls = make([]model.ToolCall, len(m.ToolCalls))
		for i, call := range m.ToolCalls {
			call.Arguments = model.CloneRaw(call.Arguments)
			call.Extra = call.Extra.Clone()
			out.ToolCalls[i] = call
		}
	}
	return out
}

// requestSource returns the Agent request boundary of the bound conversation
// invocation. Its order is the owning pipeline's: first steering, then the
// frozen canonical projection and the complete automatic maintenance procedure
// over canonical-only input, then — after a published checkpoint — the rebuilt
// projection, then the conversation request identity minted in memory and
// bound before contributions, then the selected request hooks, and finally the
// model dispatch request. The first call projects the newly admitted input
// only (first-request protection), and every later call selects the
// conversation boundary — when steering waits it returns errSteeringBoundary
// instead of supplying another request, leaving every head buffered for the
// ordinary post-terminal drain. Selection consumes no head and performs no
// cancellation or observation. Physical retries and the maintenance request
// callback never reach this boundary. The invocation's owned frame
// carries the built identity and evidence to the model effect of the same
// execution.
func (d *executionDriver) requestSource() agent.RequestSource {
	h := d.harness
	c := d.coordinator
	inv := d.conversation
	capture := d.capture
	build := func(ctx context.Context) (model.Request, error) {
		if !inv.firstRequest {
			c.mu.Lock()
			waiting := len(c.steering) > 0
			c.mu.Unlock()
			if waiting {
				return model.Request{}, errSteeringBoundary
			}
		}
		inv.firstRequest = false
		if err := ctx.Err(); err != nil { // the source never starts work under a canceled execution
			return model.Request{}, errExecutionInterrupted
		}
		canonical, checkpointID, boundaryID, err := h.projectCanonical(c, d.operationID)
		if err != nil {
			return model.Request{}, err
		}
		planInput := PlanInput{
			Trigger:           triggerAutomatic,
			Messages:          cloneRequestMessages(canonical),
			CheckpointEntryID: checkpointID,
			BoundaryEntryID:   boundaryID,
			Instructions:      capture.Compact.SystemPrompt,
			Tools:             cloneToolDefinitions(capture.Tools),
			Measurement:       contextMeasurement(c),
			ContextWindow:     capture.ContextWindow,
			OutputReserve:     capture.OutputReserve,
			Settings:          model.CloneRaw(capture.Strategy.Settings),
		}
		changed, err := d.completeMaintenance(ctx, planInput, false)
		if err != nil {
			return model.Request{}, err // the original cause stands; the outer settled guard preserves the existing terminal
		}
		if changed { // the committed checkpoint leads; the conversation view is rebuilt from it
			canonical, checkpointID, boundaryID, err = h.projectCanonical(c, d.operationID)
			if err != nil {
				return model.Request{}, err
			}
		}
		requestID, err := newHexID() // the request identity is minted in memory and bound before contributions
		if err != nil {
			return model.Request{}, fmt.Errorf("%w: %v", ErrStorage, err)
		}
		view := newRequestView(capture, checkpointID, boundaryID, canonical)
		defer func() {
			if view.release != nil {
				view.release()
			}
		}()
		if err := view.captureStage(inv.callbacks); err != nil {
			return model.Request{}, err
		}
		if err := d.runRequestHooks(ctx, inv, requestID, view); err != nil {
			return model.Request{}, err
		}
		evidence, err := acceptRequestEvidence(view.evidenceFor())
		if err != nil {
			return model.Request{}, err
		}
		req := buildDispatchRequest(canonical, evidence)
		inv.round = conversationRound{
			requestID: requestID,
			net:       evidence,
		}
		return req, nil
	}
	return func(ctx context.Context) (agent.RequestResult, error) {
		req, err := build(ctx)
		if err == errExecutionInterrupted {
			return agent.RequestResult{Interrupted: true}, nil
		}
		if err != nil {
			return agent.RequestResult{}, err
		}
		return agent.RequestResult{Request: &req}, nil
	}
}

// projectContext is the pure projection body: it snapshots the resident graph
// under the coordinator lock, releases it, and builds the messages without
// touching steering or queued buffers, returning the messages and the same-
// snapshot canonical checkpoint and boundary references. When the Session
// register's CompactionEntryID is set, the projection is the system message,
// one assistant summary message built from the named compaction entry's
// payload, and the entries after the sequence of the boundary entry that the
// named entry's payload records — the named entry and every entry at or
// before that boundary never project as messages. With the field empty the
// full history projects.
func (h *Harness) projectContext(c *coordinator, operationID string) ([]model.Message, error) {
	messages, _, _, _, err := h.projectContextRefs(c, operationID)
	return messages, err
}

// projectContextRefs projects the conversation and reports the named
// checkpoint and the canonical boundary entry of the same projection
// snapshot, plus whether the leading system prompt was injected at the top of
// the projection. The boundary is the newest conversation-bearing entry of
// the projection's kept tail; a rebuilt projection whose entry tail is the
// checkpoint alone names the checkpoint entry itself as its canonical
// boundary, so a rebuilt projection always names a complete
// checkpoint-and-tail pair — and an uncompacted projection with no
// projectable entry names none.
func (h *Harness) projectContextRefs(c *coordinator, operationID string) ([]model.Message, string, string, bool, error) {
	c.mu.Lock()
	sessionID := c.graph.Session.Identity.SessionID
	op, ok := c.graph.Operation(operationID)
	if !ok {
		c.mu.Unlock()
		return nil, "", "", false, fmt.Errorf("%w: operation %q in session %q", ErrNotFound, operationID, sessionID)
	}
	entries := c.graph.Entries
	systemPrompt := op.Admission.Execution.SystemPrompt
	compactionID := c.graph.Session.State.CheckpointEntryID
	c.mu.Unlock()

	messages := make([]model.Message, 0, len(entries)+1)
	injected := systemPrompt != ""
	if injected {
		msg, err := model.NewMessage(model.Message{
			Role:    model.RoleSystem,
			Content: []model.ContentPart{{Kind: model.PartText, Text: systemPrompt}},
		})
		if err != nil {
			return nil, "", "", false, err
		}
		messages = append(messages, msg)
	}
	// The named checkpoint's payload supplies its owned messages and
	// its boundary target's sequence is boundarySequence, at or before which
	// no entry projects; -1 means no checkpoint boundary and the full
	// history projects.
	compaction, boundarySequence := findCompactionCutoff(entries, compactionID)
	if compaction != nil {
		messages = append(messages, cloneRequestMessages(compaction.Messages)...)
	}
	boundary := ""
	for _, entry := range entries {
		if entry.Envelope.Sequence <= boundarySequence {
			continue
		}
		// The named checkpoint entry itself never displaces the kept tail's
		// canonical boundary: it names that boundary only when the entry
		// tail carries no message-bearing entry of its own, exactly as the
		// committed advance publishes it.
		if entry.Envelope.ID != compactionID && messageBearingEntryKind(entry) {
			boundary = entry.Envelope.ID
		}
		msg, ok, err := projectEntry(entry)
		if err != nil {
			return nil, "", "", false, err
		}
		if ok {
			messages = append(messages, msg)
		}
	}
	if boundary == "" && compactionID != "" {
		boundary = compactionID
	}
	return messages, compactionID, boundary, injected, nil
}

// projectCanonical is the frozen canonical conversation view one request
// round consumes: the projected messages with exactly the injected captured
// system prompt stripped (the prompt is the request's rendered text, carried
// separately) — a checkpoint's own leading system message is never stripped —
// together with the same-snapshot canonical references.
func (h *Harness) projectCanonical(c *coordinator, operationID string) ([]model.Message, string, string, error) {
	messages, checkpointID, boundaryID, injected, err := h.projectContextRefs(c, operationID)
	if err != nil {
		return nil, "", "", err
	}
	if injected && len(messages) > 0 {
		messages = messages[1:]
	}
	return messages, checkpointID, boundaryID, nil
}

// projectEntry maps one committed entry to its one model message under the
// kind-to-message mapping. An operation settlement produces no model message,
// and a compaction entry never materializes either: the register's
// CompactionEntryID carries its summary into the projection instead. Request
// evidence is likewise never conversation text: it lives on entry payloads and
// register state, never in a projected message.
func projectEntry(entry graphEntry) (model.Message, bool, error) {
	switch {
	case entry.Input != nil:
		msg, err := model.NewMessage(model.Message{
			Role:    model.RoleUser,
			Content: entry.Input.Content,
		})
		return msg, true, err
	case entry.Assistant != nil:
		calls := make([]model.ToolCall, 0, len(entry.Assistant.ToolCalls))
		for _, call := range entry.Assistant.ToolCalls {
			raw, err := base64.StdEncoding.DecodeString(call.ArgumentsBase64)
			if err != nil {
				return model.Message{}, false, fmt.Errorf("entry %s: tool call %q arguments: %w", entry.Envelope.ID, call.ID, err)
			}
			calls = append(calls, model.ToolCall{ID: call.ID, Name: call.Name, Arguments: raw, Extra: call.Extra})
		}
		msg, err := model.NewMessage(model.Message{
			Role:      model.RoleAssistant,
			Source:    entry.Assistant.Source,
			Content:   entry.Assistant.Content,
			Refusal:   entry.Assistant.Refusal,
			Extra:     entry.Assistant.Extra,
			ToolCalls: calls,
		})
		return msg, true, err
	case entry.ToolResult != nil:
		msg, err := model.NewMessage(model.Message{
			Role:       model.RoleTool,
			ToolCallID: entry.ToolResult.ToolCallID,
			Content:    []model.ContentPart{{Kind: model.PartText, Text: entry.ToolResult.Content}},
		})
		return msg, true, err
	case entry.Signal != nil:
		msg, err := model.NewMessage(model.Message{
			Role:    model.RoleUser,
			Content: []model.ContentPart{{Kind: model.PartText, Text: signalProjectedText(entry.Signal.Content)}},
		})
		return msg, true, err
	default:
		return model.Message{}, false, nil
	}
}

// signalProjectedText renders one signal's fixed content inside the retained
// system-signal text wrapper; the typed signal payload stays the canonical
// origin and subtype authority.
func signalProjectedText(content string) string {
	return "<system-signal>" + strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(content) + "</system-signal>"
}

// Interrupt interrupts one Session's execution, keyed on the durable state:
// an idle Session — no current Operation — returns nil. Otherwise both
// interrupt vehicles fire: the installed execution's context is canceled, and
// the not-yet-started executions' marker is set to the durable current
// Operation. The marker is consumed at execute's entry when it matches that
// operation — a marker for a running operation is inert for that entry and
// stays visible to the shared success guard — and canceling a retiring
// predecessor is a harmless no-op. Buffers are never discarded by an
// interrupt: unselected items drain after the interrupted Operation's
// terminal.
func (h *Harness) Interrupt(ctx context.Context, sessionID string) error {
	c, release, err := h.acquireCoordinator(ctx, sessionID)
	if err != nil {
		return err
	}
	defer release()
	c.mu.Lock()
	operationID := c.graph.Session.State.CurrentOperationID
	if operationID == "" {
		c.mu.Unlock()
		return nil
	}
	c.interruptOp = operationID
	run := c.run
	c.mu.Unlock()
	if run != nil {
		run.cancel()
	}
	return nil
}
