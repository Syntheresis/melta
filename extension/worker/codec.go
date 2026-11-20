package worker

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type modelRefJSON struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

// modelRefFrom renders one model identity as the fixed schema's required
// {provider,model} object. A zero or partial identity has no schema form and
// fails the call.
func modelRefFrom(v model.ModelRef) (modelRefJSON, error) {
	if v.Provider == "" || v.Model == "" {
		return modelRefJSON{}, fmt.Errorf("worker: model reference %q is not complete", v.String())
	}
	return modelRefJSON{Provider: v.Provider, Model: v.Model}, nil
}

// optionalModelRef renders one optional model identity; a zero value stays
// absent.
func optionalModelRef(v model.ModelRef) (*modelRefJSON, error) {
	if v.IsZero() {
		return nil, nil
	}
	encoded, err := modelRefFrom(v)
	if err != nil {
		return nil, err
	}
	return &encoded, nil
}

func modelRefValue(v modelRefJSON) model.ModelRef {
	return model.ModelRef{Provider: v.Provider, Model: v.Model}
}

type extraJSON struct {
	Key   JSONBytes `json:"key"`
	Value JSONBytes `json:"value"`
}

func extraFrom(v model.Extra) []extraJSON {
	if len(v) == 0 {
		return nil
	}
	keys := make([]string, 0, len(v))
	for key := range v {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]extraJSON, 0, len(v))
	for _, key := range keys {
		out = append(out, extraJSON{Key: JSONBytes(key), Value: JSONBytes(v[key])})
	}
	return out
}

func extraValue(v []extraJSON) model.Extra {
	if len(v) == 0 {
		return nil
	}
	out := make(model.Extra, len(v))
	for _, entry := range v {
		out[string(entry.Key)] = append(json.RawMessage(nil), entry.Value...)
	}
	return out
}

type contentPartJSON struct {
	Kind           string      `json:"kind"`
	Text           string      `json:"text,omitempty"`
	URL            string      `json:"url,omitempty"`
	OpaqueWireType string      `json:"opaque_wire_type,omitempty"`
	Extra          []extraJSON `json:"extra,omitempty"`
}

func contentFrom(v []model.ContentPart) []contentPartJSON {
	out := make([]contentPartJSON, len(v))
	for i, part := range v {
		out[i] = contentPartJSON{
			Kind:           string(part.Kind),
			Text:           part.Text,
			URL:            part.URL,
			OpaqueWireType: part.OpaqueWireType,
			Extra:          extraFrom(part.Extra),
		}
	}
	return out
}

func contentValue(v []contentPartJSON) []model.ContentPart {
	out := make([]model.ContentPart, len(v))
	for i, part := range v {
		out[i] = model.ContentPart{
			Kind:           model.PartKind(part.Kind),
			Text:           part.Text,
			URL:            part.URL,
			OpaqueWireType: part.OpaqueWireType,
			Extra:          extraValue(part.Extra),
		}
	}
	return out
}

type toolCallJSON struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Arguments JSONBytes   `json:"arguments"`
	Extra     []extraJSON `json:"extra,omitempty"`
}

func toolCallFrom(v model.ToolCall) toolCallJSON {
	return toolCallJSON{ID: v.ID, Name: v.Name, Arguments: JSONBytes(v.Arguments), Extra: extraFrom(v.Extra)}
}

func toolCallValue(v toolCallJSON) model.ToolCall {
	return model.ToolCall{ID: v.ID, Name: v.Name, Arguments: append(json.RawMessage(nil), v.Arguments...), Extra: extraValue(v.Extra)}
}

type messageJSON struct {
	Role       string            `json:"role"`
	Content    []contentPartJSON `json:"content"`
	Refusal    string            `json:"refusal,omitempty"`
	ToolCalls  []toolCallJSON    `json:"tool_calls,omitempty"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
	Name       string            `json:"name,omitempty"`
	Extra      []extraJSON       `json:"extra,omitempty"`
	Source     *modelRefJSON     `json:"source,omitempty"`
}

func messageFrom(v model.Message) (messageJSON, error) {
	out := messageJSON{
		Role:       string(v.Role),
		Content:    contentFrom(v.Content),
		Refusal:    v.Refusal,
		ToolCallID: v.ToolCallID,
		Name:       v.Name,
		Extra:      extraFrom(v.Extra),
	}
	if len(v.ToolCalls) != 0 {
		out.ToolCalls = make([]toolCallJSON, len(v.ToolCalls))
		for i, call := range v.ToolCalls {
			out.ToolCalls[i] = toolCallFrom(call)
		}
	}
	source, err := optionalModelRef(v.Source)
	if err != nil {
		return messageJSON{}, err
	}
	out.Source = source
	return out, nil
}

func messageValue(v messageJSON) model.Message {
	out := model.Message{
		Role:       model.Role(v.Role),
		Content:    contentValue(v.Content),
		Refusal:    v.Refusal,
		ToolCallID: v.ToolCallID,
		Name:       v.Name,
		Extra:      extraValue(v.Extra),
	}
	if len(v.ToolCalls) != 0 {
		out.ToolCalls = make([]model.ToolCall, len(v.ToolCalls))
		for i, call := range v.ToolCalls {
			out.ToolCalls[i] = toolCallValue(call)
		}
	}
	if v.Source != nil {
		out.Source = modelRefValue(*v.Source)
	}
	return out
}

func messagesRequired(v []model.Message) ([]messageJSON, error) {
	out := make([]messageJSON, len(v))
	for i, message := range v {
		encoded, err := messageFrom(message)
		if err != nil {
			return nil, err
		}
		out[i] = encoded
	}
	return out, nil
}

func messagesValue(v []messageJSON) []model.Message {
	out := make([]model.Message, len(v))
	for i, message := range v {
		out[i] = messageValue(message)
	}
	return out
}

type toolDefinitionJSON struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Parameters  JSONBytes `json:"parameters"`
}

func toolDefinitionFrom(v model.ToolDefinition) toolDefinitionJSON {
	return toolDefinitionJSON{Name: v.Name, Description: v.Description, Parameters: JSONBytes(v.Parameters)}
}

func toolDefinitionValue(v toolDefinitionJSON) model.ToolDefinition {
	return model.ToolDefinition{Name: v.Name, Description: v.Description, Parameters: append(json.RawMessage(nil), v.Parameters...)}
}

func toolDefinitionsFrom(v []model.ToolDefinition) []toolDefinitionJSON {
	out := make([]toolDefinitionJSON, len(v))
	for i, definition := range v {
		out[i] = toolDefinitionFrom(definition)
	}
	return out
}

func toolDefinitionsValue(v []toolDefinitionJSON) []model.ToolDefinition {
	out := make([]model.ToolDefinition, len(v))
	for i, definition := range v {
		out[i] = toolDefinitionValue(definition)
	}
	return out
}

type toolResultJSON struct {
	CallID  string `json:"call_id"`
	Status  string `json:"status"`
	Content string `json:"content"`
}

func toolResultFrom(v model.ToolResult) toolResultJSON {
	return toolResultJSON{CallID: v.CallID, Status: string(v.Status), Content: v.Content}
}

func toolResultValue(v toolResultJSON) model.ToolResult {
	return model.ToolResult{CallID: v.CallID, Status: model.ToolResultStatus(v.Status), Content: v.Content}
}

type toolOutcomeJSON struct {
	Result   toolResultJSON `json:"result"`
	Metadata JSONBytes      `json:"metadata,omitempty"`
	Evidence string         `json:"evidence"`
}

func toolOutcomeFrom(v harness.ToolOutcome) toolOutcomeJSON {
	return toolOutcomeJSON{
		Result:   toolResultFrom(v.Result),
		Metadata: JSONBytes(v.Metadata),
		Evidence: string(v.Evidence),
	}
}

func toolOutcomeValue(v toolOutcomeJSON) harness.ToolOutcome {
	return harness.ToolOutcome{
		Result:   toolResultValue(v.Result),
		Metadata: append(json.RawMessage(nil), v.Metadata...),
		Evidence: harness.ExecutionEvidence(v.Evidence),
	}
}

type permissionJSON struct {
	Permission string `json:"permission"`
	Target     string `json:"target"`
}

type toolPlanJSON struct {
	Permissions        []permissionJSON `json:"permissions"`
	CanonicalWorkspace string           `json:"canonical_workspace,omitempty"`
	Immediate          *toolOutcomeJSON `json:"immediate,omitempty"`
	Data               JSONBytes        `json:"data,omitempty"`
}

func toolPlanFrom(v extension.ToolPlan) toolPlanJSON {
	out := toolPlanJSON{
		Permissions:        make([]permissionJSON, len(v.Permissions)),
		CanonicalWorkspace: v.CanonicalWorkspace,
		Data:               JSONBytes(v.Data),
	}
	for i, permission := range v.Permissions {
		out.Permissions[i] = permissionJSON{Permission: permission.Permission, Target: permission.Target}
	}
	if v.Immediate != nil {
		immediate := toolOutcomeFrom(*v.Immediate)
		out.Immediate = &immediate
	}
	return out
}

func toolPlanValue(v toolPlanJSON) extension.ToolPlan {
	out := extension.ToolPlan{
		Permissions:        make([]harness.PermissionRequest, len(v.Permissions)),
		CanonicalWorkspace: v.CanonicalWorkspace,
		Data:               append([]byte(nil), v.Data...),
	}
	for i, permission := range v.Permissions {
		out.Permissions[i] = harness.PermissionRequest{Permission: permission.Permission, Target: permission.Target}
	}
	if v.Immediate != nil {
		immediate := toolOutcomeValue(*v.Immediate)
		out.Immediate = &immediate
	}
	return out
}

type argumentsJSON struct {
	Arguments JSONBytes `json:"arguments"`
}

// EncodeArguments encodes the shared {arguments} result of normalize and the
// before-tool hook.
func EncodeArguments(v json.RawMessage) (json.RawMessage, error) {
	return marshalValue(argumentsJSON{Arguments: JSONBytes(v)})
}

// DecodeArguments decodes the shared {arguments} result of normalize and the
// before-tool hook.
func DecodeArguments(raw json.RawMessage) (json.RawMessage, error) {
	var wire argumentsJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), wire.Arguments...), nil
}

// EncodePrepareResult encodes one prepared tool plan.
func EncodePrepareResult(v extension.ToolPlan) (json.RawMessage, error) {
	return marshalValue(toolPlanFrom(v))
}

// DecodePrepareResult decodes one prepared tool plan.
func DecodePrepareResult(raw json.RawMessage) (extension.ToolPlan, error) {
	var wire toolPlanJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return extension.ToolPlan{}, err
	}
	return toolPlanValue(wire), nil
}

// EncodeExecuteResult encodes one tool execution outcome.
func EncodeExecuteResult(v harness.ToolOutcome) (json.RawMessage, error) {
	return marshalValue(toolOutcomeFrom(v))
}

// DecodeExecuteResult decodes one tool execution outcome.
func DecodeExecuteResult(raw json.RawMessage) (harness.ToolOutcome, error) {
	var wire toolOutcomeJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return harness.ToolOutcome{}, err
	}
	return toolOutcomeValue(wire), nil
}

type requestHookInputJSON struct {
	RequestID         string                `json:"request_id"`
	Purpose           string                `json:"purpose"`
	Model             modelRefJSON          `json:"model"`
	CanonicalMessages []messageJSON         `json:"canonical_messages"`
	Messages          []messageJSON         `json:"messages"`
	CheckpointEntryID string                `json:"checkpoint_entry_id,omitempty"`
	BoundaryEntryID   string                `json:"boundary_entry_id,omitempty"`
	CapturedPrompt    string                `json:"captured_prompt"`
	Prompt            string                `json:"prompt"`
	Catalog           []toolDefinitionJSON  `json:"catalog"`
	Tools             []toolDefinitionJSON  `json:"tools"`
	MessagePositions  []requestPositionJSON `json:"message_positions"`
	Parameters        []extraJSON           `json:"parameters,omitempty"`
}

type requestPositionJSON struct {
	Position *int `json:"position,omitempty"`
}

// EncodeRequestHookParams encodes one request hook input.
func EncodeRequestHookParams(v harness.RequestHookInput) (json.RawMessage, error) {
	canonical, err := messagesRequired(v.CanonicalMessages)
	if err != nil {
		return nil, err
	}
	messages, err := messagesRequired(v.Messages)
	if err != nil {
		return nil, err
	}
	modelRef, err := modelRefFrom(v.Model)
	if err != nil {
		return nil, err
	}
	if len(v.MessagePositions) != len(v.Messages) {
		return nil, fmt.Errorf("request positions do not match messages")
	}
	positions := make([]requestPositionJSON, len(v.MessagePositions))
	for i, position := range v.MessagePositions {
		positions[i].Position = position
	}
	return marshalValue(requestHookInputJSON{
		RequestID:         v.RequestID,
		Purpose:           string(v.Purpose),
		Model:             modelRef,
		CanonicalMessages: canonical,
		Messages:          messages,
		CheckpointEntryID: v.CheckpointEntryID,
		BoundaryEntryID:   v.BoundaryEntryID,
		CapturedPrompt:    v.CapturedPrompt,
		Prompt:            v.Prompt,
		Catalog:           toolDefinitionsFrom(v.Catalog),
		Tools:             toolDefinitionsFrom(v.Tools),
		MessagePositions:  positions,
		Parameters:        extraFrom(v.Parameters),
	})
}

// DecodeRequestHookParams decodes one request hook input.
func DecodeRequestHookParams(raw json.RawMessage) (harness.RequestHookInput, error) {
	var wire requestHookInputJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return harness.RequestHookInput{}, err
	}
	if wire.MessagePositions == nil || len(wire.MessagePositions) != len(wire.Messages) {
		return harness.RequestHookInput{}, fmt.Errorf("request positions do not match messages")
	}
	positions := make([]*int, len(wire.MessagePositions))
	for i, p := range wire.MessagePositions {
		if p.Position != nil && *p.Position < 0 {
			return harness.RequestHookInput{}, fmt.Errorf("request position must be nonnegative")
		}
		positions[i] = p.Position
	}
	return harness.RequestHookInput{
		RequestID:         wire.RequestID,
		Purpose:           harness.InvocationPurpose(wire.Purpose),
		Model:             modelRefValue(wire.Model),
		CanonicalMessages: messagesValue(wire.CanonicalMessages),
		Messages:          messagesValue(wire.Messages),
		CheckpointEntryID: wire.CheckpointEntryID,
		BoundaryEntryID:   wire.BoundaryEntryID,
		CapturedPrompt:    wire.CapturedPrompt,
		Prompt:            wire.Prompt,
		Catalog:           toolDefinitionsValue(wire.Catalog),
		Tools:             toolDefinitionsValue(wire.Tools),
		MessagePositions:  positions,
		Parameters:        extraValue(wire.Parameters),
	}, nil
}

type requestConsequenceJSON struct {
	Prompt *string `json:"prompt,omitempty"`

	Edits      *[]requestMessageEditJSON `json:"edits,omitempty"`
	Tools      *[]toolDefinitionJSON     `json:"tools,omitempty"`
	Parameters []extraJSON               `json:"parameters,omitempty"`
}

type requestMessageEditJSON struct {
	Position *int               `json:"position,omitempty"`
	Content  *[]contentPartJSON `json:"content,omitempty"`
	Message  *messageJSON       `json:"message,omitempty"`
}

// EncodeRequestHookResult encodes one request consequence. An absent tool set
// remains absent; a present empty set stays present and empty.
func EncodeRequestHookResult(v harness.RequestConsequence) (json.RawMessage, error) {
	out := requestConsequenceJSON{Prompt: v.Prompt, Parameters: extraFrom(v.Parameters)}
	if v.Tools != nil {
		tools := toolDefinitionsFrom(v.Tools)
		out.Tools = &tools
	}
	if v.Edits != nil {
		edits := make([]requestMessageEditJSON, 0, len(v.Edits))
		for _, edit := range v.Edits {
			wire := requestMessageEditJSON{Position: edit.Position}

			if edit.Content != nil {
				content := contentFrom(edit.Content)
				wire.Content = &content
			}
			if edit.Message != nil {
				msg, err := messageFrom(*edit.Message)
				if err != nil {
					return nil, err
				}
				wire.Message = &msg
			}
			edits = append(edits, wire)
		}
		out.Edits = &edits
	}

	return marshalValue(out)
}

// DecodeRequestHookResult decodes one request consequence.
func DecodeRequestHookResult(raw json.RawMessage) (harness.RequestConsequence, error) {
	var wire requestConsequenceJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return harness.RequestConsequence{}, err
	}
	out := harness.RequestConsequence{Prompt: wire.Prompt, Parameters: extraValue(wire.Parameters)}
	if wire.Tools != nil {
		out.Tools = toolDefinitionsValue(*wire.Tools)
	}
	if wire.Edits != nil {
		out.Edits = make([]harness.RequestMessageEdit, 0, len(*wire.Edits))
		for _, item := range *wire.Edits {
			edit := harness.RequestMessageEdit{Position: item.Position}
			if edit.Position != nil && *edit.Position < 0 {
				return harness.RequestConsequence{}, fmt.Errorf("edit position must be a nonnegative integer")
			}
			if item.Content != nil {
				edit.Content = contentValue(*item.Content)
			}
			if item.Message != nil {
				msg := messageValue(*item.Message)
				edit.Message = &msg
			}
			if (edit.Position == nil) == (edit.Message == nil) || edit.Message != nil && edit.Content != nil {
				return harness.RequestConsequence{}, fmt.Errorf("edit must retain a position or add a message")
			}
			out.Edits = append(out.Edits, edit)
		}
	}

	return out, nil
}

// EncodeBeforeToolHookParams encodes one before-tool hook input.
func EncodeBeforeToolHookParams(v model.ToolCall) (json.RawMessage, error) {
	return marshalValue(toolCallFrom(v))
}

// DecodeBeforeToolHookParams decodes one before-tool hook input.
func DecodeBeforeToolHookParams(raw json.RawMessage) (model.ToolCall, error) {
	var wire toolCallJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return model.ToolCall{}, err
	}
	return toolCallValue(wire), nil
}

type toolOutputInputJSON struct {
	Outcome toolOutcomeJSON `json:"outcome"`
	Content string          `json:"content"`
}

// EncodeAfterToolHookParams encodes one after-tool hook input.
func EncodeAfterToolHookParams(v harness.ToolOutputInput) (json.RawMessage, error) {
	return marshalValue(toolOutputInputJSON{Outcome: toolOutcomeFrom(v.Outcome), Content: v.Content})
}

// DecodeAfterToolHookParams decodes one after-tool hook input.
func DecodeAfterToolHookParams(raw json.RawMessage) (harness.ToolOutputInput, error) {
	var wire toolOutputInputJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return harness.ToolOutputInput{}, err
	}
	return harness.ToolOutputInput{Outcome: toolOutcomeValue(wire.Outcome), Content: wire.Content}, nil
}

type contentJSON struct {
	Content string `json:"content"`
}

// EncodeAfterToolHookResult encodes one after-tool hook result.
func EncodeAfterToolHookResult(v string) (json.RawMessage, error) {
	return marshalValue(contentJSON{Content: v})
}

// DecodeAfterToolHookResult decodes one after-tool hook result.
func DecodeAfterToolHookResult(raw json.RawMessage) (string, error) {
	var wire contentJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return "", err
	}
	return wire.Content, nil
}

type planInputJSON struct {
	Trigger           string               `json:"trigger"`
	Messages          []messageJSON        `json:"messages"`
	CheckpointEntryID string               `json:"checkpoint_entry_id,omitempty"`
	BoundaryEntryID   string               `json:"boundary_entry_id"`
	Instructions      string               `json:"instructions"`
	Tools             []toolDefinitionJSON `json:"tools"`
	Measurement       *harness.UsageCount  `json:"measurement,omitempty"`
	ContextWindow     int                  `json:"context_window"`
	OutputReserve     int                  `json:"output_reserve"`
	Settings          JSONBytes            `json:"settings,omitempty"`
}

func planInputFrom(v harness.PlanInput) (planInputJSON, error) {
	messages, err := messagesRequired(v.Messages)
	if err != nil {
		return planInputJSON{}, err
	}
	return planInputJSON{
		Trigger:           v.Trigger,
		Messages:          messages,
		CheckpointEntryID: v.CheckpointEntryID,
		BoundaryEntryID:   v.BoundaryEntryID,
		Instructions:      v.Instructions,
		Tools:             toolDefinitionsFrom(v.Tools),
		Measurement:       v.Measurement,
		ContextWindow:     v.ContextWindow,
		OutputReserve:     v.OutputReserve,
		Settings:          JSONBytes(v.Settings),
	}, nil
}

func planInputValue(v planInputJSON) harness.PlanInput {
	return harness.PlanInput{
		Trigger:           v.Trigger,
		Messages:          messagesValue(v.Messages),
		CheckpointEntryID: v.CheckpointEntryID,
		BoundaryEntryID:   v.BoundaryEntryID,
		Instructions:      v.Instructions,
		Tools:             toolDefinitionsValue(v.Tools),
		Measurement:       v.Measurement,
		ContextWindow:     v.ContextWindow,
		OutputReserve:     v.OutputReserve,
		Settings:          append(json.RawMessage(nil), v.Settings...),
	}
}

type modelRequestJSON struct {
	Messages []messageJSON        `json:"messages"`
	Tools    []toolDefinitionJSON `json:"tools"`
}

type modelPlanJSON struct {
	Request modelRequestJSON `json:"request"`
	State   JSONBytes        `json:"state,omitempty"`
}

type checkpointItemJSON struct {
	Retain  *int         `json:"retain,omitempty"`
	Context *messageJSON `json:"context,omitempty"`
}

type checkpointPlanJSON struct {
	BoundaryEntryID string               `json:"boundary_entry_id"`
	Items           []checkpointItemJSON `json:"items"`
}

type planOutputJSON struct {
	Kind       string              `json:"kind"`
	Model      *modelPlanJSON      `json:"model,omitempty"`
	Checkpoint *checkpointPlanJSON `json:"checkpoint,omitempty"`
}

func planOutputFrom(v harness.PlanOutput) (planOutputJSON, error) {
	out := planOutputJSON{Kind: string(v.Kind)}
	if v.Model != nil {
		messages, err := messagesRequired(v.Model.Request.Messages)
		if err != nil {
			return planOutputJSON{}, err
		}
		modelPlan := modelPlanJSON{
			Request: modelRequestJSON{
				Messages: messages,
				Tools:    toolDefinitionsFrom(v.Model.Request.Tools),
			},
			State: JSONBytes(v.Model.State),
		}
		out.Model = &modelPlan
	}
	if v.Checkpoint != nil {
		checkpoint := checkpointPlanJSON{
			BoundaryEntryID: v.Checkpoint.BoundaryEntryID,
			Items:           make([]checkpointItemJSON, len(v.Checkpoint.Items)),
		}
		for i, item := range v.Checkpoint.Items {
			checkpoint.Items[i] = checkpointItemJSON{Retain: item.Retain}
			if item.Context != nil {
				message, err := messageFrom(*item.Context)
				if err != nil {
					return planOutputJSON{}, err
				}
				checkpoint.Items[i].Context = &message
			}
		}
		out.Checkpoint = &checkpoint
	}
	return out, nil
}

func planOutputValue(v planOutputJSON) harness.PlanOutput {
	out := harness.PlanOutput{Kind: harness.PlanKind(v.Kind)}
	if v.Model != nil {
		plan := harness.ModelPlan{
			Request: model.Request{
				Messages: messagesValue(v.Model.Request.Messages),
				Tools:    toolDefinitionsValue(v.Model.Request.Tools),
			},
			State: append(json.RawMessage(nil), v.Model.State...),
		}
		out.Model = &plan
	}
	if v.Checkpoint != nil {
		checkpoint := harness.CheckpointPlan{
			BoundaryEntryID: v.Checkpoint.BoundaryEntryID,
			Items:           make([]harness.CheckpointItem, len(v.Checkpoint.Items)),
		}
		for i, item := range v.Checkpoint.Items {
			checkpoint.Items[i] = harness.CheckpointItem{Retain: item.Retain}
			if item.Context != nil {
				message := messageValue(*item.Context)
				checkpoint.Items[i].Context = &message
			}
		}
		out.Checkpoint = &checkpoint
	}
	return out
}

// EncodePlanParams encodes one context strategy plan input.
func EncodePlanParams(v harness.PlanInput) (json.RawMessage, error) {
	encoded, err := planInputFrom(v)
	if err != nil {
		return nil, err
	}
	return marshalValue(encoded)
}

// DecodePlanParams decodes one context strategy plan input.
func DecodePlanParams(raw json.RawMessage) (harness.PlanInput, error) {
	var wire planInputJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return harness.PlanInput{}, err
	}
	return planInputValue(wire), nil
}

type modelOutputJSON struct {
	Status  string              `json:"status"`
	Source  modelRefJSON        `json:"source"`
	Message *messageJSON        `json:"message,omitempty"`
	Usage   *harness.UsageCount `json:"usage,omitempty"`
	Detail  string              `json:"detail"`
}

func modelOutputFrom(v model.Output) (modelOutputJSON, error) {
	source, err := modelRefFrom(v.Source)
	if err != nil {
		return modelOutputJSON{}, err
	}
	out := modelOutputJSON{
		Status: string(v.Status),
		Source: source,
		Usage:  usageFrom(v.Usage),
		Detail: v.Detail,
	}
	if v.Message != nil {
		message, err := messageFrom(*v.Message)
		if err != nil {
			return modelOutputJSON{}, err
		}
		out.Message = &message
	}
	return out, nil
}

func modelOutputValue(v modelOutputJSON) model.Output {
	out := model.Output{
		Status: model.OutputStatus(v.Status),
		Source: modelRefValue(v.Source),
		Usage:  usageValue(v.Usage),
		Detail: v.Detail,
	}
	if v.Message != nil {
		message := messageValue(*v.Message)
		out.Message = &message
	}
	return out
}

type consumeInputJSON struct {
	Input  planInputJSON   `json:"input"`
	State  JSONBytes       `json:"state,omitempty"`
	Output modelOutputJSON `json:"output"`
}

// EncodeConsumeParams encodes one context strategy consume input.
func EncodeConsumeParams(v harness.ConsumeInput) (json.RawMessage, error) {
	input, err := planInputFrom(v.Input)
	if err != nil {
		return nil, err
	}
	output, err := modelOutputFrom(v.Output)
	if err != nil {
		return nil, err
	}
	return marshalValue(consumeInputJSON{Input: input, State: JSONBytes(v.State), Output: output})
}

// DecodeConsumeParams decodes one context strategy consume input.
func DecodeConsumeParams(raw json.RawMessage) (harness.ConsumeInput, error) {
	var wire consumeInputJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return harness.ConsumeInput{}, err
	}
	return harness.ConsumeInput{
		Input:  planInputValue(wire.Input),
		State:  append(json.RawMessage(nil), wire.State...),
		Output: modelOutputValue(wire.Output),
	}, nil
}

// EncodePlanResult encodes one context strategy plan or consume result.
func EncodePlanResult(v harness.PlanOutput) (json.RawMessage, error) {
	encoded, err := planOutputFrom(v)
	if err != nil {
		return nil, err
	}
	return marshalValue(encoded)
}

// DecodePlanResult decodes one context strategy plan or consume result.
func DecodePlanResult(raw json.RawMessage) (harness.PlanOutput, error) {
	var wire planOutputJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return harness.PlanOutput{}, err
	}
	return planOutputValue(wire), nil
}

type failureJSON struct {
	Code    string     `json:"code"`
	Message string     `json:"message"`
	Data    *JSONBytes `json:"data,omitempty"`
	Partial *JSONBytes `json:"partial,omitempty"`
}

type serviceResultJSON struct {
	Value   *JSONBytes   `json:"value,omitempty"`
	Failure *failureJSON `json:"failure,omitempty"`
}

// EncodeServiceResult encodes one result envelope. Exactly one of the success
// value and the failure arm is present; failure data and partial payloads use
// the common zero-length-is-absent rule.
func EncodeServiceResult(v extension.Result) (json.RawMessage, error) {
	if err := extension.ValidateResultEnvelope(v); err != nil {
		return nil, fmt.Errorf("worker: result: %w", err)
	}
	out := serviceResultJSON{}
	if v.Value != nil {
		value := JSONBytes(v.Value)
		out.Value = &value
	}
	if v.Failure != nil {
		failure := &failureJSON{Code: v.Failure.Code, Message: v.Failure.Message}
		if len(v.Failure.Data) != 0 {
			data := JSONBytes(v.Failure.Data)
			failure.Data = &data
		}
		if len(v.Failure.Partial) != 0 {
			partial := JSONBytes(v.Failure.Partial)
			failure.Partial = &partial
		}
		out.Failure = failure
	}
	return marshalValue(out)
}

// DecodeServiceResult decodes one result envelope. Failure data and partial
// payloads use the common zero-length-is-absent rule. Content validity belongs
// to the accepting boundary.
func DecodeServiceResult(raw json.RawMessage) (extension.Result, error) {
	var wire serviceResultJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return extension.Result{}, err
	}
	out := extension.Result{}
	if wire.Value != nil {
		out.Value = append(json.RawMessage{}, (*wire.Value)...)
	}
	if wire.Failure != nil {
		out.Failure = &extension.Failure{Code: wire.Failure.Code, Message: wire.Failure.Message}
		if wire.Failure.Data != nil && len(*wire.Failure.Data) != 0 {
			out.Failure.Data = append(json.RawMessage{}, (*wire.Failure.Data)...)
		}
		if wire.Failure.Partial != nil && len(*wire.Failure.Partial) != 0 {
			out.Failure.Partial = append(json.RawMessage{}, (*wire.Failure.Partial)...)
		}
	}
	if err := extension.ValidateResultEnvelope(out); err != nil {
		return extension.Result{}, fmt.Errorf("worker: result: %w", err)
	}
	return out, nil
}

func usageFrom(v *model.Usage) *harness.UsageCount {
	if v == nil {
		return nil
	}
	return &harness.UsageCount{InputTokens: int64(v.InputTokens), CachedInputTokens: int64(v.CachedInputTokens), OutputTokens: int64(v.OutputTokens)}
}

func usageValue(v *harness.UsageCount) *model.Usage {
	if v == nil {
		return nil
	}
	return &model.Usage{InputTokens: int(v.InputTokens), CachedInputTokens: int(v.CachedInputTokens), OutputTokens: int(v.OutputTokens)}
}

func marshalValue(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func unmarshalValue(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return fmt.Errorf("worker: missing JSON value")
	}
	if err := rejectNullTokens(raw); err != nil {
		return fmt.Errorf("worker: invalid JSON value: %w", err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("worker: invalid JSON value: %w", err)
	}
	return nil
}
