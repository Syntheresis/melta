package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/toolargs"
	"github.com/Syntheresis/melta/model"
)

type authorTask struct {
	definitions *Config
	settings    settings
}

func (authorTask) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return normalizeCallArguments(call, normalizeTaskArgs)
}

func (t authorTask) Prepare(ctx context.Context, call extension.AuthorContext, toolCall model.ToolCall) extension.ToolPlan {
	if call.Session.OwnerSessionID != "" {
		return immediateError(toolCall.ID, errors.New("task is unavailable in an owned session"))
	}
	args, err := toolargs.Decode(toolCall.Arguments)
	if err != nil {
		return immediateError(toolCall.ID, err)
	}
	text, ok := args["prompt"].(string)
	if !ok {
		return immediateError(toolCall.ID, errors.New("missing 'prompt' parameter"))
	}
	name, ok := args["subagent_type"].(string)
	if !ok {
		return immediateError(toolCall.ID, errors.New("missing 'subagent_type' parameter"))
	}
	definition, err := t.definitions.Resolve(name)
	if err != nil || !definition.Subagent {
		return immediateError(toolCall.ID, fmt.Errorf("unknown subagent type %q", name))
	}
	result, err := call.Call(ctx, "melta.session", "members", []byte(`{}`))
	if err != nil {
		return immediateError(toolCall.ID, err)
	}
	if result.Failure != nil {
		return immediateError(toolCall.ID, errors.New(result.Failure.Message))
	}
	var members []struct {
		Kind    string `json:"kind"`
		Starter string `json:"starter"`
	}
	if err := json.Unmarshal(result.Value, &members); err != nil {
		return immediateError(toolCall.ID, err)
	}
	count := 0
	for _, member := range members {
		if member.Kind == "session" && member.Starter == ExtensionID {
			count++
		}
	}
	if count >= t.settings.MaxConcurrent {
		return immediateError(toolCall.ID, errors.New("maximum concurrent tasks reached"))
	}
	state, _ := json.Marshal(struct {
		Agent string `json:"agent"`
	}{definition.Name})
	data, _ := json.Marshal(struct {
		CallID string `json:"call_id"`
	}{toolCall.ID})
	return extension.ToolPlan{
		Data: data,
		Start: &extension.SessionStart{
			Target: definition.Name, InitialState: state,
			Input:  []model.ContentPart{{Kind: model.PartText, Text: text}},
			Report: true, OutputLimit: t.settings.MaxOutputBytes,
		},
	}
}

func (authorTask) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	var data struct {
		CallID string `json:"call_id"`
	}
	if err := json.Unmarshal(plan.Data, &data); err != nil {
		return launchOutcome("", err)
	}
	result, err := call.Call(ctx, "melta.session", "start", []byte(`{}`))
	if err == nil && result.Failure != nil {
		err = &extension.CallError{Err: errors.New(result.Failure.Message), Partial: result.Failure.Partial}
	}
	if err != nil {
		return launchOutcomeWithKey(data.CallID, err, "session_id")
	}
	var started struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(result.Value, &started); err != nil {
		return launchOutcome(data.CallID, err)
	}
	return harness.ToolOutcome{Result: model.ToolResult{CallID: data.CallID, Status: model.ResultSuccess, Content: fmt.Sprintf(taskStartedTemplate, started.SessionID)}}
}
