package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/model"
)

type authorTask struct {
	definitions *Config
	settings    settings
}

func (t authorTask) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	if call.Session.OwnerSessionID != "" {
		return extension.ToolResult{}, errors.New("task is unavailable in an owned session")
	}
	var args struct {
		Prompt       *string `json:"prompt"`
		SubagentType *string `json:"subagent_type"`
	}
	if err := call.Decode(&args); err != nil {
		return extension.ToolResult{}, err
	}
	if args.Prompt == nil {
		return extension.ToolResult{}, errors.New("missing 'prompt' parameter")
	}
	if args.SubagentType == nil {
		return extension.ToolResult{}, errors.New("missing 'subagent_type' parameter")
	}
	definition, err := t.definitions.Resolve(*args.SubagentType)
	if err != nil || !definition.Subagent {
		return extension.ToolResult{}, fmt.Errorf("unknown subagent type %q", *args.SubagentType)
	}
	members, err := call.Members(ctx)
	if err != nil {
		return extension.ToolResult{}, err
	}
	count := 0
	for _, member := range members {
		if member.Kind == "session" && member.Starter == ExtensionID {
			count++
		}
	}
	if count >= t.settings.MaxConcurrent {
		return extension.ToolResult{}, errors.New("maximum concurrent tasks reached")
	}
	state, _ := json.Marshal(struct {
		Agent string `json:"agent"`
	}{definition.Name})
	id, err := call.StartSession(ctx, extension.SessionStart{
		InitialState: state, Input: []model.ContentPart{{Kind: model.PartText, Text: *args.Prompt}},
		Report: true, OutputLimit: t.settings.MaxOutputBytes,
	})
	if err != nil {
		if id != "" {
			return extension.ToolResult{}, fmt.Errorf("%w\nSession ID: %s", err, id)
		}
		return extension.ToolResult{}, err
	}
	return extension.ToolResult{Content: fmt.Sprintf(taskStartedTemplate, id)}, nil
}
