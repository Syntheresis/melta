package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/tool"
	"github.com/Syntheresis/melta/internal/toolargs"
	"github.com/Syntheresis/melta/model"
)

func (s authorInstance) arguments(_ context.Context, call extension.AuthorContext, input model.ToolCall) (extension.ArgumentsResult, error) {
	definition, err := s.definitions.Resolve(selected(call.State))
	if err != nil || !definition.Readonly || input.Name != "run_command" {
		return extension.ArgumentsResult{}, nil
	}
	args, err := toolargs.Decode(input.Arguments)
	if err != nil {
		return extension.ArgumentsResult{}, nil
	}
	command, ok := args["command"].(string)
	if !ok {
		return extension.ArgumentsResult{}, nil
	}
	rewritten, err := tool.ReadOnlyCommand(command)
	if err != nil {
		reason := tool.ReadOnlyRunCommandRejected
		return extension.ArgumentsResult{Refusal: &reason}, nil
	}
	args["command"] = rewritten
	if background, _ := args["background"].(bool); !background {
		delete(args, "timeout")
	}
	value, err := json.Marshal(args)
	return extension.ArgumentsResult{Arguments: value}, err
}

func (s authorInstance) authorize(ctx context.Context, call extension.AuthorContext, input extension.AuthorizationInput) (*string, error) {
	definition, err := s.definitions.Resolve(selected(call.State))
	if err != nil {
		return nil, nil
	}
	refuse := func(message string) (*string, error) { return &message, nil }
	if definition.Readonly && input.ToolName == "run_command" {
		args, err := toolargs.Decode(input.Arguments)
		if err != nil {
			return refuse(tool.ReadOnlyRunCommandRejected)
		}
		command, _ := args["command"].(string)
		if !tool.ReadOnlyPreparedCommand(command) {
			return refuse(tool.ReadOnlyRunCommandRejected)
		}
		if background, _ := args["background"].(bool); !background {
			var settings struct {
				CommandTimeout int `json:"command_timeout"`
			}
			if json.Unmarshal(input.ToolSettings, &settings) != nil || settings.CommandTimeout <= 0 {
				return refuse("read-only command timeout unavailable")
			}
			timeout, ok := args["timeout"].(json.Number)
			if !ok || timeout.String() != fmt.Sprint(settings.CommandTimeout) {
				return refuse("read-only command timeout changed")
			}
		}
	}
	writeDir := strings.TrimSpace(definition.WriteDir)
	for _, pair := range input.Permissions {
		if pair.Permission != harness.PermissionFileWrite {
			continue
		}
		if writeDir == "" {
			if definition.Readonly {
				return refuse("read-only agent cannot write files")
			}
			continue
		}
		args, _ := json.Marshal(struct {
			Path string `json:"path"`
		}{writeDir})
		bound, err := call.Call(ctx, "melta.workspace", "bind", args)
		if err != nil {
			return nil, err
		}
		if bound.Failure != nil {
			return refuse(bound.Failure.Message)
		}
		var root struct {
			CanonicalPath string `json:"canonical_path"`
		}
		if err := json.Unmarshal(bound.Value, &root); err != nil {
			return nil, err
		}
		relative, err := filepath.Rel(root.CanonicalPath, pair.Target)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
			return refuse("write target is outside the agent write root")
		}
	}
	return nil, nil
}
