package agents

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/model"
)

func (s authorInstance) arguments(_ context.Context, call extension.AuthorContext, input model.ToolCall) (extension.ArgumentsResult, error) {
	definition, err := s.definitions.Resolve(selected(call.State))
	if err != nil || !definition.Readonly || input.Name != "run_command" {
		return extension.ArgumentsResult{}, nil
	}
	args, err := decodeArguments(input.Arguments)
	if err != nil {
		return extension.ArgumentsResult{}, nil
	}
	command, ok := args["command"].(string)
	if !ok {
		return extension.ArgumentsResult{}, nil
	}
	rewritten, err := ReadOnlyCommand(command)
	if err != nil {
		reason := ReadOnlyRunCommandRejected
		return extension.ArgumentsResult{Refusal: &reason}, nil
	}
	args["command"] = rewritten
	if background, _ := args["background"].(bool); !background {
		delete(args, "timeout")
	}
	value, err := json.Marshal(args)
	return extension.ArgumentsResult{Arguments: value}, err
}

func (s authorInstance) check(ctx context.Context, call extension.AuthorContext, input model.ToolCall) (*string, error) {
	definition, err := s.definitions.Resolve(selected(call.State))
	if err != nil {
		return nil, nil
	}
	refuse := func(message string) (*string, error) { return &message, nil }
	if definition.Readonly && input.Name == "run_command" {
		args, err := decodeArguments(input.Arguments)
		if err != nil {
			return refuse(ReadOnlyRunCommandRejected)
		}
		command, _ := args["command"].(string)
		if !ReadOnlyPreparedCommand(command) {
			return refuse(ReadOnlyRunCommandRejected)
		}
		if background, _ := args["background"].(bool); !background {
			if _, ok := args["timeout"]; ok {
				return refuse("read-only command timeout changed")
			}
		}
	}
	writeDir := strings.TrimSpace(definition.WriteDir)
	if input.Name == "write_file" || input.Name == "edit_file" {
		if writeDir == "" {
			if definition.Readonly {
				return refuse("read-only agent cannot write files")
			}
			return nil, nil
		}
		arguments, err := decodeArguments(input.Arguments)
		if err != nil {
			return nil, err
		}
		path, _ := arguments["path"].(string)
		bind := func(path string) (string, *string, error) {
			args, _ := json.Marshal(map[string]string{"path": path})
			result, err := call.Call(ctx, "melta.workspace", "bind", args)
			if err != nil {
				return "", nil, err
			}
			if result.Failure != nil {
				return "", &result.Failure.Message, nil
			}
			var bound struct {
				CanonicalPath string `json:"canonical_path"`
			}
			if err := json.Unmarshal(result.Value, &bound); err != nil {
				return "", nil, err
			}
			return bound.CanonicalPath, nil, nil
		}
		file, refusal, err := bind(path)
		if err != nil || refusal != nil {
			return refusal, err
		}
		root, refusal, err := bind(writeDir)
		if err != nil || refusal != nil {
			return refusal, err
		}
		relative, err := filepath.Rel(root, file)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
			return refuse("write target is outside the agent write root")
		}
	}
	return nil, nil
}
