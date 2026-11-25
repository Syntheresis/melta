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
	var args map[string]any
	err = (extension.ToolCall{Arguments: input.Arguments}).Decode(&args)
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
		var args map[string]any
		err := (extension.ToolCall{Arguments: input.Arguments}).Decode(&args)
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
		var arguments map[string]any
		err := (extension.ToolCall{Arguments: input.Arguments}).Decode(&arguments)
		if err != nil {
			return nil, err
		}
		path, _ := arguments["path"].(string)
		file, err := call.StatFile(ctx, path)
		if err != nil {
			return nil, err
		}
		root, err := call.StatFile(ctx, writeDir)
		if err != nil {
			return nil, err
		}
		relative, err := filepath.Rel(root.Path, file.Path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
			return refuse("write target is outside the agent write root")
		}
	}
	return nil, nil
}
