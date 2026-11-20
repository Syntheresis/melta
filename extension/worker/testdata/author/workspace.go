package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type workspaceTool struct{}

func (workspaceTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (workspaceTool) Prepare(ctx context.Context, call extension.AuthorContext, _ model.ToolCall) extension.ToolPlan {
	type binding struct {
		Binding string `json:"binding"`
		Path    string `json:"canonical_path"`
	}
	var targets []binding
	for _, path := range []string{"file", "sibling"} {
		arg, _ := json.Marshal(map[string]string{"path": path})
		result, err := call.Call(ctx, "melta.workspace", "bind", arg)
		if err != nil {
			return failPlan(err)
		}
		var target binding
		if err := json.Unmarshal(result.Value, &target); err != nil {
			return failPlan(err)
		}
		targets = append(targets, target)
	}
	data, _ := json.Marshal(targets)
	return extension.ToolPlan{Data: data, Permissions: []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: targets[0].Path}}}
}

func failPlan(err error) extension.ToolPlan {
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}}
}

func (workspaceTool) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	fail := func(err error) harness.ToolOutcome {
		return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}
	}
	var targets []struct {
		Binding string `json:"binding"`
	}
	if err := json.Unmarshal(plan.Data, &targets); err != nil || len(targets) != 2 {
		return fail(errors.New("invalid targets"))
	}
	arg := func(binding string) json.RawMessage {
		raw, _ := json.Marshal(map[string]string{"binding": binding})
		return raw
	}
	result, err := call.Call(ctx, "workspace-relay", "read", arg(targets[0].Binding))
	if err != nil || string(result.Value) != `{"bytes":"YmVmb3Jl"}` {
		return fail(errors.New("precondition read failed"))
	}
	if _, err := call.Call(ctx, "melta.workspace", "read", arg(targets[1].Binding)); err == nil {
		return fail(errors.New("sibling read accepted"))
	}
	if _, err := call.Call(ctx, "melta.workspace", "read", arg("forged")); err == nil {
		return fail(errors.New("unbound read accepted"))
	}
	write := func(createOnly bool) json.RawMessage {
		raw, _ := json.Marshal(map[string]any{"binding": targets[0].Binding, "bytes": "YWZ0ZXI=", "create_only": createOnly})
		return raw
	}
	unbound, _ := json.Marshal(map[string]any{"binding": "forged", "bytes": "YWZ0ZXI=", "create_only": false})
	if _, err := call.Call(ctx, "melta.workspace", "write", unbound); err == nil {
		return fail(errors.New("unbound write accepted"))
	}
	sibling, _ := json.Marshal(map[string]any{"binding": targets[1].Binding, "bytes": "YWZ0ZXI=", "create_only": false})
	if _, err := call.Call(ctx, "melta.workspace", "write", sibling); err == nil {
		return fail(errors.New("sibling write accepted"))
	}
	if _, err := call.Call(ctx, "melta.workspace", "write", write(true)); err == nil {
		return fail(errors.New("create-only overwrite accepted"))
	}
	result, err = call.Call(ctx, "melta.workspace", "write", write(false))
	if err != nil || string(result.Value) != `{"outcome":"completed"}` {
		return fail(errors.New("write failed"))
	}
	if os.Getenv("AUTHOR_FIXTURE_LOSE_REPLY") == "1" {
		os.Exit(17)
	}
	return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess}, Evidence: harness.ExecutionObserved}
}

func workspaceFixture() extension.Extension {
	return extension.Extension{
		Manifest: extension.Manifest{ID: "workspace-worker", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{}`), Services: []string{"melta.workspace"}},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			if _, err := api.RegisterService(extension.Service{Name: "workspace-relay", Methods: []extension.RegisteredMethod{{
				Method: extension.Method{Name: "read", Mode: extension.ModeServiceIO, Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
				Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return call.Call(ctx, "melta.workspace", "read", args)
				},
			}}}, nil); err != nil {
				return nil, err
			}
			_, err := api.RegisterTool(model.ToolDefinition{Name: "workspace-worker", Parameters: json.RawMessage(`{}`)}, workspaceTool{}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachAuthorization(func(ctx context.Context, call extension.AuthorContext, _ extension.AuthorizationInput) (*string, error) {
				result, err := call.Call(ctx, "melta.workspace", "bind", []byte(`{"path":"file"}`))
				if err != nil {
					return nil, err
				}
				var target struct {
					Binding string `json:"binding"`
				}
				if err := json.Unmarshal(result.Value, &target); err != nil {
					return nil, err
				}
				args, _ := json.Marshal(map[string]string{"binding": target.Binding})
				if _, err := call.Call(ctx, "melta.workspace", "read", args); err == nil {
					return nil, errors.New("restriction read accepted")
				}
				return nil, nil
			}, nil)
			return nil, err
		},
	}
}
