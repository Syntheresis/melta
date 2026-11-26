package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/model"
)

func workspaceTool(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	result, err := call.Call(ctx, "workspace-relay", "read", json.RawMessage(`{"path":"file"}`))
	if err != nil {
		return extension.ToolResult{}, fmt.Errorf("relay read: %w", err)
	}
	if string(result.Value) != `{"bytes":"YmVmb3Jl"}` {
		return extension.ToolResult{}, errors.New("precondition read failed")
	}
	if _, err := call.ReadFile(ctx, "sibling"); err != nil {
		return extension.ToolResult{}, err
	}
	if err := call.WriteFile(ctx, "sibling", []byte("after")); err != nil {
		return extension.ToolResult{}, err
	}
	if err := call.CreateFile(ctx, "file", []byte("after")); err == nil {
		return extension.ToolResult{}, errors.New("create-only overwrite accepted")
	}
	if err := call.WriteFile(ctx, "file", []byte("after")); err != nil {
		return extension.ToolResult{}, err
	}
	if os.Getenv("AUTHOR_FIXTURE_LOSE_REPLY") == "1" {
		os.Exit(17)
	}
	return extension.ToolResult{}, nil
}

func workspaceFixture() extension.Extension {
	return extension.Extension{
		Manifest: extension.Manifest{ID: "workspace-worker", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{}`), Services: []string{"melta.workspace"}},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			if _, err := api.RegisterService(extension.Service{Name: "workspace-relay", Methods: []extension.RegisteredMethod{{
				Method: extension.Method{Name: "read", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
				Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return call.Call(ctx, "melta.workspace", "read", args)
				},
			}}}, nil); err != nil {
				return nil, err
			}
			_, err := api.RegisterTool(model.ToolDefinition{Name: "workspace-worker", Parameters: json.RawMessage(`{}`)}, workspaceTool, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachToolCheck(func(ctx context.Context, call extension.AuthorContext, _ model.ToolCall) (*string, error) {
				_, err := call.ReadFile(ctx, "file")
				return nil, err
			}, nil)
			return nil, err
		},
	}
}
