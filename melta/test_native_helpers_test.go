package melta

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

const testConfigSchema = `{"type":"object","properties":{"enabled":{"type":"boolean"}},"required":["enabled"],"additionalProperties":true}`

// testNative builds one author extension with the shared fixture manifest.
func testNative(id string, activate func(context.Context, extension.API) (func(context.Context) error, error)) extension.Extension {
	return extension.Extension{
		Manifest: extension.Manifest{ID: id, Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{}`)},
		Activate: activate,
	}
}

// testServiceCapability is one fixture service named name with a single
// "call" service-IO method answered by testServiceHandler.
func testServiceCapability(name string) extension.Service {
	return extension.Service{Name: name, Methods: []extension.RegisteredMethod{{
		Method:  extension.Method{Name: "call", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
		Handler: testServiceHandler(),
	}}}
}

// testServiceHandler is the shared fixture service handler.
func testServiceHandler() extension.AuthorHandler {
	return func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
		return extension.Result{Value: json.RawMessage(`{}`)}, nil
	}
}

// testToolNative registers one tool named id backed by the given registered
// implementation, advertised with the shared testDescribeTool definition.
func testToolNative(id string, tool extension.RegisteredTool) extension.Extension {
	return testNative(id, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool(id), tool, nil)
		return nil, err
	})
}

// testDescribeTool is the shared fixture tool definition advertised for id.
func testDescribeTool(id string) model.ToolDefinition {
	return model.ToolDefinition{Name: id, Description: id, Parameters: json.RawMessage(`{"type":"object"}`)}
}

// testRegisteredTool is the shared fixture RegisteredTool: it normalizes
// arguments unchanged, prepares an immediate success outcome, and executes an
// empty outcome.
type testRegisteredTool struct{ Name string }

func (t testRegisteredTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t testRegisteredTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ok"}}}
}

func (t testRegisteredTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess, Content: "ok"}}
}
