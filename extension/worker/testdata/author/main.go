package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type fixtureTool struct {
	api    extension.API
	handle *extension.Registration
	fresh  bool
	remove bool
}

type fixtureStrategy struct{}

func (fixtureStrategy) Plan(harness.PlanInput) (harness.PlanOutput, error) {
	return harness.PlanOutput{Kind: harness.PlanNone}, nil
}
func (fixtureStrategy) Consume(harness.ConsumeInput) (harness.PlanOutput, error) {
	return harness.PlanOutput{Kind: harness.PlanNone}, nil
}

func (t *fixtureTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *fixtureTool) Prepare(ctx context.Context, call extension.AuthorContext, _ model.ToolCall) extension.ToolPlan {
	if !t.fresh {
		if _, err := call.Call(ctx, "gate", "park", json.RawMessage(`{}`)); err != nil {
			return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}}
		}
		if t.remove {
			if err := t.api.Remove(*t.handle); err != nil {
				return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}}
			}
			return extension.ToolPlan{Data: []byte("old-plan"), Start: &extension.SessionStart{Target: "raw/name", InitialState: []byte(`{"large":9007199254740993}`), Report: true}}
		}
		if _, err := t.api.RegisterTool(model.ToolDefinition{Name: "read_file", Parameters: json.RawMessage(`{}`)}, &fixtureTool{fresh: true}, *t.handle); err == nil {
			return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: "reserved replacement accepted"}}}
		}
		replacement, err := t.api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, &fixtureTool{fresh: true}, *t.handle)
		if err != nil {
			return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}}
		}
		*t.handle = replacement
	}
	return extension.ToolPlan{Data: []byte("old-plan"), Start: &extension.SessionStart{Target: "raw/name", InitialState: []byte(`{"large":9007199254740993}`), Report: true}}
}

func (t *fixtureTool) Execute(_ context.Context, _ extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	if t.fresh || string(plan.Data) != "old-plan" || plan.Start == nil || plan.Start.Target != "raw/name" || string(plan.Start.InitialState) != `{"large":9007199254740993}` || !plan.Start.Report {
		return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: "incorrect handler or plan"}}
	}
	return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess, Content: "old handler"}, Evidence: harness.ExecutionObserved}
}

func main() {
	if mode := os.Getenv("AUTHOR_FIXTURE_MODE"); mode == "workspace" || mode == "settings" {
		e := workspaceFixture()
		if mode == "settings" {
			e = settingsFixture()
		}
		if err := worker.Serve(context.Background(), e, os.Stdin, os.Stdout); err != nil {
			os.Exit(1)
		}
		return
	}
	if os.Getenv("AUTHOR_FIXTURE_SECRET") != "" {
		os.Exit(18)
	}
	e := extension.Extension{
		Manifest: extension.Manifest{ID: "author-fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
			if string(api.Settings()) == `{"fail":true}` {
				return nil, errors.New("activation refused")
			}
			if path := os.Getenv("AUTHOR_FIXTURE_MARKER"); path != "" {
				if err := os.WriteFile(path, []byte("activated"), 0600); err != nil { // #nosec G703 -- The test harness supplies a marker in its private t.TempDir to observe entry execution.
					return nil, err
				}
			}
			result, err := api.Call(ctx, "host-service", "echo", json.RawMessage(`{"entry":true}`))
			if err != nil {
				return nil, err
			}
			if string(result.Value) != `{"entry":true}` {
				return nil, errors.New("entry service returned unexpected value")
			}
			_, err = api.RegisterService(extension.Service{Name: "worker-service", Methods: []extension.RegisteredMethod{
				{
					Method: extension.Method{Name: "relay", Mode: extension.ModePure, Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
					Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
						return call.Call(ctx, "host-service", "echo", args)
					},
				},
				{
					Method: extension.Method{Name: "crash", Mode: extension.ModeServiceIO, Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
					Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
						os.Exit(17)
						return extension.Result{}, nil
					},
				},
			}}, nil)
			if err != nil {
				return nil, err
			}
			var handle extension.Registration
			tool := &fixtureTool{api: api, handle: &handle}
			handle, err = api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, tool, nil)
			if err != nil {
				return nil, err
			}
			var removable extension.Registration
			remover := &fixtureTool{api: api, handle: &removable, remove: true}
			removable, err = api.RegisterTool(model.ToolDefinition{Name: "removable-tool", Parameters: json.RawMessage(`{}`)}, remover, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterStrategy(harness.StrategyBinding{ID: "worker-strategy", Strategy: fixtureStrategy{}}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachArguments(extension.HookModePure, func(_ context.Context, _ extension.AuthorContext, call model.ToolCall) (extension.ArgumentsResult, error) {
				return extension.ArgumentsResult{Arguments: append(json.RawMessage(nil), call.Arguments...)}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachRequest(extension.HookModePure, func(_ context.Context, _ extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
				prompt := "worker prompt"
				return harness.RequestConsequence{Prompt: &prompt}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachResult(func(_ context.Context, _ extension.AuthorContext, input harness.ToolOutputInput) (string, error) {
				return "worker: " + input.Content, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachAuthorization(func(_ context.Context, _ extension.AuthorContext, input extension.AuthorizationInput) (*string, error) {
				if string(input.Arguments) != string([]byte{0xff, '{'}) {
					return nil, errors.New("authorization arguments changed")
				}
				refusal := "denied by worker"
				return &refusal, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterOperation(extension.Operation{
				Target: extension.ExposedTargetHarness,
				Method: extension.Method{Name: "operation", Mode: extension.ModePure, Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
				Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return extension.Result{Value: args}, nil
				},
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachAdmission(func(_ context.Context, _ extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
				if input.Candidate.Instructions == "skip" {
					value := "unread"
					return extension.AdmissionResult{Refusal: &value}, nil
				}
				prompt, err := input.Prompt.Default()
				if err != nil {
					return extension.AdmissionResult{}, err
				}
				return extension.AdmissionResult{Refusal: &prompt}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterCleanup(func(_ context.Context, _ extension.AuthorContext, session harness.SessionIdentity) error {
				if session.SessionID != "session" {
					return errors.New("unexpected session")
				}
				return nil
			}, nil)
			return nil, err
		},
	}
	if err := worker.Serve(context.Background(), e, os.Stdin, os.Stdout); err != nil {
		os.Exit(1)
	}
}
