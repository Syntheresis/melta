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

func (t *fixtureTool) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	if !t.fresh {
		if _, err := call.Call(ctx, "gate", "park", json.RawMessage(`{}`)); err != nil {
			return extension.ToolResult{}, err
		}
		if t.remove {
			if err := t.api.Remove(*t.handle); err != nil {
				return extension.ToolResult{}, err
			}
			return extension.ToolResult{Content: "old handler"}, nil
		}
		if _, err := t.api.RegisterTool(model.ToolDefinition{Name: "read_file", Parameters: json.RawMessage(`{}`)}, (&fixtureTool{fresh: true}).run, *t.handle); err == nil {
			return extension.ToolResult{}, errors.New("reserved replacement accepted")
		}
		replacement, err := t.api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, (&fixtureTool{fresh: true}).run, *t.handle)
		if err != nil {
			return extension.ToolResult{}, err
		}
		*t.handle = replacement
		return extension.ToolResult{Content: "old handler"}, nil
	}
	return extension.ToolResult{}, errors.New("incorrect handler")
}

func main() {
	if mode := os.Getenv("AUTHOR_FIXTURE_MODE"); mode == "workspace" {
		if err := worker.Serve(context.Background(), workspaceFixture(), os.Stdin, os.Stdout); err != nil {
			os.Exit(1)
		}
		return
	}
	if os.Getenv("AUTHOR_FIXTURE_SECRET") != "" {
		os.Exit(18)
	}
	e := extension.Extension{
		Manifest: extension.Manifest{ID: "author-fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`), Services: []string{extension.ContractCommands}},
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
					Method: extension.Method{Name: "relay", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
					Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
						return call.Call(ctx, "host-service", "echo", args)
					},
				},
				{
					Method: extension.Method{Name: "crash", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
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
			handle, err = api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, tool.run, nil)
			if err != nil {
				return nil, err
			}
			var removable extension.Registration
			remover := &fixtureTool{api: api, handle: &removable, remove: true}
			removable, err = api.RegisterTool(model.ToolDefinition{Name: "removable-tool", Parameters: json.RawMessage(`{}`)}, remover.run, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterStrategy(harness.StrategyBinding{ID: "worker-strategy", Strategy: fixtureStrategy{}}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachArguments(func(_ context.Context, _ extension.AuthorContext, call model.ToolCall) (extension.ArgumentsResult, error) {
				return extension.ArgumentsResult{Arguments: append(json.RawMessage(nil), call.Arguments...)}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachRequest(func(_ context.Context, _ extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
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
			_, err = api.AttachToolCheck(func(_ context.Context, _ extension.AuthorContext, input model.ToolCall) (*string, error) {
				if string(input.Arguments) != string([]byte{0xff, '{'}) {
					return nil, errors.New("check arguments changed")
				}
				refusal := "denied by worker"
				return &refusal, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterOperation(extension.Operation{
				Target: extension.ExposedTargetHarness,
				Method: extension.Method{Name: "operation", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
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
