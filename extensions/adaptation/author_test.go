package adaptation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type requestAPI struct {
	extension.API
	hook     extension.AuthorRequestHook
	settings json.RawMessage
}

func (a *requestAPI) Settings() json.RawMessage {
	if a.settings != nil {
		return a.settings
	}
	return []byte(`{"enabled":true}`)
}
func (a *requestAPI) AttachRequest(_ extension.HookMode, h extension.AuthorRequestHook, _ extension.Registration) (extension.Registration, error) {
	a.hook = h
	return nil, nil
}

func TestAuthorRequestCapturedSettings(t *testing.T) {
	api := &requestAPI{settings: []byte(`{"enabled":true,"entries":[{"pattern":"m","append":["first"]}]}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	api.settings = []byte(`{"enabled":true,"entries":[{"pattern":"m","append":["later"]}]}`)
	input := extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{Purpose: harness.InvocationPurposeConversation, Model: model.ModelRef{Provider: "p", Model: "m"}, Prompt: "base"}}
	result, err := api.hook(context.Background(), extension.AuthorContext{Settings: api.settings}, input)
	if err != nil || result.Prompt == nil || *result.Prompt != "base\n\nfirst" {
		t.Fatalf("captured settings: %#v %v", result, err)
	}
}

func TestAuthorRequestComposition(t *testing.T) {
	api := &requestAPI{}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	in := extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{
		Purpose: harness.InvocationPurposeConversation,
		Model:   model.ModelRef{Provider: "p", Model: "gpt-5.5"},
		Prompt:  "base",
		Tools:   []model.ToolDefinition{{Name: "write_file"}, {Name: "read_file", Description: "keep"}},
		Catalog: []model.ToolDefinition{{Name: "write_file"}, {Name: "read_file"}, {Name: "not-advertised"}},
	}}
	in.PromptAssembled = true
	assembled, err := api.hook(context.Background(), extension.AuthorContext{}, in)
	if err != nil || assembled.Prompt == nil || *assembled.Prompt == "base" || len(assembled.Tools) != 1 || assembled.Tools[0].Name != "read_file" {
		t.Fatalf("assembled = %#v, %v", assembled, err)
	}
	in.PromptAssembled = false
	complete, err := api.hook(context.Background(), extension.AuthorContext{}, in)
	if err != nil || complete.Prompt != nil || len(complete.Tools) != 1 {
		t.Fatalf("complete = %#v, %v", complete, err)
	}
	for _, prompt := range []string{"simple prompt", ""} {
		in.Prompt = prompt
		complete, err = api.hook(context.Background(), extension.AuthorContext{}, in)
		if err != nil || complete.Prompt != nil {
			t.Fatalf("bundled guidance on complete prompt %q: %#v %v", prompt, complete, err)
		}
	}
	in.Purpose = harness.InvocationPurposeMaintenance
	in.PromptAssembled = true
	summary, err := api.hook(context.Background(), extension.AuthorContext{}, in)
	if err != nil || summary.Prompt != nil || summary.Tools != nil {
		t.Fatalf("summary = %#v, %v", summary, err)
	}
}
