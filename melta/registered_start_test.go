package melta

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/model"
)

type frozenStartProbe struct {
	refusal error
}

func (f frozenStartProbe) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (f frozenStartProbe) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Start: &extension.SessionStart{
		Target: "child/name", InitialState: []byte(` {"private":1} `),
		Input: []model.ContentPart{{Kind: model.PartText, Text: "original"}},
	}}
}

func (f frozenStartProbe) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	plan.Start.Target = "substituted"
	plan.Start.Input[0].Text = "substituted"
	plan.Start.InitialState[0] = 'x'
	if _, err := call.Call(ctx, "melta.session", "start", []byte(`{"target":"forged"}`)); err == nil {
		return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: "accepted forged arguments"}}
	}
	_, err := call.Call(ctx, "melta.session", "start", []byte(`{}`))
	if !errors.Is(err, f.refusal) {
		return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: "unexpected first result"}}
	}
	_, err = call.Call(ctx, "melta.session", "start", []byte(`{}`))
	if !errors.Is(err, harness.ErrConflict) {
		return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: "start reused"}}
	}
	return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess}}
}

func TestRegisteredStartOwnsFrozenPlan(t *testing.T) {
	ctx := context.Background()
	refusal := errors.New("prepare refused")
	var observed harness.PreparationRequest
	h, err := harness.New(ctx, harness.Dependencies{
		Storage: storage.NewMemory(),
		Prepare: func(_ context.Context, req harness.PreparationRequest) (harness.PreparedExecution, error) {
			observed = req
			return harness.PreparedExecution{}, refusal
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := h.CreateSession(ctx, harness.CreateSessionRequest{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	owner := &configurationService{}
	candidate := &nativeCandidate{owner: owner}
	activation := &nativeActivation{owner: owner, id: "ext/name", services: map[string]bool{"melta.session": true}}
	entry := &nativeRegistration{activation: activation, value: nativeTool{tool: frozenStartProbe{refusal: refusal}}}
	binding := &nativeToolBinding{nativeCaptureHold: nativeCaptureHold{candidate: candidate}, entry: entry}
	host := &composition{hosts: &hostServices{background: &backgroundBridge{h: h}}}
	tool := registeredExtensionTool{binding: binding, host: host}
	prepared := tool.Prepare(ctx, ToolContext{Session: parent.Identity, Workspace: parent.Identity.Workspace}, model.ToolCall{ID: "call", Name: "start"})
	if prepared.Execute == nil {
		t.Fatalf("prepared plan = %+v", prepared)
	}
	if outcome := prepared.Execute(ctx); outcome.Result.Status != model.ResultSuccess {
		t.Fatalf("execute = %+v", outcome)
	}
	if observed.Session.Identity.OwnerSessionID != parent.Identity.SessionID ||
		string(observed.Session.ExtensionState["ext/name"]) != ` {"private":1} ` ||
		len(observed.Content) != 1 || observed.Content[0].Text != "original" {
		t.Fatalf("preparation received substituted values: %+v", observed)
	}
}
