package melta

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestServedStartUsesServingExtension(t *testing.T) {
	e := newOwnerEnv(t)
	for _, id := range []string{"startcaller", "startserver", "startplain"} {
		enableFixturePlugin(t, e.configPath, id)
	}
	writeAgentDefinitions(t, e.configPath, `{"start_agent":{"model":"prov/m","system_prompt":"simple","tools":["start_tool"]}}`)
	e.fixtureStore(storage.NewMemory())
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	entered := make(chan struct{}, 1)
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastUserText(body) == "served child" {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return textTurnEvents("child done")
		}
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-start", "start_tool", `{}`)
	})
	server := func(id, service string, grant bool) extension.Extension {
		manifest := registrarManifest(id)
		if grant {
			manifest.Services = []string{extension.ContractSession}
		}
		return extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterService(extension.Service{Name: service, Methods: []extension.RegisteredMethod{{
				Method: extension.ServiceMethod("start", `{}`, `{}`, "", ""),
				Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
					ref := model.ModelRef{Provider: "prov", Model: "m"}
					id, err := call.StartSession(ctx, extension.SessionStart{
						Inputs: harness.SessionInputs{Model: &ref}, InitialState: harness.SessionStateValue(`{"owner":"server"}`),
						Input: []model.ContentPart{{Kind: model.PartText, Text: "served child"}},
					})
					if err != nil {
						return extension.Result{}, err
					}
					value, err := json.Marshal(map[string]string{"session_id": id})
					return extension.Result{Value: value}, err
				},
			}}}, nil)
			return nil, err
		}}
	}
	ids := make(chan string, 1)
	caller := testNative("startcaller", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("start_tool"), func(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
			if _, err := call.Call(ctx, "start.plain", "start", json.RawMessage(`{}`)); !errors.Is(err, extension.ErrDenied) {
				return extension.ToolResult{}, errors.New("ungranted server started a session")
			}
			result, err := call.Call(ctx, "start.server", "start", json.RawMessage(`{}`))
			if err != nil {
				return extension.ToolResult{}, err
			}
			var body struct {
				SessionID string `json:"session_id"`
			}
			if err := json.Unmarshal(result.Value, &body); err != nil {
				return extension.ToolResult{}, err
			}
			ids <- body.SessionID
			return extension.ToolResult{Content: "started"}, nil
		}, nil)
		return nil, err
	})
	caller.Manifest.Services = []string{extension.ContractSession}
	ctx := context.Background()
	r, err := e.open(ctx, caller, server("startserver", "start.server", true), server("startplain", "start.plain", false))
	testassert.NoError(t, err)
	defer closeProjectionRuntime(r)
	session, err := r.createSession(ctx, e.dataDir, "start_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "served-start", "run")
	child := <-ids
	<-entered
	members, err := r.harness.SessionMembers(ctx, session.Identity.SessionID)
	testassert.NoError(t, err)
	found := false
	for _, member := range members {
		if member.ID == child && member.Kind == "session" && member.Starter == "startserver" {
			found = true
		}
	}
	testassert.Require(t, found, "members = %+v, want serving extension as starter", members)
	states, err := r.harness.SessionExtensionStates(ctx, child)
	testassert.NoError(t, err)
	testassert.Equal(t, string(states["startserver"]), `{"owner":"server"}`)
	_, wrong := states["startcaller"]
	testassert.Require(t, !wrong, "caller owns initial state: %+v", states)
	releaseOnce()
	awaitOperation(t, r, session.Identity.SessionID, "served-start", harness.OperationSuccess)
}
