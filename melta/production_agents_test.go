package melta

// Production-owner scenarios for the shipped Agents extension and the
// baseline host through public Open, the real Harness and the real model
// wire. Each scenario names the settled contract it exercises end to end.

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// TestHostAndAgentsModelWireModel proves the two model owners stay
// distinct through public Open: an ordinary create runs the host model A, while
// the Agents extension's selected create runs its primary definition's model B.
// Both are checked at the committed capture and in the actual HTTP request
// model (the provider-stripped model component on the wire), so neither path
// can silently bind the other's choice.
func TestHostAndAgentsModelWireModel(t *testing.T) {
	e := newOwnerEnv(t)
	writeServiceFile(t, e.configPath, `{"providers":{"prov":`+nominalProvMember(e.server.URL, "",
		`{"a":{"name":"A","context_window":262144},"b":{"name":"B","context_window":262144}}`)+`}}`)
	writeAgentDefinitions(t, e.configPath, `{"primary":{"model":"prov/b","system_prompt":"simple"}}`)
	e.server.setScript(func(context.Context, string) []string { return textTurnEvents("done") })

	host := model.ModelRef{Provider: "prov", Model: "a"}
	fromAgents := model.ModelRef{Provider: "prov", Model: "b"}
	opts := e.options()
	opts.Defaults = harness.SessionInputs{Model: &host}
	ctx := context.Background()
	r, err := open(ctx, opts)
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, r.Close(ctx)) }()
	caller := testCaller(r)

	ordinary, err := r.CreateSession(ctx, caller, e.dataDir, nil)
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, ordinary.SessionId, "op-host", "host turn")
	hostRecord := awaitOperation(t, r, ordinary.SessionId, "op-host", harness.OperationSuccess)
	testassert.Require(t, hostRecord.Admission.Execution.Model == host, "ordinary admission model = %s, want the host default %s", hostRecord.Admission.Execution.Model.String(), host.String())

	arguments := json.RawMessage(`{}`)
	workspace := e.dataDir
	created, err := r.InvokeExtension(ctx, caller, protocol.ExtensionInvocationRequest{
		ExtensionId: agents.ExtensionID, Operation: "create", RequestId: "accept-create",
		Target:    protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace},
		Arguments: &arguments,
	})
	testassert.NoError(t, err)
	testassert.Require(t, created.Failure == nil, "Agents create failure: %+v", created.Failure)
	var receipt struct {
		SessionID string `json:"session_id"`
	}
	testassert.NoError(t, json.Unmarshal(created.Value, &receipt))
	testassert.Require(t, receipt.SessionID != "", "Agents create receipt = %s, want a session id", created.Value)

	submitThroughRuntime(t, r, receipt.SessionID, "op-primary", "agents turn")
	agentsRecord := awaitOperation(t, r, receipt.SessionID, "op-primary", harness.OperationSuccess)
	testassert.Require(t, agentsRecord.Admission.Execution.Model == fromAgents, "Agents admission model = %s, want the primary definition's %s", agentsRecord.Admission.Execution.Model.String(), fromAgents.String())

	testassert.Require(t, e.server.requests() == 2, "model requests = %d, want one turn per admission", e.server.requests())
	for i, row := range []struct{ text, model string }{{"host turn", "a"}, {"agents turn", "b"}} {
		body := e.server.bodyAt(i)
		testassert.Require(t, decodeWireChatBody(t, body).Model == row.model && lastUserText(body) == row.text, "request %d model/user = %q/%q, want %q/%q", i, decodeWireChatBody(t, body).Model, lastUserText(body), row.model, row.text)
	}
}

// TestChoiceChangeWhileOperationParked proves one Session's stored
// model choice may change while an Operation is parked: the running Operation
// keeps the capture it was admitted with, and the next admission resolves the
// changed choice at admission and on the wire.
func TestChoiceChangeWhileOperationParked(t *testing.T) {
	e := newOwnerEnv(t)
	writeServiceFile(t, e.configPath, `{"providers":{"prov":`+nominalProvMember(e.server.URL, "",
		`{"m":{"name":"M","context_window":262144},"m2":{"name":"M2","context_window":262144}}`)+`}}`)
	writeAgentDefinitions(t, e.configPath, `{"solo":{"system_prompt":"simple"}}`)
	e.fixtureStore(storage.NewMemory())
	e.server.setScript(func(context.Context, string) []string { return textTurnEvents("done") })

	first := model.ModelRef{Provider: "prov", Model: "m"}
	second := model.ModelRef{Provider: "prov", Model: "m2"}
	ctx := context.Background()
	opts := e.options()
	opts.Defaults = harness.SessionInputs{Model: &first}
	r, err := open(ctx, opts)
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, r.Close(ctx)) }()
	session, err := r.createSession(ctx, e.dataDir, "solo")
	testassert.NoError(t, err)
	sessionID := session.Identity.SessionID

	gate := make(chan struct{})
	e.server.setHold(gate)
	release := sync.OnceFunc(func() { close(gate) })
	defer release()

	submitThroughRuntime(t, r, sessionID, "op-running", "running")
	awaitModelArrival(t, e)
	testassert.Require(t, decodeWireChatBody(t, e.server.bodyAt(0)).Model == first.Model, "parked request model = %q, want the stored %s", decodeWireChatBody(t, e.server.bodyAt(0)).Model, first.Model)

	_, err = r.SetSessionModel(ctx, testCaller(r), sessionID, second.String())
	testassert.NoError(t, err)
	running := readOperation(t, r, sessionID, "op-running")
	testassert.Require(t, running.Admission.Execution.Model == first, "running capture = %s, want the admission-time %s after the stored change", running.Admission.Execution.Model.String(), first.String())

	release()
	awaitOperation(t, r, sessionID, "op-running", harness.OperationSuccess)
	awaitIdleSession(t, r, sessionID)
	submitThroughRuntime(t, r, sessionID, "op-next", "next")
	next := awaitOperation(t, r, sessionID, "op-next", harness.OperationSuccess)
	testassert.Require(t, next.Admission.Execution.Model == second, "next admission model = %s, want the changed %s", next.Admission.Execution.Model.String(), second.String())
	testassert.Require(t, e.server.requests() == 2, "model requests = %d, want the parked turn and its successor", e.server.requests())
	testassert.Require(t, decodeWireChatBody(t, e.server.bodyAt(1)).Model == second.Model, "next request model = %q, want %s", decodeWireChatBody(t, e.server.bodyAt(1)).Model, second.Model)
}

// TestForkCopiesChoicesAndStateColdAfterAgentsRemoval proves the
// removed extension's Sessions stay readable and forkable without its code: a
// selected create and admission run with Agents, the owner is restarted with
// Agents disabled, and the cold source still forks a destination that carries
// the source's stored choices and extension state.
func TestForkCopiesChoicesAndStateColdAfterAgentsRemoval(t *testing.T) {
	e := newOwnerEnv(t)
	e.fixtureStore(storage.NewMemory())
	configDocument := func(enabled bool) string {
		section := `{"enabled":false,"definitions":{"primary":{"model":"prov/b","system_prompt":"simple"}}}`
		if enabled {
			section = `{"enabled":true,"definitions":{"primary":{"model":"prov/b","system_prompt":"simple"}}}`
		}
		return `{"providers":{"prov":` + nominalProvMember(e.server.URL, "",
			`{"b":{"name":"B","context_window":262144}}`) + `},"extensions":{"agents":` + section + `}}`
	}
	writeServiceFile(t, e.configPath, configDocument(true))
	e.server.setScript(func(context.Context, string) []string { return textTurnEvents("done") })

	ctx := context.Background()
	r, err := e.open(ctx)
	testassert.NoError(t, err)
	arguments := json.RawMessage(`{}`)
	workspace := e.dataDir
	created, err := r.InvokeExtension(ctx, testCaller(r), protocol.ExtensionInvocationRequest{
		ExtensionId: agents.ExtensionID, Operation: "create", RequestId: "accept-fork-create",
		Target:    protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace},
		Arguments: &arguments,
	})
	testassert.NoError(t, err)
	testassert.Require(t, created.Failure == nil, "Agents create failure: %+v", created.Failure)
	var receipt struct {
		SessionID string `json:"session_id"`
	}
	testassert.NoError(t, json.Unmarshal(created.Value, &receipt))
	sourceID := receipt.SessionID
	testassert.Require(t, sourceID != "", "Agents create receipt = %s, want a session id", created.Value)
	submitThroughRuntime(t, r, sourceID, "op-source", "source turn")
	awaitOperation(t, r, sourceID, "op-source", harness.OperationSuccess)
	awaitIdleSession(t, r, sourceID)

	selectedModel := protocol.ModelRef("prov/b")
	selectedPrompt := "stored prompt"
	selectedTools := []string{}
	_, err = r.SetSessionInputs(ctx, testCaller(r), sourceID, protocol.SetSessionInputsRequest{Inputs: protocol.SessionInputs{
		Model: &selectedModel, Prompt: &selectedPrompt, Tools: &selectedTools,
	}})
	testassert.NoError(t, err)
	sourceHeader := headerThroughRuntime(t, r, sourceID)
	sourceStates := productionSessionStates(t, r, sourceID)
	testassert.Require(t, sourceHeader.Inputs.Model != nil && *sourceHeader.Inputs.Model == model.ModelRef{Provider: "prov", Model: "b"} && sourceHeader.Inputs.Prompt != nil && *sourceHeader.Inputs.Prompt == selectedPrompt && sourceHeader.Inputs.Tools != nil && len(sourceHeader.Inputs.Tools) == 0, "source choices = %+v, want the stored model, prompt and present empty tools", sourceHeader.Inputs)
	testassert.Require(t, string(sourceStates[agents.ExtensionID]) == `{"agent":"primary"}`, "source Agents state = %q, want the selected create's state", sourceStates[agents.ExtensionID])
	testassert.NoError(t, r.Close(ctx))

	writeServiceFile(t, e.configPath, configDocument(false))
	cold, err := e.open(ctx)
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, cold.Close(ctx)) }()
	caller := testCaller(cold)

	sourcePage, err := cold.GetHistory(ctx, caller, sourceID, nil)
	testassert.NoError(t, err)
	testassert.Require(t, productionHistoryHasText(sourcePage, "source turn"), "cold source history = %d items, want the committed source input without Agents", len(sourcePage.Items))

	sourceView := sessionViewThroughRuntime(t, cold, sourceID)
	boundary := projectItemID(sourceID, commandUserInputEntry(t, sourceView, "op-source"))
	forked, err := cold.ForkSession(ctx, caller, sourceID, protocol.ForkRequest{
		BoundaryItemId: boundary, OperationId: "op-fork",
		Content: []protocol.ContentPart{textContentPart(t, "fork input", nil)},
	})
	testassert.NoError(t, err)
	forkedID := forked.Session.SessionId
	testassert.Require(t, forkedID != "" && forkedID != sourceID, "fork destination = %q, want an independent Session", forkedID)
	awaitOperation(t, cold, forkedID, "op-fork", harness.OperationSuccess)

	forkedHeader := headerThroughRuntime(t, cold, forkedID)
	forkedStates := productionSessionStates(t, cold, forkedID)
	testassert.Require(t, reflect.DeepEqual(forkedHeader.Inputs, sourceHeader.Inputs), "forked choices = %+v, want the source's %+v", forkedHeader.Inputs, sourceHeader.Inputs)
	testassert.Require(t, string(forkedStates[agents.ExtensionID]) == string(sourceStates[agents.ExtensionID]), "forked Agents state = %q, want the source's %q", forkedStates[agents.ExtensionID], sourceStates[agents.ExtensionID])

	forkedPage, err := cold.GetHistory(ctx, caller, forkedID, nil)
	testassert.NoError(t, err)
	testassert.Require(t, productionHistoryHasText(forkedPage, "fork input"), "cold forked history = %d items, want the fork input without Agents", len(forkedPage.Items))
}

// productionSessionStates reads one Session's owned extension state through the
// production owner's real Harness.
func productionSessionStates(t *testing.T, r *Melta, sessionID string) map[string]harness.SessionStateValue {
	t.Helper()
	var states map[string]harness.SessionStateValue
	err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
		var err error
		states, err = h.SessionExtensionStates(ctx, sessionID)
		return err
	})
	testassert.Require(t, err == nil, "SessionExtensionStates(%s): %v", sessionID, err)
	return states
}

// productionHistoryHasText reports whether one projected history page carries a
// committed input item with exactly the given text.
func productionHistoryHasText(page protocol.HistoryPage, text string) bool {
	for _, item := range page.Items {
		input, err := item.AsInputItem()
		if err != nil {
			continue
		}
		for _, part := range input.Content {
			value, err := part.AsTextPart()
			if err == nil && value.Text == text {
				return true
			}
		}
	}
	return false
}
