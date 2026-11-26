package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// nativeCandidateFor activates every author as one candidate and fails the
// test when any of them is inactive.
func nativeCandidateFor(t *testing.T, authors ...extension.Extension) *nativeCandidate {
	t.Helper()
	specs := make([]nativeActivationSpec, 0, len(authors))
	for _, author := range authors {
		specs = append(specs, nativeActivationSpec{extension: author, section: []byte(`{}`)})
	}
	candidate, err := (&configurationService{}).prepareNativeCandidate(context.Background(), specs)
	if err != nil {
		t.Fatalf("prepare native candidate: %v", err)
	}
	if len(candidate.inactive) != 0 {
		_ = candidate.release()
		t.Fatalf("inactive extensions: %v", candidate.inactive)
	}
	t.Cleanup(func() { _ = candidate.release() })
	return candidate
}

// nativeToolAdapter binds the registered tool name from the authors' active
// registration set and returns the Harness-facing adapter of that binding.
func nativeToolAdapter(t *testing.T, name string, authors ...extension.Extension) registeredExtensionTool {
	t.Helper()
	candidate := nativeCandidateFor(t, authors...)
	stage := candidate.snapshotForTest()
	if stage == nil {
		t.Fatal("candidate snapshot is closed")
	}
	t.Cleanup(func() { _ = stage.release() })
	binding := stage.bindTool(name)
	if binding == nil {
		t.Fatalf("tool %q is not registered", name)
	}
	t.Cleanup(func() { _ = binding.release() })
	return registeredExtensionTool{binding: binding, host: &composition{}, states: map[string]harness.SessionStateValue{}}
}

// nativeCallView opens one live callback view owned by extension id: the same
// scope a registered tool of that activation receives.
func nativeCallView(t *testing.T, id string, authors ...extension.Extension) *nativeCallScope {
	t.Helper()
	candidate := nativeCandidateFor(t, authors...)
	var activation *nativeActivation
	for _, member := range candidate.activations {
		if member.id == id {
			activation = member
			break
		}
	}
	if activation == nil {
		t.Fatalf("extension %q is not active", id)
	}
	ctx, cancel := context.WithCancel(context.Background())
	view := &nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: ctx, cancel: cancel},
		candidate:          candidate, activation: activation, kind: callKindHook, host: &composition{},
		session: harness.SessionIdentity{SessionID: testSessionID},
	}
	t.Cleanup(view.end)
	return view
}

// inertNative is one active extension with no registrations.
func inertNative(id string) extension.Extension {
	return testNative(id, func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil })
}

// serviceNative is one extension registering service name with a single
// method answered by handler.
func serviceNative(id, name string, method extension.Method, handler extension.AuthorHandler) extension.Extension {
	return testNative(id, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: name, Methods: []extension.RegisteredMethod{{
			Method: method, Handler: handler,
		}}}, nil)
		return nil, err
	})
}

func noopNativeTool(context.Context, extension.ToolCall) (extension.ToolResult, error) {
	return extension.ToolResult{Content: "ok"}, nil
}

func TestNativeToolDeclaredFailureDispatch(t *testing.T) {
	provider := serviceNative("provider", "provider", extension.ServiceMethod("call", `{}`, `{"type":"object"}`,
		`{"type":"object","properties":{"reason":{"type":"string"}},"required":["reason"]}`, `{"type":"string"}`),
		func(_ context.Context, _ extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
			if string(arguments) == `{"fail":true}` {
				return extension.Result{Failure: &extension.Failure{Code: "declared", Message: "provider refused", Data: json.RawMessage(`{"reason":"because"}`), Partial: json.RawMessage(`"partial"`)}}, nil
			}
			return extension.Result{Value: json.RawMessage(`{"ok":true}`)}, nil
		})
	tool := &planTool{}
	adapter := nativeToolAdapter(t, "tool", provider, testToolNative("tool", tool.run))
	tc := ToolContext{Session: harness.SessionIdentity{SessionID: testSessionID}}
	call := model.ToolCall{ID: "call-1", Name: "tool", Arguments: json.RawMessage(`{"fail":true}`)}
	outcome := adapter.Run(context.Background(), tc, call)
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == "declared:provider refused", "outcome = %+v, want the declared failure mapped through dispatch", outcome.Result)
	testassert.Require(t, tool.failure != nil && string(tool.failure.Data) == `{"reason":"because"}` && string(tool.failure.Partial) == `"partial"`, "dispatch lost the declared failure payload: %+v", tool.failure)
}

// planTool calls a declared service and retains its failure data.
type planTool struct {
	failure *extension.Failure
}

func (t *planTool) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	if string(call.Arguments) != `{"fail":true}` {
		return extension.ToolResult{}, errors.New("arguments changed")
	}
	result, err := call.Call(ctx, "provider", "call", json.RawMessage(`{"fail":true}`))
	if err != nil {
		return extension.ToolResult{}, err
	}
	if result.Failure == nil {
		return extension.ToolResult{}, errors.New("no failure")
	}
	t.failure = result.Failure
	return extension.ToolResult{Content: result.Failure.Code + ":" + result.Failure.Message}, nil
}

func TestNativeCallOwnsOriginAndJoinsBeforeReturn(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	provider := serviceNative("authprovider", "authprovider", extension.ServiceMethod("call", `{}`, `{}`, "", ""),
		func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			close(started)
			<-ctx.Done()
			close(finished)
			return extension.Result{}, ctx.Err()
		})
	adapter := nativeToolAdapter(t, "caller", provider, testToolNative("caller", (&originTool{started: started}).run))
	outcome := adapter.Run(context.Background(), ToolContext{}, model.ToolCall{ID: "call", Name: "caller", Arguments: json.RawMessage(`{}`)})
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v", outcome.Result)
	select {
	case <-finished:
	default:
		t.Fatal("the substituted-background nested call was not canceled and joined when the callback returned")
	}
}

type originTool struct {
	started chan struct{}
}

func (t *originTool) run(_ context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	go func() {
		_, _ = call.Call(context.Background(), "authprovider", "call", json.RawMessage(`{}`))
	}()
	<-t.started
	return extension.ToolResult{Content: "ok"}, nil
}

func TestNativeCallbackViewCancelsAndJoinsThroughHarness(t *testing.T) {
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "origin")
	writeAgentDefinitions(t, e.configPath, `{"origin_agent":{"model":"prov/m","system_prompt":"simple","tools":["origin_tool"]}}`)
	e.fixtureStore(storage.NewMemory())
	started := make(chan struct{})
	finished := make(chan struct{})
	native := testNative("origin", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "origin.provider", Methods: []extension.RegisteredMethod{{
			Method: extension.ServiceMethod("call", `{}`, `{}`, "", ""),
			Handler: func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				close(started)
				<-ctx.Done()
				close(finished)
				return extension.Result{}, ctx.Err()
			},
		}}}, nil)
		if err != nil {
			return nil, err
		}
		_, err = api.RegisterTool(testDescribeTool("origin_tool"), (&harnessOriginTool{started: started}).run, nil)
		return nil, err
	})
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-origin", "origin_tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, native)
	testassert.NoError(t, err)
	session, err := r.createSession(ctx, e.dataDir, "origin_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-origin", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-origin", harness.OperationSuccess)
	select {
	case <-finished:
	default:
		t.Fatal("the in-flight nested call was not canceled and joined before the callback returned")
	}
	testassert.NoError(t, r.Close(ctx))
}

type harnessOriginTool struct {
	started chan struct{}
}

func (t *harnessOriginTool) run(_ context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	go func() {
		_, _ = call.Call(context.Background(), "origin.provider", "call", json.RawMessage(`{}`))
	}()
	<-t.started
	return extension.ToolResult{Content: "ok"}, nil
}

type ownershipProbe struct {
	inputAfter  string
	resultValue string
}

func (p *ownershipProbe) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	arguments := []byte(`{}`)
	result, err := call.Call(ctx, "own.provider", "call", arguments)
	if err != nil {
		return extension.ToolResult{}, err
	}
	p.inputAfter = string(arguments)
	p.resultValue = string(result.Value)
	return extension.ToolResult{Content: "ok"}, nil
}

func TestNativeServiceArgumentsAndResultAreOwned(t *testing.T) {
	kept := []byte(`{"ok":true}`)
	provider := serviceNative("own", "own.provider", extension.ServiceMethod("call", `{}`, `{}`, "", ""),
		func(_ context.Context, _ extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
			if len(arguments) != 0 {
				arguments[0] = 'x'
			}
			return extension.Result{Value: kept}, nil
		})
	probe := &ownershipProbe{}
	adapter := nativeToolAdapter(t, "caller", provider, testToolNative("caller", probe.run))
	adapter.Run(context.Background(), ToolContext{}, model.ToolCall{ID: "call", Name: "caller", Arguments: json.RawMessage(`{}`)})
	// The provider's result bytes were mutated after return; the accepted value
	// must remain the original owned bytes.
	kept[1] = '!'
	testassert.Equal(t, probe.inputAfter, `{}`)
	testassert.Equal(t, probe.resultValue, `{"ok":true}`)
}

// narrowService is one fixture service whose "call" handler is replaceable.
type narrowService struct {
	handler extension.AuthorHandler
}

func (n *narrowService) native() extension.Extension {
	return serviceNative("narrowprovider", "narrow.provider", extension.ServiceMethod("call", `{}`, `{}`, "", ""),
		func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
			return n.handler(ctx, call, args)
		})
}

func TestNativeSuppliedContextNarrowsButCannotDetachOrigin(t *testing.T) {
	handler := &narrowService{}
	view := nativeCallView(t, "narrowcaller", handler.native(), inertNative("narrowcaller"))

	t.Run("already-canceled supplied context refuses before dispatch", func(t *testing.T) {
		var calls atomic.Int64
		handler.handler = func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
			calls.Add(1)
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := view.call(ctx, "narrow.provider", "call", json.RawMessage(`{}`))
		testassert.Require(t, errors.Is(err, context.Canceled), "call error = %v, want the supplied cancellation", err)
		testassert.Equal(t, calls.Load(), int64(0))
	})

	t.Run("cancellation during an admitted call cancels while origin stays live", func(t *testing.T) {
		started := make(chan struct{})
		canceled := make(chan struct{})
		handler.handler = func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return extension.Result{}, ctx.Err()
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { _, err := view.call(ctx, "narrow.provider", "call", json.RawMessage(`{}`)); result <- err }()
		<-started
		cancel()
		err := <-result
		testassert.Require(t, errors.Is(err, context.Canceled), "call error = %v, want the supplied cancellation", err)
		<-canceled
		testassert.Require(t, view.ctx.Err() == nil, "supplied cancellation detached the host origin")
	})

	t.Run("background cannot detach an expired origin", func(t *testing.T) {
		started := make(chan struct{})
		canceled := make(chan struct{})
		handler.handler = func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			return extension.Result{}, ctx.Err()
		}
		result := make(chan error, 1)
		go func() {
			_, err := view.call(context.Background(), "narrow.provider", "call", json.RawMessage(`{}`))
			result <- err
		}()
		<-started
		view.end()
		err := <-result
		testassert.Require(t, err != nil, "Background call survived origin expiry")
		<-canceled
		if _, err := view.call(context.Background(), "narrow.provider", "call", json.RawMessage(`{}`)); err == nil {
			t.Fatal("retained view remained callable")
		}
	})
}

func TestNativeSuppliedCancellationPropagatesTransitively(t *testing.T) {
	innerEntered := make(chan struct{})
	innerCanceled := make(chan struct{})
	inner := serviceNative("inner", "inner.provider", extension.ServiceMethod("call", `{}`, `{}`, "", ""),
		func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			close(innerEntered)
			<-ctx.Done()
			close(innerCanceled)
			return extension.Result{}, ctx.Err()
		})
	outer := serviceNative("outer", "outer.provider", extension.ServiceMethod("call", `{}`, `{}`, "", ""),
		func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			return call.Call(context.Background(), "inner.provider", "call", json.RawMessage(`{}`))
		})
	view := nativeCallView(t, "outercaller", inner, outer, inertNative("outercaller"))
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := view.call(ctx, "outer.provider", "call", json.RawMessage(`{}`)); result <- err }()
	<-innerEntered
	cancel()
	err := <-result
	testassert.Require(t, errors.Is(err, context.Canceled), "transitive call error = %v", err)
	select {
	case <-innerCanceled:
	default:
		t.Fatal("nested Background call did not inherit the supplied cancellation")
	}
}

func TestNativeToolFollowsInvocationCancellation(t *testing.T) {
	started := make(chan struct{})
	provider := serviceNative("normprovider", "norm.provider", extension.ServiceMethod("wait", `{}`, `{}`, "", ""),
		func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			close(started)
			<-ctx.Done()
			return extension.Result{}, ctx.Err()
		})
	tool := testNative("normtool", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("norm_tool"), func(_ context.Context, tc extension.ToolCall) (extension.ToolResult, error) {
			_, err := tc.Call(context.Background(), "norm.provider", "wait", json.RawMessage(`{}`))
			return extension.ToolResult{}, err
		}, nil)
		return nil, err
	})
	adapter := nativeToolAdapter(t, "norm_tool", provider, tool)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan harness.ToolOutcome, 1)
	go func() {
		done <- adapter.Run(ctx, ToolContext{}, model.ToolCall{ID: "call-norm", Name: "norm_tool", Arguments: json.RawMessage(`{}`)})
	}()
	<-started
	cancel()
	select {
	case outcome := <-done:
		testassert.Require(t, outcome.Result.Status == model.ResultInterrupted, "outcome = %+v, want invocation cancellation", outcome)
	case <-time.After(10 * time.Second):
		t.Fatal("Normalize did not observe invocation cancellation while the pure provider waited")
	}
}

// ownedStartProbe starts owned Sessions from one tool call.
type ownedStartProbe struct {
	content  string
	starts   int
	executes atomic.Int64

	mu        sync.Mutex
	launchErr error
	childID   string
	children  []string
}

// outcome reads the probe's one launch result under its mutex.
func (p *ownedStartProbe) outcome() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.childID, p.launchErr
}

func (p *ownedStartProbe) record(childID string, err error) {
	p.mu.Lock()
	p.childID, p.launchErr = childID, err
	p.mu.Unlock()
}

func (p *ownedStartProbe) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	modelRef := model.ModelRef{Provider: "prov", Model: "m"}
	p.executes.Add(1)
	count := p.starts
	if count == 0 {
		count = 1
	}
	var id string
	for i := 0; i < count; i++ {
		var err error
		id, err = call.StartSession(ctx, extension.SessionStart{Inputs: harness.SessionInputs{Model: &modelRef},
			InitialState: harness.SessionStateValue(`{"probe":"frozen"}`),
			Input:        []model.ContentPart{{Kind: model.PartText, Text: p.content}}})
		if err != nil {
			p.record(id, err)
			return extension.ToolResult{}, err
		}
		p.mu.Lock()
		p.children = append(p.children, id)
		p.mu.Unlock()
	}
	p.record(id, nil)
	return extension.ToolResult{Content: "launched " + id}, nil
}

// ownedStartNative registers probe as name under extension id, which requests
// the host session service it calls.
func ownedStartNative(id, name string, probe extension.ToolFunc) extension.Extension {
	manifest := extension.Manifest{ID: id, Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{}`), Services: []string{"melta.session"}}
	return extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool(name), probe, nil)
		return nil, err
	}}
}

// awaitBody waits until one recorded model request satisfies match and returns
// that request body.
func awaitBody(t *testing.T, e *ownerEnv, match func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for i := 0; i < e.server.requests(); i++ {
			if body := e.server.bodyAt(i); match(body) {
				return body
			}
		}
		testassert.Require(t, time.Now().Before(deadline), "no model request carried the wanted turn")
		time.Sleep(5 * time.Millisecond)
	}
}

// TestNativeToolStartsTwoSessions proves one tool call commits two distinct
// children with the parent Session's lineage.
func TestNativeToolStartsTwoSessions(t *testing.T) {
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "grant")
	writeAgentDefinitions(t, e.configPath, `{"grant_agent":{"model":"prov/m","system_prompt":"simple","tools":["start_tool"]}}`)
	store := storage.NewMemory()
	e.fixtureStore(store)
	probe := &ownedStartProbe{content: "child work", starts: 2}
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastUserText(body) == "child work" {
			return textTurnEvents("child done")
		}
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-start", "start_tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, ownedStartNative("grant", "start_tool", probe.run))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	session, err := r.createSession(ctx, e.dataDir, "grant_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-start", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-start", harness.OperationSuccess)
	_, launchErr := probe.outcome()
	testassert.NoError(t, launchErr)
	probe.mu.Lock()
	children := append([]string(nil), probe.children...)
	probe.mu.Unlock()
	testassert.Require(t, len(children) == 2 && children[0] != children[1] && probe.executes.Load() == 1, "children = %v (executes %d)", children, probe.executes.Load())
	for _, childID := range children {
		header, err := r.harness.ReadSessionHeader(ctx, childID)
		testassert.NoError(t, err)
		testassert.Equal(t, header.Identity.OwnerSessionID, session.Identity.SessionID)
		state, err := r.harness.SessionExtensionStates(ctx, childID)
		testassert.NoError(t, err)
		testassert.Equal(t, string(state["grant"]), `{"probe":"frozen"}`)
	}
	ids, err := store.ListSessionIDs(ctx)
	testassert.NoError(t, err)
	testassert.Equal(t, len(ids), 3)
	awaitBody(t, e, func(body string) bool { return lastUserText(body) == "child work" })
}

// TestNativeOwnedStartAllowsAnotherExtensionToNest proves the owned start is
// not limited to the Agents extension: two distinct extensions may each nest a
// Session of the same parent under their own granted start.
func TestNativeOwnedStartAllowsAnotherExtensionToNest(t *testing.T) {
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "alpha")
	enableFixturePlugin(t, e.configPath, "beta")
	writeAgentDefinitions(t, e.configPath, `{"nest_agent":{"model":"prov/m","system_prompt":"simple","tools":["alpha_start","beta_start"]}}`)
	store := storage.NewMemory()
	e.fixtureStore(store)
	alpha := &ownedStartProbe{content: "alpha work"}
	beta := &ownedStartProbe{content: "beta work"}
	e.server.setScript(func(_ context.Context, body string) []string {
		switch lastUserText(body) {
		case "alpha work", "beta work":
			return textTurnEvents("child done")
		}
		if strings.Contains(body, `"call-beta"`) {
			return textTurnEvents("done")
		}
		if strings.Contains(body, `"call-alpha"`) {
			return toolCallTurnEvents("call-beta", "beta_start", `{}`)
		}
		return toolCallTurnEvents("call-alpha", "alpha_start", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, ownedStartNative("alpha", "alpha_start", alpha.run), ownedStartNative("beta", "beta_start", beta.run))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	session, err := r.createSession(ctx, e.dataDir, "nest_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-nest", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-nest", harness.OperationSuccess)
	for name, probe := range map[string]*ownedStartProbe{"alpha": alpha, "beta": beta} {
		childID, launchErr := probe.outcome()
		testassert.NoError(t, launchErr)
		testassert.Require(t, len(childID) == 32 && probe.executes.Load() == 1, "%s child id = %q (executes %d), want one admitted start", name, childID, probe.executes.Load())
		header, err := r.harness.ReadSessionHeader(ctx, childID)
		testassert.NoError(t, err)
		testassert.Equal(t, header.Identity.OwnerSessionID, session.Identity.SessionID)
	}
	alphaID, _ := alpha.outcome()
	betaID, _ := beta.outcome()
	testassert.Require(t, alphaID != betaID, "both extensions nested the same Session %q", alphaID)
	awaitBody(t, e, func(body string) bool { return lastUserText(body) == "alpha work" })
	awaitBody(t, e, func(body string) bool { return lastUserText(body) == "beta work" })
	ids, err := store.ListSessionIDs(ctx)
	testassert.NoError(t, err)
	testassert.Equal(t, len(ids), 3)
}

func registrationTool(executes *atomic.Int64) extension.ToolFunc {
	return func(context.Context, extension.ToolCall) (extension.ToolResult, error) {
		executes.Add(1)
		return extension.ToolResult{Content: "owned"}, nil
	}
}

// TestNativeToolDefinitionBytesAreOwned proves the accepted registration is
// deep-owned: the caller may mutate its retained definition bytes after
// activation and the advertised definition stays the accepted one.
func TestNativeToolDefinitionBytesAreOwned(t *testing.T) {
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "owned_tool")
	writeAgentDefinitions(t, e.configPath, `{"owned_agent":{"model":"prov/m","system_prompt":"simple","tools":["owned_tool"]}}`)
	e.fixtureStore(storage.NewMemory())
	var executes atomic.Int64
	parameters := []byte(`{"type":"object","properties":{"path":{"type":"string"}}}`)
	definition := model.ToolDefinition{Name: "owned_tool", Description: "original registration description", Parameters: parameters}
	native := testNative("owned_tool", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(definition, registrationTool(&executes), nil)
		return nil, err
	})
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-owned", "owned_tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, native)
	testassert.NoError(t, err)
	// Caller-retained registration data is mutated after acceptance.
	definition.Parameters[2] = 'x'
	definition.Description = "mutated description"
	session, err := r.createSession(ctx, e.dataDir, "owned_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-owned", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-owned", harness.OperationSuccess)
	testassert.Equal(t, executes.Load(), int64(1))
	testassert.Require(t, strings.Contains(e.server.bodyAt(0), "original registration description"), "advertised tool = %s, want the owned original description", e.server.bodyAt(0))
	testassert.Require(t, !strings.Contains(e.server.bodyAt(0), "mutated description") && strings.Contains(e.server.bodyAt(0), `{"type":"object","properties":{"path":{"type":"string"}}}`), "advertised tool = %s, want no caller-mutated registration", e.server.bodyAt(0))
	testassert.NoError(t, r.Close(ctx))
}

// publicationIntentStore injects one failure at the real Harness tool-effect
// intent publication, leaving the durable running state for recovery.
type publicationIntentStore struct {
	harness.Storage
	armed atomic.Bool
	fired atomic.Bool
}

type publicationIntentTx struct {
	harness.Transaction
	owner *publicationIntentStore
}

func (t publicationIntentTx) ReplaceRegister(key harness.RegisterKey, revision int64, payload json.RawMessage) (harness.Register, error) {
	if t.owner.armed.Load() && key.Kind == harness.RegisterOperation && bytes.Contains(payload, []byte(`"kind":"tool"`)) {
		t.owner.fired.Store(true)
		return harness.Register{}, errors.New("injected tool intent publication failure")
	}
	return t.Transaction.ReplaceRegister(key, revision, payload)
}

func (s *publicationIntentStore) Transact(ctx context.Context, fn func(harness.Transaction) error) error {
	return s.Storage.Transact(ctx, func(tx harness.Transaction) error {
		return fn(publicationIntentTx{Transaction: tx, owner: s})
	})
}

// TestNativeToolIntentPublicationFailureRetainsRunningState proves the real
// native tool reaches the shared Harness publication boundary: a failed tool
// intent commits no execution, no result, and leaves the running Operation for
// recovery rather than fabricating a terminal.
func TestNativeToolIntentPublicationFailureRetainsRunningState(t *testing.T) {
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "intentext")
	writeAgentDefinitions(t, e.configPath, `{"intent_agent":{"model":"prov/m","system_prompt":"simple","tools":["intent_tool"]}}`)
	store := &publicationIntentStore{Storage: storage.NewMemory()}
	e.fixtureStore(store)
	var executes atomic.Int64
	native := testNative("intentext", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("intent_tool"), func(context.Context, extension.ToolCall) (extension.ToolResult, error) {
			executes.Add(1)
			return extension.ToolResult{Content: "must not run"}, nil
		}, nil)
		return nil, err
	})
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-intent", "intent_tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, native)
	testassert.NoError(t, err)
	session, err := r.createSession(ctx, e.dataDir, "intent_agent")
	testassert.NoError(t, err)
	store.armed.Store(true)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-intent", "run")
	deadline := time.Now().Add(10 * time.Second)
	for !store.fired.Load() {
		testassert.Require(t, time.Now().Before(deadline), "the tool intent publication was never attempted")
		time.Sleep(5 * time.Millisecond)
	}
	rec, err := r.harness.ReadOperation(ctx, session.Identity.SessionID, "op-intent")
	testassert.NoError(t, err)
	testassert.Require(t, rec.State.Status == harness.OperationRunning && rec.State.Terminal == nil && rec.State.ActiveEffect == nil, "operation after failed tool intent = %+v, want the running state retained without a fabricated terminal", rec.State)
	testassert.Equal(t, executes.Load(), int64(0))
	err = r.Close(ctx)
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "injected tool intent publication failure"), "Close error = %v, want the retained publication failure", err)
}

// TestNativeImplementationSettingsPreserveRawBytesCold proves a rich active
// extension section reaches durable implementation provenance through the
// canonical framing and survives a cold Harness read byte-for-byte:
// noncompact whitespace, HTML, escape spelling, exact large/exponent numbers
// and credential-reference names are all retained.
func TestNativeImplementationSettingsPreserveRawBytesCold(t *testing.T) {
	e := newOwnerEnv(t)
	section := `{"enabled":true, "api_key_env" : "OPENAI_API_KEY", "html":"<&>", "escaped":"\u003c", "large":9007199254740993, "exp":1e400}`
	config := strings.TrimSuffix(ownerConfigDocument(e.server.URL), "}") +
		`,"extensions":{"agents":{"enabled":true,"definitions":{"settings_agent":{"model":"prov/m","system_prompt":"simple","tools":["provsettings"]}}},"provsettings":` + section + `}}`
	writeServiceFile(t, e.configPath, config)
	e.fixtureStore(storage.NewMemory())
	var executes atomic.Int64
	native := testToolNative("provsettings", registrationTool(&executes))
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-settings", "provsettings", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, native)
	testassert.NoError(t, err)
	session, err := r.createSession(ctx, e.dataDir, "settings_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-settings", "run")
	rec := awaitOperation(t, r, session.Identity.SessionID, "op-settings", harness.OperationSuccess)
	find := func(capture harness.ExecutionCapture) []byte {
		for _, implementation := range capture.Implementations {
			if implementation.ExtensionID == "provsettings" {
				return implementation.Settings
			}
		}
		return nil
	}
	testassert.Require(t, string(find(rec.Admission.Execution)) == section, "admission implementation settings = %q, want the exact section bytes", find(rec.Admission.Execution))
	testassert.NoError(t, r.Close(ctx))
	r2, err := e.open(ctx, native)
	testassert.NoError(t, err)
	var cold harness.OperationRecord
	err = r2.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var readErr error
		cold, readErr = h.ReadOperation(ctx, session.Identity.SessionID, "op-settings")
		return readErr
	})
	testassert.NoError(t, err)
	testassert.Require(t, string(find(cold.Admission.Execution)) == section, "cold implementation settings = %q, want the exact section bytes", find(cold.Admission.Execution))
	testassert.NoError(t, r2.Close(ctx))
}

// normalizeIdentity is one observed callback identity of the tool or its pure
// provider. The adapter derives both from the same Harness callback identity.
type normalizeIdentity struct {
	extensionID string
	requestID   string
	callID      string
	sessionID   string
	arguments   string
}

type normalizeIdentityState struct {
	mu       sync.Mutex
	native   []normalizeIdentity
	provider []normalizeIdentity
}

type normalizeIdentityTool struct {
	state *normalizeIdentityState
}

func (t normalizeIdentityTool) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	t.state.mu.Lock()
	t.state.native = append(t.state.native, normalizeIdentity{extensionID: call.ExtensionID, requestID: call.RequestID, callID: call.ID, sessionID: call.Session.SessionID, arguments: string(call.Arguments)})
	t.state.mu.Unlock()
	if _, err := call.Call(ctx, "test.ident.provider", "call", json.RawMessage(`{}`)); err != nil {
		return extension.ToolResult{}, err
	}
	return extension.ToolResult{Content: "ok"}, nil
}

// TestNativeToolUsesCompleteIdentityCarrier verifies the tool and its nested
// service receive the same request and Session identities.
func TestNativeToolUsesCompleteIdentityCarrier(t *testing.T) {
	e := newOwnerEnv(t)
	config := strings.TrimSuffix(ownerConfigDocument(e.server.URL), "}") +
		`,"extensions":{"agents":{"enabled":true,"definitions":{"identity_agent":{"model":"prov/m","system_prompt":"simple","tools":["identity_tool"]}}},"identityext":{"enabled":true},"identprovider":{"enabled":true}}}`
	writeServiceFile(t, e.configPath, config)
	e.fixtureStore(storage.NewMemory())
	state := &normalizeIdentityState{}
	provider := testNative("identprovider", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "test.ident.provider", Methods: []extension.RegisteredMethod{{
			Method: extension.ServiceMethod("call", `{}`, `{}`, "", ""),
			Handler: func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				state.mu.Lock()
				state.provider = append(state.provider, normalizeIdentity{extensionID: call.ExtensionID, requestID: call.RequestID, sessionID: call.Session.SessionID})
				state.mu.Unlock()
				return extension.Result{Value: json.RawMessage(`{}`)}, nil
			},
		}}}, nil)
		return nil, err
	})
	consumer := testNative("identityext", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterTool(testDescribeTool("identity_tool"), (normalizeIdentityTool{state: state}).run, nil); err != nil {
			return nil, err
		}
		_, err := api.AttachArguments(func(_ context.Context, _ extension.AuthorContext, _ model.ToolCall) (extension.ArgumentsResult, error) {
			return extension.ArgumentsResult{Arguments: json.RawMessage(`{"repaired":true}`)}, nil
		}, nil)
		return nil, err
	})
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-ident", "identity_tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, provider, consumer)
	testassert.NoError(t, err)
	session, err := r.createSession(ctx, e.dataDir, "identity_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-identity", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-identity", harness.OperationSuccess)
	state.mu.Lock()
	nativeIDs := append([]normalizeIdentity(nil), state.native...)
	providerIDs := append([]normalizeIdentity(nil), state.provider...)
	state.mu.Unlock()
	testassert.Require(t, len(nativeIDs) == 1 && len(providerIDs) == 1, "tool identities = native %v provider %v, want one call", nativeIDs, providerIDs)
	testassert.Equal(t, nativeIDs[0].callID, "call-ident")
	testassert.Equal(t, nativeIDs[0].arguments, `{"repaired":true}`)
	testassert.Equal(t, nativeIDs[0].extensionID, "identityext")
	testassert.Equal(t, nativeIDs[0].sessionID, session.Identity.SessionID)
	testassert.Equal(t, providerIDs[0].extensionID, "identprovider")
	testassert.Equal(t, providerIDs[0].sessionID, nativeIDs[0].sessionID)
	testassert.Equal(t, providerIDs[0].requestID, nativeIDs[0].requestID)
	testassert.NoError(t, r.Close(ctx))
}
