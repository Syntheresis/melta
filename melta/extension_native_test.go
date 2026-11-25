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

type noopNativeTool struct{}

func (noopNativeTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (noopNativeTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: "call", Status: model.ResultSuccess, Content: "ok"}}}
}
func (noopNativeTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Result: model.ToolResult{CallID: "call", Status: model.ResultSuccess, Content: "ok"}}
}

func TestNativeToolPlanOwnershipAndDeclaredFailureDispatch(t *testing.T) {
	provider := serviceNative("provider", "provider", extension.ServiceMethod("call", `{}`, `{"type":"object"}`,
		`{"type":"object","properties":{"reason":{"type":"string"}},"required":["reason"]}`, `{"type":"string"}`),
		func(_ context.Context, _ extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
			if string(arguments) == `{"fail":true}` {
				return extension.Result{Failure: &extension.Failure{Code: "declared", Message: "provider refused", Data: json.RawMessage(`{"reason":"because"}`), Partial: json.RawMessage(`"partial"`)}}, nil
			}
			return extension.Result{Value: json.RawMessage(`{"ok":true}`)}, nil
		})
	tool := &planTool{}
	adapter := nativeToolAdapter(t, "tool", provider, testToolNative("tool", tool))
	tc := ToolContext{Session: harness.SessionIdentity{SessionID: testSessionID}}
	call := model.ToolCall{ID: "call-1", Name: "tool", Arguments: json.RawMessage(`{"fail":true}`)}
	prepared := adapter.Prepare(context.Background(), tc, call)
	testassert.Require(t, prepared.Execute != nil && len(prepared.Permissions) == 1, "plan = %+v, want the executable plan", prepared)
	outcome := prepared.Execute(context.Background())
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == "declared:provider refused", "outcome = %+v, want the declared failure mapped through dispatch", outcome.Result)
	testassert.Require(t, tool.failure != nil && string(tool.failure.Data) == `{"reason":"because"}` && string(tool.failure.Partial) == `"partial"`, "dispatch lost the declared failure payload: %+v", tool.failure)
}

// planTool prepares one plan carrying opaque Data and, at Execute, calls its
// declared service with that Data.
type planTool struct {
	callID  string
	failure *extension.Failure
}

func (t *planTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *planTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	t.callID = call.ID
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: "test.call", Target: "*"}}, Data: append([]byte("opaque:"), call.Arguments...)}
}

func (t *planTool) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	if string(plan.Data) != `opaque:{"fail":true}` {
		return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: t.callID, Status: model.ResultError, Content: "plan data changed"}}
	}
	result, err := call.Call(ctx, "provider", "call", json.RawMessage(`{"fail":true}`))
	if err != nil {
		return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: t.callID, Status: model.ResultError, Content: err.Error()}}
	}
	if result.Failure == nil {
		return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: t.callID, Status: model.ResultError, Content: "no failure"}}
	}
	t.failure = result.Failure
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: t.callID, Status: model.ResultSuccess, Content: result.Failure.Code + ":" + result.Failure.Message}}
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
	adapter := nativeToolAdapter(t, "caller", provider, testToolNative("caller", &originTool{started: started}))
	prepared := adapter.Prepare(context.Background(), ToolContext{}, model.ToolCall{ID: "call", Name: "caller", Arguments: json.RawMessage(`{}`)})
	testassert.Require(t, prepared.Execute != nil, "plan = %+v, want execution", prepared)
	outcome := prepared.Execute(context.Background())
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v", outcome.Result)
	select {
	case <-finished:
	default:
		t.Fatal("the substituted-background nested call was not canceled and joined when the callback returned")
	}
}

type originTool struct {
	started chan struct{}
	callID  string
}

func (t *originTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (t *originTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	t.callID = call.ID
	return extension.ToolPlan{}
}
func (t *originTool) Execute(_ context.Context, call extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	go func() {
		_, _ = call.Call(context.Background(), "authprovider", "call", json.RawMessage(`{}`))
	}()
	<-t.started
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: t.callID, Status: model.ResultSuccess, Content: "ok"}}
}

func TestNativeDeniedPlanDoesNotExecute(t *testing.T) {
	e := newOwnerEnv(t)
	writeServiceFile(t, e.configPath, strings.TrimSuffix(ownerConfigDocument(e.server.URL), "}")+`,"permissions":{"rules":[{"permission":"command.run","target":"denied","access":"deny"}]}}`)
	writeAgentDefinitions(t, e.configPath, `{"denied_agent":{"model":"prov/m","system_prompt":"simple","tools":["denied_tool"]}}`)
	enableFixturePlugin(t, e.configPath, "deniedext")
	e.fixtureStore(storage.NewMemory())
	var executes atomic.Int64
	native := testNative("deniedext", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("denied_tool"), &deniedTool{executes: &executes}, nil)
		return nil, err
	})
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-denied", "denied_tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, native)
	testassert.NoError(t, err)
	session, err := r.createSession(ctx, e.dataDir, "denied_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-denied", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-denied", harness.OperationSuccess)
	testassert.Equal(t, executes.Load(), int64(0))
	testassert.NoError(t, r.Close(ctx))
}

type deniedTool struct {
	executes *atomic.Int64
	callID   string
}

func (t *deniedTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (t *deniedTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	t.callID = call.ID
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: "command.run", Target: "denied"}}}
}
func (t *deniedTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	t.executes.Add(1)
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: t.callID, Status: model.ResultSuccess, Content: "must not run"}}
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
		_, err = api.RegisterTool(testDescribeTool("origin_tool"), &harnessOriginTool{started: started}, nil)
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
	callID  string
}

func (t *harnessOriginTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (t *harnessOriginTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	t.callID = call.ID
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: "command.run", Target: "test"}}}
}
func (t *harnessOriginTool) Execute(_ context.Context, call extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	go func() {
		_, _ = call.Call(context.Background(), "origin.provider", "call", json.RawMessage(`{}`))
	}()
	<-t.started
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: t.callID, Status: model.ResultSuccess, Content: "ok"}}
}

type ownershipProbe struct {
	inputAfter  string
	resultValue string
	callID      string
}

func (p *ownershipProbe) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (p *ownershipProbe) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	p.callID = call.ID
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: "command.run", Target: "test"}}}
}
func (p *ownershipProbe) Execute(ctx context.Context, call extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	arguments := []byte(`{}`)
	result, err := call.Call(ctx, "own.provider", "call", arguments)
	if err != nil {
		return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: p.callID, Status: model.ResultError, Content: err.Error()}}
	}
	p.inputAfter = string(arguments)
	p.resultValue = string(result.Value)
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: p.callID, Status: model.ResultSuccess, Content: "ok"}}
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
	adapter := nativeToolAdapter(t, "caller", provider, testToolNative("caller", probe))
	prepared := adapter.Prepare(context.Background(), ToolContext{}, model.ToolCall{ID: "call", Name: "caller", Arguments: json.RawMessage(`{}`)})
	testassert.Require(t, prepared.Execute != nil, "plan = %+v, want execution", prepared)
	prepared.Execute(context.Background())
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

// normalizeWaitTool normalizes by calling its declared pure provider with a
// detached author context: only the invocation view origin can cancel it.
type normalizeWaitTool struct{}

func (normalizeWaitTool) Normalize(tc extension.AuthorContext, _ model.ToolCall) (json.RawMessage, error) {
	_, err := tc.Call(context.Background(), "norm.provider", "wait", json.RawMessage(`{}`))
	return nil, err
}

func (normalizeWaitTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "test"}}}
}

func (normalizeWaitTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: "call-norm", Status: model.ResultSuccess, Content: "ok"}}
}

// TestNativeNormalizeFollowsInvocationCancellation proves Normalize's view
// origin is the actual invocation context: cancelling the invocation
// interrupts a waiting pure provider.
func TestNativeNormalizeFollowsInvocationCancellation(t *testing.T) {
	started := make(chan struct{})
	provider := serviceNative("normprovider", "norm.provider", extension.ServiceMethod("wait", `{}`, `{}`, "", ""),
		func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			close(started)
			<-ctx.Done()
			return extension.Result{}, ctx.Err()
		})
	tool := testNative("normtool", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("norm_tool"), normalizeWaitTool{}, nil)
		return nil, err
	})
	adapter := nativeToolAdapter(t, "norm_tool", provider, tool)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Normalize(ctx, ToolContext{}, model.ToolCall{ID: "call-norm", Name: "norm_tool", Arguments: json.RawMessage(`{}`)})
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		testassert.Require(t, errors.Is(err, context.Canceled), "Normalize error = %v, want invocation cancellation", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Normalize did not observe invocation cancellation while the pure provider waited")
	}
}

// ownedStartProbe prepares one frozen SessionStart and requests it through the
// host session service at Execute, after mutating the plan it received.
type ownedStartProbe struct {
	target   string
	content  string
	executes atomic.Int64

	mu        sync.Mutex
	launchErr error
	childID   string
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

func (p *ownedStartProbe) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (p *ownedStartProbe) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	modelRef := model.ModelRef{Provider: "prov", Model: "m"}
	return extension.ToolPlan{
		Data: []byte(call.ID),
		Start: &extension.SessionStart{
			Target:       p.target,
			Inputs:       harness.SessionInputs{Model: &modelRef},
			InitialState: harness.SessionStateValue(`{"probe":"frozen"}`),
			Input:        []model.ContentPart{{Kind: model.PartText, Text: p.content}},
		},
	}
}

func (p *ownedStartProbe) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	callID := string(plan.Data)
	p.executes.Add(1)
	plan.Start.Target = "substituted"
	plan.Start.Input[0].Text = "substituted"
	plan.Start.InitialState[0] = 'x'
	plan.Start.Inputs.Model = &model.ModelRef{Provider: "substituted", Model: "substituted"}
	result, err := call.Call(ctx, "melta.session", "start", json.RawMessage(`{}`))
	if err != nil {
		p.record("", err)
		return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: err.Error()}}
	}
	var launched struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(result.Value, &launched); err != nil {
		p.record("", err)
		return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: err.Error()}}
	}
	p.record(launched.SessionID, nil)
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: "launched " + launched.SessionID}}
}

// ownedStartNative registers probe as name under extension id, which requests
// the host session service its ToolPlan.Start consumes.
func ownedStartNative(id, name string, probe extension.RegisteredTool) extension.Extension {
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

// TestNativeOwnedStartIgnoresPlanMutation proves the real Harness path: the
// prepared start is frozen, the executor's mutation of the plan it received
// cannot substitute the target, inputs or initial state, and the committed
// child Session exists exactly once with the frozen content.
func TestNativeOwnedStartIgnoresPlanMutation(t *testing.T) {
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "grant")
	writeAgentDefinitions(t, e.configPath, `{"grant_agent":{"model":"prov/m","system_prompt":"simple","tools":["start_tool"]}}`)
	store := storage.NewMemory()
	e.fixtureStore(store)
	probe := &ownedStartProbe{target: "secondary", content: "child work"}
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
	r, err := e.open(ctx, ownedStartNative("grant", "start_tool", probe))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	session, err := r.createSession(ctx, e.dataDir, "grant_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-start", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-start", harness.OperationSuccess)
	childID, launchErr := probe.outcome()
	testassert.NoError(t, launchErr)
	testassert.Require(t, len(childID) == 32 && probe.executes.Load() == 1, "child id = %q (executes %d), want one admitted start", childID, probe.executes.Load())
	header, err := r.harness.ReadSessionHeader(ctx, childID)
	testassert.NoError(t, err)
	testassert.Equal(t, header.Identity.OwnerSessionID, session.Identity.SessionID)
	ids, err := store.ListSessionIDs(ctx)
	testassert.NoError(t, err)
	testassert.Equal(t, len(ids), 2)
	// The frozen content reached the child; no plan mutation did.
	awaitBody(t, e, func(body string) bool { return lastUserText(body) == "child work" })
	for i := 0; i < e.server.requests(); i++ {
		testassert.Require(t, !strings.Contains(e.server.bodyAt(i), "substituted"), "model request %d carried a substituted plan value: %s", i, e.server.bodyAt(i))
	}
}

// TestNativeOwnedStartDeniedPlanDoesNotExecute proves the deny side of the
// same real Harness boundary: a start plan denied by the configured policy
// never reaches the executor, so it creates no Session, and the next model
// request receives the fixed denied result instead.
func TestNativeOwnedStartDeniedPlanDoesNotExecute(t *testing.T) {
	e := newOwnerEnv(t)
	writeServiceFile(t, e.configPath, strings.TrimSuffix(ownerConfigDocument(e.server.URL), "}")+`,"permissions":{"rules":[{"permission":"session.own","target":"grant/secondary","access":"deny"}]}}`)
	enableFixturePlugin(t, e.configPath, "grant")
	writeAgentDefinitions(t, e.configPath, `{"grant_agent":{"model":"prov/m","system_prompt":"simple","tools":["start_tool"]}}`)
	store := storage.NewMemory()
	e.fixtureStore(store)
	probe := &ownedStartProbe{target: "secondary", content: "child work"}
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-start", "start_tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, ownedStartNative("grant", "start_tool", probe))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	session, err := r.createSession(ctx, e.dataDir, "grant_agent")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-denied-start", "run")
	awaitOperation(t, r, session.Identity.SessionID, "op-denied-start", harness.OperationSuccess)
	testassert.Equal(t, probe.executes.Load(), int64(0))
	testassert.Require(t, e.server.requests() == 2, "model requests = %d, want the tool-call turn and its denied follow-up", e.server.requests())
	testassert.Require(t, strings.Contains(e.server.bodyAt(1), harness.PermissionDeniedContent), "denied follow-up request = %s, want the fixed denied content", e.server.bodyAt(1))
	ids, err := store.ListSessionIDs(ctx)
	testassert.NoError(t, err)
	testassert.Equal(t, len(ids), 1)
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
	alpha := &ownedStartProbe{target: "shared", content: "alpha work"}
	beta := &ownedStartProbe{target: "shared", content: "beta work"}
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
	r, err := e.open(ctx, ownedStartNative("alpha", "alpha_start", alpha), ownedStartNative("beta", "beta_start", beta))
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

type registrationTool struct{ executes *atomic.Int64 }

func (t registrationTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (registrationTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "test"}}}
}
func (t registrationTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	t.executes.Add(1)
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: "call-owned", Status: model.ResultSuccess, Content: "owned"}}
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
		_, err := api.RegisterTool(definition, registrationTool{executes: &executes}, nil)
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

type invalidImmediateTool struct{}

func (invalidImmediateTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (invalidImmediateTool) Prepare(_ context.Context, _ extension.AuthorContext, _ model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "test"}}, Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess, Content: "immediate without identity"}}}
}
func (invalidImmediateTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{Status: model.ResultSuccess, Content: "must not run"}}
}

type invalidResultTool struct{}

func (invalidResultTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (invalidResultTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "test"}}}
}
func (invalidResultTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{Status: model.ResultSuccess, Content: "result without identity"}}
}

// TestNativeInvalidPreparedAndReturnedResultsAreSettledByHarness proves the
// real native producer's malformed outcomes reach the shared Harness
// settlement: an immediate success without the actual call identity settles
// as the internal-validation result without execution, and an executor
// success without identity settles as unknown rather than being stamped.
func TestNativeInvalidPreparedAndReturnedResultsAreSettledByHarness(t *testing.T) {
	for _, row := range []struct {
		name string
		tool extension.RegisteredTool
		want string
	}{
		{name: "invalid immediate", tool: invalidImmediateTool{}, want: "Tool call failed internal validation."},
		{name: "invalid executor result", tool: invalidResultTool{}, want: "Execution outcome unknown"},
	} {
		t.Run(row.name, func(t *testing.T) {
			e := newOwnerEnv(t)
			enableFixturePlugin(t, e.configPath, "invalid_result")
			writeAgentDefinitions(t, e.configPath, `{"invalid_agent":{"model":"prov/m","system_prompt":"simple","tools":["invalid_tool"]}}`)
			e.fixtureStore(storage.NewMemory())
			native := testNative("invalid_result", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
				_, err := api.RegisterTool(testDescribeTool("invalid_tool"), row.tool, nil)
				return nil, err
			})
			e.server.setScript(func(_ context.Context, body string) []string {
				if lastMessageRole(body) == "tool" {
					return textTurnEvents("done")
				}
				return toolCallTurnEvents("call-invalid", "invalid_tool", `{}`)
			})
			ctx := context.Background()
			r, err := e.open(ctx, native)
			testassert.NoError(t, err)
			session, err := r.createSession(ctx, e.dataDir, "invalid_agent")
			testassert.NoError(t, err)
			submitThroughRuntime(t, r, session.Identity.SessionID, "op-invalid", "run")
			awaitOperation(t, r, session.Identity.SessionID, "op-invalid", harness.OperationSuccess)
			testassert.Require(t, e.server.requests() >= 2, "model requests = %d, want the call and follow-up", e.server.requests())
			testassert.Require(t, strings.Contains(e.server.bodyAt(1), row.want), "follow-up request = %s, want %q", e.server.bodyAt(1), row.want)
			testassert.NoError(t, r.Close(ctx))
		})
	}
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

type intentFailureTool struct{ executes *atomic.Int64 }

func (t intentFailureTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (intentFailureTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "test"}}}
}
func (t intentFailureTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	t.executes.Add(1)
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{Status: model.ResultSuccess, Content: "must not run"}}
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
		_, err := api.RegisterTool(testDescribeTool("intent_tool"), intentFailureTool{executes: &executes}, nil)
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
	native := testToolNative("provsettings", registrationTool{executes: &executes})
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

func (t normalizeIdentityTool) Normalize(tc extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	t.state.mu.Lock()
	t.state.native = append(t.state.native, normalizeIdentity{extensionID: tc.ExtensionID, requestID: tc.RequestID, callID: call.ID, sessionID: tc.Session.SessionID, arguments: string(call.Arguments)})
	t.state.mu.Unlock()
	if _, err := tc.Call(context.Background(), "test.ident.provider", "call", json.RawMessage(`{}`)); err != nil {
		return nil, err
	}
	return call.Arguments, nil
}
func (t normalizeIdentityTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "test"}}}
}
func (t normalizeIdentityTool) Execute(_ context.Context, _ extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: "call-ident", Status: model.ResultSuccess, Content: "ok"}}
}

// TestNativeNormalizeUsesCompleteIdentityCarrier proves the native adapter
// consumes the Harness callback identity for the one final normalized call,
// reaching the native tool and its declared pure service with the same
// producing request and Session identities.
func TestNativeNormalizeUsesCompleteIdentityCarrier(t *testing.T) {
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
		if _, err := api.RegisterTool(testDescribeTool("identity_tool"), normalizeIdentityTool{state: state}, nil); err != nil {
			return nil, err
		}
		_, err := api.AttachArguments(extension.HookModePure, func(_ context.Context, _ extension.AuthorContext, _ model.ToolCall) (extension.ArgumentsResult, error) {
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
	testassert.Require(t, len(nativeIDs) == 1 && len(providerIDs) == 1, "normalize identities = native %v provider %v, want one final normalization", nativeIDs, providerIDs)
	testassert.Equal(t, nativeIDs[0].callID, "call-ident")
	testassert.Equal(t, nativeIDs[0].arguments, `{"repaired":true}`)
	testassert.Equal(t, nativeIDs[0].extensionID, "identityext")
	testassert.Equal(t, nativeIDs[0].sessionID, session.Identity.SessionID)
	testassert.Equal(t, providerIDs[0].extensionID, "identprovider")
	testassert.Equal(t, providerIDs[0].sessionID, nativeIDs[0].sessionID)
	testassert.Equal(t, providerIDs[0].requestID, nativeIDs[0].requestID)
	testassert.NoError(t, r.Close(ctx))
}
