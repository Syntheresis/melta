package melta

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestLostCapturedWorkerFailsRestrictedStage covers the captured-worker
// contract: an Operation admitted with a worker activation whose authorization
// hook enforces a restriction must fail its next tool call once that worker is
// lost, never execute the call without the hook; new work admitted afterwards
// proceeds without the lost extension. The positive sibling proves the live
// hook denies the same call.
func TestLostCapturedWorkerFailsRestrictedStage(t *testing.T) {
	t.Run("live worker hook denies the call", func(t *testing.T) {
		var executed atomic.Int64
		r, _, spawner, session := newRestrictedWorkerOwner(t, &executed)
		submitThroughRuntime(t, r, session, "run", "run")
		awaitOperation(t, r, session, "run", harness.OperationSuccess)
		result := toolResultFact(t, r, session, "run")
		testassert.Require(t, result.Status == model.ResultDenied && result.Content == "worker restriction", "tool result = %+v, want the authorization refusal", result)
		testassert.Equal(t, result.Evidence, harness.ExecutionNotStarted)
		testassert.Equal(t, executed.Load(), int64(0))
		testassert.Require(t, spawner.count() == 1, "worker spawns = %d, want one", spawner.count())
	})

	t.Run("lost worker fails the captured stage and new work omits it", func(t *testing.T) {
		var executed atomic.Int64
		r, e, spawner, session := newRestrictedWorkerOwner(t, &executed)
		hold := make(chan struct{})
		e.server.setHold(hold)
		submitThroughRuntime(t, r, session, "run", "run")
		select {
		case <-e.server.arrived:
		case <-time.After(executableTestTimeout):
			t.Fatal("the model request never arrived")
		}
		if err := spawner.last().Kill(); err != nil {
			t.Fatalf("kill worker: %v", err)
		}
		lost := awaitCapturedActivationLoss(t, r)
		close(hold)
		record := awaitOperation(t, r, session, "run", harness.OperationFailure)
		testassert.Require(t, record.State.Terminal != nil && record.State.Terminal.Detail == lost.Error(), "lost-stage operation = %+v, want the captured activation failure %q", record.State.Terminal, lost)
		result := toolResultFact(t, r, session, "run")
		testassert.Require(t, result.Status == model.ResultError && result.Content == lost.Error(), "lost-stage tool result = %+v, want the captured activation failure %q", result, lost)
		testassert.Equal(t, result.Evidence, harness.ExecutionNotStarted)
		testassert.Equal(t, executed.Load(), int64(0))
		testassert.Require(t, result.Implementation == nil, "lost-stage result carried an implementation: %+v", result.Implementation)

		submitThroughRuntime(t, r, session, "after", "new work")
		record = awaitOperation(t, r, session, "after", harness.OperationSuccess)
		for _, implementation := range record.Admission.Execution.Implementations {
			testassert.Require(t, implementation.ExtensionID != "peer", "new admission captured the lost extension: %+v", implementation)
		}
		body := awaitBody(t, e, func(body string) bool { return lastUserText(body) == "new work" })
		testassert.Require(t, !advertisesTool(t, body, "peer.tool"), "new admission still advertised the lost tool: %v", advertisedTools(t, body))
	})
}

// newRestrictedWorkerOwner opens the owner, enables the worker section and
// creates one Session over its own workspace.
func newRestrictedWorkerOwner(t *testing.T, executed *atomic.Int64) (*Melta, *ownerEnv, *adapterSpawner, string) {
	t.Helper()
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "peer")
	e.fixtureStore(storage.NewMemory())
	manifest := extension.Manifest{ID: "peer", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			if _, err := api.RegisterTool(model.ToolDefinition{Name: "peer.tool", Description: "restricted", Parameters: json.RawMessage(`{"type":"object"}`)}, restrictedWorkerTool{executed: executed}, nil); err != nil {
				return nil, err
			}
			if _, err := api.AttachAuthorization(func(_ context.Context, _ extension.AuthorContext, input extension.AuthorizationInput) (*string, error) {
				if input.ToolName != "peer.tool" {
					return nil, nil
				}
				refusal := "worker restriction"
				return &refusal, nil
			}, nil); err != nil {
				return nil, err
			}
			return nil, nil
		}), nil
	}}
	spawner.install()
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastUserText(body) == "new work" || lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-peer", "peer.tool", `{}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx, executableAuthorExtension(manifest, "peer", func(context.Context, []string) ([]string, error) { return []string{}, nil }))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	session, err := r.createSession(ctx, e.dataDir, "")
	testassert.NoError(t, err)
	return r, e, spawner, session.Identity.SessionID
}

// restrictedWorkerTool records execution and succeeds when reached.
type restrictedWorkerTool struct{ executed *atomic.Int64 }

func (restrictedWorkerTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (restrictedWorkerTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "*"}}}
}
func (t restrictedWorkerTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	t.executed.Add(1)
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{Status: model.ResultSuccess, Content: "executed"}}
}

// toolResultFact returns the committed tool result of one Operation.
func toolResultFact(t *testing.T, r *Melta, sessionID, operationID string) harness.ToolResultEntry {
	t.Helper()
	view := sessionViewThroughRuntime(t, r, sessionID)
	for _, fact := range historyFactsOf(view.History) {
		if fact.ToolResult != nil && fact.OperationID == operationID {
			return *fact.ToolResult
		}
	}
	t.Fatalf("operation %q has no committed tool result", operationID)
	return harness.ToolResultEntry{}
}

// awaitCapturedActivationLoss waits until the peer activation is marked lost
// and returns that loss.
func awaitCapturedActivationLoss(t *testing.T, r *Melta) error {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r.config.nativeMu.Lock()
		current := r.config.nativeCurrent
		var lost error
		if current != nil {
			for _, activation := range current.activations {
				if activation != nil && activation.id == "peer" {
					lost = activation.lost
				}
			}
		}
		r.config.nativeMu.Unlock()
		if lost != nil {
			return lost
		}
		testassert.Require(t, time.Now().Before(deadline), "peer activation was never marked lost")
		time.Sleep(10 * time.Millisecond)
	}
}
