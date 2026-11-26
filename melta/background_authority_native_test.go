package melta

// Child-authority coverage over registered tools, session.start and Harness
// admission, including committed child and settlement identities.

import (
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
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

type bridgeChildProbe struct {
	mode         string
	childStarted chan struct{}
	started      chan struct{}
	launched     chan struct{}
	done         chan struct{}

	childID      string
	launchErr    error
	retained     extension.AuthorContext
	executes     atomic.Int64
	launchedOnce sync.Once
	startedOnce  sync.Once
	doneOnce     sync.Once
}

func (p *bridgeChildProbe) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	modelRef := model.ModelRef{Provider: "prov", Model: "m"}
	start := extension.SessionStart{
		Inputs:       harness.SessionInputs{Model: &modelRef},
		InitialState: harness.SessionStateValue(`{"agent":"secondary"}`),
		Input:        []model.ContentPart{{Kind: model.PartText, Text: "child work"}},
		Report:       true,
		OutputLimit:  15360,
	}
	p.executes.Add(1)
	if p.mode == "canceled_origin" {
		p.startedOnce.Do(func() { close(p.started) })
		<-ctx.Done()
		_, p.launchErr = call.StartSession(context.Background(), start)
		p.doneOnce.Do(func() { close(p.done) })
		return extension.ToolResult{}, ctx.Err()
	}
	childID, err := call.StartSession(ctx, start)
	if err != nil {
		p.launchErr = err
		p.launchedOnce.Do(func() { close(p.launched) })
		return extension.ToolResult{}, err
	}
	p.childID = childID
	<-p.childStarted // the committed child is blocked in its own model request
	if p.mode == "interrupted_committed" {
		p.launchedOnce.Do(func() { close(p.launched) })
		<-ctx.Done()
		return extension.ToolResult{}, ctx.Err()
	}
	p.retained = call.AuthorContext
	p.launchedOnce.Do(func() { close(p.launched) })
	return extension.ToolResult{Content: childID}, nil
}

// shChildGrantNative registers the probe tool under the grant extension,
// which requests the host session service its start consumes.
func shChildGrantNative(tool extension.ToolFunc) extension.Extension {
	manifest := registrarManifest("grant")
	manifest.Services = []string{"melta.session"}
	return extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("grant_tool"), tool, nil)
		return nil, err
	}}
}

func bridgeChildCount(t *testing.T, r *Melta, store harness.Storage) int {
	t.Helper()
	ids, err := store.ListSessionIDs(context.Background())
	testassert.NoError(t, err)
	children := 0
	for _, id := range ids {
		header, err := r.harness.ReadSessionHeader(context.Background(), id)
		if err == nil && header.Identity.OwnerSessionID != "" {
			children++
		}
	}
	return children
}

func bridgeParentHasChildResult(t *testing.T, e *ownerEnv, childID string) bool {
	t.Helper()
	for i := 0; i < e.server.requests(); i++ {
		var doc struct {
			Messages []struct {
				Role       string          `json:"role"`
				Content    json.RawMessage `json:"content"`
				ToolCallID string          `json:"tool_call_id"`
			} `json:"messages"`
		}
		_ = json.Unmarshal([]byte(e.server.bodyAt(i)), &doc)
		for _, message := range doc.Messages {
			if message.Role == "tool" && message.ToolCallID == "call-probe" && strings.Contains(string(message.Content), childID) {
				return true
			}
		}
	}
	return false
}

// TestNativeChildBridgeRetainsCommittedChildren proves through the actual
// registered tool -> frozen start -> session.start -> Harness admission
// bridge: a canceled tool origin cannot start a Session even with a
// fresh context; a retained Call view is closed after return; and an
// already-committed child blocked in its own model request survives both
// parent interruption and executor return, then settles successfully with
// the exact parent/child identity.
func TestNativeChildBridgeRetainsCommittedChildren(t *testing.T) {
	for _, row := range []struct {
		name       string
		mode       string
		wantParent harness.OperationState
		wantChild  bool
	}{
		{name: "canceled origin cannot launch", mode: "canceled_origin", wantParent: harness.OperationInterruption},
		{name: "retained view cannot launch after return", mode: "retained", wantParent: harness.OperationSuccess, wantChild: true},
		{name: "executor return keeps committed child", mode: "executor_committed", wantParent: harness.OperationSuccess, wantChild: true},
		{name: "interruption keeps committed child", mode: "interrupted_committed", wantParent: harness.OperationInterruption, wantChild: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			eachPrepStore(t, func(t *testing.T, store harness.Storage) {
				e := newOwnerEnv(t)
				e.fixtureStore(store)
				enableFixturePlugin(t, e.configPath, "grant")
				writeAgentDefinitions(t, e.configPath, `{"grant_agent":{"model":"prov/m","system_prompt":"simple","tools":["grant_tool"]},"secondary":{"model":"prov/m","system_prompt":"simple","subagent":true}}`)
				childStarted := make(chan struct{})
				var childStartedOnce sync.Once
				releaseChild := make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(releaseChild) }) }
				e.server.setScript(func(_ context.Context, body string) []string {
					if lastUserText(body) == "child work" {
						childStartedOnce.Do(func() { close(childStarted) })
						<-releaseChild
						return textTurnEvents("child done")
					}
					if lastMessageRole(body) == "tool" {
						return textTurnEvents("done")
					}
					if lastUserText(body) == "run" {
						return toolCallTurnEvents("call-probe", "grant_tool", `{}`)
					}
					return textTurnEvents("done")
				})
				probe := &bridgeChildProbe{mode: row.mode, childStarted: childStarted, started: make(chan struct{}), launched: make(chan struct{}), done: make(chan struct{})}
				ctx := context.Background()
				r, err := e.open(ctx, shChildGrantNative(probe.run))
				testassert.NoError(t, err)
				t.Cleanup(func() {
					release()
					_ = r.Close(context.Background())
				})
				session, err := r.createSession(ctx, e.dataDir, "grant_agent")
				testassert.NoError(t, err)
				submitThroughRuntime(t, r, session.Identity.SessionID, "probe-parent-op", "run")

				if row.mode == "canceled_origin" {
					select {
					case <-probe.started:
					case <-time.After(10 * time.Second):
						t.Fatal("the probe execution never started")
					}
					err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
						return h.Interrupt(ctx, session.Identity.SessionID)
					})
					testassert.NoError(t, err)
					awaitOperation(t, r, session.Identity.SessionID, "probe-parent-op", row.wantParent)
					select {
					case <-probe.done:
					case <-time.After(10 * time.Second):
						t.Fatal("the canceled-origin probe never resumed")
					}
					testassert.Require(t, probe.launchErr != nil, "post-cancellation session start = %v, want a refusal", probe.launchErr)
					testassert.Equal(t, bridgeChildCount(t, r, store), 0)
					return
				}

				select {
				case <-probe.launched:
				case <-time.After(10 * time.Second):
					t.Fatal("the committed child was never observed")
				}
				testassert.Require(t, probe.executes.Load() == 1, "probe Execute calls = %d, want one per tool call", probe.executes.Load())
				testassert.Require(t, len(probe.childID) == 32, "child id = %q, want the committed child identity (launch error %v)", probe.childID, probe.launchErr)
				childHeader, err := r.harness.ReadSessionHeader(context.Background(), probe.childID)
				testassert.NoError(t, err)
				testassert.Equal(t, childHeader.Identity.OwnerSessionID, session.Identity.SessionID)
				childOpID := childHeader.CurrentOperationID
				childOp, err := r.harness.ReadOperation(context.Background(), probe.childID, childOpID)
				testassert.NoError(t, err)
				testassert.Equal(t, childOp.State.Status, harness.OperationRunning)

				if row.mode == "retained" {
					awaitOperation(t, r, session.Identity.SessionID, "probe-parent-op", row.wantParent)
					if _, err := probe.retained.Call(context.Background(), "melta.session", "start", json.RawMessage(`{}`)); !errors.Is(err, extension.ErrClosed) {
						t.Fatalf("the retained Call view error = %v, want the closed view refusal", err)
					}
					testassert.Equal(t, bridgeChildCount(t, r, store), 1)
				} else if row.mode == "executor_committed" {
					awaitOperation(t, r, session.Identity.SessionID, "probe-parent-op", row.wantParent)
					testassert.Require(t, bridgeParentHasChildResult(t, e, probe.childID), "the committed parent result does not name child %q", probe.childID)
				} else {
					err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
						return h.Interrupt(ctx, session.Identity.SessionID)
					})
					testassert.NoError(t, err)
					awaitOperation(t, r, session.Identity.SessionID, "probe-parent-op", row.wantParent)
				}

				release()
				awaitOperation(t, r, probe.childID, childOpID, harness.OperationSuccess)
				testassert.Equal(t, bridgeChildCount(t, r, store), 1)
				childHeader, err = r.harness.ReadSessionHeader(context.Background(), probe.childID)
				testassert.NoError(t, err)
				testassert.Equal(t, childHeader.Identity.OwnerSessionID, session.Identity.SessionID)
			})
		})
	}
}
