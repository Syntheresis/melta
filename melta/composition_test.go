package melta

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/Syntheresis/melta/internal/testassert"
)

func TestCompositionRejectsScopeInfoShapes(t *testing.T) {
	for _, row := range []struct {
		name string
		info ScopeInfo
	}{
		{"missing data directory", ScopeInfo{Kind: ScopeRuntime}},
		{"runtime with workspace", ScopeInfo{Kind: ScopeRuntime, DataDir: "/d", Workspace: "/w"}},
		{"runtime with session", ScopeInfo{Kind: ScopeRuntime, DataDir: "/d", SessionID: "0123456789abcdef0123456789abcdef"}},
		{"workspace without workspace", ScopeInfo{Kind: ScopeWorkspace, DataDir: "/d"}},
		{"workspace with session", ScopeInfo{Kind: ScopeWorkspace, DataDir: "/d", Workspace: "/w", SessionID: "0123456789abcdef0123456789abcdef"}},
		{"operation without operation", ScopeInfo{Kind: ScopeOperation, DataDir: "/d", Workspace: "/w", SessionID: "0123456789abcdef0123456789abcdef"}},
		{"invocation without invocation", ScopeInfo{Kind: ScopeAgent, DataDir: "/d", Workspace: "/w", SessionID: "0123456789abcdef0123456789abcdef", OperationID: "op"}},
		{"invocation without purpose", ScopeInfo{Kind: ScopeAgent, DataDir: "/d", Workspace: "/w", SessionID: "0123456789abcdef0123456789abcdef", OperationID: "op", InvocationID: "inv"}},
		{"unknown kind", ScopeInfo{Kind: ScopeKind("project"), DataDir: "/d"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := mustComposition(t)
			_, err := c.openScope(context.Background(), row.info, nil)
			testassert.Require(t, errors.Is(err, ErrComposition), "openScope error = %v, want ErrComposition", err)
		})
	}
}

func TestScopeGuardOwnsAdmittedCallsThroughClose(t *testing.T) {
	t.Run("closure joins the admitted call before disposing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			trace := &traceLog{}
			gate := make(chan struct{})
			c := mustComposition(t)
			owner := context.Background()
			rt := mustOpenScope(t, c, owner, runtimeScopeInfo(), nil)
			ws := mustOpenScope(t, c, owner, workspaceScopeInfo("/w"), []*scope{rt})
			gctx, release, err := ws.enter(context.Background())
			testassert.NoError(t, err)
			done := make(chan struct{})
			go func() {
				trace.add("call-start")
				<-gate
				trace.add("call-end")
				release()
				close(done)
			}()
			synctest.Wait()
			testassert.Require(t, !slices.Contains(trace.all(), "call-end"), "the admitted call finished before the gate released")
			closeResult := make(chan error, 1)
			go func() { closeResult <- ws.close() }()
			synctest.Wait()
			testassert.Require(t, errors.Is(gctx.Err(), context.Canceled), "scope closure did not cancel the admitted call context")
			select {
			case err := <-closeResult:
				t.Fatalf("close completed without joining the admitted call: %v", err)
			default:
			}
			close(gate)
			<-done
			testassert.NoError(t, <-closeResult)
			_, _, err = ws.enter(context.Background())
			testassert.Check(t, errors.Is(err, ErrClosed), "enter after closure: %v, want ErrClosed", err)
			testassert.NoError(t, rt.close())
		})
	})

	t.Run("owner cancellation before admission makes the scope unavailable", func(t *testing.T) {
		owner, cancelOwner := context.WithCancel(context.Background())
		c := mustComposition(t)
		ws := mustOpenScope(t, c, owner, workspaceScopeInfo("/w"), nil)
		cancelOwner()
		_, _, err := ws.enter(context.Background())
		testassert.Check(t, errors.Is(err, ErrClosed), "enter on an owner-canceled scope: %v, want ErrClosed", err)
		testassert.NoError(t, ws.close())
	})

	t.Run("canceled caller on a live scope keeps its own error", func(t *testing.T) {
		c := mustComposition(t)
		ws := mustOpenScope(t, c, context.Background(), workspaceScopeInfo("/w"), nil)
		caller, cancelCaller := context.WithCancel(context.Background())
		cancelCaller()
		_, _, err := ws.enter(caller)
		testassert.Check(t, errors.Is(err, context.Canceled), "enter from a canceled caller: %v, want context.Canceled", err)
		testassert.NoError(t, ws.close())
	})
}

// pauseWorkspacePublication holds the observation section so a workspace
// construction stays pending while a test inspects or cancels its waiters.
// Scope construction itself has no slow factory step to park on.
func pauseWorkspacePublication(w *workspaceScopes) func() {
	w.obs.mu.Lock()
	released := false
	return func() {
		if !released {
			released = true
			w.obs.mu.Unlock()
		}
	}
}

func TestWorkspaceScopeAttemptsAreSharedPerKey(t *testing.T) {
	t.Run("waiters share one construction and a canceled waiter does not cancel it", func(t *testing.T) {
		c := mustComposition(t)
		w := newWorkspaceScopes(context.Background(), c, nil, newObservation())
		releasePublication := pauseWorkspacePublication(w)
		defer releasePublication()
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		first := make(chan error, 1)
		go func() {
			_, err := w.get(firstCtx, workspaceScopeInfo("/w"))
			first <- err
		}()
		second := make(chan struct {
			sc  *scope
			err error
		}, 1)
		go func() {
			sc, err := w.get(context.Background(), workspaceScopeInfo("/w"))
			second <- struct {
				sc  *scope
				err error
			}{sc, err}
		}()
		// The single build goroutine opens the scope; both waiters join the
		// one attempt. Canceling the first waiter must not disturb it.
		eventually(t, "both waiters to be parked on the shared attempt", func() bool {
			w.mu.Lock()
			_, pending := w.attempts["/w"]
			w.mu.Unlock()
			return pending
		})
		cancelFirst()
		testassert.Require(t, errors.Is(<-first, context.Canceled), "canceled waiter did not return its own cancellation")
		releasePublication()
		{
			got := <-second
			testassert.NoError(t, got.err)
			again, err := w.get(context.Background(), workspaceScopeInfo("/w"))
			testassert.NoError(t, err)
			testassert.Require(t, again == got.sc, "a later get built a second scope for the same key")
		}
		_, openErr := w.get(context.Background(), ScopeInfo{Kind: ScopeOperation, DataDir: "/owner/data", Workspace: "/w"})
		testassert.Require(t, errors.Is(openErr, ErrComposition), "get with a non-workspace kind = %v, want ErrComposition", openErr)
		testassert.NoError(t, w.shutdown())
	})

	t.Run("an owner-canceled construction publishes nothing and a later get reports the closure", func(t *testing.T) {
		owner, cancelOwner := context.WithCancel(context.Background())
		c := mustComposition(t)
		w := newWorkspaceScopes(owner, c, nil, newObservation())
		releasePublication := pauseWorkspacePublication(w)
		defer releasePublication()
		result := make(chan error, 1)
		go func() {
			_, err := w.get(context.Background(), workspaceScopeInfo("/ws/test"))
			result <- err
		}()
		eventually(t, "the attempt to be registered", func() bool {
			w.mu.Lock()
			_, pending := w.attempts["/ws/test"]
			w.mu.Unlock()
			return pending
		})
		cancelOwner()
		releasePublication()
		err := <-result
		testassert.Require(t, errors.Is(err, context.Canceled), "rejected construction = %v", err)
		_, err = w.get(context.Background(), workspaceScopeInfo("/ws/test"))
		testassert.Require(t, err != nil, "a rejected attempt served a live scope")
		testassert.NoError(t, w.shutdown())
	})
}

func TestWorkspaceConstructionAbandonedWaiterKeepsCancellation(t *testing.T) {
	owner := context.Background()
	c := mustComposition(t)
	w := newWorkspaceScopes(owner, c, nil, newObservation())
	releasePublication := pauseWorkspacePublication(w)
	defer releasePublication()
	caller, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()
	result := make(chan error, 1)
	go func() { _, err := w.get(caller, workspaceScopeInfo("/ws/test")); result <- err }()
	eventually(t, "the attempt to be registered", func() bool {
		w.mu.Lock()
		_, pending := w.attempts["/ws/test"]
		w.mu.Unlock()
		return pending
	})
	cancelCaller()
	testassert.Require(t, errors.Is(<-result, context.Canceled), "abandoned waiter did not return cancellation")
	releasePublication()
	// The abandoned waiter did not disturb the build: the scope is live.
	scoped, err := w.get(context.Background(), workspaceScopeInfo("/ws/test"))
	testassert.NoError(t, err)
	testassert.Require(t, scoped.info.Workspace == "/ws/test", "abandoned build published %v", scoped.info)
	testassert.NoError(t, w.shutdown())
}
