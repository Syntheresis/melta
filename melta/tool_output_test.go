package melta

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
)

// TestToolOutputLiveRead proves running commands publish readable output and
// scoped notices, and errors keep their classes.
func TestToolOutputLiveRead(t *testing.T) {
	store := storage.NewMemory()
	r, e := openProjectionRuntime(t, store)
	defer closeProjectionRuntime(r)
	e.server.setScript(func(_ context.Context, body string) []string {
		switch {
		case lastMessageRole(body) == "tool" || strings.Contains(body, "Background process "):
			return textTurnEvents("done")
		case strings.Contains(body, "job"):
			return toolCallTurnEvents("call-job", "run_command", `{"command":"printf a; sleep 1; printf b; sleep 1","background":true}`)
		default:
			return toolCallTurnEvents("call-live", "run_command", `{"command":"printf a; sleep 1; printf b; sleep 1"}`)
		}
	})
	ctx, caller := context.Background(), testCaller(r)
	sessionID := createHeldSession(t, r, t.TempDir(), []string{"run_command"}).Identity.SessionID
	sub, err := r.Subscribe(64)
	testassert.NoError(t, err)
	defer sub.Close()
	notices := make(chan Event, 64)
	go func() {
		for event := range sub.Events() {
			if kind, _ := event.Discriminator(); kind == "tool_output_changed" {
				notices <- event
			}
		}
	}()
	await := func(operationID, callID string) time.Time {
		var event Event
		select {
		case event = <-notices:
		case <-time.After(10 * time.Second):
			t.Fatalf("no tool_output_changed notice for call %q in Operation %q", callID, operationID)
		}
		body, _ := event.AsToolOutputChangedEvent()
		scope, _ := body.Scope.AsOperationScope()
		testassert.Require(t, body.CallId == callID && scope.SessionId == sessionID && scope.OperationId == operationID && scope.Workspace != nil && *scope.Workspace != "", "notice = (%q, %+v), want call %q in the Operation with its Workspace", body.CallId, scope, callID)
		return time.Now()
	}
	// readLive polls the running output until want appears or the deadline passes.
	readLive := func(operationID, callID, want string) {
		deadline := time.Now().Add(10 * time.Second)
		var got string
		for {
			output, err := r.GetSessionToolOutput(ctx, caller, sessionID, operationID, callID)
			switch {
			case err == nil:
				got = output.Output
				if got == want {
					return
				}
			case errors.Is(err, harness.ErrNotFound):
			default:
				t.Fatalf("GetSessionToolOutput = %v", err)
			}
			testassert.Require(t, time.Now().Before(deadline), "running output = %q, want %q", got, want)
			time.Sleep(10 * time.Millisecond)
		}
	}

	submitThroughRuntime(t, r, sessionID, "op-live", "foreground")
	first := await("op-live", "call-live")
	readLive("op-live", "call-live", "a")
	second := await("op-live", "call-live")
	readLive("op-live", "call-live", "ab")
	testassert.Require(t, second.Sub(first) >= 240*time.Millisecond, "notices %s apart, want at most one per 250 ms", second.Sub(first))
	awaitOperation(t, r, sessionID, "op-live", harness.OperationSuccess)

	other := projectionSession(t, r, t.TempDir(), "solo").Identity.SessionID
	denied, err := r.Caller(CallerContext{Origin: harness.InputOriginUser, Scope: CallerScope{Kind: CallerScopeSession, SessionID: other}})
	testassert.NoError(t, err)
	for _, row := range [][3]string{
		{sessionID, "op-live", "call-live"}, {sessionID, "op-other", "call-live"}, {other, "op-live", "call-live"},
		{sessionID, "op-live", "not a call"}, {sessionID, "", "call-live"},
	} {
		_, err := r.GetSessionToolOutput(ctx, caller, row[0], row[1], row[2])
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "GetSessionToolOutput(%q, %q) = %v, want not_found", row[1], row[2], err)
	}
	_, err = r.GetSessionToolOutput(ctx, denied, sessionID, "op-live", "call-live")
	testassert.Require(t, errors.Is(err, ErrDenied), "ungranted caller = %v, want denied", err)

	submitThroughRuntime(t, r, sessionID, "op-job", "job")
	await("op-job", "call-job")
	readLive("op-job", "call-job", "a")
	deadline := time.Now().Add(20 * time.Second)
	for {
		page, err := r.GetHistory(ctx, caller, sessionID, nil)
		testassert.Require(t, err == nil && time.Now().Before(deadline), "the job's completion was never delivered (%v)", err)
		if hasRuntimeOriginInput(t, page.Items, "Background process ") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for {
		_, err := r.GetSessionToolOutput(ctx, caller, sessionID, "op-job", "call-job")
		if errors.Is(err, harness.ErrNotFound) {
			break
		}
		testassert.Require(t, err == nil && time.Now().Before(deadline), "delivered job output = %v, want not_found", err)
		time.Sleep(10 * time.Millisecond)
	}

	const corruptID = "0123456789abcdef0123456789abcdef"
	lifecycleInsertRegister(t, store, harness.RegisterKey{SessionID: corruptID, Kind: harness.RegisterSession}, `{"not":"a session register"}`)
	_, err = r.GetSessionToolOutput(ctx, caller, corruptID, "op", "call")
	testassert.Require(t, errors.Is(err, harness.ErrCorrupt), "corrupt Session = %v, want ErrCorrupt", err)
	r.toolOutput.register(other, "op", "call", func() string { corruptSessionRegister(t, store, other); return "live" })
	output, err := r.GetSessionToolOutput(ctx, caller, other, "op", "call")
	var corrupt *harness.CorruptionError
	testassert.Require(t, errors.Is(err, harness.ErrCorrupt) && errors.As(err, &corrupt) && corrupt.SessionID == other && output.Output == "", "preview-corrupted Session = (%q, %v), want the typed corruption", output.Output, err)
}
