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
	"github.com/Syntheresis/melta/protocol"
)

// TestForegroundToolOutputLiveReadAndNotices drives one real run_command
// through the owner: while the command runs its output grows through the live
// read, the change notices stay at most one per 250 ms, and after settlement
// every unknown, foreign or malformed identity is the same not-found class
// while a caller without the Session grant is denied.
func TestForegroundToolOutputLiveReadAndNotices(t *testing.T) {
	e := newOwnerEnv(t)
	e.fixtureStore(storage.NewMemory())
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-output", "run_command", `{"command":"printf first; sleep 1; printf second; sleep 1"}`)
	})
	ctx := context.Background()
	r, err := e.open(ctx)
	testassert.NoError(t, err)
	defer func() { _ = r.Close(ctx) }()
	session := createHeldSession(t, r, t.TempDir(), []string{"run_command"})
	sessionID := session.Identity.SessionID
	caller := testCaller(r)
	sub, err := r.Subscribe(256)
	testassert.NoError(t, err)

	type notice struct {
		at    time.Time
		event Event
	}
	notices := make(chan notice, 64)
	noticesClosed := make(chan struct{})
	go func() {
		defer close(noticesClosed)
		for event := range sub.Events() {
			if kind, _ := event.Discriminator(); kind == "tool_output_changed" {
				notices <- notice{at: time.Now(), event: event}
			}
		}
	}()

	submitThroughRuntime(t, r, sessionID, "op-live", "run")
	var outputs []string
	deadline := time.Now().Add(20 * time.Second)
	for {
		output, err := r.GetSessionToolOutput(ctx, caller, sessionID, "op-live", "call-output")
		switch {
		case err == nil:
			if len(outputs) == 0 || outputs[len(outputs)-1] != output.Output {
				outputs = append(outputs, output.Output)
			}
			if strings.Contains(output.Output, "second") {
				operation, readErr := r.GetSessionOperation(ctx, caller, sessionID, "op-live")
				testassert.Require(t, readErr == nil && operation.Operation.Status == protocol.OperationStatusRunning, "Operation = (%+v, %v), want the command's Operation still running", operation.Operation, readErr)
				goto settled
			}
		case errors.Is(err, harness.ErrNotFound):
		default:
			t.Fatalf("GetSessionToolOutput = (%+v, %v)", output, err)
		}
		testassert.Require(t, time.Now().Before(deadline), "the running command output never grew (outputs %v)", outputs)
		time.Sleep(10 * time.Millisecond)
	}
settled:
	testassert.Require(t, len(outputs) >= 2, "live outputs = %q, want output that grew while the command ran", outputs)
	awaitOperation(t, r, sessionID, "op-live", harness.OperationSuccess)
	sub.Close()
	<-noticesClosed

	var times []time.Time
	for {
		select {
		case received := <-notices:
			body, err := received.event.AsToolOutputChangedEvent()
			testassert.NoError(t, err)
			testassert.Equal(t, body.CallId, "call-output")
			scope, err := body.Scope.AsOperationScope()
			testassert.NoError(t, err)
			testassert.Require(t, scope.SessionId == sessionID && scope.OperationId == "op-live" && scope.Workspace != nil && *scope.Workspace != "", "notice scope = %+v, want the running Operation", scope)
			times = append(times, received.at)
		default:
			goto drained
		}
	}
drained:
	testassert.Require(t, len(times) >= 2, "tool output notices = %d, want one per written interval", len(times))
	for i := 1; i < len(times); i++ {
		testassert.Require(t, times[i].Sub(times[i-1]) >= 240*time.Millisecond, "tool output notices %s apart, want at most one per 250 ms", times[i].Sub(times[i-1]))
	}

	{
		_, err := r.GetSessionToolOutput(ctx, caller, sessionID, "op-live", "call-output")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "settled call output = %v, want not_found", err)
	}
	{
		_, err := r.GetSessionToolOutput(ctx, caller, sessionID, "op-other", "call-output")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "same call under another Operation = %v, want not_found", err)
	}
	other, err := r.createSession(ctx, t.TempDir(), "")
	testassert.NoError(t, err)
	otherSession := other.Identity.SessionID
	{
		_, err := r.GetSessionToolOutput(ctx, caller, otherSession, "op-live", "call-output")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "another Session's call = %v, want not_found", err)
	}
	{
		_, err := r.GetSessionToolOutput(ctx, caller, sessionID, "op-live", "not a call")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "malformed call identity = %v, want not_found", err)
	}
	{
		_, err := r.GetSessionToolOutput(ctx, caller, sessionID, "", "call-output")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "missing Operation identity = %v, want not_found", err)
	}
	denied, err := r.Caller(CallerContext{Origin: harness.InputOriginUser, Scope: CallerScope{Kind: CallerScopeSession, SessionID: otherSession}})
	testassert.NoError(t, err)
	{
		_, err := r.GetSessionToolOutput(ctx, denied, sessionID, "op-live", "call-output")
		testassert.Require(t, errors.Is(err, ErrDenied), "caller without the Session grant = %v, want denied", err)
	}
}

// TestGetSessionToolOutputReadsSessionAvailability proves the live read keeps
// the Session availability class: the preview is captured before the Session
// check, so a Session already unavailable or made unavailable by the preview
// call answers with that class instead of the preview text.
func TestGetSessionToolOutputReadsSessionAvailability(t *testing.T) {
	ctx := context.Background()
	t.Run("corrupt before the read", func(t *testing.T) {
		const corruptID = "0123456789abcdef0123456789abcdef"
		store := storage.NewMemory()
		lifecycleInsertRegister(t, store, harness.RegisterKey{SessionID: corruptID, Kind: harness.RegisterSession}, `{"not":"a session register"}`)
		r, _ := openProjectionRuntime(t, store)
		defer closeProjectionRuntime(r)
		r.toolOutput.register(corruptID, "op-live", "call-live", func() string { return "live" })

		caller := testCaller(r)
		_, operationErr := r.GetSessionOperation(ctx, caller, corruptID, "op-live")
		testassert.Require(t, errors.Is(operationErr, harness.ErrCorrupt), "GetSessionOperation = %v, want the corruption class", operationErr)
		_, outputErr := r.GetSessionToolOutput(ctx, caller, corruptID, "op-live", "call-live")
		testassert.Require(t, errors.Is(outputErr, harness.ErrCorrupt), "GetSessionToolOutput = %v, want the same corruption class", outputErr)
		var corruptErr *harness.CorruptionError
		testassert.Require(t, errors.As(outputErr, &corruptErr) && corruptErr.SessionID == corruptID, "GetSessionToolOutput error = %v, want the typed corruption for session %s", outputErr, corruptID)
	})

	t.Run("preview makes the session corrupt", func(t *testing.T) {
		const sessionID = "fedcba9876543210fedcba9876543210"
		store := storage.NewMemory()
		seedLifecycleRoot(t, store, sessionID)
		r, _ := openProjectionRuntime(t, store)
		defer closeProjectionRuntime(r)
		r.toolOutput.register(sessionID, "op-live", "call-live", func() string {
			corruptSessionRegister(t, store, sessionID)
			return "live"
		})

		output, err := r.GetSessionToolOutput(ctx, testCaller(r), sessionID, "op-live", "call-live")
		testassert.Require(t, errors.Is(err, harness.ErrCorrupt), "GetSessionToolOutput = (%q, %v), want the corruption class the preview made", output.Output, err)
		var corruptErr *harness.CorruptionError
		testassert.Require(t, errors.As(err, &corruptErr) && corruptErr.SessionID == sessionID, "GetSessionToolOutput error = %v, want the typed corruption for session %s", err, sessionID)
		testassert.Require(t, output.Output == "", "GetSessionToolOutput output = %q, want no preview text", output.Output)
	})
}
