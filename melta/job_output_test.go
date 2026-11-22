package melta

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
)

// TestBackgroundJobOutputLiveReadAndNotices drives one real run_command
// background job through the owner: the starting Operation and call address
// the job's growing output through the same live read and notice a foreground
// command uses, and the read is not found once the job's completion has been
// delivered.
func TestBackgroundJobOutputLiveReadAndNotices(t *testing.T) {
	e := newOwnerEnv(t)
	e.fixtureStore(storage.NewMemory())
	e.server.setScript(func(_ context.Context, body string) []string {
		switch {
		case lastMessageRole(body) == "tool":
			return textTurnEvents("done")
		case strings.Contains(body, "Background process "):
			return textTurnEvents("done")
		default:
			return toolCallTurnEvents("call-job", "run_command", `{"command":"sleep 0.5; printf first; sleep 2; printf second; sleep 2","background":true}`)
		}
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

	notices := make(chan Event, 64)
	noticesClosed := make(chan struct{})
	go func() {
		defer close(noticesClosed)
		for event := range sub.Events() {
			if kind, _ := event.Discriminator(); kind == "tool_output_changed" {
				notices <- event
			}
		}
	}()

	submitThroughRuntime(t, r, sessionID, "op-job", "run")

	var outputs []string
	deadline := time.Now().Add(20 * time.Second)
	for {
		output, err := r.GetSessionToolOutput(ctx, caller, sessionID, "op-job", "call-job")
		switch {
		case err == nil:
			if len(outputs) == 0 || outputs[len(outputs)-1] != output.Output {
				outputs = append(outputs, output.Output)
			}
			if strings.Contains(output.Output, "second") {
				goto observed
			}
		case errors.Is(err, harness.ErrNotFound):
		default:
			t.Fatalf("GetSessionToolOutput = (%+v, %v)", output, err)
		}
		testassert.Require(t, time.Now().Before(deadline), "the running job output never grew (outputs %v)", outputs)
		time.Sleep(10 * time.Millisecond)
	}
observed:
	testassert.Require(t, len(outputs) >= 2 && slices.Contains(outputs, "first") && strings.Contains(outputs[len(outputs)-1], "second"), "live outputs = %q, want output that grew from first to second while the job ran", outputs)

	var scopes int
	deadline = time.Now().Add(20 * time.Second)
	for scopes < 2 {
		select {
		case received := <-notices:
			body, err := received.AsToolOutputChangedEvent()
			testassert.NoError(t, err)
			testassert.Equal(t, body.CallId, "call-job")
			scope, err := body.Scope.AsOperationScope()
			testassert.NoError(t, err)
			testassert.Require(t, scope.SessionId == sessionID && scope.OperationId == "op-job" && scope.Workspace != nil && *scope.Workspace != "", "job output notice scope = %+v, want the starting Operation", scope)
			scopes++
		case <-time.After(time.Until(deadline)):
			t.Fatalf("job output notices = %d, want one per written interval", scopes)
		}
	}
	sub.Close()
	<-noticesClosed

	// The completion is delivered before the starting call's registration is
	// removed; the runtime-origin input proves the delivery happened.
	deadline = time.Now().Add(20 * time.Second)
	for {
		page, err := r.GetHistory(ctx, caller, sessionID, nil)
		testassert.Require(t, err == nil, "GetHistory: %v", err)
		if hasRuntimeOriginInput(t, page.Items, "Background process ") {
			break
		}
		testassert.Require(t, time.Now().Before(deadline), "the job's completion was never delivered")
		time.Sleep(10 * time.Millisecond)
	}
	for {
		_, err := r.GetSessionToolOutput(ctx, caller, sessionID, "op-job", "call-job")
		if errors.Is(err, harness.ErrNotFound) {
			break
		}
		testassert.Require(t, err == nil, "delivered job output = %v, want not_found", err)
		testassert.Require(t, time.Now().Before(deadline), "the delivered job output never became not_found")
		time.Sleep(10 * time.Millisecond)
	}
}
