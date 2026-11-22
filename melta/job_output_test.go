package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/protocol"
)

// TestBackgroundJobOutputLiveReadAndNotices drives one real run_command
// background job through the owner: the starting call's result metadata names
// the job, the job's output grows through the live read and its notices, and
// after the job leaves Live every unknown, foreign or malformed identity is the
// same not-found class while a caller without the Session grant is denied.
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
			if kind, _ := event.Discriminator(); kind == "job_output_changed" {
				notices <- event
			}
		}
	}()

	submitThroughRuntime(t, r, sessionID, "op-job", "run")

	// The start result's metadata names the job; the assistant item carries it
	// once the tool result commits.
	var jobID string
	deadline := time.Now().Add(20 * time.Second)
	for jobID == "" {
		page, err := r.GetHistory(ctx, caller, sessionID, nil)
		testassert.Require(t, err == nil, "GetHistory: %v", err)
		for _, item := range page.Items {
			assistant, err := item.AsAssistantItem()
			if err != nil {
				continue
			}
			for _, call := range assistant.ToolCalls {
				if call.Id != "call-job" || call.Metadata == nil {
					continue
				}
				var metadata struct {
					JobID string `json:"job_id"`
				}
				testassert.Require(t, json.Unmarshal(*call.Metadata, &metadata) == nil, "call metadata = %s, want a job_id object", *call.Metadata)
				jobID = metadata.JobID
				testassert.Require(t, jobID != "" && string(*call.Metadata) == fmt.Sprintf(`{"job_id":%q}`, jobID), "call metadata = %s, want the exact job identity", *call.Metadata)
			}
		}
		testassert.Require(t, time.Now().Before(deadline), "the start result's metadata never named the job")
		if jobID == "" {
			time.Sleep(10 * time.Millisecond)
		}
	}

	other, err := r.createSession(ctx, t.TempDir(), "")
	testassert.NoError(t, err)
	otherSession := other.Identity.SessionID
	var outputs []string
	deadline = time.Now().Add(20 * time.Second)
	for {
		output, err := r.GetSessionJobOutput(ctx, caller, sessionID, jobID)
		testassert.Require(t, err == nil, "GetSessionJobOutput = (%+v, %v)", output, err)
		testassert.Require(t, output.Output != "(No output yet)", "live job output = %q, want the capture preview rather than the read placeholder", output.Output)
		if len(outputs) == 0 || outputs[len(outputs)-1] != output.Output {
			outputs = append(outputs, output.Output)
		}
		if strings.Contains(output.Output, "second") {
			testassert.Require(t, slices.Contains(r.jobs.Live(sessionID), jobID), "Live = %v, want the job still running", r.jobs.Live(sessionID))
			_, err := r.GetSessionJobOutput(ctx, caller, otherSession, jobID)
			testassert.Require(t, errors.Is(err, harness.ErrNotFound), "another Session's running job identity = %v, want not_found", err)
			break
		}
		testassert.Require(t, time.Now().Before(deadline), "the running job output never grew (outputs %v)", outputs)
		time.Sleep(10 * time.Millisecond)
	}
	testassert.Require(t, len(outputs) >= 2 && slices.Contains(outputs, "first") && strings.Contains(outputs[len(outputs)-1], "second"), "live outputs = %q, want output that grew from first to second while the job ran", outputs)

	var noticeScopes []string
	deadline = time.Now().Add(20 * time.Second)
	for len(noticeScopes) < 2 {
		select {
		case received := <-notices:
			body, err := received.AsJobOutputChangedEvent()
			testassert.NoError(t, err)
			scope, err := body.Scope.AsJobScope()
			testassert.NoError(t, err)
			testassert.Require(t, scope.Kind == protocol.JobScopeKindJob && scope.SessionId == sessionID && scope.JobId == jobID && scope.Workspace != nil && *scope.Workspace != "", "job output notice scope = %+v, want the running job", scope)
			noticeScopes = append(noticeScopes, scope.JobId)
		case <-time.After(time.Until(deadline)):
			t.Fatalf("job output notices = %d, want one per written interval", len(noticeScopes))
		}
	}
	sub.Close()
	<-noticesClosed

	// The job leaves Live when its process exits; its output stays readable
	// until its record is removed, and the record is removed after its
	// completion delivery returns. Poll the read to its not-found state.
	deadline = time.Now().Add(20 * time.Second)
	for slices.Contains(r.jobs.Live(sessionID), jobID) {
		testassert.Require(t, time.Now().Before(deadline), "Live still lists the finished job %q", jobID)
		time.Sleep(10 * time.Millisecond)
	}
	for {
		_, err := r.GetSessionJobOutput(ctx, caller, sessionID, jobID)
		if errors.Is(err, harness.ErrNotFound) {
			break
		}
		testassert.Require(t, err == nil, "finished job output = %v, want not_found", err)
		testassert.Require(t, time.Now().Before(deadline), "finished job %q output never became not_found", jobID)
		time.Sleep(10 * time.Millisecond)
	}

	{
		_, err := r.GetSessionJobOutput(ctx, caller, sessionID, "not a job")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "malformed job identity = %v, want not_found", err)
	}
	{
		_, err := r.GetSessionJobOutput(ctx, caller, sessionID, "")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "missing job identity = %v, want not_found", err)
	}
	denied, err := r.Caller(CallerContext{Origin: harness.InputOriginUser, Scope: CallerScope{Kind: CallerScopeSession, SessionID: otherSession}})
	testassert.NoError(t, err)
	{
		_, err := r.GetSessionJobOutput(ctx, denied, sessionID, jobID)
		testassert.Require(t, errors.Is(err, ErrDenied), "caller without the Session grant = %v, want denied", err)
	}
}
