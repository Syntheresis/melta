// Contract coverage for the single Harness model-effect path over the fixed
// physical transport: real httptest requests drive model.Transport as the
// Execution.Model callback, zero-delay policies and cancellation prove the
// attempt loop, and the private standard classifier is pinned directly. Every
// settlement here is Harness-derived from the accepted stream; no test
// supplies one.
package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/agent"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// transportAttempt binds the fixed model.Transport to the Execution.Model
// physical-request shape: exactly one physical attempt per invocation. The
// protocol-warning return is dropped until its Phase 7 observation consumer
// exists.
func transportAttempt(tr *model.Transport) func(context.Context, model.Request, model.Extra) (model.Stream, error) {
	return func(ctx context.Context, req model.Request, _ model.Extra) (model.Stream, error) {
		stream, _, err := tr.Stream(ctx, req, nil)
		return stream, err
	}
}

// scriptedResponses is the fixed response script of one httptest chat server:
// each request consumes the next entry, and the last entry repeats when the
// script is exhausted. A non-200 status answers the plain body text; a 200
// streams the events as SSE data lines followed by [DONE].
type scriptedResponses struct {
	status int
	events []string
}

// sseChatServer runs the script over one httptest server and reports every
// request through the returned atomic counter and buffered channel.
func sseChatServer(t *testing.T, script ...scriptedResponses) (*httptest.Server, *atomic.Int64, <-chan struct{}) {
	t.Helper()
	var hits atomic.Int64
	hits.Store(0)
	requests := make(chan struct{}, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		select {
		case requests <- struct{}{}:
		default:
		}
		response := script[len(script)-1]
		if int(n) <= len(script) {
			response = script[n-1]
		}
		if response.status != http.StatusOK {
			http.Error(w, "provider says no", response.status)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range response.events {
			fmt.Fprintf(w, "data: %s\n\n", event)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv, &hits, requests
}

// transportExecution builds the effect fixtures' execution whose Model is the
// real transport bound to the given server.
func transportExecution(srv *httptest.Server, retry RetryPolicy) Execution {
	tr, err := model.NewTransport(model.ResolvedTransport{Model: testModelRef(), BaseURL: srv.URL})
	if err != nil {
		panic(fmt.Sprintf("transport construction failed for a valid resolved input: %v", err))
	}
	exec := effectExecution(transportAttempt(tr), nil)
	exec.Retry = retry
	return exec
}

// retrySpy is the fixtures' captured retry policy: up to limit retries of any
// failure with zero delay, recording every observed failure and 1-based
// attempt number.
type retrySpy struct {
	limit    int
	seen     []error
	attempts []int
}

func (s *retrySpy) retry(cause error, failed int) (time.Duration, bool) {
	s.seen = append(s.seen, cause)
	s.attempts = append(s.attempts, failed)
	if failed <= s.limit {
		return 0, true
	}
	return 0, false
}

// statusErrorOf asserts the failure is the transport's 429 status error.
func statusErrorOf(t *testing.T, cause error) *model.HTTPStatusError {
	t.Helper()
	var status *model.HTTPStatusError
	testassert.Require(t, errors.As(cause, &status) && status.StatusCode == http.StatusTooManyRequests, "attempt failure %v, want the transport's 429 status error", cause)
	return status
}

// requireIntentAndResultCounts asserts the durable shape of one real settled
// turn: exactly one terminal settlement and, when the accepted output carries a
// payload, exactly one assistant entry consuming the intent; the intent is
// cleared.
func requireIntentAndResultCounts(t *testing.T, h *Harness, store *graphStorage, sessionID string, assistant bool) {
	t.Helper()
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	assistants, settlements := 0, 0
	for _, entry := range graph.Entries {
		if entry.Assistant != nil {
			assistants++
		}
		if entry.Settlement != nil {
			settlements++
		}
	}
	if assistant {
		testassert.Require(t, assistants == 1 && settlements == 1, "committed %d assistants and %d settlements, want the one payload entry beside the terminal", assistants, settlements)
	} else {
		testassert.Require(t, assistants == 0 && settlements == 1, "committed %d assistants and %d settlements, want the one terminal without a payload entry", assistants, settlements)
	}
	rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, err)
	testassert.Require(t, rec.State.Terminal != nil && rec.State.ActiveEffect == nil, "settled run = %+v, want a terminal and no active intent", rec.State)
}

// TestModelEffectRetriesTransportFailures proves the retry row over the real
// transport: two 429 responses retry under the captured zero-delay policy with
// 1-based attempt numbers, the accepted stream settles ready through exactly
// one assembly, and one intent/result pair commits.
func TestModelEffectRetriesTransportFailures(t *testing.T) {
	srv, hits, _ := sseChatServer(t,
		scriptedResponses{status: http.StatusTooManyRequests},
		scriptedResponses{status: http.StatusTooManyRequests},
		scriptedResponses{status: http.StatusOK, events: []string{ // one completed text turn reporting usage
			`{"choices":[{"delta":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}`,
		}},
	)
	spy := &retrySpy{limit: 2}
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := transportExecution(srv, spy.retry)
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	if hits.Load() != 3 {
		t.Fatalf("physical requests = %d, want the two retries plus the accepted attempt", hits.Load())
	}
	testassert.Require(t, len(spy.attempts) == 2 && spy.attempts[0] == 1 && spy.attempts[1] == 2, "attempt numbers = %v, want the 1-based failed attempts [1 2]", spy.attempts)
	statusErrorOf(t, spy.seen[0])
	statusErrorOf(t, spy.seen[1])
	requireIntentAndResultCounts(t, h, store, sessionID, true)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var assistant *assistantEntry
	for i := range graph.Entries {
		if graph.Entries[i].Assistant != nil {
			assistant = graph.Entries[i].Assistant
		}
	}
	testassert.Require(t, assistant != nil && assistant.Status == model.OutputCompleted && assistant.Content[0].Text == "done", "assistant entry = %+v, want the accepted turn's completed payload", assistant)
	testassert.Require(t, assistant.Usage != nil && *assistant.Usage == (UsageCount{InputTokens: 3, OutputTokens: 2}), "assistant usage = %+v, want the accepted attempt's report", assistant.Usage)
	testassert.Require(t, assistant.RetryUsage == nil && !assistant.RetryUsageUnknown, "assistant retry usage = %+v (unknown %v), want none because every pre-acceptance failure reported nothing", assistant.RetryUsage, assistant.RetryUsageUnknown)
	wantTotals := UsageTotals{ByModel: []ModelUsage{{Model: testModelRef(), Usage: UsageCount{InputTokens: 3, OutputTokens: 2}}}}
	testassert.Require(t, usageTotalsEqual(graph.Session.State.Usage, wantTotals), "session usage = %+v, want the accepted attempt only", graph.Session.State.Usage)

	t.Run("non-retryable status stops with a reportless settlement", func(t *testing.T) {
		srv, hits, _ := sseChatServer(t, scriptedResponses{status: http.StatusUnauthorized})
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := transportExecution(srv, nil)
		testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		})))
		testassert.Equal(t, hits.Load(), int64(1))
		requireIntentAndResultCounts(t, h, store, sessionID, false)
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Status == OperationFailure && settlement.Model != nil && *settlement.Model == testModelRef() && settlement.Usage == nil && settlement.RetryUsage == nil && !settlement.RetryUsageUnknown, "settlement = %+v, want the conversation model identity with no usage report and no retry members", settlement)
	})
}

// TestModelEffectExhaustedRetriesSettleFailure proves the exhaustion row: an
// always-failing provider with a bounded policy settles the committed failure
// settlement with the transport's diagnostic, returned with a nil error, and
// the durable terminal carries exactly one settlement entry.
func TestModelEffectExhaustedRetriesSettleFailure(t *testing.T) {
	srv, hits, _ := sseChatServer(t, scriptedResponses{status: http.StatusTooManyRequests})
	spy := &retrySpy{limit: 2}
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := transportExecution(srv, spy.retry)
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	if hits.Load() != 3 {
		t.Fatalf("physical requests = %d, want the initial attempt plus both retries", hits.Load())
	}
	testassert.Require(t, len(spy.attempts) == 3 && spy.attempts[2] == 3, "attempt numbers = %v, want the policy consulted through attempt 3", spy.attempts)
	requireIntentAndResultCounts(t, h, store, sessionID, false)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	settlement := settlementEntryOf(t, graph)
	testassert.Require(t, settlement.Status == OperationFailure && strings.Contains(settlement.Detail, "429"), "settlement = %+v, want the committed outputless failure with its 429 diagnostic", settlement)
}

// TestModelEffectConnectionFailureRetries proves the connection-failure row:
// a transport failure wrapping net.OpError is retryable under the captured
// policy, and an unretryable classification stops with the failure settlement.
func TestModelEffectConnectionFailureRetries(t *testing.T) {
	srv, _, _ := sseChatServer(t, scriptedResponses{status: http.StatusOK})
	srv.Close() // every dial now fails with a connection error wrapping net.OpError

	t.Run("retryable connection failure exhausts through the policy", func(t *testing.T) {
		spy := &retrySpy{limit: 1}
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := transportExecution(srv, spy.retry)
		testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		})))
		testassert.Require(t, len(spy.attempts) == 2 && spy.attempts[0] == 1 && spy.attempts[1] == 2, "attempt numbers = %v, want the one allowed retry", spy.attempts)
		var op *net.OpError
		testassert.Require(t, errors.As(spy.seen[0], &op), "attempt failure %v, want the transport's wrapped net.OpError", spy.seen[0])
		requireIntentAndResultCounts(t, h, store, sessionID, false)
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		testassert.Require(t, settlementEntryOf(t, graph).Status == OperationFailure, "settlement = %+v, want the connection failure terminal", settlementEntryOf(t, graph))
	})

	t.Run("unretryable connection failure stops at the first attempt", func(t *testing.T) {
		spy := &retrySpy{limit: 0}
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := transportExecution(srv, spy.retry)
		testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		})))
		testassert.Require(t, len(spy.attempts) == 1, "attempt numbers = %v, want the false classification to stop at attempt 1", spy.attempts)
		requireIntentAndResultCounts(t, h, store, sessionID, false)
	})

	t.Run("a negative delay also means no retry", func(t *testing.T) {
		attempts := 0
		policy := func(error, int) (time.Duration, bool) {
			attempts++
			return -time.Second, true
		}
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := transportExecution(srv, policy)
		testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		})))
		testassert.Equal(t, attempts, 1)
		requireIntentAndResultCounts(t, h, store, sessionID, false)
	})
}

// TestModelEffectAttemptFailureUnderCancellationSettlesInterruption proves
// the pre-acceptance cancellation priority: an attempt failure observed under
// a done execution context settles the committed interruption regardless of
// the attempt error's shape or the retry policy's answer — no output, no
// assembly, and no second attempt.
func TestModelEffectAttemptFailureUnderCancellationSettlesInterruption(t *testing.T) {
	policyCalls := 0
	policy := func(error, int) (time.Duration, bool) {
		policyCalls++
		return 0, true // the supplied policy would retry every failure
	}
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		attempts++
		cancel()                                   // the execution context dies during the attempt
		return nil, errors.New("provider dropped") // not cancellation-shaped
	}
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := effectExecution(modelFn, nil)
	exec.Retry = policy
	testassert.NoError(t, executeDirect(h, c, ctx, testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	testassert.Require(t, attempts == 1 && policyCalls == 0, "attempts = %d policy calls = %d, want the done context to settle before any classification or second attempt", attempts, policyCalls)
	requireIntentAndResultCounts(t, h, store, sessionID, false)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	settlement := settlementEntryOf(t, graph)
	testassert.Require(t, settlement.Status == OperationInterruption && settlement.Detail == executionInterruptedDetail, "settlement = %+v, want the fixed interruption terminal", settlement)
	testassert.Require(t, settlement.Usage == nil && settlement.RetryUsage == nil && !settlement.RetryUsageUnknown, "settlement usage/retry usage = %+v/%+v (unknown %v), want no accepted attempt's members", settlement.Usage, settlement.RetryUsage, settlement.RetryUsageUnknown)
}

// TestModelEffectNilRetryCancelsDuringStandardBackoff proves nil Retry selects
// the private standard classifier and the Harness owns the wait: the first
// 429 enters the standard 2s backoff, observed execution cancellation during
// the backoff settles the committed interruption with no further request and
// no assembly.
func TestModelEffectNilRetryCancelsDuringStandardBackoff(t *testing.T) {
	srv, hits, requests := sseChatServer(t, scriptedResponses{status: http.StatusTooManyRequests})
	h, store, c, sessionID := newEffectHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-requests // the first attempt failed into the standard backoff
		time.Sleep(50 * time.Millisecond)
		cancel() // observed execution cancellation during the Harness-owned wait
	}()
	exec := transportExecution(srv, nil)
	started := time.Now()
	testassert.NoError(t, executeDirect(h, c, ctx, testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	elapsed := time.Since(started)
	testassert.Require(t, elapsed < 2*time.Second, "cancellation during the 2s backoff returned after %v, want the wait interrupted promptly", elapsed)
	if hits.Load() != 1 {
		t.Fatalf("physical requests = %d, want no second attempt after the cancellation", hits.Load())
	}
	requireIntentAndResultCounts(t, h, store, sessionID, false)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	settlement := settlementEntryOf(t, graph)
	testassert.Require(t, settlement.Status == OperationInterruption && settlement.Detail == executionInterruptedDetail, "settlement = %+v, want the fixed interruption terminal", settlement)
}

// TestModelEffectStandardClassifierRows pins the private standard classifier:
// HTTP 429/5xx, errors wrapping net.OpError, errors matching
// model.ErrStreamBroken, and pre-response io.EOF/io.ErrUnexpectedEOF permit
// retries 1..3 at 2s/4s/8s; every other failure, including an in-stream
// protocol failure, stops.
func TestModelEffectStandardClassifierRows(t *testing.T) {
	retryable := []error{
		&model.HTTPStatusError{StatusCode: 429, StatusText: "429"},
		&model.HTTPStatusError{StatusCode: 500, StatusText: "500"},
		&model.HTTPStatusError{StatusCode: 503, StatusText: "503"},
		&net.OpError{Op: "dial"},
		fmt.Errorf("post: %w", &net.OpError{Op: "read"}),
		io.EOF,
		io.ErrUnexpectedEOF,
		fmt.Errorf("wrapped: %w", io.ErrUnexpectedEOF),
		model.ErrStreamBroken,
		fmt.Errorf("wrapped: %w", model.ErrStreamBroken),
		&agent.StreamError{Err: fmt.Errorf("body read failed: %w", model.ErrStreamBroken)},
	}
	stopping := []error{
		&model.HTTPStatusError{StatusCode: 400, StatusText: "400"},
		&model.HTTPStatusError{StatusCode: 401, StatusText: "401"},
		&model.HTTPStatusError{StatusCode: 404, StatusText: "404"},
		&model.HTTPStatusError{StatusCode: 600, StatusText: "600"}, // beyond 5xx: the retryable status class is 429 plus 500..599, not every code >= 500
		errors.New("plain failure"),
		context.Canceled,
		model.ErrProtocol,
		&agent.StreamError{Err: model.ErrProtocol},
	}
	for _, cause := range retryable {
		for failed, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second} {
			delay, again := standardRetryPolicy(cause, failed+1)
			testassert.Require(t, again && delay == want, "standardRetryPolicy(%v, %d) = (%v, %v), want (%v, true)", cause, failed+1, delay, again, want)
		}
		{
			_, again := standardRetryPolicy(cause, 4)
			testassert.Require(t, !again, "standardRetryPolicy(%v, 4) retries, want the 1..3 bound to stop", cause)
		}
	}
	for _, cause := range stopping {
		{
			delay, again := standardRetryPolicy(cause, 1)
			testassert.Require(t, !again && delay == 0, "standardRetryPolicy(%v, 1) = (%v, %v), want no retry", cause, delay, again)
		}
	}
}

// brokenAttemptScript returns a physical callback yielding the ordered broken
// accepted streams with their optional reports and then the exhausted policy.
func brokenAttemptScript(reports []*model.Usage) (func(context.Context, model.Request, model.Extra) (model.Stream, error), *int) {
	calls := 0
	return func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		report := reports[calls]
		calls++
		if report == nil {
			return failStream(errors.New("provider failure")), nil
		}
		return failStream(errors.New("provider failure"), usageDelta(*report)), nil
	}, &calls
}

// TestModelEffectExhaustedBrokenStreamsRecordRetryUsage proves the effect's
// accepted-attempt usage record when the retry budget is exhausted by broken
// streams: the last accepted attempt's report stays the record's usage, the
// earlier attempts' reports are its retry usage, a retried attempt that
// reported nothing marks the retry usage unknown, totals include both members,
// and the context measurement reads only usage.
func TestModelEffectExhaustedBrokenStreamsRecordRetryUsage(t *testing.T) {
	u := func(input, output int) *model.Usage { return &model.Usage{InputTokens: input, OutputTokens: output} }
	run := func(t *testing.T, modelFn func(context.Context, model.Request, model.Extra) (model.Stream, error), retry RetryPolicy) (*Harness, *graphStorage, string) {
		t.Helper()
		prepared := preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			exec := effectExecution(modelFn, nil)
			exec.Retry = retry
			return exec, nil
		})
		h, store, c, sessionID := newAdmittedHarness(t, context.Background(), prepared)
		testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, prepared))
		return h, store, sessionID
	}

	t.Run("every retried attempt reported", func(t *testing.T) {
		modelFn, _ := brokenAttemptScript([]*model.Usage{u(1, 1), u(3, 3)})
		h, store, sessionID := run(t, modelFn, (&retrySpy{limit: 1}).retry)
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Status == OperationFailure && settlement.Model != nil && *settlement.Model == testModelRef(), "settlement = %+v, want the conversation model producer", settlement)
		testassert.Require(t, settlement.Usage != nil && *settlement.Usage == (UsageCount{InputTokens: 3, OutputTokens: 3}), "settlement usage = %+v, want the last accepted attempt's report", settlement.Usage)
		testassert.Require(t, settlement.RetryUsage != nil && *settlement.RetryUsage == (UsageCount{InputTokens: 1, OutputTokens: 1}) && !settlement.RetryUsageUnknown, "settlement retry usage = %+v (unknown %v), want the earlier accepted attempt's report", settlement.RetryUsage, settlement.RetryUsageUnknown)
		wantTotals := UsageTotals{ByModel: []ModelUsage{{Model: testModelRef(), Usage: UsageCount{InputTokens: 4, OutputTokens: 4}}}}
		testassert.Require(t, usageTotalsEqual(graph.Session.State.Usage, wantTotals), "session usage = %+v, want both attempt members counted", graph.Session.State.Usage)
		testassert.Require(t, !hasAssistantOrSignal(graph), "failed attempts projected an assistant entry or a signal")
		view, err := h.ReadSessionView(context.Background(), sessionID)
		testassert.NoError(t, err)
		measured := view.History.LatestContextUsage()
		testassert.Require(t, measured != nil && *measured == (UsageCount{InputTokens: 3, OutputTokens: 3}), "latest context measurement = %+v, want only the last accepted attempt", measured)
	})

	t.Run("one retried attempt reported nothing", func(t *testing.T) {
		modelFn, _ := brokenAttemptScript([]*model.Usage{u(1, 1), nil, u(3, 3)})
		_, store, sessionID := run(t, modelFn, (&retrySpy{limit: 2}).retry)
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Usage != nil && *settlement.Usage == (UsageCount{InputTokens: 3, OutputTokens: 3}), "settlement usage = %+v, want the last accepted attempt's report", settlement.Usage)
		testassert.Require(t, settlement.RetryUsage != nil && *settlement.RetryUsage == (UsageCount{InputTokens: 1, OutputTokens: 1}) && settlement.RetryUsageUnknown, "settlement retry usage = %+v (unknown %v), want the reported attempt summed and the reportless one marked unknown", settlement.RetryUsage, settlement.RetryUsageUnknown)
		wantTotals := UsageTotals{ByModel: []ModelUsage{{Model: testModelRef(), Usage: UsageCount{InputTokens: 4, OutputTokens: 4}}}}
		testassert.Require(t, usageTotalsEqual(graph.Session.State.Usage, wantTotals), "session usage = %+v, want only the reported attempts counted", graph.Session.State.Usage)
	})

	t.Run("an errored output after a broken attempt", func(t *testing.T) {
		calls := 0
		modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			calls++
			if calls == 1 {
				return failStream(errors.New("provider failure"), usageDelta(model.Usage{InputTokens: 1, OutputTokens: 1})), nil
			}
			return streamOf(turnDelta("length", "partial"), usageDelta(model.Usage{InputTokens: 3, OutputTokens: 3})), nil
		}
		h, store, sessionID := run(t, modelFn, (&retrySpy{limit: 1}).retry)
		testassert.Equal(t, calls, 2)
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Status == OperationFailure && settlement.Model != nil && *settlement.Model == testModelRef(), "settlement = %+v, want the conversation model producer", settlement)
		testassert.Require(t, settlement.Usage != nil && *settlement.Usage == (UsageCount{InputTokens: 3, OutputTokens: 3}) && settlement.RetryUsage != nil && *settlement.RetryUsage == (UsageCount{InputTokens: 1, OutputTokens: 1}) && !settlement.RetryUsageUnknown, "settlement usage = %+v retry usage = %+v (unknown %v), want the errored output's report and the broken attempt's report", settlement.Usage, settlement.RetryUsage, settlement.RetryUsageUnknown)
		testassert.Require(t, !hasAssistantOrSignal(graph), "the errored output wrote an assistant entry or a signal")
		wantTotals := UsageTotals{ByModel: []ModelUsage{{Model: testModelRef(), Usage: UsageCount{InputTokens: 4, OutputTokens: 4}}}}
		testassert.Require(t, usageTotalsEqual(graph.Session.State.Usage, wantTotals), "session usage = %+v, want both attempt reports counted", graph.Session.State.Usage)
		view, err := h.ReadSessionView(context.Background(), sessionID)
		testassert.NoError(t, err)
		measured := view.History.LatestContextUsage()
		testassert.Require(t, measured != nil && *measured == (UsageCount{InputTokens: 3, OutputTokens: 3}), "latest context measurement = %+v, want the errored output's report only", measured)
	})
}

// hasAssistantOrSignal reports whether one validated fixture graph carries an
// assistant entry or a signal entry.
func hasAssistantOrSignal(graph *sessionGraph) bool {
	for i := range graph.Entries {
		if graph.Entries[i].Assistant != nil || graph.Entries[i].Signal != nil {
			return true
		}
	}
	return false
}

// TestModelEffectRetriedUsageOverflowAbortsWithoutPublication proves the
// checked retried-usage sum: accepted attempts whose reports overflow int64
// abort the effect with the storage-failure class before any result is
// published, leaving no settlement or assistant entry and the committed
// intent for recovery — exactly like a failed commit transaction.
func TestModelEffectRetriedUsageOverflowAbortsWithoutPublication(t *testing.T) {
	calls := 0
	modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		calls++
		switch calls {
		case 1:
			return failStream(errors.New("provider failure"), usageDelta(model.Usage{InputTokens: math.MaxInt64})), nil
		case 2:
			return failStream(errors.New("provider failure"), usageDelta(model.Usage{InputTokens: 2})), nil
		default:
			return completedTurnStream(), nil
		}
	}
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := effectExecution(modelFn, nil)
	exec.Retry = (&retrySpy{limit: 2}).retry
	err := executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	}))
	testassert.Require(t, errors.Is(err, ErrStorage), "run = %v, want the storage-failure class from the overflowing retried usage", err)
	testassert.Equal(t, calls, 3)
	graph, gerr := validateFixture(t, store, sessionID)
	testassert.NoError(t, gerr)
	testassert.Require(t, len(graph.Entries) == 1 && graph.Entries[0].Input != nil, "committed entries = %+v, want only the admitted input", graph.Entries)
	rec, rerr := h.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, rerr)
	testassert.Require(t, rec.State.Status == OperationRunning && rec.State.ActiveEffect != nil, "operation state = %+v, want the committed intent left for recovery as a failed publication", rec.State)
}

// TestModelEffectCancellationDuringBackoffKeepsAcceptedUsage proves the
// cancel-during-backoff row after broken streams: the interruption settlement
// keeps the last accepted attempt's report as its usage and the earlier
// accepted attempt's report as retry_usage, and both count in the totals.
func TestModelEffectCancellationDuringBackoffKeepsAcceptedUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	modelFn, calls := brokenAttemptScript([]*model.Usage{
		{InputTokens: 1, OutputTokens: 1},
		{InputTokens: 2, OutputTokens: 2},
	})
	h, store, c, sessionID := newEffectHarness(t, modelFn)
	backoff := make(chan struct{})
	policyCalls := 0
	policy := func(error, int) (time.Duration, bool) {
		policyCalls++
		if policyCalls == 2 {
			close(backoff)
			return time.Hour, true
		}
		return 0, true
	}
	go func() {
		<-backoff
		cancel()
	}()
	prepared := preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		exec := effectExecution(modelFn, nil)
		exec.Retry = policy
		return exec, nil
	})
	testassert.NoError(t, executeDirect(h, c, ctx, testOpID, prepared))
	testassert.Require(t, *calls == 2, "physical attempts = %d, want the cancellation to stop a third attempt", *calls)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	settlement := settlementEntryOf(t, graph)
	testassert.Require(t, settlement.Status == OperationInterruption && settlement.Model != nil && *settlement.Model == testModelRef(), "settlement = %+v, want the conversation model producer", settlement)
	testassert.Require(t, settlement.Usage != nil && *settlement.Usage == (UsageCount{InputTokens: 2, OutputTokens: 2}), "settlement usage = %+v, want the last broken attempt's report", settlement.Usage)
	testassert.Require(t, settlement.RetryUsage != nil && *settlement.RetryUsage == (UsageCount{InputTokens: 1, OutputTokens: 1}) && !settlement.RetryUsageUnknown, "settlement retry usage = %+v (unknown %v), want the earlier broken attempt's report", settlement.RetryUsage, settlement.RetryUsageUnknown)
	wantTotals := UsageTotals{ByModel: []ModelUsage{{Model: testModelRef(), Usage: UsageCount{InputTokens: 3, OutputTokens: 3}}}}
	testassert.Require(t, usageTotalsEqual(graph.Session.State.Usage, wantTotals), "session usage = %+v, want both accepted attempts counted", graph.Session.State.Usage)
	var signals int
	for i := range graph.Entries {
		if graph.Entries[i].Signal != nil {
			signals++
		}
	}
	testassert.Equal(t, signals, 1)
}

// TestModelEffectBrokenBodyRetriesUnderThePolicy proves the broken-stream row
// through the real transport: a body that ends before [DONE] is
// model.ErrStreamBroken wrapping io.ErrUnexpectedEOF, the retry policy decides
// it, and the accepted retry's output keeps only its own payload while the
// broken attempt's report lands as retry usage.
func TestModelEffectBrokenBodyRetriesUnderThePolicy(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if n == 1 {
			fmt.Fprint(w, `data: {"choices":[{"delta":{"role":"assistant","content":"partial"}}]}`+"\n\n")
			fmt.Fprint(w, `data: {"usage":{"prompt_tokens":5,"completion_tokens":7}}`+"\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return // the body ends before [DONE]
		}
		fmt.Fprint(w, `data: {"choices":[{"delta":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`+"\n\n")
		fmt.Fprint(w, `data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}`+"\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	var calls int
	var cause error
	policy := func(err error, failed int) (time.Duration, bool) {
		calls++
		cause = err
		return 0, failed == 1
	}
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := transportExecution(srv, policy)
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	testassert.Require(t, hits.Load() == 2 && calls == 1, "physical requests = %d policy consultations = %d, want the one broken attempt retried", hits.Load(), calls)
	testassert.Require(t, errors.Is(cause, model.ErrStreamBroken) && errors.Is(cause, io.ErrUnexpectedEOF), "classified failure = %v, want the broken-body sentinel with its EOF cause", cause)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var assistant *assistantEntry
	for i := range graph.Entries {
		if graph.Entries[i].Assistant != nil {
			assistant = graph.Entries[i].Assistant
		}
	}
	testassert.Require(t, assistant != nil && assistant.Content[0].Text == "done", "assistant = %+v, want only the retry's content", assistant)
	testassert.Require(t, assistant.Usage != nil && *assistant.Usage == (UsageCount{InputTokens: 3, OutputTokens: 2}) && assistant.RetryUsage != nil && *assistant.RetryUsage == (UsageCount{InputTokens: 5, OutputTokens: 7}), "assistant usage = %+v retry usage = %+v, want the completed attempt and the broken attempt", assistant.Usage, assistant.RetryUsage)
}

// TestModelEffectOneAssemblyAndClose pins the ownership boundary on an
// accepted stream: the Harness invokes assembly exactly once and the stream
// closes exactly once, on every classification.
func TestModelEffectOneAssemblyAndClose(t *testing.T) {
	stream := completedTurnStream()
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := effectExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		return stream, nil
	}, nil)
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	testassert.Equal(t, stream.closes, 1)
	requireIntentAndResultCounts(t, h, store, sessionID, true)
	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, assistant.Status == model.OutputCompleted && assistant.Content[0].Text == "done", "assistant entry = %+v, want the single assembled payload", assistant)
}

// TestModelEffectBoundaryFailureKeepsAcceptedUsage proves the boundary row
// after an accepted attempt: a stream that breaks before its finish reason
// reports usage and the policy grants another attempt, then a callback
// returning neither a stream nor an error settles the boundary failure with
// the accepted attempt's report naming the conversation model and counting in
// both totals. An invalid first attempt has no accepted usage to keep.
func TestModelEffectBoundaryFailureKeepsAcceptedUsage(t *testing.T) {
	t.Run("accepted attempt then invalid attempt", func(t *testing.T) {
		calls := 0
		modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			calls++
			if calls == 1 {
				return brokenBeforeFinishStream(), nil
			}
			return nil, nil
		}
		spy := &retrySpy{limit: 1}
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := effectExecution(modelFn, nil)
		exec.Retry = spy.retry
		err := executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		}))
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "neither exactly one stream nor one error"), "run = %v, want the invalid second attempt's boundary failure", err)
		testassert.Require(t, calls == 2 && len(spy.attempts) == 1 && spy.attempts[0] == 1, "physical attempts = %d policy consultations = %v, want the broken attempt retried once and the invalid attempt never classified", calls, spy.attempts)
		requireIntentAndResultCounts(t, h, store, sessionID, false)
		graph, gerr := validateFixture(t, store, sessionID)
		testassert.NoError(t, gerr)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Status == OperationFailure && settlement.Model != nil && *settlement.Model == testModelRef(), "settlement = %+v, want the conversation model producer", settlement)
		testassert.Require(t, settlement.Usage != nil && *settlement.Usage == (UsageCount{InputTokens: 5, OutputTokens: 7}) && settlement.RetryUsage == nil && !settlement.RetryUsageUnknown, "settlement usage = %+v retry usage = %+v (unknown %v), want the accepted attempt's report alone", settlement.Usage, settlement.RetryUsage, settlement.RetryUsageUnknown)
		want := UsageTotals{ByModel: []ModelUsage{{Model: testModelRef(), Usage: UsageCount{InputTokens: 5, OutputTokens: 7}}}}
		rec, rerr := h.ReadOperation(context.Background(), sessionID, testOpID)
		testassert.NoError(t, rerr)
		testassert.Require(t, usageTotalsEqual(rec.State.Usage, want), "operation usage = %+v, want the accepted attempt's report", rec.State.Usage)
		view, verr := h.ReadSessionView(context.Background(), sessionID)
		testassert.NoError(t, verr)
		testassert.Require(t, usageTotalsEqual(view.TotalUsage, want), "session usage = %+v, want the accepted attempt's report", view.TotalUsage)
	})

	t.Run("invalid first attempt", func(t *testing.T) {
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := effectExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return nil, nil
		}, nil)
		err := executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		}))
		testassert.Require(t, err != nil, "run = nil, want the physical-request boundary failure")
		requireIntentAndResultCounts(t, h, store, sessionID, false)
		graph, gerr := validateFixture(t, store, sessionID)
		testassert.NoError(t, gerr)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Usage == nil && settlement.RetryUsage == nil && !settlement.RetryUsageUnknown, "settlement usage/retry usage = %+v/%+v (unknown %v), want no usage without an accepted attempt", settlement.Usage, settlement.RetryUsage, settlement.RetryUsageUnknown)
	})
}

// TestModelEffectRetryBudgetSpansFailureKinds proves the one shared budget:
// attempts alternating a pre-acceptance 429 and an accepted stream that breaks
// before its finish reason are classified by the same policy count, which
// observes 1, 2, 3 and 4 in order and stops the effect after the fourth
// attempt.
func TestModelEffectRetryBudgetSpansFailureKinds(t *testing.T) {
	calls := 0
	modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		calls++
		if calls%2 == 1 {
			return nil, &model.HTTPStatusError{StatusCode: http.StatusTooManyRequests, StatusText: "429"}
		}
		return brokenBeforeFinishStream(), nil
	}
	spy := &retrySpy{limit: 3}
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := effectExecution(modelFn, nil)
	exec.Retry = spy.retry
	err := executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	}))
	testassert.NoError(t, err)
	testassert.Require(t, calls == 4, "physical attempts = %d, want the policy to stop after the fourth", calls)
	testassert.Require(t, len(spy.attempts) == 4 && spy.attempts[0] == 1 && spy.attempts[1] == 2 && spy.attempts[2] == 3 && spy.attempts[3] == 4, "policy attempt numbers = %v, want 1, 2, 3, 4 across both failure kinds", spy.attempts)
	var status *model.HTTPStatusError
	var streamErr *agent.StreamError
	testassert.Require(t, errors.As(spy.seen[0], &status) && errors.As(spy.seen[1], &streamErr) && errors.As(spy.seen[2], &status) && errors.As(spy.seen[3], &streamErr), "classified failures = %v, want the alternating status and broken-stream errors", spy.seen)
	requireIntentAndResultCounts(t, h, store, sessionID, false)
	graph, gerr := validateFixture(t, store, sessionID)
	testassert.NoError(t, gerr)
	testassert.Require(t, settlementEntryOf(t, graph).Status == OperationFailure, "settlement = %+v, want the failure terminal", settlementEntryOf(t, graph))
}

// TestModelEffectInvalidAttemptOutcomeSettlesBoundaryFailure proves the
// physical-request boundary: a callback returning both a stream and an error
// closes the supplied stream exactly once and settles the boundary failure,
// and one returning neither does the same without a close.
func TestModelEffectInvalidAttemptOutcomeSettlesBoundaryFailure(t *testing.T) {
	t.Run("both a stream and an error", func(t *testing.T) {
		stream := streamOf()
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := effectExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return stream, errors.New("ambiguous outcome")
		}, nil)
		err := executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		}))
		testassert.Require(t, err != nil, "run = nil, want the physical-request boundary failure")
		testassert.Equal(t, stream.closes, 1)
		requireIntentAndResultCounts(t, h, store, sessionID, false)
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		testassert.Require(t, settlementEntryOf(t, graph).Status == OperationFailure, "settlement = %+v, want the boundary failure terminal", settlementEntryOf(t, graph))
	})

	t.Run("neither a stream nor an error", func(t *testing.T) {
		h, store, c, sessionID := newEffectHarness(t, nil)
		exec := effectExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return nil, nil
		}, nil)
		err := executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
			return exec, nil
		}))
		testassert.Require(t, err != nil, "run = nil, want the physical-request boundary failure")
		requireIntentAndResultCounts(t, h, store, sessionID, false)
	})
}
