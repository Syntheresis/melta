// Public-surface coverage for a broken accepted stream retried by the effect:
// the real transport's aborted body is one policy-classified attempt, the
// retry sends the same request, the committed assistant carries only the
// retry's content with its usage and the broken attempt's retry usage, the
// next request omits the failed stream's text, and a fork of the retried
// prefix loads with no inherited usage.
package harness_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/agent"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestPublicConversationBrokenStreamRetryRecordsBothUsages proves the
// conversation effect's accepted-broken-stream retry over the real public
// pipeline and both stores: the policy sees the *StreamError, the second
// physical request is identical to the first, one assistant commits with only
// the retry's content, the record and totals carry the last accepted attempt's
// report and the retried attempt's report, the measurement reads only the
// last report, the next request omits the failed text, and a fork of the
// retried prefix copies no usage.
func TestPublicConversationBrokenStreamRetryRecordsBothUsages(t *testing.T) {
	eachStore(t, func(t *testing.T, store harness.Storage) {
		var mu sync.Mutex
		var bodies []string
		hits := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			hits++
			n := hits
			bodies = append(bodies, string(body))
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			if n == 1 {
				fmt.Fprint(w, `data: {"choices":[{"delta":{"role":"assistant","content":"partial"}}]}`+"\n\n")
				fmt.Fprint(w, `data: {"usage":{"prompt_tokens":5,"completion_tokens":7}}`+"\n\n")
				if f, ok := w.(http.Flusher); ok {
					f.Flush() // both events reach the client before the sever
				}
				panic(http.ErrAbortHandler) // sever the accepted connection mid-body
			}
			fmt.Fprint(w, `data: {"choices":[{"delta":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`+"\n\n")
			fmt.Fprint(w, `data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}`+"\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		}))
		t.Cleanup(srv.Close)
		fn := attemptTableTransport(t, srv, "")
		var policyMu sync.Mutex
		policyCalls := 0
		var policyCause error
		retry := func(cause error, failed int) (time.Duration, bool) {
			policyMu.Lock()
			defer policyMu.Unlock()
			policyCalls++
			policyCause = cause
			return 0, failed == 1
		}

		f := newPublicFixture(t, store, newScriptModel(), nil)
		f.defaultPrep = harness.PreparedExecution{
			Capture: publicCapture(),
			Open: func(context.Context, harness.OperationAdmission) (harness.Execution, error) {
				exec := publicExecution(fn, fn)
				exec.Retry = retry
				return exec, nil
			},
		}
		defer f.close()
		session := createSession(t, f.h)

		{
			_, err := submit(t, f.h, session, "op-1", harness.MessageModeRegular, "hello")
			testassert.NoError(t, err)
		}
		rec := awaitTerminal(t, f.h, session, "op-1")
		testassert.Require(t, rec.State.Status == harness.OperationSuccess, "settled record = %+v, want terminal success", rec.State)
		policyMu.Lock()
		calls, cause := policyCalls, policyCause
		policyMu.Unlock()
		var streamErr *agent.StreamError
		testassert.Require(t, calls == 1 && errors.As(cause, &streamErr) && errors.Is(streamErr.Err, model.ErrStreamBroken), "policy calls = %d failure = %v, want the one broken-stream classification", calls, cause)
		mu.Lock()
		gotBodies := append([]string(nil), bodies...)
		mu.Unlock()
		testassert.Require(t, len(gotBodies) == 2 && gotBodies[0] == gotBodies[1], "dispatched bodies = %d, want the retry to send the same request", len(gotBodies))

		history, err := f.h.ReadHistoryView(context.Background(), session)
		testassert.NoError(t, err)
		facts := history.HistoryFacts(historyIDsOf(history.HistoryHeaders()))
		var assistant *harness.AssistantEntry
		for i := range facts {
			if facts[i].Assistant != nil {
				assistant = facts[i].Assistant
			}
		}
		testassert.Require(t, assistant != nil && assistant.Content[0].Text == "done", "assistant = %+v, want only the retry's content", assistant)
		testassert.Require(t, assistant.Usage != nil && *assistant.Usage == (harness.UsageCount{InputTokens: 3, OutputTokens: 2}) && assistant.RetryUsage != nil && *assistant.RetryUsage == (harness.UsageCount{InputTokens: 5, OutputTokens: 7}) && !assistant.RetryUsageUnknown, "assistant usage = %+v retry usage = %+v (unknown %v), want the retry's report and the broken attempt's report", assistant.Usage, assistant.RetryUsage, assistant.RetryUsageUnknown)
		wantTotals := harness.UsageTotals{ByModel: []harness.ModelUsage{{Model: publicModelRef, Usage: harness.UsageCount{InputTokens: 8, OutputTokens: 9}}}}
		view, err := f.h.ReadSessionView(context.Background(), session)
		testassert.NoError(t, err)
		testassert.Require(t, reflect.DeepEqual(view.TotalUsage, wantTotals), "session usage = %+v, want both attempt reports", view.TotalUsage)
		measured := view.History.LatestContextUsage()
		testassert.Require(t, measured != nil && *measured == (harness.UsageCount{InputTokens: 3, OutputTokens: 2}), "latest context measurement = %+v, want only the retry's report", measured)

		// The next request omits everything the failed attempt streamed.
		{
			_, err := submit(t, f.h, session, "op-2", harness.MessageModeRegular, "again")
			testassert.NoError(t, err)
		}
		rec = awaitTerminal(t, f.h, session, "op-2")
		testassert.Require(t, rec.State.Status == harness.OperationSuccess, "second record = %+v, want terminal success", rec.State)
		mu.Lock()
		next := bodies[len(bodies)-1]
		mu.Unlock()
		testassert.Require(t, strings.Contains(next, "done") && !strings.Contains(next, "partial"), "next request = %q, want the committed turn without the failed attempt's text", next)

		forkScript := newScriptModel(publicFail("fork turn settled"))
		forkScript.gate = make(chan struct{})
		f2 := newPublicFixture(t, store, forkScript, nil)
		defer f2.close()
		boundary := forkEntryOf(t, store, session, harness.EntryInput, "op-2").ID
		res, err := f2.h.Fork(context.Background(), harness.ForkRequest{Origin: harness.InputOriginUser,
			SourceSessionID: session,
			BoundaryEntryID: boundary,
			OperationID:     "fork-1",
			Content:         []model.ContentPart{{Kind: model.PartText, Text: "fork input"}},
		})
		testassert.NoError(t, err)
		dest := res.Session.Identity.SessionID
		testassert.Require(t, res.Session.State.Usage.ByModel == nil || len(res.Session.State.Usage.ByModel) == 0, "fork session usage = %+v, want no inherited totals", res.Session.State.Usage)
		<-forkScript.arrived // the destination is materialized and its own turn is parked
		destHistory, err := f2.h.ReadHistoryView(context.Background(), dest)
		testassert.NoError(t, err)
		copied := destHistory.HistoryFacts(historyIDsOf(destHistory.HistoryHeaders()))
		var copiedAssistant *harness.AssistantEntry
		for i := range copied {
			if copied[i].Assistant != nil {
				copiedAssistant = copied[i].Assistant
			}
		}
		testassert.Require(t, copiedAssistant != nil && copiedAssistant.Usage == nil && copiedAssistant.RetryUsage == nil && !copiedAssistant.RetryUsageUnknown, "copied assistant = %+v, want no inherited usage", copiedAssistant)
		destView, err := f2.h.ReadSessionView(context.Background(), dest)
		testassert.NoError(t, err)
		testassert.Require(t, len(destView.TotalUsage.ByModel) == 0, "fork destination usage = %+v, want zero inherited totals", destView.TotalUsage)
		forkScript.releaseGate()
		awaitTerminal(t, f2.h, dest, "fork-1")
		{
			err := converge(t, f2)
			testassert.NoError(t, err)
		}
	})
}
