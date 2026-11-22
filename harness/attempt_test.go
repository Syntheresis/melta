package harness

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestAcceptedAttemptUsage pins the accepted-attempt report split: no accepted
// attempt leaves every member absent, the last report is the record's usage
// while the earlier reports are its retry usage, a reportless earlier attempt
// marks the retry usage unknown without dropping the reported siblings, and an
// overflowing retry sum is the storage-failure class with no partial value.
func TestAcceptedAttemptUsage(t *testing.T) {
	u := func(input, output int64) *model.Usage {
		return &model.Usage{InputTokens: int(input), OutputTokens: int(output)}
	}
	cases := []struct {
		name    string
		reports []*model.Usage
		want    attemptUsage
		wantErr bool
	}{
		{"no accepted attempt", nil, attemptUsage{}, false},
		{"last report and summed earlier reports", []*model.Usage{u(1, 2), u(3, 4), u(5, 6)},
			attemptUsage{last: &UsageCount{InputTokens: 5, OutputTokens: 6}, retry: &UsageCount{InputTokens: 4, OutputTokens: 6}}, false},
		{"a reportless earlier attempt", []*model.Usage{u(1, 2), nil, u(5, 6)},
			attemptUsage{last: &UsageCount{InputTokens: 5, OutputTokens: 6}, retry: &UsageCount{InputTokens: 1, OutputTokens: 2}, retryUnknown: true}, false},
		{"an overflowing earlier sum", []*model.Usage{u(math.MaxInt64, 0), u(1, 0), u(3, 3)}, attemptUsage{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := acceptedAttemptUsage(tc.reports)
			if tc.wantErr {
				testassert.Require(t, errors.Is(err, ErrStorage), "acceptedAttemptUsage error = %v, want the storage-failure class", err)
			} else {
				testassert.NoError(t, err)
			}
			testassert.Require(t, reflect.DeepEqual(got, tc.want), "acceptedAttemptUsage = %+v, want %+v", got, tc.want)
		})
	}
}

// TestRunModelAttemptsRestartObservation pins the shared attempt loop's
// restart observation: every granted retry reports once through onRetry while
// the backoff runs behind it — a granted one-hour delay is reported before the
// wait, whose cancellation then interrupts the loop — and a policy that stops
// or grants a negative delay reports none.
func TestRunModelAttemptsRestartObservation(t *testing.T) {
	cases := []struct {
		name          string
		failures      int // physical attempts failing before the settling one
		policy        RetryPolicy
		cancelOnRetry bool // the one-hour grant: the reported restart cancels the run instead of waiting it out
		wantOnRetry   int
		wantKind      attemptKind
	}{
		{"one granted retry", 1, (&retrySpy{limit: 1}).retry, false, 1, attemptSettled},
		{"a granted retry per failure", 2, (&retrySpy{limit: 2}).retry, false, 2, attemptSettled},
		{"the policy stops", 1, (&retrySpy{limit: 0}).retry, false, 0, attemptFailed},
		{"a negative delay stops", 1, func(error, int) (time.Duration, bool) { return -time.Second, true }, false, 0, attemptFailed},
		{"before the backoff", 1, func(error, int) (time.Duration, bool) { return time.Hour, true }, true, 1, attemptInterrupted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The deadline only bounds the table: a loop that waited before
			// reporting the restart would never cancel through onRetry.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			attempts := 0
			attempt := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				attempts++
				if attempts > tc.failures {
					return completedTurnStream(), nil
				}
				return nil, errors.New("provider dropped")
			}
			assemble := func(model.Stream) (model.Output, error) {
				return model.Output{Status: model.OutputCompleted}, nil
			}
			reports := 0
			onRetry := func() {
				reports++
				if tc.cancelOnRetry {
					cancel()
				}
			}
			res := runModelAttempts(ctx, model.Request{}, nil, attempt, assemble, tc.policy, onRetry)
			testassert.Equal(t, res.kind, tc.wantKind)
			testassert.Require(t, reports == tc.wantOnRetry, "restart observations = %d, want %d", reports, tc.wantOnRetry)
			testassert.Require(t, res.usageErr == nil && res.usage.last == nil && res.usage.retry == nil && !res.usage.retryUnknown, "attempt usage = %+v, want no accepted report", res.usage)
		})
	}
}
