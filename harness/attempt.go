package harness

import (
	"context"
	"errors"
	"time"

	"github.com/Syntheresis/melta/agent"
	"github.com/Syntheresis/melta/model"
)

// attemptKind is the closed producer state of one shared physical attempt
// loop run. The loop is the state's sole producer; no result validator
// exists.
type attemptKind int

const (
	attemptSettled     attemptKind = iota // a stream was accepted and assembled to one output
	attemptFailed                         // nil output, the last attempt error stopped the policy
	attemptInterrupted                    // nil output, cancellation observed before or between attempts
	attemptInvalid                        // nil output, the boundary model ProtocolError or another assembly failure
)

// attemptUsage carries one effect's accepted-attempt usage: the last accepted
// attempt's report, the sum of the retried attempts' reports, and whether any
// retried attempt reported none. The retry sum is present only when at least
// one retried attempt reported.
type attemptUsage struct {
	last         *UsageCount
	retry        *UsageCount
	retryUnknown bool
}

// attemptResult is the one shared attempt loop's result: its kind, the
// assembled output when one settled, the attempt or boundary error otherwise,
// the accepted attempts' usage contribution, and the retried-attempt sum's
// accounting error when one overflowed.
type attemptResult struct {
	kind     attemptKind
	output   model.Output
	err      error
	usage    attemptUsage
	usageErr error
}

// acceptedAttemptUsage splits one effect's accepted-attempt reports: the last
// accepted attempt supplies the record's usage, and every earlier retried
// attempt that reported is added with the checked usage arithmetic. A retried
// attempt that reported nothing marks the retry usage unknown; no accepted
// attempt leaves every member absent. An overflowing retry sum returns the
// storage-failure class and no partial value.
func acceptedAttemptUsage(reports []*model.Usage) (attemptUsage, error) {
	var usage attemptUsage
	if len(reports) == 0 {
		return usage, nil
	}
	usage.last = usageCount(reports[len(reports)-1])
	for _, report := range reports[:len(reports)-1] {
		if report == nil {
			usage.retryUnknown = true
			continue
		}
		count := usageCount(report)
		if usage.retry == nil {
			usage.retry = count
			continue
		}
		sum, err := addUsageCount(*usage.retry, *count)
		if err != nil {
			return attemptUsage{}, err
		}
		usage.retry = &sum
	}
	return usage, nil
}

// runModelAttempts runs the one physical model-attempt loop shared by the
// conversation and compact model effects: check cancellation before every
// attempt, invoke the physical callback once per iteration, accept exactly
// one nonnil stream with a nil error without reclassifying a late
// cancellation, assemble each accepted stream exactly once, close any supplied
// stream once and report the boundary ProtocolError for an outcome that is both
// or neither — before the post-failure cancellation check — let observed
// cancellation win over retry classification, select the standard policy for a
// nil Retry, stop on a false classification or a negative delay, and run the
// backoff behind the cancellation check. An assembled output ends the loop;
// an accepted stream that fails before its finish reason is one more attempt
// failure whose reported usage the record keeps, and every other assembly
// error is the effect's boundary failure. Every physical attempt receives its
// own deep-owned copy of the logical request, so no callback can reach the next
// attempt's view, the retained evidence, or the captured catalog.
func runModelAttempts(ctx context.Context, req model.Request, parameters model.Extra, attempt func(context.Context, model.Request, model.Extra) (model.Stream, error), assemble func(model.Stream) (model.Output, error), retry RetryPolicy) attemptResult {
	if retry == nil {
		retry = standardRetryPolicy
	}
	var accepted []*model.Usage
	result := func(kind attemptKind, output model.Output, err error) attemptResult {
		usage, usageErr := acceptedAttemptUsage(accepted)
		return attemptResult{kind: kind, output: output, err: err, usage: usage, usageErr: usageErr}
	}
	for failed := 1; ; failed++ {
		if ctx.Err() != nil { // observed cancellation before an attempt interrupts; no output and no assembly call
			return result(attemptInterrupted, model.Output{}, nil)
		}
		owned, err := model.NewRequest(req)
		if err != nil { // the shared validator is the boundary owner; a request it rejects is a boundary violation, never dispatched
			return result(attemptInvalid, model.Output{}, &agent.ProtocolError{Boundary: "model", Detail: err.Error()})
		}
		stream, attemptErr := attempt(ctx, owned, parameters.Clone())
		if attemptErr == nil && stream != nil {
			output, assemblyErr := assemble(stream)
			if assemblyErr == nil {
				accepted = append(accepted, output.Usage)
				return result(attemptSettled, output, nil)
			}
			var streamErr *agent.StreamError
			if !errors.As(assemblyErr, &streamErr) { // every other assembly error is the effect's boundary failure.
				return result(attemptInvalid, model.Output{}, assemblyErr)
			}
			accepted = append(accepted, streamErr.Usage)
			attemptErr = assemblyErr
		} else if attemptErr == nil || stream != nil { // exactly one stream or one error must be returned; a supplied stream closes before the boundary failure
			if stream != nil {
				_ = stream.Close()
			}
			return result(attemptInvalid, model.Output{}, &agent.ProtocolError{Boundary: "model", Detail: "physical model request returned neither exactly one stream nor one error"})
		}
		// An attempt failure observed under a done execution context
		// interrupts before any classification: regardless of the attempt
		// error's shape or the retry policy's answer, cancellation is an
		// interruption with no output.
		if ctx.Err() != nil {
			return result(attemptInterrupted, model.Output{}, nil)
		}
		delay, again := retry(attemptErr, failed)
		if !again || delay < 0 {
			return result(attemptFailed, model.Output{}, attemptErr)
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done(): // cancellation during backoff interrupts; no attempt starts
			return result(attemptInterrupted, model.Output{}, nil)
		}
	}
}
