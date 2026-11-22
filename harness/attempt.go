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

type attemptUsage struct {
	last *UsageCount
	retryUsage
}

// attemptResult is the one shared attempt loop's result.
type attemptResult struct {
	kind     attemptKind
	output   model.Output
	err      error
	usage    attemptUsage
	usageErr error
}

// acceptedAttemptUsage splits one effect's accepted-attempt reports into the
// last attempt's usage and the earlier retries' checked sum; an absent earlier
// report marks the retry usage unknown and an overflow returns ErrStorage.
func acceptedAttemptUsage(reports []*model.Usage) (attemptUsage, error) {
	var usage attemptUsage
	if len(reports) == 0 {
		return usage, nil
	}
	usage.last = usageCount(reports[len(reports)-1])
	for _, report := range reports[:len(reports)-1] {
		if report == nil {
			usage.RetryUsageUnknown = true
			continue
		}
		count := usageCount(report)
		if usage.RetryUsage != nil {
			sum, err := addUsageCount(*usage.RetryUsage, *count)
			if err != nil {
				return attemptUsage{}, err
			}
			count = &sum
		}
		usage.RetryUsage = count
	}
	return usage, nil
}

// runModelAttempts runs the one physical model-attempt loop shared by the
// conversation and compact model effects: cancel-check each attempt, invoke
// the physical callback, assemble an accepted stream once, close a supplied
// stream once, and report a stream/error disagreement as the boundary
// ProtocolError. Observed cancellation wins over retry classification, a nil
// Retry selects the standard policy, onRetry sees each granted restart, and
// waits run behind the cancellation check. An assembled output ends the loop; a
// broken stream is another attempt whose reported usage is kept, and any other
// assembly error is the boundary failure.
func runModelAttempts(ctx context.Context, req model.Request, parameters model.Extra, attempt func(context.Context, model.Request, model.Extra) (model.Stream, error), assemble func(model.Stream) (model.Output, error), retry RetryPolicy, onRetry func()) attemptResult {
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
		// error's shape or the retry policy's answer, cancellation wins.
		if ctx.Err() != nil {
			return result(attemptInterrupted, model.Output{}, nil)
		}
		delay, again := retry(attemptErr, failed)
		if !again || delay < 0 {
			return result(attemptFailed, model.Output{}, attemptErr)
		}
		onRetry()
		select {
		case <-time.After(delay):
		case <-ctx.Done(): // cancellation during backoff interrupts; no attempt starts
			return result(attemptInterrupted, model.Output{}, nil)
		}
	}
}
