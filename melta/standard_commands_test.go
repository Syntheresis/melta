package melta

import (
	"context"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/cmdoutput"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

type cancelledBackground struct{}

func (cancelledBackground) StartJob(ctx context.Context, _ string, _ func(context.Context, func(string) error) error) error {
	return ctx.Err()
}

func TestCancelledBackgroundStartAbortsReservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	jobs := &fakeJobs{}
	output := newLiveToolOutput(newObservation())
	run := newStandardTools(t.TempDir(), jobs, output)["run_command"]
	tc := envCallContext(t, t.TempDir())
	tc.Background = cancelledBackground{}
	call := makeCall(t, "run_command", `{"command":"true","background":true}`)
	got := run(ctx, tc, call)
	testassert.Require(t, got.Result.Status == model.ResultInterrupted && got.Result.Content == "command cancelled", "outcome = %+v", got)
	testassert.Require(t, len(jobs.aborted) == 1 && jobs.aborted[0] == "b0b0b0b0" && len(jobs.started) == 0, "jobs = %+v", jobs)
}

func TestRunCommandPreservesCommandText(t *testing.T) {
	const command = " \tprintf x; printf y"
	jobs := &fakeJobs{}
	run := newStandardTools(t.TempDir(), jobs, newLiveToolOutput(newObservation()))["run_command"]
	tc := envCallContext(t, t.TempDir())
	tc.Background = scriptBackground{}
	original := runForegroundCommandFn
	t.Cleanup(func() { runForegroundCommandFn = original })
	runForegroundCommandFn = func(_ context.Context, got, _ string, _ int, _ *cmdoutput.Capture, _ []string) (string, error) {
		testassert.Equal(t, got, command)
		return "ok", nil
	}
	foreground := makeCall(t, "run_command", `{"command":" \tprintf x; printf y"}`)
	testassert.Equal(t, run(context.Background(), tc, foreground).Result.Status, model.ResultSuccess)
	background := makeCall(t, "run_command", `{"command":" \tprintf x; printf y","background":true}`)
	testassert.Equal(t, run(context.Background(), tc, background).Result.Status, model.ResultSuccess)
	testassert.Require(t, len(jobs.started) == 1 && jobs.started[0].Command == command, "jobs = %+v", jobs.started)
}

// TestCommandOutcomeClassifiesByDeliveredCause pins the causal settlement of
// the runner's settled ExitError shapes: the classification reads the
// delivered cause carried on the error, never the context after the runner
// has settled the real result. The seam cancels the context inside the seam
// after the runner returns — the exact post-settlement window.
func TestCommandOutcomeClassifiesByDeliveredCause(t *testing.T) {
	orig := runForegroundCommandFn
	defer func() { runForegroundCommandFn = orig }()

	t.Run("external-signal death keeps its real result after the context cancels", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runForegroundCommandFn = func(ctx context.Context, command, dir string, timeoutSec int, capture *cmdoutput.Capture, env []string) (string, error) {
			result, err := orig(ctx, command, dir, timeoutSec, capture, env)
			cancel()
			return result, err
		}
		outcome := commandOutcome(ctx, "call-1", "kill -TERM $$", "", 0, cmdoutput.NewCapture(cmdoutput.Options{Directory: t.TempDir()}), nil)
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == "Error: Exit code -1\n", "result = (%v, %q), want the real signal-death result as an error", outcome.Result.Status, outcome.Result.Content)
	})

	t.Run("own timeout keeps its real result after the context cancels", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runForegroundCommandFn = func(ctx context.Context, command, dir string, timeoutSec int, capture *cmdoutput.Capture, env []string) (string, error) {
			result, err := orig(ctx, command, dir, timeoutSec, capture, env)
			cancel()
			return result, err
		}
		outcome := commandOutcome(ctx, "call-2", "sleep 5", "", 1, cmdoutput.NewCapture(cmdoutput.Options{Directory: t.TempDir()}), nil)
		testassert.Require(t, outcome.Result.Status == model.ResultError && strings.HasPrefix(outcome.Result.Content, "Error: Exit code -1 (timeout)\n"), "result = (%v, %q), want the retained timeout text as an error", outcome.Result.Status, outcome.Result.Content)
	})
}
