package melta

import (
	"context"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// BackgroundServices is the baseline background-work surface one standard tool
// call carries: job start and completion delivery over the Melta's public
// Harness methods. An owned Session start is a declared native host service,
// never a Go callback on this interface.
type BackgroundServices interface {
	StartJob(ctx context.Context, jobID string, spawn func(ctx context.Context, deliver func(content string) error) error) error
}

// backgroundBridge is the one-field holder of the Melta's Harness. It is
// created before harness.New, threaded into the preparation, and armed with
// the constructed Harness before the owner is published. No synchronization:
// harness.New performs no I/O and the first Prepare requires the admission
// gate, so no Caller can observe the unarmed bridge.
type backgroundBridge struct {
	h *harness.Harness
}

type boundBackground struct {
	bridge    *backgroundBridge
	sessionID string
}

func (b *boundBackground) StartJob(ctx context.Context, jobID string, spawn func(context.Context, func(string) error) error) error {
	var forwarded func(context.Context, string) error
	if spawn != nil {
		forwarded = func(spawnCtx context.Context, completionID string) error {
			return spawn(spawnCtx, func(content string) error {
				return b.bridge.h.DeliverBackgroundCompletion(context.Background(), b.sessionID, completionID, content)
			})
		}
	}
	return b.bridge.h.StartJob(ctx, b.sessionID, jobID, forwarded)
}

// ToolContext carries what one tool call needs beyond the call itself: the
// Workspace, the calling Operation's admitted-input identity, the captured
// Invocation, the Session's background service, and the call-time
// subprocess environment producer. It exposes neither the complete admission
// register nor file/command-specific numeric fields.
type ToolContext struct {
	Session       harness.SessionIdentity
	Model         model.ModelRef
	Workspace     string
	OperationID   string
	InvocationID  string
	Purpose       harness.InvocationPurpose
	AdmittedEntry harness.EntryRef
	Invocation    Invocation
	Background    BackgroundServices

	// SubprocessEnv is invoked at each concrete process start — foreground
	// command and the admitted Jobs start handoff alike — and returns the
	// exact environment slice that process runs with: the live ambient
	// environment with the Melta's live credential exclusion applied (the
	// current revision's provider names, together with the preparation-time
	// captured names). It is that live exclusion service only, never a
	// promise to reread the selected execution policy: the model, prompt,
	// tools and configuration revision stay the admission's
	// frozen capture. A nil callback or an error fails the cooperative
	// command before any process starts; there is no ambient fallback.
	SubprocessEnv func(context.Context) ([]string, error)
}

// Tool is one standard tool implementation.
type Tool func(context.Context, ToolContext, model.ToolCall) harness.ToolOutcome
