package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// closedCall is the metadata callback's context: pure description has no
// binding authority, so a worker hostcall from it is closed.
func closedCall(context.Context, string, string, json.RawMessage) (extension.Result, error) {
	return extension.Result{}, extension.ErrClosed
}

// validateNull accepts only the protocol-level null result of open/dispose.
func validateNull(raw json.RawMessage) (any, error) {
	if !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("result must be null")
	}
	return nil, nil
}

// typed adapts one typed codec to the request validator signature.
func typed[T any](decode func(json.RawMessage) (T, error)) func(json.RawMessage) (any, error) {
	return func(raw json.RawMessage) (any, error) {
		value, err := decode(raw)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
}

// ownedWorkerFailure renders one worker failure as the host error, owning the
// declared partial beside the typed failure.
func ownedWorkerFailure(failure *worker.Error) error {
	if failure == nil {
		return nil
	}
	if len(failure.Partial) == 0 {
		return failure
	}
	return &extension.CallError{Err: failure, Partial: append(json.RawMessage(nil), failure.Partial...)}
}

func bindingToolResult(callID string, err error, cancelled bool) model.ToolResult {
	status := model.ResultError
	if cancelled {
		status = model.ResultInterrupted
	}
	content := "Tool call interrupted."
	if err != nil && !cancelled {
		content = err.Error()
	}
	return model.ToolResult{CallID: callID, Status: status, Content: content}
}

func decodeOutcome(outcome requestOutcome) (any, error) {
	if outcome.err == nil && outcome.failure == nil {
		return outcome.value, nil
	}
	if outcome.failure != nil {
		cause := ownedWorkerFailure(outcome.failure)
		known := outcome.failure.Interrupted || outcome.failure.Evidence != ""
		if known {
			return nil, &harness.EffectError{Evidence: outcome.failure.Evidence, Interrupted: outcome.failure.Interrupted, Err: cause}
		}
		if !(outcome.hostCancelled && outcome.failure.Class == worker.ClassCancelled) {
			return nil, cause
		}
		outcome.err = cause
	}
	err := outcome.err
	if outcome.failure != nil {
		err = ownedWorkerFailure(outcome.failure)
	}
	if err == nil {
		err = extension.ErrClosed
	}
	return nil, &harness.EffectError{Evidence: outcome.evidence, Interrupted: outcome.hostCancelled, Err: err}
}
