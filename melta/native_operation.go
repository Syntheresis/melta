package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/protocol"
)

// invokeRegisteredOperation is the registered-handler counterpart of the
// existing client invocation owner. The public envelope switches separately.
func (r *Melta) invokeRegisteredOperation(ctx context.Context, caller Caller, extensionID, methodName string, targetInput protocol.ExtensionInvocationTarget, arguments json.RawMessage, requestID string) (extension.Result, error) {
	arguments = bytes.Clone(arguments)
	var result extension.Result
	err := r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		r.config.nativeMu.Lock()
		candidate := r.config.nativeCurrent
		r.config.nativeMu.Unlock()
		if candidate == nil {
			return &worker.Error{Class: worker.ClassUnavailable, Message: "extension unavailable"}
		}
		capture := candidate.capture()
		if capture == nil {
			return extension.ErrClosed
		}
		defer capture.release()
		stage, err := capture.stage()
		if err != nil {
			return err
		}
		defer stage.release()
		entry := stage.operations[nativeOperationKey{extensionID, methodName}]
		if entry == nil {
			return fmt.Errorf("extension operation %q: %w", methodName, harness.ErrNotFound)
		}
		operation := entry.value.(struct {
			target extension.ExposedTargetKind
			method nativeServiceMethod
		})
		target, session, err := r.invocationTarget(ctx, caller, h, targetInput, operation.target)
		if err != nil {
			return err
		}
		var states map[string]harness.SessionStateValue
		if session.SessionID != "" {
			states, err = h.SessionExtensionStates(ctx, session.SessionID)
			if err != nil {
				return err
			}
		}
		args, err := acceptNativeMethod(operation.method.contract, arguments, false)
		if err != nil {
			return err
		}
		callCtx, cancel := context.WithCancel(ctx)
		receipt := &nativeReceipt{}
		var lease *artifactCallLease
		if session.SessionID != "" {
			lease = &artifactCallLease{intervals: r.artifacts, key: sessionArtifactDir(r.dataDir, session.SessionID)}
			defer lease.end()
		}
		scope := callbackScope(&nativeCallScope{
			nativeCallLifetime: nativeCallLifetime{ctx: callCtx, cancel: cancel},
			candidate:          candidate, activation: entry.activation,
			pure:    operation.method.contract.Mode == extension.ModePure,
			session: session, states: states, host: r.config.composition,
			commandScope: target, receipt: receipt, lease: lease, requestID: requestID,
		})
		author := extension.AuthorContext{
			ExtensionID: entry.activation.id, Settings: bytes.Clone(entry.activation.section),
			Session: session, State: bytes.Clone(states[entry.activation.id]), Call: scope.call, RequestID: requestID,
		}
		result, err = func() (extension.Result, error) {
			defer scope.end()
			return executeNativeProvider(scope.ctx, author, operation.method.contract, operation.method.handler, "operation", methodName, args)
		}()
		if err != nil {
			var declared *extension.CallError
			if errors.As(err, &declared) && len(declared.Partial) != 0 {
				return err
			}
			observed, partial := receipt.observed()
			if len(partial) != 0 {
				return &extension.CallError{Err: err, Partial: partial}
			}
			if observed != nil {
				return &extension.CallError{Err: err, Partial: bytes.Clone(observed.Value)}
			}
			return err
		}
		if target.Kind == CallerScopeSession && result.Failure == nil && !receipt.hasCommit() {
			if err := recheckInvocationSession(ctx, h, target); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}
