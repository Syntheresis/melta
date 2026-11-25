package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

// nativeCallKind is what one call works for. It decides which host services
// the call may reach and which stage conditions still apply.
type nativeCallKind string

const (
	callKindActivation nativeCallKind = "activation"
	callKindAdmission  nativeCallKind = "admission"
	callKindHook       nativeCallKind = "hook"
	callKindTool       nativeCallKind = "tool"
	callKindOperation  nativeCallKind = "operation"
)

// nativeGrant is the private authority one callback scope carries. It never
// appears in an authoring value.
type nativeGrant struct {
	commands     bool
	commandScope CallerScope
}

// secretValues are the private credential captures one callback scope can
// resolve: its activation's values and the enclosing Operation's
// per-extension values. They never reach a durable capture.
type secretValues struct {
	activation map[string]string
	operation  map[string]map[string]string
}

type secretValuesKey struct{}

func secretValuesFrom(ctx context.Context) secretValues {
	values, _ := ctx.Value(secretValuesKey{}).(secretValues)
	return values
}

// nativeCallLifetime closes admission before canceling and joining borrowed calls.
type nativeCallLifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	done   bool
	wg     sync.WaitGroup
}

func (v *nativeCallLifetime) admit(supplied context.Context) (context.Context, func(), error) {
	v.mu.Lock()
	if v.done {
		v.mu.Unlock()
		return nil, nil, extension.ErrClosed
	}
	v.wg.Add(1)
	v.mu.Unlock()
	if supplied == nil {
		supplied = context.Background()
	}
	finish := func() { v.wg.Done() }
	if err := v.ctx.Err(); err != nil {
		finish()
		return nil, nil, err
	}
	if err := supplied.Err(); err != nil {
		finish()
		return nil, nil, err
	}
	var merged context.Context
	var cancel context.CancelFunc
	if deadline, ok := supplied.Deadline(); ok {
		merged, cancel = context.WithDeadline(v.ctx, deadline)
	} else {
		merged, cancel = context.WithCancel(v.ctx)
	}
	stop := context.AfterFunc(supplied, cancel)
	finish = func() { stop(); cancel(); v.wg.Done() }
	if err := v.ctx.Err(); err != nil {
		finish()
		return nil, nil, err
	}
	if err := supplied.Err(); err != nil {
		finish()
		return nil, nil, err
	}
	return merged, finish, nil
}

func (v *nativeCallLifetime) end() {
	v.mu.Lock()
	v.done = true
	v.mu.Unlock()
	v.cancel()
	v.wg.Wait()
}

type callbackContextKey struct{}

// callbackContext reports whether ctx is a context Melta handed to a
// host→extension callback. Every callback except observer delivery carries it.
func callbackContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Value(callbackContextKey{}).(struct{})
	return ok
}

// callbackScope marks one nativeCallScope as a host callback into an
// activation: every context Melta hands to the callback carries the marker, so
// a command issued from it is refused. A scope created for the extension's own
// call into the host carries no mark.
func callbackScope(scope *nativeCallScope) *nativeCallScope {
	if scope.activation != nil {
		scope.ctx = context.WithValue(scope.ctx, callbackContextKey{}, struct{}{})
	}
	return scope
}

type grantContextKey struct{}

func hostGrant(ctx context.Context) nativeGrant {
	grant, _ := ctx.Value(grantContextKey{}).(nativeGrant)
	return grant
}

func acceptNativeMethod(method *extension.CompiledMethod, args json.RawMessage) (json.RawMessage, error) {
	args = append(json.RawMessage(nil), args...)
	if err := method.ValidateArguments(args); err != nil {
		return nil, fmt.Errorf("%w: %w", harness.ErrInvalid, err)
	}
	return args, nil
}

func executeNativeProvider[C any](ctx context.Context, call C, method *extension.CompiledMethod, handler func(context.Context, C, json.RawMessage) (extension.Result, error), kind, id string, arguments json.RawMessage) (extension.Result, error) {
	result, err := handler(ctx, call, arguments)
	if err != nil {
		return extension.Result{}, ownCallError(err)
	}
	result = ownResult(result)
	if err := method.ValidateResult(result); err != nil {
		return extension.Result{}, fmt.Errorf("%s %q: %w", kind, id, err)
	}
	return result, nil
}

// ownCallError owns the selected partial while preserving the enclosing error
// chain, including wrappers and joined classifications.
func ownCallError(err error) error {
	var callErr *extension.CallError
	if errors.As(err, &callErr) {
		if len(callErr.Partial) != 0 && !json.Valid(callErr.Partial) {
			return fmt.Errorf("call error partial is not one JSON value: %w", extension.ErrInvalid)
		}
		return &extension.CallError{Err: err, Partial: append(json.RawMessage(nil), callErr.Partial...)}
	}
	return err
}

func ownResult(result extension.Result) extension.Result {
	out := extension.Result{Value: append(json.RawMessage(nil), result.Value...)}
	if result.Failure != nil {
		failure := *result.Failure
		failure.Data = append(json.RawMessage(nil), result.Failure.Data...)
		failure.Partial = append(json.RawMessage(nil), result.Failure.Partial...)
		out.Failure = &failure
	}
	return out
}
