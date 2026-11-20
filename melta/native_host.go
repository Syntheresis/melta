package melta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

func acceptHostMethod(service, method string, args json.RawMessage, pure bool) (json.RawMessage, error) {
	definition, ok := extension.HostCapability(service)
	if ok {
		for _, m := range definition.Methods {
			if m.Name == method {
				contract, err := extension.CompileMethod(m)
				if err != nil {
					return nil, err
				}
				return acceptNativeMethod(contract, args, pure)
			}
		}
	}
	return nil, fmt.Errorf("host method %q: %w", method, harness.ErrNotFound)
}

func (scope *nativeCallScope) callGuardedHost(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	if scope.activation == nil || !scope.activation.services[service] || scope.pure {
		return extension.Result{}, ErrDenied
	}
	if scope.host == nil {
		return extension.Result{}, extension.ErrClosed
	}
	definition, ok := extension.HostCapability(service)
	if !ok {
		return extension.Result{}, extension.ErrClosed
	}
	var contract *extension.CompiledMethod
	for _, m := range definition.Methods {
		if m.Name == method {
			var err error
			contract, err = extension.CompileMethod(m)
			if err != nil {
				return extension.Result{}, err
			}
			break
		}
	}
	if contract == nil {
		return extension.Result{}, fmt.Errorf("host method %q: %w", method, harness.ErrNotFound)
	}
	args, err := acceptNativeMethod(contract, args, scope.pure)
	if err != nil {
		return extension.Result{}, err
	}
	call := extension.CallContext{ExtensionID: scope.activation.id, SessionID: scope.session.SessionID, Workspace: scope.session.Workspace}
	if scope.lease != nil {
		ctx = context.WithValue(ctx, artifactLeaseKey{}, scope.lease)
	}
	var handler extension.Handler
	if service == extension.ContractCredentials {
		values := make(map[string]string)
		for field, binding := range scope.activation.bindings {
			values[field] = binding.value
		}
		ctx = context.WithValue(ctx, secretValuesKey{}, secretValues{activation: values, operation: scope.operationSecrets})
		handler = func(ctx context.Context, call extension.CallContext, args json.RawMessage) (extension.Result, error) {
			var input struct {
				Field string `json:"field"`
			}
			if err := json.Unmarshal(args, &input); err != nil {
				return extension.Result{}, err
			}
			return declaredCredentialHandler(ctx, call, input.Field, scope.activation.manifest.Secrets)
		}
	} else if method == "claim_external_data" {
		owner := scope.activation.owner
		owner.nativeMu.Lock()
		cleanup := false
		for _, registration := range scope.activation.entries {
			if registration.kind == "cleanup" {
				cleanup = true
				break
			}
		}
		owner.nativeMu.Unlock()
		handler = func(ctx context.Context, call extension.CallContext, _ json.RawMessage) (extension.Result, error) {
			return scope.host.claimExternalData(ctx, call, cleanup)
		}
	} else {
		handler, ok = scope.host.dataHandler(method)
		if !ok {
			return extension.Result{}, fmt.Errorf("host method %q: %w", method, harness.ErrNotFound)
		}
	}
	return executeNativeProvider(ctx, call, contract, handler, "host service", service, args)
}
