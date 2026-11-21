package melta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

func acceptHostMethod(service, method string, args json.RawMessage, pure bool) (json.RawMessage, error) {
	definitions, ok := extension.HostService(service)
	if ok {
		for _, m := range definitions {
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

// callHostService is the one dispatch for the fixed host services that do not
// depend on a Session stage: warnings and the guarded data/credentials
// services. It reports whether the service was one of them.
func (scope *nativeCallScope) callHostService(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, bool, error) {
	switch service {
	case extension.ContractWarnings:
		if scope.activation == nil {
			return extension.Result{}, true, extension.ErrClosed
		}
		if !scope.activation.services[service] {
			return extension.Result{}, true, ErrDenied
		}
		accepted, err := acceptHostMethod(service, method, args, scope.pure)
		if err != nil {
			return extension.Result{}, true, err
		}
		result, err := scope.activation.reportWarning(ctx, method, accepted)
		return result, true, err
	case extension.ContractData, extension.ContractCredentials:
		result, err := scope.callGuardedHost(ctx, service, method, args)
		return result, true, err
	}
	return extension.Result{}, false, nil
}

func (scope *nativeCallScope) callGuardedHost(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	if scope.activation == nil || !scope.activation.services[service] || scope.pure {
		return extension.Result{}, ErrDenied
	}
	if scope.host == nil {
		return extension.Result{}, extension.ErrClosed
	}
	args, err := acceptHostMethod(service, method, args, scope.pure)
	if err != nil {
		return extension.Result{}, err
	}
	definitions, ok := extension.HostService(service)
	if !ok {
		return extension.Result{}, extension.ErrClosed
	}
	var contract *extension.CompiledMethod
	for _, m := range definitions {
		if m.Name == method {
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
	call := hostCall{ExtensionID: scope.activation.id, SessionID: scope.session.SessionID, Workspace: scope.session.Workspace}
	if scope.lease != nil {
		ctx = context.WithValue(ctx, artifactLeaseKey{}, scope.lease)
	}
	var handler hostHandler
	if service == extension.ContractCredentials {
		values := make(map[string]string)
		for field, binding := range scope.activation.bindings {
			values[field] = binding.value
		}
		ctx = context.WithValue(ctx, secretValuesKey{}, secretValues{activation: values, operation: scope.operationSecrets})
		handler = func(ctx context.Context, call hostCall, args json.RawMessage) (extension.Result, error) {
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
		handler = func(ctx context.Context, call hostCall, _ json.RawMessage) (extension.Result, error) {
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
