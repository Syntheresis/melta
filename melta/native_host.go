package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

// hostMethod is one host service method's dispatch contract: its compiled
// schemas, the call kinds allowed to reach it, whether it works for a Session,
// and the handler. The table is built and compiled once for the process.
type hostMethod struct {
	contract      *extension.CompiledMethod
	kinds         map[nativeCallKind]bool
	session       bool
	harnessTarget bool
	callerScope   bool
	handler       func(*nativeCallScope, context.Context, string, json.RawMessage) (extension.Result, error)
}

func callKinds(kinds ...nativeCallKind) map[nativeCallKind]bool {
	out := make(map[nativeCallKind]bool, len(kinds))
	for _, kind := range kinds {
		out[kind] = true
	}
	return out
}

var allCallKinds = callKinds(callKindActivation, callKindAdmission, callKindHook, callKindTool, callKindOperation)

var (
	nativeHostMethodsOnce sync.Once
	nativeHostMethods     map[string]map[string]*hostMethod
)

// hostServiceMethods returns the process-wide compiled host dispatch table.
func hostServiceMethods() map[string]map[string]*hostMethod {
	nativeHostMethodsOnce.Do(func() {
		nativeHostMethods = buildNativeHostMethods()
	})
	return nativeHostMethods
}

func buildNativeHostMethods() map[string]map[string]*hostMethod {
	table := make(map[string]map[string]*hostMethod)
	add := func(service, method string, kinds map[nativeCallKind]bool, session, harnessTarget, callerScope bool, handler func(*nativeCallScope, context.Context, string, json.RawMessage) (extension.Result, error)) {
		definitions, ok := extension.HostService(service)
		if !ok {
			panic(fmt.Sprintf("host service %q is not declared", service))
		}
		var declared extension.Method
		found := false
		for _, candidate := range definitions {
			if candidate.Name == method {
				declared, found = candidate, true
				break
			}
		}
		if !found {
			panic(fmt.Sprintf("host method %q.%q is not declared", service, method))
		}
		contract, err := extension.CompileMethod(declared)
		if err != nil {
			panic(fmt.Sprintf("host method %q.%q: %v", service, method, err))
		}
		if table[service] == nil {
			table[service] = make(map[string]*hostMethod)
		}
		table[service][method] = &hostMethod{contract: contract, kinds: kinds, session: session, harnessTarget: harnessTarget, callerScope: callerScope, handler: handler}
	}

	add(extension.ContractCredentials, "get", allCallKinds, false, false, false, (*nativeCallScope).credentialsService)
	add(extension.ContractData, "cache_namespace", allCallKinds, false, false, false, (*nativeCallScope).dataService)
	add(extension.ContractData, "session_namespace", callKinds(callKindHook, callKindTool, callKindOperation), true, false, false, (*nativeCallScope).dataService)
	add(extension.ContractData, "claim_external_data", callKinds(callKindHook, callKindTool, callKindOperation), true, false, false, (*nativeCallScope).dataService)
	add(extension.ContractWarnings, "report", allCallKinds, false, false, false, (*nativeCallScope).warningsService)
	add(extension.ContractSession, "members", callKinds(callKindAdmission, callKindHook, callKindTool, callKindOperation), true, false, false, (*nativeCallScope).sessionService)
	add(extension.ContractSession, "start", allCallKinds, true, false, false, (*nativeCallScope).sessionService)
	add(extension.ContractState, "set", callKinds(callKindHook, callKindTool, callKindOperation), true, false, false, (*nativeCallScope).stateService)
	add(extension.ContractWorkspace, "bind", allCallKinds, false, false, false, (*nativeCallScope).workspaceService)
	add(extension.ContractWorkspace, "read", allCallKinds, false, false, false, (*nativeCallScope).workspaceService)
	add(extension.ContractWorkspace, "write", allCallKinds, false, false, false, (*nativeCallScope).workspaceService)
	add(extension.ContractWorkspace, "delete", allCallKinds, false, false, false, (*nativeCallScope).workspaceService)
	add(extension.ContractSettings, "replace", callKinds(callKindOperation), false, true, false, (*nativeCallScope).settingsService)
	commands, _ := extension.HostService(extension.ContractCommands)
	for _, method := range commands {
		add(extension.ContractCommands, method.Name, callKinds(callKindOperation), false, false, true, (*nativeCallScope).commandsService)
	}
	return table
}

// dispatch is the one order of checks every host service call follows: the
// caller already established that its lifetime is open and its activation
// available. A service the host does not declare falls through to another
// extension's service with that path's own checks unchanged.
func (scope *nativeCallScope) dispatch(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	methods, hostService := hostServiceMethods()[service]
	if !hostService {
		return scope.candidate.callServiceScoped(ctx, service, method, args, scope)
	}
	if scope.activation == nil || !scope.activation.services[service] {
		return extension.Result{}, ErrDenied
	}
	entry := methods[method]
	if entry == nil {
		return extension.Result{}, fmt.Errorf("host method %q: %w", method, harness.ErrNotFound)
	}
	if !entry.kinds[scope.kind] || (entry.session && scope.session.SessionID == "") {
		return extension.Result{}, ErrDenied
	}
	if entry.harnessTarget && scope.commandScope.Kind != CallerScopeHarness {
		return extension.Result{}, ErrDenied
	}
	if entry.callerScope && scope.commandScope.Kind == "" {
		return extension.Result{}, ErrDenied
	}
	args, err := acceptNativeMethod(entry.contract, args)
	if err != nil {
		return extension.Result{}, err
	}
	result, err := entry.handler(scope, ctx, method, args)
	if err != nil {
		return extension.Result{}, ownCallError(err)
	}
	result = ownResult(result)
	if err := entry.contract.ValidateResult(result); err != nil {
		return extension.Result{}, fmt.Errorf("host service %q: %w", service, err)
	}
	return result, nil
}

func hostCallOf(scope *nativeCallScope) hostCall {
	return hostCall{ExtensionID: scope.activation.id, SessionID: scope.session.SessionID, Workspace: scope.session.Workspace}
}

func (scope *nativeCallScope) warningsService(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	return scope.activation.reportWarning(ctx, method, args)
}

func (scope *nativeCallScope) credentialsService(ctx context.Context, _ string, args json.RawMessage) (extension.Result, error) {
	if scope.host == nil {
		return extension.Result{}, extension.ErrClosed
	}
	values := make(map[string]string, len(scope.activation.bindings))
	for field, binding := range scope.activation.bindings {
		values[field] = binding.value
	}
	ctx = context.WithValue(ctx, secretValuesKey{}, secretValues{activation: values, operation: scope.operationSecrets})
	var input struct {
		Field string `json:"field"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return extension.Result{}, err
	}
	return declaredCredentialHandler(ctx, hostCallOf(scope), input.Field, scope.activation.manifest.Secrets)
}

func (scope *nativeCallScope) dataService(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	if scope.host == nil {
		return extension.Result{}, extension.ErrClosed
	}
	if scope.lease != nil {
		ctx = context.WithValue(ctx, artifactLeaseKey{}, scope.lease)
	}
	call := hostCallOf(scope)
	if method == "claim_external_data" {
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
		return scope.host.claimExternalData(ctx, call, cleanup)
	}
	handler, ok := scope.host.dataHandler(method)
	if !ok {
		return extension.Result{}, fmt.Errorf("host method %q: %w", method, harness.ErrNotFound)
	}
	return handler(ctx, call, args)
}

func (scope *nativeCallScope) sessionService(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	if scope.host == nil || scope.host.hosts == nil || scope.host.hosts.background == nil || scope.host.hosts.background.h == nil {
		return extension.Result{}, extension.ErrClosed
	}
	h := scope.host.hosts.background.h
	switch method {
	case "members":
		members, err := h.SessionMembers(ctx, scope.session.SessionID)
		return commandResult(members, err)
	case "start":
		if scope.kind != callKindTool || scope.workspaceBind || scope.start == nil {
			return extension.Result{}, ErrDenied
		}
		start := scope.start
		start.mu.Lock()
		if start.consumed {
			start.mu.Unlock()
			return extension.Result{}, fmt.Errorf("session start already consumed: %w", harness.ErrConflict)
		}
		start.consumed = true
		start.mu.Unlock()
		operationID, err := mintInstance()
		if err != nil {
			return extension.Result{}, err
		}
		var states map[string]harness.SessionStateValue
		if start.plan.InitialState != nil {
			states = map[string]harness.SessionStateValue{start.extensionID: bytes.Clone(start.plan.InitialState)}
		}
		launched, err := h.LaunchChildSession(ctx, harness.LaunchChildRequest{
			ParentSessionID: scope.session.SessionID, ExtensionID: start.extensionID,
			OperationID: operationID, Inputs: start.plan.Inputs, ExtensionState: states,
			Content: start.plan.Input, Report: start.plan.Report, OutputLimit: start.plan.OutputLimit,
		})
		value, marshalErr := json.Marshal(struct {
			SessionID string `json:"session_id"`
		}{launched.ChildSessionID})
		if marshalErr != nil {
			return extension.Result{}, marshalErr
		}
		if err != nil {
			if launched.ChildSessionID != "" {
				return extension.Result{}, &extension.CallError{Err: err, Partial: value}
			}
			return extension.Result{}, err
		}
		return extension.Result{Value: value}, nil
	}
	return extension.Result{}, fmt.Errorf("host method %q: %w", method, harness.ErrNotFound)
}

func (scope *nativeCallScope) stateService(ctx context.Context, _ string, args json.RawMessage) (extension.Result, error) {
	if scope.host == nil || scope.host.hosts == nil || scope.host.hosts.background == nil || scope.host.hosts.background.h == nil {
		return extension.Result{}, extension.ErrClosed
	}
	h := scope.host.hosts.background.h
	var input struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return extension.Result{}, fmt.Errorf("state value: %v: %w", err, harness.ErrInvalid)
	}
	if _, err := h.SetSessionExtensionState(ctx, scope.session.SessionID, scope.activation.id, harness.SessionStateValue(input.Value)); err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: json.RawMessage("null")}, nil
}

func (scope *nativeCallScope) workspaceService(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	if scope.workspace == nil {
		return extension.Result{}, ErrDenied
	}
	if method == "bind" {
		if !scope.workspaceBind {
			return extension.Result{}, ErrDenied
		}
		return scope.workspace.bind(args)
	}
	if scope.kind != callKindTool || scope.workspaceBind {
		return extension.Result{}, ErrDenied
	}
	return scope.workspace.execute(ctx, method, args)
}

func (scope *nativeCallScope) settingsService(ctx context.Context, _ string, args json.RawMessage) (extension.Result, error) {
	return scope.updateSettings(ctx, args)
}

func (scope *nativeCallScope) commandsService(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	if scope.host == nil {
		return extension.Result{}, extension.ErrClosed
	}
	handler, ok := scope.host.commandsHandler(method)
	if !ok {
		return extension.Result{}, fmt.Errorf("command method %q: %w", method, harness.ErrNotFound)
	}
	ctx = context.WithValue(ctx, grantContextKey{}, nativeGrant{commands: true, commandScope: scope.commandScope})
	return handler(ctx, hostCallOf(scope), args)
}
