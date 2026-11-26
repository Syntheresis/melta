package melta

import (
	"bytes"
	"context"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// registeredExtensionTool adapts one retained registration to the normal
// Harness tool gate. The binding is held by the enclosing capture.
type registeredExtensionTool struct {
	binding          *nativeToolBinding
	dataDir          string
	lease            *artifactCallLease
	states           map[string]harness.SessionStateValue
	host             *composition
	operationSecrets map[string]map[string]string
}

func (t registeredExtensionTool) check() error {
	owner := t.binding.candidate.owner
	owner.nativeMu.Lock()
	defer owner.nativeMu.Unlock()
	activation := t.binding.entry.activation
	if t.binding.released {
		return extension.ErrClosed
	}
	return activation.unavailableLocked()
}

// Run runs the registered tool and bounds its outcome before the Harness
// hands it to result hooks. Whatever supplied the content and metadata (the
// extension's function, a worker's result or error, or the failure text of
// an activation that was lost), the outcome leaves here bounded as
// harness.BoundToolOutcome describes.
func (t registeredExtensionTool) Run(ctx context.Context, tc ToolContext, call model.ToolCall) harness.ToolOutcome {
	if err := t.check(); err != nil {
		return harness.BoundToolOutcome(harness.ToolOutcome{Result: bindingToolResult(call.ID, err, false), Evidence: harness.ExecutionNotStarted})
	}
	child, cancel := context.WithCancel(ctx)
	activation := t.binding.entry.activation
	scope := callbackScope(&nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: child, cancel: cancel},
		candidate:          t.binding.candidate, activation: activation, kind: callKindTool,
		dataDir: t.dataDir, admittedEntry: tc.AdmittedEntry,
		session: tc.Session, states: t.states, host: t.host, lease: t.lease, operationSecrets: t.operationSecrets,
	})
	defer scope.end()
	author := extension.AuthorContext{
		ExtensionID: activation.id, Settings: bytes.Clone(activation.section),
		Session: tc.Session, State: bytes.Clone(t.states[activation.id]), Call: scope.call,
	}
	return harness.BoundToolOutcome(t.binding.entry.value.(nativeTool).tool(scope.ctx, author, call))
}

func (s *nativeSnapshot) checkHooks(tc ToolContext, sel selection) []harness.ToolCheckHook {
	var hooks []harness.ToolCheckHook
	for _, registration := range s.hooks["check"] {
		entry := registration
		hooks = append(hooks, harness.ToolCheckHook{ID: entry.id, ExtensionID: entry.activation.id, Run: func(ctx context.Context, input model.ToolCall) (*string, error) {
			if err := entry.activation.unavailable(); err != nil {
				return nil, err
			}
			scope, call := registeredHookScope(ctx, s, entry, sel, tc.Session)
			defer scope.end()
			return entry.value.(nativeHook).handler.(extension.ToolCheckHook)(scope.ctx, call, input)
		}})
	}
	return hooks
}

func registeredHookScope(ctx context.Context, stage *nativeSnapshot, entry *nativeRegistration, sel selection, session harness.SessionIdentity) (*nativeCallScope, extension.AuthorContext) {
	child, cancel := context.WithCancel(ctx)
	scope := callbackScope(&nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: child, cancel: cancel},
		candidate:          stage.candidate, activation: entry.activation, kind: callKindHook,
		session: session, states: sel.states, host: sel.invocation.snapshot.composition,
		lease: sel.lease, operationSecrets: sel.operationSecrets,
	})
	call := extension.AuthorContext{
		ExtensionID: entry.activation.id, Settings: bytes.Clone(entry.activation.section),
		Session: session, State: bytes.Clone(sel.states[entry.activation.id]), Call: scope.call,
	}
	return scope, call
}

func registeredRequestHooks(stage *nativeSnapshot, sel selection, session harness.SessionIdentity) []harness.RequestHook {
	var hooks []harness.RequestHook
	for _, entry := range stage.hooks["request"] {
		hook := entry.value.(nativeHook)
		hooks = append(hooks, harness.RequestHook{
			ID: entry.id, ExtensionID: entry.activation.id,
			Run: func(ctx context.Context, input harness.RequestHookInput) (harness.RequestConsequence, error) {
				if err := entry.activation.unavailable(); err != nil {
					return harness.RequestConsequence{}, err
				}
				scope, call := registeredHookScope(ctx, stage, entry, sel, session)
				defer scope.end()
				return hook.handler.(extension.AuthorRequestHook)(scope.ctx, call, extension.AuthorRequestInput{RequestHookInput: input, PromptAssembled: sel.candidate.Prompt == nil})
			},
		})
	}
	return hooks
}

func registeredArgumentHooks(stage *nativeSnapshot, sel selection, session harness.SessionIdentity) []harness.ToolArgumentsHook {
	var hooks []harness.ToolArgumentsHook
	for _, entry := range stage.hooks["arguments"] {
		hook := entry.value.(nativeHook)
		hooks = append(hooks, harness.ToolArgumentsHook{
			ID: entry.id, ExtensionID: entry.activation.id,
			Run: func(ctx context.Context, input model.ToolCall) (harness.ToolArgumentsResult, error) {
				if err := entry.activation.unavailable(); err != nil {
					return harness.ToolArgumentsResult{}, err
				}
				scope, call := registeredHookScope(ctx, stage, entry, sel, session)
				defer scope.end()
				result, err := hook.handler.(extension.ArgumentsHook)(scope.ctx, call, input)
				if err != nil {
					return harness.ToolArgumentsResult{}, err
				}
				if result.Refusal != nil {
					return harness.ToolArgumentsResult{Refusal: result.Refusal}, nil
				}
				if result.Arguments == nil {
					return harness.ToolArgumentsResult{Arguments: input.Arguments}, nil
				}
				return harness.ToolArgumentsResult{Arguments: result.Arguments}, nil
			},
		})
	}
	return hooks
}

func registeredResultHooks(stage *nativeSnapshot, sel selection, session harness.SessionIdentity) []harness.ToolOutputHook {
	var hooks []harness.ToolOutputHook
	for _, entry := range stage.hooks["result"] {
		hook := entry.value.(nativeHook)
		hooks = append(hooks, harness.ToolOutputHook{
			ID: entry.id, ExtensionID: entry.activation.id,
			Run: func(ctx context.Context, input harness.ToolOutputInput) (string, error) {
				if err := entry.activation.unavailable(); err != nil {
					return "", err
				}
				scope, call := registeredHookScope(ctx, stage, entry, sel, session)
				defer scope.end()
				return hook.handler.(extension.ResultHook)(scope.ctx, call, input)
			},
		})
	}
	return hooks
}
