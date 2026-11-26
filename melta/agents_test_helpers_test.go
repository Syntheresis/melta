package melta

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/extension"
	agents "github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/protocol"
)

// agentState supplies one Agents-owned Session state value.
func agentState(name string) map[string]harness.SessionStateValue {
	raw, _ := json.Marshal(struct {
		Agent string `json:"agent"`
	}{name})
	return map[string]harness.SessionStateValue{agents.ExtensionID: raw}
}

// agentsTestNative is the real supplied Agents extension author entry.
func agentsTestNative() extension.Extension {
	return agents.Author()
}

// preparedService is the isolated service harness over the production
// resource path: the owner composition and data root are attached so
// candidate publication prepares the runtime resource set and the author
// specs activate.
func (h *serviceHarness) preparedService(owner context.Context, authors ...extension.Extension) *configurationService {
	h.t.Helper()
	svc := h.service(owner, authors...)
	svc.dataDir = h.dataDir
	return svc
}

// editAgentModel drives the supplied Agents published set_model operation
// through its registered handler for tests that assert the retained model
// semantics.
func (s *configurationService) editAgentModel(ctx context.Context, agentType, ref string) (configurationCapture, error) {
	s.nativeMu.Lock()
	candidate := s.nativeCurrent
	s.nativeMu.Unlock()
	if candidate == nil {
		return configurationCapture{}, extension.ErrClosed
	}
	stage := candidate.snapshotForTest()
	if stage == nil {
		return configurationCapture{}, extension.ErrClosed
	}
	defer stage.release()
	entry := stage.operations[nativeOperationKey{agents.ExtensionID, "set_model"}]
	if entry == nil {
		return configurationCapture{}, extension.ErrClosed
	}
	operation := entry.value.(struct {
		target extension.ExposedTargetKind
		method nativeServiceMethod
	})
	arguments, err := json.Marshal(struct {
		DefinitionID string `json:"definition_id"`
		Model        string `json:"model"`
	}{DefinitionID: agentType, Model: ref})
	if err != nil {
		return configurationCapture{}, err
	}
	callCtx, cancel := context.WithCancel(ctx)
	scope := callbackScope(&nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: callCtx, cancel: cancel},
		candidate:          candidate, activation: entry.activation, kind: callKindOperation,
		commandScope: harnessScope,
	})
	author := extension.AuthorContext{
		ExtensionID: entry.activation.id, Settings: bytes.Clone(entry.activation.section), Call: scope.call,
	}
	_, err = func() (extension.Result, error) {
		defer scope.end()
		return executeNativeProvider(scope.ctx, author, operation.method.contract, operation.method.handler, "operation", "set_model", arguments)
	}()
	if err != nil {
		return configurationCapture{}, err
	}
	return configurationCapture{snapshot: s.current()}, nil
}

// updateSettingsOnly drives the settings arm of the settings request.
func updateSettingsOnly(r *Melta, ctx context.Context, settings protocol.Settings) (protocol.SettingsMutation, error) {
	return r.UpdateSettings(ctx, testCaller(r), protocol.UpdateSettingsRequest{Settings: settings})
}

// setAgentTypeModel drives the supplied Agents registered set_model harness
// operation for tests that assert the retained model propagation.
func (r *Melta) setAgentTypeModel(ctx context.Context, agentType, ref string) (extension.Result, error) {
	arguments, err := json.Marshal(struct {
		DefinitionID string `json:"definition_id"`
		Model        string `json:"model"`
	}{DefinitionID: agentType, Model: ref})
	if err != nil {
		return extension.Result{}, err
	}
	return r.invokeRegisteredOperation(ctx, testCaller(r), agents.ExtensionID, "set_model", protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}, arguments, "")
}
