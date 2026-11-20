package melta

import (
	"context"
	"encoding/json"
	"fmt"

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

// editAgentModel drives the supplied Agents registered set_model operation
// through the production settings-update path against the published
// candidate, for tests that assert the retained model semantics.
func (s *configurationService) editAgentModel(ctx context.Context, agentType, ref string) (configurationCapture, error) {
	s.nativeMu.Lock()
	candidate := s.nativeCurrent
	var activation *nativeActivation
	if candidate != nil {
		for _, a := range candidate.activations {
			if a != nil && a.id == agents.ExtensionID {
				activation = a
				break
			}
		}
	}
	s.nativeMu.Unlock()
	if candidate == nil || activation == nil {
		return configurationCapture{}, fmt.Errorf("agents activation is not published: %w", extension.ErrClosed)
	}
	arguments, _ := json.Marshal(struct {
		DefinitionID string `json:"definition_id"`
		Model        string `json:"model"`
	}{DefinitionID: agentType, Model: ref})
	callCtx, cancel := context.WithCancel(ctx)
	scope := &nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: callCtx, cancel: cancel},
		candidate:          candidate, activation: activation,
		commandScope: harnessScope, receipt: &nativeReceipt{},
	}
	defer scope.end()
	settingsArgs, _ := json.Marshal(struct {
		Service   string          `json:"service"`
		Method    string          `json:"method"`
		Arguments json.RawMessage `json:"arguments"`
	}{agents.ConfigurationCapabilityID, "set_model", arguments})
	if _, err := scope.call(callCtx, extension.ContractSettings, "update", settingsArgs); err != nil {
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
