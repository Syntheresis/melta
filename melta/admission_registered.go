package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// runRegisteredAdmission runs the captured admission stage in attachment order.
func runRegisteredAdmission(ctx context.Context, stage *nativeSnapshot, input extension.AdmissionInput, states map[string]harness.SessionStateValue, host *composition, known map[string]bool, defaultStrategy string) (extension.AdmissionCandidate, error) {
	candidate := ownAdmissionCandidate(input.Candidate)
	for _, entry := range stage.hooks["admission"] {
		if err := entry.activation.unavailable(); err != nil {
			return extension.AdmissionCandidate{}, err
		}
		callCtx, cancel := context.WithCancel(ctx)
		scope := callbackScope(&nativeCallScope{
			nativeCallLifetime: nativeCallLifetime{ctx: callCtx, cancel: cancel},
			candidate:          stage.candidate, activation: entry.activation, pure: true,
			session: input.Session, states: states, host: host,
		})
		call := extension.AuthorContext{
			ExtensionID: entry.activation.id, Settings: bytes.Clone(entry.activation.section),
			Session: input.Session, State: bytes.Clone(states[entry.activation.id]), Call: scope.call,
		}
		owned := input
		owned.Candidate = ownAdmissionCandidate(candidate)
		owned.ToolCatalog = ownToolDefinitions(input.ToolCatalog)
		owned.ModelCatalog = slices.Clone(input.ModelCatalog)
		owned.Input = slices.Clone(input.Input)
		for i := range owned.Input {
			owned.Input[i].Extra = owned.Input[i].Extra.Clone()
		}
		result, err := func() (extension.AdmissionResult, error) {
			defer scope.end()
			return entry.value.(nativeHook).handler.(extension.AdmissionHook)(scope.ctx, call, owned)
		}()
		if err != nil {
			return extension.AdmissionCandidate{}, err
		}
		if result.Refusal != nil {
			if result.Candidate != nil {
				return extension.AdmissionCandidate{}, fmt.Errorf("admission hook returned both candidate and refusal: %w", harness.ErrInvalid)
			}
			return extension.AdmissionCandidate{}, fmt.Errorf("admission refused: %s: %w", *result.Refusal, harness.ErrInvalid)
		}
		if result.Candidate == nil {
			continue
		}
		next := ownAdmissionCandidate(*result.Candidate)
		if (next.Model.Provider == "") != (next.Model.Model == "") {
			return extension.AdmissionCandidate{}, fmt.Errorf("admission hook returned a partial model: %w", harness.ErrInvalid)
		}
		if next.Strategy.ID == "" || (len(next.Strategy.Settings) != 0 && !json.Valid(next.Strategy.Settings)) {
			return extension.AdmissionCandidate{}, fmt.Errorf("admission hook returned an invalid strategy: %w", harness.ErrInvalid)
		}
		if next.Strategy.ID != defaultStrategy && stage.strategies[next.Strategy.ID] == nil {
			return extension.AdmissionCandidate{}, fmt.Errorf("admission hook returned unknown strategy %q: %w", next.Strategy.ID, harness.ErrInvalid)
		}
		seen := make(map[string]bool, len(next.Tools))
		for _, definition := range next.Tools {
			if err := model.ValidateToolDefinition(definition); err != nil {
				return extension.AdmissionCandidate{}, fmt.Errorf("admission tool: %v: %w", err, harness.ErrInvalid)
			}
			if seen[definition.Name] || !known[definition.Name] {
				return extension.AdmissionCandidate{}, fmt.Errorf("admission hook returned duplicate or unknown tool %q: %w", definition.Name, harness.ErrInvalid)
			}
			seen[definition.Name] = true
		}
		candidate = next
	}
	return candidate, nil
}

func ownAdmissionCandidate(value extension.AdmissionCandidate) extension.AdmissionCandidate {
	if value.Prompt != nil {
		prompt := *value.Prompt
		value.Prompt = &prompt
	}
	value.Tools = ownToolDefinitions(value.Tools)
	value.Strategy.Settings = bytes.Clone(value.Strategy.Settings)
	return value
}
