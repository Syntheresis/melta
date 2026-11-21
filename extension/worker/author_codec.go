package worker

import (
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// DecodeAuthorFact reads one committed fact delivered to an observer.
func DecodeAuthorFact(raw json.RawMessage) (extension.Fact, error) {
	var fact extension.Fact
	if err := unmarshalValue(raw, &fact); err != nil {
		return extension.Fact{}, err
	}
	if fact.Kind != extension.FactOperationAdmitted && fact.Kind != extension.FactOperationSettled {
		return extension.Fact{}, fmt.Errorf("worker: unknown fact kind %q", fact.Kind)
	}
	return fact, nil
}

// EncodeAuthorRequest keeps the author-only prompt fact separate from v4.
func EncodeAuthorRequest(input extension.AuthorRequestInput) (json.RawMessage, error) {
	base, err := EncodeRequestHookParams(input.RequestHookInput)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Request         JSONBytes `json:"request_base64"`
		PromptAssembled bool      `json:"prompt_assembled"`
	}{JSONBytes(base), input.PromptAssembled})
}

func DecodeAuthorRequest(raw json.RawMessage) (extension.AuthorRequestInput, error) {
	var wire struct {
		Request         JSONBytes `json:"request_base64"`
		PromptAssembled bool      `json:"prompt_assembled"`
	}
	if err := unmarshalValue(raw, &wire); err != nil {
		return extension.AuthorRequestInput{}, err
	}
	input, err := DecodeRequestHookParams(json.RawMessage(wire.Request))
	return extension.AuthorRequestInput{RequestHookInput: input, PromptAssembled: wire.PromptAssembled}, err
}

type authorStartJSON struct {
	Target       string    `json:"target"`
	Inputs       JSONBytes `json:"inputs"`
	InitialState JSONBytes `json:"initial_state"`
	Input        JSONBytes `json:"input"`
	Report       bool      `json:"report"`
	OutputLimit  int       `json:"output_limit"`
}

type authorInputsJSON struct {
	Model *struct {
		Provider string `json:"provider"`
		Model    string `json:"model"`
	} `json:"model,omitempty"`
	Instructions *string   `json:"instructions,omitempty"`
	Prompt       *string   `json:"prompt,omitempty"`
	Tools        *[]string `json:"tools,omitempty"`
	Strategy     *struct {
		ID       string     `json:"id"`
		Settings *JSONBytes `json:"settings,omitempty"`
	} `json:"strategy,omitempty"`
}

func authorInputsFrom(inputs harness.SessionInputs) authorInputsJSON {
	wire := authorInputsJSON{Instructions: inputs.Instructions, Prompt: inputs.Prompt}
	if inputs.Tools != nil {
		wire.Tools = &inputs.Tools
	}
	if inputs.Model != nil {
		wire.Model = &struct {
			Provider string `json:"provider"`
			Model    string `json:"model"`
		}{inputs.Model.Provider, inputs.Model.Model}
	}
	if inputs.Strategy != nil {
		wire.Strategy = &struct {
			ID       string     `json:"id"`
			Settings *JSONBytes `json:"settings,omitempty"`
		}{ID: inputs.Strategy.ID}
		if inputs.Strategy.Settings != nil {
			settings := JSONBytes(inputs.Strategy.Settings)
			wire.Strategy.Settings = &settings
		}
	}
	return wire
}

func authorInputsValue(wire authorInputsJSON) harness.SessionInputs {
	inputs := harness.SessionInputs{Instructions: wire.Instructions, Prompt: wire.Prompt}
	if wire.Tools != nil {
		inputs.Tools = *wire.Tools
	}
	if wire.Model != nil {
		inputs.Model = &model.ModelRef{Provider: wire.Model.Provider, Model: wire.Model.Model}
	}
	if wire.Strategy != nil {
		inputs.Strategy = &harness.StrategySelection{ID: wire.Strategy.ID}
		if wire.Strategy.Settings != nil {
			inputs.Strategy.Settings = append(json.RawMessage{}, (*wire.Strategy.Settings)...)
		}
	}
	return inputs
}

type authorCandidateJSON struct {
	Model        modelRefJSON          `json:"model"`
	Prompt       *string               `json:"prompt,omitempty"`
	Instructions string                `json:"instructions"`
	Tools        *[]toolDefinitionJSON `json:"tools,omitempty"`
	Strategy     authorStrategyJSON    `json:"strategy"`
}

type authorStrategyJSON struct {
	ID       string     `json:"id"`
	Settings *JSONBytes `json:"settings,omitempty"`
}

func authorCandidateFrom(candidate extension.AdmissionCandidate) (authorCandidateJSON, error) {
	ref, err := modelRefFrom(candidate.Model)
	if err != nil {
		return authorCandidateJSON{}, err
	}
	wire := authorCandidateJSON{Model: ref, Prompt: candidate.Prompt, Instructions: candidate.Instructions,
		Strategy: authorStrategyJSON{ID: candidate.Strategy.ID}}
	if candidate.Strategy.Settings != nil {
		settings := JSONBytes(candidate.Strategy.Settings)
		wire.Strategy.Settings = &settings
	}
	if candidate.Tools != nil {
		tools := toolDefinitionsFrom(candidate.Tools)
		wire.Tools = &tools
	}
	return wire, nil
}

func authorCandidateValue(wire authorCandidateJSON) extension.AdmissionCandidate {
	candidate := extension.AdmissionCandidate{Model: modelRefValue(wire.Model), Prompt: wire.Prompt,
		Instructions: wire.Instructions, Strategy: harness.StrategySelection{ID: wire.Strategy.ID}}
	if wire.Tools != nil {
		candidate.Tools = toolDefinitionsValue(*wire.Tools)
	}
	if wire.Strategy.Settings != nil {
		candidate.Strategy.Settings = append(json.RawMessage{}, (*wire.Strategy.Settings)...)
	}
	return candidate
}

type authorAdmissionJSON struct {
	Session      harness.SessionIdentity `json:"session"`
	Input        []contentPartJSON       `json:"input"`
	Choices      authorInputsJSON        `json:"choices"`
	Candidate    authorCandidateJSON     `json:"candidate"`
	ModelCatalog []modelRefJSON          `json:"model_catalog"`
	ToolCatalog  []toolDefinitionJSON    `json:"tool_catalog"`
}

func EncodeAuthorAdmission(input extension.AdmissionInput) (json.RawMessage, error) {
	candidate, err := authorCandidateFrom(input.Candidate)
	if err != nil {
		return nil, err
	}
	models := make([]modelRefJSON, len(input.ModelCatalog))
	for i, ref := range input.ModelCatalog {
		models[i], err = modelRefFrom(ref)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(authorAdmissionJSON{
		Session: input.Session, Input: contentFrom(input.Input), Choices: authorInputsFrom(input.Choices),
		Candidate: candidate, ModelCatalog: models, ToolCatalog: toolDefinitionsFrom(input.ToolCatalog),
	})
}

func DecodeAuthorAdmission(raw json.RawMessage) (extension.AdmissionInput, error) {
	var wire authorAdmissionJSON
	if err := unmarshalValue(raw, &wire); err != nil {
		return extension.AdmissionInput{}, err
	}
	models := make([]model.ModelRef, len(wire.ModelCatalog))
	for i, ref := range wire.ModelCatalog {
		models[i] = modelRefValue(ref)
	}
	return extension.AdmissionInput{Session: wire.Session, Input: contentValue(wire.Input), Choices: authorInputsValue(wire.Choices),
		Candidate: authorCandidateValue(wire.Candidate), ModelCatalog: models, ToolCatalog: toolDefinitionsValue(wire.ToolCatalog)}, nil
}

func EncodeAuthorAdmissionResult(result extension.AdmissionResult) (json.RawMessage, error) {
	wire := struct {
		Candidate *authorCandidateJSON `json:"candidate,omitempty"`
		Refusal   *string              `json:"refusal,omitempty"`
	}{Refusal: result.Refusal}
	if result.Candidate != nil {
		candidate, err := authorCandidateFrom(*result.Candidate)
		if err != nil {
			return nil, err
		}
		wire.Candidate = &candidate
	}
	return json.Marshal(wire)
}

func DecodeAuthorAdmissionResult(raw json.RawMessage) (extension.AdmissionResult, error) {
	var wire struct {
		Candidate *authorCandidateJSON `json:"candidate,omitempty"`
		Refusal   *string              `json:"refusal,omitempty"`
	}
	if err := unmarshalValue(raw, &wire); err != nil {
		return extension.AdmissionResult{}, err
	}
	result := extension.AdmissionResult{Refusal: wire.Refusal}
	if wire.Candidate != nil {
		candidate := authorCandidateValue(*wire.Candidate)
		result.Candidate = &candidate
	}
	return result, nil
}

type authorPlanJSON struct {
	Plan  JSONBytes        `json:"plan"`
	Start *authorStartJSON `json:"start,omitempty"`
}

type authorArgumentsResultJSON struct {
	Arguments *JSONBytes `json:"arguments,omitempty"`
	Refusal   *string    `json:"refusal,omitempty"`
}

type authorAuthorizationJSON struct {
	ToolName           string                      `json:"tool_name"`
	Arguments          JSONBytes                   `json:"arguments"`
	CanonicalWorkspace string                      `json:"canonical_workspace"`
	ToolSettings       JSONBytes                   `json:"tool_settings"`
	Permissions        []harness.PermissionRequest `json:"permissions,omitempty"`
}

func EncodeAuthorAuthorization(v extension.AuthorizationInput) (json.RawMessage, error) {
	return json.Marshal(authorAuthorizationJSON{v.ToolName, JSONBytes(v.Arguments), v.CanonicalWorkspace, JSONBytes(v.ToolSettings), v.Permissions})
}

func DecodeAuthorAuthorization(raw json.RawMessage) (extension.AuthorizationInput, error) {
	var v authorAuthorizationJSON
	if err := json.Unmarshal(raw, &v); err != nil {
		return extension.AuthorizationInput{}, err
	}
	return extension.AuthorizationInput{ToolName: v.ToolName, Arguments: append(json.RawMessage(nil), v.Arguments...), CanonicalWorkspace: v.CanonicalWorkspace, ToolSettings: append(json.RawMessage(nil), v.ToolSettings...), Permissions: v.Permissions}, nil
}

func EncodeAuthorRefusal(refusal *string) (json.RawMessage, error) {
	return json.Marshal(struct {
		Refusal *string `json:"refusal,omitempty"`
	}{refusal})
}

func DecodeAuthorRefusal(raw json.RawMessage) (*string, error) {
	var v struct {
		Refusal *string `json:"refusal,omitempty"`
	}
	err := json.Unmarshal(raw, &v)
	return v.Refusal, err
}

func EncodeAuthorArgumentsResult(result extension.ArgumentsResult) (json.RawMessage, error) {
	wire := authorArgumentsResultJSON{Refusal: result.Refusal}
	if result.Arguments != nil {
		value := JSONBytes(result.Arguments)
		wire.Arguments = &value
	}
	return json.Marshal(wire)
}

func DecodeAuthorArgumentsResult(raw json.RawMessage) (extension.ArgumentsResult, error) {
	var wire authorArgumentsResultJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return extension.ArgumentsResult{}, err
	}
	result := extension.ArgumentsResult{Refusal: wire.Refusal}
	if wire.Arguments != nil {
		result.Arguments = append(json.RawMessage{}, (*wire.Arguments)...)
	}
	return result, nil
}

// EncodeAuthorPlan preserves the complete author plan as opaque payloads.
func EncodeAuthorPlan(plan extension.ToolPlan) (json.RawMessage, error) {
	base, err := EncodePrepareResult(plan)
	if err != nil {
		return nil, err
	}
	wire := authorPlanJSON{Plan: JSONBytes(base)}
	if plan.Start != nil {
		inputs, err := json.Marshal(authorInputsFrom(plan.Start.Inputs))
		if err != nil {
			return nil, err
		}
		var parts []contentPartJSON
		if plan.Start.Input != nil {
			parts = contentFrom(plan.Start.Input)
		}
		input, err := json.Marshal(parts)
		if err != nil {
			return nil, err
		}
		wire.Start = &authorStartJSON{
			Target: plan.Start.Target, Inputs: inputs, InitialState: JSONBytes(plan.Start.InitialState),
			Input: input, Report: plan.Start.Report, OutputLimit: plan.Start.OutputLimit,
		}
	}
	return json.Marshal(wire)
}

// DecodeAuthorPlan decodes the complete author plan.
func DecodeAuthorPlan(raw json.RawMessage) (extension.ToolPlan, error) {
	var wire authorPlanJSON
	if err := json.Unmarshal(raw, &wire); err != nil {
		return extension.ToolPlan{}, err
	}
	plan, err := DecodePrepareResult(json.RawMessage(wire.Plan))
	if err != nil || wire.Start == nil {
		return plan, err
	}
	var inputs authorInputsJSON
	if err := json.Unmarshal(wire.Start.Inputs, &inputs); err != nil {
		return extension.ToolPlan{}, err
	}
	var input []contentPartJSON
	if err := json.Unmarshal(wire.Start.Input, &input); err != nil {
		return extension.ToolPlan{}, err
	}
	var parts []model.ContentPart
	if input != nil {
		parts = contentValue(input)
	}
	plan.Start = &extension.SessionStart{
		Target: wire.Start.Target, Inputs: authorInputsValue(inputs), InitialState: append(harness.SessionStateValue(nil), wire.Start.InitialState...),
		Input: parts, Report: wire.Start.Report, OutputLimit: wire.Start.OutputLimit,
	}
	return plan, nil
}
