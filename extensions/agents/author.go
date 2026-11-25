package agents

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

const emptyObject = `{"type":"object","additionalProperties":false}`
const agentInput = `{"type":"object","required":["agent"],"properties":{"agent":{"type":"string","minLength":1}},"additionalProperties":false}`
const createInput = `{"type":"object","properties":{"agent":{"type":"string","minLength":1}},"additionalProperties":false}`
const currentOutput = `{"type":"object","properties":{"agent":{"type":"string","minLength":1}},"additionalProperties":false}`
const rosterOutput = `{"type":"array","items":{"type":"object","required":["name","description","subagent","model"],"properties":{"name":{"type":"string"},"description":{"type":"string"},"subagent":{"type":"boolean"},"model":{"type":"string"}},"additionalProperties":false}}`

// Author returns the activation entry for Agents.
func Author() extension.Extension {
	return extension.Extension{
		Manifest: extension.Manifest{ID: ExtensionID, Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(configSchema), Services: []string{"melta.state", "melta.session", "melta.commands", "melta.settings", "melta.workspace", "melta.warnings"}},
		Activate: activateAuthor,
	}
}

func activateAuthor(ctx context.Context, api extension.API) (func(context.Context) error, error) {
	var section struct {
		Definitions json.RawMessage `json:"definitions"`
	}
	if err := json.Unmarshal(api.Settings(), &section); err != nil {
		return nil, err
	}
	if len(section.Definitions) == 0 {
		section.Definitions = []byte(`{}`)
	}
	definitions, err := parse(section.Definitions)
	if err != nil {
		return nil, err
	}
	for _, warning := range definitions.Warnings() {
		args, _ := json.Marshal(struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
		}{warning.Kind, warning.Error()})
		if _, err := api.Call(ctx, "melta.warnings", "report", args); err != nil {
			return nil, err
		}
	}
	settings, err := decodeSettings(api.Settings())
	if err != nil {
		return nil, err
	}
	s := authorInstance{definitions: definitions}
	operations := []extension.Operation{
		{Target: extension.ExposedTargetHarness, Method: extension.ServiceMethod("list", emptyObject, rosterOutput, "", ""), Handler: s.list},
		{Target: extension.ExposedTargetSession, Method: extension.ServiceMethod("current", emptyObject, currentOutput, "", ""), Handler: s.current},
		{Target: extension.ExposedTargetSession, Method: extension.ServiceMethod("select", agentInput, `{"type":"null"}`, "", ""), Handler: s.selectAgent},
		{Target: extension.ExposedTargetWorkspace, Method: extension.ServiceMethod("create", createInput, `{"type":"object"}`, "", `{"type":"object"}`), Handler: s.create},
		{Target: extension.ExposedTargetHarness, Method: extension.ServiceMethod("set_model", setModelInputSchema, `{"type":"null"}`, "", ""), Handler: s.setModel},
	}
	for _, op := range operations {
		if _, err := api.RegisterOperation(op, nil); err != nil {
			return nil, err
		}
	}
	if _, err := api.RegisterTool(model.ToolDefinition{Name: TaskCapabilityID, Description: taskDescription(definitions), Parameters: []byte(taskParameters)}, authorTask{definitions: definitions, settings: settings}, nil); err != nil {
		return nil, err
	}
	if _, err := api.AttachAdmission(s.admit, nil); err != nil {
		return nil, err
	}
	if _, err := api.AttachArguments(extension.HookModePure, s.arguments, nil); err != nil {
		return nil, err
	}
	if _, err := api.AttachAuthorization(s.authorize, nil); err != nil {
		return nil, err
	}
	return nil, nil
}

func taskDescription(definitions *Config) string {
	var description strings.Builder
	description.WriteString(taskDescriptionPrefix + "\n\nAvailable subagent types:\n")
	for _, definition := range definitions.All() {
		if definition.Subagent {
			fmt.Fprintf(&description, "- %s: %s\n", definition.Name, definition.Description)
		}
	}
	return description.String()
}

type authorInstance struct {
	definitions *Config
}

func (s authorInstance) list(_ context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
	type row struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Subagent    bool   `json:"subagent"`
		Model       string `json:"model"`
	}
	rows := make([]row, 0)
	for _, item := range s.definitions.All() {
		rows = append(rows, row{item.Name, item.Description, item.Subagent, item.Model})
	}
	value, err := json.Marshal(rows)
	return extension.Result{Value: value}, err
}

func selected(state harness.SessionStateValue) string {
	var value struct {
		Agent string `json:"agent"`
	}
	if json.Unmarshal(state, &value) != nil {
		return ""
	}
	return value.Agent
}

func (s authorInstance) current(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
	name := selected(call.State)
	if _, err := s.definitions.Resolve(name); err != nil {
		name = ""
	}
	value, err := json.Marshal(struct {
		Agent string `json:"agent,omitempty"`
	}{name})
	return extension.Result{Value: value}, err
}

func (s authorInstance) selectAgent(ctx context.Context, call extension.AuthorContext, raw json.RawMessage) (extension.Result, error) {
	var input struct {
		Agent string `json:"agent"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return extension.Result{}, err
	}
	if _, err := s.definitions.Resolve(input.Agent); err != nil {
		return extension.Result{}, fmt.Errorf("%v: %w", err, harness.ErrInvalid)
	}
	args, _ := json.Marshal(struct {
		Value any `json:"value"`
	}{struct {
		Agent string `json:"agent"`
	}{input.Agent}})
	return call.Call(ctx, "melta.state", "set", args)
}

func (s authorInstance) create(ctx context.Context, call extension.AuthorContext, raw json.RawMessage) (extension.Result, error) {
	var input struct {
		Agent *string `json:"agent"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return extension.Result{}, err
	}
	name := "primary"
	if input.Agent != nil {
		name = *input.Agent
	}
	if _, err := s.definitions.Resolve(name); err != nil {
		return extension.Result{}, fmt.Errorf("%v: %w", err, harness.ErrInvalid)
	}
	args, _ := json.Marshal(struct {
		Workspace    string `json:"workspace"`
		InitialState any    `json:"initial_state"`
	}{call.Session.Workspace, struct {
		Agent string `json:"agent"`
	}{name}})
	return call.Call(ctx, "melta.commands", "create_session", args)
}

func (s authorInstance) setModel(ctx context.Context, call extension.AuthorContext, raw json.RawMessage) (extension.Result, error) {
	transformed, err := transformModel(call.Settings, raw, parse)
	if err != nil {
		return extension.Result{}, err
	}
	args, err := json.Marshal(struct {
		Section json.RawMessage `json:"section"`
	}{transformed.Value})
	if err != nil {
		return extension.Result{}, err
	}
	return call.Call(ctx, "melta.settings", "replace", args)
}
