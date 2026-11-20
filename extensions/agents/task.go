// Package agents owns the Agents extension: the custom Agent definition
// format, its read-only schema-bound resolver, the supplied model
// configuration edit, and the child-session task tool. Every contribution is
// one ordinary Harness-scoped native component over public contracts.
package agents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/toolargs"
	"github.com/Syntheresis/melta/model"
)

const (
	// ExtensionID is the supplied extension identity.
	ExtensionID = "agents"
	// ConfigurationCapabilityID is the supplied pure configuration-edit
	// service.
	ConfigurationCapabilityID = "agents.configuration"
	// TaskCapabilityID is the model-visible child-session tool.
	TaskCapabilityID = "task"
)

// taskDescriptionPrefix is the fixed model-facing introduction every task
// tool description opens with.
const taskDescriptionPrefix = "Spawn a subagent to work on a task in the background. The subagent runs in its own context with the toolset defined by its subagent_type. You will be notified when it completes; the result arrives as a message. Continue with your next steps in the meantime."

// taskParameters is the retained schema: prompt and subagent_type, both
// required strings, with their retained leaf descriptions.
const taskParameters = `{"type":"object","properties":{"prompt":{"type":"string","description":"The task prompt for this subagent."},"subagent_type":{"type":"string","description":"The type of subagent to use."}},"required":["prompt","subagent_type"]}`

// taskStartedTemplate is the retained immediate success text; the single
// argument is the child session ID. No live-members line is forced.
const taskStartedTemplate = "Task started in the background with session ID: `%s`. You will be notified when it completes. Continue with your next steps; the result will arrive as a message when it is ready."

const configSchema = `{"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"},"definitions":true,"task":{"anyOf":[{"type":"null"},{"type":"object","properties":{"max_concurrent":{"anyOf":[{"type":"integer","minimum":1,"maximum":20},{"type":"null"}]},"max_output_bytes":{"anyOf":[{"type":"integer","minimum":1024,"maximum":1048576},{"type":"null"}]}},"additionalProperties":true}]}},"additionalProperties":true}`

// settings is the tool's private configuration, decoded once from the task
// member of the active extensions.agents section at Open. Nil input and
// missing or null-valued members keep the defaults; unknown members are
// ignored; every consumed field is range-checked against the retained
// bounds.
type settings struct {
	MaxConcurrent  int `json:"max_concurrent"`
	MaxOutputBytes int `json:"max_output_bytes"`
}

func defaultSettings() settings {
	return settings{MaxConcurrent: 4, MaxOutputBytes: 15360}
}

// decodeSettings decodes the task member on top of the defaults and enforces
// the retained ranges.
func decodeSettings(raw json.RawMessage) (settings, error) {
	s := defaultSettings()
	if len(bytes.TrimSpace(raw)) != 0 {
		var section struct {
			Task json.RawMessage `json:"task"`
		}
		if err := json.Unmarshal(raw, &section); err != nil {
			return settings{}, fmt.Errorf("tasks: invalid settings: %w", err)
		}
		if len(section.Task) != 0 {
			if err := json.Unmarshal(section.Task, &s); err != nil {
				return settings{}, fmt.Errorf("tasks: invalid settings: %w", err)
			}
		}
	}
	switch {
	case s.MaxConcurrent < 1 || s.MaxConcurrent > 20:
		return settings{}, errors.New("tasks: max_concurrent must be between 1 and 20")
	case s.MaxOutputBytes < 1024 || s.MaxOutputBytes > 1048576:
		return settings{}, errors.New("tasks: max_output_bytes must be between 1024 and 1048576")
	}
	return s, nil
}

// launchOutcome renders one launch failure through the uniform tool-error
// path: the harness's rejection details are model-facing and arrive unwrapped.
func launchOutcome(callID string, err error) harness.ToolOutcome {
	return launchOutcomeWithKey(callID, err, "child_session_id")
}

func launchOutcomeWithKey(callID string, err error, key string) harness.ToolOutcome {
	content := err.Error()
	var callErr *extension.CallError
	if errors.As(err, &callErr) && len(callErr.Partial) != 0 {
		var partial map[string]json.RawMessage
		var id string
		if json.Unmarshal(callErr.Partial, &partial) == nil && json.Unmarshal(partial[key], &id) == nil && id != "" {
			content += "\nSession ID: " + id
		}
	}
	return harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: content}}
}

// immediateError is the normalization-class immediate outcome: status error
// carrying the validation diagnostic, per the tool-boundary validation
// contract.
func immediateError(callID string, cause error) extension.ToolPlan {
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{
		Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: cause.Error()},
	}}
}

// normalizeTaskArgs is the task tool's member validator: private
// `_melta_` fields are stripped, a present prompt or subagent_type must
// be a string, and every other member is preserved; Prepare owns the
// semantics.
func normalizeTaskArgs(args map[string]any) (map[string]any, error) {
	clean := make(map[string]any, len(args))
	for key, value := range args {
		if strings.HasPrefix(key, "_melta_") {
			continue
		}
		if key == "prompt" || key == "subagent_type" {
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("task: %s must be a string", key)
			}
		}
		clean[key] = value
	}
	return clean, nil
}

// normalizeCallArguments is the one shared Normalize body.
func normalizeCallArguments(call model.ToolCall, normalize func(map[string]any) (map[string]any, error)) (json.RawMessage, error) {
	args, err := toolargs.Decode(call.Arguments)
	if err != nil {
		return nil, err
	}
	normalized, err := normalize(args)
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}
