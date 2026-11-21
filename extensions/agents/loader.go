package agents

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Syntheresis/melta/model"
)

type Config struct {
	defs     map[string]Definition
	warnings []Warning
}

// parse loads the agent definitions: built-ins with their locked fields,
// configured overlays and custom definitions, with the retained
// invalid-custom warning/drop behavior.
func parse(data []byte) (*Config, error) {
	var user map[string]json.RawMessage
	if err := json.Unmarshal(data, &user); err != nil {
		return nil, err
	}
	if user == nil {
		user = map[string]json.RawMessage{}
	}

	cfg := &Config{defs: copyBuiltins()}
	names := make([]string, 0, len(user))
	for name := range user {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		raw := user[name]
		if _, ok := builtins[name]; ok {
			def, err := decodeBuiltinOverlay(raw)
			if err != nil {
				cfg.warnings = append(cfg.warnings, Warning{
					Kind:    "invalid_builtin_overlay",
					Name:    name,
					Message: err.Error(),
				})
				continue
			}
			cfg.applyBuiltinOverlay(name, def)
			continue
		}
		def, err := decodeDefinition(raw)
		if err != nil {
			cfg.warnings = append(cfg.warnings, Warning{
				Kind:    "invalid_agent_type",
				Name:    name,
				Message: err.Error(),
			})
			continue
		}
		if err := validateCustom(name, def); err != nil {
			cfg.warnings = append(cfg.warnings, Warning{
				Kind:    "invalid_agent_type",
				Name:    name,
				Message: err.Error(),
			})
			continue
		}
		cfg.defs[name] = copyDefinition(def)
	}
	return cfg, nil
}

// decodeDefinition decodes one custom definition entry: the entry must be an
// object and every member keeps the ordinary typed decoder's behavior.
func decodeDefinition(raw json.RawMessage) (Definition, error) {
	if err := ensureJSONObject(raw); err != nil {
		return Definition{}, err
	}
	var def Definition
	if err := json.Unmarshal(raw, &def); err != nil {
		return Definition{}, err
	}
	return def, nil
}

func decodeBuiltinOverlay(raw json.RawMessage) (Definition, error) {
	if err := ensureJSONObject(raw); err != nil {
		return Definition{}, err
	}
	var overlay struct {
		Model string `json:"model,omitempty"`
	}
	if err := json.Unmarshal(raw, &overlay); err != nil {
		return Definition{}, err
	}
	return Definition{Model: overlay.Model}, nil
}

func ensureJSONObject(raw json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return fmt.Errorf("agent entry must be an object: %w", err)
	}
	if object == nil {
		return fmt.Errorf("agent entry must be an object")
	}
	return nil
}

func (c *Config) Warnings() []Warning {
	return append([]Warning(nil), c.warnings...)
}

func (c *Config) Resolve(name string) (Resolved, error) {
	if c == nil {
		return Resolved{}, fmt.Errorf("unknown agent type %q", name)
	}
	return c.resolve(name, map[string]bool{})
}

func (c *Config) All() []Resolved {
	if c == nil {
		return nil
	}
	var out []Resolved
	seen := map[string]bool{}
	for _, name := range builtinOrder {
		resolved, err := c.Resolve(name)
		if err == nil {
			out = append(out, resolved)
			seen[name] = true
		}
	}
	var custom []string
	for name := range c.defs {
		if !seen[name] {
			custom = append(custom, name)
		}
	}
	sort.Strings(custom)
	for _, name := range custom {
		resolved, err := c.Resolve(name)
		if err == nil {
			out = append(out, resolved)
		}
	}
	return out
}

func (c *Config) applyBuiltinOverlay(name string, def Definition) {
	if def.Model == "" {
		return
	}
	if _, err := model.Parse(def.Model); err != nil {
		c.warnings = append(c.warnings, Warning{
			Kind:    "invalid_builtin_model",
			Name:    name,
			Message: fmt.Sprintf("model must be a provider-prefixed string: %v", err),
		})
		return
	}
	current := c.defs[name]
	current.Model = def.Model
	c.defs[name] = current
}

func (c *Config) resolve(name string, seen map[string]bool) (Resolved, error) {
	def, ok := c.defs[name]
	if !ok {
		return Resolved{}, fmt.Errorf("unknown agent type %q", name)
	}
	if seen[name] {
		return Resolved{}, fmt.Errorf("agent type inheritance cycle at %q", name)
	}
	seen[name] = true

	var out Resolved
	parent := parentName(name)
	if parent != "" {
		inherited, err := c.resolve(parent, seen)
		if err != nil {
			return Resolved{}, err
		}
		out = inherited
	}
	out.Name = name
	out.Builtin = isBuiltin(name)
	applyDefinition(&out, def)
	return out, nil
}

func applyDefinition(out *Resolved, def Definition) {
	if def.Model != "" {
		out.Model = def.Model
	}
	if def.SystemPrompt != "" {
		out.SystemPrompt = def.SystemPrompt
	}
	if def.Prompt != nil {
		out.Prompt = *def.Prompt
	}
	if def.Tools != nil {
		out.Tools = append([]string{}, (*def.Tools)...)
	}
	if def.Capabilities != nil {
		if len(*def.Capabilities) == 0 {
			out.Capabilities = nil // an empty list clears the inherited selection
		} else {
			out.Capabilities = append([]string(nil), (*def.Capabilities)...)
		}
	}
	if def.Readonly != nil {
		out.Readonly = *def.Readonly
	}
	if def.WriteDir != nil {
		out.WriteDir = *def.WriteDir
	}
	if def.Description != nil {
		out.Description = *def.Description
	}
	if def.Subagent != nil {
		out.Subagent = *def.Subagent
	}
}

func validateCustom(name string, def Definition) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("name is empty")
	}
	if def.Model != "" {
		if _, err := model.Parse(def.Model); err != nil {
			return fmt.Errorf("model must be a provider-prefixed string: %w", err)
		}
	}
	if def.SystemPrompt != "" {
		switch def.SystemPrompt {
		case SystemPromptNone, SystemPromptSimple, SystemPromptFull:
		default:
			return fmt.Errorf("system_prompt must be one of %q, %q, or %q", SystemPromptNone, SystemPromptSimple, SystemPromptFull)
		}
	}
	if def.Tools != nil {
		for _, name := range *def.Tools {
			if name == "" {
				return fmt.Errorf("tool name is empty")
			}
		}
	}
	if def.Capabilities != nil {
		seen := make(map[string]bool, len(*def.Capabilities))
		for _, id := range *def.Capabilities {
			if seen[id] {
				return fmt.Errorf("duplicate capability %q", id)
			}
			seen[id] = true
		}
	}
	return nil
}

func parentName(name string) string {
	switch name {
	case "primary":
		return ""
	case "secondary":
		return "primary"
	default:
		return "secondary"
	}
}

func isBuiltin(name string) bool {
	_, ok := builtins[name]
	return ok
}

func copyBuiltins() map[string]Definition {
	out := make(map[string]Definition, len(builtins))
	for name, def := range builtins {
		out[name] = copyDefinition(def)
	}
	return out
}

func copyDefinition(def Definition) Definition {
	if def.Prompt != nil {
		def.Prompt = stringPtr(*def.Prompt)
	}
	if def.Tools != nil {
		def.Tools = toolsPtr(*def.Tools)
	}
	if def.Capabilities != nil {
		def.Capabilities = toolsPtr(*def.Capabilities)
	}
	if def.Readonly != nil {
		def.Readonly = boolPtr(*def.Readonly)
	}
	if def.WriteDir != nil {
		def.WriteDir = stringPtr(*def.WriteDir)
	}
	if def.Description != nil {
		def.Description = stringPtr(*def.Description)
	}
	if def.Subagent != nil {
		def.Subagent = boolPtr(*def.Subagent)
	}
	return def
}
