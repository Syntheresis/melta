package harness

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/Syntheresis/melta/model"
)

// SessionInputs holds optional overrides of the host's execution defaults.
type SessionInputs struct {
	Model        *model.ModelRef
	Instructions *string
	Prompt       *string
	Tools        []string
	Strategy     *StrategySelection
}

// Owned detaches all mutable values, preserving absent and empty lists.
func (v SessionInputs) Owned() SessionInputs {
	if v.Model != nil {
		value := *v.Model
		v.Model = &value
	}
	if v.Instructions != nil {
		value := *v.Instructions
		v.Instructions = &value
	}
	if v.Prompt != nil {
		value := *v.Prompt
		v.Prompt = &value
	}
	v.Tools = slices.Clone(v.Tools)
	if v.Strategy != nil {
		value := *v.Strategy
		value.Settings = slices.Clone(value.Settings)
		v.Strategy = &value
	}
	return v
}

func (v SessionInputs) normalized() (SessionInputs, error) {
	v = v.Owned()
	if v.Instructions != nil && v.Prompt != nil {
		return v, fmt.Errorf("instructions and prompt are mutually exclusive")
	}
	if v.Strategy != nil {
		if v.Strategy.ID == "" {
			return v, fmt.Errorf("strategy name must not be empty")
		}
		if len(v.Strategy.Settings) != 0 && !json.Valid(v.Strategy.Settings) {
			return v, fmt.Errorf("strategy settings must be JSON")
		}
	}
	seen := make(map[string]bool, len(v.Tools))
	if v.Tools != nil {
		tools := make([]string, 0, len(v.Tools))
		for _, name := range v.Tools {
			if name == "" {
				return v, fmt.Errorf("tool name must not be empty")
			}
			if !seen[name] {
				seen[name] = true
				tools = append(tools, name)
			}
		}
		v.Tools = tools
	}
	return v, nil
}

func (v SessionInputs) MarshalJSON() ([]byte, error) {
	v, err := v.normalized()
	if err != nil {
		return nil, err
	}
	members := make(map[string]json.RawMessage)
	if v.Model != nil {
		members["model"], err = encodeModelRef(*v.Model)
		if err != nil {
			return nil, err
		}
	}
	if v.Instructions != nil {
		members["instructions"], _ = json.Marshal(*v.Instructions)
	}
	if v.Prompt != nil {
		members["prompt"], _ = json.Marshal(*v.Prompt)
	}
	if v.Tools != nil {
		members["tools"], _ = json.Marshal(v.Tools)
	}
	if v.Strategy != nil {
		members["strategy"], err = json.Marshal(v.Strategy)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(members)
}

func (v *SessionInputs) UnmarshalJSON(data []byte) error {
	members, err := decodePayloadObject(data)
	if err != nil {
		return err
	}
	if err := rejectUnknownMembers(members, "model", "instructions", "prompt", "tools", "strategy"); err != nil {
		return err
	}
	var out SessionInputs
	for _, key := range []string{"model", "instructions", "prompt", "tools", "strategy"} {
		raw, err := member(members, key, false)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		switch key {
		case "model":
			obj, err := decodePayloadObject(raw)
			if err != nil {
				return err
			}
			ref, err := decodeModelRef(obj)
			if err != nil {
				return err
			}
			out.Model = &ref
		case "instructions":
			out.Instructions = new(string)
			err = json.Unmarshal(raw, out.Instructions)
		case "prompt":
			out.Prompt = new(string)
			err = json.Unmarshal(raw, out.Prompt)
		case "tools":
			err = json.Unmarshal(raw, &out.Tools)
		case "strategy":
			out.Strategy = new(StrategySelection)
			err = json.Unmarshal(raw, out.Strategy)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	out, err = out.normalized()
	if err == nil {
		*v = out
	}
	return err
}

// SessionStateValue is one extension's uninterpreted JSON value.
type SessionStateValue []byte

func (v SessionStateValue) MarshalJSON() ([]byte, error) {
	if len(v) > 1<<20 || !json.Valid(v) {
		return nil, fmt.Errorf("session state must be JSON no larger than 1 MiB")
	}
	return frame(v), nil
}

func (v *SessionStateValue) UnmarshalJSON(data []byte) error {
	b, err := framedMember(map[string]json.RawMessage{"value": data}, "value", true)
	if err != nil {
		return err
	}
	if len(b) > 1<<20 || !json.Valid(b) {
		return fmt.Errorf("session state must be JSON no larger than 1 MiB")
	}
	*v = SessionStateValue(b)
	return nil
}
