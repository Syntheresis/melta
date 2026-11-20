package agents

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

const setModelInputSchema = `{"type":"object","required":["definition_id","model"],"properties":{"definition_id":{"type":"string","minLength":1},"model":{"type":"string"}},"additionalProperties":false}`

func transformModel(rawSection, arguments json.RawMessage, parseDefinitions func([]byte) (*Config, error)) (extension.Result, error) {
	var request struct {
		DefinitionID string `json:"definition_id"`
		Model        string `json:"model"`
	}
	if err := json.Unmarshal(arguments, &request); err != nil {
		return extension.Result{}, err
	}
	if request.Model != "" {
		if _, err := model.Parse(request.Model); err != nil {
			return extension.Result{}, fmt.Errorf("model ref %q: %v: %w", request.Model, err, harness.ErrInvalid)
		}
	}
	section, err := decodeObject(rawSection, "agents section")
	if err != nil {
		return extension.Result{}, err
	}
	definitions := map[string]json.RawMessage{}
	if len(section["definitions"]) != 0 {
		definitions, err = decodeObject(section["definitions"], "agents definitions")
		if err != nil {
			return extension.Result{}, err
		}
	}
	data, err := json.Marshal(definitions)
	if err != nil {
		return extension.Result{}, err
	}
	parsed, err := parseDefinitions(data)
	if err != nil {
		return extension.Result{}, fmt.Errorf("decode agent definitions: %w", err)
	}
	if _, err := parsed.Resolve(request.DefinitionID); err != nil {
		return extension.Result{}, fmt.Errorf("%v: %w", err, harness.ErrInvalid)
	}
	var fields map[string]json.RawMessage
	if raw, ok := definitions[request.DefinitionID]; ok {
		if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return extension.Result{}, fmt.Errorf("agents.%s must be an object: %w", request.DefinitionID, harness.ErrInvalid)
		}
	} else {
		fields = map[string]json.RawMessage{}
	}
	if request.Model == "" {
		delete(fields, "model")
	} else {
		if fields["model"], err = json.Marshal(request.Model); err != nil {
			return extension.Result{}, err
		}
	}
	if definitions[request.DefinitionID], err = json.Marshal(fields); err != nil {
		return extension.Result{}, err
	}
	if section["definitions"], err = json.Marshal(definitions); err != nil {
		return extension.Result{}, err
	}
	transformed, err := json.Marshal(section)
	if err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: transformed}, nil
}

// decodeObject decodes one section document into a raw-member object; null is
// the empty object and any other shape is invalid input.
func decodeObject(raw json.RawMessage, label string) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, fmt.Errorf("%s must be an object: %w", label, harness.ErrInvalid)
	}
	if object == nil {
		object = map[string]json.RawMessage{}
	}
	return object, nil
}
