package extension

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// WireBytes is an opaque byte value encoded as canonical padded base64.
type WireBytes []byte

func (b WireBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(b))
}

func (b *WireBytes) UnmarshalJSON(data []byte) error {
	var encoded *string
	if err := json.Unmarshal(data, &encoded); err != nil || encoded == nil {
		return errors.New("opaque value must be a base64 string")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(*encoded)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != *encoded {
		return errors.New("opaque value must be canonical base64")
	}
	*b = decoded
	return nil
}

type WireExtra struct {
	Key   WireBytes `json:"key"`
	Value WireBytes `json:"value"`
}

func EncodeExtra(extra model.Extra) []WireExtra {
	keys := make([]string, 0, len(extra))
	for key := range extra {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out []WireExtra
	for _, key := range keys {
		out = append(out, WireExtra{Key: WireBytes(key), Value: WireBytes(extra[key])})
	}
	return out
}

func DecodeExtra(extra []WireExtra) model.Extra {
	if len(extra) == 0 {
		return nil
	}
	out := make(model.Extra, len(extra))
	for _, entry := range extra {
		out[string(entry.Key)] = append(json.RawMessage(nil), entry.Value...)
	}
	return out
}

type WireContentPart struct {
	Kind           string      `json:"kind"`
	Text           string      `json:"text,omitempty"`
	URL            string      `json:"url,omitempty"`
	OpaqueWireType string      `json:"opaque_wire_type,omitempty"`
	Extra          []WireExtra `json:"extra,omitempty"`
}

func EncodeContentParts(parts []model.ContentPart) []WireContentPart {
	out := make([]WireContentPart, len(parts))
	for i, part := range parts {
		out[i] = WireContentPart{Kind: string(part.Kind), Text: part.Text, URL: part.URL, OpaqueWireType: part.OpaqueWireType, Extra: EncodeExtra(part.Extra)}
	}
	return out
}

func DecodeContentParts(parts []WireContentPart) []model.ContentPart {
	out := make([]model.ContentPart, len(parts))
	for i, part := range parts {
		out[i] = model.ContentPart{Kind: model.PartKind(part.Kind), Text: part.Text, URL: part.URL, OpaqueWireType: part.OpaqueWireType, Extra: DecodeExtra(part.Extra)}
	}
	return out
}

type sessionInputsJSON struct {
	Model *struct {
		Provider string `json:"provider"`
		Model    string `json:"model"`
	} `json:"model,omitempty"`
	Instructions *string   `json:"instructions,omitempty"`
	Prompt       *string   `json:"prompt,omitempty"`
	Tools        *[]string `json:"tools,omitempty"`
	Strategy     *struct {
		ID       string     `json:"id"`
		Settings *WireBytes `json:"settings,omitempty"`
	} `json:"strategy,omitempty"`
}

type sessionStartJSON struct {
	Inputs       WireBytes `json:"inputs"`
	InitialState WireBytes `json:"initial_state"`
	Input        WireBytes `json:"input"`
	Report       bool      `json:"report"`
	OutputLimit  int       `json:"output_limit"`
}

func EncodeSessionStart(start SessionStart) (json.RawMessage, error) {
	inputs := sessionInputsJSON{Instructions: start.Inputs.Instructions, Prompt: start.Inputs.Prompt}
	if start.Inputs.Tools != nil {
		inputs.Tools = &start.Inputs.Tools
	}
	if start.Inputs.Model != nil {
		inputs.Model = &struct {
			Provider string `json:"provider"`
			Model    string `json:"model"`
		}{start.Inputs.Model.Provider, start.Inputs.Model.Model}
	}
	if start.Inputs.Strategy != nil {
		inputs.Strategy = &struct {
			ID       string     `json:"id"`
			Settings *WireBytes `json:"settings,omitempty"`
		}{ID: start.Inputs.Strategy.ID}
		if start.Inputs.Strategy.Settings != nil {
			settings := WireBytes(start.Inputs.Strategy.Settings)
			inputs.Strategy.Settings = &settings
		}
	}
	rawInputs, err := json.Marshal(inputs)
	if err != nil {
		return nil, err
	}
	rawParts, err := json.Marshal(EncodeContentParts(start.Input))
	if err != nil {
		return nil, err
	}
	return json.Marshal(sessionStartJSON{Inputs: rawInputs, InitialState: WireBytes(start.InitialState), Input: rawParts, Report: start.Report, OutputLimit: start.OutputLimit})
}

// decodeSessionDocument decodes exactly one non-null JSON document with the
// native strict rules, so an undeclared member or concatenated document is
// refused wherever a session document is read.
func decodeSessionDocument(raw []byte, dst any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("document must not be null")
	}
	return protocol.DecodeDocument(raw, dst)
}

func DecodeSessionStart(raw json.RawMessage) (SessionStart, error) {
	var wire sessionStartJSON
	if err := decodeSessionDocument(raw, &wire); err != nil {
		return SessionStart{}, err
	}
	var inputs sessionInputsJSON
	if err := decodeSessionDocument(wire.Inputs, &inputs); err != nil {
		return SessionStart{}, err
	}
	var parts []WireContentPart
	if err := decodeSessionDocument(wire.Input, &parts); err != nil {
		return SessionStart{}, err
	}
	start := SessionStart{InitialState: append(harness.SessionStateValue(nil), wire.InitialState...), Report: wire.Report, OutputLimit: wire.OutputLimit}
	start.Inputs.Instructions, start.Inputs.Prompt = inputs.Instructions, inputs.Prompt
	if inputs.Tools != nil {
		start.Inputs.Tools = *inputs.Tools
	}
	if inputs.Model != nil {
		start.Inputs.Model = &model.ModelRef{Provider: inputs.Model.Provider, Model: inputs.Model.Model}
	}
	if inputs.Strategy != nil {
		start.Inputs.Strategy = &harness.StrategySelection{ID: inputs.Strategy.ID}
		if inputs.Strategy.Settings != nil {
			start.Inputs.Strategy.Settings = append(json.RawMessage{}, (*inputs.Strategy.Settings)...)
		}
	}
	if len(parts) != 0 {
		start.Input = DecodeContentParts(parts)
	}
	return start, nil
}
