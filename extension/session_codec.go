package extension

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

// sessionStartDocument is the start document of the melta.session start host
// call: the Session-inputs object create_session takes, an optional raw initial
// state as state.set carries, and the content parts submit takes.
type sessionStartDocument struct {
	Inputs       harness.SessionInputs  `json:"inputs"`
	InitialState json.RawMessage        `json:"initial_state,omitempty"`
	Input        []protocol.ContentPart `json:"input"`
	Report       bool                   `json:"report"`
	OutputLimit  int                    `json:"output_limit"`
}

// EncodeSessionStart encodes one typed start. Its parts are caller input, so
// each is checked by the model's content rule before it is projected; the
// initial state is written as its own JSON value, so it keeps its bytes. Any
// failure is invalid input, as the host would class the same start.
func EncodeSessionStart(start SessionStart) (json.RawMessage, error) {
	raw, err := encodeSessionStart(start)
	if err != nil {
		return nil, fmt.Errorf("session start: %v: %w", err, harness.ErrInvalid)
	}
	return raw, nil
}

func encodeSessionStart(start SessionStart) (json.RawMessage, error) {
	for _, part := range start.Input {
		if err := model.ValidateContentPart(part); err != nil {
			return nil, err
		}
	}
	parts, err := ProtocolContent(start.Input)
	if err != nil {
		return nil, err
	}
	input := make([]json.RawMessage, len(parts))
	for i, part := range parts {
		input[i], _ = part.MarshalJSON() // a raw union returns its bytes; it cannot fail
	}
	inputs, err := json.Marshal(start.Inputs)
	if err != nil {
		return nil, err
	}
	report, _ := json.Marshal(start.Report)
	limit, _ := json.Marshal(start.OutputLimit)
	members := map[string]json.RawMessage{"inputs": inputs, "input": writeArray(input), "report": report, "output_limit": limit}
	if len(start.InitialState) != 0 {
		if !json.Valid(start.InitialState) {
			return nil, errors.New("initial state is not one JSON value")
		}
		members["initial_state"] = json.RawMessage(start.InitialState)
	}
	return writeObject(members), nil
}

// DecodeSessionStart decodes one start document with the generated request
// value rules, the Session-inputs decoder create_session uses and the content
// parts submit uses, so every member name matches exactly.
func DecodeSessionStart(raw json.RawMessage) (SessionStart, error) {
	var document sessionStartDocument
	if err := protocol.DecodeValue(raw, &document); err != nil {
		return SessionStart{}, err
	}
	input, err := ModelContent(document.Input)
	if err != nil {
		return SessionStart{}, err
	}
	start := SessionStart{Inputs: document.Inputs, InitialState: harness.SessionStateValue(document.InitialState), Report: document.Report, OutputLimit: document.OutputLimit}
	if len(input) != 0 {
		start.Input = input
	}
	return start, nil
}
