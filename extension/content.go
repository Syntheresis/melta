package extension

import (
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// ProtocolContent maps one validated content list to its generated union
// parts. Each part is written from its own members, every extra value as its
// own JSON value, so extras keep their bytes; the generated constructors would
// parse and re-encode them.
func ProtocolContent(parts []model.ContentPart) ([]protocol.ContentPart, error) {
	out := make([]protocol.ContentPart, 0, len(parts))
	for _, part := range parts {
		var members map[string]json.RawMessage
		switch part.Kind {
		case model.PartText:
			members = map[string]json.RawMessage{"kind": json.RawMessage(`"text"`), "text": encodedString(part.Text)}
		case model.PartImageURL:
			members = map[string]json.RawMessage{"kind": json.RawMessage(`"image_url"`), "url": encodedString(part.URL)}
		case model.PartOpaque:
			members = map[string]json.RawMessage{"kind": json.RawMessage(`"opaque"`), "opaque_wire_type": encodedString(part.OpaqueWireType)}
		default:
			return nil, fmt.Errorf("content part kind %q is not one of text, image_url or opaque", part.Kind)
		}
		if len(part.Extra) != 0 {
			members["extra"] = writeObject(part.Extra)
		}
		var wire protocol.ContentPart
		_ = wire.UnmarshalJSON(writeObject(members)) // a raw union stores its bytes; it cannot fail
		out = append(out, wire)
	}
	return out, nil
}

// encodedString encodes one string as JSON; a string always encodes.
func encodedString(value string) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

// ProtocolExtra maps one validated raw extra set to the generated raw-valued
// object, the same map shape converted directly. Absent extras stay omitted.
func ProtocolExtra(extra model.Extra) *protocol.JSONObject {
	if len(extra) == 0 {
		return nil
	}
	out := protocol.JSONObject(extra)
	return &out
}

// ModelContent maps one generated content union list to canonical model parts
// through the generated accessors. It performs union decoding only: the
// Harness accepting boundary validates and clones every part through
// model.NewContentPart before any admission or effect. Unknown discriminator
// and union decode failures wrap the shared harness.ErrInvalid sentinel.
func ModelContent(parts []protocol.ContentPart) ([]model.ContentPart, error) {
	out := make([]model.ContentPart, 0, len(parts))
	for i, wire := range parts {
		part, err := modelContentPart(wire)
		if err != nil {
			return nil, fmt.Errorf("content[%d]: %w", i, err)
		}
		out = append(out, part)
	}
	return out, nil
}

// modelContentPart maps one generated union member to its canonical kind with
// exactly that kind's field; extras ride the identical raw-valued map
// conversion, so numbers and opaque extras are preserved. It adds no
// model-semantic validation: the Harness boundary owns that rule.
func modelContentPart(wire protocol.ContentPart) (model.ContentPart, error) {
	invalid := func(format string, args ...any) (model.ContentPart, error) {
		return model.ContentPart{}, fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), harness.ErrInvalid)
	}
	kind, err := wire.Discriminator()
	if err != nil {
		return invalid("decode content part: %v", err)
	}
	var part model.ContentPart
	switch kind {
	case "text":
		value, err := wire.AsTextPart()
		if err != nil {
			return invalid("decode text part: %v", err)
		}
		part = model.ContentPart{Kind: model.PartText, Text: value.Text, Extra: modelExtra(value.Extra)}
	case "image_url":
		value, err := wire.AsImageURLPart()
		if err != nil {
			return invalid("decode image_url part: %v", err)
		}
		part = model.ContentPart{Kind: model.PartImageURL, URL: value.Url, Extra: modelExtra(value.Extra)}
	case "opaque":
		value, err := wire.AsOpaquePart()
		if err != nil {
			return invalid("decode opaque part: %v", err)
		}
		part = model.ContentPart{Kind: model.PartOpaque, OpaqueWireType: value.OpaqueWireType, Extra: modelExtra(value.Extra)}
	default:
		return invalid("content part kind %q is not one of text, image_url or opaque", kind)
	}
	return part, nil
}

// modelExtra converts one generated raw-valued object to the canonical extra
// set by the identical direct map conversion; the Harness accepting boundary
// clones it through model.NewContentPart, so no reference to the wire value
// escapes.
func modelExtra(extra *protocol.JSONObject) model.Extra {
	if extra == nil {
		return nil
	}
	return model.Extra(*extra)
}
