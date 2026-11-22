package model

import "encoding/json"

// ReasoningText returns the provider reasoning text carried by one message's
// extras and content parts. It reads the bundled catalog's shapes in
// precedence order: the reasoning_content string, else the reasoning string,
// else the reasoning_details array's objects' text, else every opaque thinking
// part's thinking string or its array of text objects. Each shape decides on
// its own, so a chosen shape's empty result stays empty instead of falling
// through to a lower-precedence shape.
func ReasoningText(extra Extra, parts []ContentPart) string {
	if text, ok := extraJSONString(extra, "reasoning_content"); ok {
		return text
	}
	if text, ok := extraJSONString(extra, "reasoning"); ok {
		return text
	}
	if raw, ok := extra["reasoning_details"]; ok && classifyJSON(raw) == extraKindArray {
		return arrayObjectText(raw)
	}
	var out string
	for _, part := range parts {
		if part.Kind != PartOpaque || part.OpaqueWireType != "thinking" {
			continue
		}
		if text, ok := extraJSONString(part.Extra, "thinking"); ok {
			out += text
		} else {
			out += arrayObjectText(part.Extra["thinking"])
		}
	}
	return out
}

// extraJSONString returns one member's JSON string value; any other shape is
// absent.
func extraJSONString(members map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := members[key]
	if !ok || classifyJSON(raw) != extraKindString {
		return "", false
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return "", false
	}
	return text, true
}

// arrayObjectText concatenates, in order, each object element's text JSON
// string member of one JSON array. A value that is not an array yields
// nothing, and so does an element that is not an object or carries no text
// string member.
func arrayObjectText(raw json.RawMessage) string {
	var elements []json.RawMessage
	if json.Unmarshal(raw, &elements) != nil {
		return ""
	}
	var out string
	for _, element := range elements {
		var members map[string]json.RawMessage
		if json.Unmarshal(element, &members) != nil {
			continue
		}
		if text, ok := extraJSONString(members, "text"); ok {
			out += text
		}
	}
	return out
}
