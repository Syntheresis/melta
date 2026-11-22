package model

import "encoding/json"

// ReasoningText returns provider reasoning text in the bundled catalog's precedence order: the reasoning_content string, else the reasoning string, else the reasoning_details array's object texts, else every opaque thinking part's thinking string or array of texts. A chosen shape's empty result is final.
func ReasoningText(extra Extra, parts []ContentPart) string {
	for _, key := range [...]string{"reasoning_content", "reasoning"} {
		if text, ok := decodeJSONValue(extra[key]).(string); ok {
			return text
		}
	}
	if elements, ok := decodeJSONValue(extra["reasoning_details"]).([]any); ok {
		return texts(elements)
	}
	var out string
	for _, part := range parts {
		if part.Kind == PartOpaque && part.OpaqueWireType == "thinking" {
			switch thinking := decodeJSONValue(part.Extra["thinking"]).(type) {
			case string:
				out += thinking
			case []any:
				out += texts(thinking)
			}
		}
	}
	return out
}

// texts concatenates the text strings of object elements in arrival order.
func texts(elements []any) (out string) {
	for _, element := range elements {
		members, _ := element.(map[string]any)
		if text, ok := members["text"].(string); ok {
			out += text
		}
	}
	return
}

// decodeJSONValue decodes one raw JSON member into its generic Go value; malformed or absent input yields nil.
func decodeJSONValue(raw json.RawMessage) any {
	var v any
	_ = json.Unmarshal(raw, &v)
	return v
}
