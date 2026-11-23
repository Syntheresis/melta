package model

import "encoding/json"

// reasoningStringExtraKeys are the message extra keys that carry reasoning as a string, in precedence order.
var reasoningStringExtraKeys = [...]string{"reasoning_content", "reasoning"}

// reasoningDetailsExtraKey is the message extra key that carries reasoning as an array of text objects.
const reasoningDetailsExtraKey = "reasoning_details"

// thinkingOpaqueWireType is the opaque content wire type that carries reasoning.
const thinkingOpaqueWireType = "thinking"

// ReasoningText returns provider reasoning text in this function's precedence order: the reasoning_content string, else the reasoning string, else the reasoning_details array's object texts, else every opaque thinking part's thinking string or array of texts. A chosen shape's empty result is final.
func ReasoningText(extra Extra, parts []ContentPart) string {
	for _, key := range reasoningStringExtraKeys {
		if text, ok := decodeJSONValue(extra[key]).(string); ok {
			return text
		}
	}
	if elements, ok := decodeJSONValue(extra[reasoningDetailsExtraKey]).([]any); ok {
		return texts(elements)
	}
	var out string
	for _, part := range parts {
		if part.Kind == PartOpaque && part.OpaqueWireType == thinkingOpaqueWireType {
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

// RetainInterruptedContent returns msg reduced to what an interrupted response keeps: role and source identity, text content parts, refusal, reasoning extras, and opaque parts with wire type thinking; every other content part and extra is dropped.
func RetainInterruptedContent(msg Message) Message {
	out := Message{Role: msg.Role, Source: msg.Source, Refusal: msg.Refusal}
	for _, part := range msg.Content {
		if part.Kind == PartText || part.Kind == PartOpaque && part.OpaqueWireType == thinkingOpaqueWireType {
			out.Content = append(out.Content, part)
		}
	}
	for _, key := range append(reasoningStringExtraKeys[:], reasoningDetailsExtraKey) {
		if raw, ok := msg.Extra[key]; ok {
			if out.Extra == nil {
				out.Extra = Extra{}
			}
			out.Extra[key] = raw
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
