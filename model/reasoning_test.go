package model

import (
	"encoding/json"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
)

// TestReasoningText pins the one reasoning-text rule: each supported provider
// shape decides on its own, in precedence order, and a chosen shape's result
// is final even when it is empty.
func TestReasoningText(t *testing.T) {
	thinking := func(values ...string) []ContentPart {
		parts := make([]ContentPart, 0, len(values))
		for _, value := range values {
			parts = append(parts, ContentPart{Kind: PartOpaque, OpaqueWireType: "thinking", Extra: Extra{"thinking": json.RawMessage(value)}})
		}
		return parts
	}
	rows := []struct {
		name  string
		extra Extra
		parts []ContentPart
		want  string
	}{
		{
			name:  "reasoning content wins over every lower shape",
			extra: Extra{"reasoning_content": json.RawMessage(`"content"`), "reasoning": json.RawMessage(`"reasoning"`), "reasoning_details": json.RawMessage(`[{"text":"details"}]`)},
			parts: thinking(`"parts"`),
			want:  "content",
		},
		{
			name:  "reasoning wins over details and thinking parts",
			extra: Extra{"reasoning": json.RawMessage(`"reasoning"`), "reasoning_details": json.RawMessage(`[{"text":"details"}]`)},
			parts: thinking(`"parts"`),
			want:  "reasoning",
		},
		{
			name:  "details concatenate their text in order",
			extra: Extra{"reasoning_details": json.RawMessage(`[{"type":"text","text":"a"},{"text":"c"},{"text":7},1]`)},
			want:  "ac",
		},
		{
			name:  "empty details array is the chosen shape",
			extra: Extra{"reasoning_details": json.RawMessage(`[]`)},
			parts: thinking(`"ignored"`),
			want:  "",
		},
		{
			name:  "encrypted-only details are the chosen shape",
			extra: Extra{"reasoning_details": json.RawMessage(`[{"type":"reasoning.encrypted","data":"opaque"}]`)},
			parts: thinking(`"ignored"`),
			want:  "",
		},
		{
			name:  "thinking parts concatenate their strings and array texts",
			parts: append(thinking(`"a"`, `[{"text":"b"},{"text":7},1,{"text":"c"}]`), ContentPart{Kind: PartOpaque, OpaqueWireType: "redacted_thinking", Extra: Extra{"thinking": json.RawMessage(`"ignored"`)}}, ContentPart{Kind: PartText, Text: "ignored"}),
			want:  "abc",
		},
		{
			name:  "non-string shapes fall through",
			extra: Extra{"reasoning_content": json.RawMessage(`7`), "reasoning": json.RawMessage(`true`), "reasoning_details": json.RawMessage(`{"text":"not an array"}`)},
			parts: thinking(`"parts"`),
			want:  "parts",
		},
		{
			name:  "non-string shapes with no lower shape yield nothing",
			extra: Extra{"reasoning_content": json.RawMessage(`7`), "reasoning": json.RawMessage(`false`), "reasoning_details": json.RawMessage(`{"text":"not an array"}`)},
			want:  "",
		},
		{
			name:  "unknown keys and other opaque wire types yield nothing",
			extra: Extra{"other": json.RawMessage(`"ignored"`)},
			parts: []ContentPart{{Kind: PartOpaque, OpaqueWireType: "image_url", Extra: Extra{"thinking": json.RawMessage(`"ignored"`)}}},
			want:  "",
		},
		{
			name:  "an empty chosen reasoning content is final",
			extra: Extra{"reasoning_content": json.RawMessage(`""`), "reasoning": json.RawMessage(`"lower"`)},
			want:  "",
		},
		{
			name:  "an empty chosen reasoning is final",
			extra: Extra{"reasoning": json.RawMessage(`""`), "reasoning_details": json.RawMessage(`[{"text":"lower"}]`)},
			want:  "",
		},
		{
			name: "nothing present yields nothing",
			want: "",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			testassert.Equal(t, ReasoningText(row.extra, row.parts), row.want)
		})
	}
}
