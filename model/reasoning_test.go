package model

import (
	"encoding/json"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
)

// TestReasoningText pins the reasoning-text precedence: each supported shape decides on its own in order, a chosen shape's empty result is final, and non-string or other opaque shapes contribute nothing.
func TestReasoningText(t *testing.T) {
	thinking := func(raws ...string) []ContentPart {
		parts := make([]ContentPart, 0, len(raws))
		for _, raw := range raws {
			parts = append(parts, ContentPart{Kind: PartOpaque, OpaqueWireType: "thinking", Extra: Extra{"thinking": json.RawMessage(raw)}})
		}
		return parts
	}
	rows := []struct {
		name  string
		extra Extra
		parts []ContentPart
		want  string
	}{
		{"content-first", Extra{"reasoning_content": json.RawMessage(`"content"`), "reasoning": json.RawMessage(`"reason"`), "reasoning_details": json.RawMessage(`[{"text":"details"}]`)}, thinking(`"parts"`), "content"},
		{"reasoning-second", Extra{"reasoning": json.RawMessage(`"reason"`), "reasoning_details": json.RawMessage(`[{"text":"details"}]`)}, thinking(`"parts"`), "reason"},
		{"details-concatenate", Extra{"reasoning_details": json.RawMessage(`[{"text":"a"},{"text":"c"},{"text":7},1]`)}, nil, "ac"},
		{"details-before-parts-and-final-when-empty", Extra{"reasoning_details": json.RawMessage(`[{"type":"reasoning.encrypted","data":"x"}]`)}, thinking(`"parts"`), ""},
		{"chosen-empty-is-final", Extra{"reasoning_content": json.RawMessage(`""`), "reasoning": json.RawMessage(`"lower"`)}, nil, ""},
		{"non-string-falls-through", Extra{"reasoning_content": json.RawMessage(`7`), "reasoning": json.RawMessage(`true`), "reasoning_details": json.RawMessage(`{"text":"x"}`)}, thinking(`"parts"`), "parts"},
		{"thinking-parts-concatenate", nil, append(thinking(`"a"`, `[{"text":"b"},{"text":"c"}]`), ContentPart{Kind: PartOpaque, OpaqueWireType: "redacted_thinking", Extra: Extra{"thinking": json.RawMessage(`"ignored"`)}}, ContentPart{Kind: PartText, Text: "ignored"}), "abc"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			testassert.Equal(t, ReasoningText(row.extra, row.parts), row.want)
		})
	}
}
