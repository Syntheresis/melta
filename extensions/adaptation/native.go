package adaptation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/adaptation/patch"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

const configSchema = `{"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"},"entries":{"type":"array","items":{"type":"object","required":["pattern"],"properties":{"pattern":{"type":"string"},"prompt":{"type":"string"},"append":{"type":"array","items":{"type":"string"}},"exclude_tools":{"type":"array","items":{"type":"string"}},"tool_description_replacements":{"type":"object","propertyNames":{"minLength":1},"additionalProperties":{"type":"string"}},"offer_patch":{"type":"boolean"}},"additionalProperties":true}}},"additionalProperties":true}`

// Author registers request-local model adaptation and the multi-file patch
// tool matching models may advertise.
func Author() extension.Extension {
	return extension.Extension{
		Manifest: extension.Manifest{ID: "adaptation", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(configSchema), Services: []string{"melta.workspace"}},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			entries, err := decodeEntries(api.Settings())
			if err != nil {
				return nil, err
			}
			if _, err := api.RegisterTool(patch.Definition(), patch.Tool, nil); err != nil {
				return nil, err
			}
			_, err = api.AttachRequest(func(_ context.Context, _ extension.AuthorContext, input extension.AuthorRequestInput) (harness.RequestConsequence, error) {
				if input.Purpose != harness.InvocationPurposeConversation {
					return harness.RequestConsequence{}, nil
				}
				matched := entry{}
				if found := matchEntryIn(entries, input.Model.Model); found != nil {
					matched = *found
				}
				result := harness.RequestConsequence{}
				text := input.Prompt
				changed := false
				if matched.adapt.Prompt != nil {
					text, changed = *matched.adapt.Prompt, true
				}
				if len(matched.adapt.Append) != 0 && (!matched.bundled || input.PromptAssembled) {
					text, changed = TransformPrompt(text, &Adaptation{Append: matched.adapt.Append}), true
				}
				if changed {
					result.Prompt = &text
				}
				excluded := make(map[string]bool, len(matched.adapt.ExcludeTools))
				for _, name := range matched.adapt.ExcludeTools {
					excluded[name] = true
				}
				replacer := newToolDescriptionReplacer(matched.adapt.ToolDescriptionReplacements)
				result.Tools = make([]model.ToolDefinition, 0, len(input.Tools))
				for _, tool := range input.Tools {
					if tool.Name == patch.ToolName && !matched.adapt.OfferPatch {
						continue
					}
					if excluded[tool.Name] {
						continue
					}
					tool.Description = replacer.Replace(tool.Description)
					result.Tools = append(result.Tools, tool)
				}
				return result, nil
			}, nil)
			return nil, err
		},
	}
}

// configuredEntry is one user-supplied match entry. It accepts the same
// pattern/append/exclude_tools/tool_description_replacements fields as the
// bundled rows plus an optional complete prompt replacement. Unknown fields
// are uninterpreted.
type configuredEntry struct {
	Pattern                     string            `json:"pattern"`
	Prompt                      *string           `json:"prompt"`
	Append                      []string          `json:"append"`
	ExcludeTools                []string          `json:"exclude_tools"`
	ToolDescriptionReplacements map[string]string `json:"tool_description_replacements"`
	OfferPatch                  bool              `json:"offer_patch"`
}

// decodeEntries parses and owns the configured entries once from the captured
// section, then places them before the bundled rows. Only the exact
// schema-recognized member names are consumed: differently cased or unknown
// members stay uninterpreted even when their JSON types are invalid, and a
// lower-cased member cannot be overridden by a differently cased sibling.
func decodeEntries(section json.RawMessage) ([]entry, error) {
	var members map[string]json.RawMessage
	if len(bytes.TrimSpace(section)) != 0 {
		if err := json.Unmarshal(section, &members); err != nil {
			return nil, fmt.Errorf("decode adaptation section: %w", err)
		}
	}
	var rawEntries []json.RawMessage
	if raw, ok := members["entries"]; ok {
		if err := json.Unmarshal(raw, &rawEntries); err != nil {
			return nil, fmt.Errorf("decode adaptation entries: %w", err)
		}
	}
	entries := make([]entry, 0, len(rawEntries)+len(bundledTable))
	for i, raw := range rawEntries {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("decode adaptation entries[%d]: %w", i, err)
		}
		var configured configuredEntry
		for _, member := range []struct {
			name   string
			target any
		}{
			{"pattern", &configured.Pattern},
			{"prompt", &configured.Prompt},
			{"append", &configured.Append},
			{"exclude_tools", &configured.ExcludeTools},
			{"tool_description_replacements", &configured.ToolDescriptionReplacements},
			{"offer_patch", &configured.OfferPatch},
		} {
			if value, ok := fields[member.name]; ok {
				if err := json.Unmarshal(value, member.target); err != nil {
					return nil, fmt.Errorf("decode adaptation entries[%d].%s: %w", i, member.name, err)
				}
			}
		}
		entries = append(entries, entry{
			pattern: configured.Pattern,
			adapt: Adaptation{
				Prompt:                      configured.Prompt,
				ExcludeTools:                append([]string(nil), configured.ExcludeTools...),
				Append:                      append([]string(nil), configured.Append...),
				ToolDescriptionReplacements: cloneStringMap(configured.ToolDescriptionReplacements),
				OfferPatch:                  configured.OfferPatch,
			},
		})
	}
	return append(entries, bundledTable...), nil
}
