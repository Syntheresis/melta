package agents

import (
	"context"
	"slices"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/model"
)

func (s authorInstance) admit(_ context.Context, call extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
	candidate := input.Candidate
	changed := false
	definition, err := s.definitions.Resolve(selected(call.State))
	if err == nil {
		changed = true
		if definition.Model != "" {
			if ref, err := model.Parse(definition.Model); err == nil {
				candidate.Model = ref
			}
		}
		switch definition.SystemPrompt {
		case SystemPromptNone:
			text := strings.TrimSpace(definition.Prompt)
			candidate.Prompt = &text
		case SystemPromptSimple:
			text, err := simplePrompt(input.Prompt, definition.Prompt)
			if err != nil {
				return extension.AdmissionResult{}, err
			}
			candidate.Prompt = &text
		default:
			candidate.Instructions = formatInstructions(definition.Prompt)
			candidate.Prompt = nil
		}
		if definition.Tools != nil {
			candidate.Tools = make([]model.ToolDefinition, 0, len(definition.Tools))
			for _, name := range definition.Tools {
				for _, tool := range input.ToolCatalog {
					if tool.Name == name {
						candidate.Tools = append(candidate.Tools, tool)
						break
					}
				}
			}
		}
		if definition.Readonly && strings.TrimSpace(definition.WriteDir) == "" {
			candidate.Tools = withoutTools(candidate.Tools, "write_file", "edit_file")
		}
		if definition.Readonly {
			candidate.Tools = slices.Clone(candidate.Tools)
			for i := range candidate.Tools {
				if candidate.Tools[i].Name == "run_command" {
					candidate.Tools[i].Description = ReadOnlyRunCommandDescription
				}
			}
		}
	}
	if input.Session.OwnerSessionID != "" {
		candidate.Tools = withoutTools(candidate.Tools, TaskCapabilityID)
		changed = true
	}
	if !changed {
		return extension.AdmissionResult{}, nil
	}
	return extension.AdmissionResult{Candidate: &candidate}, nil
}

func withoutTools(tools []model.ToolDefinition, names ...string) []model.ToolDefinition {
	kept := make([]model.ToolDefinition, 0, len(tools))
	for _, tool := range tools {
		excluded := false
		for _, name := range names {
			excluded = excluded || tool.Name == name
		}
		if !excluded {
			kept = append(kept, tool)
		}
	}
	return kept
}

func simplePrompt(access extension.PromptAccess, instructions string) (string, error) {
	rules, sections, err := access.Rules()
	if err != nil {
		return "", err
	}
	var parts []string
	for _, section := range access.Sections() {
		switch section.Name {
		case "identity", "core_rules", "compaction_awareness":
			parts = append(parts, strings.TrimSpace(section.Text))
		}
	}
	parts = append(parts, strings.TrimSpace(access.Environment()))
	for _, section := range sections {
		if section.Name == "safety" {
			parts = append(parts, strings.TrimSpace(section.Text))
		}
	}
	if text := formatInstructions(instructions); text != "" {
		parts = append(parts, text)
	}
	if text := strings.TrimSpace(rules); text != "" {
		parts = append(parts, text)
	}
	return strings.TrimSpace(strings.Join(parts, "\n\n")), nil
}

const agentPromptHeading = "## Your Role and Instructions"

// formatInstructions renders this extension's instruction heading and body,
// keeping a body that already opens with the role heading unchanged.
func formatInstructions(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	if !startsWithAgentPromptHeading(body) {
		body = agentPromptHeading + "\n\n" + body
	}
	return body
}

func startsWithAgentPromptHeading(body string) bool {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "#") {
			return false
		}
		headingText := strings.TrimLeft(line, "#")
		headingText = strings.TrimSpace(headingText)
		headingText = strings.TrimSpace(strings.TrimRight(headingText, "#"))
		headingText = strings.ToLower(headingText)
		headingText = strings.ReplaceAll(headingText, "-", " ")
		headingText = strings.ReplaceAll(headingText, "_", " ")
		headingText = strings.Join(strings.Fields(headingText), " ")
		return headingText == "your role and instructions"
	}
	return false
}
