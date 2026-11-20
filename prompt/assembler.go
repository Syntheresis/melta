// Package prompt assembles the baseline system prompt from explicit caller
// inputs. It owns no ambient person, location, environment or clock: rules
// reading binds the caller's directories, and assembly renders only the
// supplied identity, instructions, rule text and captured environment values.
package prompt

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//go:embed core_rules.md
var coreRulesSection string

//go:embed rules_file_guide.md
var rulesFileGuideSection string

//go:embed compaction_awareness.md
var compactionAwarenessSection string

//go:embed safety.md
var safetySection string

//go:embed tone.md
var toneSection string

//go:embed task_execution.md
var taskExecutionSection string

//go:embed language.md
var languageSection string

var overridableOrder = []string{"safety", "tone", "task_execution", "language"}

var overridableSections = map[string]string{
	"safety":         safetySection,
	"tone":           toneSection,
	"task_execution": taskExecutionSection,
	"language":       languageSection,
}

const (
	WarnRulesTooLarge  = "rules_too_large"
	WarnRulesNotFound  = "rules_not_found"
	WarnRulesReadError = "rules_read_error"
)

// Warning is one non-fatal rule-reading diagnostic.
type Warning struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

// Rules is the captured rule text one assembly consumes. Warnings carries the
// diagnostics of the read that produced it.
type Rules struct {
	GlobalText    string    `json:"global_text"`
	WorkspaceText string    `json:"workspace_text"`
	Warnings      []Warning `json:"warnings,omitempty"`
}

// AssemblyInput is the complete pure input of one baseline prompt. Identity is
// the application-supplied persona. OS, Shell and OSDescription are captured
// environment values rendered by the baseline; OS is the platform identifier.
type AssemblyInput struct {
	Identity      string    `json:"identity"`
	Instructions  string    `json:"instructions"`
	Rules         Rules     `json:"rules"`
	Workspace     string    `json:"workspace"`
	SessionStart  time.Time `json:"session_start"`
	OS            string    `json:"os"`
	Shell         string    `json:"shell"`
	OSDescription string    `json:"os_description"`
}

// ReadRules reads the current global and Workspace AGENTS.md text. An empty
// global directory performs no global lookup. Ordinary read problems become
// warnings while the readable layer still contributes its text.
func ReadRules(globalDir, workspace string) Rules {
	var rules Rules

	if globalDir != "" {
		content, err := readRulesFile(globalDir)
		if err != nil {
			rules.Warnings = append(rules.Warnings, Warning{Kind: WarnRulesReadError, Message: "Failed to read global rules file: " + err.Error()})
			content = ""
		}
		rules.GlobalText = content
	}

	workspaceContent, err := readRulesFile(workspace)
	if err != nil {
		rules.Warnings = append(rules.Warnings, Warning{Kind: WarnRulesReadError, Message: "Failed to read project rules file: " + err.Error()})
		workspaceContent = ""
	}
	rules.WorkspaceText = workspaceContent

	if rules.WorkspaceText == "" && rules.GlobalText == "" {
		rules.Warnings = append(rules.Warnings, Warning{Kind: WarnRulesNotFound, Message: "No AGENTS.md found"})
	}

	combined := rules.GlobalText + "\x00" + rules.WorkspaceText
	if len(combined) > 20000 {
		rules.Warnings = append(rules.Warnings, Warning{Kind: WarnRulesTooLarge, Message: fmt.Sprintf("Rules file exceeds 20,000 characters (%d chars). Consider trimming it.", len(combined))})
	}
	return rules
}

// Assemble renders the complete baseline prompt for input. It performs no I/O
// and reads no ambient value: equal inputs produce equal output.
func Assemble(input AssemblyInput) string {
	return buildSpec(input)
}

func buildSpec(input AssemblyInput) string {
	var b strings.Builder

	for _, section := range BaselineSections(input) {
		b.WriteString(strings.TrimSpace(section.Text))
		b.WriteString("\n\n")
	}

	b.WriteString(Environment(input))
	b.WriteString("\n\n")

	rulesContent := CombinedRules(input.Rules)
	for _, section := range RuleSections(input.Rules) {
		b.WriteString(strings.TrimSpace(section.Text))
		b.WriteString("\n\n")
	}

	writeInstructions(&b, input.Instructions)

	trimmed := strings.TrimSpace(rulesContent)
	if trimmed != "" {
		b.WriteString(trimmed)
		b.WriteString("\n\n")
	}

	return strings.TrimSpace(b.String())
}

// Section is one named baseline prompt section.
type Section struct {
	Name string
	Text string
}

// CombinedRules joins the global and Workspace rules in their assembly order.
func CombinedRules(rules Rules) string {
	return strings.TrimSpace(rules.GlobalText + "\n\n" + rules.WorkspaceText)
}

// BaselineSections returns the sections preceding the environment.
func BaselineSections(input AssemblyInput) []Section {
	return []Section{
		{"identity", input.Identity},
		{"core_rules", coreRulesSection},
		{"rules_file_guide", rulesFileGuideSection},
		{"compaction_awareness", compactionAwarenessSection},
	}
}

// RuleSections returns the sections not suppressed by rules headings.
func RuleSections(rules Rules) []Section {
	overridden := detectOverrides(CombinedRules(rules))
	var sections []Section
	for _, name := range overridableOrder {
		if !overridden[name] {
			sections = append(sections, Section{name, overridableSections[name]})
		}
	}
	return sections
}

// writeInstructions inserts the supplied instructions text as one trimmed
// block, adding nothing: rendering the extension's own heading belongs to the
// extension that supplies the text.
func writeInstructions(b *strings.Builder, body string) {
	body = strings.TrimSpace(body)
	if body == "" {
		return
	}
	b.WriteString(body)
	b.WriteString("\n\n")
}

// Environment renders the captured environment without reading rules.
func Environment(input AssemblyInput) string {
	return fmt.Sprintf("## Environment\n\nWorking directory: %s\nPlatform: %s\nShell: %s\nOS: %s\nSession started: %s",
		input.Workspace,
		input.OS,
		input.Shell,
		input.OSDescription,
		input.SessionStart.Format("2006-01-02 15:04:05 MST"),
	)
}

func readRulesFile(dir string) (string, error) {
	path := filepath.Join(dir, "AGENTS.md")
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	return string(data), nil
}

func detectOverrides(rulesContent string) map[string]bool {
	result := map[string]bool{
		"safety":         false,
		"tone":           false,
		"task_execution": false,
		"language":       false,
	}
	for _, line := range strings.Split(rulesContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		headingText := strings.TrimLeft(trimmed, "#")
		headingText = strings.TrimSpace(headingText)
		headingText = strings.TrimSpace(strings.TrimRight(headingText, "#"))
		headingText = strings.ToLower(headingText)
		headingText = strings.ReplaceAll(headingText, "-", " ")
		headingText = strings.ReplaceAll(headingText, "_", " ")
		headingText = strings.Join(strings.Fields(headingText), "_")
		if _, ok := result[headingText]; ok {
			result[headingText] = true
		}
	}
	return result
}
