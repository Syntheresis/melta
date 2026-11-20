package prompt_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/prompt"
)

func assemblyInput(workspace string, rules prompt.Rules) prompt.AssemblyInput {
	return prompt.AssemblyInput{
		Identity:      "## Identity\n\nCALLER_IDENTITY_MARKER",
		Instructions:  "Do the task.",
		Rules:         rules,
		Workspace:     workspace,
		SessionStart:  testStart,
		OS:            "testos",
		Shell:         "/bin/testsh",
		OSDescription: "TestOS/testarch",
	}
}

func assembleInput(t *testing.T, input prompt.AssemblyInput) string {
	t.Helper()
	return prompt.Assemble(input)
}

func assertOrder(t *testing.T, text string, markers ...string) {
	t.Helper()
	previous := -1
	for _, marker := range markers {
		index := strings.Index(text, marker)
		testassert.Require(t, index >= 0, "missing order marker %q\n%s", marker, text)
		testassert.Require(t, index >= previous, "marker %q at %d appeared before previous marker at %d\n%s", marker, index, previous, text)
		previous = index
	}
}

func TestAssembleBaselineSectionsEnvironmentAndRules(t *testing.T) {
	workspace := t.TempDir()
	rules := prompt.Rules{GlobalText: "GLOBAL_RULES_MARKER", WorkspaceText: "WORKSPACE_RULES_MARKER"}
	assembled := assembleInput(t, assemblyInput(workspace, rules))

	for _, want := range []string{
		"## Identity",
		"CALLER_IDENTITY_MARKER",
		"## Core Rules",
		"## Rules File Guide",
		"## Compaction Awareness",
		"## Environment",
		"Working directory: " + workspace,
		"Platform: testos",
		"Shell: /bin/testsh",
		"OS: TestOS/testarch",
		"Session started: " + testStart.Format("2006-01-02 15:04:05 MST"),
		"## Safety",
		"## Tone",
		"## Task Execution",
		"## Language",
		"Do the task.",
		"GLOBAL_RULES_MARKER",
		"WORKSPACE_RULES_MARKER",
	} {
		testassert.Require(t, strings.Contains(assembled, want), "prompt missing %q\n%s", want, assembled)
	}
	assertOrder(t, assembled,
		"## Identity",
		"CALLER_IDENTITY_MARKER",
		"## Core Rules",
		"## Rules File Guide",
		"## Compaction Awareness",
		"## Environment",
		"Working directory: "+workspace,
		"## Safety",
		"## Tone",
		"## Task Execution",
		"## Language",
		"Do the task.",
		"GLOBAL_RULES_MARKER",
		"WORKSPACE_RULES_MARKER",
	)
}

func TestAssembleHeadingSuppression(t *testing.T) {
	const (
		safetyBody   = "Before modifying or deleting files, consider whether the action is reversible."
		toneBody     = "Be direct and concise."
		taskBody     = "For simple, well-defined tasks: execute immediately."
		languageBody = "Respond in the same language the user writes in."
	)
	sections := map[string]string{
		"safety":         safetyBody,
		"tone":           toneBody,
		"task_execution": taskBody,
		"language":       languageBody,
	}
	cases := []struct {
		name       string
		global     string
		workspace  string
		suppressed []string
	}{
		{"level 1 safety", "", "# Safety\n\nUSER_SAFETY_MARKER", []string{"safety"}},
		{"level 2 tone", "", "## Tone\n\nUSER_TONE_MARKER", []string{"tone"}},
		{"level 3 task execution", "", "### Task Execution\n\nUSER_TASK_MARKER", []string{"task_execution"}},
		{"task execution", "", "## Task Execution\n\nUSER_TASK_MARKER", []string{"task_execution"}},
		{"task underscore", "", "## task_execution\n\nUSER_TASK_MARKER", []string{"task_execution"}},
		{"task hyphen", "", "## task-execution\n\nUSER_TASK_MARKER", []string{"task_execution"}},
		{"task tab", "", "## Task\tExecution\n\nUSER_TASK_MARKER", []string{"task_execution"}},
		{"task multiple spaces", "", "## Task  Execution\n\nUSER_TASK_MARKER", []string{"task_execution"}},
		{"task closing hashes", "", "## Task Execution ##\n\nUSER_TASK_MARKER", []string{"task_execution"}},
		{"combined headings", "", "# Safety\ntext\n## Tone\n### Task Execution\n# other\n\nCOMBINED_USER_MARKER", []string{"safety", "tone", "task_execution"}},
		{"global safety suppression", "# Safety\n\nGLOBAL_SAFETY_MARKER", "", []string{"safety"}},
		{"unrelated other heading", "", "## Other\n\nUSER_OTHER_MARKER", nil},
		{"unrelated environment heading", "", "## Environment\n\nUSER_ENVIRONMENT_MARKER", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rules := prompt.Rules{GlobalText: tc.global, WorkspaceText: tc.workspace}
			assembled := assembleInput(t, assemblyInput(t.TempDir(), rules))
			suppressed := make(map[string]bool, len(tc.suppressed))
			for _, name := range tc.suppressed {
				suppressed[name] = true
			}
			for name, body := range sections {
				if suppressed[name] {
					testassert.Require(t, !strings.Contains(assembled, body), "built-in %s content survived its matching user heading in %q\n%s", name, tc.workspace+tc.global, assembled)
					continue
				}
				testassert.Require(t, strings.Contains(assembled, body), "unrelated built-in %s content was suppressed by %q\n%s", name, tc.workspace+tc.global, assembled)
			}
			userText := tc.workspace
			if userText == "" {
				userText = tc.global
			}
			testassert.Require(t, strings.Contains(assembled, strings.TrimSpace(userText)), "user rules text is missing\n%s", assembled)
		})
	}
}

func TestAssembleInstructionsInsertedAsGiven(t *testing.T) {
	workspace := t.TempDir()
	for _, instructions := range []string{
		"Do the task.",
		"## Your Role and Instructions\n\nDo the task.",
		"# Your own heading\n\nDo the task.",
	} {
		input := assemblyInput(workspace, prompt.Rules{})
		input.Instructions = instructions
		assembled := assembleInput(t, input)
		testassert.Require(t, strings.Contains(assembled, strings.TrimSpace(instructions)), "instructions %q lost their body\n%s", instructions, assembled)
		testassert.Require(t, strings.Count(assembled, "## Your Role and Instructions") == strings.Count(instructions, "## Your Role and Instructions"), "instructions %q gained an injected heading\n%s", instructions, assembled)
	}
}

func TestAssembleUsesOnlyExplicitInputs(t *testing.T) {
	home := t.TempDir()
	testassert.NoError(t, os.MkdirAll(filepath.Join(home, ".melta"), 0o700))
	writeRules(t, filepath.Join(home, ".melta"), "AMBIENT_HOME_RULES")
	t.Setenv("HOME", home)
	t.Setenv("SHELL", "/ambient-shell")

	workspace := t.TempDir()
	input := assemblyInput(workspace, prompt.Rules{WorkspaceText: "EXPLICIT_RULES"})
	input.Identity = ""
	input.Shell = "caller-shell"
	assembled := assembleInput(t, input)

	for _, forbidden := range []string{"AMBIENT_HOME_RULES", "Melta, an agentic coding assistant", "/ambient-shell"} {
		testassert.Require(t, !strings.Contains(assembled, forbidden), "assembly carried implicit input %q\n%s", forbidden, assembled)
	}
	testassert.Require(t, strings.Contains(assembled, "Shell: caller-shell") && strings.Contains(assembled, "EXPLICIT_RULES"), "assembly lost an explicit input\n%s", assembled)

	ambientRules := readRules(t, "", workspace)
	testassert.Require(t, ambientRules.GlobalText == "" && !strings.Contains(ambientRules.GlobalText, "AMBIENT_HOME_RULES"), "ReadRules looked up the ambient home: %+v", ambientRules)
}

func TestAssembleDoesNotRereadWorkspaceRules(t *testing.T) {
	workspace := t.TempDir()
	input := assemblyInput(workspace, prompt.Rules{})
	writeRules(t, workspace, "LATE_WORKSPACE_RULES")
	assembled := assembleInput(t, input)
	testassert.Require(t, !strings.Contains(assembled, "LATE_WORKSPACE_RULES"), "pure assembly read the Workspace instead of the captured rules")
}

func TestAssembleIsDeterministicForEqualInputs(t *testing.T) {
	workspace := t.TempDir()
	rules := readRules(t, t.TempDir(), workspace)
	firstInput := assemblyInput(workspace, rules)
	firstInput.SessionStart = testStart
	secondInput := assemblyInput(workspace, rules)
	secondInput.SessionStart = testStart.Add(90 * time.Minute)
	first := assembleInput(t, firstInput)
	second := assembleInput(t, secondInput)
	testassert.Require(t, first != second, "distinct SessionStart values produced identical prompts")
	testassert.Require(t, strings.Contains(first, "Session started: "+testStart.Format("2006-01-02 15:04:05 MST")), "first prompt lost its SessionStart\n%s", first)
	testassert.Require(t, strings.Contains(second, "Session started: "+secondInput.SessionStart.Format("2006-01-02 15:04:05 MST")), "second prompt lost its SessionStart\n%s", second)
	for i := 0; i < 5; i++ {
		testassert.Equal(t, assembleInput(t, firstInput), first)
		testassert.Equal(t, assembleInput(t, secondInput), second)
	}
}
