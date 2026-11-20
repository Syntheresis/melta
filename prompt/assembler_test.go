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

var testStart = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func writeRules(t *testing.T, dir, content string) {
	t.Helper()
	testassert.NoError(t, os.WriteFile(filepath.Join(dir, "AGENTS.md"), []byte(content), 0o600))
}

func readRules(t *testing.T, globalDir, workspace string) prompt.Rules {
	t.Helper()
	return prompt.ReadRules(globalDir, workspace)
}

func TestReadRulesReadsOnlyAgentsInGlobalThenWorkspaceOrder(t *testing.T) {
	globalDir := t.TempDir()
	workspaceRoot := t.TempDir()
	workspace := filepath.Join(workspaceRoot, "workspace")
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))
	writeRules(t, globalDir, "GLOBAL_RULES")
	writeRules(t, workspaceRoot, "PARENT_AGENTS_MARKER")
	writeRules(t, workspace, "WORKSPACE_RULES")
	testassert.NoError(t, os.WriteFile(filepath.Join(workspace, "CLAUDE.md"), []byte("ALTERNATIVE_NAME_MARKER"), 0o600))

	rules := readRules(t, globalDir, workspace)
	testassert.Require(t, rules.GlobalText == "GLOBAL_RULES" && rules.WorkspaceText == "WORKSPACE_RULES", "rules = %+v, want both layers", rules)
	testassert.Require(t, len(rules.Warnings) == 0, "warnings = %+v, want none", rules.Warnings)

	assembled := prompt.Assemble(prompt.AssemblyInput{Rules: rules, Workspace: workspace, SessionStart: testStart})
	testassert.Require(t, strings.Index(assembled, "GLOBAL_RULES") < strings.Index(assembled, "WORKSPACE_RULES"), "global rules did not render before workspace rules")
	testassert.Require(t, !strings.Contains(assembled, "PARENT_AGENTS_MARKER") && !strings.Contains(assembled, "ALTERNATIVE_NAME_MARKER"), "assembly searched a parent or alternative rules file")
}

func TestReadRulesEmptyGlobalDirectoryPerformsNoGlobalLookup(t *testing.T) {
	workspace := t.TempDir()
	rules := readRules(t, "", workspace)
	testassert.Require(t, rules.GlobalText == "" && len(rules.Warnings) == 1 && rules.Warnings[0].Kind == prompt.WarnRulesNotFound, "rules = %+v, want no global layer and only the not-found warning", rules)
}

func TestReadRulesWarningsKindsMessagesAndOrder(t *testing.T) {
	type layer struct {
		content    string
		unreadable bool
	}
	type wantWarning struct {
		kind          string
		messagePrefix string
	}
	cases := []struct {
		name          string
		global        layer
		workspace     layer
		wantGlobal    string
		wantWorkspace string
		wantWarnings  []wantWarning
	}{
		{
			name:         "both layers fail",
			global:       layer{unreadable: true},
			workspace:    layer{unreadable: true},
			wantWarnings: []wantWarning{{prompt.WarnRulesReadError, "Failed to read global rules file: "}, {prompt.WarnRulesReadError, "Failed to read project rules file: "}, {prompt.WarnRulesNotFound, "No AGENTS.md found"}},
		},
		{
			name:          "global read error keeps workspace text",
			global:        layer{unreadable: true},
			workspace:     layer{content: "WORKSPACE_OK"},
			wantWorkspace: "WORKSPACE_OK",
			wantWarnings:  []wantWarning{{prompt.WarnRulesReadError, "Failed to read global rules file: "}},
		},
		{
			name:         "workspace read error keeps global text",
			global:       layer{content: "GLOBAL_OK"},
			workspace:    layer{unreadable: true},
			wantGlobal:   "GLOBAL_OK",
			wantWarnings: []wantWarning{{prompt.WarnRulesReadError, "Failed to read project rules file: "}},
		},
	}
	setup := func(t *testing.T, l layer) string {
		t.Helper()
		dir := t.TempDir()
		if l.unreadable {
			testassert.NoError(t, os.Mkdir(filepath.Join(dir, "AGENTS.md"), 0o700))
		} else if l.content != "" {
			writeRules(t, dir, l.content)
		}
		return dir
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			globalDir := setup(t, tc.global)
			workspace := setup(t, tc.workspace)
			rules := readRules(t, globalDir, workspace)
			testassert.Require(t, rules.GlobalText == tc.wantGlobal && rules.WorkspaceText == tc.wantWorkspace, "rules = %+v, want global %q and workspace %q", rules, tc.wantGlobal, tc.wantWorkspace)
			testassert.Require(t, len(rules.Warnings) == len(tc.wantWarnings), "warnings = %+v, want %d entries", rules.Warnings, len(tc.wantWarnings))
			for i, want := range tc.wantWarnings {
				got := rules.Warnings[i]
				testassert.Require(t, got.Kind == want.kind && strings.HasPrefix(got.Message, want.messagePrefix), "warning %d = %+v, want kind %q and message prefix %q", i, got, want.kind, want.messagePrefix)
			}
		})
	}
}

func TestReadRulesCompleteOversizedTextAndWarningThreshold(t *testing.T) {
	globalDir := t.TempDir()
	workspace := t.TempDir()
	oversized := strings.Repeat("x", 20001)
	writeRules(t, workspace, oversized)
	rules := readRules(t, globalDir, workspace)
	testassert.Require(t, rules.WorkspaceText == oversized, "oversized rules text was truncated")
	testassert.Require(t, len(rules.Warnings) == 1 && rules.Warnings[0].Kind == prompt.WarnRulesTooLarge, "warnings = %+v, want exactly one rules_too_large warning and no rules_not_found", rules.Warnings)

	assembled := prompt.Assemble(prompt.AssemblyInput{Rules: rules, Workspace: workspace, SessionStart: testStart})
	testassert.Require(t, strings.Contains(assembled, oversized), "assembled prompt dropped the complete oversized rules text")
}
