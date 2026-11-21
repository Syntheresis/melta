package melta

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/testassert"
)

func baseSurfaceRequest(t *testing.T) surfaceRequest {
	t.Helper()
	return surfaceRequest{
		hostIdentity:   "FIXTURE_IDENTITY",
		globalRulesDir: t.TempDir(),
		workspace:      t.TempDir(),
		shell:          "/bin/fixture-shell",
	}
}

func TestComposeSurfaceLazyReplacementAndMemoizedRead(t *testing.T) {
	req := baseSurfaceRequest(t)
	path := filepath.Join(req.workspace, "AGENTS.md")
	testassert.NoError(t, os.WriteFile(path, []byte("FIRST RULE"), 0o600))
	provider := newSurfacePrompt(req)
	empty := ""
	replaced, err := provider.resolve(&empty)
	testassert.NoError(t, err)
	if replaced.Prompt != "" || len(replaced.Warnings) != 0 || provider.read || provider.assembled {
		t.Fatalf("replacement read default: %+v", replaced)
	}
	first, err := provider.defaultPrompt()
	testassert.NoError(t, err)
	if !strings.Contains(first.Prompt, "FIRST RULE") {
		t.Fatal("default did not read actual rules")
	}
	testassert.NoError(t, os.WriteFile(path, []byte("SECOND RULE"), 0o600))
	again, err := provider.defaultPrompt()
	testassert.NoError(t, err)
	if again.Prompt != first.Prompt || strings.Contains(again.Prompt, "SECOND RULE") {
		t.Fatal("default was read again")
	}
}

func TestComposeSurfaceLazyWarningsFollowRead(t *testing.T) {
	req := baseSurfaceRequest(t)
	path := filepath.Join(req.workspace, "AGENTS.md")
	testassert.NoError(t, os.Mkdir(path, 0o700))
	provider := newSurfacePrompt(req)
	replacement := "replacement"
	result, err := provider.resolve(&replacement)
	testassert.NoError(t, err)
	if len(result.Warnings) != 0 || provider.read {
		t.Fatalf("unread replacement has warnings: %+v", result)
	}
	rules := provider.readRules()
	if len(rules.Warnings) == 0 || provider.assembled {
		t.Fatal("actual read did not report its warning")
	}
	replaced, err := provider.resolve(&replacement)
	testassert.NoError(t, err)
	if replaced.Prompt != replacement || !reflect.DeepEqual(replaced.Warnings, rules.Warnings) || provider.assembled {
		t.Fatalf("replacement lost actual read warnings: %+v", replaced)
	}
	testassert.NoError(t, os.Remove(path))
	testassert.NoError(t, os.WriteFile(path, []byte("new rules"), 0o600))
	result, err = provider.defaultPrompt()
	testassert.NoError(t, err)
	again, err := provider.defaultPrompt()
	testassert.NoError(t, err)
	if !reflect.DeepEqual(again, result) {
		t.Fatal("repeated read changed the admission result")
	}
	replaced, err = provider.resolve(&replacement)
	testassert.NoError(t, err)
	if replaced.Prompt != replacement || !reflect.DeepEqual(replaced.Warnings, result.Warnings) {
		t.Fatalf("replacement lost actual read warnings: %+v", replaced)
	}
}

func TestComposeSurfaceRulesDemandBeforeDefault(t *testing.T) {
	req := baseSurfaceRequest(t)
	path := filepath.Join(req.workspace, "AGENTS.md")
	testassert.NoError(t, os.WriteFile(path, []byte("## Tone\n\nearly rule"), 0o600))
	provider := newSurfacePrompt(req)
	sections, environment := provider.sections(), provider.environment()
	if len(sections) == 0 || !strings.Contains(environment, req.workspace) || provider.read || provider.assembled {
		t.Fatalf("pure values read rules: %v %q", sections, environment)
	}
	rules := provider.readRules()
	if rules.WorkspaceText != "## Tone\n\nearly rule" || provider.assembled {
		t.Fatalf("rules demand assembled default: %+v", rules)
	}
	testassert.NoError(t, os.WriteFile(path, []byte("late rule"), 0o600))
	combined, ruleSections, err := provider.ruleSections()
	testassert.NoError(t, err)
	if combined != "## Tone\n\nearly rule" || len(ruleSections) == 0 {
		t.Fatalf("rules values = %q, %+v", combined, ruleSections)
	}
	for _, section := range ruleSections {
		if section.Name == "tone" {
			t.Fatal("rules heading did not suppress tone")
		}
	}
	result, err := provider.defaultPrompt()
	testassert.NoError(t, err)
	if !strings.Contains(result.Prompt, "early rule") || strings.Contains(result.Prompt, "late rule") {
		t.Fatal("default did not reuse the admission's rules read")
	}
	for _, section := range append(sections, ruleSections...) {
		if !strings.Contains(result.Prompt, strings.TrimSpace(section.Text)) {
			t.Fatalf("assembled prompt missing section %s", section.Name)
		}
	}
	if !strings.Contains(result.Prompt, environment) {
		t.Fatal("assembled prompt missing captured environment")
	}
}

func TestComposeSurfaceRulesPrecedenceAndOverrides(t *testing.T) {
	globalDir := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(globalDir, "AGENTS.md"), []byte("GLOBAL CONTENT"), 0o600))
	workspace := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(workspace, "AGENTS.md"), []byte("PROJECT CONTENT"), 0o600))
	req := baseSurfaceRequest(t)
	req.globalRulesDir, req.workspace = globalDir, workspace
	result, err := newSurfacePrompt(req).defaultPrompt()
	testassert.NoError(t, err)
	testassert.Require(t, strings.Contains(result.Prompt, "GLOBAL CONTENT") && strings.Contains(result.Prompt, "PROJECT CONTENT"), "the composed prompt lost a rules layer")
	testassert.Require(t, strings.Index(result.Prompt, "GLOBAL CONTENT") < strings.Index(result.Prompt, "PROJECT CONTENT"), "project rules did not render after global rules")
	// A user heading overrides the built-in section while the rest of the
	// assembly is unchanged.
	testassert.NoError(t, os.WriteFile(filepath.Join(workspace, "AGENTS.md"), []byte("## Tone\n\nProject tone rule."), 0o600))
	result, err = newSurfacePrompt(req).defaultPrompt()
	testassert.NoError(t, err)
	testassert.Require(t, !strings.Contains(result.Prompt, "Be direct and concise"), "the built-in tone section survived a user tone heading override")
	testassert.Require(t, strings.Contains(result.Prompt, "Project tone rule."), "the overriding project tone rule is missing")
}

func TestComposeSurfaceZeroSessionStartSamplesComposeTime(t *testing.T) {
	req := baseSurfaceRequest(t)
	before := time.Now().Add(-time.Second)
	result, err := newSurfacePrompt(req).defaultPrompt()
	after := time.Now().Add(time.Second)
	testassert.NoError(t, err)
	const marker = "Session started: "
	i := strings.Index(result.Prompt, marker)
	testassert.Require(t, i >= 0, "the composed prompt lost the session start: %q", result.Prompt)
	line := result.Prompt[i+len(marker):]
	if j := strings.IndexByte(line, '\n'); j >= 0 {
		line = line[:j]
	}
	sampled, err := time.ParseInLocation("2006-01-02 15:04:05 MST", line, time.Local)
	testassert.Require(t, err == nil, "parsing the sampled session start %q: %v", line, err)
	testassert.Require(t, !sampled.Before(before) && !sampled.After(after), "sampled session start = %s, want a compose-time value within [%s, %s]", sampled, before, after)
	// An explicit session start is used verbatim.
	req.sessionStart = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	result, err = newSurfacePrompt(req).defaultPrompt()
	testassert.NoError(t, err)
	testassert.Require(t, strings.Contains(result.Prompt, "Session started: 2026-01-02 03:04:05 UTC"), "the explicit session start did not render verbatim: %q", result.Prompt)
}
