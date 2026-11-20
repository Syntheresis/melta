package agents

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
)

// retainedSummarizerPrompt is the compact built-in's retained summarizer
// prompt, pinned here as an independent literal of the resolved compact
// definition.
const retainedSummarizerPrompt = `You are summarizing a coding agent conversation so the agent can continue working with reduced context. Your summary must preserve everything the agent needs to continue without re-reading files or re-asking the user.

Produce a summary with these sections:

## Goal
What the user asked for, in their words. Include exact quotes of key instructions.

## Files
Which files were read, created, or modified. For modified files: what changed and why.

## Current state
What has been done so far. What works, what doesn't.

## Unfinished work
Tasks started but not completed. Errors not yet resolved. Things the agent said it would do next.

## Decisions
Choices the user made or confirmed. Constraints they stated. Things they rejected.

Be specific — include file paths, function names, error messages, line numbers. Do not generalize. If the user said "don't use React," write that exactly, not "user has framework preferences."`

func TestResolveBuiltinsInheritThroughPrimary(t *testing.T) {
	cfg, err := parse([]byte(`{"primary":{"model":"test/base-model"},"secondary":{"model":"test/secondary-model"}}`))
	testassert.NoError(t, err)

	secondary, err := cfg.Resolve("secondary")
	testassert.NoError(t, err)
	testassert.Require(t, secondary.Model == "test/secondary-model" && secondary.SystemPrompt == SystemPromptSimple, "secondary = model:%q system_prompt:%q, want its own overlay and simple prompt", secondary.Model, secondary.SystemPrompt)

	explore, err := cfg.Resolve("explore")
	testassert.NoError(t, err)
	testassert.Require(t, explore.Model == "test/secondary-model" && explore.SystemPrompt == SystemPromptSimple && explore.Subagent && explore.Readonly, "got %v, want %v; got %v, want %v; explore booleans = subagent:%v readonly:%v", explore.Model, "test/secondary-model", explore.SystemPrompt, SystemPromptSimple, explore.Subagent, explore.Readonly)

	review, err := cfg.Resolve("review")
	testassert.NoError(t, err)
	testassert.Require(t, reflect.DeepEqual(review.Tools, StandardTools) && review.Model == "test/secondary-model" && review.SystemPrompt == SystemPromptSimple, "review = model:%q system_prompt:%q tools:%v, want secondary's resolved values inherited unmodified", review.Model, review.SystemPrompt, review.Tools)
}

func TestRetainedBuiltinRosterOrderAndResolution(t *testing.T) {
	cfg, err := parse([]byte(`{}`))
	testassert.NoError(t, err)
	wantOrder := []string{"primary", "secondary", "explore", "review", "compact"}
	got := namesOf(cfg.All())
	testassert.Require(t, reflect.DeepEqual(got, wantOrder), "builtin roster = %v, want %v in order", got, wantOrder)
	for _, name := range wantOrder {
		resolved, err := cfg.Resolve(name)
		testassert.Require(t, err == nil, "Resolve(%s): %v", name, err)
		testassert.Require(t, resolved.Builtin, "%s Builtin = false, want true", name)
	}

	{
		_, err := cfg.Resolve("plan")
		testassert.Require(t, err != nil && strings.Contains(err.Error(), `unknown agent type "plan"`), "Resolve(plan) error = %v, want unknown agent type before any resolution", err)
	}
}

func TestCustomTypeNamedPlanFollowsOrdinaryRules(t *testing.T) {
	cfg, err := parse([]byte(`{
  "primary": {"model":"test/base-model"},
  "plan": {"prompt":"Write design notes.","tools":["read_file"],"readonly":true,"write_dir":"docs","description":"Notes writer."}
}`))
	testassert.NoError(t, err)

	resolved, err := cfg.Resolve("plan")
	testassert.NoError(t, err)
	testassert.Require(t, !resolved.Builtin, `custom type "plan" marked as builtin`)
	testassert.Require(t, resolved.Model == "test/base-model" && resolved.SystemPrompt == SystemPromptSimple && reflect.DeepEqual(resolved.Tools, []string{"read_file"}), "custom plan = model:%q system_prompt:%q, want ordinary secondary inheritance of unset values; plan tools = %v, want the custom set only", resolved.Model, resolved.SystemPrompt, resolved.Tools)
	{
		got := strings.TrimSpace(resolved.Prompt)
		testassert.Require(t, got == "Write design notes." && !contains(resolved.Tools, "submit_plan"), `custom plan prompt/tools = %q/%v, want its own prompt without a built-in tool`, resolved.Prompt, resolved.Tools)
	}
	testassert.Require(t, resolved.Readonly && resolved.WriteDir == "docs" && resolved.Description == "Notes writer." && resolved.Subagent, "plan readonly/write_dir = %v/%q, want the literal custom values; custom plan description/subagent = %q/%v, want its own description and the inherited subagent flag", resolved.Readonly, resolved.WriteDir, resolved.Description, resolved.Subagent)

	all := namesOf(cfg.All())
	wantOrder := []string{"primary", "secondary", "explore", "review", "compact", "plan"}
	testassert.Require(t, reflect.DeepEqual(all, wantOrder), "All = %v, want builtins in order then the custom type", all)
}

func TestBuiltinsExposeConcreteRootAndCodeOnlyCompact(t *testing.T) {
	cfg, err := parse([]byte(`{}`))
	testassert.NoError(t, err)

	primary, err := cfg.Resolve("primary")
	testassert.NoError(t, err)
	testassert.Require(t, primary.SystemPrompt == SystemPromptFull && reflect.DeepEqual(primary.Tools, StandardTools) && !primary.Readonly && !primary.Subagent, "primary = %+v, want full prompt and standard tools; primary booleans = readonly:%v subagent:%v", primary, primary.Readonly, primary.Subagent)

	compactAgent, err := cfg.Resolve("compact")
	testassert.NoError(t, err)
	testassert.Require(t, compactAgent.SystemPrompt == SystemPromptNone && compactAgent.Prompt == retainedSummarizerPrompt && len(compactAgent.Tools) == 0 && compactAgent.Readonly && !compactAgent.Subagent, "compact prompt = system:%q body:%q; compact capability fields = %+v", compactAgent.SystemPrompt, compactAgent.Prompt, compactAgent)
}

func TestToolsPresenceControlsInheritanceEmptyAndSubset(t *testing.T) {
	cfg, err := parse([]byte(`{
  "empty": {"tools": []},
  "reader": {"tools": ["read_file", "run_command"]}
}`))
	testassert.NoError(t, err)

	empty, err := cfg.Resolve("empty")
	testassert.NoError(t, err)
	testassert.Require(t, len(empty.Tools) == 0, "empty tools = %v, want explicit empty set", empty.Tools)
	reader, err := cfg.Resolve("reader")
	testassert.NoError(t, err)
	testassert.Require(t, reflect.DeepEqual(reader.Tools, []string{"read_file", "run_command"}), "reader tools = %v, want exact subset", reader.Tools)
	review, err := cfg.Resolve("review")
	testassert.NoError(t, err)
	testassert.Require(t, reflect.DeepEqual(review.Tools, StandardTools), "review tools = %v, want inherited standard tools", review.Tools)
}

func TestBuiltinLockedFieldsIgnoredAndModelHonored(t *testing.T) {
	cfg, err := parse([]byte(`{
  "explore": {
    "model": "test/explore-model",
    "system_prompt": "none",
    "tools": "not an array",
    "readonly": false,
    "description": "override"
  }
}`))
	testassert.NoError(t, err)
	explore, err := cfg.Resolve("explore")
	testassert.NoError(t, err)
	testassert.Require(t, explore.Model == "test/explore-model" && explore.SystemPrompt == SystemPromptSimple && explore.Readonly && explore.Description != "override" && len(explore.Tools) != 0, "got %v, want %v; locked fields were not ignored: %+v", explore.Model, "test/explore-model", explore)
	if len(cfg.Warnings()) != 0 {
		t.Fatalf("warnings = %#v, want malformed locked built-in fields ignored", cfg.Warnings())
	}
}

func TestInvalidCustomDropsOnlyBadEntryWithWarning(t *testing.T) {
	cfg, err := parse([]byte(`{
  "good": {"tools": ["read_file"], "system_prompt": "simple", "model": "test/good"},
  "bad_prompt": {"system_prompt": "huge"},
  "bad_model": {"model": "not-prefixed"},
  "bad_shape": "not an object",
  "bad_tools_shape": {"tools": "read_file"}
}`))
	testassert.NoError(t, err)
	{
		_, err := cfg.Resolve("good")
		testassert.NoError(t, err)
	}
	for _, name := range []string{"bad_prompt", "bad_model", "bad_shape", "bad_tools_shape"} {
		{
			_, err := cfg.Resolve(name)
			testassert.Require(t, err != nil, "Resolve(%s) succeeded, want dropped", name)
		}
	}
	warnings := cfg.Warnings()
	testassert.Require(t, len(warnings) == 4, "warnings = %#v, want 4 invalid custom warnings", warnings)
	for _, warning := range warnings {
		testassert.Equal(t, warning.Kind, "invalid_agent_type")
	}
}

func TestBuiltinNameWinsOverUserLockedFields(t *testing.T) {
	cfg, err := parse([]byte(`{"explore":{"prompt":"custom prompt","description":"custom","subagent":false}}`))
	testassert.NoError(t, err)
	explore, err := cfg.Resolve("explore")
	testassert.NoError(t, err)
	testassert.Require(t, explore.Prompt != "custom prompt" && explore.Description != "custom" && explore.Subagent, "builtin was overridden by colliding user entry: %+v", explore)
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func namesOf(values []Resolved) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = value.Name
	}
	return out
}

// TestParseCapabilityAndToolSelection pins the definitions' own selection
// lists: order, explicit empty clearing, duplicate and invalid-shape drops,
// and tool names retained as configured, duplicates included.
func TestParseCapabilityAndToolSelection(t *testing.T) {
	cfg, err := parse([]byte(`{
  "plan": {"capabilities": ["cap-b", "cap-a"], "tools": ["own_tool"]},
  "sweeper": {"capabilities": []},
  "plain": {},
  "duplicate_cap": {"capabilities": ["cap-a", "cap-a"]},
  "broken": {"capabilities": "not-a-list"},
  "custom_tools": {"tools": ["own_tool", "own_tool", "read_file"]},
  "omitted": {},
  "empty": {"tools": []}
}`))
	testassert.NoError(t, err)
	for _, drop := range []string{"duplicate_cap", "broken"} {
		_, err := cfg.Resolve(drop)
		testassert.Require(t, err != nil, "Resolve(%s) succeeded, want dropped", drop)
	}
	warnings := cfg.Warnings()
	testassert.Require(t, len(warnings) == 2, "warnings = %#v, want two invalid_agent_type drops", warnings)
	for _, warning := range warnings {
		testassert.Equal(t, warning.Kind, "invalid_agent_type")
	}
	plan, err := cfg.Resolve("plan")
	testassert.NoError(t, err)
	testassert.Require(t, reflect.DeepEqual(plan.Capabilities, []string{"cap-b", "cap-a"}), "plan capabilities = %q, want the selected order", plan.Capabilities)
	testassert.Require(t, reflect.DeepEqual(plan.Tools, []string{"own_tool"}), "plan tools = %q, want the configured selection", plan.Tools)
	for _, name := range []string{"sweeper", "plain", "secondary"} {
		resolved, err := cfg.Resolve(name)
		testassert.Require(t, err == nil, "Resolve(%s): %v", name, err)
		testassert.Require(t, resolved.Capabilities == nil, "%s capabilities = %q, want absent, cleared, or inherited-empty", name, resolved.Capabilities)
	}
	worker, err := cfg.Resolve("custom_tools")
	testassert.NoError(t, err)
	testassert.Require(t, reflect.DeepEqual(worker.Tools, []string{"own_tool", "own_tool", "read_file"}), "custom_tools = %q, want the configured names retained", worker.Tools)
	testassert.Require(t, cfg.defs["omitted"].Tools == nil, "omitted definition tools = %v, want nil", cfg.defs["omitted"].Tools)
	emptyDef := cfg.defs["empty"]
	testassert.Require(t, emptyDef.Tools != nil && len(*emptyDef.Tools) == 0, "empty definition tools = %#v, want an explicit empty list", emptyDef.Tools)
	primary, err := cfg.Resolve("primary")
	testassert.NoError(t, err)
	testassert.Require(t, reflect.DeepEqual(primary.Tools, StandardTools), "primary default tools = %q, want the standard roster", primary.Tools)
}

func TestParseRejectsEmptyToolNames(t *testing.T) {
	cfg, err := parse([]byte(`{"bad": {"tools": [""]}}`))
	testassert.NoError(t, err)
	_, err = cfg.Resolve("bad")
	testassert.Require(t, err != nil, "Resolve(bad) succeeded, want dropped for an empty tool name")
	warnings := cfg.Warnings()
	testassert.Require(t, len(warnings) == 1 && warnings[0].Kind == "invalid_agent_type", "warnings = %#v, want one invalid_agent_type drop", warnings)
	testassert.Equal(t, warnings[0].Message, "tool name is empty")
}
