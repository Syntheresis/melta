package adaptation_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/adaptation"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// bundledGPTGuidanceLine and bundledGPTGuidanceReplacement are the complete
// literal standard run_command guidance line the bundled GPT rows replace and
// its GPT form.
const bundledGPTGuidanceLine = "- Do not use this tool to read file contents — use read_file. Do not use this tool to edit files — use edit_file or write_file."
const bundledGPTGuidanceReplacement = "- Do not use this tool to read file contents — use read_file. Do not use this tool to edit files — use apply_patch."

// fixtureCatalog is the ordered advertisement the tests shape: the file tools
// the GPT rows hide, plus the standard command and an unrelated tool whose
// prose merely names the same phrase.
func fixtureCatalog() []model.ToolDefinition {
	return []model.ToolDefinition{
		{Name: "read_file", Description: "reads one file"},
		{Name: "write_file", Description: "writes one file"},
		{Name: "edit_file", Description: "edits one file"},
		{Name: "apply_patch", Description: "applies one patch"},
		{Name: "run_command", Description: bundledGPTGuidanceLine},
		{Name: "note_editor", Description: "Edit edit_file or write_file via one call."},
	}
}

func fixtureToolNames() []string {
	return toolNames(fixtureCatalog())
}

func toolNames(tools []model.ToolDefinition) []string {
	if tools == nil {
		return nil
	}
	out := make([]string, 0, len(tools))
	for _, definition := range tools {
		out = append(out, definition.Name)
	}
	return out
}

// changedDescriptions returns the descriptions that differ from the fixture.
func changedDescriptions(tools []model.ToolDefinition) map[string]string {
	original := make(map[string]string)
	for _, definition := range fixtureCatalog() {
		original[definition.Name] = definition.Description
	}
	var out map[string]string
	for _, definition := range tools {
		if definition.Description != original[definition.Name] {
			if out == nil {
				out = make(map[string]string)
			}
			out[definition.Name] = definition.Description
		}
	}
	return out
}

type hookAPI struct {
	extension.API
	settings json.RawMessage
	hook     extension.AuthorRequestHook
}

func (a *hookAPI) Settings() json.RawMessage { return a.settings }
func (a *hookAPI) RegisterTool(_ model.ToolDefinition, _ extension.RegisteredTool, _ extension.Registration) (extension.Registration, error) {
	return nil, nil
}
func (a *hookAPI) AttachRequest(h extension.AuthorRequestHook, _ extension.Registration) (extension.Registration, error) {
	a.hook = h
	return nil, nil
}

func activate(t *testing.T, section string) (extension.AuthorRequestHook, error) {
	t.Helper()
	api := &hookAPI{settings: json.RawMessage(section)}
	_, err := adaptation.Author().Activate(context.Background(), api)
	return api.hook, err
}

func shape(t *testing.T, section string, ref model.ModelRef, assembled bool, tools []model.ToolDefinition) harness.RequestConsequence {
	t.Helper()
	hook, err := activate(t, section)
	testassert.NoError(t, err)
	consequence, err := hook(context.Background(), extension.AuthorContext{}, extension.AuthorRequestInput{
		RequestHookInput: harness.RequestHookInput{
			Purpose: harness.InvocationPurposeConversation,
			Model:   ref,
			Prompt:  "BASE_PROMPT",
			Catalog: fixtureCatalog(),
			Tools:   tools,
		},
		PromptAssembled: assembled,
	})
	testassert.NoError(t, err)
	return consequence
}

func shapeSection(t *testing.T, section string, ref model.ModelRef, assembled bool) harness.RequestConsequence {
	t.Helper()
	return shape(t, section, ref, assembled, fixtureCatalog())
}

func shapeBundled(t *testing.T, ref model.ModelRef, assembled bool) harness.RequestConsequence {
	t.Helper()
	return shapeSection(t, `{"enabled":true}`, ref, assembled)
}

func TestConfiguredSchemaValidatesConsumedTypes(t *testing.T) {
	manifest := adaptation.Author().Manifest
	for _, section := range []string{
		`{"enabled":true}`,
		`{"enabled":true,"entries":[]}`,
		`{"enabled":true,"entries":[{"pattern":""}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","name":"unconsumed","unknown":1}]}`,
		`{"enabled":true,"entries":[{"pattern":"model-*","prompt":"","append":["a",""],"exclude_tools":[],"tool_description_replacements":{},"offer_patch":true}],"unclaimed":true}`,
	} {
		err := extension.ValidateManifestSection(manifest, json.RawMessage(section))
		testassert.Check(t, err == nil, "ValidateManifestSection(%s) = %v, want valid", section, err)
	}
	for _, section := range []string{
		`{"entries":[]}`,
		`{"enabled":true,"entries":{}}`,
		`{"enabled":true,"entries":[{}]}`,
		`{"enabled":true,"entries":[{"pattern":1}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","prompt":1}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","append":"a"}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","exclude_tools":[1]}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","tool_description_replacements":{"k":1}}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","tool_description_replacements":{"":"v"}}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","offer_patch":1}]}`,
	} {
		err := extension.ValidateManifestSection(manifest, json.RawMessage(section))
		testassert.Check(t, err != nil, "ValidateManifestSection(%s) accepted an invalid consumed shape", section)
	}
}

func TestRequestAppliesShippedBundledRows(t *testing.T) {
	got := shapeBundled(t, model.ModelRef{Provider: "openai", Model: "gpt-5.5-mini"}, true)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "apply_patch", "run_command", "note_editor"}), "advertised tools = %q, want the excluded file tools absent in incoming order", toolNames(got.Tools))
	testassert.Require(t, reflect.DeepEqual(changedDescriptions(got.Tools), map[string]string{"run_command": bundledGPTGuidanceReplacement}), "descriptions = %q, want the one literal guidance-line rule", changedDescriptions(got.Tools))
	testassert.Require(t, got.Prompt != nil && strings.HasPrefix(*got.Prompt, "BASE_PROMPT\n\n") && strings.Contains(*got.Prompt, "Use more tool calls when they can improve the response"), "prompt = %v, want the baseline with the shipped guidance paragraph appended", got.Prompt)

	// The grok and gemini rows carry append paragraphs with no tool levers;
	// a match still states the complete advertisement.
	for _, ref := range []model.ModelRef{{Provider: "xai", Model: "grok-build-0.1"}, {Provider: "google", Model: "gemini-3-pro"}} {
		got := shapeBundled(t, ref, true)
		testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}) && changedDescriptions(got.Tools) == nil && got.Prompt != nil && strings.HasPrefix(*got.Prompt, "BASE_PROMPT\n\n"), "%s treatment = %+v, want append paragraphs and the incoming tools without offer_patch", ref.String(), got)
	}
}

func TestRequestBundledGuidanceOnlyOnAssembledPrompt(t *testing.T) {
	ref := model.ModelRef{Provider: "openai", Model: "gpt-5.5"}
	assembled := shapeBundled(t, ref, true)
	testassert.Require(t, assembled.Prompt != nil && *assembled.Prompt != "BASE_PROMPT", "assembled bundled treatment = %+v, want the guidance append", assembled)
	complete := shapeBundled(t, ref, false)
	testassert.Require(t, complete.Prompt == nil, "complete-prompt bundled treatment = %+v, want no automatic bundled guidance", complete)
	testassert.Require(t, reflect.DeepEqual(complete.Tools, assembled.Tools), "complete-prompt bundled tools = %+v, want the same exclusions and replacements", complete.Tools)
}

func TestConfiguredEntriesPrecedeBundledOnEveryPrompt(t *testing.T) {
	section := `{"enabled":true,"entries":[{"pattern":"*","prompt":"CONFIGURED","append":["USER_TAIL"]}]}`
	for _, assembled := range []bool{true, false} {
		// The user catch-all matches the bundled GPT id first, so neither its
		// exclusions nor its assembled-only guidance apply.
		got := shapeSection(t, section, model.ModelRef{Provider: "openai", Model: "gpt-5.5"}, assembled)
		testassert.Require(t, got.Prompt != nil && *got.Prompt == "CONFIGURED\n\nUSER_TAIL", "assembled=%v configured treatment = %+v, want the complete replacement followed by the explicit append", assembled, got)
		testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}) && changedDescriptions(got.Tools) == nil, "assembled=%v configured treatment = %+v, want the unexcluded advertisement without apply_patch and no bundled levers", assembled, got)
	}

	// A present empty prompt replacement replaces the text with empty; the
	// explicit append still follows it.
	got := shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","prompt":"","append":["TAIL"]}]}`, model.ModelRef{Provider: "openai", Model: "gpt-5.5"}, false)
	testassert.Require(t, got.Prompt != nil && *got.Prompt == "TAIL", "empty replacement treatment = %+v, want the append over empty text", got)

	// Configured entries are ordered with the first match deciding.
	got = shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","prompt":"FIRST"},{"pattern":"*","prompt":"SECOND"}]}`, model.ModelRef{Provider: "openai", Model: "gpt-5.5"}, true)
	testassert.Require(t, got.Prompt != nil && *got.Prompt == "FIRST", "ordered configured treatment = %+v, want the first user match", got)
}

func TestConfiguredEmptyAndCatchAllPatternsMatch(t *testing.T) {
	// An empty pattern is a literal empty id, not a wildcard.
	section := `{"enabled":true,"entries":[{"pattern":"","prompt":"EMPTY_ID"}]}`
	got := shapeSection(t, section, model.ModelRef{}, true)
	testassert.Require(t, got.Prompt != nil && *got.Prompt == "EMPTY_ID", "empty-pattern treatment = %+v, want the configured replacement", got)
	got = shapeSection(t, section, model.ModelRef{Provider: "prov", Model: "claude-x"}, true)
	testassert.Require(t, got.Prompt == nil && reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "nonempty-id treatment = %+v, want baseline prompt and the incoming tools without apply_patch", got)

	got = shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","prompt":"ANY"}]}`, model.ModelRef{Provider: "prov", Model: "claude-x"}, true)
	testassert.Require(t, got.Prompt != nil && *got.Prompt == "ANY", "catch-all treatment = %+v, want the configured replacement", got)

	// Unknown extension-owned members are uninterpreted.
	got = shapeSection(t, `{"enabled":true,"unclaimed":1,"entries":[{"pattern":"*","prompt":"UNKNOWN","ignored":["x"]}]}`, model.ModelRef{Provider: "prov", Model: "claude-x"}, true)
	testassert.Require(t, got.Prompt != nil && *got.Prompt == "UNKNOWN", "unknown-member treatment = %+v, want the consumed fields only", got)
}

// TestConfiguredDecodeUsesExactCaseJSONNames proves the extension consumes
// only the schema-recognized exact-case member names.
func TestConfiguredDecodeUsesExactCaseJSONNames(t *testing.T) {
	section := `{"enabled":true,
		"entries":[{"pattern":"*","prompt":"LOWER","append":["LOWER_APPEND"],"exclude_tools":["write_file"],"tool_description_replacements":{"reads":"LOWER_READ"}}],
		"Entries":[{"pattern":"*","prompt":"UPPER","append":["UPPER_APPEND"],"exclude_tools":["read_file"],"tool_description_replacements":{"writes":"UPPER_WRITE"}}]}`
	got := shapeSection(t, section, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, got.Prompt != nil && *got.Prompt == "LOWER\n\nLOWER_APPEND", "cased section prompt = %v, want the lowercase entry only", got.Prompt)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "edit_file", "run_command", "note_editor"}), "cased section tools = %q, want the lowercase exclusions only and no apply_patch without offer_patch", toolNames(got.Tools))
	testassert.Require(t, reflect.DeepEqual(changedDescriptions(got.Tools), map[string]string{"read_file": "LOWER_READ one file"}), "cased section descriptions = %q, want the lowercase replacement key only", changedDescriptions(got.Tools))

	miscased := `{"entries":[{"pattern":"*","Prompt":"UPPER","Append":["UPPER"],"Exclude_Tools":["read_file"],"Tool_Description_Replacements":{"writes":"UPPER"}}]}`
	got = shapeSection(t, miscased, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, got.Prompt == nil && changedDescriptions(got.Tools) == nil && reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "miscased entry treatment = %+v, want no consumed configured fields", got)

	got = shapeSection(t, `{"entries_extra":[],"Entries":[{"pattern":"*","prompt":"UPPER"}]}`, model.ModelRef{Provider: "anthropic", Model: "claude-x"}, true)
	testassert.Require(t, got.Prompt == nil && reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "uppercase entries treatment = %+v, want baseline prompt and the incoming tools without apply_patch", got)

	unknownInvalid := `{"Entries":1,"ENTRIES":{},"entries":[{"pattern":"*","Prompt":1,"Append":{},"Exclude_Tools":"bad","Tool_Description_Replacements":[1]}]}`
	got = shapeSection(t, unknownInvalid, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, got.Prompt == nil && changedDescriptions(got.Tools) == nil && reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "unknown invalid fields treatment = %+v, want the matching entry with no configured levers", got)

	got = shapeSection(t, `{"entries":[{"pattern":"*","tool_description_replacements":{"READS":"UPPER"}}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, changedDescriptions(got.Tools) == nil, "literal replacement keys = %q, want no case-folded match against reads one file", changedDescriptions(got.Tools))
}

func TestConfiguredAdvertisementIsIncomingMinusExclusions(t *testing.T) {
	got := shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","exclude_tools":["write_file","edit_file"]}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "run_command", "note_editor"}), "configured view = %q, want the incoming tools minus its exclusions and apply_patch", toolNames(got.Tools))

	got = shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","exclude_tools":["read_file","write_file","edit_file","apply_patch","run_command","note_editor"]}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, got.Tools != nil && len(got.Tools) == 0, "all-excluded view = %#v, want a present empty advertisement", got.Tools)

	got = shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","exclude_tools":[]}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "unexcluded view = %q, want exactly the incoming tools without apply_patch", toolNames(got.Tools))

	// A match selects from the tools earlier hooks left advertised, in their
	// order, never restoring a catalog member.
	incoming := []model.ToolDefinition{
		{Name: "note_editor", Description: "Edit edit_file or write_file via one call."},
		{Name: "run_command", Description: bundledGPTGuidanceLine},
		{Name: "apply_patch", Description: "applies one patch"},
	}
	got = shape(t, `{"enabled":true,"entries":[{"pattern":"*","prompt":"SUBSET"}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true, incoming)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"note_editor", "run_command"}), "prompt-only view = %q, want the incoming subset in its order without apply_patch", toolNames(got.Tools))
	got = shape(t, `{"enabled":true,"entries":[{"pattern":"*","exclude_tools":["read_file","run_command"]}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true, incoming)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"note_editor"}), "exclusion view = %q, want the incoming order minus the exclusion and apply_patch, the absent name not restored", toolNames(got.Tools))
	got = shape(t, `{"enabled":true,"entries":[{"pattern":"*","prompt":"NONE_VISIBLE"}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true, []model.ToolDefinition{})
	testassert.Require(t, got.Tools != nil && len(got.Tools) == 0, "empty incoming view = %#v, want a present empty advertisement", got.Tools)
}

func TestConfiguredDescriptionReplacementTransformsOnce(t *testing.T) {
	got := shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","tool_description_replacements":{"edit_file":"apply_patch","apply_patch":"PATCH"}}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true)
	want := map[string]string{
		"note_editor": "Edit apply_patch or write_file via one call.",
		"run_command": "- Do not use this tool to read file contents — use read_file. Do not use this tool to edit files — use apply_patch or write_file.",
	}
	testassert.Require(t, reflect.DeepEqual(changedDescriptions(got.Tools), want), "description replacements = %q, want only the tools whose text contains a key, each transformed once", changedDescriptions(got.Tools))
}

func TestRequestOfferPatchKeepsOnlyIncomingVisibility(t *testing.T) {
	got := shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*","offer_patch":true}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), fixtureToolNames()), "offer_patch view = %q, want the complete incoming advertisement including apply_patch", toolNames(got.Tools))

	got = shapeSection(t, `{"enabled":true,"entries":[{"pattern":"*"}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "row without offer_patch view = %q, want apply_patch removed", toolNames(got.Tools))

	admitted := []model.ToolDefinition{
		{Name: "read_file", Description: "reads one file"},
		{Name: "run_command", Description: bundledGPTGuidanceLine},
	}
	got = shape(t, `{"enabled":true,"entries":[{"pattern":"*","offer_patch":true,"tool_description_replacements":{"reads":"READ"}}]}`, model.ModelRef{Provider: "prov", Model: "any"}, true, admitted)
	testassert.Require(t, reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "run_command"}), "admission-excluded apply_patch view = %q, want it never restored", toolNames(got.Tools))
}

func TestRequestUnmatchedModelIsBaseline(t *testing.T) {
	got := shapeBundled(t, model.ModelRef{Provider: "anthropic", Model: "claude-x"}, true)
	testassert.Require(t, got.Prompt == nil && reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "unmatched model treatment = %+v, want the baseline prompt and the incoming tools without apply_patch", got)
}

func TestRequestMatchesTheModelComponentOnly(t *testing.T) {
	got := shapeBundled(t, model.ModelRef{Provider: "gpt-5", Model: "unrelated"}, true)
	testassert.Require(t, got.Prompt == nil && reflect.DeepEqual(toolNames(got.Tools), []string{"read_file", "write_file", "edit_file", "run_command", "note_editor"}), "provider-shaped ref treatment = %+v, want the baseline prompt and the incoming tools without apply_patch", got)
	got = shapeBundled(t, model.ModelRef{Provider: "openai", Model: "GPT-5.4"}, true)
	testassert.Require(t, len(got.Tools) == 4, "case-insensitive row did not match: %+v", got)
}

func TestRequestReturnsIndependentContainers(t *testing.T) {
	section := `{"enabled":true,"entries":[{"pattern":"*","append":["USER"],"exclude_tools":["write_file"],"tool_description_replacements":{"reads":"READ"}}]}`
	first := shapeSection(t, section, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, first.Prompt != nil && len(first.Tools) != 0, "configured row did not resolve: %+v", first)
	*first.Prompt = "tampered"
	first.Tools[0].Description = "tampered"

	second := shapeSection(t, section, model.ModelRef{Provider: "prov", Model: "any"}, true)
	testassert.Require(t, strings.HasPrefix(*second.Prompt, "BASE_PROMPT\n\nUSER") && second.Tools[0].Name == "read_file" && second.Tools[0].Description == "READ one file", "a mutated result leaked into the next request: %+v", second)
}

// TestActivationRejectsRecognizedWrongTypeMembers keeps the exact lowercase
// consumed names typed: a wrong type under a recognized name is rejected even
// though unknown/miscased names are ignored.
func TestActivationRejectsRecognizedWrongTypeMembers(t *testing.T) {
	for _, section := range []string{
		`{"enabled":true,"entries":{}}`,
		`{"enabled":true,"entries":[{"pattern":1}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","prompt":1}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","append":"a"}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","exclude_tools":"a"}]}`,
		`{"enabled":true,"entries":[{"pattern":"*","tool_description_replacements":{"k":1}}]}`,
	} {
		_, err := activate(t, section)
		testassert.Require(t, err != nil, "Activate accepted recognized wrong-type section %s", section)
	}
}
