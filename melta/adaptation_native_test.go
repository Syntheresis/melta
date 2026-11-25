package melta

// The shipped Adaptation extension's final configuration and request surface
// through the real owner: configured entries precede bundled rows, hooks run
// in attachment order (a later replacement sees the earlier contribution),
// and a reload's prior capture keeps its entries while the next capture sees
// the new ones.

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/adaptation"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

const adaptationRuntimeAgents = `{
  "visible": {"model": "prov/adaptive", "system_prompt": "full", "tools": ["read_file", "write_file", "apply_patch"]},
  "modefull": {"model": "prov/adaptive", "system_prompt": "full", "tools": ["read_file"]},
  "modesimple": {"model": "prov/adaptive", "system_prompt": "simple", "tools": ["read_file"]},
  "modenone": {"model": "prov/adaptive", "system_prompt": "none", "tools": ["read_file"]},
  "bundledfull": {"model": "prov/gpt-5.5", "system_prompt": "full", "tools": ["read_file", "write_file", "edit_file", "apply_patch"]},
  "bundlednone": {"model": "prov/gpt-5.5", "system_prompt": "none", "tools": ["read_file", "write_file", "edit_file", "apply_patch"]},
  "readonly": {"model": "prov/adaptive", "system_prompt": "full", "tools": ["read_file", "write_file"], "readonly": true},
  "plainpatch": {"model": "prov/adaptive", "system_prompt": "simple", "tools": ["read_file", "apply_patch"]},
  "gptexcluded": {"model": "prov/gpt-5.5", "system_prompt": "simple", "tools": ["read_file"]},
  "reload": {"model": "prov/adaptive", "system_prompt": "full", "tools": ["read_file"]}
}`

const adaptationRuntimeConfig = `{"providers":{"prov":` + `{"transport":{"base_url":"` + "%s" + `","api_key_env":""},"discovery":false,"models":{"adaptive":{"name":"A","context_window":262144},"gpt-5.5":{"name":"G","context_window":262144}}}` + `},"extensions":{"adaptation":{"enabled":true,"entries":` + "%s" + `}}}`

func adaptationConfigDocument(endpoint, entries string) string {
	return strings.Replace(strings.Replace(adaptationRuntimeConfig, "%s", endpoint, 1), "%s", entries, 1)
}

func writeAdaptationSection(t *testing.T, configPath, section string) {
	t.Helper()
	data, err := os.ReadFile(configPath)
	testassert.NoError(t, err)
	var root map[string]json.RawMessage
	testassert.NoError(t, json.Unmarshal(data, &root))
	var extensions map[string]json.RawMessage
	testassert.NoError(t, json.Unmarshal(root["extensions"], &extensions))
	extensions["adaptation"] = json.RawMessage(section)
	root["extensions"], err = json.Marshal(extensions)
	testassert.NoError(t, err)
	data, err = json.Marshal(root)
	testassert.NoError(t, err)
	writeServiceFile(t, configPath, string(data))
}

func writeAdaptationEntries(t *testing.T, configPath, entries string) {
	t.Helper()
	writeAdaptationSection(t, configPath, `{"enabled":true,"entries":`+entries+`}`)
}

// shRequestMarker registers one pure request hook that appends a marker to
// the effective prompt, so attachment order is observable on the wire.
func shRequestMarker(id, marker string) extension.Extension {
	return testNative(id, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachRequest(func(_ context.Context, _ extension.AuthorContext, input extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			prompt := input.Prompt + "|" + marker
			return harness.RequestConsequence{Prompt: &prompt}, nil
		}, nil)
		return nil, err
	})
}

// newAdaptationRuntime opens the real owner over the fixture's catalog,
// agents and configured entries, with the shipped adaptation native beside
// the suite's prompt marker and any extra plugin.
func newAdaptationRuntime(t *testing.T, entries string, extra ...extension.Extension) (*productionEnv, *Melta) {
	t.Helper()
	e := newProductionEnv(t, storage.NewMemory())
	return e, openAdaptationRuntime(t, e, entries, append([]extension.Extension{agentsTestNative(), shRequestMarker("marker", "hooked")}, extra...)...)
}

// openAdaptationRuntime opens one fixture environment's catalog, agents and
// configured entries with the caller-ordered plugin set; the shipped
// adaptation native is appended last unless the caller already placed it.
func openAdaptationRuntime(t *testing.T, e *productionEnv, entries string, plugins ...extension.Extension) *Melta {
	t.Helper()
	writeServiceFile(t, e.configPath, adaptationConfigDocument(e.server.URL, entries))
	writeAgentDefinitions(t, e.configPath, adaptationRuntimeAgents)
	e.server.setScript(func(context.Context, string) []string { return textTurnEvents("done") })
	order := append([]extension.Extension(nil), plugins...)
	placed := false
	for _, plugin := range order {
		placed = placed || plugin.Manifest.ID == "adaptation"
	}
	if !placed {
		order = append(order, adaptation.Author())
	}
	r, err := e.openPlugins(context.Background(), order...)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

func adaptationWorkspace(t *testing.T, e *productionEnv, name string) string {
	t.Helper()
	workspace := e.workspace(name)
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))
	return workspace
}

func namedAdaptationOperation(t *testing.T, r *Melta, workspace, agent, operation string) harness.OperationRecord {
	t.Helper()
	session, err := r.createSession(context.Background(), workspace, agent)
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, operation, "hello")
	return awaitOperation(t, r, session.Identity.SessionID, operation, harness.OperationSuccess)
}

func catalogTool(t *testing.T, record harness.OperationRecord, name string) model.ToolDefinition {
	t.Helper()
	for _, definition := range record.Admission.Execution.Tools {
		if definition.Name == name {
			return definition
		}
	}
	t.Fatalf("captured catalog has no tool %q: %+v", name, record.Admission.Execution.Tools)
	return model.ToolDefinition{}
}

// assertAdaptationWire proves one recorded model request's effective system
// prompt and advertisement equal the explicit expected values: request-local
// contributions reach the wire, and the captured initial advertisement keeps
// the admitted selection.
func assertAdaptationWire(t *testing.T, doc wireChatBody, record harness.OperationRecord, wantPrompt string, wantTools []string, wantDescription string) {
	t.Helper()
	var system string
	found := false
	for _, message := range doc.Messages {
		if message.Role == "system" {
			testassert.NoError(t, json.Unmarshal(message.Content, &system))
			found = true
			break
		}
	}
	testassert.Require(t, found, "wire request has no system message: %+v", doc.Messages)
	testassert.Equal(t, system, wantPrompt)
	testassert.Require(t, slices.Equal(wireToolNames(doc), wantTools), "wire tools = %v, want %v", wireToolNames(doc), wantTools)
	testassert.Require(t, len(doc.Tools) == len(wantTools), "wire carries %d tools, want %d", len(doc.Tools), len(wantTools))
	if wantDescription != "" {
		var hit bool
		for _, tool := range doc.Tools {
			hit = hit || strings.Contains(tool.Function.Description, wantDescription)
		}
		testassert.Require(t, hit, "wire tool descriptions = %+v, want one containing %q", doc.Tools, wantDescription)
	}
}

// wireSystemPrompt returns the system message text of one recorded request.
func wireSystemPrompt(t *testing.T, doc wireChatBody) string {
	t.Helper()
	for _, message := range doc.Messages {
		if message.Role == "system" {
			var system string
			testassert.NoError(t, json.Unmarshal(message.Content, &system))
			return system
		}
	}
	t.Fatalf("wire request has no system message: %+v", doc.Messages)
	return ""
}

// TestAdaptationNativeToolSelectionFollowsPrepareVisibility proves a matched
// rule selects from the tools the preceding admission hook left visible,
// never from the complete eligible catalog the rule also receives.
func TestAdaptationNativeToolSelectionFollowsPrepareVisibility(t *testing.T) {
	hider := func() *recordHook {
		return &recordHook{name: "hook.first", events: &traceLog{}, admissionMutate: func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
			visible := make([]model.ToolDefinition, 0, len(in.Candidate.Tools))
			for _, definition := range in.Candidate.Tools {
				if definition.Name != "write_file" {
					visible = append(visible, definition)
				}
			}
			candidate := in.Candidate
			candidate.Tools = visible
			return extension.AdmissionResult{Candidate: &candidate}, nil
		}}
	}
	for _, row := range []struct {
		name    string
		entries string
		want    []string
	}{
		{"prompt only", `[{"pattern":"*adaptive*","prompt":"ADAPTED"}]`, []string{"read_file"}},
		{"exclusion", `[{"pattern":"*adaptive*","exclude_tools":["apply_patch"]}]`, []string{"read_file"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			e := newProductionEnv(t, storage.NewMemory())
			r := openAdaptationRuntime(t, e, row.entries, agentsTestNative(), hookPlugin(hider()), parkPlugin(&parkingHook{}))
			workspace := adaptationWorkspace(t, e, "adapt-visible")
			record := namedAdaptationOperation(t, r, workspace, "visible", "op-visible")
			testassert.Require(t, slices.Equal(record.Admission.Execution.InitialToolNames, []string{"read_file", "apply_patch"}), "advertisement = %v, want the hook-visible tools", record.Admission.Execution.InitialToolNames)
			testassert.Require(t, slices.Equal(wireToolNames(decodeWireChatBody(t, e.server.bodyAt(0))), row.want), "wire tools = %v, want %v", wireToolNames(decodeWireChatBody(t, e.server.bodyAt(0))), row.want)
		})
	}
}

func TestAdaptationNativeConfiguredEntriesThroughRuntime(t *testing.T) {
	const entries = `[{"pattern":"*adaptive*","prompt":"CONFIGURED","append":["TAIL"],"exclude_tools":["write_file"],"tool_description_replacements":{"edit_file":"apply_patch","apply_patch":"PATCH"},"offer_patch":true}]`
	ctx := context.Background()

	// Attachment order decides: the hook before adaptation is overwritten,
	// the hook after it sees the configured prompt and appends its own marker.
	e := newProductionEnv(t, storage.NewMemory())
	orderedRuntime := openAdaptationRuntime(t, e, entries, agentsTestNative(), shRequestMarker("marker", "hooked"))
	workspace := adaptationWorkspace(t, e, "adapt")
	before := e.server.requests()
	ordered := namedAdaptationOperation(t, orderedRuntime, workspace, "visible", "op-ordered")
	assertAdaptationWire(t, decodeWireChatBody(t, e.server.bodyAt(before)), ordered, "CONFIGURED\n\nTAIL", []string{"read_file", "apply_patch"}, "using the PATCH (V4A) patch format")
	{
		testassert.Require(t, slices.Equal(ordered.Admission.Execution.InitialToolNames, []string{"read_file", "write_file", "apply_patch"}), "ordered advertisement = %v, want the admitted selection", ordered.Admission.Execution.InitialToolNames)
		writeDescription := catalogTool(t, ordered, "write_file").Description
		testassert.Require(t, !strings.Contains(writeDescription, "use PATCH instead."), "write_file description = %q, want the request-local replacement never durable", writeDescription)
		{
			var wireWriteDescription string
			for _, tool := range decodeWireChatBody(t, e.server.bodyAt(before)).Tools {
				if tool.Function.Name == "write_file" {
					wireWriteDescription = tool.Function.Description
				}
			}
			testassert.Require(t, wireWriteDescription == "", "wire carries write_file, want the configured exclusion")
		}
	}

	eReversed := newProductionEnv(t, storage.NewMemory())
	reversedRuntime := openAdaptationRuntime(t, eReversed, entries, agentsTestNative(), adaptation.Author(), shRequestMarker("marker", "hooked"))
	reversedWorkspace := adaptationWorkspace(t, eReversed, "adapt-reversed")
	beforeReversed := eReversed.server.requests()
	reversed := namedAdaptationOperation(t, reversedRuntime, reversedWorkspace, "visible", "op-reversed")
	assertAdaptationWire(t, decodeWireChatBody(t, eReversed.server.bodyAt(beforeReversed)), reversed, "CONFIGURED\n\nTAIL|hooked", []string{"read_file", "apply_patch"}, "using the PATCH (V4A) patch format")

	// A configured prompt and its explicit append apply in every assembly
	// mode; equal text takes the same later transformation.
	for _, row := range []struct{ agent, operation string }{
		{"modefull", "op-mode-full"},
		{"modesimple", "op-mode-simple"},
		{"modenone", "op-mode-none"},
	} {
		beforeMode := e.server.requests()
		got := namedAdaptationOperation(t, orderedRuntime, workspace, row.agent, row.operation)
		assertAdaptationWire(t, decodeWireChatBody(t, e.server.bodyAt(beforeMode)), got, "CONFIGURED\n\nTAIL", []string{"read_file"}, "")
	}

	// Bundled guidance stays a full-mode append only.
	beforeFull := e.server.requests()
	namedAdaptationOperation(t, orderedRuntime, workspace, "bundledfull", "op-bundled-full")
	testassert.Require(t, strings.Contains(wireSystemPrompt(t, decodeWireChatBody(t, e.server.bodyAt(beforeFull))), "Use more tool calls when they can improve the response"), "bundled full wire prompt, want the guidance append")
	beforeNone := e.server.requests()
	namedAdaptationOperation(t, orderedRuntime, workspace, "bundlednone", "op-bundled-none")
	testassert.Require(t, !strings.Contains(wireSystemPrompt(t, decodeWireChatBody(t, e.server.bodyAt(beforeNone))), "Use more tool calls when they can improve the response"), "bundled none wire prompt, want no automatic guidance")
	testassert.Require(t, slices.Equal(wireToolNames(decodeWireChatBody(t, e.server.bodyAt(beforeNone))), []string{"read_file", "apply_patch"}) && slices.Equal(wireToolNames(decodeWireChatBody(t, e.server.bodyAt(beforeFull))), []string{"read_file", "apply_patch"}), "bundled advertisements = %v / %v, want the bundled exclusions in both modes", wireToolNames(decodeWireChatBody(t, e.server.bodyAt(beforeNone))), wireToolNames(decodeWireChatBody(t, e.server.bodyAt(beforeFull))))

	// Constraint-ineligible tools stay unavailable: the configured catch-all
	// cannot recover the Agents-readonly-excluded write tool.
	beforeReadonly := e.server.requests()
	readonly := namedAdaptationOperation(t, orderedRuntime, workspace, "readonly", "op-readonly")
	assertAdaptationWire(t, decodeWireChatBody(t, e.server.bodyAt(beforeReadonly)), readonly, "CONFIGURED\n\nTAIL", []string{"read_file"}, "")
	testassert.Require(t, slices.Equal(readonly.Admission.Execution.InitialToolNames, []string{"read_file"}), "readonly advertisement = %v, want the readable tool only", readonly.Admission.Execution.InitialToolNames)
	testassert.NoError(t, orderedRuntime.Close(ctx))
	testassert.NoError(t, reversedRuntime.Close(ctx))
}

// TestAdaptationPatchAdvertisement proves apply_patch reaches the wire only
// when the matched row offers it and the incoming admission still contains
// it; absence is never inferred from description text.
func TestAdaptationPatchAdvertisement(t *testing.T) {
	for _, row := range []struct {
		name    string
		entries string
		agent   string
		want    []string
	}{
		{"unmatched model", `[]`, "visible", []string{"read_file", "write_file"}},
		{"row without offer_patch", `[{"pattern":"*adaptive*"}]`, "plainpatch", []string{"read_file"}},
		{"row with offer_patch", `[{"pattern":"*adaptive*","offer_patch":true}]`, "plainpatch", []string{"read_file", "apply_patch"}},
		{"bundled offer with admission exclusion", `[]`, "gptexcluded", []string{"read_file"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			e := newProductionEnv(t, storage.NewMemory())
			r := openAdaptationRuntime(t, e, row.entries, agentsTestNative())
			workspace := adaptationWorkspace(t, e, "patch-advertisement")
			namedAdaptationOperation(t, r, workspace, row.agent, "op-patch-advertisement")
			wire := decodeWireChatBody(t, e.server.bodyAt(0))
			testassert.Require(t, slices.Equal(wireToolNames(wire), row.want), "wire tools = %v, want %v", wireToolNames(wire), row.want)
			testassert.Require(t, slices.Contains(wireToolNames(wire), "apply_patch") == slices.Contains(row.want, "apply_patch"), "description text inferred apply_patch availability: %+v", wire.Tools)
			testassert.NoError(t, r.Close(context.Background()))
		})
	}
}

func TestAdaptationReloadPriorCaptureKeepsEntries(t *testing.T) {
	const entries = `[{"pattern":"*adaptive*","prompt":"FIRST"}]`
	park := &parkingHook{}
	e, r := newAdaptationRuntime(t, entries, parkPlugin(park))
	ctx := context.Background()
	workspace := adaptationWorkspace(t, e, "adapt-reload")

	// One in-flight preparation keeps revision 1's configured prompt while
	// the reload publishes revision 2 for the next capture.
	session, err := r.createSession(ctx, workspace, "reload")
	testassert.NoError(t, err)
	release, arrived := make(chan struct{}), make(chan struct{}, 1)
	park.arm(release, arrived)
	done := make(chan error, 1)
	go func() {
		_, submitErr := submitRawThroughRuntime(ctx, r, session.Identity.SessionID, "op-first", "hello", harness.MessageModeRegular)
		done <- submitErr
	}()
	<-arrived
	writeAdaptationEntries(t, e.configPath, `[{"pattern":"*adaptive*","prompt":"SECOND"}]`)
	_, reloadErr := r.Reload(ctx, testCaller(r))
	testassert.NoError(t, reloadErr)
	firstRequest := e.server.requests()
	close(release)
	testassert.NoError(t, <-done)
	first := awaitOperation(t, r, session.Identity.SessionID, "op-first", harness.OperationSuccess)
	firstWire := e.server.bodyAt(firstRequest)
	testassert.Require(t, strings.Contains(firstWire, "FIRST"), "prior capture wire = %.200q, want the revision 1 prompt", firstWire)

	beforeSecond := e.server.requests()
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-second", "hello")
	second := awaitOperation(t, r, session.Identity.SessionID, "op-second", harness.OperationSuccess)
	secondWire := e.server.bodyAt(beforeSecond)
	testassert.Require(t, strings.Contains(secondWire, "SECOND") && !strings.Contains(secondWire, "FIRST"), "next capture wire = %.200q, want the revision 2 prompt", secondWire)
	testassert.Require(t, first.Admission.Execution.ConfigurationRevision != second.Admission.Execution.ConfigurationRevision, "revision = %q vs %q, want the reload's revision to reach only the next capture", first.Admission.Execution.ConfigurationRevision, second.Admission.Execution.ConfigurationRevision)
	testassert.NoError(t, r.Close(ctx))
}

func TestAdaptationNativeConfigurationPublication(t *testing.T) {
	ctx := context.Background()
	invalid := `{"providers":{"prov":{"transport":{"base_url":"https://prov.test/v1","api_key_env":""},"discovery":false,"models":{"m":{"name":"M","context_window":4096}}}},"extensions":{"adaptation":{"enabled":true,"entries":[{"pattern":1}]}}}`
	e := newProductionEnv(t, storage.NewMemory())
	writeServiceFile(t, e.configPath, invalid)
	writeAgentDefinitions(t, e.configPath, `{}`)
	invalidRuntime, err := e.openPlugins(ctx, adaptation.Author())
	testassert.NoError(t, err)
	testassert.Require(t, invalidRuntime.config.current().native.inactive["adaptation"] != nil, "enabled invalid adaptation = %v, want it left out with a reason", invalidRuntime.config.current().native.inactive)
	testassert.NoError(t, invalidRuntime.Close(ctx))

	// The same invalid bytes are uninterpreted while the section is inactive,
	// including across a reload.
	inactive := strings.Replace(invalid, `{"enabled":true,"entries":[{"pattern":1}]}`, `{"enabled":false,"entries":[{"pattern":1}]}`, 1)
	writeServiceFile(t, e.configPath, inactive)
	r, err := e.openPlugins(ctx, adaptation.Author())
	testassert.NoError(t, err)
	writeAdaptationSection(t, e.configPath, `{"enabled":false,"entries":[{"pattern":2}]}`)
	_, err = r.Reload(ctx, testCaller(r))
	testassert.NoError(t, err)
	data, err := os.ReadFile(e.configPath)
	testassert.Require(t, err == nil && strings.Contains(string(data), `"enabled":false,"entries":[{"pattern":2}]`), "inactive reload changed or dropped the retained section: %s (%v)", data, err)
	testassert.NoError(t, r.Close(ctx))

	// An enabled section of an uninstalled extension is uninterpreted data.
	writeAdaptationSection(t, e.configPath, `{"enabled":true,"entries":[{"pattern":1}]}`)
	uninstalled, err := e.openPlugins(ctx)
	testassert.NoError(t, err)
	testassert.NoError(t, uninstalled.Close(ctx))

	// Melta publication accepts the exact lowercase members beside a
	// differently cased sibling; the hook consumes only the lowercase values.
	cased := newProductionEnv(t, storage.NewMemory())
	writeServiceFile(t, cased.configPath, `{"providers":{"prov":`+nominalProvMember(cased.server.URL, "", `{"adaptive":{"name":"A","context_window":262144}}`)+`},"extensions":{"adaptation":{"enabled":true,"entries":[{"pattern":"*adaptive*","prompt":"LOWER","append":["TAIL"]}],"Entries":[{"pattern":"*adaptive*","prompt":"UPPER","append":["UPPER_TAIL"]}],"Unrelated":true}}}`)
	writeAgentDefinitions(t, cased.configPath, `{"modefull": {"model": "prov/adaptive", "system_prompt": "full", "tools": ["read_file"]}}`)
	cased.server.setScript(func(context.Context, string) []string { return textTurnEvents("done") })
	runtimeCased, err := cased.openPlugins(ctx, agentsTestNative(), adaptation.Author())
	testassert.NoError(t, err)
	casedWorkspace := adaptationWorkspace(t, cased, "adapt-cased")
	casedRecord := namedAdaptationOperation(t, runtimeCased, casedWorkspace, "modefull", "op-cased")
	assertAdaptationWire(t, decodeWireChatBody(t, cased.server.bodyAt(0)), casedRecord, "LOWER\n\nTAIL", []string{"read_file"}, "")
	testassert.NoError(t, runtimeCased.Close(ctx))

	// The settings writer preserves the configured section; a settings save
	// whose adaptation members no longer fit the schema publishes the write
	// and leaves only the extension inactive.
	validEntries := `[{"pattern":"*","unknown":true}]`
	e2, r2 := newAdaptationRuntime(t, validEntries, parkPlugin(&parkingHook{}))
	view, err := r2.GetConfiguration(ctx, testCaller(r2))
	testassert.NoError(t, err)
	settings := view.Settings
	_, err = r2.UpdateSettings(ctx, testCaller(r2), protocol.UpdateSettingsRequest{Settings: settings})
	testassert.NoError(t, err)
	written, err := r2.GetConfiguration(ctx, testCaller(r2))
	testassert.NoError(t, err)
	beforeGeneration := written.ConfigurationRevision.Generation
	data, err = os.ReadFile(e2.configPath)
	testassert.NoError(t, err)
	testassert.Require(t, strings.Contains(string(data), `"entries":[{"pattern":"*","unknown":true}]`), "settings write lost the configured entries: %s", data)

	settings = written.Settings
	settings.Extensions["adaptation"] = `{"enabled":true,"entries":[{"pattern":"*","prompt":1}]}`
	_, err = r2.UpdateSettings(ctx, testCaller(r2), protocol.UpdateSettingsRequest{Settings: settings})
	testassert.NoError(t, err)
	after, err := os.ReadFile(e2.configPath)
	testassert.Require(t, err == nil && strings.Contains(string(after), `"prompt":1`), "a rejected settings write left the file unchanged: %s", after)
	fresh, err := r2.GetConfiguration(ctx, testCaller(r2))
	testassert.NoError(t, err)
	testassert.Require(t, fresh.ConfigurationRevision.Generation != beforeGeneration, "invalid-section publication did not advance the generation: %q vs %q", fresh.ConfigurationRevision.Generation, beforeGeneration)
	testassert.Require(t, r2.config.current().native.inactive["adaptation"] != nil, "invalid active adaptation = %v, want it left out with a reason", r2.config.current().native.inactive)
	testassert.NoError(t, r2.Close(ctx))
}
