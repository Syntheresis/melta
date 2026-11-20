package melta

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
	"github.com/Syntheresis/melta/protocol"
)

const testConfigDocument = `{
  "providers": {
    "prov": {
      "transport": {"base_url": "https://api.prov.test/v1", "api_key_env": ""},"discovery": false,
      "models": {"m": {"name": "M", "context_window": 9007199254740993}}
    }
  },
  "sessions": {"auto_archive": false, "archive_after_days": 3},
  "extensions": {"alpha": {"threshold": 9007199254740993}},
  "permissions": {"allow": ["read_file"]},
  "compaction": {"summarizer_provider": "legacy"}
}`

func TestSessionInputsProtocolModelConversion(t *testing.T) {
	for _, row := range []struct {
		wire *protocol.ModelRef
		ref  *model.ModelRef
	}{
		{nil, nil},
		{func() *protocol.ModelRef { value := protocol.ModelRef("provider/part/model"); return &value }(), &model.ModelRef{Provider: "provider", Model: "part/model"}},
	} {
		inputs, err := sessionInputsFromProtocol(protocol.SessionInputs{Model: row.wire})
		testassert.NoError(t, err)
		testassert.Require(t, reflect.DeepEqual(inputs.Model, row.ref), "model = %+v, want %+v", inputs.Model, row.ref)
		projected := projectSessionInputs(inputs)
		testassert.Require(t, reflect.DeepEqual(projected.Model, row.wire), "projected = %+v, want %+v", projected.Model, row.wire)
		data, err := json.Marshal(projected)
		testassert.NoError(t, err)
		var members map[string]json.RawMessage
		testassert.NoError(t, json.Unmarshal(data, &members))
		if row.wire == nil {
			_, present := members["model"]
			testassert.Require(t, !present, "absent model encoded: %s", data)
		} else {
			var rendered string
			testassert.NoError(t, json.Unmarshal(members["model"], &rendered))
			testassert.Equal(t, rendered, string(*row.wire))
		}
	}
	invalid := protocol.ModelRef("invalid")
	_, err := sessionInputsFromProtocol(protocol.SessionInputs{Model: &invalid})
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "invalid wire model = %v", err)
}

const testAgentsDocument = `{
  "primary": {"model": "prov/m"},
  "plan": {
    "model": "prov/m",
    "system_prompt": "simple",
    "prompt": "plan prompt",
    "tools": ["plan_tool", "plan_tool", "read_file"],
    "description": "planner",
    "subagent": true
  }
}`

// assembleConfiguration publishes one captured document set through the real
// candidate path with the supplied Agents extension, so the carrier fields
// are the retained test values.
func assembleConfiguration(t *testing.T, generation uint64, configData, agentsData string) (*configuration, error) {
	t.Helper()
	h := newServiceHarness(t)
	writeServiceFile(t, h.configPath, configData)
	if agentsData != "" {
		writeAgentDefinitions(t, h.configPath, agentsData)
	}
	svc := h.preparedService(context.Background(), agentsTestNative())
	captured, err := svc.publish(context.Background())
	if err != nil {
		return nil, err
	}
	captured.snapshot.generation = generation
	return captured.snapshot, nil
}

func testSnapshot(t *testing.T) *configuration {
	t.Helper()
	snapshot, err := assembleConfiguration(t, 3, testConfigDocument, testAgentsDocument)
	testassert.NoError(t, err)
	return snapshot
}

// TestNewConfigurationSnapshot proves the private snapshot consumes exactly
// one decoded input set: the sessions policy, the plugin values and the
// catalog layers keep exact numbers through the UseNumber decode, and the
// publication generation is carried unchanged.
func TestNewConfigurationSnapshot(t *testing.T) {
	snapshot := testSnapshot(t)

	testassert.Require(t, snapshot.generation == 3 && !snapshot.sessions.AutoArchive && snapshot.sessions.ArchiveAfterDays == 3 && snapshot.sessions.DeleteAfterArchiveDays == 7, "got %v, want %v; sessions = %+v, want the decoded policy over the true/7/7 defaults", snapshot.generation, 3, snapshot.sessions)

	provider, found := snapshot.catalog.Providers["prov"]
	testassert.Require(t, found, "captured providers never reached the assembled catalog: %+v", snapshot.catalog)
	{
		modelEntry := provider.Models["m"]
		testassert.Require(t, modelEntry != nil && modelEntry.ContextWindow == 9007199254740993, "catalog model = %+v, want the exact integer preserved through the decode", modelEntry)
	}
	// The legacy compaction member is unowned now: unknown members are
	// ignored and the absent model/prompt keep their defaults.
	testassert.Require(t, snapshot.compactionValues.model.IsZero() && snapshot.compactionValues.prompt == prompt.DefaultSummarizerPrompt, "compaction values = %+v, want the absent-section defaults", snapshot.compactionValues)
	plugin, ok := snapshot.extensions["alpha"]
	testassert.Require(t, ok, "extensions = %+v, want the alpha entry", snapshot.extensions)
	var pluginFields map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(plugin)))
	decoder.UseNumber()
	testassert.NoError(t, decoder.Decode(&pluginFields))
	{
		number, isNumber := pluginFields["threshold"].(json.Number)
		testassert.Require(t, isNumber && number.String() == "9007199254740993", "plugin threshold = %#v, want the exact json.Number preserved", pluginFields["threshold"])
	}
	testassert.Require(t, len(snapshot.catalogWarnings) == 0, "valid input produced catalog warnings: %+v", snapshot.catalogWarnings)
}

// TestNewConfigurationNullAndDefaults proves the target documents accept one
// object or null-as-empty everywhere: absent or null sections keep the
// true/7/7 session defaults, and the legacy whole-document rules
// (old-shape rejection, permission decoding) never reject the captured bytes.
func TestNewConfigurationNullAndDefaults(t *testing.T) {
	for _, doc := range []string{`{}`, `{"sessions":null,"extensions":null,"providers":null}`} {
		snapshot, err := assembleConfiguration(t, 1, doc, "")
		testassert.Require(t, err == nil, "newConfiguration(%s): %v", doc, err)
		testassert.Require(t, snapshot.sessions.AutoArchive && snapshot.sessions.ArchiveAfterDays == 7 && snapshot.sessions.DeleteAfterArchiveDays == 7 && snapshot.extensions == nil, "sessions under %s = %+v, want true/7/7; extensions under %s = %+v, want none", doc, snapshot.sessions, doc, snapshot.extensions)
	}
}

// TestNewConfigurationRejectsCandidates proves every malformed consumed
// input rejects the candidate: trailing or non-object documents, malformed
// or non-object sections, and the overflowed session days never produce a
// partially usable snapshot.
func TestNewConfigurationRejectsCandidates(t *testing.T) {
	for _, row := range []struct {
		name       string
		configData string
		agentsData string
	}{
		{name: "config not an object", configData: `[]`, agentsData: ``},
		{name: "config trailing value", configData: `{} {}`, agentsData: ``},
		{name: "config truncated", configData: `{"providers":`, agentsData: ``},
		{name: "sessions malformed", configData: `{"sessions":{"auto_archive":}}`, agentsData: ``},
		{name: "sessions not an object", configData: `{"sessions":5}`, agentsData: ``},
		{name: "sessions day overflow", configData: `{"sessions":{"archive_after_days":106752}}`, agentsData: ``},
		{name: "delete day overflow", configData: `{"sessions":{"delete_after_archive_days":106752}}`, agentsData: ``},
		{name: "extensions not an object", configData: `{"extensions":[]}`, agentsData: ``},
	} {
		{
			snapshot, err := assembleConfiguration(t, 1, row.configData, row.agentsData)
			testassert.Require(t, err != nil, "%s produced %+v, want rejection", row.name, snapshot)
		}
	}
}

// TestNewConfigurationLeavesInvalidAuthorInactive proves the uniform
// inactive rule: an enabled Agents section whose definitions cannot parse
// leaves the candidate published, the extension inactive with its reason,
// and the rest of the revision intact.
func TestNewConfigurationLeavesInvalidAuthorInactive(t *testing.T) {
	snapshot, err := assembleConfiguration(t, 1, `{}`, `[]`)
	testassert.NoError(t, err)
	testassert.Require(t, snapshot.native.inactive[agents.ExtensionID] != nil, "inactive map = %v, want the agents entry", snapshot.native.inactive)
}

// TestNewConfigurationKeepsExistingWarningTypes proves non-fatal catalog
// problems are retained in the existing warning type instead of failing the
// candidate. Definition-format rejection belongs to the owning extension's
// activation.
func TestNewConfigurationKeepsExistingWarningTypes(t *testing.T) {
	snapshot, err := assembleConfiguration(t, 0,
		`{"providers":{"broken":5}}`,
		`{}`)
	testassert.NoError(t, err)
	testassert.Require(t, len(snapshot.catalogWarnings) == 1 && snapshot.catalogWarnings[0].Kind == "user_config_skip", "catalog warnings = %+v, want the retained catalog warning type", snapshot.catalogWarnings)
}

// TestConfigurationPermissionCapture proves the captured permission inputs:
// the raw global member and the Workspace bytes keyed by directory ID resolve
// through the same revision, an unknown Workspace resolves global-only, a
// malformed block follows the built-in fallback posture without failing the
// candidate, and the retained legacy interactive member is malformed target
// policy.
func TestConfigurationPermissionCapture(t *testing.T) {
	const globalAllow = `{"rules":[{"permission":"file.write","target":"*","access":"allow"}]}`
	const workspaceDeny = `{"rules":[{"permission":"file.write","target":"*","access":"deny"}]}`
	const globalDoc = `{"providers":{"prov":{"transport":{"base_url":"https://api.prov.test/v1","api_key_env":""},"models":{"m":{"name":"M","context_window":100}}}},"permissions":` + globalAllow + `}`

	snapshot, err := assembleConfiguration(t, 2, globalDoc, `{"agent":{"model":"prov/m"}}`)
	testassert.NoError(t, err)
	testassert.Require(t, string(snapshot.permissions) == globalAllow, "captured global member = %q, want the raw bytes unchanged", snapshot.permissions)
	wsID, err := config.WorkspacePermissionDirID("/ws")
	testassert.NoError(t, err)
	snapshot.workspacePermissions = map[string]json.RawMessage{wsID: json.RawMessage(workspaceDeny)}

	// The Workspace rule decides over the global rule from the same capture.
	want := harness.ResolvePermissionPolicy(json.RawMessage(globalAllow), json.RawMessage(workspaceDeny))
	{
		got := snapshot.permissionPolicy("/ws")
		testassert.Require(t, reflect.DeepEqual(got, want), "workspace policy = %#v, want the resolved workspace-over-global capture", got)
	}
	// An unknown Workspace resolves global-only.
	globalOnly := harness.ResolvePermissionPolicy(json.RawMessage(globalAllow), nil)
	{
		got := snapshot.permissionPolicy("/other")
		testassert.Require(t, reflect.DeepEqual(got, globalOnly), "unknown-workspace policy = %#v, want the global-only capture", got)
	}
	// A malformed Workspace block discards both user levels for that
	// Workspace's resolutions: built-in fallback despite the valid global
	// member.
	snapshot.workspacePermissions = map[string]json.RawMessage{wsID: json.RawMessage(`{"rules":5}`)}
	var builtin harness.PermissionPolicy
	{
		got := snapshot.permissionPolicy("/ws")
		testassert.Require(t, reflect.DeepEqual(got, builtin), "malformed-workspace policy = %#v, want the built-in fallback", got)
	}
	{
		got := snapshot.permissionPolicy("/other")
		testassert.Require(t, reflect.DeepEqual(got, globalOnly), "other-workspace policy after the malformed sibling = %#v, want the untouched global capture", got)
	}

	// The retained legacy interactive permissions shape is not migrated: it
	// is malformed target policy, and the candidate still builds.
	legacy := testSnapshot(t)
	{
		got := legacy.permissionPolicy("/ws")
		testassert.Require(t, reflect.DeepEqual(got, builtin), "legacy-member policy = %#v, want the built-in fallback", got)
	}
}
