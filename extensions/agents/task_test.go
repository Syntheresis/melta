package agents

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestDecodeSettingsRangesAndShapes proves the settings decoder the tool's
// preparation owns: absent/blank/null input and null members keep the
// defaults, unknown members are ignored, both retained ranges accept their
// exact boundaries and reject everything outside with the retained texts, and
// malformed or wrong-typed input carries the invalid-settings wrap.
func TestDecodeSettingsRangesAndShapes(t *testing.T) {
	keepDefaults := []json.RawMessage{
		nil,
		json.RawMessage(""),
		json.RawMessage("  \n"),
		json.RawMessage("null"),
		json.RawMessage(`{}`),
		json.RawMessage(`{"task":{"max_concurrent":null,"max_output_bytes":null}}`),
		json.RawMessage(`{"task":{"unknown_member":1,"max_output_bytes":null}}`),
	}
	for _, raw := range keepDefaults {
		s, err := decodeSettings(raw)
		testassert.Require(t, err == nil, "decodeSettings(%s): %v", raw, err)
		testassert.Check(t, s == (settings{MaxConcurrent: 4, MaxOutputBytes: 15360}), "decodeSettings(%s) = %+v, want the defaults 4 and 15360", raw, s)
	}
	inRange := []json.RawMessage{
		json.RawMessage(`{"task":{"max_concurrent":1,"max_output_bytes":1024}}`),
		json.RawMessage(`{"task":{"max_concurrent":20,"max_output_bytes":1048576}}`),
		json.RawMessage(`{"task":{"max_concurrent":7}}`),
		json.RawMessage(`{"task":{"max_output_bytes":2048}}`),
	}
	for _, raw := range inRange {
		_, err := decodeSettings(raw)
		testassert.Check(t, err == nil, "decodeSettings(%s) = %v, want nil", raw, err)
	}
	rejected := []struct {
		raw  json.RawMessage
		want string
	}{
		{json.RawMessage(`{"task":{"max_concurrent":0}}`), "tasks: max_concurrent must be between 1 and 20"},
		{json.RawMessage(`{"task":{"max_concurrent":21}}`), "tasks: max_concurrent must be between 1 and 20"},
		{json.RawMessage(`{"task":{"max_output_bytes":1023}}`), "tasks: max_output_bytes must be between 1024 and 1048576"},
		{json.RawMessage(`{"task":{"max_output_bytes":1048577}}`), "tasks: max_output_bytes must be between 1024 and 1048576"},
	}
	for _, tc := range rejected {
		_, err := decodeSettings(tc.raw)
		testassert.Check(t, err != nil && err.Error() == tc.want, "decodeSettings(%s) = %v, want exactly %q", tc.raw, err, tc.want)
	}
	wrongType := []json.RawMessage{
		json.RawMessage(`{"task":{"max_output_bytes":"many"}}`),
		json.RawMessage(`{"task":{"max_concurrent":"4"}}`),
		json.RawMessage(`{"task":{"max_concurrent":4.5}}`),
		json.RawMessage(`{"task":{"max_concurrent":4.0}}`),
		json.RawMessage(`{"task":{"max_concurrent":4e0}}`),
		json.RawMessage(`[]`),
	}
	for _, raw := range wrongType {
		_, err := decodeSettings(raw)
		testassert.Check(t, err != nil && strings.HasPrefix(err.Error(), "tasks: invalid settings: "), "decodeSettings(%s) = %v, want the tasks invalid-settings wrap", raw, err)
	}
	{
		s, err := decodeSettings(json.RawMessage(`{"task":{"MAX_CONCURRENT":1}}`))
		testassert.Require(t, err == nil, "decodeSettings with a mixed-case member name: %v", err)
		testassert.Check(t, s.MaxConcurrent == 1, "MaxConcurrent = %d, want 1 from the mixed-case member name", s.MaxConcurrent)
	}
}

// openTool activates the shipped Agents entry with the captured extension
// section and returns its registered task tool: settings are decoded and
// retained at activation, never at Prepare.
func openTool(t *testing.T, section string) (extension.ToolFunc, error) {
	t.Helper()
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: json.RawMessage(section)}
	if section == "" {
		api.settings = json.RawMessage(`{"enabled":true}`)
	}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		return nil, err
	}
	testassert.Require(t, api.tool != nil, "activation registered no task tool")
	return api.tool, nil
}

// members answers the tool's live-member lookup with count Agents Sessions.
func members(count int) func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
	return func(_ context.Context, service, method string, _ json.RawMessage) (extension.Result, error) {
		var out []map[string]string
		for i := 0; i < count; i++ {
			out = append(out, map[string]string{"kind": "session", "id": "s", "starter": ExtensionID})
		}
		value, _ := json.Marshal(out)
		if out == nil {
			value = []byte(`[]`)
		}
		return extension.Result{Value: value}, nil
	}
}

// taskCall builds one completed task call for the direct preparation tests.
func taskCall(t *testing.T, args string) model.ToolCall {
	t.Helper()
	call, err := model.NewToolCall(model.ToolCall{ID: "call-1", Name: "task", Arguments: json.RawMessage(args)})
	testassert.NoError(t, err)
	return call
}

// TestPrepareRejectsMissingAndUnknown proves the retained shape errors.
func TestPrepareRejectsMissingAndUnknown(t *testing.T) {
	tool, err := openTool(t, "")
	testassert.NoError(t, err)
	tc := extension.AuthorContext{Call: members(0)}
	prepare := func(args string) string {
		t.Helper()
		_, err := tool(context.Background(), extension.ToolCall{AuthorContext: tc, Arguments: json.RawMessage(args)})
		testassert.Require(t, err != nil, "call(%s) succeeded", args)
		return err.Error()
	}
	{
		got := prepare(`{}`)
		testassert.Check(t, got == "missing 'prompt' parameter", "missing prompt content = %q, want %q", got, "missing 'prompt' parameter")
	}
	{
		got := prepare(`{"prompt":"do it"}`)
		testassert.Check(t, got == "missing 'subagent_type' parameter", "missing subagent_type content = %q, want %q", got, "missing 'subagent_type' parameter")
	}
	{
		got := prepare(`{"prompt":"do it","subagent_type":"ghost"}`)
		testassert.Check(t, got == `unknown subagent type "ghost"`, "unknown type content = %q, want the unknown subagent refusal", got)
	}
	{
		got := prepare(`{"prompt":"do it","subagent_type":"primary"}`)
		testassert.Check(t, got == `unknown subagent type "primary"`, "non-subagent content = %q, want the unknown subagent refusal", got)
	}
}

// TestActivationRetainsTaskSettings proves the single activation-time decode:
// absent, null and default member forms keep the retained defaults, an
// integer object retains its values, and non-integer lexemes the settings
// schema admits as integers are rejected before the activation succeeds.
func TestActivationRetainsTaskSettings(t *testing.T) {
	limits := func(t *testing.T, tool extension.ToolFunc) (int, int) {
		t.Helper()
		args := json.RawMessage(`{"prompt":"do it","subagent_type":"secondary"}`)
		output := 0
		invoke := func(count int) error {
			call := extension.AuthorContext{Call: func(ctx context.Context, service, method string, raw json.RawMessage) (extension.Result, error) {
				if method == "members" {
					return members(count)(ctx, service, method, raw)
				}
				start, err := extension.DecodeSessionStart(raw)
				if err != nil {
					return extension.Result{}, err
				}
				output = start.OutputLimit
				return extension.Result{Value: json.RawMessage(`{"session_id":"id"}`)}, nil
			}}
			_, err := tool(context.Background(), extension.ToolCall{AuthorContext: call, Arguments: args})
			return err
		}
		testassert.NoError(t, invoke(0))
		concurrent := 0
		for concurrent < 21 {
			if invoke(concurrent+1) != nil {
				break
			}
			concurrent++
		}
		return concurrent + 1, output
	}
	for _, row := range []struct {
		name    string
		section string
	}{
		{"absent section", ""},
		{"null task", `{"enabled":true,"task":null}`},
		{"absent members", `{"enabled":true,"task":{}}`},
		{"null members", `{"enabled":true,"task":{"max_concurrent":null,"max_output_bytes":null}}`},
	} {
		t.Run(row.name, func(t *testing.T) {
			tool, err := openTool(t, row.section)
			testassert.NoError(t, err)
			concurrent, output := limits(t, tool)
			testassert.Check(t, concurrent == 4 && output == 15360, "retained limits = %d/%d, want the defaults 4/15360", concurrent, output)
		})
	}
	tool, err := openTool(t, `{"enabled":true,"task":{"max_concurrent":7,"max_output_bytes":2048}}`)
	testassert.NoError(t, err)
	concurrent, output := limits(t, tool)
	testassert.Check(t, concurrent == 7 && output == 2048, "retained limits = %d/%d, want 7/2048", concurrent, output)
	for _, section := range []string{
		`{"task":{"max_concurrent":1.0}}`,
		`{"task":{"max_concurrent":1e0}}`,
		`{"task":{"max_output_bytes":2048.0}}`,
	} {
		_, err := openTool(t, section)
		testassert.Check(t, err != nil && strings.HasPrefix(err.Error(), "tasks: invalid settings: "), "activate(%s) = %v, want the invalid-settings rejection", section, err)
	}
}

// TestConfigSchemaKeepsNullDefaults proves the shipped config schema admits
// the retained absent/null/default forms and integer members while refusing a
// non-object task document.
func TestConfigSchemaKeepsNullDefaults(t *testing.T) {
	manifest := Author().Manifest
	for _, section := range []string{
		`{"enabled":true}`,
		`{"enabled":true,"task":null}`,
		`{"enabled":true,"task":{}}`,
		`{"enabled":true,"task":{"max_concurrent":null,"max_output_bytes":null}}`,
		`{"enabled":true,"task":{"max_concurrent":7,"max_output_bytes":2048}}`,
	} {
		testassert.Check(t, extension.ValidateManifestSection(manifest, json.RawMessage(section)) == nil, "config schema refused the retained form %s", section)
	}
	testassert.Check(t, extension.ValidateManifestSection(manifest, json.RawMessage(`{"enabled":true,"task":5}`)) != nil, "config schema accepted a non-object task document")
}
