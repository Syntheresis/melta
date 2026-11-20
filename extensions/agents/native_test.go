package agents

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
)

func activatedAPI(t *testing.T, section string) *authorAPI {
	t.Helper()
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: json.RawMessage(section)}
	_, err := Author().Activate(context.Background(), api)
	testassert.NoError(t, err)
	return api
}

// TestAuthorListsRosterInResolverOrder proves the list operation projects the
// resolved roster in order with each definition's own description, subagent
// flag and model.
func TestAuthorListsRosterInResolverOrder(t *testing.T) {
	api := activatedAPI(t, `{"enabled":true,"definitions":{
		"primary":{"model":"prov/m"},
		"plan":{"model":"prov/m","system_prompt":"simple","prompt":"plan prompt","tools":["plan_tool"],"readonly":true,"description":"planner","subagent":true},
		"modelless":{"system_prompt":"none"}
	}}`)
	result, err := api.operations["list"].Handler(context.Background(), extension.AuthorContext{}, json.RawMessage(`{}`))
	testassert.NoError(t, err)
	var rows []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Subagent    bool   `json:"subagent"`
		Model       string `json:"model"`
	}
	testassert.NoError(t, json.Unmarshal(result.Value, &rows))
	var names []string
	for _, row := range rows {
		names = append(names, row.Name)
		if row.Name == "plan" {
			testassert.Require(t, row.Description == "planner" && row.Subagent && row.Model == "prov/m", "plan row = %+v", row)
		}
	}
	testassert.Require(t, reflect.DeepEqual(names, []string{"primary", "secondary", "explore", "review", "compact", "modelless", "plan"}), "list order = %q", names)
}

// TestAuthorSetModelTransformsLatestSection proves the pure model edit
// consumes the latest owning section it is handed, preserves every other
// member and definition field byte-exactly, and retains the known-definition
// and model invalid-input failures.
func TestAuthorSetModelTransformsLatestSection(t *testing.T) {
	api := activatedAPI(t, `{"enabled":true}`)
	testassert.Require(t, api.service.Name == ConfigurationCapabilityID && len(api.service.Methods) == 1 && api.service.Methods[0].Method.Mode == extension.ModePure, "configuration service = %+v, want one pure set_model", api.service)
	transform := func(section, arguments string) (extension.Result, error) {
		input, err := json.Marshal(struct {
			Section   json.RawMessage `json:"section"`
			Arguments json.RawMessage `json:"arguments"`
		}{json.RawMessage(section), json.RawMessage(arguments)})
		testassert.NoError(t, err)
		return api.service.Methods[0].Handler(context.Background(), extension.AuthorContext{}, input)
	}
	result, err := transform(`{"enabled":true,"task":{"max_concurrent":7},"unknown":9007199254740993,"definitions":{"worker":{"system_prompt":"simple","description":"kept","tools":[]},"other":{"model":"p/old"}}}`, `{"definition_id":"worker","model":"prov/m"}`)
	testassert.NoError(t, err)
	var transformed map[string]json.RawMessage
	testassert.NoError(t, json.Unmarshal(result.Value, &transformed))
	testassert.Equal(t, strings.TrimSpace(string(transformed["task"])), `{"max_concurrent":7}`)
	testassert.Equal(t, strings.TrimSpace(string(transformed["unknown"])), `9007199254740993`)
	var definitions map[string]json.RawMessage
	testassert.NoError(t, json.Unmarshal(transformed["definitions"], &definitions))
	testassert.Equal(t, strings.TrimSpace(string(definitions["worker"])), `{"description":"kept","model":"prov/m","system_prompt":"simple","tools":[]}`)
	testassert.Equal(t, strings.TrimSpace(string(definitions["other"])), `{"model":"p/old"}`)

	cleared, err := transform(string(result.Value), `{"definition_id":"worker","model":""}`)
	testassert.NoError(t, err)
	testassert.NoError(t, json.Unmarshal(cleared.Value, &transformed))
	testassert.NoError(t, json.Unmarshal(transformed["definitions"], &definitions))
	testassert.Equal(t, strings.TrimSpace(string(definitions["worker"])), `{"description":"kept","system_prompt":"simple","tools":[]}`)

	for _, row := range []struct {
		name      string
		section   string
		arguments string
	}{
		{"unknown definition", `{"enabled":true,"definitions":{"worker":{"system_prompt":"simple"}}}`, `{"definition_id":"ghost","model":"prov/m"}`},
		{"malformed model", `{"enabled":true,"definitions":{"worker":{"system_prompt":"simple"}}}`, `{"definition_id":"worker","model":"noslash"}`},
		{"non-object entry", `{"enabled":true,"definitions":{"primary":5}}`, `{"definition_id":"primary","model":"prov/m"}`},
		{"non-object section", `5`, `{"definition_id":"worker","model":"prov/m"}`},
	} {
		_, err := transform(row.section, row.arguments)
		testassert.Require(t, errors.Is(err, harness.ErrInvalid), "%s = %v, want harness.ErrInvalid", row.name, err)
	}
}
