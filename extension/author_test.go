package extension

import (
	"encoding/json"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func TestRegistrationMethodAndManifestValidation(t *testing.T) {
	manifest := Manifest{ID: "example", Version: "1", APIVersion: APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	mismatch := manifest
	mismatch.APIVersion = "unsupported"
	if err := ValidateManifest(mismatch); err == nil {
		t.Fatal("API mismatch accepted")
	}
	reserved := manifest
	reserved.ID = HostProvider
	if err := ValidateManifest(reserved); err == nil {
		t.Fatal("reserved host identity accepted")
	}
	if err := ValidateManifestSection(manifest, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManifestSection(manifest, json.RawMessage(`[]`)); err == nil {
		t.Fatal("invalid section accepted")
	}
	method := Method{Name: "get", Mode: ModePure, Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{"type":"string"}`)}
	bound, err := CompileMethod(method)
	if err != nil {
		t.Fatal(err)
	}
	method.Input[0] = 'x'
	if err := bound.ValidateArguments(json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := bound.ValidateArguments(json.RawMessage(`[]`)); err == nil {
		t.Fatal("invalid arguments accepted")
	}
	if _, err := CompileMethod(Method{}); err == nil {
		t.Fatal("invalid method accepted")
	}
}

func TestRegistrationStartOwned(t *testing.T) {
	start := &SessionStart{
		Target: "one/two", InitialState: harness.SessionStateValue(`{"a":1}`),
		Inputs: harness.SessionInputs{Tools: []string{"tool"}},
		Input:  []model.ContentPart{{Kind: model.PartText, Text: "hello", Extra: model.Extra{"x": json.RawMessage(`{"v":1}`)}}},
	}
	plan := ToolPlan{Start: start}.Owned()
	start.InitialState[0] = 'x'
	start.Inputs.Tools[0] = "changed"
	start.Input[0].Extra["x"][0] = 'x'
	if plan.Start.Target != "one/two" || string(plan.Start.InitialState) != `{"a":1}` ||
		plan.Start.Inputs.Tools[0] != "tool" || string(plan.Start.Input[0].Extra["x"]) != `{"v":1}` {
		t.Fatalf("start not owned: %+v", plan.Start)
	}
}
