package worker_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func TestAuthorAdmissionCodecsWireShape(t *testing.T) {
	empty := ""
	result := extension.AdmissionResult{Candidate: &extension.AdmissionCandidate{
		Model: model.ModelRef{Provider: "provider", Model: "model"}, Prompt: &empty,
		Tools: []model.ToolDefinition{}, Strategy: harness.StrategySelection{ID: "s", Settings: json.RawMessage(`{"n":9007199254740993}`)},
	}}
	raw, err := worker.EncodeAuthorAdmissionResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"candidate":`)) || !bytes.Contains(raw, []byte(`"prompt":""`)) || !bytes.Contains(raw, []byte(`"tools":[]`)) || bytes.Contains(raw, []byte(`"value":`)) {
		t.Fatalf("unstable result shape: %s", raw)
	}
	got, err := worker.DecodeAuthorAdmissionResult(raw)
	if err != nil || !reflect.DeepEqual(got, result) {
		t.Fatalf("result = %+v, %v", got, err)
	}
	absent, err := worker.EncodeAuthorAdmissionResult(extension.AdmissionResult{})
	if err != nil || string(absent) != "{}" {
		t.Fatalf("absent result = %s, %v", absent, err)
	}
	if _, err := worker.DecodeAuthorAdmissionResult(json.RawMessage(`{"candidate":null}`)); err == nil {
		t.Fatal("accepted null candidate")
	}
	withoutTools := result
	candidate := *result.Candidate
	candidate.Tools = nil
	withoutTools.Candidate = &candidate
	raw, err = worker.EncodeAuthorAdmissionResult(withoutTools)
	if err != nil || bytes.Contains(raw, []byte(`"tools"`)) {
		t.Fatalf("omitted tools = %s, %v", raw, err)
	}
	input := extension.AdmissionInput{
		Choices: harness.SessionInputs{Tools: []string{}},
		Candidate: extension.AdmissionCandidate{Model: model.ModelRef{Provider: "provider", Model: "model"},
			Tools: []model.ToolDefinition{{Name: "tool", Parameters: json.RawMessage{0xff, '{'}}}},
		ModelCatalog: []model.ModelRef{{Provider: "provider", Model: "model"}},
		ToolCatalog:  []model.ToolDefinition{{Name: "tool", Parameters: json.RawMessage(`{"n":9007199254740993}`)}},
	}
	raw, err = worker.EncodeAuthorAdmission(input)
	if err != nil || !bytes.Contains(raw, []byte(`"candidate":{"model":{"provider":"provider","model":"model"}`)) ||
		!bytes.Contains(raw, []byte(`"choices":{"tools":[]}`)) || bytes.Contains(raw, []byte(`"Candidate"`)) {
		t.Fatalf("unstable admission shape: %s, %v", raw, err)
	}
	decoded, err := worker.DecodeAuthorAdmission(raw)
	if err != nil || decoded.Choices.Tools == nil || !reflect.DeepEqual(decoded.Candidate, input.Candidate) ||
		!reflect.DeepEqual(decoded.ModelCatalog, input.ModelCatalog) || !reflect.DeepEqual(decoded.ToolCatalog, input.ToolCatalog) {
		t.Fatalf("admission round trip: %+v, %v", decoded, err)
	}
}

func TestAuthorPlanCodecsRoundTrip(t *testing.T) {
	instructions, prompt := "instructions", "prompt"
	plan := extension.ToolPlan{Data: []byte{0xff, 0}, Start: &extension.SessionStart{
		Target: "raw/name", Inputs: harness.SessionInputs{
			Model: &model.ModelRef{Provider: "incomplete"}, Instructions: &instructions, Prompt: &prompt,
			Tools: []string{"same", "same"}, Strategy: &harness.StrategySelection{Settings: json.RawMessage{0xff, '{'}},
		}, InitialState: []byte(`{"n":9007199254740993}`),
	}}
	raw, err := worker.EncodeAuthorPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	got, err := worker.DecodeAuthorPlan(raw)
	if err != nil || !reflect.DeepEqual(got.Start, plan.Start) || !reflect.DeepEqual(got.Data, plan.Data) {
		t.Fatalf("plan = %+v, %v", got, err)
	}
}
