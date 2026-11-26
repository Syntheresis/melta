package melta

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func TestRegisteredAdmissionOrdersAndValidatesHooks(t *testing.T) {
	for _, introduceUnknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordered", true: "unknown tool"}[introduceUnknown], func(t *testing.T) {
			owner := &configurationService{}
			guest := extension.Extension{
				Manifest: registrarManifest("one"),
				Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
					_, err := api.AttachAdmission(func(_ context.Context, call extension.AuthorContext, in extension.AdmissionInput) (extension.AdmissionResult, error) {
						if string(call.State) != ` {"n":1} ` {
							t.Fatalf("extension state = %q", call.State)
						}
						call.State[0] = 'x'
						result := in.Candidate
						result.Instructions = "first"
						return extension.AdmissionResult{Candidate: &result}, nil
					}, nil)
					if err != nil {
						return nil, err
					}
					_, err = api.AttachAdmission(func(_ context.Context, call extension.AuthorContext, in extension.AdmissionInput) (extension.AdmissionResult, error) {
						if in.Candidate.Instructions != "first" || string(call.State) != ` {"n":1} ` {
							t.Fatalf("second hook input = %+v state %q", in.Candidate, call.State)
						}
						result := in.Candidate
						result.Instructions = "second"
						if introduceUnknown {
							result.Tools = append(result.Tools, model.ToolDefinition{Name: "invented", Parameters: json.RawMessage(`{}`)})
						}
						return extension.AdmissionResult{Candidate: &result}, nil
					}, nil)
					return nil, err
				},
			}
			candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: guest, section: []byte(`{}`)}})
			if err != nil {
				t.Fatal(err)
			}
			defer candidate.release()
			stage := candidate.snapshotForTest()
			defer stage.release()
			states := map[string]harness.SessionStateValue{"one": []byte(` {"n":1} `)}
			input := extension.AdmissionInput{Candidate: extension.AdmissionCandidate{
				Strategy: harness.StrategySelection{ID: harness.IncludedStrategyID},
				Tools:    []model.ToolDefinition{{Name: "read", Parameters: json.RawMessage(`{}`)}},
			}}
			got, err := runRegisteredAdmission(context.Background(), stage, input, states, nil, nil, map[string]bool{"read": true}, harness.IncludedStrategyID)
			if introduceUnknown {
				if !errors.Is(err, harness.ErrInvalid) {
					t.Fatalf("unknown tool error = %v", err)
				}
			} else if err != nil || got.Instructions != "second" {
				t.Fatalf("result = %+v, %v", got, err)
			}
			if string(states["one"]) != ` {"n":1} ` {
				t.Fatalf("hook mutated stored state: %q", states["one"])
			}
		})
	}
}
