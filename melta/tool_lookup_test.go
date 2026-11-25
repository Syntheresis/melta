package melta

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func fixtureToolLookup(prepare func(context.Context, model.ToolCall) toolPlan, normalize func(context.Context, model.ToolCall) (json.RawMessage, error)) func(string) (harness.ToolBinding, bool, error) {
	return func(name string) (harness.ToolBinding, bool, error) {
		return harness.ToolBinding{Identity: harness.ToolImplementation{ID: name}, Validate: func(json.RawMessage) error { return nil }, Run: func(ctx context.Context, call model.ToolCall) harness.ToolOutcome {
			return runTool(ctx, call, normalize, prepare)
		}}, true, nil
	}
}

// TestRegisteredToolBindingIdentity proves the registration identity at the
// owner lookup: a bound registered tool carries its minted registration ID and
// owning extension ID, never the advertised tool name, and an unregistered
// name resolves to nothing.
func TestRegisteredToolBindingIdentity(t *testing.T) {
	owner := &configurationService{}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
		extension: testToolNative("probe", testRegisteredTool{}), section: []byte(`{}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	stage := candidate.snapshotForTest()
	defer stage.release()
	bound := stage.bindTool("probe")
	if bound == nil {
		t.Fatal("registered tool not bound")
	}
	identity := harness.ToolImplementation{ID: bound.entry.id, ExtensionID: bound.entry.activation.id}
	if identity.ID == "" || identity.ID == "probe" || identity.ExtensionID != "probe" {
		t.Fatalf("bound identity = %+v", identity)
	}
	if stage.bindTool("missing") != nil {
		t.Fatal("unknown tool bound")
	}
}

func fixtureBinding(inv harness.InvocationExecution, name string) harness.ToolBinding {
	binding, ok, err := inv.LookupTool(name)
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("missing test tool " + name)
	}
	return binding
}

func TestImmediateToolOutcomeClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outcome  harness.ToolOutcome
		status   model.ToolResultStatus
		content  string
		evidence harness.ExecutionEvidence
	}{
		{"success overrides evidence", harness.ToolOutcome{Result: model.ToolResult{CallID: "call", Status: model.ResultSuccess, Content: "ok"}, Evidence: harness.ExecutionUnknown}, model.ResultSuccess, "ok", harness.ExecutionObserved},
		{"error overrides evidence", harness.ToolOutcome{Result: model.ToolResult{CallID: "call", Status: model.ResultError, Content: "failed"}, Evidence: harness.ExecutionObserved}, model.ResultError, "failed", harness.ExecutionNotStarted},
		{"interrupted overrides evidence", harness.ToolOutcome{Result: model.ToolResult{CallID: "call", Status: model.ResultInterrupted, Content: "stopped"}, Evidence: harness.ExecutionUnknown}, model.ResultInterrupted, "stopped", harness.ExecutionNotStarted},
		{"denial overrides evidence", harness.ToolOutcome{Result: model.ToolResult{CallID: "call", Status: model.ResultDenied, Content: "denied"}, Evidence: harness.ExecutionObserved}, model.ResultDenied, "denied", harness.ExecutionNotStarted},
		{"wrong call id", harness.ToolOutcome{Result: model.ToolResult{CallID: "other", Status: model.ResultSuccess}}, model.ResultError, "Tool call failed internal validation.", harness.ExecutionNotStarted},
		{"missing call id", harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess}}, model.ResultError, "Tool call failed internal validation.", harness.ExecutionNotStarted},
		{"invalid status", harness.ToolOutcome{Result: model.ToolResult{CallID: "call", Status: "invalid"}}, model.ResultError, "Tool call failed internal validation.", harness.ExecutionNotStarted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookup := fixtureToolLookup(func(context.Context, model.ToolCall) toolPlan {
				return toolPlan{Immediate: &tc.outcome}
			}, func(_ context.Context, call model.ToolCall) (json.RawMessage, error) {
				return call.Arguments, nil
			})
			binding, ok, err := lookup("tool")
			if err != nil || !ok {
				t.Fatalf("lookup = %v, %v", ok, err)
			}
			got := binding.Run(context.Background(), model.ToolCall{ID: "call", Name: "tool", Arguments: []byte(`{}`)})
			if got.Result.CallID != "call" || got.Result.Status != tc.status || got.Result.Content != tc.content || got.Evidence != tc.evidence {
				t.Fatalf("outcome = %+v, want %s %q %s", got, tc.status, tc.content, tc.evidence)
			}
		})
	}
	t.Run("normalization error skips preparation", func(t *testing.T) {
		prepared := false
		lookup := fixtureToolLookup(func(context.Context, model.ToolCall) toolPlan {
			prepared = true
			return toolPlan{}
		}, func(context.Context, model.ToolCall) (json.RawMessage, error) {
			return nil, errors.New("normalize failed")
		})
		binding, ok, err := lookup("tool")
		if err != nil || !ok {
			t.Fatalf("lookup = %v, %v", ok, err)
		}
		got := binding.Run(context.Background(), model.ToolCall{ID: "call", Name: "tool", Arguments: []byte(`{}`)})
		if prepared || got.Result.CallID != "call" || got.Result.Status != model.ResultError || got.Result.Content != "normalize failed" || got.Evidence != harness.ExecutionNotStarted {
			t.Fatalf("outcome = %+v, prepared = %v", got, prepared)
		}
	})
}
