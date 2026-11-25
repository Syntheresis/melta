package melta

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func fixtureToolLookup(run func(context.Context, model.ToolCall) harness.ToolOutcome) func(string) (harness.ToolBinding, bool, error) {
	return func(name string) (harness.ToolBinding, bool, error) {
		return harness.ToolBinding{Identity: harness.ToolImplementation{ID: name}, Validate: func(json.RawMessage) error { return nil }, Run: run}, true, nil
	}
}

// TestRegisteredToolBindingIdentity proves the registration identity at the
// owner lookup: a bound registered tool carries its minted registration ID and
// owning extension ID, never the advertised tool name, and an unregistered
// name resolves to nothing.
func TestRegisteredToolBindingIdentity(t *testing.T) {
	owner := &configurationService{}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
		extension: testToolNative("probe", testRegisteredTool), section: []byte(`{}`),
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
