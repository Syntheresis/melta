package melta

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func fixtureToolLookup(prepare func(context.Context, model.ToolCall) harness.PreparedTool, normalize func(context.Context, model.ToolCall) (json.RawMessage, error)) func(string) (harness.ToolBinding, bool) {
	return func(name string) (harness.ToolBinding, bool) {
		return harness.ToolBinding{Identity: harness.ToolImplementation{ID: name}, Normalize: normalize, Prepare: prepare}, true
	}
}

func TestStandardToolBindingIdentity(t *testing.T) {
	c := &composition{
		exports:        map[string]capabilityExport{"sleep": {standard: true}},
		standardValues: map[string]Tool{"sleep": sleepTool{}},
	}
	bound, err := c.bindTool(selection{}, "sleep")
	if err != nil || bound.identity != (harness.ToolImplementation{ID: "sleep"}) {
		t.Fatalf("bound = %+v, err = %v", bound.identity, err)
	}
}

func fixturePrepare(inv harness.InvocationExecution, ctx context.Context, call model.ToolCall) harness.PreparedTool {
	binding, ok := inv.LookupTool(call.Name)
	if !ok {
		panic("missing test tool " + call.Name)
	}
	return binding.Prepare(ctx, call)
}
