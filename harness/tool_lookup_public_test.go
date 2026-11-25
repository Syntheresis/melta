package harness_test

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func fixtureToolLookup(run func(context.Context, model.ToolCall) harness.ToolOutcome) func(string) (harness.ToolBinding, bool, error) {
	if run == nil {
		run = func(context.Context, model.ToolCall) harness.ToolOutcome { return harness.ToolOutcome{} }
	}
	return func(name string) (harness.ToolBinding, bool, error) {
		return harness.ToolBinding{
			Identity: harness.ToolImplementation{ID: name},
			Validate: func(json.RawMessage) error { return nil },
			Run:      run,
		}, true, nil
	}
}
