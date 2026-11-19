package harness_test

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func fixtureToolLookup(prepare func(context.Context, model.ToolCall) harness.PreparedTool, normalize func(context.Context, model.ToolCall) (json.RawMessage, error)) func(string) (harness.ToolBinding, bool) {
	if prepare == nil {
		prepare = func(context.Context, model.ToolCall) harness.PreparedTool { return harness.PreparedTool{} }
	}
	if normalize == nil {
		normalize = publicNormalize
	}
	return func(name string) (harness.ToolBinding, bool) {
		return harness.ToolBinding{Identity: harness.ToolImplementation{ID: name}, Normalize: normalize, Prepare: prepare}, true
	}
}
