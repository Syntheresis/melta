package patch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/model"
)

// ToolName is the model-facing tool identity.
const ToolName = "apply_patch"

// Definition is the model-facing tool definition.
func Definition() model.ToolDefinition {
	return model.ToolDefinition{Name: ToolName, Description: description, Parameters: json.RawMessage(parameters)}
}

// Tool applies a multi-file V4A patch through Workspace operations.
func Tool(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	var args struct {
		Input string `json:"input"`
	}
	if err := call.Decode(&args); err != nil {
		return extension.ToolResult{}, err
	}
	prepared, err := parseAndStatTargets(ctx, call.AuthorContext, args.Input)
	if err != nil {
		return extension.ToolResult{}, err
	}
	summary, previews, err := executePlan(ctx, call.AuthorContext, prepared)
	if err != nil {
		if summary != "" {
			return extension.ToolResult{}, fmt.Errorf("%s\n\n%w", summary, err)
		}
		return extension.ToolResult{}, err
	}
	outcome := extension.ToolResult{Content: summary}
	if metadata := previewMetadata(previews); metadata != nil {
		if raw, marshalErr := json.Marshal(metadata); marshalErr == nil {
			outcome.Metadata = raw
		}
	}
	return outcome, nil
}

const description = `Edits, creates, deletes, or renames files using the apply_patch (V4A) patch format.
- Wrap every change between *** Begin Patch and *** End Patch, with one section per file.
- Start each section with a header: *** Add File: <path> (every following line is a + line of new content), *** Update File: <path> (edit in place), or *** Delete File: <path> (nothing follows).
- To rename, put *** Move to: <new path> on the line right after *** Update File: <path>.
- In an Update, write each change as a hunk starting with @@. Prefix context lines with a space, removed lines with -, and added lines with +. Include about 3 lines of context around each change, and add @@ <enclosing function or class> when that context is not unique.
- Use file paths relative to the project root.`

const parameters = `{
  "type": "object",
  "properties": {
    "input": {
      "type": "string",
      "description": "The full patch text, from *** Begin Patch to *** End Patch."
    }
  },
  "required": ["input"]
}`
