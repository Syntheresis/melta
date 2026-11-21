package patch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// ToolName is the model-facing tool identity.
const ToolName = "apply_patch"

// Definition is the model-facing tool definition.
func Definition() model.ToolDefinition {
	return model.ToolDefinition{Name: ToolName, Description: description, Parameters: json.RawMessage(parameters)}
}

// Tool implements multi-file V4A patching through the Host's Workspace file
// operations.
type Tool struct{}

func (Tool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	args, err := decodeArguments(call.Arguments)
	if err != nil {
		return nil, err
	}
	clean := make(map[string]any, len(args))
	for key, value := range args {
		if strings.HasPrefix(key, "_melta_") {
			continue
		}
		clean[key] = value
	}
	if value, present := clean["input"]; present {
		if _, ok := value.(string); !ok {
			return nil, errors.New("apply_patch: input must be a string")
		}
	}
	return json.Marshal(clean)
}

func (Tool) Prepare(ctx context.Context, author extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	args, err := decodeArguments(call.Arguments)
	if err != nil {
		return invalid(call.ID, err)
	}
	input, _ := args["input"].(string)
	plan, root, permissions, err := preparePlan(ctx, author.Call, input)
	if err != nil {
		return denied(call.ID)
	}
	plan.CallID = call.ID
	data, err := json.Marshal(plan)
	if err != nil {
		return denied(call.ID)
	}
	return extension.ToolPlan{Permissions: permissions, CanonicalWorkspace: root, Data: data}
}

func (Tool) Execute(ctx context.Context, author extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	var prepared planData
	if err := json.Unmarshal(plan.Data, &prepared); err != nil {
		return harness.ToolOutcome{Result: model.ToolResult{CallID: prepared.CallID, Status: model.ResultError, Content: err.Error()}}
	}
	summary, previews, err := executePlan(ctx, author.Call, prepared)
	if err != nil {
		content := err.Error()
		if summary != "" {
			content = summary + "\n\n" + content
		}
		return harness.ToolOutcome{Result: model.ToolResult{CallID: prepared.CallID, Status: model.ResultError, Content: content}}
	}
	outcome := harness.ToolOutcome{Result: model.ToolResult{CallID: prepared.CallID, Status: model.ResultSuccess, Content: summary}}
	if metadata := previewMetadata(previews); metadata != nil {
		if raw, marshalErr := json.Marshal(metadata); marshalErr == nil {
			outcome.Metadata = raw
		}
	}
	return outcome
}

// invalid settles an undecodable prepared argument set as an ordinary
// validation error.
func invalid(callID string, cause error) extension.ToolPlan {
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: cause.Error()}}}
}

// denied settles a failed preparation as the fixed permission denial.
func denied(callID string) extension.ToolPlan {
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultDenied, Content: harness.PermissionDeniedContent}}}
}

// decodeArguments strictly decodes one call's JSON arguments into an owned
// object, preserving every numeric lexeme.
func decodeArguments(raw json.RawMessage) (map[string]any, error) {
	var args map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&args); err != nil {
		return nil, fmt.Errorf("arguments must be a JSON object: %w", err)
	}
	if args == nil {
		return nil, errors.New("arguments must be a JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("arguments must be one JSON object")
	}
	return args, nil
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
