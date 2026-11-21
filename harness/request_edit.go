package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Syntheresis/melta/model"
)

func validateRequestParameters(parameters model.Extra) error {
	for key, value := range parameters {
		if strings.HasPrefix(key, "_melta_") || slices.Contains(model.ReservedBodyKeys(), key) || !json.Valid(value) {
			return fmt.Errorf("invalid request body parameter %q", key)
		}
	}
	return nil
}

func mergeRequestParameters(base, assignments model.Extra) model.Extra {
	out := make(model.Extra, len(base)+len(assignments))
	maps.Copy(out, base.Clone())
	maps.Copy(out, assignments.Clone())
	return out
}

// requestMessageBase allows a cold read to address checkpoint and tail
// independently, without making a full prefix copy for every request.
type requestMessageBase struct {
	checkpoint, tail             []model.Message
	checkpointOwners, tailOwners []int
	tailOffset                   int
}

func (b requestMessageBase) len() int { return len(b.checkpoint) + len(b.tail) }
func (b requestMessageBase) get(position int) model.Message {
	if position < len(b.checkpoint) {
		return b.checkpoint[position]
	}
	return b.tail[position-len(b.checkpoint)]
}

func validateRequestEdits(edits []RequestMessageEdit) error {
	for i, edit := range edits {
		switch {
		case edit.Position != nil && edit.Message == nil && *edit.Position >= 0:
			for _, part := range edit.Content {
				if err := model.ValidateContentPart(part); err != nil {
					return fmt.Errorf("edits[%d]: %w", i, err)
				}
			}
		case edit.Position == nil && edit.Message != nil && edit.Content == nil:
			if _, err := validateContributionMessage(*edit.Message); err != nil {
				return fmt.Errorf("edits[%d]: %w", i, err)
			}
		default:
			return fmt.Errorf("edits[%d] must address a position or add a message", i)
		}
	}
	return nil
}

// validateEditedPositions admits only authentic source pairings. The source
// may reuse an ID after a result, so the result is tied to the exact source
// assistant occurrence, not just the ID.
func validateEditedPositions(base requestMessageBase, edits []RequestMessageEdit) error {
	seen := make(map[int]bool, len(edits))
	outstanding := make(map[string]int)
	for _, edit := range edits {
		if edit.Position == nil {
			continue
		}
		p := *edit.Position
		if p < 0 || p >= base.len() || seen[p] {
			return fmt.Errorf("invalid or duplicate request message position %d", p)
		}
		seen[p] = true
		m := base.get(p)
		for _, call := range m.ToolCalls {
			if _, ok := outstanding[call.ID]; ok {
				return fmt.Errorf("request edit repeats still-outstanding tool call %q", call.ID)
			}
			outstanding[call.ID] = p
		}
		if m.ToolCallID != "" {
			sourceOwner := -1
			if p < len(base.checkpoint) {
				sourceOwner = base.checkpointOwners[p]
			} else {
				if base.tailOwners == nil {
					// Live and summary bases are uncut tails.
					base.tailOwners = toolResultOwners(base.tail)
				}
				if owner := base.tailOwners[p-len(base.checkpoint)]; owner >= base.tailOffset {
					sourceOwner = owner - base.tailOffset + len(base.checkpoint)
				}
			}
			owner, ok := outstanding[m.ToolCallID]
			if !ok || owner != sourceOwner {
				return fmt.Errorf("request edit position %d has no authentic outstanding tool call %q", p, m.ToolCallID)
			}
			delete(outstanding, m.ToolCallID)
		}
	}
	for id := range outstanding {
		return fmt.Errorf("request edit omits a result for tool call %q", id)
	}
	return nil
}

func toolResultOwners(messages []model.Message) []int {
	owners := make([]int, len(messages))
	open := make(map[string][]int)
	for i, m := range messages {
		owners[i] = -1
		for _, call := range m.ToolCalls {
			open[call.ID] = append(open[call.ID], i)
		}
		if queue := open[m.ToolCallID]; m.ToolCallID != "" && len(queue) != 0 {
			owners[i] = queue[0]
			if len(queue) == 1 {
				delete(open, m.ToolCallID)
			} else {
				open[m.ToolCallID] = queue[1:]
			}
		}
	}
	return owners
}

func materializeRequestEdits(base []model.Message, edits []RequestMessageEdit) []model.Message {
	if edits == nil {
		return base
	}
	messages := make([]model.Message, 0, len(edits))
	for _, edit := range edits {
		if edit.Position == nil {
			messages = append(messages, *edit.Message)
			continue
		}
		m := base[*edit.Position]
		if edit.Content != nil {
			m.Content = edit.Content
		}
		messages = append(messages, m)
	}
	return messages
}

func cloneRequestEdits(edits []RequestMessageEdit) []RequestMessageEdit {
	out := slices.Clone(edits)
	for i, edit := range edits {
		if edit.Position != nil {
			position := *edit.Position
			out[i].Position = &position
		}
		if edit.Content != nil {
			out[i].Content = append([]model.ContentPart{}, ownContentParts(edit.Content)...)
		}
		if edit.Message != nil {
			m := cloneMessageValue(*edit.Message)
			out[i].Message = &m
		}
	}
	return out
}

func encodeRequestEdits(edits []RequestMessageEdit) (json.RawMessage, error) {
	items := make([]map[string]json.RawMessage, 0, len(edits))
	for _, edit := range edits {
		obj := make(map[string]json.RawMessage, 2)
		if edit.Position != nil {
			obj["position"] = mustMarshal(*edit.Position)
			if edit.Content != nil {
				content := make([]json.RawMessage, 0, len(edit.Content))
				for _, part := range edit.Content {
					raw, err := encodeContentPart(part)
					if err != nil {
						return nil, err
					}
					content = append(content, raw)
				}
				obj["content"] = mustMarshal(content)
			}
		} else {
			raw, err := encodeMessageValue(*edit.Message)
			if err != nil {
				return nil, err
			}
			obj["message"] = raw
		}
		items = append(items, obj)
	}
	return json.Marshal(items)
}

func decodeRequestEdits(obj map[string]json.RawMessage) ([]RequestMessageEdit, error) {
	if _, ok := obj["edits"]; !ok {
		return nil, nil
	}
	items, err := arrayMember(obj, "edits", true)
	if err != nil {
		return nil, err
	}
	out := make([]RequestMessageEdit, 0, len(items))
	for _, item := range items {
		fields, err := decodePayloadObject(item)
		if err != nil {
			return nil, err
		}
		if err := rejectUnknownMembers(fields, "position", "content", "message"); err != nil {
			return nil, err
		}
		var edit RequestMessageEdit
		if raw, ok := fields["position"]; ok {
			var position int
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, &position) != nil {
				return nil, fmt.Errorf("edit position must be an integer")
			}
			edit.Position = &position
		}
		if _, ok := fields["content"]; ok {
			parts, err := arrayMember(fields, "content", true)
			if err != nil {
				return nil, err
			}
			edit.Content = make([]model.ContentPart, 0, len(parts))
			for _, part := range parts {
				value, err := decodeContentPart(part)
				if err != nil {
					return nil, err
				}
				edit.Content = append(edit.Content, value)
			}
		}
		if raw, ok := fields["message"]; ok {
			message, err := decodeMessageValue(raw)
			if err != nil {
				return nil, err
			}
			edit.Message = &message
		}
		out = append(out, edit)
	}
	return out, nil
}

func encodeRequestTools(tools []model.ToolDefinition) (json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(tools))
	for _, tool := range tools {
		raw, err := encodeToolDefinition(tool)
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return json.Marshal(out)
}

func decodeRequestTools(obj map[string]json.RawMessage) ([]model.ToolDefinition, error) {
	if _, ok := obj["tools"]; !ok {
		return nil, nil
	}
	items, err := arrayMember(obj, "tools", true)
	if err != nil {
		return nil, err
	}
	out := make([]model.ToolDefinition, 0, len(items))
	for _, item := range items {
		tool, err := decodeToolDefinition(item)
		if err != nil {
			return nil, err
		}
		out = append(out, tool)
	}
	return out, nil
}

func encodeRequestInputs(obj map[string]json.RawMessage, edits []RequestMessageEdit, tools []model.ToolDefinition, parameters model.Extra) (err error) {
	if edits != nil {
		if obj["edits"], err = encodeRequestEdits(edits); err != nil {
			return err
		}
	}
	if tools != nil {
		if obj["tools"], err = encodeRequestTools(tools); err != nil {
			return err
		}
	}
	if parameters != nil {
		obj["parameters"], err = marshalExtra(parameters)
	}
	return err
}

func decodeRequestInputs(obj map[string]json.RawMessage, edits *[]RequestMessageEdit, tools *[]model.ToolDefinition, parameters *model.Extra) (err error) {
	if *edits, err = decodeRequestEdits(obj); err != nil {
		return err
	}
	if *tools, err = decodeRequestTools(obj); err != nil {
		return err
	}
	*parameters, err = extraMember(obj, "parameters", false)
	return err
}
