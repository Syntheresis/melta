package extension

import (
	"encoding/json"
	"errors"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// ErrClosed reports a call whose activation, registration or callback
// boundary has ended.
var ErrClosed = errors.New("extension: closed")

// ErrDenied reports a call outside what its kind, its grants or its target
// allow.
var ErrDenied = errors.New("extension: denied")

// ToolPlan is one prepared tool call. Data is opaque bytes owned by the tool
// and returned to Execute unchanged; the host never interprets it.
type ToolPlan struct {
	Permissions        []harness.PermissionRequest
	CanonicalWorkspace string
	Immediate          *harness.ToolOutcome
	Data               []byte
	Start              *SessionStart
}

// Owned returns an independent copy of the plan and its byte fields.
func (p ToolPlan) Owned() ToolPlan {
	out := p
	out.Permissions = append([]harness.PermissionRequest(nil), p.Permissions...)
	if p.Immediate != nil {
		immediate := *p.Immediate
		immediate.Metadata = append(json.RawMessage(nil), p.Immediate.Metadata...)
		immediate.Result.Content = p.Immediate.Result.Content
		out.Immediate = &immediate
	}
	out.Data = append([]byte(nil), p.Data...)
	if p.Start != nil {
		start := *p.Start
		start.Inputs = p.Start.Inputs.Owned()
		start.InitialState = append(harness.SessionStateValue(nil), p.Start.InitialState...)
		start.Input = append([]model.ContentPart(nil), p.Start.Input...)
		for i := range start.Input {
			start.Input[i].Extra = start.Input[i].Extra.Clone()
		}
		out.Start = &start
	}
	return out
}

// Result is one method result: exactly a schema-valid success value or one
// declared failure.
type Result struct {
	Value   json.RawMessage `json:"value,omitempty"`
	Failure *Failure        `json:"failure,omitempty"`
}

// Failure is the common domain-failure arm: nonempty Code and Message are
// always valid, while Data and Partial are independent optional payloads
// governed by their method's own declared Error and Partial schemas. A
// zero-length payload is absent; any nonempty bytes, whitespace included, are
// a present payload and must satisfy the declared schema.
type Failure struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
	Partial json.RawMessage `json:"partial,omitempty"`
}
