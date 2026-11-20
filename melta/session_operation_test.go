package melta

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// TestGetSessionOperationProjectsCapturedInputs proves the Operation read
// returns the admitted execution inputs — the captured model, a
// hook-replaced prompt, the admitted tool subset in admitted order and the
// selected strategy — and serializes no private implementation, extension
// state or settings fields.
func TestGetSessionOperationProjectsCapturedInputs(t *testing.T) {
	e := newOwnerEnv(t)
	enableFixturePlugin(t, e.configPath, "hook")
	e.server.setScript(func(context.Context, string) []string { return textTurnEvents("done") })
	hook := testNative("hook", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachAdmission(func(_ context.Context, _ extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
			candidate := input.Candidate
			replaced := "replaced prompt"
			candidate.Prompt = &replaced
			return extension.AdmissionResult{Candidate: &candidate}, nil
		}, nil)
		return nil, err
	})
	ctx := context.Background()
	r, err := e.open(ctx, hook)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	tools := []string{"write_file", "read_file"}
	session, err := r.CreateSession(ctx, testCaller(r), e.dataDir, &protocol.SessionInputs{Tools: &tools})
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.SessionId, "op-read", "read")
	awaitOperation(t, r, session.SessionId, "op-read", harness.OperationSuccess)

	read, err := r.GetSessionOperation(ctx, testCaller(r), session.SessionId, "op-read")
	testassert.NoError(t, err)
	testassert.Require(t, read.Operation.OperationId == "op-read" && read.Operation.Status == protocol.OperationStatusSuccess, "operation = %+v", read.Operation)
	testassert.Equal(t, string(read.Inputs.Model), "prov/m")
	testassert.Equal(t, read.Inputs.Prompt, "replaced prompt")
	testassert.Equal(t, read.Inputs.Strategy.Id, harness.IncludedStrategyID)
	names := make([]string, 0, len(read.Inputs.Tools))
	for _, tool := range read.Inputs.Tools {
		names = append(names, tool.Name)
		testassert.Require(t, tool.Description != "" && len(tool.Parameters) != 0, "tool %q lost its definition: %+v", tool.Name, tool)
	}
	testassert.Require(t, strings.Join(names, ",") == "write_file,read_file", "admitted tools = %v, want the admitted selection order", names)

	wire, err := json.Marshal(read)
	testassert.NoError(t, err)
	for _, absent := range []string{"implementations", `"state"`, `"extensions"`} {
		testassert.Require(t, !strings.Contains(string(wire), absent), "Operation read carries %s: %s", absent, wire)
	}
	var doc struct {
		Inputs map[string]json.RawMessage `json:"inputs"`
	}
	testassert.NoError(t, json.Unmarshal(wire, &doc))
	for _, key := range []string{"model", "prompt", "tools", "strategy"} {
		_, ok := doc.Inputs[key]
		testassert.Require(t, ok, "Operation inputs lost %q: %s", key, wire)
	}
	testassert.Require(t, len(doc.Inputs) == 4, "Operation inputs carry extra keys: %s", wire)
}

// TestGetSessionOperationScopesBySession proves the read is a Session read:
// another Session's Operation is not found for a caller authorized over this
// Session, an unknown Operation is not found, and a caller without this
// Session's grant is denied.
func TestGetSessionOperationScopesBySession(t *testing.T) {
	e := newOwnerEnv(t)
	e.server.setScript(func(context.Context, string) []string { return textTurnEvents("done") })
	ctx := context.Background()
	r, err := e.open(ctx)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	first, err := r.CreateSession(ctx, testCaller(r), e.dataDir, nil)
	testassert.NoError(t, err)
	second, err := r.CreateSession(ctx, testCaller(r), e.dataDir, nil)
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, first.SessionId, "op-first", "first")
	awaitOperation(t, r, first.SessionId, "op-first", harness.OperationSuccess)

	{
		_, err := r.GetSessionOperation(ctx, testCaller(r), second.SessionId, "op-first")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "other Session's Operation = %v, want harness.ErrNotFound", err)
	}
	{
		_, err := r.GetSessionOperation(ctx, testCaller(r), first.SessionId, "op-missing")
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "unknown Operation = %v, want harness.ErrNotFound", err)
	}
	other, err := r.Caller(CallerContext{Origin: harness.InputOriginUser, Scope: CallerScope{Kind: CallerScopeSession, SessionID: second.SessionId}})
	testassert.NoError(t, err)
	{
		_, err := r.GetSessionOperation(ctx, other, first.SessionId, "op-first")
		testassert.Require(t, errors.Is(err, ErrDenied), "other Session caller = %v, want ErrDenied", err)
	}
}

// TestProjectOperationInputsDefaultAll proves a capture without an explicit
// tool selection advertises every captured tool in captured order.
func TestProjectOperationInputsDefaultAll(t *testing.T) {
	capture := harness.ExecutionCapture{
		Model:        model.ModelRef{Provider: "prov", Model: "m"},
		SystemPrompt: "prompt",
		Tools: []model.ToolDefinition{
			{Name: "z_tool", Description: "z", Parameters: json.RawMessage(`{"type":"object"}`)},
			{Name: "a_tool", Description: "a", Parameters: json.RawMessage(`{"type":"object"}`)},
		},
		InitialToolNames: nil,
		Strategy:         harness.StrategyCapture{ID: harness.IncludedStrategyID, Settings: json.RawMessage(`{}`)},
	}
	inputs, err := projectOperationInputs(capture)
	testassert.NoError(t, err)
	names := make([]string, 0, len(inputs.Tools))
	for _, tool := range inputs.Tools {
		names = append(names, tool.Name)
	}
	testassert.Require(t, strings.Join(names, ",") == "z_tool,a_tool", "default-all tools = %v, want the captured order", names)
}
