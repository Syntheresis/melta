package extension

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type recordingAPI struct {
	API
	call func(context.Context, string, string, json.RawMessage) (Result, error)
}

func (api recordingAPI) Settings() json.RawMessage { return json.RawMessage(`{"enabled":true}`) }
func (api recordingAPI) Call(ctx context.Context, service, method string, args json.RawMessage) (Result, error) {
	return api.call(ctx, service, method, args)
}

func TestRunTool(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		err      error
		status   model.ToolResultStatus
		evidence harness.ExecutionEvidence
	}{
		{"success", context.Background(), nil, model.ResultSuccess, harness.ExecutionObserved},
		{"error", context.Background(), errors.New("failed"), model.ResultError, harness.ExecutionObserved},
		{"empty error", context.Background(), errors.New(""), model.ResultError, harness.ExecutionObserved},
		{"unknown", context.Background(), ErrOutcomeUnknown, model.ResultError, harness.ExecutionUnknown},
		{"unknown cancellation", cancelled, fmt.Errorf("%w: %w", ErrOutcomeUnknown, context.Canceled), model.ResultInterrupted, harness.ExecutionUnknown},
		{"cancellation", cancelled, context.Canceled, model.ResultInterrupted, harness.ExecutionObserved},
		{"late success", cancelled, nil, model.ResultSuccess, harness.ExecutionObserved},
		{"late unrelated error", cancelled, errors.New("failed"), model.ResultError, harness.ExecutionObserved},
		{"live cancellation", context.Background(), context.Canceled, model.ResultError, harness.ExecutionObserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := RunTool(tc.ctx, func(context.Context, ToolCall) (ToolResult, error) {
				return ToolResult{Content: "ok", Metadata: json.RawMessage(`{"x":1}`)}, tc.err
			}, ToolCall{ID: "id"})
			if out.Result.CallID != "id" || out.Result.Status != tc.status || out.Evidence != tc.evidence {
				t.Fatalf("outcome = %+v", out)
			}
			if tc.name == "empty error" && out.Result.Content != "Tool call failed." {
				t.Fatalf("empty error content = %q", out.Result.Content)
			}
		})
	}
	out := RunTool(context.Background(), func(context.Context, ToolCall) (ToolResult, error) {
		return ToolResult{}, errors.New(string([]byte{'x', 0xff}))
	}, ToolCall{ID: "id"})
	if !utf8.ValidString(out.Result.Content) || !strings.HasPrefix(out.Result.Content, "x") {
		t.Fatalf("invalid UTF-8 error: %q", out.Result.Content)
	}
}

func TestToolCallDecode(t *testing.T) {
	var value map[string]any
	if err := (ToolCall{Arguments: json.RawMessage(`{"n":9007199254740993}`)}).Decode(&value); err != nil || value["n"] != json.Number("9007199254740993") {
		t.Fatalf("decoded %v, %v", value, err)
	}
	for _, raw := range []string{`[]`, `null`, `{} {}`, `{`} {
		if err := (ToolCall{Arguments: json.RawMessage(raw)}).Decode(&value); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

func TestSessionStartCodec(t *testing.T) {
	instructions, prompt := "instructions", "prompt"
	start := SessionStart{
		Inputs:       harness.SessionInputs{Tools: []string{"a", "a"}, Strategy: &harness.StrategySelection{ID: "x", Settings: json.RawMessage("{")}},
		InitialState: harness.SessionStateValue(`{"n":9007199254740993}`),
		Input:        []model.ContentPart{{Kind: model.PartText, Text: "hello", Extra: model.Extra{"x": json.RawMessage(`{"a":1}`)}}},
		Report:       true, OutputLimit: 10,
	}
	raw, err := EncodeSessionStart(start)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSessionStart(raw)
	if err != nil || !reflect.DeepEqual(got, start) {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	start.Inputs.Model = &model.ModelRef{Provider: "incomplete"}
	start.Inputs.Instructions, start.Inputs.Prompt = &instructions, &prompt
	start.Inputs.Strategy = &harness.StrategySelection{Settings: json.RawMessage{0xff, '{'}}
	raw, err = EncodeSessionStart(start)
	if err != nil {
		t.Fatal(err)
	}
	got, err = DecodeSessionStart(raw)
	if err != nil || !reflect.DeepEqual(got, start) {
		t.Fatalf("partial model and opaque strategy round trip: %+v, %v", got, err)
	}
	start.Inputs.Model = &model.ModelRef{Provider: "provider", Model: "model"}
	raw, err = EncodeSessionStart(start)
	if err != nil {
		t.Fatal(err)
	}
	got, err = DecodeSessionStart(raw)
	if err != nil || !reflect.DeepEqual(got, start) {
		t.Fatalf("complete model round trip: %+v, %v", got, err)
	}
}

func TestTypedCalls(t *testing.T) {
	ctx := context.Background()
	start := SessionStart{}
	startDoc, err := EncodeSessionStart(start)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		service, method, args, result string
		run                           func(AuthorContext) error
	}{
		{ContractWorkspace, "stat", `{"path":"x"}`, `{"canonical_path":"/x","exists":true}`, func(c AuthorContext) error {
			v, err := c.StatFile(ctx, "x")
			if err == nil && (v.Path != "/x" || !v.Exists) {
				return errors.New("bad stat")
			}
			return err
		}},
		{ContractWorkspace, "read", `{"path":"x"}`, `{"bytes":"YQ=="}`, func(c AuthorContext) error {
			v, err := c.ReadFile(ctx, "x")
			if err == nil && string(v) != "a" {
				return errors.New("bad read")
			}
			return err
		}},
		{ContractWorkspace, "write", `{"path":"x","bytes":"YQ==","create_only":false}`, `{"outcome":"completed"}`, func(c AuthorContext) error { return c.WriteFile(ctx, "x", []byte("a")) }},
		{ContractWorkspace, "write", `{"path":"x","bytes":"","create_only":true}`, `{"outcome":"completed"}`, func(c AuthorContext) error { return c.CreateFile(ctx, "x", nil) }},
		{ContractWorkspace, "delete", `{"path":"x"}`, `{"outcome":"completed"}`, func(c AuthorContext) error { return c.DeleteFile(ctx, "x") }},
		{ContractState, "set", `{"value":{"n":1}}`, `null`, func(c AuthorContext) error { return c.SetState(ctx, json.RawMessage(`{"n":1}`)) }},
		{ContractSession, "members", `{}`, `[{"kind":"session","id":"id","starter":"ext"}]`, func(c AuthorContext) error {
			v, err := c.Members(ctx)
			if err == nil && (len(v) != 1 || v[0].ID != "id") {
				return errors.New("bad members")
			}
			return err
		}},
		{ContractSession, "start", string(startDoc), `{"session_id":"id"}`, func(c AuthorContext) error {
			v, err := c.StartSession(ctx, start)
			if err == nil && v != "id" {
				return errors.New("bad id")
			}
			return err
		}},
		{ContractSettings, "replace", `{"section":{"enabled":true}}`, `null`, func(c AuthorContext) error { return c.ReplaceSettings(ctx, json.RawMessage(`{"enabled":true}`)) }},
		{ContractCredentials, "get", `{"field":"key"}`, `{"value":"secret"}`, func(c AuthorContext) error {
			v, err := c.Credential(ctx, "key")
			if err == nil && v != "secret" {
				return errors.New("bad credential")
			}
			return err
		}},
		{ContractData, "session_namespace", `{}`, `{"directory":"/session"}`, func(c AuthorContext) error {
			v, err := c.SessionDataDir(ctx)
			if err == nil && v != "/session" {
				return errors.New("bad directory")
			}
			return err
		}},
		{ContractData, "cache_namespace", `{}`, `{"directory":"/cache"}`, func(c AuthorContext) error {
			v, err := c.CacheDir(ctx)
			if err == nil && v != "/cache" {
				return errors.New("bad directory")
			}
			return err
		}},
		{ContractData, "claim_external_data", `{}`, `null`, func(c AuthorContext) error { return c.ClaimExternalData(ctx) }},
		{ContractWarnings, "report", `{"kind":"k","message":"m"}`, `null`, func(c AuthorContext) error { return c.ReportWarning(ctx, "k", "m") }},
		{ContractCommands, "submit", `{}`, `{"id":"id"}`, func(c AuthorContext) error {
			var v struct {
				ID string `json:"id"`
			}
			err := c.Command(ctx, "submit", nil, &v)
			if err == nil && v.ID != "id" {
				return errors.New("bad command")
			}
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.service+"/"+tc.method, func(t *testing.T) {
			api := recordingAPI{call: func(_ context.Context, service, method string, args json.RawMessage) (Result, error) {
				if service != tc.service || method != tc.method || !bytes.Equal(args, []byte(tc.args)) {
					t.Fatalf("call = %s %s %s", service, method, args)
				}
				return Result{Value: json.RawMessage(tc.result)}, nil
			}}
			for _, c := range []AuthorContext{{Call: api.Call}, Activation(api)} {
				if err := tc.run(c); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestTypedCallPartial(t *testing.T) {
	cause := errors.New("failed")
	callErr := &CallError{Err: cause, Partial: json.RawMessage(`{"outcome":"uncertain"}`)}
	c := AuthorContext{Call: func(context.Context, string, string, json.RawMessage) (Result, error) { return Result{}, callErr }}
	for _, run := range []func() error{
		func() error { return c.WriteFile(context.Background(), "x", nil) },
		func() error { return c.CreateFile(context.Background(), "x", nil) },
		func() error { return c.DeleteFile(context.Background(), "x") },
	} {
		err := run()
		var got *CallError
		if !errors.Is(err, ErrOutcomeUnknown) || !errors.As(err, &got) || got != callErr {
			t.Fatalf("error = %v", err)
		}
	}
	c.Call = func(context.Context, string, string, json.RawMessage) (Result, error) {
		return Result{}, &CallError{Err: cause, Partial: json.RawMessage(`{"session_id":"id"}`)}
	}
	id, err := c.StartSession(context.Background(), SessionStart{})
	if id != "id" || !errors.Is(err, cause) {
		t.Fatalf("start = %q, %v", id, err)
	}
	c.Call = func(context.Context, string, string, json.RawMessage) (Result, error) {
		return Result{Failure: &Failure{Code: "refused", Message: "no", Partial: json.RawMessage(`{"session_id":"partial"}`)}}, nil
	}
	id, err = c.StartSession(context.Background(), SessionStart{})
	if id != "partial" || err == nil || !strings.Contains(err.Error(), "refused: no") {
		t.Fatalf("declared failure start = %q, %v", id, err)
	}
	c.Call = func(context.Context, string, string, json.RawMessage) (Result, error) {
		return Result{Failure: &Failure{Code: "refused", Message: "no"}}, nil
	}
	if err := c.DeleteFile(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "refused") || !strings.Contains(err.Error(), "no") {
		t.Fatalf("failure = %v", err)
	}
}
