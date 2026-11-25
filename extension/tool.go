package extension

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type ToolFunc func(context.Context, ToolCall) (ToolResult, error)

type ToolCall struct {
	AuthorContext
	ID        string
	Name      string
	Arguments json.RawMessage
}

type ToolResult struct {
	Content  string
	Metadata json.RawMessage
}

var ErrOutcomeUnknown = errors.New("extension: outcome unknown")

func (c ToolCall) Decode(v any) error {
	raw := bytes.TrimSpace(c.Arguments)
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("tool arguments must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return errors.New("tool arguments contain trailing JSON")
		}
		return err
	}
	return nil
}

func RunTool(ctx context.Context, f ToolFunc, call ToolCall) harness.ToolOutcome {
	result, err := f(ctx, call)
	out := harness.ToolOutcome{
		Result:   model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: result.Content},
		Metadata: result.Metadata, Evidence: harness.ExecutionObserved,
	}
	if err != nil {
		out.Metadata = nil
		out.Result.Status = model.ResultError
		out.Result.Content = strings.ToValidUTF8(err.Error(), "\uFFFD")
		if out.Result.Content == "" {
			out.Result.Content = "Tool call failed."
		}
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			out.Result.Status = model.ResultInterrupted
		}
		if errors.Is(err, ErrOutcomeUnknown) {
			out.Evidence = harness.ExecutionUnknown
		}
	}
	return out
}

// Activation supplies the entry point's settings and service calls.
func Activation(api API) AuthorContext {
	return AuthorContext{Settings: api.Settings(), Call: api.Call}
}

type FileStat struct {
	Path   string
	Exists bool
}

type Member struct {
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Starter string `json:"starter"`
}

func (c AuthorContext) invoke(ctx context.Context, service, method string, request, value any) error {
	args, err := json.Marshal(request)
	if err != nil {
		return err
	}
	result, err := c.Call(ctx, service, method, args)
	if err != nil {
		return err
	}
	if result.Failure != nil {
		return &CallError{Err: fmt.Errorf("%s: %s", result.Failure.Code, result.Failure.Message), Partial: result.Failure.Partial}
	}
	if value != nil {
		return json.Unmarshal(result.Value, value)
	}
	return nil
}

func (c AuthorContext) StatFile(ctx context.Context, path string) (FileStat, error) {
	var result struct {
		Path   string `json:"canonical_path"`
		Exists bool   `json:"exists"`
	}
	err := c.invoke(ctx, ContractWorkspace, "stat", struct {
		Path string `json:"path"`
	}{path}, &result)
	return FileStat{Path: result.Path, Exists: result.Exists}, err
}

func (c AuthorContext) ReadFile(ctx context.Context, path string) ([]byte, error) {
	var result struct {
		Bytes []byte `json:"bytes"`
	}
	err := c.invoke(ctx, ContractWorkspace, "read", struct {
		Path string `json:"path"`
	}{path}, &result)
	return result.Bytes, err
}

func (c AuthorContext) writeFile(ctx context.Context, path string, content []byte, createOnly bool) error {
	err := c.invoke(ctx, ContractWorkspace, "write", struct {
		Path       string    `json:"path"`
		Bytes      WireBytes `json:"bytes"`
		CreateOnly bool      `json:"create_only"`
	}{path, WireBytes(content), createOnly}, nil)
	return uncertainMutation(err)
}

func (c AuthorContext) WriteFile(ctx context.Context, path string, content []byte) error {
	return c.writeFile(ctx, path, content, false)
}

func (c AuthorContext) CreateFile(ctx context.Context, path string, content []byte) error {
	return c.writeFile(ctx, path, content, true)
}

func (c AuthorContext) DeleteFile(ctx context.Context, path string) error {
	return uncertainMutation(c.invoke(ctx, ContractWorkspace, "delete", struct {
		Path string `json:"path"`
	}{path}, nil))
}

func uncertainMutation(err error) error {
	var callErr *CallError
	if errors.As(err, &callErr) {
		var partial struct {
			Outcome string `json:"outcome"`
		}
		if json.Unmarshal(callErr.Partial, &partial) == nil && partial.Outcome == "uncertain" {
			return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
		}
	}
	return err
}

func (c AuthorContext) SetState(ctx context.Context, value json.RawMessage) error {
	return c.invoke(ctx, ContractState, "set", struct {
		Value json.RawMessage `json:"value"`
	}{value}, nil)
}

func (c AuthorContext) Members(ctx context.Context) ([]Member, error) {
	var members []Member
	err := c.invoke(ctx, ContractSession, "members", struct{}{}, &members)
	return members, err
}

func (c AuthorContext) StartSession(ctx context.Context, start SessionStart) (string, error) {
	args, err := EncodeSessionStart(start)
	if err != nil {
		return "", err
	}
	var value struct {
		SessionID string `json:"session_id"`
	}
	err = c.invoke(ctx, ContractSession, "start", args, &value)
	if err != nil {
		var callErr *CallError
		if errors.As(err, &callErr) {
			_ = json.Unmarshal(callErr.Partial, &value)
		}
		return value.SessionID, err
	}
	return value.SessionID, err
}

func (c AuthorContext) ReplaceSettings(ctx context.Context, section json.RawMessage) error {
	return c.invoke(ctx, ContractSettings, "replace", struct {
		Section json.RawMessage `json:"section"`
	}{section}, nil)
}

func (c AuthorContext) Credential(ctx context.Context, field string) (string, error) {
	var result struct {
		Value string `json:"value"`
	}
	err := c.invoke(ctx, ContractCredentials, "get", struct {
		Field string `json:"field"`
	}{field}, &result)
	return result.Value, err
}

func (c AuthorContext) directory(ctx context.Context, method string) (string, error) {
	var result struct {
		Directory string `json:"directory"`
	}
	err := c.invoke(ctx, ContractData, method, struct{}{}, &result)
	return result.Directory, err
}

func (c AuthorContext) SessionDataDir(ctx context.Context) (string, error) {
	return c.directory(ctx, "session_namespace")
}
func (c AuthorContext) CacheDir(ctx context.Context) (string, error) {
	return c.directory(ctx, "cache_namespace")
}
func (c AuthorContext) ClaimExternalData(ctx context.Context) error {
	return c.invoke(ctx, ContractData, "claim_external_data", struct{}{}, nil)
}
func (c AuthorContext) ReportWarning(ctx context.Context, kind, message string) error {
	return c.invoke(ctx, ContractWarnings, "report", struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}{kind, message}, nil)
}
func (c AuthorContext) Command(ctx context.Context, method string, request, result any) error {
	if request == nil {
		request = struct{}{}
	}
	return c.invoke(ctx, ContractCommands, method, request, result)
}
