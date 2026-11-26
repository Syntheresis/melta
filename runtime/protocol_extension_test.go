package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/melta"
	"github.com/Syntheresis/melta/protocol"
)

// extensionOwnerOptions mounts the shared native fixture and one
// zero-capability extension over a value configuration.
func extensionOwnerOptions(t *testing.T) melta.Options {
	t.Helper()
	dataDir := t.TempDir()
	empty := extension.Extension{
		Manifest: extension.Manifest{ID: "empty", Version: "1.0.0", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
			return nil, nil
		},
	}
	fixture := extension.Extension{
		Manifest: extension.Manifest{ID: "fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`), Services: []string{"melta.commands"}},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			operations := []extension.Operation{
				{Target: extension.ExposedTargetHarness, Method: extension.ServiceMethod("raw", `{}`, `{}`, "", ""), Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return extension.Result{Value: append(json.RawMessage(nil), args...)}, nil
				}},
				{Target: extension.ExposedTargetHarness, Method: extension.ServiceMethod("echo", `{"type":"object"}`, `{}`, "", ""), Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return extension.Result{Value: args}, nil
				}},
				{Target: extension.ExposedTargetHarness, Method: extension.ServiceMethod("fail", `{}`, `{}`, `{}`, `{}`), Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
					return extension.Result{Failure: &extension.Failure{Code: "guest_code", Message: "guest message", Data: json.RawMessage("{\n  \"big\": 9007199254740993\n}"), Partial: json.RawMessage(`["partial", null, 1]`)}}, nil
				}},
				{Target: extension.ExposedTargetHarness, Method: extension.ServiceMethod("corrupt", `{}`, `{}`, "", ""), Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
					return extension.Result{}, fmt.Errorf("fixture corrupt: %w", harness.ErrCorrupt)
				}},
				{Target: extension.ExposedTargetHarness, Method: extension.ServiceMethod("call_error", `{}`, `{}`, "", ""), Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					var input struct {
						Mode string `json:"mode"`
					}
					_ = json.Unmarshal(args, &input)
					partial := json.RawMessage("{\n  \"big\": 9007199254740993,\n  \"text\": \"line\\\\nbreak\"\n}")
					switch input.Mode {
					case "bad":
						partial = json.RawMessage("{")
					case "null":
						partial = json.RawMessage("null")
					case "empty":
						partial = nil
					}
					return extension.Result{}, &extension.CallError{Err: fmt.Errorf("fixture call failure: %w", harness.ErrStorage), Partial: partial}
				}},
				{Target: extension.ExposedTargetSession, Method: extension.ServiceMethod("command", `{}`, `{}`, "", ""), Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					var input struct {
						Method    string          `json:"method"`
						Arguments json.RawMessage `json:"arguments"`
					}
					if err := json.Unmarshal(args, &input); err != nil {
						return extension.Result{}, err
					}
					return call.Call(ctx, "melta.commands", input.Method, input.Arguments)
				}},
			}
			for _, op := range operations {
				if _, err := api.RegisterOperation(op, nil); err != nil {
					return nil, err
				}
			}
			return nil, nil
		},
	}
	return melta.Options{
		DataDir:       dataDir,
		Configuration: melta.NewValueConfigurationSource([]byte(`{"extensions":{"fixture":{"enabled":true},"empty":{"enabled":true}}}`)),
		Credentials:   melta.NewEnvironmentCredentialSource(dataDir + "/.env"),
		Extensions:    []extension.Extension{fixture, empty},
	}
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	testassert.NoError(t, err)
	return string(body)
}

// TestProtocolExtensionCatalogAndInvocation proves the HTTP owner: the
// catalog includes the zero-capability extension and hidden methods, success
// and declared-failure bodies preserve exact raw values, and every invalid
// request refuses before dispatch.
func TestProtocolExtensionCatalogAndInvocation(t *testing.T) {
	ctx := context.Background()
	_, _, server := mountedOwner(t, ctx, extensionOwnerOptions(t))
	client := mountedClient(t, server)
	catalogResponse, err := client.ListExtensionsWithResponse(ctx)
	testassert.NoError(t, err)
	testassert.Require(t, catalogResponse.JSON200 != nil, "catalog status = %d, body = %s", catalogResponse.StatusCode(), catalogResponse.Body)
	catalog := *catalogResponse.JSON200
	var empty, fixture *protocol.RegisteredExtension
	for i := range catalog {
		switch catalog[i].ExtensionId {
		case "empty":
			empty = &catalog[i]
		case "fixture":
			fixture = &catalog[i]
		}
	}
	testassert.Require(t, empty != nil && len(empty.Tools) == 0 && len(empty.Operations) == 0, "empty catalog entry = %+v", empty)
	testassert.Require(t, fixture != nil, "fixture catalog entry is missing")
	var sawRaw, sawRetain bool
	for _, operation := range fixture.Operations {
		sawRetain = sawRetain || operation.Method.Name == "retain"
		sawRaw = sawRaw || operation.Method.Name == "raw" && operation.Target == protocol.RegisteredExtensionOperationTargetHarness
	}
	testassert.Require(t, sawRaw && !sawRetain, "catalog exposed hidden methods or omitted raw: %+v", fixture.Operations)

	invoke := func(body string) *http.Response {
		return rawMounted(t, http.MethodPost, server.Endpoint()+"/v1/extensions/invoke", server.credential, body)
	}
	target := `{"kind":"harness"}`
	rawArguments := "{\n  \"big\": 9007199254740993,\n  \"text\": \"line\\n\\t<>&\"\n}"
	success := invoke(`{"extension_id":"fixture","operation":"raw","target":` + target + `,"arguments":` + rawArguments + `,"request_id":"req-1"}`)
	testassert.Require(t, success.StatusCode == http.StatusOK, "raw success status = %d", success.StatusCode)
	successBody := readBody(t, success)
	testassert.Require(t, strings.HasPrefix(successBody, `{"kind":"success","result":`) && strings.Contains(successBody, rawArguments), "raw success body lost bytes: %q", successBody)

	nullSuccess := invoke(`{"extension_id":"fixture","operation":"raw","target":` + target + `,"arguments":null,"request_id":"req-null"}`)
	nullBody := readBody(t, nullSuccess)
	testassert.Require(t, nullSuccess.StatusCode == http.StatusOK, "null success status = %d body = %s", nullSuccess.StatusCode, nullBody)
	testassert.Require(t, nullBody == `{"kind":"success","result":null}`, "null success body = %q, want the exact null result", nullBody)

	failure := invoke(`{"extension_id":"fixture","operation":"fail","target":` + target + `,"arguments":{},"request_id":"req-fail"}`)
	testassert.Require(t, failure.StatusCode == http.StatusUnprocessableEntity, "declared failure status = %d", failure.StatusCode)
	failureBody := readBody(t, failure)
	testassert.Require(t, strings.Contains(failureBody, `"kind":"failure"`) && strings.Contains(failureBody, `"code":"guest_code"`) && strings.Contains(failureBody, `"message":"guest message"`) && strings.Contains(failureBody, "{\n  \"big\": 9007199254740993\n}") && strings.Contains(failureBody, `"partial_result":["partial", null, 1]`), "declared failure body lost exact payloads: %q", failureBody)

	baseline := invoke(`{"extension_id":"fixture","operation":"corrupt","target":` + target + `,"arguments":{},"request_id":"req-corrupt"}`)
	baselineBody := readBody(t, baseline)
	testassert.Require(t, baseline.StatusCode == http.StatusUnprocessableEntity, "baseline 422 status = %d body = %s", baseline.StatusCode, baselineBody)
	var baselineError protocol.Error
	testassert.NoError(t, json.Unmarshal([]byte(baselineBody), &baselineError))
	testassert.Require(t, baselineError.Code == protocol.ErrorCodeCorrupt, "baseline 422 code = %q, want corrupt", baselineError.Code)

	for _, rejection := range []struct {
		name   string
		body   string
		status int
	}{
		{"unknown member", `{"extension_id":"fixture","operation":"raw","target":` + target + `,"arguments":{},"request_id":"req-x","operation_id":"forged"}`, http.StatusBadRequest},
		{"missing routing field", `{"extension_id":"fixture","operation":"raw","target":` + target + `,"arguments":{}}`, http.StatusBadRequest},
		{"missing target", `{"extension_id":"fixture","operation":"raw","arguments":{},"request_id":"req-x"}`, http.StatusBadRequest},
		{"target other member", `{"extension_id":"fixture","operation":"raw","target":{"kind":"harness","session_id":"` + strings.Repeat("a", 32) + `"},"arguments":{},"request_id":"req-x"}`, http.StatusBadRequest},
		{"old contract field", `{"extension_id":"fixture","operation":"raw","contract_id":"other/v1","target":` + target + `,"arguments":{},"request_id":"req-x"}`, http.StatusBadRequest},
		{"old contract version", `{"extension_id":"fixture","operation":"raw","contract_version":"2","target":` + target + `,"arguments":{},"request_id":"req-x"}`, http.StatusBadRequest},
		{"arguments outside method schema", `{"extension_id":"fixture","operation":"echo","target":` + target + `,"arguments":null,"request_id":"req-x"}`, http.StatusBadRequest},
		{"unexposed method", `{"extension_id":"fixture","operation":"assemble","target":` + target + `,"arguments":{},"request_id":"req-x"}`, http.StatusNotFound},
		{"unknown extension", `{"extension_id":"unknown","operation":"x","target":` + target + `,"arguments":{},"request_id":"req-x"}`, http.StatusNotFound},
	} {
		response := invoke(rejection.body)
		testassert.Require(t, response.StatusCode == rejection.status, "%s status = %d, want %d; body = %s", rejection.name, response.StatusCode, rejection.status, readBody(t, response))
	}

	// A malformed generated command argument refuses through the fixed
	// service's own generated-value decoder with no state mutation.
	create := rawMounted(t, http.MethodPost, server.Endpoint()+"/v1/sessions", server.credential, `{"workspace":"`+t.TempDir()+`"}`)
	createBody := readBody(t, create)
	testassert.Require(t, create.StatusCode == http.StatusCreated, "create session status = %d body = %s", create.StatusCode, createBody)
	var session protocol.Session
	testassert.NoError(t, json.Unmarshal([]byte(createBody), &session))
	malformed := invoke(`{"extension_id":"fixture","operation":"command","target":{"kind":"session","session_id":"` + session.SessionId + `"},"arguments":{"method":"submit","arguments":{"session_id":"forged","operation_id":"op-x","mode":"regular","content":[]}},"request_id":"req-command"}`)
	testassert.Require(t, malformed.StatusCode == http.StatusBadRequest, "malformed command status = %d body = %s", malformed.StatusCode, readBody(t, malformed))
}

// TestProtocolExtensionUnmappedCallErrorRetainsPartial proves a directly
// forwarded CallError keeps the ordinary classified baseline Error and its
// exact produced partial through the public invocation HTTP boundary.
func TestProtocolExtensionUnmappedCallErrorRetainsPartial(t *testing.T) {
	ctx := context.Background()
	_, _, server := mountedOwner(t, ctx, extensionOwnerOptions(t))
	invoke := func(mode string) (int, string) {
		body := `{"extension_id":"fixture","operation":"call_error","target":{"kind":"harness"},"arguments":{"mode":"` + mode + `"},"request_id":"req-call-error"}`
		response := rawMounted(t, http.MethodPost, server.Endpoint()+"/v1/extensions/invoke", server.credential, body)
		return response.StatusCode, readBody(t, response)
	}
	decode := func(received string) map[string]json.RawMessage {
		var members map[string]json.RawMessage
		testassert.NoError(t, json.Unmarshal([]byte(received), &members))
		return members
	}
	status, received := invoke("valid")
	members := decode(received)
	testassert.Require(t, status == http.StatusInternalServerError, "call-error status = %d body = %s", status, received)
	var code string
	testassert.NoError(t, json.Unmarshal(members["code"], &code))
	testassert.Require(t, code == "storage", "call-error code = %q, want storage", code)
	partial, ok := members["partial_result"]
	testassert.Require(t, ok, "call-error body omitted partial_result: %s", received)
	testassert.Require(t, string(partial) == "{\n  \"big\": 9007199254740993,\n  \"text\": \"line\\\\nbreak\"\n}", "partial_result = %q, want the exact produced bytes", partial)

	status, received = invoke("null")
	members = decode(received)
	testassert.Require(t, status == http.StatusInternalServerError && string(members["partial_result"]) == "null", "null partial = (%d, %s)", status, received)
	status, received = invoke("empty")
	members = decode(received)
	_, present := members["partial_result"]
	testassert.Require(t, status == http.StatusInternalServerError && !present, "empty partial = (%d, %s), want the member absent", status, received)
	status, received = invoke("bad")
	testassert.Require(t, status == http.StatusInternalServerError, "malformed partial status = %d body = %s", status, received)
	testassert.Require(t, json.Valid([]byte(received)), "malformed partial produced invalid HTTP JSON: %q", received)
	members = decode(received)
	_, present = members["partial_result"]
	testassert.Require(t, !present, "malformed partial was published: %s", received)
}
