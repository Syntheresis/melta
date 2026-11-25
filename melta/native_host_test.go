package melta

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/protocol"
)

func TestScopedServiceCredentialsUseServingActivation(t *testing.T) {
	r, _ := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	manifest := registrarManifest("credential")
	manifest.Services = []string{extension.ContractCredentials}
	manifest.Secrets = []extension.SecretRef{{Field: "/key", Lifetime: extension.SecretActivation}}
	e := extension.Extension{Manifest: manifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return nil, nil
	}}
	candidate, err := r.config.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
		extension: e, section: []byte(`{"key":"NAME"}`), bindings: map[string]declaredBinding{"/key": {name: "NAME", value: "secret"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	ctx, cancel := context.WithCancel(context.Background())
	scope := &nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: ctx, cancel: cancel},
		candidate:          candidate, activation: candidate.activations[0], kind: callKindHook, host: r.config.composition,
	}
	defer scope.end()
	result, err := scope.call(ctx, extension.ContractCredentials, "get", []byte(`{"field":"/key"}`))
	if err != nil || string(result.Value) != `{"value":"secret"}` {
		t.Fatalf("own credential: %s %v", result.Value, err)
	}
	if _, err := scope.call(ctx, extension.ContractCredentials, "get", []byte(`{"field":"/other"}`)); err == nil {
		t.Fatal("undeclared credential accepted")
	}
	scope.kind = callKindAdmission
	if _, err := scope.call(ctx, extension.ContractCredentials, "get", []byte(`{"field":"/key"}`)); err != nil {
		t.Fatalf("admission credential call: %v", err)
	}
}

// TestSessionStartDocumentIsStrict proves a hand-built start whose nested
// inputs carry an undeclared member is refused as an invalid host call and
// creates no Session.
func TestSessionStartDocumentIsStrict(t *testing.T) {
	store := storage.NewMemory()
	r, _ := openProjectionRuntime(t, store)
	defer closeProjectionRuntime(r)
	ctx := context.Background()
	parent, err := r.createSession(ctx, t.TempDir(), "solo")
	testassert.NoError(t, err)
	manifest := registrarManifest("start-document")
	manifest.Services = []string{extension.ContractSession}
	candidate, err := r.config.prepareNativeCandidate(ctx, []nativeActivationSpec{{
		extension: extension.Extension{Manifest: manifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
			return nil, nil
		}},
		section: []byte(`{}`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	callCtx, cancel := context.WithCancel(ctx)
	scope := &nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: callCtx, cancel: cancel},
		candidate:          candidate, activation: candidate.activations[0], kind: callKindTool,
		session: parent.Identity, host: r.config.composition,
	}
	defer scope.end()
	encoded := func(document string) string {
		return base64.StdEncoding.EncodeToString([]byte(document))
	}
	start := json.RawMessage(fmt.Sprintf(`{"inputs":%q,"initial_state":"","input":%q,"report":false,"output_limit":0}`,
		encoded(`{"prompt":"x","unknown":true}`), encoded(`[]`)))
	before, err := store.ListSessionIDs(callCtx)
	testassert.NoError(t, err)
	_, err = scope.call(callCtx, extension.ContractSession, "start", start)
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "start error = %v, want the invalid host-call class", err)
	testassert.Require(t, ErrorCode(err) == protocol.ErrorCodeInvalid, "error code = %s, want invalid", ErrorCode(err))
	after, err := store.ListSessionIDs(callCtx)
	testassert.NoError(t, err)
	testassert.Equal(t, len(after), len(before))
}
