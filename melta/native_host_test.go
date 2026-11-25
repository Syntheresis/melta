package melta

import (
	"context"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/internal/storage"
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
