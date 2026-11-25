package melta

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/protocol"
)

func TestScopedServiceWarningsBufferAndPublish(t *testing.T) {
	r, _ := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	manifest := registrarManifest("warning-test")
	manifest.Services = []string{"melta.warnings", extension.ContractWorkspace}
	var borrowed func(context.Context, string, string, json.RawMessage) (extension.Result, error)
	e := extension.Extension{Manifest: manifest, Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.Call(ctx, "melta.warnings", "report", []byte(`{"kind":"entry","message":"before"}`)); err != nil {
			return nil, err
		}
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "report", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				borrowed = call.Call
				return call.Call(ctx, "melta.warnings", "report", args)
			},
		}, nil)
		return nil, err
	}}
	candidate, err := r.config.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if len(candidate.inactive) != 0 {
		t.Fatal(candidate.inactive)
	}
	if warningExists(r.warnings, "plugin:warning-test", "entry") {
		t.Fatal("warning escaped before publication")
	}
	if err := candidate.publishForTest(); err != nil {
		t.Fatal(err)
	}
	if !warningExists(r.warnings, "plugin:warning-test", "entry") {
		t.Fatal("published warning missing")
	}
	target := protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}
	result, err := r.invokeRegisteredOperation(context.Background(), testCaller(r), "warning-test", "report", target, []byte(`{"kind":"later","message":"after"}`), "")
	if err != nil || string(result.Value) != "null" || !warningExists(r.warnings, "plugin:warning-test", "later") {
		t.Fatalf("later warning = %s, %v", result.Value, err)
	}
	if _, err := borrowed(context.Background(), "melta.warnings", "report", []byte(`{"kind":"late","message":"forbidden"}`)); !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("borrowed warning callback remained live: %v", err)
	}
	scope := &nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: context.Background()}, candidate: candidate, activation: candidate.activations[0], kind: callKindOperation, host: r.config.composition}
	if _, err := scope.call(context.Background(), extension.ContractWarnings, "missing", []byte(`{}`)); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("unknown host method: %v", err)
	}
	scope.kind = callKindTool
	if _, err := scope.call(context.Background(), extension.ContractWorkspace, "write", []byte(`{"path":"x","bytes":"eA==","create_only":null}`)); !errors.Is(err, harness.ErrInvalid) {
		t.Fatalf("invalid host document: %v", err)
	}
	_, err = r.invokeRegisteredOperation(context.Background(), testCaller(r), "warning-test", "report", target, []byte(`{"kind":"spoof","message":"bad","source":"other"}`), "")
	if !errors.Is(err, harness.ErrInvalid) || warningExists(r.warnings, "plugin:warning-test", "spoof") {
		t.Fatalf("forged warning source: %v", err)
	}
}

func warningExists(store *warningStore, source, kind string) bool {
	_, warnings := store.snapshot()
	for _, warning := range warnings {
		if warning.Source == source && warning.Kind == kind {
			return true
		}
	}
	return false
}

func TestScopedServiceWarningsDeniedAndFailedCandidate(t *testing.T) {
	r, _ := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	manifest := registrarManifest("warning-denied")
	manifest.Services = []string{"melta.warnings"}
	var seen error
	e := extension.Extension{Manifest: manifest, Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		_, seen = api.Call(ctx, "melta.warnings", "report", []byte(`{"kind":"denied","message":"not delivered"}`))
		return nil, nil
	}}
	candidate, err := r.config.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`), approvedServices: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if !errors.Is(seen, ErrDenied) {
		t.Fatalf("unapproved warning: %v", seen)
	}
	if err := candidate.publishForTest(); err != nil {
		t.Fatal(err)
	}
	if warningExists(r.warnings, "plugin:warning-denied", "denied") {
		t.Fatal("unapproved warning delivered")
	}
	manifest.ID = "warning-failed"
	failed := extension.Extension{Manifest: manifest, Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.Call(ctx, "melta.warnings", "report", []byte(`{"kind":"failed","message":"not published"}`)); err != nil {
			return nil, err
		}
		return nil, errors.New("entry failed")
	}}
	next, err := r.config.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: failed, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer next.release()
	if next.inactive["warning-failed"] == nil {
		t.Fatal("failed activation was accepted")
	}
	if err := next.publishForTest(); err != nil {
		t.Fatal(err)
	}
	if warningExists(r.warnings, "plugin:warning-failed", "failed") {
		t.Fatal("failed candidate warning delivered")
	}
}
