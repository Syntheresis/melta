package melta

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/protocol"
)

func TestSettingsUpdateRegisteredTransformConcurrentWriter(t *testing.T) {
	h := newServiceHarness(t)
	initial := `{"enabled":true,"key":"KEY","grant":"old","value":0}`
	writeServiceFile(t, h.configPath, `{"providers":{},"extensions":{"setting":`+initial+`}}`)
	svc := h.preparedService(context.Background())
	if _, err := svc.publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	manifest := registrarManifest("setting")
	manifest.SettingsSchema = []byte(`{"type":"object","required":["enabled","key"],"properties":{"enabled":{"type":"boolean"},"key":{"type":"string"},"value":{"type":"integer"}}}`)
	manifest.Services = []string{"melta.settings"}
	manifest.Secrets = []extension.SecretRef{{Field: "/key", Lifetime: extension.SecretActivation}}
	var calls atomic.Int32
	var seen json.RawMessage
	var beforeUpdate func()
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "own/opaque", Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "change/opaque", Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				calls.Add(1)
				var input struct {
					Section   json.RawMessage `json:"section"`
					Arguments json.RawMessage `json:"arguments"`
				}
				if err := json.Unmarshal(args, &input); err != nil {
					return extension.Result{}, err
				}
				seen = append([]byte(nil), input.Arguments...)
				section, err := decodeRoot(input.Section)
				if err != nil {
					return extension.Result{}, err
				}
				var edit map[string]json.RawMessage
				if err := json.Unmarshal(input.Arguments, &edit); err != nil {
					return extension.Result{}, err
				}
				for key, value := range edit {
					section[key] = value
				}
				result, err := marshalRawObject(section, true)
				return extension.Result{Value: result}, err
			},
		}}}, nil)
		if err == nil {
			for _, op := range []struct {
				name   string
				target extension.ExposedTargetKind
			}{{"update", extension.ExposedTargetHarness}, {"update-workspace", extension.ExposedTargetWorkspace}, {"update-session", extension.ExposedTargetSession}} {
				_, err = api.RegisterOperation(extension.Operation{Target: op.target, Method: extension.Method{Name: op.name, Mode: extension.ModeServiceIO, Input: []byte(`{}`), Success: []byte(`{}`)},
					Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
						if beforeUpdate != nil {
							beforeUpdate()
						}
						return call.Call(ctx, "melta.settings", "update", args)
					}}, nil)
				if err != nil {
					break
				}
			}
		}
		return nil, err
	}}
	svc.authors = append(svc.authors, nativeActivationSpec{extension: e, section: []byte(initial)})
	candidate, err := svc.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(initial)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if err := candidate.publishForTest(); err != nil {
		t.Fatal(err)
	}
	r, env := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	workspace := filepath.Join(env.home, "settings-scope")
	session := projectionSession(t, r, workspace, "solo").Identity.SessionID
	original := r.config
	r.config = svc
	defer func() { r.config = original }()
	target := protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}
	invoke := func(args json.RawMessage) error {
		_, err := r.invokeRegisteredOperation(context.Background(), testCaller(r), "setting", "update", target, args, "")
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	scope := &nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: ctx, cancel: cancel}, candidate: candidate, activation: candidate.activations[0]}
	defer scope.end()
	for _, forbidden := range []CallerScope{{}, {Kind: CallerScopeWorkspace, Workspace: "/tmp"}, {Kind: CallerScopeSession, SessionID: "session"}} {
		scope.commandScope = forbidden
		_, err := scope.call(ctx, "melta.settings", "update", []byte(`{"service":"own/opaque","method":"change/opaque","arguments":{}}`))
		if !errors.Is(err, ErrDenied) || calls.Load() != 0 || svc.current().generation != 1 {
			t.Fatalf("forbidden settings scope %+v: %v calls=%d", forbidden, err, calls.Load())
		}
	}
	scope.commandScope = harnessScope
	for _, row := range []struct {
		name   string
		target protocol.ExtensionInvocationTarget
	}{{"update-workspace", workspaceTarget(workspace)}, {"update-session", sessionTarget(session)}} {
		_, err := r.invokeRegisteredOperation(context.Background(), testCaller(r), "setting", row.name, row.target, []byte(`{"service":"own/opaque","method":"change/opaque","arguments":{}}`), "")
		if !errors.Is(err, ErrDenied) || calls.Load() != 0 || svc.current().generation != 1 {
			t.Fatalf("forbidden registered operation %s: %v", row.name, err)
		}
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := svc.mutate(context.Background(), configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
			close(entered)
			<-resume
			sections, err := decodeRoot(roots.config["extensions"])
			if err != nil {
				return 0, err
			}
			section, err := decodeRoot(sections["setting"])
			if err != nil {
				return 0, err
			}
			section["value"] = json.RawMessage(`42`)
			sections["setting"], err = marshalRawObject(section, true)
			if err == nil {
				roots.config["extensions"], err = marshalRawObject(sections, true)
			}
			return editMainConfig, err
		}})
		firstDone <- err
	}()
	<-entered
	secondDone := make(chan error, 1)
	secondEntered := make(chan struct{})
	beforeUpdate = func() { close(secondEntered) }
	go func() {
		err := invoke(json.RawMessage(`{"service":"own/opaque","method":"change/opaque","arguments":{"grant" : "new","large":9007199254740993}}`))
		secondDone <- err
	}()
	<-secondEntered
	close(resume)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	beforeUpdate = nil
	if calls.Load() != 1 || string(seen) != `{"grant" : "new","large":9007199254740993}` {
		t.Fatalf("transform calls=%d args=%s", calls.Load(), seen)
	}
	raw, err := os.ReadFile(h.configPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Extensions map[string]struct {
			Value int    `json:"value"`
			Grant string `json:"grant"`
			Key   string `json:"key"`
		} `json:"extensions"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if got := doc.Extensions["setting"]; got.Value != 42 || got.Grant != "new" || got.Key != "KEY" {
		t.Fatalf("combined settings: %+v", got)
	}
	before := string(raw)
	err = invoke(json.RawMessage(`{"service":"own/opaque","method":"change/opaque","arguments":{"key":"OTHER"}}`))
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("authority-changing transform: %v", err)
	}
	raw, err = os.ReadFile(h.configPath)
	if err != nil || string(raw) != before {
		t.Fatalf("authority-changing transform wrote config: %v", err)
	}
	generation := svc.current().generation
	for _, arguments := range []string{`{"enabled":false}`, `{"value":"invalid"}`} {
		request := `{"service":"own/opaque","method":"change/opaque","arguments":` + arguments + `}`
		if err := invoke([]byte(request)); err == nil {
			t.Fatalf("invalid transform accepted: %s", arguments)
		}
		raw, err := os.ReadFile(h.configPath)
		if err != nil || string(raw) != before || svc.current().generation != generation {
			t.Fatalf("invalid transform published: %s %v", arguments, err)
		}
	}
	// An invalid ordinary field leaves the extension inactive while the rest
	// of the candidate publishes, so its own transform is unavailable until a
	// later valid write repairs the section.
	_, err = svc.mutate(context.Background(), configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
		sections, err := extensionSections(roots, "setting")
		if err != nil {
			return 0, err
		}
		section, err := decodeRoot(sections["setting"])
		if err != nil {
			return 0, err
		}
		section["value"] = json.RawMessage(`"repair-me"`)
		raw, err := marshalRawObject(section, true)
		if err != nil {
			return 0, err
		}
		return writeExtensionSection(roots, sections, "setting", raw)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := invoke([]byte(`{"service":"own/opaque","method":"change/opaque","arguments":{"value":9}}`)); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("transform over an inactive section = %v, want not-found", err)
	}
	_, err = svc.mutate(context.Background(), configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
		sections, err := extensionSections(roots, "setting")
		if err != nil {
			return 0, err
		}
		section, err := decodeRoot(sections["setting"])
		if err != nil {
			return 0, err
		}
		section["value"] = json.RawMessage(`7`)
		raw, err := marshalRawObject(section, true)
		if err != nil {
			return 0, err
		}
		return writeExtensionSection(roots, sections, "setting", raw)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := invoke([]byte(`{"service":"own/opaque","method":"change/opaque","arguments":{"value":9}}`)); err != nil {
		t.Fatalf("repaired ordinary field: %v", err)
	}
	parked, continueOld := make(chan struct{}), make(chan struct{})
	beforeUpdate = func() { close(parked); <-continueOld }
	retainedDone := make(chan error, 1)
	go func() {
		retainedDone <- invoke([]byte(`{"service":"own/opaque","method":"change/opaque","arguments":{"value":10}}`))
	}()
	<-parked
	sameAuthority, err := svc.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: rawSection(t, h.configPath, "setting")}})
	if err != nil {
		t.Fatal(err)
	}
	defer sameAuthority.release()
	if err := sameAuthority.publishForTest(); err != nil {
		t.Fatal(err)
	}
	close(continueOld)
	if err := <-retainedDone; err != nil {
		t.Fatalf("retained same-authority transform: %v", err)
	}
	beforeUpdate = nil
	generation = svc.current().generation
	next, err := svc.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: rawSection(t, h.configPath, "setting"), approvedServices: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	defer next.release()
	if err := next.publishForTest(); err != nil {
		t.Fatal(err)
	}
	_, err = scope.call(context.Background(), "melta.settings", "update", json.RawMessage(`{"service":"own/opaque","method":"change/opaque","arguments":{"grant":"other"}}`))
	if !errors.Is(err, ErrDenied) || svc.current().generation != generation {
		t.Fatalf("stale grant after narrowing: %v", err)
	}
}

func rawSection(t *testing.T, path, id string) json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var root struct {
		Extensions map[string]json.RawMessage `json:"extensions"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	return root.Extensions[id]
}

func TestSettingsUpdateRegisteredWorkerTransform(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "settings-worker")
	cmd := exec.Command("go", "build", "-o", binary, "./testdata/author")
	cmd.Dir = filepath.Join("..", "extension", "worker")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v: %s", err, output)
	}
	h := newServiceHarness(t)
	section := []byte(`{"enabled":true,"value":1}`)
	writeServiceFile(t, h.configPath, `{"providers":{},"extensions":{"settings-worker":`+string(section)+`}}`)
	svc := h.preparedService(context.Background())
	if _, err := svc.publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	manifest := registrarManifest("settings-worker")
	manifest.SettingsSchema = []byte(`{"type":"object"}`)
	manifest.Services = []string{"melta.settings", "melta.warnings"}
	svc.attachWarnings(newWarningStore())
	adapter := newObservationAdapter(svc.obs, svc.warnings)
	svc.composition.reportWarning = adapter.reportPluginWarning
	guest := executableAuthorExtension(manifest, binary, func(context.Context, []string) ([]string, error) {
		return append(os.Environ(), "AUTHOR_FIXTURE_MODE=settings"), nil
	})
	candidate, err := svc.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: guest, section: section, installed: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if len(candidate.inactive) != 0 {
		t.Fatal(candidate.inactive)
	}
	if warningExists(svc.warnings, "plugin:settings-worker", "worker-entry") {
		t.Fatal("worker warning escaped before publication")
	}
	if err := candidate.publishForTest(); err != nil {
		t.Fatal(err)
	}
	if !warningExists(svc.warnings, "plugin:settings-worker", "worker-entry") {
		t.Fatal("worker entry warning missing")
	}
	if _, err := candidate.callServiceScoped(context.Background(), "worker/transform", "warn", []byte(`{"kind":"worker-later","message":"after"}`), &nativeCallScope{}); err != nil {
		t.Fatal(err)
	}
	if !warningExists(svc.warnings, "plugin:settings-worker", "worker-later") {
		t.Fatal("worker callback warning missing")
	}
	r, _ := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	original := r.config
	r.config = svc
	defer func() { r.config = original }()
	_, err = r.invokeRegisteredOperation(context.Background(), testCaller(r), "settings-worker", "update",
		protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}, []byte(`{"service":"worker/transform","method":"change/value","arguments":9007199254740993}`), "")
	if err != nil {
		t.Fatal(err)
	}
	got := rawSection(t, h.configPath, "settings-worker")
	var values map[string]json.RawMessage
	if err := json.Unmarshal(got, &values); err != nil || string(values["value"]) != "9007199254740993" {
		t.Fatalf("worker settings = %s, %v", got, err)
	}
	denied, err := svc.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
		extension: guest, section: section, installed: true, approvedServices: []string{"melta.settings"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer denied.release()
	if denied.inactive["settings-worker"] == nil {
		t.Fatal("worker activated without its requested warning grant")
	}
}
