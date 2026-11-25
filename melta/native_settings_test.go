package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/protocol"
)

// TestSettingsReplace covers the own-settings replace through the native API:
// a harness-target client operation replaces its own section, every section
// change while the handler is parked conflicts, a retired activation whose
// section is unchanged still replaces, and the authority and schema refusals
// keep their classes.
func TestSettingsReplace(t *testing.T) {
	h := newServiceHarness(t)
	t.Setenv("SETTINGS_KEY", "first")
	const initial = `{"enabled":true,"key":"SETTINGS_KEY","grant":"old","value":0}`
	writeServiceFile(t, h.configPath, `{"providers":{},"extensions":{"setting":`+initial+`}}`)
	manifest := registrarManifest("setting")
	manifest.SettingsSchema = []byte(`{"type":"object","required":["enabled","key"],"properties":{"enabled":{"type":"boolean"},"key":{"type":"string"},"value":{"type":"integer"},"large":{"type":"integer"}}}`)
	manifest.Services = []string{extension.ContractSettings}
	manifest.Secrets = []extension.SecretRef{{Field: "/key", Lifetime: extension.SecretActivation}}
	var beforeMu sync.Mutex
	var beforeReplace func()
	takeBefore := func() func() {
		beforeMu.Lock()
		defer beforeMu.Unlock()
		return beforeReplace
	}
	setBefore := func(hook func()) {
		beforeMu.Lock()
		defer beforeMu.Unlock()
		beforeReplace = hook
	}
	operation := func(name string, target extension.ExposedTargetKind) extension.Operation {
		return extension.Operation{
			Target: target,
			Method: extension.Method{Name: name, Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				if hook := takeBefore(); hook != nil {
					hook()
				}
				payload, err := marshalRawObject(map[string]json.RawMessage{"section": args}, true)
				if err != nil {
					return extension.Result{}, err
				}
				return call.Call(ctx, extension.ContractSettings, "replace", payload)
			},
		}
	}
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		for _, op := range []extension.Operation{
			operation("replace", extension.ExposedTargetHarness),
			operation("replace-workspace", extension.ExposedTargetWorkspace),
			operation("replace-session", extension.ExposedTargetSession),
		} {
			if _, err := api.RegisterOperation(op, nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}}
	svc := h.preparedService(context.Background())
	svc.authors = append(svc.authors, nativeActivationSpec{extension: e, section: []byte(initial)})
	if _, err := svc.publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, _ := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	original := r.config
	r.config = svc
	defer func() { r.config = original }()
	invoke := func(section string) error {
		_, err := r.invokeRegisteredOperation(context.Background(), testCaller(r), "setting", "replace", harnessTarget(), json.RawMessage(section), "")
		return err
	}
	reset := func(t *testing.T) {
		t.Helper()
		writeServiceFile(t, h.configPath, `{"providers":{},"extensions":{"setting":`+initial+`}}`)
		if _, err := svc.publish(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	sectionBytes := func() json.RawMessage { return rawSection(t, h.configPath, "setting") }
	scopeFor := func(t *testing.T) *nativeCallScope {
		t.Helper()
		candidate := svc.current().native
		if candidate == nil {
			t.Fatal("no published native candidate")
		}
		capture := candidate.capture()
		if capture == nil {
			t.Fatal("no admission capture")
		}
		var activation *nativeActivation
		for _, entry := range capture.activations {
			if entry.id == "setting" {
				activation = entry
				break
			}
		}
		if activation == nil {
			capture.release()
			t.Fatal("no published setting activation")
		}
		ctx, cancel := context.WithCancel(context.Background())
		scope := &nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: ctx, cancel: cancel}, candidate: candidate, activation: activation, kind: callKindOperation, commandScope: harnessScope}
		t.Cleanup(func() {
			scope.end()
			capture.release()
		})
		return scope
	}
	replaceThrough := func(t *testing.T, scope *nativeCallScope, section string) error {
		t.Helper()
		payload, err := marshalRawObject(map[string]json.RawMessage{"section": json.RawMessage(section)}, true)
		if err != nil {
			t.Fatal(err)
		}
		_, err = scope.call(scope.ctx, extension.ContractSettings, "replace", payload)
		return err
	}

	t.Run("replaces the section and the next admission uses it", func(t *testing.T) {
		reset(t)
		proposed := `{"enabled":true,"key":"SETTINGS_KEY","grant":"new","large":9007199254740993,"value":0}`
		if err := invoke(proposed); err != nil {
			t.Fatalf("replace: %v", err)
		}
		if got := string(sectionBytes()); got != proposed {
			t.Fatalf("section = %s, want the proposed bytes %s", got, proposed)
		}
		var members map[string]json.RawMessage
		if err := json.Unmarshal(sectionBytes(), &members); err != nil || string(members["large"]) != "9007199254740993" {
			t.Fatalf("number lexeme = %s, %v", members["large"], err)
		}
		capture := svc.current().native.capture()
		if capture == nil {
			t.Fatal("no admission capture")
		}
		defer capture.release()
		var admitted *nativeActivation
		for _, entry := range capture.activations {
			if entry.id == "setting" {
				admitted = entry
				break
			}
		}
		if admitted == nil {
			t.Fatal("next admission does not use the setting activation")
		}
		if !bytes.Equal(admitted.section, []byte(proposed)) {
			t.Fatalf("next admission section = %s, want the proposed bytes", admitted.section)
		}
	})

	t.Run("section changes while the call is parked conflict without a write", func(t *testing.T) {
		rows := []struct {
			name string
			edit func(*testing.T)
		}{
			{"section edited", func(t *testing.T) {
				_, err := svc.mutate(context.Background(), configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
					sections, err := extensionSections(roots, "setting")
					if err != nil {
						return 0, err
					}
					section, err := decodeRoot(sections["setting"])
					if err != nil {
						return 0, err
					}
					section["value"] = json.RawMessage(`42`)
					raw, err := marshalRawObject(section, true)
					if err != nil {
						return 0, err
					}
					return writeExtensionSection(roots, sections, "setting", raw)
				}})
				if err != nil {
					t.Fatal(err)
				}
			}},
			{"section removed", func(t *testing.T) {
				_, err := svc.mutate(context.Background(), configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
					sections, err := extensionSections(roots, "setting")
					if err != nil {
						return 0, err
					}
					delete(sections, "setting")
					raw, err := marshalRawObject(sections, true)
					if err != nil {
						return 0, err
					}
					roots.config["extensions"] = raw
					return editMainConfig, nil
				}})
				if err != nil {
					t.Fatal(err)
				}
			}},
			{"extensions member removed", func(t *testing.T) {
				_, err := svc.mutate(context.Background(), configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
					delete(roots.config, "extensions")
					return editMainConfig, nil
				}})
				if err != nil {
					t.Fatal(err)
				}
			}},
		}
		for _, row := range rows {
			t.Run(row.name, func(t *testing.T) {
				reset(t)
				entered, release := make(chan struct{}), make(chan struct{})
				setBefore(func() { close(entered); <-release })
				done := make(chan error, 1)
				go func() { done <- invoke(`{"enabled":true,"key":"SETTINGS_KEY","grant":"changed"}`) }()
				<-entered
				row.edit(t)
				before := string(mustRead(t, h.configPath))
				close(release)
				err := <-done
				setBefore(nil)
				if !errors.Is(err, harness.ErrConflict) || ErrorCode(err) != protocol.ErrorCodeConflict {
					t.Fatalf("replace over a changed section = %v, want conflict", err)
				}
				if after := string(mustRead(t, h.configPath)); after != before {
					t.Fatalf("conflict changed the file: %s", after)
				}
			})
		}
	})

	t.Run("a second replace from the same activation", func(t *testing.T) {
		const changed = `{"enabled":true,"key":"SETTINGS_KEY","grant":"second"}`
		t.Run("after the first changed the section conflicts", func(t *testing.T) {
			reset(t)
			scope := scopeFor(t)
			if err := replaceThrough(t, scope, `{"enabled":true,"key":"SETTINGS_KEY","grant":"first"}`); err != nil {
				t.Fatalf("first replace: %v", err)
			}
			if err := replaceThrough(t, scope, changed); !errors.Is(err, harness.ErrConflict) || ErrorCode(err) != protocol.ErrorCodeConflict {
				t.Fatalf("second replace = %v, want conflict", err)
			}
		})
		t.Run("after a first that left the bytes unchanged succeeds", func(t *testing.T) {
			reset(t)
			scope := scopeFor(t)
			if err := replaceThrough(t, scope, initial); err != nil {
				t.Fatalf("unchanged first replace: %v", err)
			}
			if err := replaceThrough(t, scope, changed); err != nil {
				t.Fatalf("second replace: %v", err)
			}
			if got := string(sectionBytes()); got != changed {
				t.Fatalf("section = %s, want %s", got, changed)
			}
		})
	})

	t.Run("proposals that change authority are denied", func(t *testing.T) {
		reset(t)
		for _, section := range []string{
			`{"enabled":false,"key":"SETTINGS_KEY"}`,
			`{"enabled":true,"key":"OTHER"}`,
		} {
			before := string(mustRead(t, h.configPath))
			if err := invoke(section); !errors.Is(err, ErrDenied) {
				t.Fatalf("replace %s = %v, want denied", section, err)
			}
			if after := string(mustRead(t, h.configPath)); after != before {
				t.Fatalf("denied replace %s changed the file", section)
			}
		}
	})

	t.Run("a proposal failing the settings schema is invalid", func(t *testing.T) {
		reset(t)
		before := string(mustRead(t, h.configPath))
		err := invoke(`{"enabled":true,"key":"SETTINGS_KEY","value":"not-an-integer"}`)
		if ErrorCode(err) != protocol.ErrorCodeInvalid {
			t.Fatalf("schema-failing replace = %v, want the invalid class", err)
		}
		if after := string(mustRead(t, h.configPath)); after != before {
			t.Fatal("invalid replace changed the file")
		}
	})

	t.Run("only a harness-target operation may replace", func(t *testing.T) {
		reset(t)
		workspace := t.TempDir()
		session := projectionSession(t, r, workspace, "solo").Identity.SessionID
		scope := scopeFor(t)
		for _, row := range []struct {
			name  string
			scope CallerScope
		}{
			{"hook or tool", CallerScope{}},
			{"workspace scope", CallerScope{Kind: CallerScopeWorkspace, Workspace: "/tmp"}},
			{"session scope", CallerScope{Kind: CallerScopeSession, SessionID: "session"}},
		} {
			scope.commandScope = row.scope
			if err := replaceThrough(t, scope, `{"enabled":true,"key":"SETTINGS_KEY"}`); !errors.Is(err, ErrDenied) {
				t.Fatalf("%s replace = %v, want denied", row.name, err)
			}
		}
		for _, row := range []struct {
			name      string
			operation string
			target    protocol.ExtensionInvocationTarget
		}{
			{"workspace operation", "replace-workspace", workspaceTarget(workspace)},
			{"session operation", "replace-session", sessionTarget(session)},
		} {
			_, err := r.invokeRegisteredOperation(context.Background(), testCaller(r), "setting", row.operation, row.target, []byte(`{"enabled":true,"key":"SETTINGS_KEY"}`), "")
			if !errors.Is(err, ErrDenied) {
				t.Fatalf("%s replace = %v, want denied", row.name, err)
			}
		}
	})

	t.Run("no settings grant is denied", func(t *testing.T) {
		reset(t)
		denied, err := svc.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(initial), approvedServices: []string{}}})
		if err != nil {
			t.Fatal(err)
		}
		defer denied.release()
		ctx, cancel := context.WithCancel(context.Background())
		scope := &nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: ctx, cancel: cancel}, candidate: denied, activation: denied.activations[0], kind: callKindOperation, commandScope: harnessScope}
		defer scope.end()
		before := string(mustRead(t, h.configPath))
		if err := replaceThrough(t, scope, `{"enabled":true,"key":"SETTINGS_KEY"}`); !errors.Is(err, ErrDenied) {
			t.Fatalf("replace without the grant = %v, want denied", err)
		}
		if after := string(mustRead(t, h.configPath)); after != before {
			t.Fatal("a call without the grant changed the file")
		}
	})

	t.Run("a retired activation whose section is unchanged still replaces", func(t *testing.T) {
		reset(t)
		retired := svc.current().native.activations[0]
		entered, release := make(chan struct{}), make(chan struct{})
		setBefore(func() { close(entered); <-release })
		proposed := `{"enabled":true,"key":"SETTINGS_KEY","grant":"rotated"}`
		done := make(chan error, 1)
		go func() { done <- invoke(proposed) }()
		<-entered
		t.Setenv("SETTINGS_KEY", "second")
		if _, err := svc.publish(context.Background()); err != nil {
			t.Fatal(err)
		}
		if svc.current().native.activations[0] == retired {
			t.Fatal("credential rotation reused the activation")
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("retired activation replace: %v", err)
		}
		setBefore(nil)
		if got := string(sectionBytes()); got != proposed {
			t.Fatalf("section = %s, want %s", got, proposed)
		}
	})
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

// TestSettingsReplaceThroughWorker covers the own-settings replace through the
// worker protocol: the in-test executable extension warns from its entry and a
// callback, replaces its own section through a harness-target operation, and is
// inactive without the warning grant it requests.
func TestSettingsReplaceThroughWorker(t *testing.T) {
	h := newServiceHarness(t)
	section := []byte(`{"enabled":true,"value":1}`)
	writeServiceFile(t, h.configPath, `{"providers":{},"extensions":{"settings-worker":`+string(section)+`}}`)
	svc := h.preparedService(context.Background())
	if _, err := svc.publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	manifest := registrarManifest("settings-worker")
	manifest.SettingsSchema = []byte(`{"type":"object"}`)
	manifest.Services = []string{extension.ContractSettings, extension.ContractWarnings}
	svc.attachWarnings(newWarningStore())
	adapter := newObservationAdapter(svc.obs, svc.warnings)
	svc.composition.reportWarning = adapter.reportPluginWarning
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
			if _, err := api.Call(ctx, extension.ContractWarnings, "report", []byte(`{"kind":"worker-entry","message":"before"}`)); err != nil {
				return nil, err
			}
			_, err := api.RegisterService(extension.Service{Name: "worker/transform", Methods: []extension.RegisteredMethod{{
				Method: extension.Method{Name: "warn", Input: []byte(`{}`), Success: []byte(`{}`)},
				Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return call.Call(ctx, extension.ContractWarnings, "report", args)
				},
			}}}, nil)
			if err == nil {
				_, err = api.RegisterOperation(extension.Operation{
					Target: extension.ExposedTargetHarness,
					Method: extension.Method{Name: "replace", Input: []byte(`{}`), Success: []byte(`{}`)},
					Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
						replacement, err := decodeRoot(call.Settings)
						if err != nil {
							return extension.Result{}, err
						}
						replacement["value"] = args
						raw, err := marshalRawObject(replacement, true)
						if err != nil {
							return extension.Result{}, err
						}
						payload, err := marshalRawObject(map[string]json.RawMessage{"section": raw}, true)
						if err != nil {
							return extension.Result{}, err
						}
						return call.Call(ctx, extension.ContractSettings, "replace", payload)
					},
				}, nil)
			}
			return nil, err
		}), nil
	}}
	spawner.install()
	guest := executableAuthorExtension(manifest, "settings-worker", adapterEnvironment)
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
	if _, err := candidate.callServiceScoped(context.Background(), "worker/transform", "warn", []byte(`{"kind":"worker-later","message":"after"}`), &nativeCallScope{kind: callKindHook}); err != nil {
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
	_, err = r.invokeRegisteredOperation(context.Background(), testCaller(r), "settings-worker", "replace", harnessTarget(), []byte(`9007199254740993`), "")
	if err != nil {
		t.Fatal(err)
	}
	got := rawSection(t, h.configPath, "settings-worker")
	var values map[string]json.RawMessage
	if err := json.Unmarshal(got, &values); err != nil || string(values["value"]) != "9007199254740993" {
		t.Fatalf("worker settings = %s, %v", got, err)
	}
	denied, err := svc.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
		extension: guest, section: section, installed: true, approvedServices: []string{extension.ContractSettings},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer denied.release()
	if denied.inactive["settings-worker"] == nil {
		t.Fatal("worker activated without its requested warning grant")
	}
}
