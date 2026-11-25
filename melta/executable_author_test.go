package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func TestExecutableAuthorPromptTokenIsolation(t *testing.T) {
	a := newExecutableActivation("unused", nil)
	calls := 0
	a.author = &executableAuthor{}
	grant, end := authorPromptGrant(context.Background(), extension.AuthorContext{}, extension.PromptAccess{Default: func() (string, error) { calls++; return "prompt", nil }})
	defer end()
	a.tokens["valid"] = grant
	a.tokens["unavailable"] = closedCall
	request := func(id, token string) worker.Frame {
		t.Helper()
		params, _ := json.Marshal(worker.AuthorHostcallParams{Token: token, Service: extension.ContractPrompt, Method: "default", Arguments: worker.JSONBytes(`{}`)})
		if err := a.handleFrame(worker.Frame{Kind: worker.KindRequest, ID: id, Method: worker.MethodHostcall, Params: params}); err != nil {
			t.Fatal(err)
		}
		encoded := <-a.out
		frame, err := worker.NewReader(bytes.NewReader(encoded.encoded)).ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	if frame := request("foreign", "foreign"); frame.Error == nil || frame.Error.Class != worker.ClassClosed || calls != 0 {
		t.Fatalf("foreign prompt token: %+v, calls %d", frame, calls)
	}
	if frame := request("unavailable", "unavailable"); frame.Error == nil || frame.Error.Class != worker.ClassClosed || calls != 0 {
		t.Fatalf("unavailable prompt context: %+v, calls %d", frame, calls)
	}
	if frame := request("valid", "valid"); frame.Error != nil || calls != 1 {
		t.Fatalf("valid prompt token: %+v, calls %d", frame, calls)
	}
	a.mu.Lock()
	delete(a.tokens, "valid")
	a.mu.Unlock()
	if frame := request("stale", "valid"); frame.Error == nil || frame.Error.Class != worker.ClassClosed || calls != 1 {
		t.Fatalf("stale prompt token: %+v, calls %d", frame, calls)
	}
}

func TestExecutableAuthorHostcallTokenIsolation(t *testing.T) {
	a := newExecutableActivation("unused", nil)
	a.author = &executableAuthor{registrations: map[string]extension.Registration{"handle": struct{}{}}}
	calls := 0
	a.tokens["valid"] = func(_ context.Context, _, _ string, args json.RawMessage) (extension.Result, error) {
		calls++
		return extension.Result{Value: args}, nil
	}
	request := func(id, token string) worker.Frame {
		t.Helper()
		params, _ := json.Marshal(worker.AuthorHostcallParams{Token: token, Service: "service", Method: "call", Arguments: worker.JSONBytes(`{}`)})
		if err := a.handleFrame(worker.Frame{Kind: worker.KindRequest, ID: id, Method: worker.MethodHostcall, Params: params}); err != nil {
			t.Fatal(err)
		}
		encoded := <-a.out
		frame, err := worker.NewReader(bytes.NewReader(encoded.encoded)).ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	if frame := request("foreign", "foreign"); frame.Error == nil || frame.Error.Class != worker.ClassClosed || calls != 0 {
		t.Fatalf("foreign token: %+v, calls %d", frame, calls)
	}
	if frame := request("handle", "handle"); frame.Error == nil || frame.Error.Class != worker.ClassClosed || calls != 0 {
		t.Fatalf("registration handle used as token: %+v, calls %d", frame, calls)
	}
	if frame := request("valid", "valid"); frame.Error != nil || calls != 1 {
		t.Fatalf("valid token: %+v, calls %d", frame, calls)
	}
	a.mu.Lock()
	delete(a.tokens, "valid")
	a.mu.Unlock()
	if frame := request("stale", "valid"); frame.Error == nil || frame.Error.Class != worker.ClassClosed || calls != 1 {
		t.Fatalf("stale token: %+v, calls %d", frame, calls)
	}
}

func TestExecutableAuthorPromptHostcallJoins(t *testing.T) {
	entered, proceed := make(chan struct{}), make(chan struct{})
	grant, end := authorPromptGrant(context.Background(), extension.AuthorContext{}, extension.PromptAccess{Default: func() (string, error) {
		close(entered)
		<-proceed
		return "value", nil
	}})
	result := make(chan error, 1)
	go func() {
		_, err := grant(context.Background(), extension.ContractPrompt, "default", json.RawMessage(`{}`))
		result <- err
	}()
	<-entered
	ended := make(chan struct{})
	go func() { end(); close(ended) }()
	select {
	case <-ended:
		t.Fatal("scope returned before prompt call completed")
	default:
	}
	close(proceed)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	<-ended
	if _, err := grant(context.Background(), extension.ContractPrompt, "default", json.RawMessage(`{}`)); !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("expired prompt grant: %v", err)
	}
}

func TestExecutableAuthorFixtureHandshakeAndService(t *testing.T) {
	t.Setenv("AUTHOR_FIXTURE_SECRET", "private")
	binary := filepath.Join(t.TempDir(), "worker")
	cmd := exec.Command("go", "build", "-o", binary, "./testdata/author")
	cmd.Dir = filepath.Join("..", "extension", "worker")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fixture: %v: %s", err, output)
	}
	manifest := authorFixtureManifest()
	for _, mismatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "service", true: "manifest mismatch"}[mismatch], func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "started")
			selected := manifest
			if mismatch {
				selected.Version = "2"
			}
			source, dataDir := t.TempDir(), t.TempDir()
			binaryBytes, err := os.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "worker"), binaryBytes, 0700); err != nil {
				t.Fatal(err)
			}
			manifestBytes, err := worker.EncodeManifest(selected, "worker")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifestBytes, 0600); err != nil {
				t.Fatal(err)
			}
			manager := newPackageManager(dataDir)
			if _, err := manager.install(source); err != nil {
				t.Fatal(err)
			}
			selection, ok, err := readSelection(filepath.Join(dataDir, "extensions", extensionDataHash(selected.ID)))
			if err != nil || !ok {
				t.Fatalf("selection: %+v, %v", selection, err)
			}
			selected, entry, err := loadAuthorPackage(dataDir, extensionDataHash(selected.ID), selection)
			if err != nil {
				t.Fatal(err)
			}
			owner := &configurationService{}
			spawns := 0
			host := extension.Extension{Manifest: registrarManifest("host-fixture"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
				_, err := api.RegisterService(extension.Service{Name: "host-service", Methods: []extension.RegisteredMethod{{
					Method: extension.Method{Name: "echo", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
					Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
						return extension.Result{Value: args}, nil
					},
				}}}, nil)
				return nil, err
			}}
			environment := func(_ context.Context, excluded []string) ([]string, error) {
				spawns++
				if !slices.Contains(excluded, "AUTHOR_FIXTURE_SECRET") {
					t.Error("candidate secret missing before spawn")
				}
				env := []string{"AUTHOR_FIXTURE_MARKER=" + marker}
				for _, item := range os.Environ() {
					name, _, _ := strings.Cut(item, "=")
					if !slices.Contains(excluded, name) {
						env = append(env, item)
					}
				}
				return env, nil
			}
			guest := executableAuthorExtension(selected, entry, environment)
			candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
				{extension: host, section: json.RawMessage(`{}`)},
				{extension: guest, section: json.RawMessage(`{}`), installed: true, bindings: map[string]declaredBinding{"/secret": {name: "AUTHOR_FIXTURE_SECRET"}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer candidate.release()
			if mismatch {
				if candidate.inactive[selected.ID] == nil {
					t.Fatal("mismatched manifest accepted")
				}
				if _, err := os.Stat(marker); !os.IsNotExist(err) {
					t.Fatalf("entry ran before manifest accepted: %v", err)
				}
				if err := candidate.publishForTest(); err != nil {
					t.Fatal(err)
				}
				corrected := executableAuthorExtension(manifest, entry, environment)
				next, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
					{extension: host, section: json.RawMessage(`{}`)},
					{extension: corrected, section: json.RawMessage(`{}`), installed: true, bindings: map[string]declaredBinding{"/secret": {name: "AUTHOR_FIXTURE_SECRET"}}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if next.inactive[manifest.ID] == nil || spawns != 1 {
					t.Fatalf("pre-hello failure was not sticky: inactive %v, spawns %d", next.inactive, spawns)
				}
				next.release()
				return
			}
			if len(candidate.inactive) != 0 {
				t.Fatal(candidate.inactive)
			}
			if err := candidate.publishForTest(); err != nil {
				t.Fatal(err)
			}
			result, err := candidate.callServiceScoped(context.Background(), "worker-service", "relay", json.RawMessage(`{"value":123}`), &nativeCallScope{})
			if err != nil || string(result.Value) != `{"value":123}` {
				t.Fatalf("nested worker call: %s, %v", result.Value, err)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatal(err)
			}
			work := candidate.capture()
			if work == nil {
				t.Fatal("admission capture missing")
			}
			defer work.release()
			oldStage, err := work.stage()
			if err != nil {
				t.Fatal(err)
			}
			oldBinding := oldStage.bindTool("worker-tool")
			if oldBinding == nil {
				t.Fatal("missing worker tool")
			}
			oldTool := oldBinding.entry.value.(nativeTool).tool
			call := model.ToolCall{ID: "call", Name: "worker-tool", Arguments: json.RawMessage(`{"arg":1}`)}
			parked, proceed := make(chan struct{}), make(chan struct{})
			finished := make(chan harness.ToolOutcome, 1)
			go func() {
				finished <- oldTool(context.Background(), extension.AuthorContext{Call: func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
					close(parked)
					<-proceed
					return extension.Result{Value: json.RawMessage(`{}`)}, nil
				}}, call)
			}()
			select {
			case <-parked:
			case outcome := <-finished:
				t.Fatalf("tool returned before parking: %+v", outcome)
			case <-time.After(5 * time.Second):
				t.Fatal("prepare did not park")
			}
			during, err := work.stage()
			if err != nil {
				t.Fatal(err)
			}
			if during.tools["worker-tool"] != oldBinding.entry {
				t.Fatal("parked callback changed the registration too early")
			}
			during.release()
			close(proceed)
			outcome := <-finished
			if outcome.Result.Status != model.ResultSuccess || outcome.Result.Content != "old handler" {
				t.Fatalf("old handler: %+v", outcome)
			}
			newStage, err := work.stage()
			if err != nil {
				t.Fatal(err)
			}
			if newStage.tools["worker-tool"] == oldBinding.entry {
				t.Fatal("replacement did not appear in next stage")
			}
			oldStage.release()
			oldBinding.release()
			newStage.release()
			removalStage, err := work.stage()
			if err != nil {
				t.Fatal(err)
			}
			removalBinding := removalStage.bindTool("removable-tool")
			if removalBinding == nil {
				t.Fatal("missing removable tool")
			}
			var releases atomic.Int32
			owner.nativeMu.Lock()
			release := removalBinding.entry.release
			removalBinding.entry.release = func() { releases.Add(1); release() }
			owner.nativeMu.Unlock()
			removalTool := removalBinding.entry.value.(nativeTool).tool
			removalOutcome := removalTool(context.Background(), extension.AuthorContext{Call: func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: json.RawMessage(`{}`)}, nil
			}}, model.ToolCall{ID: "remove", Name: "removable-tool", Arguments: json.RawMessage(`{}`)})
			if removalOutcome.Result.Status != model.ResultSuccess || releases.Load() != 0 {
				t.Fatalf("self-removal released handler: %+v, releases %d", removalOutcome, releases.Load())
			}
			afterRemoval, err := work.stage()
			if err != nil || afterRemoval.tools["removable-tool"] != nil {
				t.Fatalf("removed registration still visible: %v", err)
			}
			afterRemoval.release()
			removalStage.release()
			if releases.Load() != 0 {
				t.Fatal("plan did not retain removed handler")
			}
			removalBinding.release()
			removalBinding.release()
			if releases.Load() != 1 {
				t.Fatalf("release count = %d", releases.Load())
			}
			stage := candidate.snapshotForTest()
			defer stage.release()
			strategy := stage.strategies["worker-strategy"]
			if strategy == nil {
				t.Fatal("missing worker strategy")
			}
			strategyImpl := strategy.value.(harness.StrategyBinding).Strategy
			if result, err := strategyImpl.Plan(harness.PlanInput{Trigger: "manual"}); err != nil || result.Kind != harness.PlanNone {
				t.Fatalf("strategy plan: %+v, %v", result, err)
			}
			if result, err := strategyImpl.Consume(harness.ConsumeInput{Input: harness.PlanInput{Trigger: "manual"}, Output: model.Output{Source: model.ModelRef{Provider: "p", Model: "m"}}}); err != nil || result.Kind != harness.PlanNone {
				t.Fatalf("strategy consume: %+v, %v", result, err)
			}
			if len(stage.hooks["arguments"]) != 1 {
				t.Fatalf("argument registrations: %d", len(stage.hooks["arguments"]))
			}
			arguments := stage.hooks["arguments"][0].value.(nativeHook).handler.(extension.ArgumentsHook)
			repaired, err := arguments(context.Background(), extension.AuthorContext{}, model.ToolCall{ID: "bad", Name: "worker-tool", Arguments: json.RawMessage{0xff, '{'}})
			if err != nil || string(repaired.Arguments) != string([]byte{0xff, '{'}) {
				t.Fatalf("opaque arguments: %x, %v", repaired.Arguments, err)
			}
			if len(stage.hooks["request"]) != 1 || len(stage.hooks["result"]) != 1 {
				t.Fatal("missing request or result hook")
			}
			request := stage.hooks["request"][0].value.(nativeHook).handler.(extension.AuthorRequestHook)
			consequence, err := request(context.Background(), extension.AuthorContext{}, extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{Model: model.ModelRef{Provider: "p", Model: "m"}}})
			if err != nil || consequence.Prompt == nil || *consequence.Prompt != "worker prompt" {
				t.Fatalf("request: %+v, %v", consequence, err)
			}
			resultHook := stage.hooks["result"][0].value.(nativeHook).handler.(extension.ResultHook)
			content, err := resultHook(context.Background(), extension.AuthorContext{}, harness.ToolOutputInput{Content: "output"})
			if err != nil || content != "worker: output" {
				t.Fatalf("result hook: %q, %v", content, err)
			}
			if len(stage.hooks["check"]) != 1 {
				t.Fatal("missing check hook")
			}
			authorization := stage.hooks["check"][0].value.(nativeHook).handler.(extension.ToolCheckHook)
			refusal, err := authorization(context.Background(), extension.AuthorContext{}, model.ToolCall{Arguments: json.RawMessage{0xff, '{'}})
			if err != nil || refusal == nil || *refusal != "denied by worker" {
				t.Fatalf("authorization: %v, %v", refusal, err)
			}
			operation := stage.operations[nativeOperationKey{manifest.ID, "operation"}]
			if operation == nil {
				t.Fatal("missing operation")
			}
			operationMethod := operation.value.(struct {
				target extension.ExposedTargetKind
				method nativeServiceMethod
			}).method
			operationResult, err := operationMethod.handler(context.Background(), extension.AuthorContext{}, json.RawMessage(`{"number":123}`))
			if err != nil || string(operationResult.Value) != `{"number":123}` {
				t.Fatalf("operation: %s, %v", operationResult.Value, err)
			}
			if len(stage.hooks["admission"]) != 1 {
				t.Fatal("missing admission hook")
			}
			admission := stage.hooks["admission"][0].value.(nativeHook).handler.(extension.AdmissionHook)
			promptReads := 0
			admissionResult, err := admission(context.Background(), extension.AuthorContext{}, extension.AdmissionInput{Candidate: extension.AdmissionCandidate{Model: model.ModelRef{Provider: "p", Model: "m"}}, Prompt: extension.PromptAccess{Default: func() (string, error) {
				promptReads++
				return "lazy prompt", nil
			}}})
			if err != nil || admissionResult.Refusal == nil || *admissionResult.Refusal != "lazy prompt" || promptReads != 1 {
				t.Fatalf("admission: %+v, %v, reads %d", admissionResult, err, promptReads)
			}
			promptReads = 0
			admissionResult, err = admission(context.Background(), extension.AuthorContext{}, extension.AdmissionInput{Candidate: extension.AdmissionCandidate{
				Model: model.ModelRef{Provider: "p", Model: "m"}, Instructions: "skip",
			}, Prompt: extension.PromptAccess{Default: func() (string, error) {
				promptReads++
				return "unexpected", nil
			}}})
			if err != nil || admissionResult.Refusal == nil || *admissionResult.Refusal != "unread" || promptReads != 0 {
				t.Fatalf("unread prompt: %+v, %v, reads %d", admissionResult, err, promptReads)
			}
			if len(stage.hooks["cleanup"]) != 1 {
				t.Fatalf("cleanup registrations: %d", len(stage.hooks["cleanup"]))
			}
			cleanup := stage.hooks["cleanup"][0].value.(extension.CleanupHook)
			if err := cleanup(context.Background(), extension.AuthorContext{ExtensionID: manifest.ID}, harness.SessionIdentity{SessionID: "session"}); err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			captured := candidate.capture()
			if captured == nil {
				t.Fatal("admission capture failed")
			}
			defer captured.release()
			_, err = candidate.callServiceScoped(context.Background(), "worker-service", "crash", json.RawMessage(`{}`), &nativeCallScope{})
			if err == nil {
				t.Fatal("lost worker served a call")
			}
			if _, err := candidate.callServiceScoped(context.Background(), "worker-service", "relay", json.RawMessage(`{}`), &nativeCallScope{}); err == nil {
				t.Fatal("lost activation served another call")
			}
			if _, err := captured.stage(); err == nil {
				t.Fatal("captured work skipped lost worker")
			}
			next := candidate.capture()
			if next == nil {
				t.Fatal("subsequent admission capture failed")
			}
			defer next.release()
			nextStage, err := next.stage()
			if err != nil {
				t.Fatal(err)
			}
			defer nextStage.release()
			if stagedService(nextStage, "worker-service") != nil || stagedService(nextStage, "host-service") == nil {
				t.Fatal("new admission did not omit only the failed worker")
			}
			if result, err := candidate.callServiceScoped(context.Background(), "host-service", "echo", json.RawMessage(`{"ok":true}`), &nativeCallScope{}); err != nil || string(result.Value) != `{"ok":true}` {
				t.Fatalf("healthy sibling: %s, %v", result.Value, err)
			}
			makeCandidate := func(section string, secret string, enabled bool) *nativeCandidate {
				t.Helper()
				specs := []nativeActivationSpec{{extension: host, section: json.RawMessage(`{}`)}}
				if enabled {
					specs = append(specs, nativeActivationSpec{extension: guest, section: json.RawMessage(section), installed: true, bindings: map[string]declaredBinding{"/secret": {name: secret}}})
				}
				c, err := owner.prepareNativeCandidate(context.Background(), specs)
				if err != nil {
					t.Fatal(err)
				}
				return c
			}
			changed := makeCandidate(`{"changed":true}`, "ROTATED_SECRET", true)
			defer changed.release()
			if changed.inactive[selected.ID] == nil || spawns != 1 {
				t.Fatalf("failure lost on settings/credential change: inactive %v, spawns %d", changed.inactive, spawns)
			}
			if err := changed.publishForTest(); err != nil {
				t.Fatal(err)
			}
			discarded := makeCandidate(`{}`, "", false)
			discarded.release()
			stillFailed := makeCandidate(`{}`, "AUTHOR_FIXTURE_SECRET", true)
			defer stillFailed.release()
			if stillFailed.inactive[selected.ID] == nil || spawns != 1 {
				t.Fatalf("discarded disable cleared failure: inactive %v, spawns %d", stillFailed.inactive, spawns)
			}
			disabled := makeCandidate(`{}`, "", false)
			defer disabled.release()
			if err := disabled.publishForTest(); err != nil {
				t.Fatal(err)
			}
			reenabled := makeCandidate(`{}`, "AUTHOR_FIXTURE_SECRET", true)
			defer reenabled.release()
			if reenabled.inactive[selected.ID] != nil || spawns != 2 {
				t.Fatalf("published re-enable: inactive %v, spawns %d", reenabled.inactive, spawns)
			}
			if err := reenabled.publishForTest(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExecutableAuthorManifestInstallRejectsInvalid(t *testing.T) {
	manifest := authorFixtureManifest()
	for _, test := range []struct {
		name string
		edit func(*worker.Manifest)
	}{
		{"api", func(m *worker.Manifest) { m.Manifest.APIVersion = "4" }},
		{"entry", func(m *worker.Manifest) { m.EntryPoint = "../worker" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, dataDir := t.TempDir(), t.TempDir()
			marker := filepath.Join(source, "ran")
			if err := os.WriteFile(filepath.Join(source, "worker"), []byte("#!/bin/sh\ntouch "+marker+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			pkg := worker.Manifest{EntryPoint: "worker", Manifest: manifest}
			test.edit(&pkg)
			raw, err := json.Marshal(pkg)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "manifest.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := newPackageManager(dataDir).install(source); err == nil {
				t.Fatal("invalid package accepted")
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("installation executed package: %v", err)
			}
		})
	}
}

func TestExecutableAuthorRetiredLossKeepsSuccessor(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "worker")
	cmd := exec.Command("go", "build", "-o", binary, "./testdata/author")
	cmd.Dir = filepath.Join("..", "extension", "worker")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fixture: %v: %s", err, output)
	}
	owner := &configurationService{}
	spawns := 0
	host := extension.Extension{Manifest: registrarManifest("host-fixture"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "host-service", Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "echo", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: args}, nil
			},
		}}}, nil)
		return nil, err
	}}
	manifest := authorFixtureManifest()
	guest := executableAuthorExtension(manifest, binary, func(context.Context, []string) ([]string, error) {
		spawns++
		return os.Environ(), nil
	})
	build := func(section string) *nativeCandidate {
		t.Helper()
		c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
			{extension: host, section: json.RawMessage(`{}`)},
			{extension: guest, section: json.RawMessage(section), installed: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(c.inactive) != 0 {
			t.Fatalf("candidate inactive: %v", c.inactive)
		}
		return c
	}
	invalid, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: host, section: json.RawMessage(`{}`)},
		{extension: guest, section: json.RawMessage(`[]`), installed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if invalid.inactive[manifest.ID] == nil || spawns != 0 {
		t.Fatalf("invalid settings: inactive %v, spawns %d", invalid.inactive, spawns)
	}
	defer invalid.release()
	if err := invalid.publishForTest(); err != nil {
		t.Fatal(err)
	}
	old := build(`{}`)
	defer old.release()
	if err := old.publishForTest(); err != nil {
		t.Fatal(err)
	}
	captured := old.capture()
	defer captured.release()
	parkedStage, err := captured.stage()
	if err != nil {
		t.Fatal(err)
	}
	binding := parkedStage.bindTool("worker-tool")
	tool := binding.entry.value.(nativeTool).tool
	parked, proceed := make(chan struct{}), make(chan struct{})
	finished := make(chan harness.ToolOutcome, 1)
	go func() {
		finished <- tool(context.Background(), extension.AuthorContext{Call: func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
			close(parked)
			<-proceed
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}}, model.ToolCall{ID: "parked", Name: "worker-tool", Arguments: json.RawMessage(`{}`)})
	}()
	select {
	case <-parked:
	case outcome := <-finished:
		t.Fatalf("callback returned before parking: %+v", outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not park")
	}
	successor := build(`{"revision":2}`)
	defer successor.release()
	if err := successor.publishForTest(); err != nil {
		t.Fatal(err)
	}
	if spawns != 2 {
		t.Fatalf("rotation spawned %d workers", spawns)
	}
	close(proceed)
	if outcome := <-finished; outcome.Result.Status != model.ResultSuccess {
		t.Fatalf("old handler failed after publication: %+v", outcome)
	}
	parkedStage.release()
	binding.release()
	if _, err := old.callServiceScoped(context.Background(), "worker-service", "crash", json.RawMessage(`{}`), &nativeCallScope{}); err == nil {
		t.Fatal("retired worker did not fail")
	}
	if _, err := captured.stage(); err == nil {
		t.Fatal("old capture did not see its own worker failure")
	}
	next := successor.capture()
	defer next.release()
	stage, err := next.stage()
	if err != nil || stagedService(stage, "worker-service") == nil {
		t.Fatalf("healthy successor omitted: %v", err)
	}
	stage.release()
	if result, err := successor.callServiceScoped(context.Background(), "worker-service", "relay", json.RawMessage(`{"ok":true}`), &nativeCallScope{}); err != nil || string(result.Value) != `{"ok":true}` {
		t.Fatalf("healthy successor: %s, %v", result.Value, err)
	}
	reused := build(`{"revision":2}`)
	defer reused.release()
	if spawns != 2 {
		t.Fatalf("healthy successor restarted: %d", spawns)
	}
	disabled, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: host, section: json.RawMessage(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.release()
	if err := disabled.publishForTest(); err != nil {
		t.Fatal(err)
	}
	failed, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: host, section: json.RawMessage(`{}`)},
		{extension: guest, section: json.RawMessage(`{"fail":true}`), installed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if failed.inactive[manifest.ID] == nil || spawns != 3 {
		t.Fatalf("activation failure: inactive %v, spawns %d", failed.inactive, spawns)
	}
	defer failed.release()
	if err := failed.publishForTest(); err != nil {
		t.Fatal(err)
	}
	sticky, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: host, section: json.RawMessage(`{}`)},
		{extension: guest, section: json.RawMessage(`{}`), installed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sticky.inactive[manifest.ID] == nil || spawns != 3 {
		t.Fatalf("activation failure not sticky: inactive %v, spawns %d", sticky.inactive, spawns)
	}
	sticky.release()
}

func TestExecutableAuthorPreHelloFailureSticky(t *testing.T) {
	owner := &configurationService{}
	manifest := extension.Manifest{ID: "author", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	attempts := 0
	guest := executableAuthorExtension(manifest, "unused", func(context.Context, []string) ([]string, error) {
		attempts++
		return nil, errors.New("environment unavailable")
	})
	build := func(section string) *nativeCandidate {
		t.Helper()
		c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: guest, section: json.RawMessage(section), installed: true}})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	invalid := build(`[]`)
	defer invalid.release()
	if invalid.inactive[manifest.ID] == nil || attempts != 0 {
		t.Fatalf("invalid section: %v, attempts %d", invalid.inactive, attempts)
	}
	if err := invalid.publishForTest(); err != nil {
		t.Fatal(err)
	}
	failed := build(`{}`)
	defer failed.release()
	if failed.inactive[manifest.ID] == nil || attempts != 1 {
		t.Fatalf("environment failure: %v, attempts %d", failed.inactive, attempts)
	}
	if err := failed.publishForTest(); err != nil {
		t.Fatal(err)
	}
	edited := build(`{"changed":true}`)
	defer edited.release()
	if edited.inactive[manifest.ID] == nil || attempts != 1 {
		t.Fatalf("failure was retried: %v, attempts %d", edited.inactive, attempts)
	}
}
