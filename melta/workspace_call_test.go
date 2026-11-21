package melta

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/atomicfs"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/model"
)

type workspaceTestTool struct {
	path     string
	borrowed func(context.Context, string, string, json.RawMessage) (extension.Result, error)
	err      error
}

func workspaceTestLease(t *testing.T, dataDir, session string) *artifactCallLease {
	t.Helper()
	intervals := newArtifactIntervals()
	key := sessionArtifactDir(dataDir, session)
	release, err := intervals.acquireExecution(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return &artifactCallLease{intervals: intervals, key: key, borrowed: true, release: release}
}

func (t *workspaceTestTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *workspaceTestTool) Prepare(ctx context.Context, call extension.AuthorContext, _ model.ToolCall) extension.ToolPlan {
	t.borrowed = call.Call
	if string(call.State) != `{"own":true}` {
		t.err = errors.New("missing captured state")
		return extension.ToolPlan{Immediate: &harness.ToolOutcome{}}
	}
	result, err := call.Call(ctx, "melta.workspace", "bind", json.RawMessage(`{"path":"file"}`))
	if err != nil {
		t.err = err
		return extension.ToolPlan{Immediate: &harness.ToolOutcome{}}
	}
	var binding struct {
		Binding string `json:"binding"`
	}
	if err := json.Unmarshal(result.Value, &binding); err != nil {
		t.err = err
		return extension.ToolPlan{Immediate: &harness.ToolOutcome{}}
	}
	data, _ := json.Marshal(map[string]string{"binding": binding.Binding})
	return extension.ToolPlan{Data: data, Permissions: []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: t.path}}}
}

func (t *workspaceTestTool) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	credential, err := call.Call(ctx, extension.ContractCredentials, "get", []byte(`{"field":"/key"}`))
	if err != nil || string(credential.Value) != `{"value":"operation-secret"}` {
		t.err = errors.New("missing captured operation credential")
		return harness.ToolOutcome{}
	}
	result, err := call.Call(ctx, "melta.workspace", "read", plan.Data)
	if err == nil && string(result.Value) != `{"bytes":"YmVmb3Jl"}` {
		err = errors.New("wrong read bytes")
	}
	if err == nil {
		var data map[string]any
		err = json.Unmarshal(plan.Data, &data)
		data["bytes"] = "YWZ0ZXI="
		data["create_only"] = false
		write, _ := json.Marshal(data)
		if err == nil {
			_, err = call.Call(ctx, "melta.workspace", "write", write)
		}
	}
	t.err = err
	return harness.ToolOutcome{}
}

func TestWorkspaceRegisteredNativeTool(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	impl := &workspaceTestTool{path: path}
	manifest := registrarManifest("workspace")
	manifest.Services = []string{"melta.workspace", extension.ContractCredentials}
	manifest.Secrets = []extension.SecretRef{{Field: "/key", Lifetime: extension.SecretOperation}}
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(model.ToolDefinition{Name: "workspace-test", Parameters: []byte(`{}`)}, impl, nil)
		return nil, err
	}}
	owner := &configurationService{}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	stage := candidate.snapshotForTest()
	defer stage.release()
	binding := stage.bindTool("workspace-test")
	defer binding.release()
	tc := ToolContext{Session: harness.SessionIdentity{SessionID: "session", Workspace: root}, Workspace: root, AdmittedEntry: harness.EntryRef{SessionID: "session", EntryID: "entry"}}
	dataDir := t.TempDir()
	adapted := registeredExtensionTool{binding: binding, dataDir: dataDir, lease: workspaceTestLease(t, dataDir, "session"),
		states: map[string]harness.SessionStateValue{"workspace": []byte(`{"own":true}`)}, host: &composition{},
		operationSecrets: map[string]map[string]string{"workspace": {"/key": "operation-secret"}}}
	prepared := adapted.Prepare(context.Background(), tc, model.ToolCall{})
	if impl.err != nil || prepared.Immediate != nil {
		t.Fatalf("prepare: %v", impl.err)
	}
	if _, err := impl.borrowed(context.Background(), "melta.workspace", "bind", json.RawMessage(`{"path":"file"}`)); !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("expired preparation callback: %v", err)
	}
	prepared.Execute(context.Background())
	if impl.err != nil {
		t.Fatal(impl.err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "after" {
		t.Fatalf("file = %q, %v", data, err)
	}
}

func TestWorkspaceRestrictionWitnessCannotRead(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest := registrarManifest("restriction")
	manifest.Services = []string{"melta.workspace"}
	called := false
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachAuthorization(func(ctx context.Context, call extension.AuthorContext, _ extension.AuthorizationInput) (*string, error) {
			result, err := call.Call(ctx, "melta.workspace", "bind", []byte(`{"path":"file"}`))
			if err != nil {
				return nil, err
			}
			var target struct {
				Binding string `json:"binding"`
			}
			if err := json.Unmarshal(result.Value, &target); err != nil {
				return nil, err
			}
			args, _ := json.Marshal(map[string]string{"binding": target.Binding})
			if _, err := call.Call(ctx, "melta.workspace", "read", args); !errors.Is(err, ErrDenied) {
				return nil, errors.New("restriction could read witness")
			}
			return nil, nil
		}, nil)
		return nil, err
	}}
	owner := &configurationService{}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	stage := candidate.snapshotForTest()
	defer stage.release()
	hooks := stage.authorizationHooks(ToolContext{Workspace: root}, nil, nil, nil, nil, nil)
	_, err = hooks[0].Run(context.Background(), harness.PreparedAction{}, func(path string) (string, error) {
		called = true
		return filepath.Join(root, path), nil
	})
	if err != nil || !called {
		t.Fatalf("restriction witness: called=%v err=%v", called, err)
	}
}

func TestWorkspaceRegisteredWorkerTool(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "workspace-worker")
	cmd := exec.Command("go", "build", "-o", binary, "./testdata/author")
	cmd.Dir = filepath.Join("..", "extension", "worker")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v: %s", err, output)
	}
	manifest := registrarManifest("workspace-worker")
	manifest.Services = []string{"melta.workspace"}
	guest := executableAuthorExtension(manifest, binary, func(context.Context, []string) ([]string, error) {
		return append(os.Environ(), "AUTHOR_FIXTURE_MODE=workspace"), nil
	})
	owner := &configurationService{}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: guest, section: []byte(`{}`), installed: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if len(candidate.inactive) != 0 {
		t.Fatal(candidate.inactive)
	}
	stage := candidate.snapshotForTest()
	defer stage.release()
	binding := stage.bindTool("workspace-worker")
	if binding == nil {
		t.Fatal("worker tool not registered")
	}
	defer binding.release()
	runtime, env := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(runtime)
	root := filepath.Join(env.home, "worker-workspace")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"file", "sibling"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("before"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	session := projectionSession(t, runtime, root, "solo").Identity
	operation := submitCodeOperation(t, runtime, session.SessionID, "worker-write", "test")
	entry := operation.Admission.AdmittedEntry
	release, err := runtime.artifacts.acquireExecution(context.Background(), sessionArtifactDir(runtime.dataDir, session.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	tc := ToolContext{Session: session, Workspace: root, AdmittedEntry: entry}
	adapted := registeredExtensionTool{binding: binding, dataDir: runtime.dataDir, lease: &artifactCallLease{intervals: runtime.artifacts, key: sessionArtifactDir(runtime.dataDir, session.SessionID), borrowed: true, release: release}}
	prepared := adapted.Prepare(context.Background(), tc, model.ToolCall{ID: "call", Name: "workspace-worker", Arguments: []byte(`{}`)})
	if prepared.Immediate != nil {
		t.Fatalf("prepare: %+v", prepared.Immediate)
	}
	hooks := stage.authorizationHooks(tc, nil, nil, nil, nil, nil)
	bound := false
	if len(hooks) != 1 {
		t.Fatalf("worker restrictions: %d", len(hooks))
	}
	reason, err := hooks[0].Run(context.Background(), harness.PreparedAction{}, func(path string) (string, error) {
		bound = true
		return filepath.Join(root, path), nil
	})
	if err != nil || reason != nil || !bound {
		t.Fatalf("worker witness restriction: %v %v %v", reason, bound, err)
	}
	outcome := prepared.Execute(context.Background())
	if outcome.Result.Status != model.ResultSuccess {
		t.Fatalf("execute: %+v", outcome)
	}
	data, err := os.ReadFile(filepath.Join(root, "file"))
	if err != nil || string(data) != "after" {
		t.Fatalf("file = %q, %v", data, err)
	}
	release()
	groups, err := runtime.ListSessionCodeSnapshots(context.Background(), testCaller(runtime), session.SessionID)
	if err != nil || len(groups) != 1 || groups[0].OperationId != "worker-write" || len(groups[0].Files) != 1 {
		t.Fatalf("worker snapshot groups: %+v %v", groups, err)
	}
	reverted, err := runtime.RevertSessionCode(context.Background(), testCaller(runtime), session.SessionID, "worker-write")
	if err != nil || len(reverted.Restored) != 1 {
		t.Fatalf("worker revert: %+v %v", reverted, err)
	}
	data, err = os.ReadFile(filepath.Join(root, "file"))
	if err != nil || string(data) != "before" {
		t.Fatalf("reverted file = %q, %v", data, err)
	}
	lostGuest := executableAuthorExtension(manifest, binary, func(context.Context, []string) ([]string, error) {
		return append(os.Environ(), "AUTHOR_FIXTURE_MODE=workspace", "AUTHOR_FIXTURE_LOSE_REPLY=1"), nil
	})
	lostOwner := &configurationService{}
	lostCandidate, err := lostOwner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: lostGuest, section: []byte(`{}`), installed: true}})
	if err != nil || len(lostCandidate.inactive) != 0 {
		t.Fatalf("lost worker activation: %v", err)
	}
	defer lostCandidate.release()
	lostStage := lostCandidate.snapshotForTest()
	defer lostStage.release()
	lostBinding := lostStage.bindTool("workspace-worker")
	defer lostBinding.release()
	lostOperation := submitCodeOperation(t, runtime, session.SessionID, "worker-lost", "test")
	lostRelease, err := runtime.artifacts.acquireExecution(context.Background(), sessionArtifactDir(runtime.dataDir, session.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	lostTC := ToolContext{Session: session, Workspace: root, AdmittedEntry: lostOperation.Admission.AdmittedEntry}
	lostTool := registeredExtensionTool{binding: lostBinding, dataDir: runtime.dataDir, lease: &artifactCallLease{intervals: runtime.artifacts, key: sessionArtifactDir(runtime.dataDir, session.SessionID), borrowed: true, release: lostRelease}}
	lostPlan := lostTool.Prepare(context.Background(), lostTC, model.ToolCall{ID: "lost", Name: "workspace-worker", Arguments: []byte(`{}`)})
	if lostPlan.Immediate != nil {
		t.Fatalf("lost worker preparation: %+v", lostPlan.Immediate)
	}
	lostResult := lostPlan.Execute(context.Background())
	lostRelease()
	if lostResult.Evidence != harness.ExecutionUnknown {
		t.Fatalf("lost worker evidence: %+v", lostResult)
	}
	data, err = os.ReadFile(filepath.Join(root, "file"))
	if err != nil || string(data) != "after" {
		t.Fatalf("lost reply mutation = %q, %v", data, err)
	}
	restricted, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: guest, section: []byte(`{}`), installed: true, approvedServices: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	defer restricted.release()
	restrictedStage := restricted.snapshotForTest()
	defer restrictedStage.release()
	restrictedBinding := restrictedStage.bindTool("workspace-worker")
	defer restrictedBinding.release()
	restrictedTool := registeredExtensionTool{binding: restrictedBinding, dataDir: t.TempDir(), lease: &artifactCallLease{borrowed: true}}
	refused := restrictedTool.Prepare(context.Background(), tc, model.ToolCall{ID: "call", Name: "workspace-worker", Arguments: []byte(`{}`)})
	if refused.Immediate == nil || refused.Immediate.Result.Status != model.ResultError {
		t.Fatalf("host-denied workspace grant: %+v", refused)
	}
}

func TestWorkspacePlanBoundAuthority(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	plan := newNativeWorkspace(root, dataDir, harness.EntryRef{SessionID: "session", EntryID: "entry"}, workspaceTestLease(t, dataDir, "session"))
	result, err := plan.bind(json.RawMessage(`{"path":"file"}`))
	if err != nil {
		t.Fatal(err)
	}
	var binding struct {
		Binding string `json:"binding"`
	}
	if err := json.Unmarshal(result.Value, &binding); err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(map[string]any{"binding": binding.Binding})
	grant := []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: path}}
	result, err = plan.execute(context.Background(), "read", args, grant)
	if err != nil || string(result.Value) != `{"bytes":"YmVmb3Jl"}` {
		t.Fatalf("write grant precondition read: %s %v", result.Value, err)
	}
	other := newNativeWorkspace(root, t.TempDir(), harness.EntryRef{SessionID: "session", EntryID: "entry"}, &artifactCallLease{borrowed: true})
	if _, err := other.execute(context.Background(), "read", args, grant); !errors.Is(err, ErrDenied) {
		t.Fatalf("borrowed handle: %v", err)
	}
	if _, err := plan.execute(context.Background(), "read", args, nil); !errors.Is(err, ErrDenied) {
		t.Fatalf("ungranted read: %v", err)
	}
	write, _ := json.Marshal(map[string]any{"binding": binding.Binding, "bytes": "YWZ0ZXI=", "create_only": false})
	invalid, _ := json.Marshal(map[string]any{"binding": binding.Binding, "bytes": "eA", "create_only": false})
	if _, err := plan.execute(context.Background(), "write", invalid, grant); !errors.Is(err, harness.ErrInvalid) {
		t.Fatalf("noncanonical base64: %v", err)
	}
	result, err = plan.execute(context.Background(), "write", write, grant)
	if err != nil || string(result.Value) != `{"outcome":"completed"}` {
		t.Fatalf("write: %s %v", result.Value, err)
	}
	create, _ := json.Marshal(map[string]any{"binding": binding.Binding, "bytes": "eA==", "create_only": true})
	_, err = plan.execute(context.Background(), "write", create, grant)
	var callErr *extension.CallError
	if err == nil || errors.As(err, &callErr) && len(callErr.Partial) != 0 {
		t.Fatalf("untouched refusal: %v", err)
	}
}

func TestWorkspaceRejectsWrongArtifactLease(t *testing.T) {
	root, dataDir := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	lease := &artifactCallLease{borrowed: true, key: sessionArtifactDir(dataDir, "other"), release: func() {}}
	plan := newNativeWorkspace(root, dataDir, harness.EntryRef{SessionID: "session", EntryID: "entry"}, lease)
	result, err := plan.bind([]byte(`{"path":"file"}`))
	if err != nil {
		t.Fatal(err)
	}
	var target struct {
		Binding string `json:"binding"`
	}
	if err := json.Unmarshal(result.Value, &target); err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(map[string]string{"binding": target.Binding})
	_, err = plan.execute(context.Background(), "read", args, []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: filepath.Join(root, "file")}})
	if !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("wrong artifact lease accepted: %v", err)
	}
}

func TestWorkspaceReportsCompletedMutationOnRecordFailure(t *testing.T) {
	root, dataDir := t.TempDir(), t.TempDir()
	plan := newNativeWorkspace(root, dataDir, harness.EntryRef{SessionID: "session", EntryID: "entry"}, workspaceTestLease(t, dataDir, "session"))
	result, err := plan.bind([]byte(`{"path":"file"}`))
	if err != nil {
		t.Fatal(err)
	}
	var target struct {
		Binding string `json:"binding"`
	}
	if err := json.Unmarshal(result.Value, &target); err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(map[string]any{"binding": target.Binding, "bytes": "YWZ0ZXI=", "create_only": false})
	previous := atomicfs.SyncFileFunc
	count := 0
	atomicfs.SyncFileFunc = func(f *os.File) error {
		if strings.HasPrefix(filepath.Base(f.Name()), "meta.json.tmp-") {
			count++
			if count == 3 {
				return errors.New("record failure")
			}
		}
		if previous != nil {
			return previous(f)
		}
		return f.Sync()
	}
	defer func() { atomicfs.SyncFileFunc = previous }()
	_, err = plan.execute(context.Background(), "write", args, []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: filepath.Join(root, "file")}})
	var failure *extension.CallError
	if !errors.As(err, &failure) || string(failure.Partial) != `{"outcome":"completed"}` {
		t.Fatalf("completed partial: %v", err)
	}
	data, readErr := os.ReadFile(filepath.Join(root, "file"))
	if readErr != nil || string(data) != "after" {
		t.Fatalf("completed content: %q %v", data, readErr)
	}
}
