package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/atomicfs"
	"github.com/Syntheresis/melta/internal/snapshot"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
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

func (t *workspaceTestTool) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	t.borrowed = call.Call
	if string(call.State) != `{"own":true}` {
		t.err = errors.New("missing captured state")
		return extension.ToolResult{}, t.err
	}
	credential, err := call.Call(ctx, extension.ContractCredentials, "get", []byte(`{"field":"/key"}`))
	if err != nil || string(credential.Value) != `{"value":"operation-secret"}` {
		t.err = errors.New("missing captured operation credential")
		return extension.ToolResult{}, t.err
	}
	content, err := call.ReadFile(ctx, "file")
	if err == nil && string(content) != "before" {
		err = errors.New("wrong read bytes")
	}
	if err == nil {
		err = call.WriteFile(ctx, "file", []byte("after"))
	}
	t.err = err
	return extension.ToolResult{}, err
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
		_, err := api.RegisterTool(model.ToolDefinition{Name: "workspace-test", Parameters: []byte(`{}`)}, impl.run, nil)
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
	outcome := adapted.Run(context.Background(), tc, model.ToolCall{ID: "call", Arguments: []byte(`{}`)})
	if impl.err != nil || outcome.Result.Status != model.ResultSuccess {
		t.Fatalf("run: %+v %v", outcome, impl.err)
	}
	if _, err := impl.borrowed(context.Background(), "melta.workspace", "stat", json.RawMessage(`{"path":"file"}`)); !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("expired callback: %v", err)
	}
	if impl.err != nil {
		t.Fatal(impl.err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "after" {
		t.Fatalf("file = %q, %v", data, err)
	}
}

func TestWorkspaceCheckHookCanRead(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest := registrarManifest("restriction")
	manifest.Services = []string{"melta.workspace"}
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachToolCheck(func(ctx context.Context, call extension.AuthorContext, _ model.ToolCall) (*string, error) {
			content, err := call.ReadFile(ctx, "file")
			if err != nil || string(content) != "secret" {
				return nil, fmt.Errorf("restriction could not read file: %q %v", content, err)
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
	hooks := stage.checkHooks(ToolContext{Session: harness.SessionIdentity{SessionID: "session", Workspace: root}, Workspace: root}, selection{invocation: Invocation{snapshot: &configuration{}}})
	_, err = hooks[0].Run(context.Background(), model.ToolCall{})
	if err != nil {
		t.Fatalf("restriction witness: %v", err)
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
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost=%v", lost), func(t *testing.T) {
			e := newOwnerEnv(t)
			enableFixturePlugin(t, e.configPath, "workspace-worker")
			e.fixtureStore(storage.NewMemory())
			var spawns atomic.Int64
			guest := executableAuthorExtension(manifest, binary, func(context.Context, []string) ([]string, error) {
				spawns.Add(1)
				env := append(os.Environ(), "AUTHOR_FIXTURE_MODE=workspace")
				if lost {
					env = append(env, "AUTHOR_FIXTURE_LOSE_REPLY=1")
				}
				return env, nil
			})
			var calls atomic.Int64
			e.server.setScript(func(_ context.Context, body string) []string {
				if lastMessageRole(body) == "tool" {
					return textTurnEvents("done")
				}
				calls.Add(1)
				return toolCallTurnEvents("call", "workspace-worker", `{}`)
			})
			ctx := context.Background()
			r, err := e.open(ctx, guest)
			testassert.NoError(t, err)
			defer closeProjectionRuntime(r)
			root := filepath.Join(e.home, "worker-workspace")
			testassert.NoError(t, os.MkdirAll(root, 0700))
			for _, name := range []string{"file", "sibling"} {
				testassert.NoError(t, os.WriteFile(filepath.Join(root, name), []byte("before"), 0600))
			}
			session, err := r.createSession(ctx, root, "")
			testassert.NoError(t, err)
			submitThroughRuntime(t, r, session.Identity.SessionID, "worker-write", "test")
			status := harness.OperationSuccess
			if lost {
				status = harness.OperationFailure
			}
			awaitOperation(t, r, session.Identity.SessionID, "worker-write", status)
			outcome := toolResultFact(t, r, session.Identity.SessionID, "worker-write")
			if lost {
				testassert.Require(t, outcome.Status == model.ResultError && outcome.Evidence == harness.ExecutionUnknown, "lost result: %+v", outcome)
			} else {
				testassert.Require(t, outcome.Status == model.ResultSuccess, "result: %+v", outcome)
			}
			testassert.Equal(t, calls.Load(), int64(1))
			testassert.Equal(t, spawns.Load(), int64(1))
			assertFileContent(t, filepath.Join(root, "file"), "after")
			groups, err := r.ListSessionCodeSnapshots(ctx, testCaller(r), session.Identity.SessionID)
			testassert.Require(t, err == nil && len(groups) == 1 && len(groups[0].Files) == 2, "snapshot groups: %+v %v", groups, err)
			if !lost {
				awaitRestorableSession(t, r, session.Identity.SessionID)
				reverted, err := r.RevertSessionCode(ctx, testCaller(r), session.Identity.SessionID, "worker-write")
				testassert.Require(t, err == nil && len(reverted.Restored) == 2, "revert: %+v %v", reverted, err)
				assertFileContent(t, filepath.Join(root, "file"), "before")
			}
		})
	}
	t.Run("workspace grant denied", func(t *testing.T) {
		guest := executableAuthorExtension(manifest, binary, func(context.Context, []string) ([]string, error) {
			return append(os.Environ(), "AUTHOR_FIXTURE_MODE=workspace"), nil
		})
		owner := &configurationService{}
		candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: guest, section: []byte(`{}`), installed: true, approvedServices: []string{}}})
		testassert.NoError(t, err)
		defer candidate.release()
		stage := candidate.snapshotForTest()
		defer stage.release()
		binding := stage.bindTool("workspace-worker")
		testassert.Require(t, binding != nil, "worker tool not registered")
		defer binding.release()
		root := t.TempDir()
		for _, name := range []string{"file", "sibling"} {
			testassert.NoError(t, os.WriteFile(filepath.Join(root, name), []byte("before"), 0600))
		}
		tool := registeredExtensionTool{binding: binding}
		outcome := tool.Run(context.Background(), ToolContext{Session: harness.SessionIdentity{SessionID: "session", Workspace: root}},
			model.ToolCall{ID: "call", Name: "workspace-worker", Arguments: []byte(`{}`)})
		testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, "relay read") && strings.Contains(outcome.Result.Content, "denied"), "denied relay read: %+v", outcome)
		for _, name := range []string{"file", "sibling"} {
			assertFileContent(t, filepath.Join(root, name), "before")
		}
	})
}

func TestWorkspacePathAuthority(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	scope := nativeCallScope{session: harness.SessionIdentity{SessionID: "session", Workspace: root}, dataDir: dataDir, admittedEntry: harness.EntryRef{SessionID: "session", EntryID: "entry"}, lease: workspaceTestLease(t, dataDir, "session")}
	result, err := scope.workspaceCall(context.Background(), "read", json.RawMessage(`{"path":"file"}`))
	if err != nil || string(result.Value) != `{"bytes":"YmVmb3Jl"}` {
		t.Fatalf("write grant precondition read: %s %v", result.Value, err)
	}
	write := json.RawMessage(`{"path":"file","bytes":"YWZ0ZXI=","create_only":false}`)
	invalid := json.RawMessage(`{"path":"file","bytes":"eA","create_only":false}`)
	if _, err := scope.workspaceCall(context.Background(), "write", invalid); !errors.Is(err, harness.ErrInvalid) {
		t.Fatalf("noncanonical base64: %v", err)
	}
	result, err = scope.workspaceCall(context.Background(), "write", write)
	if err != nil || string(result.Value) != `{"outcome":"completed"}` {
		t.Fatalf("write: %s %v", result.Value, err)
	}
	create := json.RawMessage(`{"path":"file","bytes":"eA==","create_only":true}`)
	_, err = scope.workspaceCall(context.Background(), "write", create)
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
	scope := nativeCallScope{session: harness.SessionIdentity{SessionID: "session", Workspace: root}, dataDir: dataDir, admittedEntry: harness.EntryRef{SessionID: "session", EntryID: "entry"}, lease: lease}
	result, err := scope.workspaceCall(context.Background(), "read", []byte(`{"path":"file"}`))
	if err != nil || string(result.Value) != `{"bytes":"YmVmb3Jl"}` {
		t.Fatalf("read with unrelated artifact lease: %s, %v", result.Value, err)
	}
	_, err = scope.workspaceCall(context.Background(), "write", []byte(`{"path":"file","bytes":"eA==","create_only":false}`))
	if !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("wrong artifact lease accepted: %v", err)
	}
}

func TestWorkspaceReportsCompletedMutationOnRecordFailure(t *testing.T) {
	root, dataDir := t.TempDir(), t.TempDir()
	scope := nativeCallScope{session: harness.SessionIdentity{SessionID: "session", Workspace: root}, dataDir: dataDir, admittedEntry: harness.EntryRef{SessionID: "session", EntryID: "entry"}, lease: workspaceTestLease(t, dataDir, "session")}
	args := json.RawMessage(`{"path":"file","bytes":"YWZ0ZXI=","create_only":false}`)
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
	_, err := scope.workspaceCall(context.Background(), "write", args)
	var failure *extension.CallError
	if !errors.As(err, &failure) || string(failure.Partial) != `{"outcome":"completed"}` {
		t.Fatalf("completed partial: %v", err)
	}
	data, readErr := os.ReadFile(filepath.Join(root, "file"))
	if readErr != nil || string(data) != "after" {
		t.Fatalf("completed content: %q %v", data, readErr)
	}
}

// TestWorkspaceMutationsOfOneExecutionRunOneAtATime proves that one
// execution's Workspace writes and deletes hold the lease's mutation slot from
// the preimage capture through the recorded identity, that a read runs while a
// mutation holds the slot, and that a waiting mutation returns its own
// cancellation instead of starting after the holder finishes.
func TestWorkspaceMutationsOfOneExecutionRunOneAtATime(t *testing.T) {
	for _, method := range []string{"write", "delete"} {
		t.Run(method, func(t *testing.T) {
			root, dataDir := t.TempDir(), t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "file"), []byte("before"), 0600); err != nil {
				t.Fatal(err)
			}
			lease := workspaceTestLease(t, dataDir, "session")
			scope := nativeCallScope{session: harness.SessionIdentity{SessionID: "session", Workspace: root}, dataDir: dataDir, admittedEntry: harness.EntryRef{SessionID: "session", EntryID: "entry"}, lease: lease}
			held := func() bool {
				lease.mu.Lock()
				defer lease.mu.Unlock()
				return len(lease.mutation) == 1
			}
			args := json.RawMessage(`{"path":"file"}`)
			if method == "write" {
				args = json.RawMessage(`{"path":"file","bytes":"YWZ0ZXI=","create_only":false}`)
			}
			parked := make(chan struct{}, 2)
			resume := make(chan struct{}, 1)
			var syncs atomic.Int32
			previous := atomicfs.SyncFileFunc
			atomicfs.SyncFileFunc = func(f *os.File) error {
				if strings.HasPrefix(filepath.Base(f.Name()), "meta.json.tmp-") {
					if n := syncs.Add(1); n == 1 || n == 3 {
						parked <- struct{}{}
						<-resume
					}
				}
				if previous != nil {
					return previous(f)
				}
				return f.Sync()
			}
			var result extension.Result
			var callErr error
			done := make(chan struct{})
			defer func() {
				close(resume)
				<-done
				atomicfs.SyncFileFunc = previous
			}()
			go func() {
				defer close(done)
				result, callErr = scope.workspaceCall(context.Background(), method, args)
			}()
			awaitSignal(t, parked, "the mutation's preimage sync")
			if !held() {
				t.Fatal("mutation lock is free at the preimage capture")
			}
			if method == "write" {
				readDone := make(chan struct{})
				var readResult extension.Result
				var readErr error
				go func() {
					defer close(readDone)
					readResult, readErr = scope.workspaceCall(context.Background(), "read", json.RawMessage(`{"path":"file"}`))
				}()
				awaitSignal(t, readDone, "the read during the parked mutation")
				if readErr != nil || string(readResult.Value) != `{"bytes":"YmVmb3Jl"}` {
					t.Fatalf("read during mutation: %s %v", readResult.Value, readErr)
				}
				inner, cancel := context.WithCancel(context.Background())
				defer cancel()
				waiting := &doneConsultedContext{Context: inner, consulted: make(chan struct{})}
				waiterDone := make(chan struct{})
				var waiterErr error
				go func() {
					defer close(waiterDone)
					_, waiterErr = scope.workspaceCall(waiting, "delete", json.RawMessage(`{"path":"file"}`))
				}()
				awaitSignal(t, waiting.consulted, "the waiting mutation's wait for the lock")
				cancel()
				awaitSignal(t, waiterDone, "the cancelled waiting mutation")
				if !errors.Is(waiterErr, context.Canceled) {
					t.Fatalf("waiting mutation: %v", waiterErr)
				}
				if !held() {
					t.Fatal("mutation lock is free while the write is parked")
				}
				assertFileContent(t, filepath.Join(root, "file"), "before")
			}
			resume <- struct{}{}
			awaitSignal(t, parked, "the mutation's identity sync")
			if !held() {
				t.Fatal("mutation lock is free at the identity record")
			}
			resume <- struct{}{}
			awaitSignal(t, done, "the mutation")
			if callErr != nil || string(result.Value) != `{"outcome":"completed"}` {
				t.Fatalf("%s: %s %v", method, result.Value, callErr)
			}
			if held() {
				t.Fatal("mutation lock is held after the call returned")
			}
			reverted, err := snapshot.RevertCodeGroups([]string{groupDir(dataDir, "session", "entry")})
			if err != nil || len(reverted.Restored) != 1 || len(reverted.Skipped) != 0 {
				t.Fatalf("revert: %+v %v", reverted, err)
			}
			assertFileContent(t, filepath.Join(root, "file"), "before")
		})
	}
}

// doneConsultedContext closes consulted the first time Done is called, so a
// test learns that the call holding it has started waiting on its context.
type doneConsultedContext struct {
	context.Context
	once      sync.Once
	consulted chan struct{}
}

func (c *doneConsultedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.consulted) })
	return c.Context.Done()
}
