package melta

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extensions/adaptation"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

const adaptationPatchAgent = `{"patcher": {"model": "prov/gpt-5.5", "system_prompt": "simple", "tools": ["read_file", "apply_patch"]}}`

const adaptationPatchInput = "*** Begin Patch\n*** Add File: added.txt\n+new\n*** Update File: updated.txt\n@@\n-old\n+new\n*** Delete File: deleted.txt\n*** Update File: moved.txt\n*** Move to: renamed.txt\n@@\n-alpha\n+ALPHA\n*** End Patch"

func adaptationPatchArguments(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"input": adaptationPatchInput})
	testassert.NoError(t, err)
	return string(raw)
}

// TestAdaptationPatchCorpusThroughRuntime runs the patch corpus through the
// registered Adaptation tool on a matching model: file results, preview
// metadata and snapshot capture, public reversion, and a cold read without
// Adaptation.
func TestAdaptationPatchCorpusThroughRuntime(t *testing.T) {
	ctx := context.Background()
	e := newProductionEnv(t, storage.NewMemory())
	writeServiceFile(t, e.configPath, adaptationConfigDocument(e.server.URL, `[]`))
	writeAgentDefinitions(t, e.configPath, adaptationPatchAgent)
	patchArguments := adaptationPatchArguments(t)
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("patch-call", "apply_patch", patchArguments)
	})
	r, err := e.openPlugins(ctx, agentsTestNative(), adaptation.Author())
	testassert.NoError(t, err)
	workspace := adaptationWorkspace(t, e, "patch-corpus")
	for name, content := range map[string]string{"updated.txt": "old\n", "deleted.txt": "gone\n", "moved.txt": "alpha\n"} {
		testassert.NoError(t, os.WriteFile(filepath.Join(workspace, name), []byte(content), 0o600))
	}
	session, err := r.createSession(ctx, workspace, "patcher")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-patch", "apply the corpus")
	record := awaitOperation(t, r, session.Identity.SessionID, "op-patch", harness.OperationSuccess)

	// The matching bundled row offers apply_patch, and the tool runs.
	testassert.Require(t, slices.Contains(record.Admission.Execution.InitialToolNames, "apply_patch"), "admitted tools = %v, want the configured apply_patch", record.Admission.Execution.InitialToolNames)
	assertFileContent(t, filepath.Join(workspace, "added.txt"), "new")
	assertFileContent(t, filepath.Join(workspace, "updated.txt"), "new\n")
	assertPathMissing(t, filepath.Join(workspace, "deleted.txt"))
	assertPathMissing(t, filepath.Join(workspace, "moved.txt"))
	assertFileContent(t, filepath.Join(workspace, "renamed.txt"), "ALPHA\n")

	result := toolResultFact(t, r, session.Identity.SessionID, "op-patch")
	testassert.Require(t, result.Status == model.ResultSuccess && strings.Contains(result.Content, "Success. Updated the following files:"), "patch result = %+v, want the success summary", result)
	var metadata struct {
		Files []struct {
			Path string `json:"path"`
			Op   string `json:"op"`
		} `json:"edit_preview_files"`
	}
	{
		err := json.Unmarshal(result.Metadata, &metadata)
		testassert.Require(t, err == nil && len(metadata.Files) == 5, "metadata = %s, want five per-file previews", result.Metadata)
	}
	var ops []string
	for _, file := range metadata.Files {
		ops = append(ops, file.Path+"="+file.Op)
	}
	testassert.Require(t, strings.Join(ops, ",") == "added.txt=A,updated.txt=M,deleted.txt=D,renamed.txt=M,moved.txt=D", "previews = %v, want the applied A/M/D list in plan order", ops)

	// Every mutation is captured in the Operation's one snapshot group.
	groups, err := r.ListSessionCodeSnapshots(ctx, testCaller(r), session.Identity.SessionID)
	testassert.Require(t, err == nil && len(groups) == 1 && groups[0].OperationId == "op-patch" && len(groups[0].Files) == 5, "snapshot groups = %+v (%v), want the patch's five files", groups, err)
	var snapshotted []string
	for _, file := range groups[0].Files {
		snapshotted = append(snapshotted, filepath.Base(file.Path))
	}
	slices.Sort(snapshotted)
	testassert.Require(t, slices.Equal(snapshotted, []string{"added.txt", "deleted.txt", "moved.txt", "renamed.txt", "updated.txt"}), "snapshot files = %v, want every touched path", snapshotted)

	// Public reversion restores every target.
	awaitRestorableSession(t, r, session.Identity.SessionID)
	reverted, err := r.RevertSessionCode(ctx, testCaller(r), session.Identity.SessionID, "op-patch")
	testassert.Require(t, err == nil && len(reverted.Skipped) == 0, "revert = %+v (%v), want no skipped target", reverted, err)
	var restored []string
	for _, path := range reverted.Restored {
		restored = append(restored, filepath.Base(path))
	}
	slices.Sort(restored)
	testassert.Require(t, slices.Equal(restored, []string{"added.txt", "deleted.txt", "moved.txt", "renamed.txt", "updated.txt"}), "restored = %v, want every touched path", restored)
	assertPathMissing(t, filepath.Join(workspace, "added.txt"))
	assertFileContent(t, filepath.Join(workspace, "updated.txt"), "old\n")
	assertFileContent(t, filepath.Join(workspace, "deleted.txt"), "gone\n")
	assertFileContent(t, filepath.Join(workspace, "moved.txt"), "alpha\n")
	assertPathMissing(t, filepath.Join(workspace, "renamed.txt"))

	// A cold read without Adaptation shows the recorded call and result.
	testassert.NoError(t, r.Close(ctx))
	cold, err := e.openPlugins(ctx)
	testassert.NoError(t, err)
	defer func() { _ = cold.Close(ctx) }()
	page, err := cold.GetHistory(ctx, testCaller(cold), session.Identity.SessionID, nil)
	testassert.NoError(t, err)
	encoded, err := json.Marshal(page)
	testassert.NoError(t, err)
	testassert.Require(t, strings.Contains(string(encoded), `"apply_patch"`) && strings.Contains(string(encoded), "Success. Updated the following files:"), "cold history = %.400s, want the recorded patch call and result", encoded)
}

// TestAdaptationPatchDeniesOneMemberBeforeAnyWrite proves one denied
// authorization member leaves the complete patch unapplied.
func TestAdaptationPatchDeniesOneMemberBeforeAnyWrite(t *testing.T) {
	ctx := context.Background()
	e := newProductionEnv(t, storage.NewMemory())
	writeServiceFile(t, e.configPath, adaptationConfigDocument(e.server.URL, `[]`))
	writeAgentDefinitions(t, e.configPath, adaptationPatchAgent)
	arguments, err := json.Marshal(map[string]any{"input": "*** Begin Patch\n*** Update File: notes.txt\n*** Move to: .env\n@@\n-alpha\n+ALPHA\n*** End Patch"})
	testassert.NoError(t, err)
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("denied-patch", "apply_patch", string(arguments))
	})
	r, err := e.openPlugins(ctx, agentsTestNative(), adaptation.Author())
	testassert.NoError(t, err)
	defer func() { _ = r.Close(ctx) }()
	workspace := adaptationWorkspace(t, e, "patch-denied")
	testassert.NoError(t, os.WriteFile(filepath.Join(workspace, "notes.txt"), []byte("alpha\n"), 0o600))
	session, err := r.createSession(ctx, workspace, "patcher")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-denied", "move notes")
	awaitOperation(t, r, session.Identity.SessionID, "op-denied", harness.OperationSuccess)

	result := toolResultFact(t, r, session.Identity.SessionID, "op-denied")
	testassert.Require(t, result.Status == model.ResultDenied && result.Content == harness.PermissionDeniedContent, "denied patch = %+v, want the fixed policy denial", result)
	assertFileContent(t, filepath.Join(workspace, "notes.txt"), "alpha\n")
	assertPathMissing(t, filepath.Join(workspace, ".env"))
	groups, err := r.ListSessionCodeSnapshots(ctx, testCaller(r), session.Identity.SessionID)
	testassert.Require(t, err == nil && len(groups) == 0, "snapshot groups = %+v (%v), want none for the denied patch", groups, err)
}

// TestAdaptationPatchBindsBeforeEffects proves a bound path repointed before
// execution and a destination appearing before the create-only write are both
// refused through the registered Adaptation tool.
func TestAdaptationPatchBindsBeforeEffects(t *testing.T) {
	ctx := context.Background()
	candidate, err := (&configurationService{}).prepareNativeCandidate(ctx, []nativeActivationSpec{{
		extension: adaptation.Author(), section: []byte(`{"enabled":true}`),
	}})
	testassert.NoError(t, err)
	defer candidate.release()
	stage := candidate.snapshotForTest()
	defer stage.release()
	binding := stage.bindTool("apply_patch")
	testassert.Require(t, binding != nil, "adaptation registered no apply_patch tool")
	defer binding.release()
	dataDir := t.TempDir()
	tool := registeredExtensionTool{binding: binding, dataDir: dataDir, lease: workspaceTestLease(t, dataDir, "session"), host: &composition{}}

	t.Run("repointed bound path", func(t *testing.T) {
		root := t.TempDir()
		real1, real2 := filepath.Join(root, "real1.txt"), filepath.Join(root, "real2.txt")
		testassert.NoError(t, os.WriteFile(real1, []byte("before"), 0o600))
		testassert.NoError(t, os.WriteFile(real2, []byte("other"), 0o600))
		link := filepath.Join(root, "link.txt")
		testassert.NoError(t, os.Symlink(real1, link))
		tc := ToolContext{Session: harness.SessionIdentity{SessionID: "session", Workspace: root}, Workspace: root, AdmittedEntry: harness.EntryRef{SessionID: "session", EntryID: "entry"}}
		arguments, err := json.Marshal(map[string]string{"input": "*** Begin Patch\n*** Update File: link.txt\n@@\n-before\n+after\n*** End Patch"})
		testassert.NoError(t, err)
		call := model.ToolCall{ID: "call-1", Name: "apply_patch", Arguments: arguments}
		normalized, err := tool.Normalize(ctx, tc, call)
		testassert.NoError(t, err)
		call.Arguments = normalized
		prepared := tool.Prepare(ctx, tc, call)
		testassert.Require(t, prepared.Immediate == nil, "prepare = %+v", prepared.Immediate)
		testassert.Require(t, len(prepared.Permissions) == 1 && prepared.Permissions[0].Permission == harness.PermissionFileWrite && prepared.Permissions[0].Target == real1, "permissions = %+v, want only the target's file.write grant", prepared.Permissions)
		testassert.NoError(t, os.Remove(link))
		testassert.NoError(t, os.Symlink(real2, link))
		outcome := prepared.Execute(ctx)
		testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, "approved canonical path changed"), "outcome = %+v, want the changed-binding refusal", outcome.Result)
		assertFileContent(t, real1, "before")
		assertFileContent(t, real2, "other")
	})

	t.Run("destination exists at execution start", func(t *testing.T) {
		root := t.TempDir()
		tc := ToolContext{Session: harness.SessionIdentity{SessionID: "session", Workspace: root}, Workspace: root, AdmittedEntry: harness.EntryRef{SessionID: "session", EntryID: "entry"}}
		arguments, err := json.Marshal(map[string]string{"input": "*** Begin Patch\n*** Add File: first.txt\n+one\n*** Add File: late.txt\n+two\n*** End Patch"})
		testassert.NoError(t, err)
		call := model.ToolCall{ID: "call-1", Name: "apply_patch", Arguments: arguments}
		normalized, err := tool.Normalize(ctx, tc, call)
		testassert.NoError(t, err)
		call.Arguments = normalized
		prepared := tool.Prepare(ctx, tc, call)
		testassert.Require(t, prepared.Immediate == nil, "prepare = %+v", prepared.Immediate)
		testassert.NoError(t, os.WriteFile(filepath.Join(root, "late.txt"), []byte("outside"), 0o600))
		outcome := prepared.Execute(ctx)
		testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, "already exists"), "outcome = %+v, want the current-existence refusal", outcome.Result)
		assertPathMissing(t, filepath.Join(root, "first.txt"))
		assertFileContent(t, filepath.Join(root, "late.txt"), "outside")
	})
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	testassert.Require(t, err == nil && string(data) == want, "%s = (%q, %v), want %q", path, data, err, want)
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s exists: %v", path, err)
	}
}

// TestAdaptationBuiltinPrimaryPatchAdvertisement proves the built-in primary
// advertises and runs apply_patch on an offering bundled model while the same
// primary on a non-offering model does not advertise it.
func TestAdaptationBuiltinPrimaryPatchAdvertisement(t *testing.T) {
	for _, row := range []struct {
		name       string
		model      string
		wantTool   bool
		toolScript bool
	}{
		{"offering model", "prov/gpt-5.5", true, true},
		{"non-offering model", "prov/adaptive", false, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			e := newProductionEnv(t, storage.NewMemory())
			document := adaptationConfigDocument(e.server.URL, `[]`)
			document = strings.Replace(document, `{`, `{"model":"`+row.model+`",`, 1)
			writeServiceFile(t, e.configPath, document)
			writeAgentDefinitions(t, e.configPath, `{}`)
			arguments, err := json.Marshal(map[string]string{"input": "*** Begin Patch\n*** Add File: primary-patch.txt\n+patched\n*** End Patch"})
			testassert.NoError(t, err)
			e.server.setScript(func(_ context.Context, body string) []string {
				if !row.toolScript || lastMessageRole(body) == "tool" {
					return textTurnEvents("done")
				}
				return toolCallTurnEvents("primary-patch", "apply_patch", string(arguments))
			})
			r, err := e.openPlugins(ctx, agentsTestNative(), adaptation.Author())
			testassert.NoError(t, err)
			defer func() { _ = r.Close(ctx) }()
			workspace := adaptationWorkspace(t, e, "primary-patch")
			session, err := r.createSession(ctx, workspace, "primary")
			testassert.NoError(t, err)
			submitThroughRuntime(t, r, session.Identity.SessionID, "op-primary", "patch a file")
			record := awaitOperation(t, r, session.Identity.SessionID, "op-primary", harness.OperationSuccess)
			testassert.Require(t, slices.Contains(record.Admission.Execution.InitialToolNames, "apply_patch"), "admitted tools = %v, want the built-in primary to carry apply_patch", record.Admission.Execution.InitialToolNames)
			wire := decodeWireChatBody(t, e.server.bodyAt(0))
			testassert.Require(t, slices.Contains(wireToolNames(wire), "apply_patch") == row.wantTool, "wire tools = %v, want apply_patch=%v", wireToolNames(wire), row.wantTool)
			if row.wantTool {
				assertFileContent(t, filepath.Join(workspace, "primary-patch.txt"), "patched")
			} else {
				assertPathMissing(t, filepath.Join(workspace, "primary-patch.txt"))
			}
		})
	}
}
