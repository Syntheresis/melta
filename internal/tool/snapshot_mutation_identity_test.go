package tool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/atomicfs"
	"github.com/Syntheresis/melta/internal/testassert"
)

// Last-produced snapshot identities must come only from what the mutation
// producer actually completed: retained write byte counts and known-complete
// produced content, never a post-error filesystem reread. The pre-mutation
// proof clear must also hold: when it fails, no mutation I/O may run.

// --- incomplete and foreign-byte mutation failures over the real store ---

func TestShortWriteClaimsNoProducedIdentityAndRevertSkips(t *testing.T) {
	for _, step := range []mutationFailureStep{failShortWriteError, failShortWriteNil} {
		for _, tc := range producerMutationCases() {
			t.Run(string(step)+"_"+tc.name, func(t *testing.T) {
				runProducerMutationCase(t, tc, step)
			})
		}
	}
}

func TestForeignWriteBeforeErrorKeepsOwnProducedIdentityAndRevertSkips(t *testing.T) {
	for _, step := range []mutationFailureStep{failForeignWriteError, failForeignSync} {
		for _, tc := range producerMutationCases() {
			t.Run(string(step)+"_"+tc.name, func(t *testing.T) {
				runProducerMutationCase(t, tc, step)
			})
		}
	}
}

// --- mutation file I/O counting ---

type mutationIOCount struct {
	opens     int
	writes    int
	truncates int
}

type countingMutationFile struct {
	mutationFile
	count *mutationIOCount
}

func (c *countingMutationFile) Write(p []byte) (int, error) {
	c.count.writes++
	return c.mutationFile.Write(p)
}

func (c *countingMutationFile) Truncate(size int64) error {
	c.count.truncates++
	return c.mutationFile.Truncate(size)
}

func withMutationFileCounting(t *testing.T, count *mutationIOCount) {
	t.Helper()
	prev := openWriteTargetForMutationFunc
	openWriteTargetForMutationFunc = func(absPath string) (mutationFile, bool, error) {
		f, existed, err := prev(absPath)
		if err == nil {
			count.opens++
			f = &countingMutationFile{mutationFile: f, count: count}
		}
		return f, existed, err
	}
	t.Cleanup(func() { openWriteTargetForMutationFunc = prev })
}

// --- snapshot meta publication failure seam ---

type injectedMetaSyncError string

func (e injectedMetaSyncError) Error() string { return string(e) }

var errInjectedMetaSync = injectedMetaSyncError("injected snapshot meta publish failure")

// withSnapshotMetaPublishFailure injects a failure on the nth meta.json
// temp-file publication (capture writes and proof/identity writes) done while
// the seam is installed, leaving every other publication to the real sync.
func withSnapshotMetaPublishFailure(t *testing.T, nth int) {
	t.Helper()
	previous := atomicfs.SyncFileFunc
	metaSyncs := 0
	atomicfs.SyncFileFunc = func(f *os.File) error {
		if strings.HasPrefix(filepath.Base(f.Name()), "meta.json.tmp-") {
			metaSyncs++
			if metaSyncs == nth {
				return errInjectedMetaSync
			}
		}
		if previous != nil {
			return previous(f)
		}
		return f.Sync()
	}
	t.Cleanup(func() { atomicfs.SyncFileFunc = previous })
}

// --- pre-mutation proof clear and prior-proof restoration ---

func TestProofClearFailureBeforeMutationLeavesFileAndPreimageUntouched(t *testing.T) {
	root := t.TempDir()
	path := snapshotLifecycleFile(t, root, "file.txt", "before")
	groupDir := filepath.Join(t.TempDir(), "group")
	store := newCodeGroupStore(t, groupDir)
	{
		_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "content": "first"})
		testassert.NoError(t, err)
	}
	testassert.Require(t, soleSnapshotMeta(t, store).LastWrite != nil && soleSnapshotMeta(t, store).LastWrite.Hash == snapshotIdentityHash("first"), "seed LastWrite = %+v, want the first write's proof", soleSnapshotMeta(t, store).LastWrite)

	withSnapshotMetaPublishFailure(t, 1)
	counts := &mutationIOCount{}
	withMutationFileCounting(t, counts)

	_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "content": "second"})

	testassert.Require(t, err != nil, "write whose pre-mutation proof clear fails succeeded, want refusal without mutation")
	assertFileContent(t, path, "first")
	last := soleSnapshotMeta(t, store).LastWrite
	testassert.Require(t, last != nil && last.Hash == snapshotIdentityHash("first"), "LastWrite after proof-clear failure = %+v, want the prior proof unchanged", last)
	assertEntryOriginal(t, groupDir, "before")
	testassert.Require(t, counts.writes == 0 && counts.truncates == 0, "mutation file I/O = %+v, want no Write/Truncate after a failed proof clear", counts)
}

func TestProvenPreMutationFailureRestoresPriorProof(t *testing.T) {
	root := t.TempDir()
	path := snapshotLifecycleFile(t, root, "file.txt", "before")
	groupDir := filepath.Join(t.TempDir(), "group")
	store := newCodeGroupStore(t, groupDir)
	{
		_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "content": "first"})
		testassert.NoError(t, err)
	}

	_, err := execEdit(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "old_string": "missing", "new_string": "second"})

	testassert.Require(t, err != nil, "edit with no match succeeded, want a pre-mutation failure")
	assertFileContent(t, path, "first")
	last := soleSnapshotMeta(t, store).LastWrite
	testassert.Require(t, last != nil && last.Hash == snapshotIdentityHash("first"), "LastWrite after proven non-mutating failure = %+v, want the prior proof restored", last)
	result := revertTestGroup(t, groupDir)
	testassert.Require(t, len(result.Restored) == 1 && result.Restored[0] == path && len(result.Skipped) == 0, "restored = %v skipped = %+v, want the retained snapshot to restore %s", result.Restored, result.Skipped, path)
	assertFileContent(t, path, "before")
}

func TestFailedPriorProofRestoreKeepsNilProofAndSkipsLaterRevert(t *testing.T) {
	root := t.TempDir()
	path := snapshotLifecycleFile(t, root, "file.txt", "before")
	groupDir := filepath.Join(t.TempDir(), "group")
	store := newCodeGroupStore(t, groupDir)
	{
		_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "content": "first"})
		testassert.NoError(t, err)
	}

	// The failing publication is the prior-proof restore: the pre-mutation
	// proof clear that precedes it succeeds, the restore is the second meta
	// publication of this call.
	withSnapshotMetaPublishFailure(t, 2)
	_, err := execEdit(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "old_string": "missing", "new_string": "second"})

	testassert.Require(t, err != nil && strings.Contains(err.Error(), "restore snapshot proof"), "edit error = %v, want the failed proof restoration reported", err)
	assertFileContent(t, path, "first")
	testassert.Require(t, soleSnapshotMeta(t, store).LastWrite == nil, "LastWrite after failed proof restoration = %+v, want nil proof", soleSnapshotMeta(t, store).LastWrite)
	assertEntryOriginal(t, groupDir, "before")
	result := revertTestGroup(t, groupDir)
	testassert.Require(t, len(result.Restored) == 0 && len(result.Skipped) == 1 && result.Skipped[0].Path == path, "restored = %v skipped = %+v, want the nil proof to skip %s", result.Restored, result.Skipped, path)
	assertSnapshotEntryCount(t, groupDir, 1)
	assertFileContent(t, path, "first")
}

func TestFailedFinalIdentityRecordKeepsNilProofAndSkipsRevert(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "created.txt")
	groupDir := filepath.Join(t.TempDir(), "group")
	store := newCodeGroupStore(t, groupDir)

	// Publications under the seam: capture meta, pre-mutation proof clear,
	// and the final identity record — only the final record fails.
	withSnapshotMetaPublishFailure(t, 3)
	_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "content": "created"})

	testassert.Require(t, err != nil && strings.Contains(err.Error(), "record snapshot identity"), "write error = %v, want the failed final identity record", err)
	assertFileContent(t, path, "created")
	testassert.Require(t, soleSnapshotMeta(t, store).LastWrite == nil, "LastWrite after failed final record = %+v, want nil proof", soleSnapshotMeta(t, store).LastWrite)
	assertSnapshotEntryCount(t, groupDir, 1)
	result := revertTestGroup(t, groupDir)
	testassert.Require(t, len(result.Restored) == 0 && len(result.Skipped) == 1 && result.Skipped[0].Path == path, "restored = %v skipped = %+v, want the nil proof to skip %s", result.Restored, result.Skipped, path)
	assertFileContent(t, path, "created")
}

func TestPatchPartialSummaryUsesCompletedMutation(t *testing.T) {
	for _, tc := range []struct {
		name, patch, target, targetContent string
		want, forbidden                    []string
		record                             int
		step                               mutationFailureStep
		targetMissing, sourceUnchanged     bool
	}{
		{name: "add_record", patch: "*** Add File: new.txt\n+after", target: "new.txt", targetContent: "after", want: []string{"A new.txt"}, record: 3},
		{name: "update_sync_record", patch: "*** Update File: old.txt\n@@\n-before\n+after", target: "old.txt", targetContent: "after", want: []string{"M old.txt"}, record: 3, step: failAfterSync},
		{name: "delete_record", patch: "*** Delete File: old.txt", target: "old.txt", targetMissing: true, want: []string{"D old.txt"}, record: 3},
		{name: "move_destination_record", patch: "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after", target: "new.txt", targetContent: "after", sourceUnchanged: true, want: []string{"M new.txt"}, forbidden: []string{"D old.txt"}, record: 3},
		{name: "move_destination_short", patch: "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after", target: "new.txt", targetContent: "af", sourceUnchanged: true, forbidden: []string{"M new.txt", "D old.txt"}, step: failShortWriteError},
		{name: "move_source_record", patch: "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after", target: "old.txt", targetMissing: true, want: []string{"M new.txt", "D old.txt"}, record: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			old := snapshotLifecycleFile(t, root, "old.txt", "before")
			path := filepath.Join(root, tc.target)
			store := newTempCodeGroupStore(t)
			if tc.record != 0 {
				withSnapshotMetaPublishFailure(t, tc.record)
			}
			if tc.step != "" {
				withMutationFailure(t, path, tc.step)
			}
			_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, tc.patch)})
			var exitErr *ExitError
			testassert.Require(t, errors.As(err, &exitErr), "error = %v, want partial summary", err)
			for _, line := range tc.want {
				testassert.Require(t, strings.Contains(exitErr.Output, line), "summary = %q, want %q", exitErr.Output, line)
			}
			for _, line := range tc.forbidden {
				testassert.Require(t, !strings.Contains(exitErr.Output, line), "summary = %q, must not report %q", exitErr.Output, line)
			}
			meta := metaForCanonical(t, store, path)
			testassert.Require(t, meta.LastWrite == nil, "LastWrite = %+v, want nil", meta.LastWrite)
			if tc.sourceUnchanged {
				assertFileContent(t, old, "before")
			}
			if tc.targetMissing {
				assertPathMissing(t, path)
			} else {
				assertFileContent(t, path, tc.targetContent)
			}
			result := revertTestGroup(t, store.groupDir)
			testassert.Require(t, len(result.Skipped) == 1, "revert = %+v, want skipped uncertain proof", result)
		})
	}
}

func TestPatchPreconditionsUseCurrentExistence(t *testing.T) {
	for _, tc := range []struct {
		name, input          string
		exists, missingAfter bool
	}{
		{"add_existing_disappears", "*** Add File: file.txt\n+after", true, false},
		{"update_missing_appears", "*** Update File: file.txt\n@@\n-before\n+after", false, false},
		{"delete_missing_appears", "*** Delete File: file.txt", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "file.txt")
			if tc.exists {
				testassert.NoError(t, os.WriteFile(path, []byte("before"), 0o644))
			}
			store := newTempCodeGroupStore(t)
			args, err := NormalizePatchArgs(map[string]any{"input": applyPatchInput(t, tc.input)})
			testassert.NoError(t, err)
			call, err := PreparePatchCall(root, "", "", CapabilityOptions{}, store, args)
			testassert.NoError(t, err)
			if tc.exists {
				testassert.NoError(t, os.Remove(path))
			} else {
				testassert.NoError(t, os.WriteFile(path, []byte("before"), 0o644))
			}
			_, err = call.Execute(context.Background(), nil)
			testassert.NoError(t, err)
			if tc.missingAfter {
				assertPathMissing(t, path)
			} else {
				assertFileContent(t, path, "after")
			}
			result := revertTestGroup(t, store.groupDir)
			testassert.Require(t, len(result.Restored) == 1 && len(result.Skipped) == 0, "revert = %+v", result)
			if tc.exists {
				assertPathMissing(t, path)
			} else {
				assertFileContent(t, path, "before")
			}
		})
	}
	t.Run("move_destination_disappears", func(t *testing.T) {
		root := t.TempDir()
		source := snapshotLifecycleFile(t, root, "old.txt", "before")
		destination := snapshotLifecycleFile(t, root, "new.txt", "outside")
		store := newTempCodeGroupStore(t)
		args, err := NormalizePatchArgs(map[string]any{"input": applyPatchInput(t, "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after")})
		testassert.NoError(t, err)
		call, err := PreparePatchCall(root, "", "", CapabilityOptions{}, store, args)
		testassert.NoError(t, err)
		testassert.NoError(t, os.Remove(destination))
		_, err = call.Execute(context.Background(), nil)
		testassert.NoError(t, err)
		assertPathMissing(t, source)
		assertFileContent(t, destination, "after")
		result := revertTestGroup(t, store.groupDir)
		testassert.Require(t, len(result.Restored) == 2 && len(result.Skipped) == 0, "revert = %+v", result)
		assertFileContent(t, source, "before")
		assertPathMissing(t, destination)
	})
}

func TestPatchUpdateDisappearsDuringCapture(t *testing.T) {
	root := t.TempDir()
	path := snapshotLifecycleFile(t, root, "file.txt", "before")
	store := newRecordingCodeGroupStore(t)
	store.onSnapshot = func(int) { testassert.NoError(t, os.Remove(path)) }
	result, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{
		"input": applyPatchInput(t, "*** Update File: file.txt\n@@\n-before\n+after"),
	})
	testassert.NoError(t, err)
	testassert.Require(t, strings.Contains(result.Result, "M file.txt"), "result = %q, want completed update", result.Result)
	assertFileContent(t, path, "after")
	last := soleSnapshotMeta(t, store.testCodeGroupStore).LastWrite
	testassert.Require(t, last != nil && last.Hash == snapshotIdentityHash("after"), "LastWrite = %+v, want produced bytes", last)
	restored := revertTestGroup(t, store.groupDir)
	testassert.Require(t, len(restored.Restored) == 1 && restored.Restored[0] == path && len(restored.Skipped) == 0, "revert = %+v", restored)
	assertFileContent(t, path, "before")
}

func TestMutationPreflightBeforeCapture(t *testing.T) {
	for _, tc := range []struct {
		name   string
		run    func(*testing.T, string, string, SnapshotStore) error
		delete bool
	}{
		{"write", func(t *testing.T, root, path string, store SnapshotStore) error {
			_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "content": "after"})
			return err
		}, false},
		{"edit", func(t *testing.T, root, path string, store SnapshotStore) error {
			_, err := execEdit(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": path, "old_string": "before", "new_string": "after"})
			return err
		}, false},
		{"patch_update", func(t *testing.T, root, _ string, store SnapshotStore) error {
			_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Update File: file.txt\n@@\n-before\n+after")})
			return err
		}, false},
		{"patch_delete", func(t *testing.T, root, _ string, store SnapshotStore) error {
			_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Delete File: file.txt")})
			return err
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := snapshotLifecycleFile(t, root, "file.txt", "before")
			testassert.NoError(t, os.Chmod(path, 0o444))
			if f, err := os.OpenFile(path, os.O_RDWR, 0); err == nil {
				f.Close()
				t.Skip("filesystem permits writing a read-only file")
			} else if !errors.Is(err, os.ErrPermission) {
				t.Fatal(err)
			}
			store := newRecordingCodeGroupStore(t)
			err := tc.run(t, root, path, store)
			if tc.delete {
				testassert.NoError(t, err)
				assertPathMissing(t, path)
				result := revertTestGroup(t, store.groupDir)
				testassert.Require(t, len(result.Restored) == 1 && len(result.Skipped) == 0, "revert = %+v", result)
				assertFileContent(t, path, "before")
			} else {
				testassert.Require(t, err != nil, "mutation of read-only target succeeded")
				testassert.Require(t, len(store.calls) == 0, "captures = %+v, want preflight refusal", store.calls)
				assertSnapshotEntryCount(t, store.groupDir, 0)
				assertFileContent(t, path, "before")
			}
		})
	}
}

func TestPreparedEditAndWriteWithAppearedTarget(t *testing.T) {
	for _, edit := range []bool{true, false} {
		t.Run(map[bool]string{true: "edit", false: "write"}[edit], func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "file.txt")
			store := newTempCodeGroupStore(t)
			var call *PreparedCall
			var args map[string]any
			var err error
			if edit {
				args, err = NormalizeEditArgs(map[string]any{"path": path, "old_string": "before", "new_string": "after"})
				testassert.NoError(t, err)
				call, err = PrepareEditCall(root, "", "", CapabilityOptions{}, store, args)
				testassert.NoError(t, err)
			} else {
				args, err = NormalizeWriteArgs(map[string]any{"path": path, "content": "after"})
				testassert.NoError(t, err)
				call, err = PrepareWriteCall(root, "", "", CapabilityOptions{}, store, args)
				testassert.NoError(t, err)
			}
			testassert.NoError(t, os.WriteFile(path, []byte("before"), 0o644))
			_, err = call.Execute(context.Background(), nil)
			testassert.NoError(t, err)
			assertFileContent(t, path, "after")
		})
	}
}

// --- destination appeared during apply: refusal plus abandonment cleanup ---

// appearedDestinationBytes are the outside-origin bytes that create the
// destination between plan validation and the apply-time open.
var appearedDestinationBytes = []byte("appeared")

type injectedAbandonError string

func (e injectedAbandonError) Error() string { return string(e) }

var errInjectedAbandon = injectedAbandonError("injected snapshot abandonment failure")

func TestAppearedDestinationWithFailedAbandonReportsRefusalAndCleanupFailure(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "created.txt")
	groupDir := filepath.Join(t.TempDir(), "group")
	store := newCodeGroupStore(t, groupDir)
	{
		_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Add File: created.txt\n+one")})
		testassert.NoError(t, err)
	}
	testassert.NoError(t, os.Remove(path))

	wrapped := &recordingCodeGroupStore{testCodeGroupStore: store, plantBefore: path, failAbandon: true}
	_, err := execPatch(t, root, "", "", CapabilityOptions{}, wrapped, map[string]any{"input": applyPatchInput(t, "*** Add File: created.txt\n+two")})

	testassert.Require(t, err != nil, "Add whose destination appeared with failing abandonment succeeded, want refusal")
	msg := err.Error()
	testassert.Require(t, strings.Contains(msg, "appeared during apply"), "err = %v, want the destination-appeared refusal itself", err)
	testassert.Require(t, strings.Contains(msg, "additionally failed to abandon snapshot"), "err = %v, want the abandonment failure reported alongside the refusal", err)
	testassert.Require(t, strings.Contains(msg, errInjectedAbandon.Error()), "err = %v, want the injected cleanup failure visible", err)
	testassert.Require(t, !strings.Contains(msg, "%!w(<nil>)"), "err = %q, want the refusal wrapped around the cleanup failure, no %%!w(<nil>)", msg)
	testassert.Require(t, len(wrapped.discards) == 0, "discards = %v, want none: the reused entry is not discardable", wrapped.discards)
	assertFileContent(t, path, string(appearedDestinationBytes))
	testassert.Require(t, soleSnapshotMeta(t, store).LastWrite == nil, "LastWrite after failed restoration = %+v, want the cleared nil proof preserved", soleSnapshotMeta(t, store).LastWrite)
	assertSnapshotEntryCount(t, groupDir, 1)
}

func TestAppearedDestinationWithCompletedAbandonKeepsEntryAndPriorProof(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "created.txt")
	groupDir := filepath.Join(t.TempDir(), "group")
	store := newCodeGroupStore(t, groupDir)
	{
		_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Add File: created.txt\n+one")})
		testassert.NoError(t, err)
	}
	testassert.NoError(t, os.Remove(path))

	wrapped := &recordingCodeGroupStore{testCodeGroupStore: store, plantBefore: path}
	_, err := execPatch(t, root, "", "", CapabilityOptions{}, wrapped, map[string]any{"input": applyPatchInput(t, "*** Add File: created.txt\n+two")})

	testassert.Require(t, err != nil && strings.Contains(err.Error(), "appeared during apply"), "err = %v, want the destination-appeared refusal", err)
	testassert.Require(t, !strings.Contains(err.Error(), "additionally failed"), "err = %v, want the completed abandonment reported without a cleanup failure", err)
	testassert.Require(t, !strings.Contains(err.Error(), "%!w(<nil>)"), "err = %q, want no %%!w(<nil>)", err.Error())
	testassert.Require(t, len(wrapped.discards) == 0, "discards = %v, want none: the reused entry survives", wrapped.discards)
	assertFileContent(t, path, string(appearedDestinationBytes))
	last := soleSnapshotMeta(t, store).LastWrite
	testassert.Require(t, last != nil && last.Hash == snapshotIdentityHash("one"), "LastWrite after refusal = %+v, want the existing entry's prior proof restored", last)
	assertSnapshotEntryCount(t, groupDir, 1)
}

// --- post-capture rebinding refusals for every mutation producer ---

func rebindingLifecycleFixture(t *testing.T) (root, link, real1, real2 string) {
	t.Helper()
	root = t.TempDir()
	real1 = filepath.Join(root, "real1.txt")
	real2 = filepath.Join(root, "real2.txt")
	for _, p := range []string{real1, real2} {
		testassert.NoError(t, os.WriteFile(p, []byte("before"), 0o644))
	}
	link = filepath.Join(root, "link.txt")
	testassert.NoError(t, os.Symlink(real1, link))
	return root, link, real1, real2
}

func rebindLink(t *testing.T, link, target string) {
	t.Helper()
	testassert.NoError(t, os.Remove(link))
	testassert.NoError(t, os.Symlink(target, link))
}

// TestMutationToolsRefuseRebindingDuringSnapshotCapture threads every patch
// mutation (Update, Add, Delete, Move destination, Move source) and the
// write/edit producers through target, Workspace-root, and write-dir rebinding
// flips that land AFTER the real snapshot capture (the wrapper's onSnapshot
// seam): the post-capture fail-closed revalidators must refuse, leave BOTH
// possible targets unmutated, and abandon the refused capture on disk — a
// freshly created capture is discarded; a reused entry survives with its prior
// proof restored (shared abandonment rule, observed by the two reused rows).
// Move-source refusals must keep the already completed destination and report
// the partial work.
func TestMutationToolsRefuseRebindingDuringSnapshotCapture(t *testing.T) {
	refusalFlip := func(store *recordingCodeGroupStore, at int, flip func()) {
		store.onSnapshot = func(call int) {
			if call == at {
				flip()
			}
		}
	}
	assertPartialCommitted := func(t *testing.T, err error, committed string) {
		t.Helper()
		var exitErr *ExitError
		testassert.Require(t, errors.As(err, &exitErr) && strings.Contains(exitErr.Output, committed), "Execute err = %v, want partial work reporting %s", err, committed)
	}
	assertPlantedSymlink := func(t *testing.T, path string) {
		t.Helper()
		info, err := os.Lstat(path)
		testassert.Require(t, err == nil && info.Mode()&os.ModeSymlink != 0, "%s = %v %v, want the planted symlink, never a written regular file", path, info, err)
	}
	// A refused capture created by this call must be discarded on disk with
	// zero recorded identity for the refused operation.
	assertFreshCaptureAbandoned := func(t *testing.T, store *recordingCodeGroupStore, canonical string) {
		t.Helper()
		testassert.Require(t, len(store.discards) == 1, "discards = %v, want exactly one discard for the refused capture of %s", store.discards, canonical)
		assertEntryDiscarded(t, store, canonical)
		testassert.Require(t, len(store.identityRecords) == 0, "identity records = %+v, want none for the refused operation", store.identityRecords)
	}
	// A refused capture that reused this group's existing entry must leave the
	// entry and its preimage on disk with the prior proof restored, and
	// discard nothing.
	assertReusedCapturePreserved := func(t *testing.T, store *recordingCodeGroupStore, canonical, priorProof string) {
		t.Helper()
		testassert.Require(t, len(store.discards) == 0, "discards = %v, want none: a reused entry is never discarded", store.discards)
		assertEntryPresent(t, store, canonical)
		last := metaForCanonical(t, store.testCodeGroupStore, canonical).LastWrite
		testassert.Require(t, last != nil && last.Hash == snapshotIdentityHash(priorProof), "LastWrite after refusal = %+v, want the prior proof %q restored", last, priorProof)
	}

	type rebindingCase struct {
		name  string
		build func(t *testing.T, store *recordingCodeGroupStore) (run func() error, expect string, assertOutcome func(*testing.T, error))
	}
	cases := []rebindingCase{
		{
			name: "write_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				root, link, real1, real2 := rebindingLifecycleFixture(t)
				refusalFlip(store, 1, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": link, "content": "after"})
					return err
				}
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, real1, "before")
					assertFileContent(t, real2, "before")
					assertFreshCaptureAbandoned(t, store, real1)
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			// Reused-entry sibling of write_target: the seed write already
			// created the group's entry for real1, so the refused capture
			// reuses it — the entry must survive with the prior proof
			// restored instead of being discarded.
			name: "write_reused_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				root, link, real1, real2 := rebindingLifecycleFixture(t)
				_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": link, "content": "first"})
				testassert.NoError(t, err)
				refusalFlip(store, 2, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": link, "content": "after"})
					return err
				}
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, real1, "first")
					assertFileContent(t, real2, "before")
					assertReusedCapturePreserved(t, store, real1, "first")
					assertSnapshotIdentityContent(t, store.identityRecords, store.entryIDFor(real1), "first")
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			name: "edit_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				root, link, real1, real2 := rebindingLifecycleFixture(t)
				refusalFlip(store, 1, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execEdit(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": link, "old_string": "before", "new_string": "after"})
					return err
				}
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, real1, "before")
					assertFileContent(t, real2, "before")
					assertFreshCaptureAbandoned(t, store, real1)
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			name: "patch_update_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				root, link, real1, real2 := rebindingLifecycleFixture(t)
				refusalFlip(store, 1, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Update File: link.txt\n@@\n-before\n+after")})
					return err
				}
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, real1, "before")
					assertFileContent(t, real2, "before")
					assertFreshCaptureAbandoned(t, store, real1)
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			name: "patch_add_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				base := t.TempDir()
				approved := filepath.Join(base, "link.txt") // approved canonical: the absent leaf itself
				replacement := filepath.Join(base, "replacement.txt")
				testassert.NoError(t, os.WriteFile(replacement, []byte("secret"), 0o644))
				refusalFlip(store, 1, func() { testassert.NoError(t, os.Symlink(replacement, approved)) })
				run := func() error {
					_, err := execPatch(t, base, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Add File: link.txt\n+after")})
					return err
				}
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, replacement, "secret")
					assertPlantedSymlink(t, approved)
					assertFreshCaptureAbandoned(t, store, approved)
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			name: "patch_delete_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				root, link, real1, real2 := rebindingLifecycleFixture(t)
				refusalFlip(store, 1, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Delete File: link.txt")})
					return err
				}
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, real1, "before")
					assertFileContent(t, real2, "before")
					assertFreshCaptureAbandoned(t, store, real1)
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			name: "patch_move_dst_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				base := t.TempDir()
				src := filepath.Join(base, "src.txt")
				testassert.NoError(t, os.WriteFile(src, []byte("old content"), 0o644))
				approved := filepath.Join(base, "new.txt") // approved destination: the absent leaf itself
				replacement := filepath.Join(base, "replacement.txt")
				testassert.NoError(t, os.WriteFile(replacement, []byte("secret"), 0o644))
				refusalFlip(store, 1, func() { testassert.NoError(t, os.Symlink(replacement, approved)) })
				run := func() error {
					_, err := execPatch(t, base, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Update File: src.txt\n*** Move to: new.txt\n@@\n-old content\n+NEW")})
					return err
				}
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, src, "old content")
					assertFileContent(t, replacement, "secret")
					assertPlantedSymlink(t, approved)
					assertFreshCaptureAbandoned(t, store, approved)
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			name: "patch_move_src_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				root, link, real1, real2 := rebindingLifecycleFixture(t)
				dst := filepath.Join(root, "new.txt")
				refusalFlip(store, 2, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Update File: link.txt\n*** Move to: new.txt\n@@\n-before\n+after")})
					return err
				}
				assert := func(t *testing.T, err error) {
					assertFileContent(t, dst, "after")
					assertFileContent(t, real1, "before")
					assertFileContent(t, real2, "before")
					assertPartialCommitted(t, err, "M new.txt")
					testassert.Require(t, len(store.discards) == 1, "discards = %v, want exactly one discard: the refused source capture", store.discards)
					assertEntryPresent(t, store, dst)
					assertEntryDiscarded(t, store, real1)
					testassert.Require(t, len(store.identityRecords) == 1, "identity records = %+v, want only the completed destination record", store.identityRecords)
					assertSnapshotIdentityContent(t, store.identityRecords, store.entryIDFor(dst), "after")
				}
				return run, "approved canonical path changed", assert
			},
		},
		{
			// Reused-entry sibling of patch_move_src_target: the seed write
			// created the group's entry for real1, so the refused source
			// capture reuses it — the entry must survive with the prior
			// proof restored while the completed destination stays recorded.
			name: "patch_move_src_reused_target",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				root, link, real1, real2 := rebindingLifecycleFixture(t)
				dst := filepath.Join(root, "new.txt")
				_, err := execWrite(t, root, "", "", CapabilityOptions{}, store, map[string]any{"path": link, "content": "first"})
				testassert.NoError(t, err)
				refusalFlip(store, 3, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execPatch(t, root, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, "*** Update File: link.txt\n*** Move to: new.txt\n@@\n-first\n+after")})
					return err
				}
				assert := func(t *testing.T, err error) {
					assertFileContent(t, dst, "after")
					assertFileContent(t, real1, "first")
					assertFileContent(t, real2, "before")
					assertPartialCommitted(t, err, "M new.txt")
					assertReusedCapturePreserved(t, store, real1, "first")
					assertEntryPresent(t, store, dst)
					testassert.Require(t, len(store.identityRecords) == 2, "identity records = %+v, want the seed write plus the completed destination", store.identityRecords)
					assertSnapshotIdentityContent(t, store.identityRecords, store.entryIDFor(dst), "after")
				}
				return run, "approved canonical path changed", assert
			},
		},
	}

	// Workspace-root rebinding rows.
	for _, op := range []struct {
		name     string
		seed     func(t *testing.T, realA, realB string)
		input    func() string
		flipCall int
		assert   func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, err error)
	}{
		{
			name: "patch_update", seed: func(t *testing.T, realA, realB string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "file.txt"), []byte("hi"), 0o644))
				testassert.NoError(t, os.WriteFile(filepath.Join(realB, "file.txt"), []byte("secret"), 0o644))
			},
			input:    func() string { return "*** Update File: file.txt\n@@\n-hi\n+after" },
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertFileContent(t, filepath.Join(realA, "file.txt"), "hi")
				assertFileContent(t, filepath.Join(realB, "file.txt"), "secret")
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "file.txt"))
			},
		},
		{
			name: "patch_add", seed: func(*testing.T, string, string) {},
			input:    func() string { return "*** Add File: new.txt\n+after" },
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertPathMissing(t, filepath.Join(realA, "new.txt"))
				assertPathMissing(t, filepath.Join(realB, "new.txt"))
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "new.txt"))
			},
		},
		{
			name: "patch_delete", seed: func(t *testing.T, realA, realB string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "gone.txt"), []byte("doomed"), 0o644))
				testassert.NoError(t, os.WriteFile(filepath.Join(realB, "gone.txt"), []byte("secret"), 0o644))
			},
			input:    func() string { return "*** Delete File: gone.txt" },
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertFileContent(t, filepath.Join(realA, "gone.txt"), "doomed")
				assertFileContent(t, filepath.Join(realB, "gone.txt"), "secret")
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "gone.txt"))
			},
		},
		{
			name: "patch_move_dst", seed: func(t *testing.T, realA, _ string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "src.txt"), []byte("old content"), 0o644))
			},
			input:    func() string { return "*** Update File: src.txt\n*** Move to: new.txt\n@@\n-old content\n+NEW" },
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertFileContent(t, filepath.Join(realA, "src.txt"), "old content")
				assertPathMissing(t, filepath.Join(realA, "new.txt"))
				assertPathMissing(t, filepath.Join(realB, "new.txt"))
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "new.txt"))
			},
		},
		{
			name: "patch_move_src", seed: func(t *testing.T, realA, _ string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "old.txt"), []byte("before"), 0o644))
			},
			input:    func() string { return "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after" },
			flipCall: 2,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, err error) {
				assertFileContent(t, filepath.Join(realA, "new.txt"), "after")
				assertFileContent(t, filepath.Join(realA, "old.txt"), "before")
				assertPathMissing(t, filepath.Join(realB, "old.txt"))
				assertPartialCommitted(t, err, "M new.txt")
				testassert.Require(t, len(store.discards) == 1, "discards = %v, want exactly one discard: the refused source capture", store.discards)
				assertEntryPresent(t, store, filepath.Join(realA, "new.txt"))
				assertEntryDiscarded(t, store, filepath.Join(realA, "old.txt"))
				testassert.Require(t, len(store.identityRecords) == 1, "identity records = %+v, want only the completed destination record", store.identityRecords)
			},
		},
	} {
		cases = append(cases, rebindingCase{
			name: op.name + "_root",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				base := t.TempDir()
				realA := filepath.Join(base, "realA")
				realB := filepath.Join(base, "realB")
				for _, dir := range []string{realA, realB} {
					testassert.NoError(t, os.Mkdir(dir, 0o755))
				}
				op.seed(t, realA, realB)
				rootLink := filepath.Join(base, "root")
				testassert.NoError(t, os.Symlink(realA, rootLink))
				refusalFlip(store, op.flipCall, func() { rebindLink(t, rootLink, realB) })
				run := func() error {
					_, err := execPatch(t, rootLink, "", "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, op.input())})
					return err
				}
				assert := func(tt *testing.T, err error) { op.assert(tt, store, realA, realB, err) }
				return run, "approved canonical workspace root changed", assert
			},
		})
	}

	// Write/edit producers revalidated against a flipped Workspace root.
	for _, tool := range []struct {
		name string
		run  func(t *testing.T, store *recordingCodeGroupStore, rootLink string) error
	}{
		{name: "write", run: func(t *testing.T, store *recordingCodeGroupStore, rootLink string) error {
			_, err := execWrite(t, rootLink, "", "", CapabilityOptions{}, store, map[string]any{"path": "file.txt", "content": "after"})
			return err
		}},
		{name: "edit", run: func(t *testing.T, store *recordingCodeGroupStore, rootLink string) error {
			_, err := execEdit(t, rootLink, "", "", CapabilityOptions{}, store, map[string]any{"path": "file.txt", "old_string": "hi", "new_string": "after"})
			return err
		}},
	} {
		cases = append(cases, rebindingCase{
			name: tool.name + "_root",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				base := t.TempDir()
				realA := filepath.Join(base, "realA")
				realB := filepath.Join(base, "realB")
				for _, dir := range []string{realA, realB} {
					testassert.NoError(t, os.Mkdir(dir, 0o755))
				}
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "file.txt"), []byte("hi"), 0o644))
				testassert.NoError(t, os.WriteFile(filepath.Join(realB, "file.txt"), []byte("secret"), 0o644))
				rootLink := filepath.Join(base, "root")
				testassert.NoError(t, os.Symlink(realA, rootLink))
				refusalFlip(store, 1, func() { rebindLink(t, rootLink, realB) })
				run := func() error { return tool.run(t, store, rootLink) }
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, filepath.Join(realA, "file.txt"), "hi")
					assertFileContent(t, filepath.Join(realB, "file.txt"), "secret")
					assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "file.txt"))
				}
				return run, "approved canonical workspace root changed", assert
			},
		})
	}

	// Write-dir rebinding rows.
	for _, op := range []struct {
		name     string
		seed     func(t *testing.T, realA, realB string)
		input    func(realA string) string
		flipCall int
		assert   func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, err error)
	}{
		{
			name: "patch_update", seed: func(t *testing.T, realA, realB string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "file.txt"), []byte("hi"), 0o644))
				testassert.NoError(t, os.WriteFile(filepath.Join(realB, "file.txt"), []byte("secret"), 0o644))
			},
			input: func(realA string) string {
				return "*** Update File: " + filepath.Join(realA, "file.txt") + "\n@@\n-hi\n+after"
			},
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertFileContent(t, filepath.Join(realA, "file.txt"), "hi")
				assertFileContent(t, filepath.Join(realB, "file.txt"), "secret")
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "file.txt"))
			},
		},
		{
			name: "patch_add", seed: func(*testing.T, string, string) {},
			input:    func(realA string) string { return "*** Add File: " + filepath.Join(realA, "new.txt") + "\n+after" },
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertPathMissing(t, filepath.Join(realA, "new.txt"))
				assertPathMissing(t, filepath.Join(realB, "new.txt"))
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "new.txt"))
			},
		},
		{
			name: "patch_delete", seed: func(t *testing.T, realA, realB string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "gone.txt"), []byte("doomed"), 0o644))
				testassert.NoError(t, os.WriteFile(filepath.Join(realB, "gone.txt"), []byte("secret"), 0o644))
			},
			input:    func(realA string) string { return "*** Delete File: " + filepath.Join(realA, "gone.txt") },
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertFileContent(t, filepath.Join(realA, "gone.txt"), "doomed")
				assertFileContent(t, filepath.Join(realB, "gone.txt"), "secret")
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "gone.txt"))
			},
		},
		{
			name: "patch_move_dst", seed: func(t *testing.T, realA, _ string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "src.txt"), []byte("old content"), 0o644))
			},
			input: func(realA string) string {
				return "*** Update File: " + filepath.Join(realA, "src.txt") + "\n*** Move to: " + filepath.Join(realA, "new.txt") + "\n@@\n-old content\n+NEW"
			},
			flipCall: 1,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, _ error) {
				assertFileContent(t, filepath.Join(realA, "src.txt"), "old content")
				assertPathMissing(t, filepath.Join(realA, "new.txt"))
				assertPathMissing(t, filepath.Join(realB, "new.txt"))
				assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "new.txt"))
			},
		},
		{
			name: "patch_move_src", seed: func(t *testing.T, realA, realB string) {
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "old.txt"), []byte("before"), 0o644))
				testassert.NoError(t, os.WriteFile(filepath.Join(realB, "old.txt"), []byte("secret"), 0o644))
			},
			input: func(realA string) string {
				return "*** Update File: " + filepath.Join(realA, "old.txt") + "\n*** Move to: " + filepath.Join(realA, "new.txt") + "\n@@\n-before\n+after"
			},
			flipCall: 2,
			assert: func(t *testing.T, store *recordingCodeGroupStore, realA, realB string, err error) {
				assertFileContent(t, filepath.Join(realA, "new.txt"), "after")
				assertFileContent(t, filepath.Join(realA, "old.txt"), "before")
				assertFileContent(t, filepath.Join(realB, "old.txt"), "secret")
				assertPartialCommitted(t, err, "M "+filepath.Join(realA, "new.txt"))
				testassert.Require(t, len(store.discards) == 1, "discards = %v, want exactly one discard: the refused source capture", store.discards)
				assertEntryPresent(t, store, filepath.Join(realA, "new.txt"))
				assertEntryDiscarded(t, store, filepath.Join(realA, "old.txt"))
				testassert.Require(t, len(store.identityRecords) == 1, "identity records = %+v, want only the completed destination record", store.identityRecords)
			},
		},
	} {
		cases = append(cases, rebindingCase{
			name: op.name + "_write_dir",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				base := t.TempDir()
				realA := filepath.Join(base, "realA")
				realB := filepath.Join(base, "realB")
				for _, dir := range []string{realA, realB} {
					testassert.NoError(t, os.Mkdir(dir, 0o755))
				}
				wd := filepath.Join(base, "wd")
				testassert.NoError(t, os.Symlink(realA, wd))
				op.seed(t, realA, realB)
				wdCanonical, err := bindWriteDir(base, "apply_patch", wd)
				testassert.NoError(t, err)
				refusalFlip(store, op.flipCall, func() { rebindLink(t, wd, realB) })
				run := func() error {
					_, err := execPatch(t, base, "", wdCanonical, CapabilityOptions{WriteDir: wd}, store, map[string]any{"input": applyPatchInput(t, op.input(realA))})
					return err
				}
				assert := func(tt *testing.T, err error) { op.assert(tt, store, realA, realB, err) }
				return run, "approved canonical write_dir changed", assert
			},
		})
	}

	// Write/edit producers revalidated against a flipped write-dir witness.
	for _, tool := range []struct {
		name string
		run  func(t *testing.T, store *recordingCodeGroupStore, base, realA, wd string) error
	}{
		{name: "write", run: func(t *testing.T, store *recordingCodeGroupStore, base, realA, wd string) error {
			_, err := execWrite(t, base, "", "", CapabilityOptions{WriteDir: wd}, store, map[string]any{"path": filepath.Join(realA, "file.txt"), "content": "after"})
			return err
		}},
		{name: "edit", run: func(t *testing.T, store *recordingCodeGroupStore, base, realA, wd string) error {
			_, err := execEdit(t, base, "", "", CapabilityOptions{WriteDir: wd}, store, map[string]any{"path": filepath.Join(realA, "file.txt"), "old_string": "hi", "new_string": "after"})
			return err
		}},
	} {
		cases = append(cases, rebindingCase{
			name: tool.name + "_write_dir",
			build: func(t *testing.T, store *recordingCodeGroupStore) (func() error, string, func(*testing.T, error)) {
				base := t.TempDir()
				realA := filepath.Join(base, "realA")
				realB := filepath.Join(base, "realB")
				for _, dir := range []string{realA, realB} {
					testassert.NoError(t, os.Mkdir(dir, 0o755))
				}
				wd := filepath.Join(base, "wd")
				testassert.NoError(t, os.Symlink(realA, wd))
				testassert.NoError(t, os.WriteFile(filepath.Join(realA, "file.txt"), []byte("hi"), 0o644))
				testassert.NoError(t, os.WriteFile(filepath.Join(realB, "file.txt"), []byte("secret"), 0o644))
				refusalFlip(store, 1, func() { rebindLink(t, wd, realB) })
				run := func() error { return tool.run(t, store, base, realA, wd) }
				assert := func(t *testing.T, _ error) {
					assertFileContent(t, filepath.Join(realA, "file.txt"), "hi")
					assertFileContent(t, filepath.Join(realB, "file.txt"), "secret")
					assertFreshCaptureAbandoned(t, store, filepath.Join(realA, "file.txt"))
				}
				return run, "approved canonical write_dir changed", assert
			},
		})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newRecordingCodeGroupStore(t)
			run, expect, assertOutcome := tc.build(t, store)
			err := run()
			testassert.Require(t, err != nil && strings.Contains(err.Error(), expect), "Execute error = %v, want %q refusal", err, expect)
			assertOutcome(t, err)
		})
	}
}
