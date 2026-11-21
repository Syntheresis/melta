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
		_, err := execWrite(t, root, "", store, map[string]any{"path": path, "content": "first"})
		testassert.NoError(t, err)
	}
	testassert.Require(t, soleSnapshotMeta(t, store).LastWrite != nil && soleSnapshotMeta(t, store).LastWrite.Hash == snapshotIdentityHash("first"), "seed LastWrite = %+v, want the first write's proof", soleSnapshotMeta(t, store).LastWrite)

	withSnapshotMetaPublishFailure(t, 1)
	counts := &mutationIOCount{}
	withMutationFileCounting(t, counts)

	_, err := execWrite(t, root, "", store, map[string]any{"path": path, "content": "second"})

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
		_, err := execWrite(t, root, "", store, map[string]any{"path": path, "content": "first"})
		testassert.NoError(t, err)
	}

	_, err := execEdit(t, root, "", store, map[string]any{"path": path, "old_string": "missing", "new_string": "second"})

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
		_, err := execWrite(t, root, "", store, map[string]any{"path": path, "content": "first"})
		testassert.NoError(t, err)
	}

	// The failing publication is the prior-proof restore: the pre-mutation
	// proof clear that precedes it succeeds, the restore is the second meta
	// publication of this call.
	withSnapshotMetaPublishFailure(t, 2)
	_, err := execEdit(t, root, "", store, map[string]any{"path": path, "old_string": "missing", "new_string": "second"})

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
	_, err := execWrite(t, root, "", store, map[string]any{"path": path, "content": "created"})

	testassert.Require(t, err != nil && strings.Contains(err.Error(), "record snapshot identity"), "write error = %v, want the failed final identity record", err)
	assertFileContent(t, path, "created")
	testassert.Require(t, soleSnapshotMeta(t, store).LastWrite == nil, "LastWrite after failed final record = %+v, want nil proof", soleSnapshotMeta(t, store).LastWrite)
	assertSnapshotEntryCount(t, groupDir, 1)
	result := revertTestGroup(t, groupDir)
	testassert.Require(t, len(result.Restored) == 0 && len(result.Skipped) == 1 && result.Skipped[0].Path == path, "restored = %v skipped = %+v, want the nil proof to skip %s", result.Restored, result.Skipped, path)
	assertFileContent(t, path, "created")
}

func TestMutationPreflightBeforeCapture(t *testing.T) {
	for _, tc := range []struct {
		name   string
		run    func(*testing.T, string, string, SnapshotStore) error
		delete bool
	}{
		{"write", func(t *testing.T, root, path string, store SnapshotStore) error {
			_, err := execWrite(t, root, "", store, map[string]any{"path": path, "content": "after"})
			return err
		}, false},
		{"edit", func(t *testing.T, root, path string, store SnapshotStore) error {
			_, err := execEdit(t, root, "", store, map[string]any{"path": path, "old_string": "before", "new_string": "after"})
			return err
		}, false},
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
				call, err = PrepareEditCall(root, "", store, args)
				testassert.NoError(t, err)
			} else {
				args, err = NormalizeWriteArgs(map[string]any{"path": path, "content": "after"})
				testassert.NoError(t, err)
				call, err = PrepareWriteCall(root, "", store, args)
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
func TestAppearedDestinationWithFailedAbandonReportsRefusalAndCleanupFailure(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "created.txt")
	groupDir := filepath.Join(t.TempDir(), "group")
	store := newCodeGroupStore(t, groupDir)
	binding, _, _, err := BindWorkspace(root, "created.txt")
	testassert.NoError(t, err)
	outcome, err := binding.Write(store, []byte("one"), true, nil)
	testassert.Require(t, err == nil && outcome == MutationCompleted, "seed write = (%v, %v), want completed", outcome, err)
	testassert.NoError(t, os.Remove(path))

	wrapped := &recordingCodeGroupStore{testCodeGroupStore: store, plantBefore: path, failAbandon: true}
	outcome, err = binding.Write(wrapped, []byte("two"), true, nil)

	testassert.Require(t, outcome == MutationUntouched, "outcome = %v, want untouched on the refused write", outcome)
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
	binding, _, _, err := BindWorkspace(root, "created.txt")
	testassert.NoError(t, err)
	outcome, err := binding.Write(store, []byte("one"), true, nil)
	testassert.Require(t, err == nil && outcome == MutationCompleted, "seed write = (%v, %v), want completed", outcome, err)
	testassert.NoError(t, os.Remove(path))

	wrapped := &recordingCodeGroupStore{testCodeGroupStore: store, plantBefore: path}
	outcome, err = binding.Write(wrapped, []byte("two"), true, nil)

	testassert.Require(t, outcome == MutationUntouched, "outcome = %v, want untouched on the refused write", outcome)
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "appeared during apply"), "err = %v, want the destination-appeared refusal", err)
	testassert.Require(t, !strings.Contains(err.Error(), "additionally failed"), "err = %v, want the completed abandonment reported without a cleanup failure", err)
	testassert.Require(t, !strings.Contains(err.Error(), "%!w(<nil>)"), "err = %q, want no %%!w(<nil>)", err.Error())
	testassert.Require(t, len(wrapped.discards) == 0, "discards = %v, want none: the reused entry survives", wrapped.discards)
	assertFileContent(t, path, string(appearedDestinationBytes))
	last := soleSnapshotMeta(t, store).LastWrite
	testassert.Require(t, last != nil && last.Hash == snapshotIdentityHash("one"), "LastWrite after refusal = %+v, want the existing entry's prior proof restored", last)
	assertSnapshotEntryCount(t, groupDir, 1)
}

func TestMutationToolsRefuseRebindingDuringSnapshotCapture(t *testing.T) {
	refusalFlip := func(store *recordingCodeGroupStore, at int, flip func()) {
		store.onSnapshot = func(call int) {
			if call == at {
				flip()
			}
		}
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
					_, err := execWrite(t, root, "", store, map[string]any{"path": link, "content": "after"})
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
				_, err := execWrite(t, root, "", store, map[string]any{"path": link, "content": "first"})
				testassert.NoError(t, err)
				refusalFlip(store, 2, func() { rebindLink(t, link, real2) })
				run := func() error {
					_, err := execWrite(t, root, "", store, map[string]any{"path": link, "content": "after"})
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
					_, err := execEdit(t, root, "", store, map[string]any{"path": link, "old_string": "before", "new_string": "after"})
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
	}

	// Workspace-root rebinding rows.
	// Write/edit producers revalidated against a flipped Workspace root.
	for _, tool := range []struct {
		name string
		run  func(t *testing.T, store *recordingCodeGroupStore, rootLink string) error
	}{
		{name: "write", run: func(t *testing.T, store *recordingCodeGroupStore, rootLink string) error {
			_, err := execWrite(t, rootLink, "", store, map[string]any{"path": "file.txt", "content": "after"})
			return err
		}},
		{name: "edit", run: func(t *testing.T, store *recordingCodeGroupStore, rootLink string) error {
			_, err := execEdit(t, rootLink, "", store, map[string]any{"path": "file.txt", "old_string": "hi", "new_string": "after"})
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
