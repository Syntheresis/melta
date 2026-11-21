package patch

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestApplyPatchAllOrNothingValidationFailure(t *testing.T) {
	root := t.TempDir()
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Add File: new.txt\n+hello\n*** Update File: ghost.go\n@@\n-missing\n+replacement")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "does not exist")
	{
		_, err := os.Stat(filepath.Join(root, "new.txt"))
		testassert.Require(t, os.IsNotExist(err), "Add landed despite later op's validation failure: stat err = %v", err)
	}
	testassert.Require(t, len(ws.writes) == 0, "writes = %v, want none before the whole patch validates", ws.writes)
}

func TestApplyPatchAllOrNothingPartialApplyError(t *testing.T) {
	root := t.TempDir()
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Add File: new.txt\n+hi\n*** Delete File: ghost.txt")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "does not exist")
	{
		_, err := os.Stat(filepath.Join(root, "new.txt"))
		testassert.Require(t, os.IsNotExist(err), "Add landed despite later op's validation failure: stat err = %v", err)
	}
}

func TestApplyPatchMidWriteFailureReportsCommittedFiles(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "x.txt"), []byte("alpha\nBEFORE\nbeta"), 0o644))
	ws := newTestWorkspace(t, root)
	ws.writeErr = func(call int) error {
		if call == 2 {
			return errors.New("simulated disk full")
		}
		return nil
	}
	input := applyPatchInput(t, "*** Add File: new.txt\n+hi\n*** Update File: x.txt\n@@\n alpha\n-BEFORE\n+AFTER\n beta")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "A new.txt")
	testassert.Require(t, strings.Contains(outcome.Result.Content, "M x.txt") && strings.Contains(outcome.Result.Content, "simulated disk full"), "content = %q, want the completed write reported beside its record failure", outcome.Result.Content)
	assertFileContent(t, filepath.Join(root, "x.txt"), "alpha\nAFTER\nbeta")
}

func TestApplyPatchRefusesUpdateWhenContentChangesAfterValidation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "target.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("alpha\nBEFORE\nbeta"), 0o644))
	ws := newTestWorkspace(t, root)
	ws.onWrite = func(call int) {
		if call == 1 {
			testassert.NoError(t, os.WriteFile(path, []byte("alpha\nCHANGED\nbeta"), 0o644))
		}
	}
	input := applyPatchInput(t, "*** Add File: first.txt\n+hi\n*** Update File: target.txt\n@@\n alpha\n-BEFORE\n+AFTER\n beta")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "changed after validation")
	assertFileContent(t, path, "alpha\nCHANGED\nbeta")
}

func TestApplyPatchRefusesDeleteWhenContentChangesAfterValidation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "target.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("delete me"), 0o644))
	ws := newTestWorkspace(t, root)
	ws.onWrite = func(call int) {
		if call == 1 {
			testassert.NoError(t, os.WriteFile(path, []byte("changed"), 0o644))
		}
	}
	input := applyPatchInput(t, "*** Add File: first.txt\n+hi\n*** Delete File: target.txt")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "changed after validation")
	assertFileContent(t, path, "changed")
}

func TestApplyPatchRefusesMoveSourceBeforeDestinationWrite(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "old.txt")
	destination := filepath.Join(root, "new.txt")
	testassert.NoError(t, os.WriteFile(source, []byte("old content"), 0o644))
	ws := newTestWorkspace(t, root)
	ws.onWrite = func(call int) {
		if call == 1 {
			testassert.NoError(t, os.WriteFile(source, []byte("changed"), 0o644))
		}
	}
	input := applyPatchInput(t, "*** Add File: first.txt\n+hi\n*** Update File: old.txt\n*** Move to: new.txt")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "changed after validation")
	testassert.Require(t, !strings.Contains(outcome.Result.Content, "M new.txt"), "content = %q, want no destination claim before the source reread", outcome.Result.Content)
	assertFileContent(t, source, "changed")
	assertPathMissing(t, destination)
}

func TestApplyPatchRefusesMoveSourceWhenContentChangesAfterDestinationWrite(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "old.txt")
	destination := filepath.Join(root, "new.txt")
	testassert.NoError(t, os.WriteFile(source, []byte("old content"), 0o644))
	ws := newTestWorkspace(t, root)
	ws.onWrite = func(call int) {
		if call == 2 {
			testassert.NoError(t, os.WriteFile(source, []byte("changed"), 0o644))
		}
	}
	input := applyPatchInput(t, "*** Add File: first.txt\n+hi\n*** Update File: old.txt\n*** Move to: new.txt")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "changed after validation")
	testassert.Require(t, strings.Contains(outcome.Result.Content, "M new.txt"), "content = %q, want the completed destination reported", outcome.Result.Content)
	assertFileContent(t, source, "changed")
	assertFileContent(t, destination, "old content")
}

func TestApplyPatchShortWriteReportsUncertainTarget(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "old.txt")
	destination := filepath.Join(root, "new.txt")
	testassert.NoError(t, os.WriteFile(source, []byte("before"), 0o644))
	ws := newTestWorkspace(t, root)
	ws.shortWriteAt = 1
	input := applyPatchInput(t, "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "incomplete")
	testassert.Require(t, strings.Contains(outcome.Result.Content, "M new.txt (uncertain)"), "content = %q, want the uncertain destination reported as uncertain", outcome.Result.Content)
	testassert.Require(t, !strings.Contains(outcome.Result.Content, "M new.txt\n") && !strings.Contains(outcome.Result.Content, "D old.txt"), "content = %q, want no completed claim", outcome.Result.Content)
	assertFileContent(t, source, "before")
	assertPathMissing(t, destination)
}

func TestApplyPatchWritesEveryTouchedFileOnce(t *testing.T) {
	root := t.TempDir()
	ws := newTestWorkspace(t, root)
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "old.go"), []byte("a\nB\nc"), 0o644))
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "x.txt"), []byte("a\nB\nc"), 0o644))

	input := applyPatchInput(t, "*** Update File: x.txt\n@@\n a\n-B\n+B2\n c\n*** Update File: old.go\n*** Move to: new.go\n@@\n a\n-B\n+B2\n c")
	outcome := runPatch(t, ws, input)
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v, want success", outcome.Result)
	testassert.Equal(t, len(ws.writes), 2)
	testassert.Equal(t, len(ws.deletes), 1)
	assertFileContent(t, filepath.Join(root, "x.txt"), "a\nB2\nc")
	assertFileContent(t, filepath.Join(root, "new.go"), "a\nB2\nc")
	assertPathMissing(t, filepath.Join(root, "old.go"))
}

func TestApplyPatchAddFailsIfFileExists(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "exists.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("old"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Add File: exists.txt\n+new")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "already exists")
	assertFileContent(t, path, "old")
	testassert.Require(t, len(ws.writes) == 0, "writes = %v, want none", ws.writes)
}

func TestApplyPatchMoveFailsIfDestinationExists(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "old.go"), []byte("alpha\nBEFORE\nbeta"), 0o644))
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "new.go"), []byte("preexisting"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: old.go\n*** Move to: new.go\n@@\n alpha\n-BEFORE\n+AFTER\n beta")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "already exists")
	testassert.Require(t, len(ws.writes) == 0, "writes = %v, want none", ws.writes)
}

func TestApplyPatchCanonicalCollisionRejectedBeforeApply(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("hi"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: a.txt\n@@\n-hi\n+bye\n*** Update File: ./a.txt\n@@\n-hi\n+there")
	outcome := runPatch(t, ws, input)
	testassert.Require(t, outcome.Result.Status == model.ResultDenied && outcome.Result.Content == harness.PermissionDeniedContent, "outcome = %+v, want the fixed denial", outcome.Result)
	assertFileContent(t, filepath.Join(root, "a.txt"), "hi")
	testassert.Require(t, len(ws.writes) == 0, "writes = %v, want none", ws.writes)
}

func TestApplyPatchCanonicalCollisionSymlinkRejected(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real.txt")
	link := filepath.Join(root, "link.txt")
	testassert.NoError(t, os.WriteFile(real, []byte("content"), 0o644))
	testassert.NoError(t, os.Symlink(real, link))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: real.txt\n@@\n content\n+changed\n*** Delete File: link.txt")
	outcome := runPatch(t, ws, input)
	testassert.Require(t, outcome.Result.Status == model.ResultDenied && outcome.Result.Content == harness.PermissionDeniedContent, "outcome = %+v, want the fixed denial", outcome.Result)
	assertFileContent(t, real, "content")
	testassert.Require(t, len(ws.writes) == 0, "writes = %v, want none", ws.writes)
}

func TestApplyPatchFuzzyMatchPreservesActualTrailingWhitespaceContext(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.go")
	testassert.NoError(t, os.WriteFile(path, []byte("alpha   \nBEFORE\nbeta"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: a.go\n@@\n alpha\n-BEFORE\n+AFTER\n beta")
	testassert.Require(t, runPatch(t, ws, input).Result.Status == model.ResultSuccess, "patch failed")
	assertFileContent(t, path, "alpha   \nAFTER\nbeta")
}

func TestApplyPatchFuzzyMatchPreservesActualUnicodeContext(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.go")
	testassert.NoError(t, os.WriteFile(path, []byte("say \u201Chello\u201D\nBEFORE\nbeta"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: a.go\n@@\n say \"hello\"\n-BEFORE\n+AFTER\n beta")
	testassert.Require(t, runPatch(t, ws, input).Result.Status == model.ResultSuccess, "patch failed")
	assertFileContent(t, path, "say \u201Chello\u201D\nAFTER\nbeta")
}

func TestApplyPatchSyntheticEOFContextDoesNotCreateTrailingNewline(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("foo\nbar"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: a.txt\n@@\n foo\n-bar\n+baz\n ")
	outcome := runPatch(t, ws, input)
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v, want success", outcome.Result)
	assertFileContent(t, path, "foo\nbaz")
	rows := previewRowsFor(t, metadataFiles(t, outcome), "a.txt")
	testassert.Require(t, len(rows) == 3 && rows[0].Kind == kindContext && rows[0].Text == "foo" && rows[1].Kind == kindRemove && rows[1].Text == "bar" && rows[2].Kind == kindAdd && rows[2].Text == "baz", "preview rows = %#v, want context/remove/add only", rows)
}

func TestApplyPatchSyntheticEOFRemoveFails(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("foo"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: a.txt\n@@\n-\n+bar")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "Failed to find expected lines in a.txt:")
	assertFileContent(t, path, "foo")
}

func TestApplyPatchRealEOFContextPreservesTrailingNewline(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("foo\nbar\n"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: a.txt\n@@\n foo\n-bar\n+baz\n ")
	outcome := runPatch(t, ws, input)
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v, want success", outcome.Result)
	assertFileContent(t, path, "foo\nbaz\n")
	rows := previewRowsFor(t, metadataFiles(t, outcome), "a.txt")
	testassert.Require(t, len(rows) == 4 && rows[3].Kind == kindContext && rows[3].Text == "", "preview rows = %#v, want real EOF context retained", rows)
}

func TestApplyPatchRealEmptyRemoveStillWorks(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "a.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("foo\n\nbaz"), 0o644))
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Update File: a.txt\n@@\n foo\n-\n+bar\n baz")
	testassert.Require(t, runPatch(t, ws, input).Result.Status == model.ResultSuccess, "patch failed")
	assertFileContent(t, path, "foo\nbar\nbaz")
}

func TestApplyPatchDestinationAppearingBeforeCreateOnlyWriteIsRefused(t *testing.T) {
	root := t.TempDir()
	ws := newTestWorkspace(t, root)
	// The destination appears during the earlier target's write, after the
	// precondition checks and before the create-only destination write.
	ws.onWrite = func(call int) {
		if call == 1 {
			testassert.NoError(t, os.WriteFile(filepath.Join(root, "late.txt"), []byte("outside"), 0o600))
		}
	}
	input := applyPatchInput(t, "*** Add File: first.txt\n+one\n*** Add File: late.txt\n+two")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "appeared during apply")
	testassert.Require(t, strings.Contains(outcome.Result.Content, "A first.txt"), "content = %q, want the completed earlier target retained", outcome.Result.Content)
	assertFileContent(t, filepath.Join(root, "first.txt"), "one")
	assertFileContent(t, filepath.Join(root, "late.txt"), "outside")
}

// TestApplyPatchPreconditionsUseCurrentExistence proves the
// precondition rule: Prepare-time existence is not used; Execute reads
// each target's current state before the first mutation.
func TestApplyPatchPreconditionsUseCurrentExistence(t *testing.T) {
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
			ws := newTestWorkspace(t, root)
			plan := preparePatch(t, ws, applyPatchInput(t, tc.input))
			testassert.Require(t, plan.Immediate == nil, "prepare = %+v", plan.Immediate)
			if tc.exists {
				testassert.NoError(t, os.Remove(path))
			} else {
				testassert.NoError(t, os.WriteFile(path, []byte("before"), 0o644))
			}
			outcome := executePrepared(t, ws, plan)
			testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v, want success against current existence", outcome.Result)
			if tc.missingAfter {
				assertPathMissing(t, path)
			} else {
				assertFileContent(t, path, "after")
			}
		})
	}
	t.Run("move_destination_disappears", func(t *testing.T) {
		root := t.TempDir()
		source := filepath.Join(root, "old.txt")
		testassert.NoError(t, os.WriteFile(source, []byte("before"), 0o644))
		destination := filepath.Join(root, "new.txt")
		testassert.NoError(t, os.WriteFile(destination, []byte("outside"), 0o644))
		ws := newTestWorkspace(t, root)
		plan := preparePatch(t, ws, applyPatchInput(t, "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after"))
		testassert.Require(t, plan.Immediate == nil, "prepare = %+v", plan.Immediate)
		testassert.NoError(t, os.Remove(destination))
		outcome := executePrepared(t, ws, plan)
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v, want success against current existence", outcome.Result)
		assertPathMissing(t, source)
		assertFileContent(t, destination, "after")
	})
}

func previewRowsFor(t *testing.T, files []fileEntry, path string) []previewRow {
	t.Helper()
	testassert.Require(t, len(files) == 1, "preview files = %+v, want exactly one entry", files)
	for _, f := range files {
		if f.Path == path {
			testassert.Require(t, len(f.Preview.Hunks) == 1, "preview for %s = %+v, want one hunk", path, f.Preview)
			return f.Preview.Hunks[0].Rows
		}
	}
	t.Fatalf("no preview entry for %s in %+v", path, files)
	return nil
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
