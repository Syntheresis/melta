package patch

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestApplyPatchMetadataEmitsEditPreviewFiles(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "x.txt"), []byte("alpha\nBEFORE\nbeta"), 0o644))
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("bye"), 0o644))
	ws := newTestWorkspace(t, root)

	input := applyPatchInput(t, "*** Add File: new.txt\n+hi\n*** Update File: x.txt\n@@\n alpha\n-BEFORE\n+AFTER\n beta\n*** Delete File: a.txt")
	outcome := runPatch(t, ws, input)
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v, want success", outcome.Result)

	files := metadataFiles(t, outcome)
	testassert.Equal(t, len(files), 3)
	testassert.Require(t, files[0].Op == "A" && files[0].Path == "new.txt", "files[0] = %+v, want A new.txt", files[0])
	testassert.Require(t, len(files[0].Preview.Hunks) == 1 && len(files[0].Preview.Hunks[0].Rows) == 1, "files[0].Preview = %+v, want one hunk with one add row", files[0].Preview)
	testassert.Require(t, files[1].Op == "M" && files[1].Path == "x.txt", "files[1] = %+v, want M x.txt", files[1])
	testassert.Equal(t, len(files[1].Preview.Hunks), 1)
	wantRows := []previewRow{
		{Kind: kindContext, OldLine: 1, NewLine: 1, Text: "alpha"},
		{Kind: kindRemove, OldLine: 2, Text: "BEFORE"},
		{Kind: kindAdd, NewLine: 2, Text: "AFTER"},
		{Kind: kindContext, OldLine: 3, NewLine: 3, Text: "beta"},
	}
	testassert.Equal(t, len(files[1].Preview.Hunks[0].Rows), len(wantRows))
	for i, row := range files[1].Preview.Hunks[0].Rows {
		testassert.Require(t, row == wantRows[i], "files[1] hunk row[%d] = %+v, want %+v", i, row, wantRows[i])
	}
	testassert.Require(t, files[2].Op == "D" && files[2].Path == "a.txt", "files[2] = %+v, want D a.txt", files[2])
}

func TestApplyPatchMetadataMoveTwoEntries(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "old.go"), []byte("a\nB\nc"), 0o644))
	ws := newTestWorkspace(t, root)

	input := applyPatchInput(t, "*** Update File: old.go\n*** Move to: new.go\n@@\n a\n-B\n+B2\n c")
	outcome := runPatch(t, ws, input)
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "outcome = %+v, want success", outcome.Result)

	files := metadataFiles(t, outcome)
	testassert.Equal(t, len(files), 2)
	testassert.Require(t, files[0].Op == "M" && files[0].Path == "new.go", "files[0] = %+v, want M new.go", files[0])
	testassert.Require(t, files[1].Op == "D" && files[1].Path == "old.go", "files[1] = %+v, want D old.go", files[1])
}

func TestApplyPatchFailureEmitsNoPreviewMetadata(t *testing.T) {
	root := t.TempDir()
	ws := newTestWorkspace(t, root)
	input := applyPatchInput(t, "*** Delete File: ghost.txt")
	outcome := runPatch(t, ws, input)
	outcomeFailed(t, outcome, "")
	testassert.Require(t, len(outcome.Metadata) == 0, "metadata after failure = %s, want none", outcome.Metadata)
}
