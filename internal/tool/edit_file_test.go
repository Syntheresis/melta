package tool

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
)

func execEdit(t *testing.T, root string, store SnapshotStore, args map[string]any) (string, error) {
	t.Helper()
	path, oldString, newString, replaceAll, err := EditArgs(args)
	if err != nil {
		return "", err
	}
	result, _, err := EditFile(root, store, path, oldString, newString, replaceAll)
	return result, err
}

func TestEditFileReplacesUniqueStringAndReportsLineRange(t *testing.T) {
	path := readFileTestFile(t, "file.txt", "alpha\nbeta\ngamma")
	root := filepath.Dir(path)

	result, err := execEdit(t, root, newTempCodeGroupStore(t), map[string]any{
		"path":       path,
		"old_string": "beta",
		"new_string": "delta",
	})
	testassert.NoError(t, err)
	testassert.Equal(t, result, "Edited "+path+" (1 replacement, lines 2-2).")
	assertFileContent(t, path, "alpha\ndelta\ngamma")
}

func TestEditFileReplaceAllReportsAllLineRanges(t *testing.T) {
	path := readFileTestFile(t, "file.txt", "same\nother\nsame")
	root := filepath.Dir(path)

	result, err := execEdit(t, root, newTempCodeGroupStore(t), map[string]any{
		"path":        path,
		"old_string":  "same",
		"new_string":  "done",
		"replace_all": true,
	})
	testassert.NoError(t, err)
	testassert.Equal(t, result, "Edited "+path+" (2 replacements, lines 1-1, 3-3).")
	assertFileContent(t, path, "done\nother\ndone")
}

func TestEditFileRejectsAmbiguousMatchWithoutReplaceAll(t *testing.T) {
	path := readFileTestFile(t, "file.txt", "same\nsame")
	root := filepath.Dir(path)

	_, err := execEdit(t, root, newTempCodeGroupStore(t), map[string]any{
		"path":       path,
		"old_string": "same",
		"new_string": "done",
	})
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "old_string matches 2 locations"), "Execute error = %v, want ambiguous-match error", err)
	assertFileContent(t, path, "same\nsame")
}

func TestEditFileValidatesOldString(t *testing.T) {
	path := readFileTestFile(t, "file.txt", "content")
	root := filepath.Dir(path)

	tests := []struct {
		name      string
		oldString string
		newString string
		want      string
	}{
		{name: "empty", oldString: "", newString: "x", want: "old_string must not be empty"},
		{name: "identical", oldString: "content", newString: "content", want: "old_string and new_string are identical"},
		{name: "not found", oldString: "missing", newString: "x", want: "old_string not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := execEdit(t, root, newTempCodeGroupStore(t), map[string]any{
				"path":       path,
				"old_string": tt.oldString,
				"new_string": tt.newString,
			})
			testassert.Require(t, err != nil && strings.Contains(err.Error(), tt.want), "Execute error = %v, want containing %q", err, tt.want)
			assertFileContent(t, path, "content")
		})
	}
}

func TestEditFileMultilineReplacementLineRange(t *testing.T) {
	path := readFileTestFile(t, "file.txt", "start\nold\nend")
	root := filepath.Dir(path)

	result, err := execEdit(t, root, newTempCodeGroupStore(t), map[string]any{
		"path":       path,
		"old_string": "old",
		"new_string": "new\nline",
	})
	testassert.NoError(t, err)
	testassert.Equal(t, result, "Edited "+path+" (1 replacement, lines 2-3).")
	assertFileContent(t, path, "start\nnew\nline\nend")
}

func TestEditFileCapturesSnapshotBeforeEdit(t *testing.T) {
	path := readFileTestFile(t, "file.txt", "before")
	root := filepath.Dir(path)
	store := &recordingCodeGroupStore{testCodeGroupStore: newCodeGroupStore(t, filepath.Join(t.TempDir(), "group"))}

	result, err := execEdit(t, root, store, map[string]any{
		"path":       path,
		"old_string": "before",
		"new_string": "after",
	})
	testassert.NoError(t, err)
	testassert.Equal(t, result, "Edited "+path+" (1 replacement, lines 1-1).")
	testassert.Equal(t, len(store.calls), 1)
	testassert.Require(t, store.calls[0].path == path, "snapshot call = %+v, want path=%q", store.calls[0], path)
	assertEntryPreimage(t, store, path, "before")
	assertFileContent(t, path, "after")
}

func TestEditFileSnapshotUsesCanonicalTargetAndRequestedResult(t *testing.T) {
	root := t.TempDir()
	realDir := t.TempDir()
	target := filepath.Join(realDir, "file.txt")
	testassert.NoError(t, os.WriteFile(target, []byte("before"), 0o644))
	linkDir := filepath.Join(root, "linked")
	testassert.NoError(t, os.Symlink(realDir, linkDir))
	requested := filepath.Join(linkDir, "file.txt")
	store := &recordingCodeGroupStore{testCodeGroupStore: newCodeGroupStore(t, filepath.Join(t.TempDir(), "group"))}

	result, err := execEdit(t, root, store, map[string]any{
		"path":       requested,
		"old_string": "before",
		"new_string": "after",
	})

	testassert.NoError(t, err)
	testassert.Equal(t, result, "Edited "+requested+" (1 replacement, lines 1-1).")
	testassert.Equal(t, len(store.calls), 1)
	testassert.Require(t, store.calls[0].path == requested && store.calls[0].canonical == target, "snapshot call = %+v, want original=%q canonical=%q", store.calls[0], requested, target)
	assertEntryPreimage(t, store, target, "before")
	assertFileContent(t, target, "after")
}

func TestApplyEditPureHelper(t *testing.T) {
	edit, err := ApplyEdit("a\nb\na", "a", "z", true, "file.txt")
	testassert.NoError(t, err)
	testassert.Require(t, edit.UpdatedContent == "z\nb\nz" && edit.Summary == "Edited file.txt (2 replacements, lines 1-1, 3-3)." && edit.LineRanges == "1-1, 3-3" && edit.Count == 2, "ApplyEdit result = %+v, want all replacements and line ranges", edit)
}
