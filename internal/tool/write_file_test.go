package tool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
)

func execWrite(t *testing.T, root string, store SnapshotStore, args map[string]any) (string, error) {
	t.Helper()
	path, content, err := WriteArgs(args)
	if err != nil {
		return "", err
	}
	result, _, err := WriteFile(root, store, path, content)
	return result, err
}

func TestWriteFileCreatesNewFileAndParentDirs(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nested", "new.txt")

	result, err := execWrite(t, root, newTempCodeGroupStore(t), map[string]any{
		"path":    path,
		"content": "created",
	})
	testassert.NoError(t, err)
	testassert.Equal(t, result, "Wrote "+path+".")
	assertFileContent(t, path, "created")
}

func TestWriteFileWritesContentExactlyIncludingTrailingNewline(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "exact.txt")

	{
		_, err := execWrite(t, root, newTempCodeGroupStore(t), map[string]any{
			"path":    path,
			"content": "no trailing newline",
		})
		testassert.NoError(t, err)
	}
	assertFileContent(t, path, "no trailing newline")

	{
		_, err := execWrite(t, root, newTempCodeGroupStore(t), map[string]any{
			"path":    path,
			"content": "with trailing newline\n",
		})
		testassert.NoError(t, err)
	}
	assertFileContent(t, path, "with trailing newline\n")
}

func TestWriteFileCapturesSnapshotBeforeWrite(t *testing.T) {
	path := readFileTestFile(t, "file.txt", "before")
	root := filepath.Dir(path)
	store := &recordingCodeGroupStore{testCodeGroupStore: newCodeGroupStore(t, filepath.Join(t.TempDir(), "group"))}

	result, err := execWrite(t, root, store, map[string]any{
		"path":    path,
		"content": "after",
	})
	testassert.NoError(t, err)
	testassert.Equal(t, result, "Wrote "+path+".")
	testassert.Equal(t, len(store.calls), 1)
	testassert.Require(t, store.calls[0].path == path, "snapshot call = %+v, want path=%q", store.calls[0], path)
	assertEntryPreimage(t, store, path, "before")
	assertFileContent(t, path, "after")
}

func TestWriteFileSnapshotUsesCanonicalTargetAndRequestedResult(t *testing.T) {
	root := t.TempDir()
	realDir := t.TempDir()
	target := filepath.Join(realDir, "file.txt")
	testassert.NoError(t, os.WriteFile(target, []byte("before"), 0o644))
	linkDir := filepath.Join(root, "linked")
	testassert.NoError(t, os.Symlink(realDir, linkDir))
	requested := filepath.Join(linkDir, "file.txt")
	store := &recordingCodeGroupStore{testCodeGroupStore: newCodeGroupStore(t, filepath.Join(t.TempDir(), "group"))}

	result, err := execWrite(t, root, store, map[string]any{
		"path":    requested,
		"content": "after",
	})

	testassert.NoError(t, err)
	testassert.Equal(t, result, "Wrote "+requested+".")
	testassert.Equal(t, len(store.calls), 1)
	testassert.Require(t, store.calls[0].path == requested && store.calls[0].canonical == target, "snapshot call = %+v, want original=%q canonical=%q", store.calls[0], requested, target)
	assertEntryPreimage(t, store, target, "before")
	assertFileContent(t, target, "after")
}
