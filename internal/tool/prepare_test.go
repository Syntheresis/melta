package tool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/testassert"
)

func TestSuggestFromBoundDirectoryReadsThroughNoFollowDescriptor(t *testing.T) {
	root := t.TempDir()
	realNames := filepath.Join(root, "real-dir")
	testassert.NoError(t, os.Mkdir(realNames, 0o755))
	testassert.NoError(t, os.WriteFile(filepath.Join(realNames, "config.json"), []byte("{}"), 0o644))
	otherNames := filepath.Join(root, "other-dir")
	testassert.NoError(t, os.Mkdir(otherNames, 0o755))
	testassert.NoError(t, os.WriteFile(filepath.Join(otherNames, "config.json"), []byte("{}"), 0o644))

	path := filepath.Join(realNames, "confg.json")
	testassert.NoError(t, os.WriteFile(path, []byte("{}"), 0o644))
	bound, err := bindCanonicalTarget(root, "read_file", path)
	testassert.Require(t, err == nil && bound.leafExists, "binding = %+v, %v", bound, err)
	testassert.NoError(t, os.Remove(path))
	got := suggestFromBoundDirectory(bound.canonical, bound.displayAbs)
	testassert.Require(t, strings.Contains(got, "Did you mean:") && strings.Contains(got, "config.json"), "suggestions = %q, want scored names from the bound directory", got)

	// Replace the bound directory with a symlink pointing elsewhere: the
	// listing must go through the bound no-follow descriptor and fail
	// closed, never follow the replacement and list the redirected names.
	testassert.NoError(t, os.RemoveAll(realNames))
	testassert.NoError(t, os.Symlink(otherNames, realNames))
	got = suggestFromBoundDirectory(bound.canonical, bound.displayAbs)
	testassert.Require(t, got == "read_file: file not found: "+filepath.Join(realNames, "confg.json"), "suggestions = %q, want plain not-found without redirected suggestions", got)
}

func TestReadArgsExactIntegers(t *testing.T) {
	cases := []struct {
		name      string
		args      map[string]any
		wantErr   string
		wantOff   int
		wantLimit int
	}{
		{
			name:      "plain json.Number integers",
			args:      map[string]any{"path": "a.txt", "offset": json.Number("2"), "limit": json.Number("3")},
			wantOff:   2,
			wantLimit: 3,
		},
		{
			name:      "integral spellings 1.0 and 1e0",
			args:      map[string]any{"path": "a.txt", "offset": json.Number("2.0"), "limit": json.Number("1e0")},
			wantOff:   2,
			wantLimit: 1,
		},
		{
			name:      "integers beyond float64 precision stay exact",
			args:      map[string]any{"path": "a.txt", "offset": json.Number("9007199254740993"), "limit": json.Number("4")},
			wantOff:   9007199254740993,
			wantLimit: 4,
		},
		{
			name:      "long integral spelling accepted without the length cap",
			args:      map[string]any{"path": "a.txt", "offset": json.Number("1." + strings.Repeat("0", 600))},
			wantOff:   1,
			wantLimit: 500,
		},
		{
			name:      "nonpositive values take retained clamps",
			args:      map[string]any{"path": "a.txt", "offset": json.Number("-10"), "limit": json.Number("0")},
			wantOff:   1,
			wantLimit: 500,
		},
		{
			name:      "absent keeps defaults",
			args:      map[string]any{"path": "a.txt"},
			wantOff:   1,
			wantLimit: 500,
		},
		{
			name:    "overflow rejected",
			args:    map[string]any{"path": "a.txt", "offset": json.Number("99999999999999999999")},
			wantErr: "is too large",
		},
		{
			name:    "empty path rejected",
			args:    map[string]any{"path": ""},
			wantErr: "path is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path, offset, limit, err := ReadArgs(tc.args, 500)
			if tc.wantErr != "" {
				testassert.Require(t, err != nil && strings.Contains(err.Error(), tc.wantErr), "ReadArgs error = %v, want containing %q", err, tc.wantErr)
				return
			}
			testassert.NoError(t, err)
			testassert.Equal(t, path, "a.txt")
			testassert.Equal(t, offset, tc.wantOff)
			testassert.Equal(t, limit, tc.wantLimit)
		})
	}
}

func TestIntegerArgumentsRejectInvalidValues(t *testing.T) {
	for _, value := range []any{"wrong", nil, true, json.Number("invalid"), json.Number("1.5")} {
		for _, key := range []string{"offset", "limit"} {
			_, _, _, err := ReadArgs(map[string]any{"path": "a", key: value}, 500)
			testassert.Require(t, err != nil, "read %s accepted %#v", key, value)
		}
		_, _, _, err := RunCommandArgs(map[string]any{"command": "true", "timeout": value}, 120)
		testassert.Require(t, err != nil, "timeout accepted %#v", value)
	}
}

func TestWriteEditAndProcessArgs(t *testing.T) {
	path, content, err := WriteArgs(map[string]any{"path": "a", "content": ""})
	testassert.Require(t, err == nil && path == "a" && content == "", "write args = %q %q %v", path, content, err)
	_, _, err = WriteArgs(map[string]any{"path": ""})
	testassert.Require(t, err != nil, "empty write path accepted")
	path, oldString, newString, replaceAll, err := EditArgs(map[string]any{"path": "a", "old_string": "x", "new_string": ""})
	testassert.Require(t, err == nil && path == "a" && oldString == "x" && newString == "" && !replaceAll, "edit args = %q %q %q %v %v", path, oldString, newString, replaceAll, err)
	_, _, _, _, err = EditArgs(map[string]any{"path": ""})
	testassert.Require(t, err != nil, "empty edit path accepted")
	for _, action := range []string{"read", "kill", "list"} {
		gotAction, id, err := ProcessArgs(map[string]any{"action": action, "id": "deadbeef"})
		testassert.Require(t, err == nil && gotAction == action && id == "deadbeef", "process args = %q %q %v", gotAction, id, err)
	}
	for _, args := range []map[string]any{
		{"action": "other"}, {"action": "read"}, {"action": "kill"}, {"action": "read", "id": "DEADBEEF"}, {"action": "read", "id": "abcdefg0"}, {"action": "read", "id": "abc"},
	} {
		_, _, err := ProcessArgs(args)
		testassert.Require(t, err != nil, "accepted process args = %+v", args)
	}
}

func TestReadFileClampsHugeWindowsToRemainingLines(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "lines.txt"), []byte("one\ntwo\nthree"), 0o644))
	result, err := execRead(t, root, config.ToolsConfig{ReadMaxLines: 500}, map[string]any{
		"path": "lines.txt", "limit": json.Number("9223372036854775807"),
	})
	testassert.NoError(t, err)
	want := strings.Join([]string{"1\tone", "2\ttwo", "3\tthree"}, "\n")
	testassert.Equal(t, result, want)

	// Same clamp from a mid-file offset: neither the window end nor the
	// footer's offset+limit-1 may overflow.
	result, err = execRead(t, root, config.ToolsConfig{ReadMaxLines: 500}, map[string]any{
		"path": "lines.txt", "offset": json.Number("2"), "limit": json.Number("9223372036854775807"),
	})
	testassert.NoError(t, err)
	want = strings.Join([]string{"2\ttwo", "3\tthree", "(Showing lines 2-3 of 3.)"}, "\n")
	testassert.Equal(t, result, want)
}

func TestScoreFileSuggestionsConsumesSuppliedEntries(t *testing.T) {
	entries := []os.DirEntry{
		fsDirEntry("main.go", false),
		fsDirEntry("sub", true),
		fsDirEntry("config.json", false),
		fsDirEntry("config.local.json", false),
		fsDirEntry("unrelated.md", false),
	}
	got := scoreFileSuggestions(entries, "confg.json")
	var names []string
	for _, s := range got {
		names = append(names, s.name)
	}
	// Prefix-ratio and small-distance matches only; directories skipped;
	// nearest distance first.
	testassert.Require(t, len(got) == 2 && names[0] == "config.json" && names[1] == "config.local.json", "suggestions = %v, want the two config matches nearest-first", names)
	{
		got := scoreFileSuggestions(entries, "main.go")
		testassert.Require(t, len(got) == 1 && got[0].name == "main.go", "suggestions = %+v, want only the exact stem match", got)
	}
}

func fsDirEntry(name string, isDir bool) os.DirEntry {
	var mode os.FileMode = 0o644
	if isDir {
		mode = os.ModeDir
	}
	return fsFakeEntry{name: name, mode: mode}
}

type fsFakeEntry struct {
	name string
	mode os.FileMode
}

func (e fsFakeEntry) Name() string               { return e.name }
func (e fsFakeEntry) IsDir() bool                { return e.mode.IsDir() }
func (e fsFakeEntry) Type() os.FileMode          { return e.mode.Type() }
func (e fsFakeEntry) Info() (os.FileInfo, error) { return nil, os.ErrNotExist }

func TestRepeatedReadsReturnContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("one\ntwo"), 0o644))
	for i := 0; i < 2; i++ {
		result, err := execRead(t, dir, config.ToolsConfig{ReadMaxLines: 500}, map[string]any{"path": path})
		testassert.Require(t, err == nil, "read %d error = %v", i+1, err)
		// Target reads never return a cached-read notice.
		testassert.Require(t, strings.Contains(result, "1\tone") && !strings.Contains(result, "File unchanged"), "read %d result = %q, want full content", i+1, result)
	}
}

func TestEditAndWriteFilePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "file.txt")
	testassert.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o755))
	testassert.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))
	store := newTempCodeGroupStore(t)

	result, err := execWrite(t, dir, store, map[string]any{"path": path, "content": "changed"})
	testassert.Require(t, err == nil && result == "Wrote "+path+".", "write = %q, %v", result, err)
	assertFileContent(t, path, "changed")
	result, err = execEdit(t, dir, store, map[string]any{"path": path, "old_string": "changed", "new_string": "final"})
	testassert.Require(t, err == nil && strings.Contains(result, "lines 1-"), "edit = %q, %v", result, err)
	assertFileContent(t, path, "final")
	_, err = execEdit(t, dir, store, map[string]any{"path": path, "old_string": "absent", "new_string": "x"})
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "not found"), "edit error = %v", err)
	assertFileContent(t, path, "final")
	nested := filepath.Join(dir, "created", "deep", "new.txt")
	_, err = execWrite(t, dir, store, map[string]any{"path": nested, "content": "made"})
	testassert.NoError(t, err)
	assertFileContent(t, nested, "made")
}

// TestWriteAndEditDiscardSnapshotClaimWhenTargetRepointed verifies that
// a target repointed during capture leaves both files untouched and discards
// the unmutated snapshot claim.
func TestWriteAndEditDiscardSnapshotClaimWhenTargetRepointed(t *testing.T) {
	dir := t.TempDir()
	real1 := filepath.Join(dir, "real1.txt")
	real2 := filepath.Join(dir, "real2.txt")
	for _, p := range []string{real1, real2} {
		testassert.NoError(t, os.WriteFile(p, []byte("before"), 0o644))
	}
	link := filepath.Join(dir, "link.txt")
	repointTo := func(target string) {
		t.Helper()
		testassert.NoError(t, os.Remove(link))
		testassert.NoError(t, os.Symlink(target, link))
	}
	testassert.NoError(t, os.Symlink(real1, link))

	// Write path: the snapshot hook flips the approved symlink after
	// capture, exactly in the snapshot-to-mutation window. The execution
	// must fail on the changed canonical target AND discard the snapshot
	// claim; neither file may be mutated.
	repointTo(real1)
	store := &recordingCodeGroupStore{testCodeGroupStore: newTempCodeGroupStore(t), onSnapshot: func(call int) {
		if call == 1 {
			repointTo(real2)
		}
	}}
	_, err := execWrite(t, dir, store, map[string]any{"path": link, "content": "after"})
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "approved canonical path changed"), "write error = %v", err)
	testassert.Require(t, len(store.discards) == 1 && store.discards[0] == store.entryIDFor(real1), "write discards = %v, want the unmutated snapshot claim for %s discarded once", store.discards, real1)
	assertEntryDiscarded(t, store, real1)
	assertSnapshotEntryCount(t, store.groupDir, 0)
	assertFileContent(t, real1, "before")
	assertFileContent(t, real2, "before")

	// Edit path: same window, same discard-before-mutation behavior.
	repointTo(real1)
	store2 := &recordingCodeGroupStore{testCodeGroupStore: newTempCodeGroupStore(t), onSnapshot: func(call int) {
		if call == 1 {
			repointTo(real2)
		}
	}}
	_, err = execEdit(t, dir, store2, map[string]any{"path": link, "old_string": "before", "new_string": "after"})
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "approved canonical path changed"), "edit error = %v", err)
	testassert.Require(t, len(store2.discards) == 1 && store2.discards[0] == store2.entryIDFor(real1), "edit discards = %v, want the unmutated snapshot claim for %s discarded once", store2.discards, real1)
	assertEntryDiscarded(t, store2, real1)
	assertSnapshotEntryCount(t, store2.groupDir, 0)
	assertFileContent(t, real1, "before")
	assertFileContent(t, real2, "before")
}
