package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/testassert"
)

func TestPrepareReadBindsParentDirectoryOnlyWhenLeafMissing(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "real.txt")
	testassert.NoError(t, os.WriteFile(existing, []byte("one\ntwo\nthree"), 0o644))

	// Existing leaf: one file target, no parent-directory binding, so an
	// ordinary existing-file read requires no parent permission.
	args, err := NormalizeReadArgs(map[string]any{"path": "real.txt"}, 500)
	testassert.NoError(t, err)
	prepared, err := PrepareReadCall(root, "", config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	testassert.Require(t, len(prepared.Targets) == 1, "targets = %+v, want exactly the file target", prepared.Targets)
	testassert.Equal(t, prepared.Targets[0].CanonicalPath, existing)

	// Missing leaf: the parent directory is bound as a second declared
	// file.read target for the authorized suggestion listing.
	args, err = NormalizeReadArgs(map[string]any{"path": "missing.txt"}, 500)
	testassert.NoError(t, err)
	prepared, err = PrepareReadCall(root, "", config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	testassert.Require(t, len(prepared.Targets) == 2, "targets = %+v, want file plus parent directory", prepared.Targets)
	testassert.Require(t, prepared.Targets[1].CanonicalPath == root, "parent target = %+v, want directory binding on %q", prepared.Targets[1], root)
}

func TestPrepareReadDisappearedLeafReturnsPlainNotFound(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "gone.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("content"), 0o644))
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "config.json"), []byte("{}"), 0o644))

	args, err := NormalizeReadArgs(map[string]any{"path": "gone.txt"}, 500)
	testassert.NoError(t, err)
	prepared, err := PrepareReadCall(root, "", config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	testassert.Require(t, len(prepared.Targets) == 1, "targets = %+v, want the existing leaf only", prepared.Targets)
	// The leaf existed during preparation but disappears before opening:
	// plain not-found, no undeclared fallback I/O, no suggestions.
	testassert.NoError(t, os.Remove(path))
	result, err := prepared.Execute(context.Background(), nil)
	testassert.NoError(t, err)
	testassert.Require(t, result == "read_file: file not found: "+filepath.Join(root, "gone.txt"), "Execute result = %q, want plain not-found without suggestions", result)
}

func TestSuggestFromBoundDirectoryReadsThroughNoFollowDescriptor(t *testing.T) {
	root := t.TempDir()
	realNames := filepath.Join(root, "real-dir")
	testassert.NoError(t, os.Mkdir(realNames, 0o755))
	testassert.NoError(t, os.WriteFile(filepath.Join(realNames, "config.json"), []byte("{}"), 0o644))
	otherNames := filepath.Join(root, "other-dir")
	testassert.NoError(t, os.Mkdir(otherNames, 0o755))
	testassert.NoError(t, os.WriteFile(filepath.Join(otherNames, "config.json"), []byte("{}"), 0o644))

	parent := readTarget{displayPath: realNames, canonicalPath: realNames}
	got := suggestFromBoundDirectory(parent, filepath.Join(realNames, "confg.json"))
	testassert.Require(t, strings.Contains(got, "Did you mean:") && strings.Contains(got, "config.json"), "suggestions = %q, want scored names from the bound directory", got)

	// Replace the bound directory with a symlink pointing elsewhere: the
	// listing must go through the bound no-follow descriptor and fail
	// closed, never follow the replacement and list the redirected names.
	testassert.NoError(t, os.RemoveAll(realNames))
	testassert.NoError(t, os.Symlink(otherNames, realNames))
	got = suggestFromBoundDirectory(parent, filepath.Join(realNames, "confg.json"))
	testassert.Require(t, got == "read_file: file not found: "+filepath.Join(realNames, "confg.json"), "suggestions = %q, want plain not-found without redirected suggestions", got)
}

// TestConsumedIntAtUse pins the point-of-use consumption of the canonical
// integer representation: validated lexemes parse to their int, and every
// non-canonical value class — absent, non-json.Number, non-integer lexeme,
// platform-int overflow — reads as zero without erroring, because
// normalization owns every rejection.
func TestConsumedIntAtUse(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
		key  string
		want int
	}{
		{"canonical integer lexeme", map[string]any{"offset": json.Number("3")}, "offset", 3},
		{"negative lexeme", map[string]any{"offset": json.Number("-2")}, "offset", -2},
		{"absent key", map[string]any{}, "offset", 0},
		{"non-json.Number value", map[string]any{"offset": float64(3)}, "offset", 0},
		{"null value", map[string]any{"offset": nil}, "offset", 0},
		{"non-integer lexeme", map[string]any{"offset": json.Number("x")}, "offset", 0},
		{"platform-int overflow", map[string]any{"offset": json.Number("99999999999999999999")}, "offset", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testassert.Equal(t, consumedIntAtUse(tc.args, tc.key), tc.want)
		})
	}
}

func TestNormalizeReadArgsStrictIntegers(t *testing.T) {
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
			name:    "float64 rejected in the strict target path",
			args:    map[string]any{"path": "a.txt", "offset": float64(3)},
			wantErr: "must be an integer",
		},
		{
			name:      "absent keeps defaults",
			args:      map[string]any{"path": "a.txt"},
			wantOff:   1,
			wantLimit: 500,
		},
		{
			name:    "present null offset rejected",
			args:    map[string]any{"path": "a.txt", "offset": nil},
			wantErr: "must be an integer",
		},
		{
			name:    "present null limit rejected",
			args:    map[string]any{"path": "a.txt", "limit": nil},
			wantErr: "must be an integer",
		},
		{
			name:    "fraction rejected",
			args:    map[string]any{"path": "a.txt", "offset": json.Number("1.5")},
			wantErr: "must be a whole number",
		},
		{
			// A fraction so deep it rounds to 1 under 256-bit float
			// arithmetic must still be rejected by the exact lexeme parse.
			name:    "deep fraction below float rounding precision rejected",
			args:    map[string]any{"path": "a.txt", "offset": json.Number("1." + strings.Repeat("0", 80) + "1")},
			wantErr: "must be a whole number",
		},
		{
			name:    "float fraction rejected",
			args:    map[string]any{"path": "a.txt", "limit": 2.5},
			wantErr: "must be an integer",
		},
		{
			name:    "wrong type rejected",
			args:    map[string]any{"path": "a.txt", "offset": "3"},
			wantErr: "must be an integer",
		},
		{
			name:    "overflow rejected",
			args:    map[string]any{"path": "a.txt", "offset": json.Number("99999999999999999999")},
			wantErr: "is too large",
		},
		{
			name:    "missing path rejected",
			args:    map[string]any{"offset": float64(1)},
			wantErr: "path is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args, err := NormalizeReadArgs(tc.args, 500)
			if tc.wantErr != "" {
				testassert.Require(t, err != nil && strings.Contains(err.Error(), tc.wantErr), "NormalizeReadArgs error = %v, want containing %q", err, tc.wantErr)
				return
			}
			testassert.NoError(t, err)
			// Consumed integers normalize to their canonical-integer
			// json.Number lexemes — compared against the literal expected
			// lexeme, not through the producer: 2.0 becomes "2", 1e0
			// becomes "1", and a 601-digit integral spelling becomes "1".
			offsetNumber, ok := args["offset"].(json.Number)
			if !ok || string(offsetNumber) != strconv.Itoa(tc.wantOff) {
				t.Fatalf("normalized offset = %v (%T), want canonical integer lexeme %q", args["offset"], args["offset"], strconv.Itoa(tc.wantOff))
			}
			limitNumber, ok := args["limit"].(json.Number)
			if !ok || string(limitNumber) != strconv.Itoa(tc.wantLimit) {
				t.Fatalf("normalized limit = %v (%T), want canonical integer lexeme %q", args["limit"], args["limit"], strconv.Itoa(tc.wantLimit))
			}
		})
	}
}

func TestNormalizeWriteEditArgsStrictConsumedFields(t *testing.T) {
	{
		_, err := NormalizeWriteArgs(map[string]any{"path": "a.txt", "content": 123})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "content is required"), "NormalizeWriteArgs error = %v, want content required error", err)
	}
	{
		_, err := NormalizeWriteArgs(map[string]any{"path": "a.txt"})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "content is required"), "NormalizeWriteArgs error = %v, want content required error", err)
	}
	{
		_, err := NormalizeEditArgs(map[string]any{"path": "a.txt", "old_string": 1, "new_string": "x"})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "old_string is required"), "NormalizeEditArgs error = %v, want old_string required error", err)
	}
	{
		_, err := NormalizeEditArgs(map[string]any{"path": "a.txt", "old_string": "x", "new_string": true})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "new_string is required"), "NormalizeEditArgs error = %v, want new_string required error", err)
	}
	{
		_, err := NormalizeEditArgs(map[string]any{"path": "a.txt", "old_string": "x", "new_string": "y", "replace_all": "yes"})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "replace_all must be a boolean"), "NormalizeEditArgs error = %v, want replace_all type error", err)
	}
	{
		_, err := NormalizeEditArgs(map[string]any{"path": "a.txt", "old_string": "x", "new_string": "y", "replace_all": nil})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "replace_all must be a boolean"), "NormalizeEditArgs error = %v, want present-null replace_all rejected", err)
	}
	args, err := NormalizeEditArgs(map[string]any{"path": "a.txt", "old_string": "x", "new_string": ""})
	testassert.NoError(t, err)
	testassert.Equal(t, args["new_string"], "")
}

func TestNormalizeArgsStripsPrivateFieldsAndRetainsUnrelated(t *testing.T) {
	// Locally defined private-field literals stand in for model-supplied
	// private `_melta_` fields of every retired receipt shape.
	const (
		privateCanonicalPath = "_melta_canonical_path"
		privatePatchReceipt  = "_melta_apply_patch_receipt"
	)
	args, err := NormalizeWriteArgs(map[string]any{
		"path":                        "a.txt",
		"content":                     "x",
		privateCanonicalPath:          "/forged/canonical",
		privatePatchReceipt:           map[string]any{"forged": true},
		"_melta_apply_patch_receipt2": "forged",
		"unrelated_metadata":          "kept",
		"_lightcode_receipt":          "ordinary",
	})
	testassert.NoError(t, err)
	{
		_, forged := args[privateCanonicalPath]
		testassert.Require(t, !forged, "normalized args retained the private canonical path field")
	}
	{
		_, forged := args[privatePatchReceipt]
		testassert.Require(t, !forged, "normalized args retained the private apply_patch receipt field")
	}
	_, forged := args["_melta_apply_patch_receipt2"]
	testassert.Require(t, !forged, "normalized args retained a second private receipt")
	testassert.Equal(t, args["_lightcode_receipt"], any("ordinary"))
	testassert.Require(t, args["unrelated_metadata"] == "kept" && args["path"] == "a.txt" && args["content"] == "x", "normalized args = %+v, want unrelated fields retained; normalized args = %+v, want schema fields preserved", args, args)
}

func TestReadFileClampsHugeWindowsToRemainingLines(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "lines.txt"), []byte("one\ntwo\nthree"), 0o644))
	rootCanonical, err := bindWorkspaceRoot(root)
	testassert.NoError(t, err)

	// A MaxInt64 limit must not overflow end/last-line arithmetic: the
	// window clamps to the remaining lines. The strict normalizer
	// genuinely consumes the MaxInt64 lexeme.
	args, err := NormalizeReadArgs(map[string]any{
		"path":  "lines.txt",
		"limit": json.Number("9223372036854775807"),
	}, 500)
	testassert.NoError(t, err)
	prepared, err := PrepareReadCall(root, rootCanonical, config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	result, err := prepared.Execute(context.Background(), nil)
	testassert.NoError(t, err)
	want := strings.Join([]string{"1\tone", "2\ttwo", "3\tthree"}, "\n")
	testassert.Equal(t, result, want)

	// Same clamp from a mid-file offset: neither the window end nor the
	// footer's offset+limit-1 may overflow.
	args, err = NormalizeReadArgs(map[string]any{
		"path":   "lines.txt",
		"offset": json.Number("2"),
		"limit":  json.Number("9223372036854775807"),
	}, 500)
	testassert.NoError(t, err)
	prepared, err = PrepareReadCall(root, rootCanonical, config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	result, err = prepared.Execute(context.Background(), nil)
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

// workspaceRepointFixture builds the two real trees and the Workspace root
// symlink the repointed-root cases share. The caller keeps each operation's
// own normalization, preparation and execution.
func workspaceRepointFixture(t *testing.T, realAContent string) (realA, realB, root string) {
	t.Helper()
	base := t.TempDir()
	realA = filepath.Join(base, "realA")
	realB = filepath.Join(base, "realB")
	for _, dir := range []string{realA, realB} {
		testassert.NoError(t, os.Mkdir(dir, 0o755))
	}
	testassert.NoError(t, os.WriteFile(filepath.Join(realA, "f.txt"), []byte(realAContent), 0o644))
	root = filepath.Join(base, "w")
	testassert.NoError(t, os.Symlink(realA, root))
	return realA, realB, root
}

// repointWorkspaceRoot points the Workspace root symlink at another tree.
func repointWorkspaceRoot(t *testing.T, root, target string) {
	t.Helper()
	testassert.NoError(t, os.Remove(root))
	testassert.NoError(t, os.Symlink(target, root))
}

func TestPrepareReadRevalidatesRepointedWorkspaceRoot(t *testing.T) {
	realA, realB, root := workspaceRepointFixture(t, "A-content")
	testassert.NoError(t, os.WriteFile(filepath.Join(realB, "f.txt"), []byte("B-content"), 0o644))

	// The target is addressed directly under the real directory, so its
	// canonical path is independent of the root symlink; only the bound
	// Workspace root canonical can detect the repoint.
	args, err := NormalizeReadArgs(map[string]any{"path": filepath.Join(realA, "f.txt")}, 500)
	testassert.NoError(t, err)
	prepared, err := PrepareReadCall(root, "", config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	// Baseline: with the binding intact the read succeeds.
	{
		result, err := prepared.Execute(context.Background(), nil)
		testassert.Require(t, err == nil && strings.Contains(result, "A-content"), "Execute = %q, %v; want A-content", result, err)
	}

	// Repoint the Workspace root after preparation: the closure must refuse
	// the changed root binding instead of reading a substituted tree.
	args, err = NormalizeReadArgs(map[string]any{"path": filepath.Join(realA, "f.txt")}, 500)
	testassert.NoError(t, err)
	prepared, err = PrepareReadCall(root, "", config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	repointWorkspaceRoot(t, root, realB)
	{
		_, err := prepared.Execute(context.Background(), nil)
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "workspace root changed"), "Execute error = %v, want workspace root change refusal", err)
	}
	assertFileContent(t, filepath.Join(realA, "f.txt"), "A-content")
	assertFileContent(t, filepath.Join(realB, "f.txt"), "B-content")
}

func TestPrepareWriteRevalidatesRepointedWorkspaceRoot(t *testing.T) {
	realA, realB, root := workspaceRepointFixture(t, "x")
	store := newTempCodeGroupStore(t)

	args, err := NormalizeWriteArgs(map[string]any{"path": filepath.Join(realA, "f.txt"), "content": "y"})
	testassert.NoError(t, err)
	prepared, err := PrepareWriteCall(root, "", store, args)
	testassert.NoError(t, err)
	repointWorkspaceRoot(t, root, realB)
	{
		_, err := prepared.Execute(context.Background(), nil)
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "workspace root changed"), "Execute error = %v, want workspace root change refusal", err)
	}
	// No mutation happened under either tree.
	assertFileContent(t, filepath.Join(realA, "f.txt"), "x")
	{
		_, err := os.Stat(filepath.Join(realB, "f.txt"))
		testassert.Require(t, os.IsNotExist(err), "realB f.txt stat err = %v, want absent (no substituted mutation)", err)
	}
}

func TestPrepareTargetNilTrackerRepeatedReadsReturnContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	testassert.NoError(t, os.WriteFile(path, []byte("one\ntwo"), 0o644))
	args, err := NormalizeReadArgs(map[string]any{"path": path}, 500)
	testassert.NoError(t, err)
	prepared, err := PrepareReadCall(dir, "", config.ToolsConfig{ReadMaxLines: 500}, args)
	testassert.NoError(t, err)
	for i := 0; i < 2; i++ {
		result, err := prepared.Execute(context.Background(), nil)
		testassert.Require(t, err == nil, "read %d error = %v", i+1, err)
		// Target reads never return a cached-read notice.
		testassert.Require(t, strings.Contains(result, "1\tone") && !strings.Contains(result, "File unchanged"), "read %d result = %q, want full content", i+1, result)
	}
}

func TestPrepareReadTargetsExpressParentAllowDeny(t *testing.T) {
	dir := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte("{}"), 0o644))

	t.Run("parent denied denies missing-leaf suggestions", func(t *testing.T) {
		// Missing leaf: the parent directory is a declared target, so a
		// parent denial is expressible before any enumeration.
		args, err := NormalizeReadArgs(map[string]any{"path": "confg.json"}, 500)
		testassert.NoError(t, err)
		prepared, err := PrepareReadCall(dir, "", config.ToolsConfig{ReadMaxLines: 500}, args)
		testassert.NoError(t, err)
		testassert.Require(t, len(prepared.Targets) == 2, "targets = %+v, want file plus parent directory", prepared.Targets)
		// Every prepared target is checked; a parent denial must refuse
		// the call before execution, so no enumeration runs.
		allowed := true
		for _, target := range prepared.Targets {
			if target.CanonicalPath == dir {
				allowed = false
				break
			}
		}
		testassert.Require(t, !allowed, "parent denial was not expressible through the prepared targets")
		{
			_, err := prepared.Execute(context.Background(), nil)
			testassert.NoError(t, err)
		}
	})

	t.Run("parent denied allows existing-file read", func(t *testing.T) {
		// Existing leaf: only the file target is declared, so a parent
		// denial does not block an allowed existing-file read.
		args, err := NormalizeReadArgs(map[string]any{"path": "config.json"}, 500)
		testassert.NoError(t, err)
		prepared, err := PrepareReadCall(dir, "", config.ToolsConfig{ReadMaxLines: 500}, args)
		testassert.NoError(t, err)
		testassert.Require(t, len(prepared.Targets) == 1 && prepared.Targets[0].CanonicalPath == filepath.Join(dir, "config.json"), "targets = %+v, want only the file target (parent not required)", prepared.Targets)
		result, err := prepared.Execute(context.Background(), nil)
		testassert.Require(t, err == nil && strings.Contains(result, "{}"), "Execute = %q, %v; want file content (parent permission not required)", result, err)
	})
}

func TestPreparedEditAndWritePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "file.txt")
	testassert.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o755))
	testassert.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))
	store := newTempCodeGroupStore(t)

	// Prepared write through the exported entry (no tracker, nil-tracker
	// target path) overwrites content.
	args, err := NormalizeWriteArgs(map[string]any{"path": path, "content": "changed"})
	testassert.NoError(t, err)
	prepared, err := PrepareWriteCall(dir, "", store, args)
	testassert.NoError(t, err)
	testassert.Require(t, len(prepared.Targets) == 1 && prepared.Targets[0].CanonicalPath == path, "targets = %+v, want the single canonical write target", prepared.Targets)
	{
		result, err := prepared.Execute(context.Background(), nil)
		testassert.Require(t, err == nil && result == "Wrote "+path+".", "Execute = %q, %v; want write result", result, err)
	}
	assertFileContent(t, path, "changed")

	// Prepared edit through the exported entry applies the replacement.
	editArgs, err := NormalizeEditArgs(map[string]any{"path": path, "old_string": "changed", "new_string": "final"})
	testassert.NoError(t, err)
	editPrepared, err := PrepareEditCall(dir, "", store, editArgs)
	testassert.NoError(t, err)
	testassert.Require(t, len(editPrepared.Targets) == 1 && editPrepared.Targets[0].CanonicalPath == path, "edit targets = %+v, want the single canonical edit target", editPrepared.Targets)
	{
		result, err := editPrepared.Execute(context.Background(), nil)
		testassert.Require(t, err == nil && strings.Contains(result, "lines 1-"), "Execute = %q, %v; want edit summary", result, err)
	}
	assertFileContent(t, path, "final")

	// A prepared edit whose old_string is missing fails inside the closure
	// without mutating.
	missing, err := NormalizeEditArgs(map[string]any{"path": path, "old_string": "absent", "new_string": "x"})
	testassert.NoError(t, err)
	missingPrepared, err := PrepareEditCall(dir, "", store, missing)
	testassert.NoError(t, err)
	{
		_, err := missingPrepared.Execute(context.Background(), nil)
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "not found"), "Execute error = %v, want missing match error", err)
	}
	assertFileContent(t, path, "final")

	// A prepared write to a missing nested path creates the required
	// parent directories as part of the single file.write target.
	nested := filepath.Join(dir, "created", "deep", "new.txt")
	nestedArgs, err := NormalizeWriteArgs(map[string]any{"path": nested, "content": "made"})
	testassert.NoError(t, err)
	nestedPrepared, err := PrepareWriteCall(dir, "", store, nestedArgs)
	testassert.NoError(t, err)
	{
		_, err := nestedPrepared.Execute(context.Background(), nil)
		testassert.NoError(t, err)
	}
	assertFileContent(t, nested, "made")
}

// TestPrepareWriteAndEditDiscardSnapshotClaimOnPostSnapshotRevalidationFailure
// extends the post-snapshot repoint window: when the post-snapshot binding
// revalidation refuses a repointed target, the unmutated snapshot claim must
// be discarded exactly like any other pre-mutation failure (the retained
// legacy discard-before-mutation behavior), not left claimed.
func TestPrepareWriteAndEditDiscardSnapshotClaimOnPostSnapshotRevalidationFailure(t *testing.T) {
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
	args, err := NormalizeWriteArgs(map[string]any{"path": link, "content": "after"})
	testassert.NoError(t, err)
	prepared, err := PrepareWriteCall(dir, "", store, args)
	testassert.NoError(t, err)
	{
		_, err := prepared.Execute(context.Background(), nil)
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "approved canonical path changed"), "write Execute error = %v, want post-snapshot canonical change refusal", err)
	}
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
	editArgs, err := NormalizeEditArgs(map[string]any{"path": link, "old_string": "before", "new_string": "after"})
	testassert.NoError(t, err)
	editPrepared, err := PrepareEditCall(dir, "", store2, editArgs)
	testassert.NoError(t, err)
	{
		_, err := editPrepared.Execute(context.Background(), nil)
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "approved canonical path changed"), "edit Execute error = %v, want post-snapshot canonical change refusal", err)
	}
	testassert.Require(t, len(store2.discards) == 1 && store2.discards[0] == store2.entryIDFor(real1), "edit discards = %v, want the unmutated snapshot claim for %s discarded once", store2.discards, real1)
	assertEntryDiscarded(t, store2, real1)
	assertSnapshotEntryCount(t, store2.groupDir, 0)
	assertFileContent(t, real1, "before")
	assertFileContent(t, real2, "before")
}
