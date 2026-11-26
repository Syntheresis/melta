package patch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// testWorkspace is the test double for the Host's melta.workspace service. It
// resolves paths exactly as the Host does and performs the four Workspace
// operations over the real filesystem.
type testWorkspace struct {
	root string

	mu           sync.Mutex
	writes       []string
	deletes      []string
	onWrite      func(int)
	onStat       func(int)
	stats        int
	writeErr     func(int) error
	shortWriteAt int
}

func newTestWorkspace(t *testing.T, root string) *testWorkspace {
	t.Helper()
	return &testWorkspace{root: root}
}

func (w *testWorkspace) call(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	if service != extension.ContractWorkspace {
		return extension.Result{}, fmt.Errorf("unexpected service %q", service)
	}
	switch method {
	case "stat":
		var input struct {
			Path *string `json:"path"`
		}
		if err := json.Unmarshal(args, &input); err != nil || input.Path == nil || *input.Path == "" {
			return extension.Result{}, fmt.Errorf("invalid stat arguments")
		}
		w.stats++
		if w.onStat != nil {
			w.onStat(w.stats)
		}
		canonical, exists, err := testResolve(w.root, *input.Path)
		if err != nil {
			return extension.Result{}, err
		}
		value, err := json.Marshal(struct {
			CanonicalPath string `json:"canonical_path"`
			Exists        bool   `json:"exists"`
		}{canonical, exists})
		return extension.Result{Value: value}, err
	case "read":
		target, err := w.target(args)
		if err != nil {
			return extension.Result{}, err
		}
		data, err := os.ReadFile(target)
		if err != nil {
			return extension.Result{}, err
		}
		value, err := json.Marshal(struct {
			Bytes []byte `json:"bytes"`
		}{data})
		return extension.Result{Value: value}, err
	case "write":
		var input struct {
			Path       string `json:"path"`
			Bytes      []byte `json:"bytes"`
			CreateOnly bool   `json:"create_only"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			return extension.Result{}, err
		}
		target, _, err := testResolve(w.root, input.Path)
		if err != nil {
			return extension.Result{}, err
		}
		if input.CreateOnly {
			if _, err := os.Stat(target); err == nil {
				return extension.Result{}, fmt.Errorf("path %s appeared during apply (Add requires the path to not exist)", target)
			}
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return extension.Result{}, err
		}
		index := w.recordWrite(target)
		w.mu.Lock()
		short := w.shortWriteAt == index
		w.mu.Unlock()
		if short {
			return extension.Result{}, &extension.CallError{Err: fmt.Errorf("write: incomplete 1 of %d bytes", len(input.Bytes)), Partial: json.RawMessage(`{"outcome":"uncertain"}`)}
		}
		var injected error
		w.mu.Lock()
		if w.writeErr != nil {
			injected = w.writeErr(index)
		}
		hook := w.onWrite
		w.mu.Unlock()
		if err := os.WriteFile(target, input.Bytes, 0o644); err != nil {
			return extension.Result{}, err
		}
		if hook != nil {
			hook(index)
		}
		if injected != nil {
			return extension.Result{}, &extension.CallError{Err: injected, Partial: json.RawMessage(`{"outcome":"completed"}`)}
		}
		return extension.Result{Value: json.RawMessage(`{"outcome":"completed"}`)}, nil
	case "delete":
		target, err := w.target(args)
		if err != nil {
			return extension.Result{}, err
		}
		w.mu.Lock()
		w.deletes = append(w.deletes, target)
		w.mu.Unlock()
		if err := os.Remove(target); err != nil {
			return extension.Result{}, err
		}
		return extension.Result{Value: json.RawMessage(`{"outcome":"completed"}`)}, nil
	default:
		return extension.Result{}, fmt.Errorf("unexpected method %q", method)
	}
}

func (w *testWorkspace) target(args json.RawMessage) (string, error) {
	var input struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return "", err
	}
	path, _, err := testResolve(w.root, input.Path)
	return path, err
}

func (w *testWorkspace) recordWrite(target string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, target)
	return len(w.writes)
}

// testResolve mirrors the Host's canonicalization: an existing leaf is
// resolved through every symlink; an absent leaf resolves its deepest
// existing parent and appends the remaining components.
func testResolve(root, path string) (string, bool, error) {
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false, err
	}
	if _, err := os.Lstat(abs); err == nil {
		canonical, err := filepath.EvalSymlinks(abs)
		return canonical, true, err
	} else if !os.IsNotExist(err) {
		return "", false, err
	}
	parent := filepath.Dir(abs)
	tail := []string{filepath.Base(abs)}
	for {
		if _, err := os.Lstat(parent); err == nil {
			canonicalParent, err := filepath.EvalSymlinks(parent)
			if err != nil {
				return "", false, err
			}
			return filepath.Join(append([]string{canonicalParent}, tail...)...), false, nil
		} else if !os.IsNotExist(err) {
			return "", false, err
		}
		next := filepath.Dir(parent)
		if next == parent {
			return abs, false, nil
		}
		tail = append([]string{filepath.Base(parent)}, tail...)
		parent = next
	}
}

// runPatch drives one patch call.
func runPatch(t *testing.T, ws *testWorkspace, input string) harness.ToolOutcome {
	t.Helper()
	arguments, err := json.Marshal(map[string]string{"input": input})
	testassert.NoError(t, err)
	return extension.RunTool(context.Background(), Tool, extension.ToolCall{AuthorContext: extension.AuthorContext{Call: ws.call}, ID: "call-1", Name: ToolName, Arguments: arguments})
}

func applyPatchInput(t *testing.T, s string) string {
	t.Helper()
	return "*** Begin Patch\n" + s + "\n*** End Patch"
}

func metadataFiles(t *testing.T, outcome harness.ToolOutcome) []fileEntry {
	t.Helper()
	if len(outcome.Metadata) == 0 {
		return nil
	}
	var metadata map[string]any
	testassert.NoError(t, json.Unmarshal(outcome.Metadata, &metadata))
	value, ok := metadata["edit_preview_files"]
	testassert.Require(t, ok, "edit_preview_files not present: %s", outcome.Metadata)
	raw, err := json.Marshal(value)
	testassert.NoError(t, err)
	var files []fileEntry
	testassert.NoError(t, json.Unmarshal(raw, &files))
	return files
}

func outcomeFailed(t *testing.T, outcome harness.ToolOutcome, want string) {
	t.Helper()
	testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content != "" && (want == "" || strings.Contains(outcome.Result.Content, want)), "outcome = %+v, want an error containing %q", outcome.Result, want)
}
