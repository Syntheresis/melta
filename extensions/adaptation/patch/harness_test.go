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
// resolves bindings exactly as the Host does and performs the four Workspace
// operations over the real filesystem.
type testWorkspace struct {
	root string

	mu           sync.Mutex
	next         int
	bindings     map[string]string
	writes       []string
	deletes      []string
	onWrite      func(int)
	writeErr     func(int) error
	shortWriteAt int
}

func newTestWorkspace(t *testing.T, root string) *testWorkspace {
	t.Helper()
	return &testWorkspace{root: root, bindings: make(map[string]string)}
}

func (w *testWorkspace) call(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	if service != workspaceService {
		return extension.Result{}, fmt.Errorf("unexpected service %q", service)
	}
	switch method {
	case "bind":
		var input struct {
			Path *string `json:"path"`
		}
		if err := json.Unmarshal(args, &input); err != nil || input.Path == nil || *input.Path == "" {
			return extension.Result{}, fmt.Errorf("invalid bind arguments")
		}
		canonical, exists, err := testResolve(w.root, *input.Path)
		if err != nil {
			return extension.Result{}, err
		}
		w.mu.Lock()
		w.next++
		handle := fmt.Sprintf("binding-%d", w.next)
		w.bindings[handle] = canonical
		w.mu.Unlock()
		value, err := json.Marshal(struct {
			Binding       string `json:"binding"`
			CanonicalPath string `json:"canonical_path"`
			Exists        bool   `json:"exists"`
		}{handle, canonical, exists})
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
			Binding    string `json:"binding"`
			Bytes      []byte `json:"bytes"`
			CreateOnly bool   `json:"create_only"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			return extension.Result{}, err
		}
		target, err := w.targetByHandle(input.Binding)
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
		Binding string `json:"binding"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return "", err
	}
	return w.targetByHandle(input.Binding)
}

func (w *testWorkspace) targetByHandle(handle string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	target, ok := w.bindings[handle]
	if !ok {
		return "", fmt.Errorf("unknown binding %q", handle)
	}
	return target, nil
}

func (w *testWorkspace) recordWrite(target string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, target)
	return len(w.writes)
}

// testResolve mirrors the Host's bind canonicalization: an existing leaf is
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

// preparePatch drives one patch call through Normalize and Prepare.
func preparePatch(t *testing.T, ws *testWorkspace, input string) extension.ToolPlan {
	t.Helper()
	arguments, err := json.Marshal(map[string]string{"input": input})
	testassert.NoError(t, err)
	call := model.ToolCall{ID: "call-1", Name: ToolName, Arguments: arguments}
	normalized, err := (Tool{}).Normalize(extension.AuthorContext{}, call)
	testassert.NoError(t, err)
	call.Arguments = normalized
	return (Tool{}).Prepare(context.Background(), extension.AuthorContext{Call: ws.call}, call)
}

func executePrepared(t *testing.T, ws *testWorkspace, plan extension.ToolPlan) harness.ToolOutcome {
	t.Helper()
	if plan.Immediate != nil {
		return *plan.Immediate
	}
	return (Tool{}).Execute(context.Background(), extension.AuthorContext{Call: ws.call}, plan)
}

// runPatch drives one patch call through Normalize, Prepare and Execute.
func runPatch(t *testing.T, ws *testWorkspace, input string) harness.ToolOutcome {
	t.Helper()
	return executePrepared(t, ws, preparePatch(t, ws, input))
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
