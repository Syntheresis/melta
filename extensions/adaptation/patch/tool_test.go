package patch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestPatchAddWithWorkerErrors(t *testing.T) {
	ws := newTestWorkspace(t, t.TempDir())
	call := func(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		result, err := ws.call(ctx, service, method, args)
		if err != nil {
			return extension.Result{}, worker.Classify(err)
		}
		return result, nil
	}
	args := json.RawMessage(`{"input":"*** Begin Patch\n*** Add File: new.txt\n+hello\n*** End Patch"}`)
	_, err := Tool(context.Background(), extension.ToolCall{AuthorContext: extension.AuthorContext{Call: call}, Arguments: args})
	testassert.NoError(t, err)
	assertFileContent(t, filepath.Join(ws.root, "new.txt"), "hello")
}

func TestPatchUncertainFailureWrapsCause(t *testing.T) {
	ws := newTestWorkspace(t, t.TempDir())
	cause := errors.New("write failed")
	failure := &extension.CallError{Err: cause, Partial: json.RawMessage(`{"outcome":"uncertain"}`)}
	call := func(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if method == "write" {
			return extension.Result{}, failure
		}
		return ws.call(ctx, service, method, args)
	}
	args := json.RawMessage(`{"input":"*** Begin Patch\n*** Add File: new.txt\n+hello\n*** End Patch"}`)
	_, err := Tool(context.Background(), extension.ToolCall{AuthorContext: extension.AuthorContext{Call: call}, Arguments: args})
	var got *extension.CallError
	testassert.Require(t, errors.Is(err, cause) && errors.Is(err, extension.ErrOutcomeUnknown) && errors.As(err, &got) && got == failure,
		"error = %v, want the uncertain marker and original failure", err)
	testassert.Require(t, strings.Contains(err.Error(), "A new.txt (uncertain)\n\n") && strings.Contains(err.Error(), "write failed"), "report = %v", err)
}

func TestPatchInvalidInput(t *testing.T) {
	for _, args := range []string{`{"input":42}`, `{}`} {
		_, err := Tool(context.Background(), extension.ToolCall{Arguments: []byte(args)})
		testassert.Require(t, err != nil, "input %s accepted", args)
	}
}

func TestPatchProducesPreview(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("hi"), 0o644))
	ws := newTestWorkspace(t, root)
	arguments := []byte(`{"input":"*** Begin Patch\n*** Update File: a.txt\n@@\n-hi\n+bye\n*** End Patch"}`)
	outcome := extension.RunTool(context.Background(), Tool, extension.ToolCall{AuthorContext: extension.AuthorContext{Call: ws.call}, ID: "call-1", Name: ToolName, Arguments: arguments})
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "Success. Updated the following files:"), "outcome = %+v, want success", outcome.Result)
	files := metadataFiles(t, outcome)
	testassert.Require(t, len(files) == 1 && files[0].Path == "a.txt" && files[0].Op == "M", "previews = %+v, want the applied M preview for a.txt", files)
	assertFileContent(t, filepath.Join(root, "a.txt"), "bye")
}

func TestPreparePatchFailuresReturnDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		setup func(*testing.T, string)
	}{
		{name: "parse error", input: "*** Begin Patch\n*** End Patch"},
		{name: "stat failure", input: "*** Begin Patch\n*** Update File: loop/file.txt\n@@\n-hi\n+bye\n*** End Patch", setup: func(t *testing.T, root string) {
			testassert.NoError(t, os.Symlink("loop", filepath.Join(root, "loop")))
		}},
		{name: "canonical collision", input: "*** Begin Patch\n*** Update File: a.txt\n@@\n-hi\n+bye\n*** Update File: ./a.txt\n@@\n-hi\n+there\n*** End Patch", setup: func(t *testing.T, root string) {
			testassert.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("hi"), 0o644))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if tc.setup != nil {
				tc.setup(t, root)
			}
			outcome := runPatch(t, newTestWorkspace(t, root), tc.input)
			testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content != "", "result = %+v, want a diagnostic", outcome)
		})
	}
}
