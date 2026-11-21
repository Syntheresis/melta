package patch

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestNormalizePatchArgsStrictInputType(t *testing.T) {
	{
		_, err := (Tool{}).Normalize(extension.AuthorContext{}, model.ToolCall{Arguments: []byte(`{"input":42}`)})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "apply_patch: input must be a string"), "Normalize error = %v, want input type error", err)
	}
	{
		_, err := (Tool{}).Normalize(extension.AuthorContext{}, model.ToolCall{Arguments: []byte(`{"input":null}`)})
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "apply_patch: input must be a string"), "Normalize error = %v, want present-null input rejected", err)
	}
	normalized, err := (Tool{}).Normalize(extension.AuthorContext{}, model.ToolCall{Arguments: []byte(`{"input":"*** Begin Patch\n*** End Patch","_melta_receipt":"forged","unrelated":"kept"}`)})
	testassert.NoError(t, err)
	var args map[string]any
	testassert.NoError(t, json.Unmarshal(normalized, &args))
	testassert.Require(t, args["unrelated"] == "kept", "normalized args = %s, want unrelated fields retained", normalized)
	{
		_, ok := args["_melta_receipt"]
		testassert.Require(t, !ok, "normalized args retained a private field")
	}
	// Missing input stays a later patch-syntax failure, which is the fixed
	// denial for a failed canonical preparation.
	root := t.TempDir()
	ws := newTestWorkspace(t, root)
	missing, err := (Tool{}).Normalize(extension.AuthorContext{}, model.ToolCall{Arguments: []byte(`{}`)})
	testassert.NoError(t, err)
	plan := (Tool{}).Prepare(context.Background(), extension.AuthorContext{Call: ws.call}, model.ToolCall{ID: "call-1", Name: ToolName, Arguments: missing})
	testassert.Require(t, plan.Immediate != nil && plan.Immediate.Result.Status == model.ResultDenied && plan.Immediate.Result.Content == harness.PermissionDeniedContent, "plan = %+v, want the fixed denial for missing input", plan.Immediate)
}

func TestPreparePatchExecutesParsedPatchWithoutReparse(t *testing.T) {
	root := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("hi"), 0o644))
	ws := newTestWorkspace(t, root)
	arguments := []byte(`{"input":"*** Begin Patch\n*** Update File: a.txt\n@@\n-hi\n+bye\n*** End Patch"}`)
	call := model.ToolCall{ID: "call-1", Name: ToolName, Arguments: arguments}
	normalized, err := (Tool{}).Normalize(extension.AuthorContext{}, call)
	testassert.NoError(t, err)
	call.Arguments = normalized
	author := extension.AuthorContext{Call: ws.call}
	plan := (Tool{}).Prepare(context.Background(), author, call)
	testassert.Require(t, plan.Immediate == nil, "prepare = %+v", plan.Immediate)

	// Later argument mutation must not leak into the prepared plan.
	for i := range call.Arguments {
		call.Arguments[i] = 'x'
	}
	outcome := (Tool{}).Execute(context.Background(), author, plan)
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "Success. Updated the following files:"), "outcome = %+v, want the prepared plan executed", outcome.Result)
	files := metadataFiles(t, outcome)
	testassert.Require(t, len(files) == 1 && files[0].Path == "a.txt" && files[0].Op == "M", "previews = %+v, want the applied M preview for a.txt", files)
	assertFileContent(t, filepath.Join(root, "a.txt"), "bye")
}

func TestPreparePatchFailuresAreFixedDenials(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		setup func(*testing.T, string)
	}{
		{name: "parse error", input: "*** Begin Patch\n*** End Patch"},
		{name: "bind failure", input: "*** Begin Patch\n*** Update File: loop/file.txt\n@@\n-hi\n+bye\n*** End Patch", setup: func(t *testing.T, root string) {
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
			plan := preparePatch(t, newTestWorkspace(t, root), tc.input)
			testassert.Require(t, plan.Immediate != nil && plan.Immediate.Result.Status == model.ResultDenied && plan.Immediate.Result.Content == harness.PermissionDeniedContent, "immediate = %+v, want the fixed denial", plan.Immediate)
		})
	}
}
