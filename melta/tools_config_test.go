package melta

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestConfiguredToolsSettingsTakeEffect proves the published top-level tools
// values change tool behavior through the real captured Invocation channel:
// the configured read_max_lines is the normalization default limit and
// bounds the executed read window end-to-end, while the zero Invocation keeps
// the shipped defaults. The configured Invocation comes from the test-only
// runtime bridge, because outside the runtime package only the zero
// Invocation is constructible.
func TestConfiguredToolsSettingsTakeEffect(t *testing.T) {
	ctx := context.Background()
	byID, _ := openToolsComposed(t, t.TempDir(), &fakeJobs{}, nil)
	value, ok := byID["read_file"]
	testassert.Require(t, ok, "plugin instance exports no read_file")
	readTool := value
	call := func(t *testing.T, args string) model.ToolCall {
		t.Helper()
		completed, err := model.NewToolCall(model.ToolCall{ID: "call-1", Name: "read_file", Arguments: json.RawMessage(args)})
		testassert.NoError(t, err)
		return completed
	}

	configured, err := ConfiguredInvocationForTest(`{"read_max_lines":77}`, "", "", nil)
	testassert.NoError(t, err)

	ws := t.TempDir()
	lines := make([]string, 100)
	for i := range lines {
		lines[i] = strings.Repeat("x", 10)
	}
	testassert.NoError(t, os.WriteFile(filepath.Join(ws, "big.txt"), []byte(strings.Join(lines, "\n")), 0o644))
	tc := ToolContext{
		Workspace:     ws,
		AdmittedEntry: harness.EntryRef{SessionID: "0123456789abcdef0123456789abcdef", EntryID: "fedcba9876543210fedcba9876543210"},
		Invocation:    configured,
	}
	outcome := readTool(ctx, tc, call(t, `{"path":"big.txt"}`))
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.HasPrefix(outcome.Result.Content, "1\txxxxxxxxxx\n") && strings.Contains(outcome.Result.Content, "\n77\txxxxxxxxxx\n") && strings.HasSuffix(outcome.Result.Content, "(Showing lines 1-77 of 100. Use offset=78 to continue.)"), "result = %+v, want success; content = %q, want the configured 77-line window; content = %q, want the configured-limit pagination footer", outcome.Result, outcome.Result.Content, outcome.Result.Content)

	testassert.NoError(t, os.WriteFile(filepath.Join(ws, "default.txt"), []byte(strings.Repeat("x\n", 500)+"x"), 0o644))
	tc.Invocation = Invocation{}
	outcome = readTool(ctx, tc, call(t, `{"path":"default.txt"}`))
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "(Showing lines 1-500 of 501."), "default result = %+v", outcome.Result)
}
