package melta

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// Direct standard-tool contract tests: the baseline tool values are built
// from one constructed state the way the owner composes them; the zero
// Invocation exercises the defaults path, and configuration-bearing behavior
// is covered by the publication tests in the runtime package.

// testEntry is a valid-looking Harness admitted-input identity.
const (
	testSessionID = "0123456789abcdef0123456789abcdef"
	testEntryID   = "fedcba9876543210fedcba9876543210"
)

// directTools constructs the baseline tool values over one data root with no
// jobs service, the way newStandardTools composes them.
func directTools(dataDir string) map[string]Tool {
	return newStandardTools(dataDir, nil, newLiveToolOutput(newObservation()))
}

// callToolContext is the ToolContext the direct tests prepare with: a real
// temporary Workspace, a valid-looking admitted-input identity, and the zero
// Invocation (defaults).
func callToolContext(workspace string) ToolContext {
	return ToolContext{
		Workspace:     workspace,
		AdmittedEntry: harness.EntryRef{SessionID: testSessionID, EntryID: testEntryID},
		Invocation:    Invocation{},
	}
}

func makeCall(t *testing.T, name, args string) model.ToolCall {
	t.Helper()
	call, err := model.NewToolCall(model.ToolCall{ID: "call-1", Name: name, Arguments: json.RawMessage(args)})
	testassert.NoError(t, err)
	return call
}

// prepare normalizes and prepares one call, returning the plan.
func prepare(t *testing.T, tool Tool, tc ToolContext, name, args string) toolPlan {
	t.Helper()
	return tool.Prepare(context.Background(), tc, makeCall(t, name, args))
}

// normalize runs one tool's normalization over raw argument bytes.
func normalize(t *testing.T, tool Tool, tc ToolContext, name, args string) (json.RawMessage, error) {
	t.Helper()
	return tool.Normalize(context.Background(), tc, makeCall(t, name, args))
}

// groupDir is the exact per-Operation code-group directory the mutating
// calls must derive from the ToolContext's admitted-input identity.
func groupDir(dataDir, sessionID, entryID string) string {
	return filepath.Join(dataDir, "code", sessionID, entryID)
}

func TestStandardToolSchemaRejectsBeforeRun(t *testing.T) {
	e := newOwnerEnv(t)
	e.fixtureStore(storage.NewMemory())
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("invalid", "read_file", `{"path":17}`)
	})
	r, err := e.open(context.Background())
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	session, err := r.createSession(context.Background(), t.TempDir(), "")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "schema", "read")
	awaitOperation(t, r, session.Identity.SessionID, "schema", harness.OperationSuccess)
	result := toolResultFact(t, r, session.Identity.SessionID, "schema")
	testassert.Require(t, result.Status == model.ResultError && result.Evidence == harness.ExecutionNotStarted && strings.Contains(result.Content, "path"), "result = %+v", result)
	testassert.Require(t, result.Implementation == nil && len(result.NormalizedArguments) == 0, "validation exit recorded selection: %+v", result)
}

func TestStandardToolPreparationFailureNotStarted(t *testing.T) {
	e := newOwnerEnv(t)
	e.fixtureStore(storage.NewMemory())
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("invalid", "read_file", `{"path":"loop/file"}`)
	})
	r, err := e.open(context.Background())
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	workspace := t.TempDir()
	testassert.NoError(t, os.Symlink("loop", filepath.Join(workspace, "loop")))
	session, err := r.createSession(context.Background(), workspace, "")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "path", "read")
	awaitOperation(t, r, session.Identity.SessionID, "path", harness.OperationSuccess)
	result := toolResultFact(t, r, session.Identity.SessionID, "path")
	testassert.Require(t, result.Status == model.ResultError && result.Evidence == harness.ExecutionNotStarted && strings.Contains(result.Content, "resolve path"), "result = %+v", result)
}

func TestValidateConfigSettings(t *testing.T) {
	validate := func(raw string) error {
		return validateToolsSettings(json.RawMessage(raw))
	}
	for _, raw := range []string{
		"", " ", "{}", "null",
		`{"max_output_bytes":null,"read_max_lines":null,"read_line_max_chars":null,"command_timeout":null}`,
		`{"max_output_bytes":1,"read_max_lines":1,"read_line_max_chars":1,"command_timeout":1}`,
		`{"max_output_bytes":9223372036854775807,"read_max_lines":1,"read_line_max_chars":1,"command_timeout":1}`,
		`{"command_timeout":9223372036}`,
		// Every other member is ignored, including max_background_processes
		// regardless of its JSON value.
		`{"max_background_processes":5}`,
		`{"max_background_processes":"many","unknown_member":{"deep":[1,2]}}`,
	} {
		{
			err := validate(raw)
			testassert.Check(t, err == nil, "ValidateConfig(%s) = %v, want accepted", raw, err)
		}
	}
	for _, raw := range []string{
		`{"max_output_bytes":0}`, `{"read_max_lines":-1}`, `{"read_line_max_chars":0}`, `{"command_timeout":0}`,
		`{"command_timeout":-5}`,
		`{"max_output_bytes":"many"}`, `{"read_max_lines":true}`, `{"command_timeout":1.5}`,
		// Integer overflow and the seconds-to-duration bound (MaxInt64/time.Second).
		`{"max_output_bytes":99999999999999999999}`,
		`{"command_timeout":9223372036854775808}`,
		`{"command_timeout":9223372036854775807}`,
		`{"command_timeout":9223372037}`,
		// Wrong section shapes.
		`[]`, `"tools"`, `5`,
	} {
		{
			err := validate(raw)
			testassert.Check(t, err != nil, "ValidateConfig(%s) = nil, want rejected", raw)
		}
	}
}

// TestStandardToolDeclarations proves the baseline advertisement: the five
// standard declarations arrive in order, each with its own name, a nonempty
// description and a valid parameters schema.
func TestStandardToolDeclarations(t *testing.T) {
	want := []string{"read_file", "write_file", "edit_file", "run_command", "process"}
	declarations := standardToolDeclarations()
	testassert.Equal(t, len(declarations), len(want))
	for i, declaration := range declarations {
		testassert.Check(t, declaration.id == want[i], "declaration %d id = %q, want %q", i, declaration.id, want[i])
		testassert.Check(t, declaration.definition.Name == want[i], "declaration %d name = %q, want %q", i, declaration.definition.Name, want[i])
		testassert.Check(t, declaration.definition.Description != "" && json.Valid(declaration.definition.Parameters), "declaration %d produced an unusable definition", i)
	}
}

// assertJSONEqual compares two JSON payloads by their decoded value.
func assertJSONEqual(t *testing.T, got json.RawMessage, want any) {
	t.Helper()
	wantRaw, err := json.Marshal(want)
	testassert.NoError(t, err)
	var gotValue, wantValue any
	{
		err := json.Unmarshal(got, &gotValue)
		testassert.Require(t, err == nil, "decode described schema %s: %v", got, err)
	}
	{
		err := json.Unmarshal(wantRaw, &wantValue)
		testassert.Require(t, err == nil, "decode retained schema %s: %v", wantRaw, err)
	}
	testassert.Check(t, reflect.DeepEqual(gotValue, wantValue), "described schema = %s, want the retained schema %s", got, wantRaw)
}

// wantRunCommandDescription is the independent golden fixture for the
// ordinary run_command description, verified against the retained value. It
// is deliberately a test-local literal rather than the production constant,
// so drift in descriptions.go fails this test.
const wantRunCommandDescription = `Executes a shell command and returns combined stdout and stderr.
- Each call starts a fresh shell in the project root. Environment variables, aliases, and working directory do not persist between calls. Use "cd /path && command" if you need a different working directory.
- Foreground commands use the default timeout. Background commands run until they exit, are killed, or reach an explicit timeout parameter.
- For commands that may run for a long time, keep producing output, wait on external state, and are not needed before your next step, set background=true. It returns immediately with a process ID. You will be notified when it finishes. To read its output while it is still running, or to kill it, use process. Do not use background=true for commands that will probably finish in a few seconds.
- Do not use this tool to read file contents — use read_file. Do not use this tool to edit files — use edit_file or write_file.`

// wantRunCommandParameters is the independent golden fixture for the one
// run_command schema the ordinary and readonly surfaces share.
const wantRunCommandParameters = `{
  "type": "object",
  "properties": {
    "command": {
      "type": "string",
      "description": "The shell command to execute."
    },
    "timeout": {
      "type": "integer",
      "description": "Timeout in seconds for this command. Overrides the default."
    },
    "background": {
      "type": "boolean",
      "description": "If true, run the command in the background and return immediately with a process ID."
    }
  },
  "required": ["command"]
}`

// wantProcessDescription and wantProcessParameters are the independent golden
// fixtures for the process tool's model-visible surface, verified against the
// retained values. They are deliberately test-local literals rather than the
// production constants, so drift in descriptions.go fails these tests.
const wantProcessDescription = "Manage background processes started by run_command with background=true.\n" +
	"\t- Use action \"read\" with id to read the output of a running background process.\n" +
	"\t- Use action \"kill\" with id to terminate a background process.\n" +
	"\t- Use action \"list\" to list background processes in the current session and their status."

const wantProcessParameters = `{
  "type": "object",
  "properties": {
    "action": {
      "type": "string",
      "description": "Action to perform: \"read\" to get output, \"kill\" to terminate, \"list\" to list processes in the current session."
    },
    "id": {
      "type": "string",
      "description": "Process ID returned by run_command with background=true."
    }
  },
  "required": ["action"]
}`

// TestRunCommandDescriptionAndSchemaMatchRetained pins the ordinary
// run_command surface to the retained text: the complete description with the
// background bullets and the retained schema. The expectations are independent
// test-local fixtures, not the production constants.
func TestRunCommandDescriptionAndSchemaMatchRetained(t *testing.T) {
	var ordinary model.ToolDefinition
	for _, declaration := range standardToolDeclarations() {
		if declaration.id == "run_command" {
			ordinary = declaration.definition
		}
	}
	testassert.Check(t, ordinary.Description == wantRunCommandDescription, "ordinary description = %q, want the retained description %q", ordinary.Description, wantRunCommandDescription)
	assertJSONEqual(t, ordinary.Parameters, json.RawMessage(wantRunCommandParameters))
}

// TestProcessDescriptionAndSchemaMatchRetained pins the process tool's
// model-visible surface to the independent test-local fixtures.
func TestProcessDescriptionAndSchemaMatchRetained(t *testing.T) {
	var got model.ToolDefinition
	for _, declaration := range standardToolDeclarations() {
		if declaration.id == "process" {
			got = declaration.definition
		}
	}
	testassert.Check(t, got.Description == wantProcessDescription, "process description = %q, want the retained description %q", got.Description, wantProcessDescription)
	assertJSONEqual(t, got.Parameters, json.RawMessage(wantProcessParameters))
	testassert.Check(t, got.Name == "process", "process definition = %+v, want name process", got)
}

func TestNormalizeArguments(t *testing.T) {
	byID := directTools(t.TempDir())
	tc := callToolContext(t.TempDir())

	t.Run("read_file applies defaults and strips private fields", func(t *testing.T) {
		raw, err := normalize(t, byID["read_file"], tc, "read_file", `{"path":"a.txt","_melta_receipt":"x"}`)
		testassert.NoError(t, err)
		var args map[string]any
		testassert.NoError(t, json.Unmarshal(raw, &args))
		testassert.Require(t, args["path"] == "a.txt" && args["offset"] == float64(1) && args["limit"] == float64(500), "normalized = %s, want the path with default offset/limit", raw)
		{
			_, ok := args["_melta_receipt"]
			testassert.Require(t, !ok, "normalized arguments retained a private field")
		}
	})
	t.Run("read_file keeps exact integral spellings and rejects fractions and overflow", func(t *testing.T) {
		raw, err := normalize(t, byID["read_file"], tc, "read_file", `{"path":"a","offset":1e0,"limit":2.0}`)
		testassert.NoError(t, err)
		testassert.Require(t, strings.Contains(string(raw), `"offset":1`) && strings.Contains(string(raw), `"limit":2`), "normalized = %s, want exact integral values", raw)
		{
			_, err := normalize(t, byID["read_file"], tc, "read_file", `{"path":"a","limit":2.5}`)
			testassert.Check(t, err != nil, "fractional limit normalized, want rejection")
		}
		{
			_, err := normalize(t, byID["read_file"], tc, "read_file", `{"path":"a","offset":99999999999999999999}`)
			testassert.Check(t, err != nil, "overflowing offset normalized, want rejection")
		}
	})
	t.Run("strict schema errors", func(t *testing.T) {
		for name, args := range map[string]string{
			"read_file":   `{"path":5}`,
			"read_file2":  `{"path":"a","limit":"many"}`,
			"write_file":  `{"path":"a"}`,
			"write_file2": `{"path":"a","content":5}`,
			"edit_file":   `{"path":"a","old_string":"x","new_string":5}`,
			"edit_file2":  `{"path":"a","old_string":"x","new_string":"y","replace_all":"yes"}`,
		} {
			toolName := name
			if strings.HasSuffix(toolName, "2") {
				toolName = strings.TrimSuffix(toolName, "2")
			}
			{
				_, err := normalize(t, byID[toolName], tc, toolName, args)
				testassert.Check(t, err != nil, "%s normalized %s, want rejection", toolName, args)
			}
		}
	})
	t.Run("process requires string action and id members when present", func(t *testing.T) {
		for _, args := range []string{
			`{"action":5}`,
			`{"action":null}`,
			`{"action":"read","id":5}`,
			`{"action":"list","id":true}`,
		} {
			{
				_, err := normalize(t, byID["process"], tc, "process", args)
				testassert.Check(t, err != nil, "process normalized %s, want rejection", args)
			}
		}
		raw, err := normalize(t, byID["process"], tc, "process", `{"action":"read","id":"deadbeef","_melta_receipt":"x","_lightcode_receipt":"ordinary","note":"kept"}`)
		testassert.NoError(t, err)
		var normalized map[string]any
		testassert.NoError(t, json.Unmarshal(raw, &normalized))
		testassert.Require(t, normalized["action"] == "read" && normalized["id"] == "deadbeef" && normalized["note"] == "kept", "normalized = %s, want the members preserved", raw)
		testassert.Equal(t, normalized["_lightcode_receipt"], any("ordinary"))
		{
			_, ok := normalized["_melta_receipt"]
			testassert.Require(t, !ok, "normalized arguments retained a private field")
		}
	})
	t.Run("malformed, null and trailing argument bytes are validation errors", func(t *testing.T) {
		for _, args := range []string{``, `not json`, `null`, `[1]`, `{"path":"a"} trailing`, `"str"`} {
			for _, name := range []string{"read_file", "write_file", "edit_file", "run_command", "process"} {
				{
					_, err := normalize(t, byID[name], tc, name, args)
					testassert.Check(t, err != nil, "%s normalized %q, want rejection", name, args)
				}
			}
		}
	})
}

// TestPrepareBindingSurvivesCanonicalPathsRepoint proves the one-binding
// contract at the plugin boundary: the canonical Workspace root the plugin
// resolves for containment is the same witness the shared preparation binds,
// so repointing the symlink between the plugin's resolution and the shared
// preparation's binding fails the execution with the changed-binding error
// instead of executing under the repointed tree.
func TestPrepareBindingSurvivesCanonicalPathsRepoint(t *testing.T) {
	cells := []struct {
		name    string
		toolID  string
		args    string
		content string // planted in the repointed-to directory when nonempty
	}{
		{"read_file revalidates the root witness", "read_file", `{"path":"target.txt"}`, "substituted\n"},
		{"write_file revalidates the root witness", "write_file", `{"path":"target.txt","content":"after"}`, ""},
		{"edit_file revalidates the root witness", "edit_file", `{"path":"target.txt","old_string":"beta","new_string":"BETA"}`, "beta\n"},
	}
	for _, cell := range cells {
		cell := cell
		t.Run(cell.name, func(t *testing.T) {
			byID := directTools(t.TempDir())
			realA, realB := t.TempDir(), t.TempDir()
			flipTo := realB
			link := filepath.Join(t.TempDir(), "w")
			workspace := link
			testassert.NoError(t, os.Symlink(realA, link))
			if cell.content != "" {
				testassert.NoError(t, os.WriteFile(filepath.Join(flipTo, "target.txt"), []byte(cell.content), 0o644))
			}
			// The seam repoints the symlink on its return: after the plugin's
			// canonical resolution, before the shared preparation binds.
			orig := canonicalPathsFn
			canonicalPathsFn = func(root string) (string, error) {
				w, err := orig(root)
				if err != nil {
					return w, err
				}
				testassert.NoError(t, os.Remove(link))
				testassert.NoError(t, os.Symlink(flipTo, link))
				return w, err
			}
			defer func() { canonicalPathsFn = orig }()
			plan := prepare(t, byID[cell.toolID], callToolContext(workspace), cell.toolID, cell.args)
			outcome := plan.Execute(context.Background())
			testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, "changed"), "result = %+v, want the changed-binding failure without substitution", outcome.Result)
		})
	}
}
