package melta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/jobs"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// openReadOnlyProfile opens the production owner with one read-only Agents
// profile selected in a fresh Session over workspace, plus the optional
// extensions after Agents.
func openReadOnlyProfile(t *testing.T, workspace string, extras ...extension.Extension) (*Melta, *ownerEnv, *fakeJobs, string) {
	t.Helper()
	e := newOwnerEnv(t)
	writeAgentDefinitions(t, e.configPath, `{"reader":{"model":"prov/m","system_prompt":"simple","readonly":true,"tools":["run_command"]}}`)
	jobs := &fakeJobs{}
	e.fixtureJobs(&exitingJobs{fakeJobs: jobs})
	for _, extra := range extras {
		enableFixturePlugin(t, e.configPath, extra.Manifest.ID)
	}
	ctx := context.Background()
	r, err := e.open(ctx, append([]extension.Extension{agents.Author()}, extras...)...)
	testassert.NoError(t, err)
	t.Cleanup(func() { testassert.NoError(t, r.Close(context.Background())) })
	session, err := r.createSession(ctx, workspace, "reader")
	testassert.NoError(t, err)
	return r, e, jobs, session.Identity.SessionID
}

// readOnlyCallScript scripts one run_command call and a final text turn.
func readOnlyCallScript(arguments string) func(context.Context, string) []string {
	return func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("call-command", "run_command", arguments)
	}
}

// exitingJobs records each start like fakeJobs and delivers one successful
// exit so a production owner's background member settles and Close joins.
type exitingJobs struct{ *fakeJobs }

func (j *exitingJobs) Start(req jobs.StartRequest) error {
	err := j.fakeJobs.Start(req)
	if err == nil && req.OnExit != nil {
		go req.OnExit(jobs.ExitResult{ID: req.JobID, Command: req.Command, ExitCode: 0})
	}
	return err
}

// TestReadOnlyProfileForegroundTimeoutIgnoresModelOverride proves the
// production owner's read-only profile clears a model foreground timeout
// override: a command that outlives the model's 1 second request still
// completes under the captured default.
func TestReadOnlyProfileForegroundTimeoutIgnoresModelOverride(t *testing.T) {
	workspace := t.TempDir()
	r, e, _, session := openReadOnlyProfile(t, workspace)
	e.server.setScript(readOnlyCallScript(`{"command":"cat pipe","timeout":1}`))
	fifo := filepath.Join(workspace, "pipe")
	testassert.NoError(t, exec.Command("mkfifo", fifo).Run())
	go func() {
		time.Sleep(2 * time.Second)
		if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
			_, _ = f.WriteString("done")
			_ = f.Close()
		}
	}()
	start := time.Now()
	submitThroughRuntime(t, r, session, "op-timeout", "run")
	awaitOperation(t, r, session, "op-timeout", harness.OperationSuccess)
	testassert.Require(t, time.Since(start) >= 2*time.Second, "operation settled after %s, want the captured default timeout", time.Since(start))
	result := toolResultFact(t, r, session, "op-timeout")
	testassert.Require(t, result.Status == model.ResultSuccess && result.Content == "done", "tool result = %+v, want the command output", result)
}

// TestReadOnlyProfileLaterArgumentHookTimeoutRefused proves a later
// before-tool hook cannot reinstate a nondefault foreground timeout: the
// captured authorization refuses it and the command result never appears.
func TestReadOnlyProfileLaterArgumentHookTimeoutRefused(t *testing.T) {
	gate := testNative("gate", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachArguments(func(_ context.Context, _ extension.AuthorContext, call model.ToolCall) (extension.ArgumentsResult, error) {
			if call.Name != "run_command" {
				return extension.ArgumentsResult{}, nil
			}
			var args map[string]any
			if err := json.Unmarshal(call.Arguments, &args); err != nil {
				return extension.ArgumentsResult{}, err
			}
			args["timeout"] = 999
			value, err := json.Marshal(args)
			return extension.ArgumentsResult{Arguments: value}, err
		}, nil)
		return nil, err
	})
	workspace := t.TempDir()
	r, e, _, session := openReadOnlyProfile(t, workspace, gate)
	e.server.setScript(readOnlyCallScript(`{"command":"pwd"}`))
	submitThroughRuntime(t, r, session, "op-hook-timeout", "run")
	awaitOperation(t, r, session, "op-hook-timeout", harness.OperationSuccess)
	result := toolResultFact(t, r, session, "op-hook-timeout")
	testassert.Require(t, result.Status != model.ResultSuccess && result.Content == "read-only command timeout changed", "tool result = %+v, want the captured timeout refusal", result)
	testassert.Equal(t, result.Evidence, harness.ExecutionNotStarted)
}

// TestReadOnlyProfileBackgroundTimeoutOverrideStillWorks proves background
// timeout semantics survive the read-only handling: the rewritten command is
// dispatched in the Session's Workspace, the model timeout is forwarded when
// at least one, and an absent or sub-one timeout becomes 0.
func TestReadOnlyProfileBackgroundTimeoutOverrideStillWorks(t *testing.T) {
	workspace := t.TempDir()
	r, e, jobs, session := openReadOnlyProfile(t, workspace)
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		switch lastUserText(body) {
		case "background one":
			return toolCallTurnEvents("call-one", "run_command", `{"command":"pwd","background":true}`)
		case "background five":
			return toolCallTurnEvents("call-five", "run_command", `{"command":"git status","background":true,"timeout":5}`)
		}
		return textTurnEvents("done")
	})
	submitThroughRuntime(t, r, session, "op-five", "background five")
	awaitOperation(t, r, session, "op-five", harness.OperationSuccess)
	want := "git --no-pager --no-optional-locks -c core.fsmonitor=false -c diff.external= -c core.pager=cat status"
	testassert.Require(t, len(jobs.started) == 1, "background starts = %d, want one", len(jobs.started))
	started := jobs.started[0]
	testassert.Require(t, started.Command == want && started.TimeoutSec == 5 && started.Workspace == workspace && started.SessionID == session, "background start = %+v, want the rewritten command, timeout 5, Workspace %q and Session %q", started, workspace, session)

	secondSession, err := r.createSession(context.Background(), workspace, "reader")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, secondSession.Identity.SessionID, "op-one", "background one")
	awaitOperation(t, r, secondSession.Identity.SessionID, "op-one", harness.OperationSuccess)
	testassert.Require(t, len(jobs.started) == 2, "background starts = %d, want two", len(jobs.started))
	second := jobs.started[1]
	testassert.Require(t, second.TimeoutSec == 0 && second.Command == "pwd", "second background start = %+v, want timeout 0 and pwd", second)
}

// TestReadOnlyProfilePlainCommandExecutes proves a plain allowlisted command
// executes in the Session Workspace.
func TestReadOnlyProfilePlainCommandExecutes(t *testing.T) {
	workspace := t.TempDir()
	r, e, _, session := openReadOnlyProfile(t, workspace)
	e.server.setScript(readOnlyCallScript(`{"command":"pwd"}`))
	submitThroughRuntime(t, r, session, "op-pwd", "run")
	awaitOperation(t, r, session, "op-pwd", harness.OperationSuccess)
	result := toolResultFact(t, r, session, "op-pwd")
	testassert.Require(t, result.Status == model.ResultSuccess && strings.TrimSpace(result.Content) == workspace, "tool result = %+v, want pwd in the Session Workspace %q", result, workspace)
}

// TestReadOnlyProfileRejectsNonAllowlistedAndBlank proves refused commands
// settle as the fixed authorization refusal and never execute.
func TestReadOnlyProfileRejectsNonAllowlistedAndBlank(t *testing.T) {
	for _, arguments := range []string{
		`{"command":"curl example.com"}`,
		`{"command":""}`,
		`{"command":"   "}`,
		"{\"command\":\" \\n\\t \"}",
	} {
		t.Run(arguments, func(t *testing.T) {
			workspace := t.TempDir()
			r, e, jobs, session := openReadOnlyProfile(t, workspace)
			e.server.setScript(readOnlyCallScript(arguments))
			submitThroughRuntime(t, r, session, "op-refused", "run")
			awaitOperation(t, r, session, "op-refused", harness.OperationSuccess)
			result := toolResultFact(t, r, session, "op-refused")
			testassert.Require(t, result.Status != model.ResultSuccess && result.Content == agents.ReadOnlyRunCommandRejected, "tool result = %+v, want the fixed read-only refusal", result)
			testassert.Equal(t, result.Evidence, harness.ExecutionNotStarted)
			testassert.Require(t, len(jobs.started) == 0, "refused command reached the jobs layer: %+v", jobs.started)
			testassert.Require(t, !strings.Contains(result.Content, "curl"), "refusal echoed the command: %+v", result)
		})
	}
}

// TestReadOnlyProfileGitRewriteUsesExactInjectedOptions proves every allowed
// git command dispatches the exact safe-git rewrite, suppressing the
// repository's configured content helpers.
func TestReadOnlyProfileGitRewriteUsesExactInjectedOptions(t *testing.T) {
	const prefix = "git --no-pager --no-optional-locks -c core.fsmonitor=false -c diff.external= -c core.pager=cat "
	for _, tc := range []struct {
		command string
		want    string
	}{
		{"git diff -- file.secret", prefix + "diff --no-ext-diff --no-textconv -- file.secret"},
		{"git log -p -1 -- file.secret", prefix + "log --no-ext-diff --no-textconv -p -1 -- file.secret"},
		{"git show HEAD -- file.secret", prefix + "show --no-ext-diff --no-textconv HEAD -- file.secret"},
		{"git show HEAD:file.secret", prefix + "show --no-ext-diff --no-textconv HEAD:file.secret"},
		{"git blame -- file.secret", prefix + "blame --no-ext-diff --no-textconv -- file.secret"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			repo := t.TempDir()
			runGit := func(args ...string) {
				t.Helper()
				cmd := exec.Command("git", args...)
				cmd.Dir = repo
				output, err := cmd.CombinedOutput()
				testassert.Require(t, err == nil, "git %v: %v\n%s", args, err, output)
			}
			runGit("init")
			runGit("config", "user.email", "test@example.invalid")
			runGit("config", "user.name", "Test User")
			marker := filepath.Join(repo, "helper-ran")
			textconv := filepath.Join(repo, "textconv.sh")
			external := filepath.Join(repo, "external.sh")
			writeScript := func(path, content string) {
				t.Helper()
				testassert.NoError(t, os.WriteFile(path, []byte(content), 0o755))
			}
			writeScript(textconv, fmt.Sprintf("#!/bin/sh\nprintf textconv >> %q\ncat \"$1\"\n", marker))
			writeScript(external, fmt.Sprintf("#!/bin/sh\nprintf external >> %q\nexit 0\n", marker))
			testassert.NoError(t, os.WriteFile(filepath.Join(repo, ".gitattributes"), []byte("*.secret diff=secret\n"), 0o644))
			testassert.NoError(t, os.WriteFile(filepath.Join(repo, "file.secret"), []byte("one\n"), 0o644))
			runGit("add", ".")
			runGit("commit", "-m", "initial")
			runGit("config", "diff.secret.textconv", textconv)
			runGit("config", "diff.external", external)
			testassert.NoError(t, os.WriteFile(filepath.Join(repo, "file.secret"), []byte("two\n"), 0o644))
			runGit("add", "file.secret")
			runGit("commit", "-m", "second")
			testassert.NoError(t, os.WriteFile(filepath.Join(repo, "file.secret"), []byte("three\n"), 0o644))

			var mu sync.Mutex
			authorized := ""
			gate := testNative("gate", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
				_, err := api.AttachToolCheck(func(_ context.Context, _ extension.AuthorContext, input model.ToolCall) (*string, error) {
					if input.Name == "run_command" {
						var args struct {
							Command string `json:"command"`
						}
						if json.Unmarshal(input.Arguments, &args) == nil {
							mu.Lock()
							authorized = args.Command
							mu.Unlock()
						}
					}
					return nil, nil
				}, nil)
				return nil, err
			})
			r, e, _, session := openReadOnlyProfile(t, repo, gate)
			e.server.setScript(readOnlyCallScript(fmt.Sprintf(`{"command":%q}`, tc.command)))
			submitThroughRuntime(t, r, session, "op-git", "run")
			awaitOperation(t, r, session, "op-git", harness.OperationSuccess)
			mu.Lock()
			got := authorized
			mu.Unlock()
			testassert.Require(t, got == tc.want, "authorized command = %q, want the exact rewrite %q", got, tc.want)
			if data, err := os.ReadFile(marker); err == nil {
				t.Fatalf("rewritten command ran content helpers: marker = %q", data)
			} else if !os.IsNotExist(err) {
				t.Fatalf("marker read error = %v", err)
			}
			result := toolResultFact(t, r, session, "op-git")
			testassert.Require(t, result.Status == model.ResultSuccess && result.Content != "", "tool result = %+v, want a non-empty rewritten command output", result)
		})
	}
}
