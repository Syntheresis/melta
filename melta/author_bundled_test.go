package melta

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/adaptation"
	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
)

// TestBuiltinPrimaryKeepsRegisteredTools proves the built-in primary leaves
// the candidate's tool set unchanged, so a tool registered by another
// extension is advertised.
func TestBuiltinPrimaryKeepsRegisteredTools(t *testing.T) {
	e := newOwnerEnv(t)
	writeAgentDefinitions(t, e.configPath, `{}`)
	e.fixtureStore(storage.NewMemory())
	e.server.setScript(func(_ context.Context, _ string) []string { return textTurnEvents("done") })
	probe := extension.Extension{Manifest: registrarManifest("third-party"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("third_party_tool"), testRegisteredTool{}, nil)
		return nil, err
	}}
	enableFixturePlugin(t, e.configPath, "third-party")
	ctx := context.Background()
	r, err := e.open(ctx, probe)
	testassert.NoError(t, err)
	defer func() { _ = r.Close(ctx) }()
	session, err := r.createSession(ctx, e.dataDir, "primary")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-third", "hello")
	record := awaitOperation(t, r, session.Identity.SessionID, "op-third", harness.OperationSuccess)
	testassert.Require(t, slices.Contains(record.Admission.Execution.InitialToolNames, "third_party_tool"), "admitted tools = %v, want the third-party tool", record.Admission.Execution.InitialToolNames)
}

func TestBundledAuthorNativeWorkerParity(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "bundled")
	command := exec.Command("go", "build", "-o", binary, "./testdata/authorbundled")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, output)
	}
	for _, entry := range []extension.Extension{agents.Author(), adaptation.Author()} {
		for _, worker := range []bool{false, true} {
			t.Run(entry.Manifest.ID+map[bool]string{false: "/native", true: "/worker"}[worker], func(t *testing.T) {
				selected := entry
				if worker {
					selected = executableAuthorExtension(entry.Manifest, binary, func(context.Context, []string) ([]string, error) {
						return append(os.Environ(), "BUNDLED_EXTENSION="+entry.Manifest.ID), nil
					})
				}
				owner := &configurationService{}
				var section []byte
				if entry.Manifest.ID == "agents" {
					section = []byte(`{"enabled":true,"definitions":{"guard":{"tools":["missing","run_command"],"readonly":true,"description":"guard agent"}}}`)
				} else {
					section = []byte(`{"enabled":true,"entries":[{"pattern":"configured","append":["custom"]}]}`)
				}
				candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: selected, section: section, installed: worker}})
				if err != nil || candidate.inactive[entry.Manifest.ID] != nil {
					t.Fatalf("activation: %v %v", err, candidate.inactive)
				}
				defer candidate.release()
				stage := candidate.snapshotForTest()
				defer stage.release()
				if entry.Manifest.ID == "adaptation" {
					hook := stage.hooks["request"][0].value.(nativeHook).handler.(extension.AuthorRequestHook)
					result, err := hook(context.Background(), extension.AuthorContext{}, extension.AuthorRequestInput{PromptAssembled: true, RequestHookInput: harness.RequestHookInput{Purpose: harness.InvocationPurposeConversation, Model: model.ModelRef{Provider: "p", Model: "gpt-5.5"}, Prompt: "base", Tools: []model.ToolDefinition{{Name: "write_file"}, {Name: "read_file"}}}})
					if err != nil || result.Prompt == nil || *result.Prompt == "base" || len(result.Tools) != 1 || result.Tools[0].Name != "read_file" {
						t.Fatalf("adaptation: %#v %v", result, err)
					}
					summary, err := hook(context.Background(), extension.AuthorContext{}, extension.AuthorRequestInput{PromptAssembled: true, RequestHookInput: harness.RequestHookInput{Purpose: harness.InvocationPurposeMaintenance, Model: model.ModelRef{Provider: "p", Model: "gpt-5.5"}}})
					if err != nil || summary.Prompt != nil || summary.Tools != nil {
						t.Fatalf("summary: %#v %v", summary, err)
					}
					composed, err := hook(context.Background(), extension.AuthorContext{}, extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{Purpose: harness.InvocationPurposeConversation, Model: model.ModelRef{Provider: "p", Model: "gpt-5.5"}, Prompt: "complete", Catalog: []model.ToolDefinition{{Name: "write_file"}, {Name: "read_file"}, {Name: "hidden"}}, Tools: []model.ToolDefinition{{Name: "read_file"}}}})
					if err != nil || composed.Prompt != nil || len(composed.Tools) != 1 || composed.Tools[0].Name != "read_file" {
						t.Fatalf("complete prompt and hidden tools: %#v %v", composed, err)
					}
					custom, err := hook(context.Background(), extension.AuthorContext{}, extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{Purpose: harness.InvocationPurposeConversation, Model: model.ModelRef{Provider: "p", Model: "configured"}, Prompt: "complete"}})
					if err != nil || custom.Prompt == nil || *custom.Prompt != "complete\n\ncustom" {
						t.Fatalf("configured complete prompt: %#v %v", custom, err)
					}
					return
				}
				op := stage.operations[nativeOperationKey{"agents", "create"}].value.(struct {
					target extension.ExposedTargetKind
					method nativeServiceMethod
				}).method
				call := extension.AuthorContext{Session: harness.SessionIdentity{Workspace: "/workspace"}, Call: func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
					if service != "melta.commands" || method != "create_session" || string(args) != `{"workspace":"/workspace","initial_state":{"agent":"primary"}}` {
						t.Fatalf("host call: %s.%s %s", service, method, args)
					}
					return extension.Result{Value: []byte(`{"session_id":"created"}`)}, nil
				}}
				result, err := op.handler(context.Background(), call, []byte(`{}`))
				if err != nil || string(result.Value) != `{"session_id":"created"}` {
					t.Fatalf("create: %s %v", result.Value, err)
				}
				call.Call = func(_ context.Context, _, _ string, _ json.RawMessage) (extension.Result, error) {
					return extension.Result{}, &extension.CallError{Err: errors.New("failed after create"), Partial: []byte(`{"session_id":"created"}`)}
				}
				_, err = op.handler(context.Background(), call, []byte(`{}`))
				var partial *extension.CallError
				if !errors.As(err, &partial) || string(partial.Partial) != `{"session_id":"created"}` {
					t.Fatalf("create partial: %v", err)
				}
				selectOp := stage.operations[nativeOperationKey{"agents", "select"}].value.(struct {
					target extension.ExposedTargetKind
					method nativeServiceMethod
				}).method
				call.Call = func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
					if service != "melta.state" || method != "set" || string(args) != `{"value":{"agent":"explore"}}` {
						t.Fatalf("selection call: %s.%s %s", service, method, args)
					}
					return extension.Result{Value: []byte(`null`)}, nil
				}
				result, err = selectOp.handler(context.Background(), call, []byte(`{"agent":"explore"}`))
				if err != nil || string(result.Value) != `null` {
					t.Fatalf("selection: %s %v", result.Value, err)
				}
				call.State = []byte(`{"agent":"explore"}`)
				catalog := []model.ToolDefinition{{Name: "run_command", Parameters: []byte(`{}`)}, {Name: "task", Parameters: []byte(`{}`)}}
				admit := stage.hooks["admission"][0].value.(nativeHook).handler.(extension.AdmissionHook)
				empty := ""
				access := extension.PromptAccess{
					Sections:    func() []prompt.Section { return nil },
					Environment: func() string { return "" },
					Rules:       func() (string, []prompt.Section, error) { return "", nil, nil },
				}
				admitted, err := admit(context.Background(), call, extension.AdmissionInput{Candidate: extension.AdmissionCandidate{Model: model.ModelRef{Provider: "p", Model: "m"}, Prompt: &empty, Tools: []model.ToolDefinition{}}, ToolCatalog: catalog, Prompt: access})
				if err != nil || admitted.Candidate == nil || admitted.Candidate.Prompt == nil || len(admitted.Candidate.Tools) != 2 || admitted.Candidate.Tools[0].Name != "run_command" || admitted.Candidate.Tools[1].Name != "task" {
					t.Fatalf("explore admission: %#v %v", admitted, err)
				}
				arguments := stage.hooks["arguments"][0].value.(nativeHook).handler.(extension.ArgumentsHook)
				call.State = []byte(`{"agent":"guard"}`)
				guarded, err := admit(context.Background(), call, extension.AdmissionInput{Candidate: extension.AdmissionCandidate{Model: model.ModelRef{Provider: "p", Model: "m"}, Prompt: &empty}, ToolCatalog: catalog, Prompt: access})
				if err != nil || guarded.Candidate == nil || len(guarded.Candidate.Tools) != 1 || guarded.Candidate.Tools[0].Name != "run_command" {
					t.Fatalf("missing tool admission: %#v %v", guarded, err)
				}
				denied, err := arguments(context.Background(), call, model.ToolCall{ID: "cmd", Name: "run_command", Arguments: []byte(`{"command":"rm file"}`)})
				if err != nil || denied.Refusal == nil {
					t.Fatalf("missing tool command restriction: %#v %v", denied, err)
				}
				auth := stage.hooks["authorization"][0].value.(nativeHook).handler.(extension.AuthorizationHook)
				refusal, err := auth(context.Background(), call, extension.AuthorizationInput{ToolName: "other", Permissions: []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: "/workspace/file"}}})
				if err != nil || refusal == nil {
					t.Fatalf("missing tool write restriction: %v %v", refusal, err)
				}
				call.State = []byte(`{"agent":"explore"}`)
				rewrite, err := arguments(context.Background(), call, model.ToolCall{ID: "cmd", Name: "run_command", Arguments: []byte(`{"command":"git status","timeout":4}`)})
				if err != nil || rewrite.Refusal != nil {
					t.Fatalf("rewrite: %#v %v", rewrite, err)
				}
				var normalized map[string]any
				if err := json.Unmarshal(rewrite.Arguments, &normalized); err != nil {
					t.Fatal(err)
				}
				normalized["timeout"], normalized["background"] = 120, false
				raw, _ := json.Marshal(normalized)
				authorize := stage.hooks["authorization"][0].value.(nativeHook).handler.(extension.AuthorizationHook)
				input := extension.AuthorizationInput{ToolName: "run_command", Arguments: raw, ToolSettings: []byte(`{"command_timeout":120}`)}
				reason, err := authorize(context.Background(), call, input)
				if err != nil || reason != nil {
					t.Fatalf("authorization: %v %v", reason, err)
				}
				normalized["timeout"] = 121
				input.Arguments, _ = json.Marshal(normalized)
				reason, err = authorize(context.Background(), call, input)
				if err != nil || reason == nil {
					t.Fatalf("changed timeout accepted: %v %v", reason, err)
				}
				task := stage.bindTool("task")
				if task == nil {
					t.Fatal("task not registered")
				}
				defer task.release()
				if !strings.Contains(task.entry.value.(nativeTool).definition.Description, "- guard: guard agent") {
					t.Fatal("task roster omitted guard")
				}
				impl := task.entry.value.(nativeTool).tool
				call.Call = func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
					if service != "melta.session" || string(args) != "{}" {
						t.Fatalf("task host call: %s.%s %s", service, method, args)
					}
					switch method {
					case "members":
						return extension.Result{Value: []byte(`[]`)}, nil
					case "start":
						return extension.Result{Value: []byte(`{"session_id":"child"}`)}, nil
					default:
						t.Fatalf("unexpected task method: %s", method)
						return extension.Result{}, nil
					}
				}
				plan := impl.Prepare(context.Background(), call, model.ToolCall{ID: "task-call", Name: "task", Arguments: []byte(`{"subagent_type":"explore","prompt":"work"}`)})
				if plan.Immediate != nil || plan.Start == nil || plan.Start.Target != "explore" || string(plan.Start.InitialState) != `{"agent":"explore"}` {
					t.Fatalf("task plan: %#v", plan)
				}
				guardPlan := impl.Prepare(context.Background(), call, model.ToolCall{ID: "guard-call", Name: "task", Arguments: []byte(`{"subagent_type":"guard","prompt":"work"}`)})
				if guardPlan.Immediate != nil || guardPlan.Start == nil || guardPlan.Start.Target != "guard" {
					t.Fatalf("task roster disagreement: %#v", guardPlan)
				}
				outcome := impl.Execute(context.Background(), call, plan)
				if outcome.Result.Status != model.ResultSuccess {
					t.Fatalf("task outcome: %#v", outcome)
				}
				call.Call = func(_ context.Context, _, _ string, _ json.RawMessage) (extension.Result, error) {
					return extension.Result{}, &extension.CallError{Err: errors.New("failed after start"), Partial: []byte(`{"session_id":"child"}`)}
				}
				outcome = impl.Execute(context.Background(), call, plan)
				if outcome.Result.Status != model.ResultError || !strings.Contains(outcome.Result.Content, "child") {
					t.Fatalf("task partial: %#v", outcome)
				}
			})
		}
	}
}

func TestAgentsRegisteredWriteWitness(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "allowed"), 0700); err != nil {
		t.Fatal(err)
	}
	owner := &configurationService{}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: agents.Author(), section: []byte(`{"enabled":true,"definitions":{"writer":{"write_dir":"allowed"}}}`)}})
	if err != nil || candidate.inactive["agents"] != nil {
		t.Fatalf("activation: %v %v", err, candidate.inactive)
	}
	defer candidate.release()
	stage := candidate.snapshotForTest()
	defer stage.release()
	hooks := stage.authorizationHooks(ToolContext{Workspace: root}, map[string]harness.SessionStateValue{"agents": []byte(`{"agent":"writer"}`)}, nil, nil, nil, nil)
	binds := 0
	bind := func(path string) (string, error) {
		binds++
		return filepath.Join(root, path), nil
	}
	action := harness.PreparedAction{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: filepath.Join(root, "allowed", "file")}}}
	refusal, err := hooks[0].Run(context.Background(), action, bind)
	if err != nil || refusal != nil || binds != 1 {
		t.Fatalf("inside: %v %v binds=%d", refusal, err, binds)
	}
	action.Permissions[0].Target = filepath.Join(root, "elsewhere")
	refusal, err = hooks[0].Run(context.Background(), action, bind)
	if err != nil || refusal == nil {
		t.Fatalf("outside: %v %v", refusal, err)
	}
}
