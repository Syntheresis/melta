package agents

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
)

type authorAPI struct {
	extension.API
	operations    map[string]extension.Operation
	admission     extension.AdmissionHook
	request       extension.AuthorRequestHook
	arguments     extension.ArgumentsHook
	authorization extension.AuthorizationHook
	settings      json.RawMessage
	service       extension.Service
	tool          extension.RegisteredTool
}

func (a *authorAPI) Settings() json.RawMessage {
	if a.settings != nil {
		return a.settings
	}
	return []byte(`{"enabled":true}`)
}
func (a *authorAPI) RegisterOperation(op extension.Operation, _ extension.Registration) (extension.Registration, error) {
	a.operations[op.Method.Name] = op
	return nil, nil
}
func (a *authorAPI) RegisterService(s extension.Service, _ extension.Registration) (extension.Registration, error) {
	a.service = s
	return nil, nil
}
func (a *authorAPI) RegisterTool(_ model.ToolDefinition, tool extension.RegisteredTool, _ extension.Registration) (extension.Registration, error) {
	a.tool = tool
	return nil, nil
}
func (a *authorAPI) AttachAdmission(h extension.AdmissionHook, _ extension.Registration) (extension.Registration, error) {
	a.admission = h
	return nil, nil
}
func (a *authorAPI) AttachRequest(_ extension.HookMode, h extension.AuthorRequestHook, _ extension.Registration) (extension.Registration, error) {
	a.request = h
	return nil, nil
}
func (a *authorAPI) AttachArguments(_ extension.HookMode, h extension.ArgumentsHook, _ extension.Registration) (extension.Registration, error) {
	a.arguments = h
	return nil, nil
}

func TestAuthorAdmissionChoices(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	empty := ""
	base := extension.AdmissionCandidate{Model: model.ModelRef{Provider: "host", Model: "m"}, Instructions: "incoming", Tools: []model.ToolDefinition{{Name: "read_file"}}}
	input := extension.AdmissionInput{Choices: harness.SessionInputs{Prompt: &empty, Tools: []string{}}, Candidate: base, ToolCatalog: base.Tools}
	result, err := api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"explore"}`)}, input)
	if err != nil || result.Candidate == nil || result.Candidate.Prompt != base.Prompt || result.Candidate.Instructions != "incoming" || len(result.Candidate.Tools) != 1 {
		t.Fatalf("explicit choices lost: %#v, %v", result, err)
	}
	result, err = api.admission(context.Background(), extension.AuthorContext{}, input)
	if err != nil || result.Candidate != nil {
		t.Fatalf("unselected changed candidate: %#v, %v", result, err)
	}
	input.Candidate.Tools = []model.ToolDefinition{}
	result, err = api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"explore"}`)}, input)
	if err != nil || result.Candidate == nil || result.Candidate.Tools == nil {
		t.Fatalf("explicit empty advertisement became absent: %#v, %v", result, err)
	}
}

func TestAuthorAdmissionUnavailableToolKeepsRestrictions(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true,"definitions":{"missing":{"tools":["ghost","run_command"],"readonly":true}}}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	call := extension.AuthorContext{State: []byte(`{"agent":"missing"}`)}
	empty := ""
	result, err := api.admission(context.Background(), call, extension.AdmissionInput{Choices: harness.SessionInputs{Prompt: &empty}, ToolCatalog: []model.ToolDefinition{{Name: "run_command"}}})
	if err != nil || result.Candidate == nil || len(result.Candidate.Tools) != 1 || result.Candidate.Tools[0].Name != "run_command" {
		t.Fatalf("available tools: %#v %v", result, err)
	}
	refusal, err := api.authorization(context.Background(), call, extension.AuthorizationInput{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: "/file"}}})
	if err != nil || refusal == nil {
		t.Fatalf("unavailable tool lifted write restriction: %v %v", refusal, err)
	}
	rewrite, err := api.arguments(context.Background(), call, model.ToolCall{Name: "run_command", Arguments: []byte(`{"command":"rm file"}`)})
	if err != nil || rewrite.Refusal == nil {
		t.Fatalf("unavailable tool lifted command restriction: %#v %v", rewrite, err)
	}
}

func TestAuthorAdmissionModelPrecedence(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true,"definitions":{"primary":{"model":"profile/b"}}}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	host := model.ModelRef{Provider: "host", Model: "a"}
	input := extension.AdmissionInput{Candidate: extension.AdmissionCandidate{Model: host}}
	result, err := api.admission(context.Background(), extension.AuthorContext{}, input)
	if err != nil || result.Candidate != nil {
		t.Fatalf("unselected model: %#v %v", result, err)
	}
	call := extension.AuthorContext{State: []byte(`{"agent":"primary"}`)}
	result, err = api.admission(context.Background(), call, input)
	if err != nil || result.Candidate == nil || result.Candidate.Model != (model.ModelRef{Provider: "profile", Model: "b"}) {
		t.Fatalf("profile model: %#v %v", result, err)
	}
	input.Choices.Model = &host
	result, err = api.admission(context.Background(), call, input)
	if err != nil || result.Candidate == nil || result.Candidate.Model != host {
		t.Fatalf("explicit model: %#v %v", result, err)
	}
	complete := "previous hook"
	input.Choices.Model = nil
	input.Candidate.Prompt = &complete
	result, err = api.admission(context.Background(), call, input)
	if err != nil || result.Candidate == nil || result.Candidate.Prompt != nil {
		t.Fatalf("profile must select default assembly: %#v %v", result, err)
	}
}

func TestAuthorTaskReadvertisement(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true,"definitions":{"worker":{"tools":["custom"],"subagent":true,"description":"custom worker"}}}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	input := extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{Purpose: harness.InvocationPurposeConversation, Tools: []model.ToolDefinition{{Name: "task", Description: "custom worker"}}}}
	call := extension.AuthorContext{}
	result, err := api.request(context.Background(), call, input)
	if err != nil || result.Tools != nil {
		t.Fatalf("advertised roster: %#v %v", result, err)
	}
	call.Session.OwnerSessionID = "owner"
	result, err = api.request(context.Background(), call, input)
	if err != nil || result.Tools == nil || len(result.Tools) != 0 {
		t.Fatalf("owned session: %#v %v", result, err)
	}
}
func (a *authorAPI) AttachAuthorization(h extension.AuthorizationHook, _ extension.Registration) (extension.Registration, error) {
	a.authorization = h
	return nil, nil
}

func TestAuthorReadonlyFinalCommand(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	call := extension.AuthorContext{State: []byte(`{"agent":"explore"}`)}
	result, err := api.arguments(context.Background(), call, model.ToolCall{Name: "run_command", Arguments: []byte(`{"command":"git status","timeout":17}`)})
	if err != nil || result.Refusal != nil || strings.Contains(string(result.Arguments), "timeout") {
		t.Fatalf("arguments: %#v %v", result, err)
	}
	var args map[string]any
	if err := json.Unmarshal(result.Arguments, &args); err != nil {
		t.Fatal(err)
	}
	args["timeout"], args["background"] = 120, false
	normalized, _ := json.Marshal(args)
	input := extension.AuthorizationInput{ToolName: "run_command", Arguments: normalized, ToolSettings: []byte(`{"command_timeout":120}`)}
	refusal, err := api.authorization(context.Background(), call, input)
	if err != nil || refusal != nil {
		t.Fatalf("safe command refused: %v %v", refusal, err)
	}
	args["timeout"] = 121
	input.Arguments, _ = json.Marshal(args)
	refusal, err = api.authorization(context.Background(), call, input)
	if err != nil || refusal == nil {
		t.Fatalf("changed timeout accepted: %v %v", refusal, err)
	}
	args["timeout"], args["command"] = 120, "rm file"
	input.Arguments, _ = json.Marshal(args)
	refusal, err = api.authorization(context.Background(), call, input)
	if err != nil || refusal == nil {
		t.Fatalf("changed script accepted: %v %v", refusal, err)
	}
	background, err := api.arguments(context.Background(), call, model.ToolCall{Name: "run_command", Arguments: []byte(`{"command":"git status","timeout":777,"background":true}`)})
	if err != nil || background.Refusal != nil || !strings.Contains(string(background.Arguments), `"timeout":777`) {
		t.Fatalf("background override lost: %#v %v", background, err)
	}
	input.Arguments = background.Arguments
	refusal, err = api.authorization(context.Background(), call, input)
	if err != nil || refusal != nil {
		t.Fatalf("background override refused: %v %v", refusal, err)
	}
}

func TestAuthorWriteRoot(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	call := extension.AuthorContext{State: []byte(`{"agent":"explore"}`)}
	input := extension.AuthorizationInput{ToolName: "other", Permissions: []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: "/workspace/file"}}}
	refusal, err := api.authorization(context.Background(), call, input)
	if err != nil || refusal == nil {
		t.Fatalf("read-only write = %v, %v", refusal, err)
	}
}

func TestAuthorPromptModesAndWriteRoot(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true,"definitions":{"writer":{"system_prompt":"simple","prompt":"work","write_dir":"allowed","readonly":true},"blank":{"system_prompt":"none","prompt":""},"full":{"system_prompt":"full","prompt":"full body"},"headed":{"system_prompt":"full","prompt":"# your-role-and-instructions\n\nalready headed"}}}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	reads := 0
	input := extension.AdmissionInput{Prompt: extension.PromptAccess{
		Sections: func() []prompt.Section {
			return []prompt.Section{{Name: "identity", Text: "identity"}, {Name: "core_rules", Text: "core"}, {Name: "rules_file_guide", Text: "omit"}, {Name: "compaction_awareness", Text: "compact"}}
		},
		Environment: func() string { return "environment" },
		Rules: func() (string, []prompt.Section, error) {
			reads++
			return "rules", []prompt.Section{{Name: "safety", Text: "safe"}}, nil
		},
	}}
	input.ToolCatalog = []model.ToolDefinition{{Name: "write_file"}, {Name: "run_command"}}
	input.Choices.Tools = []string{"write_file", "run_command"}
	input.Candidate.Tools = input.ToolCatalog
	result, err := api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"writer"}`)}, input)
	if err != nil || reads != 1 || result.Candidate == nil || result.Candidate.Prompt == nil || strings.Contains(*result.Candidate.Prompt, "omit") || !strings.Contains(*result.Candidate.Prompt, "## Your Role and Instructions\n\nwork") {
		t.Fatalf("simple prompt: %#v %v reads=%d", result, err, reads)
	}
	if len(result.Candidate.Tools) != 2 || result.Candidate.Tools[0].Name != "write_file" {
		t.Fatalf("write root hid mutations: %#v", result.Candidate.Tools)
	}
	result, err = api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"blank"}`)}, input)
	if err != nil || reads != 1 || result.Candidate.Prompt == nil || *result.Candidate.Prompt != "" {
		t.Fatalf("empty complete prompt: %#v %v reads=%d", result, err, reads)
	}
	result, err = api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"full"}`)}, input)
	if err != nil || reads != 1 || result.Candidate.Prompt != nil || result.Candidate.Instructions != "## Your Role and Instructions\n\nfull body" {
		t.Fatalf("full instructions: %#v %v reads=%d", result, err, reads)
	}
	result, err = api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"headed"}`)}, input)
	if err != nil || reads != 1 || result.Candidate.Prompt != nil || result.Candidate.Instructions != "# your-role-and-instructions\n\nalready headed" {
		t.Fatalf("already-headed instructions: %#v %v reads=%d", result, err, reads)
	}
	explicit := ""
	input.Choices.Instructions = &explicit
	input.Candidate.Instructions = explicit
	result, err = api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"writer"}`)}, input)
	if err != nil || reads != 1 || result.Candidate == nil || result.Candidate.Prompt != nil || result.Candidate.Instructions != "" {
		t.Fatalf("explicit empty instructions: %#v %v reads=%d", result, err, reads)
	}
	call := extension.AuthorContext{State: []byte(`{"agent":"writer"}`), Call: func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.workspace" || method != "bind" || string(args) != `{"path":"allowed"}` {
			t.Fatalf("unexpected binding: %s.%s %s", service, method, args)
		}
		return extension.Result{Value: []byte(`{"canonical_path":"/workspace/allowed"}`)}, nil
	}}
	action := extension.AuthorizationInput{ToolName: "other", Permissions: []harness.PermissionRequest{{Permission: harness.PermissionFileWrite, Target: "/workspace/allowed/a"}}}
	refusal, err := api.authorization(context.Background(), call, action)
	if err != nil || refusal != nil {
		t.Fatalf("inside root: %v %v", refusal, err)
	}
	action.Permissions[0].Target = "/workspace/allowed-sibling/a"
	refusal, err = api.authorization(context.Background(), call, action)
	if err != nil || refusal == nil {
		t.Fatalf("outside root: %v %v", refusal, err)
	}
	rewrite, err := api.arguments(context.Background(), call, model.ToolCall{Name: "run_command", Arguments: []byte(`{"command":"rm file"}`)})
	if err != nil || rewrite.Refusal == nil {
		t.Fatalf("write root lifted command restriction: %#v %v", rewrite, err)
	}
}

func TestAuthorOperations(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"list", "current", "select", "create", "set_model"} {
		method, err := extension.CompileMethod(api.operations[name].Method)
		if err != nil {
			t.Fatalf("%s schema: %v", name, err)
		}
		if err := method.ValidateArguments([]byte(`{"unexpected":true}`)); err == nil {
			t.Fatalf("%s accepts extra fields", name)
		}
	}
	current := api.operations["current"].Handler
	result, err := current(context.Background(), extension.AuthorContext{}, []byte(`{}`))
	if err != nil || string(result.Value) != `{}` {
		t.Fatalf("current = %s, %v", result.Value, err)
	}
	call := extension.AuthorContext{Call: func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.state" || method != "set" || string(args) != `{"value":{"agent":"explore"}}` {
			t.Fatalf("state call = %s.%s %s", service, method, args)
		}
		return extension.Result{Value: []byte("null")}, nil
	}}
	result, err = api.operations["select"].Handler(context.Background(), call, []byte(`{"agent":"explore"}`))
	if err != nil || string(result.Value) != "null" {
		t.Fatalf("select = %s, %v", result.Value, err)
	}
	if _, err := api.operations["select"].Handler(context.Background(), call, []byte(`{"agent":"unknown"}`)); err == nil {
		t.Fatal("unknown agent accepted")
	}
	call.Session.Workspace = "/workspace"
	call.Call = func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.commands" || method != "create_session" || !strings.Contains(string(args), `"initial_state":{"agent":"primary"}`) || !strings.Contains(string(args), `"workspace":"/workspace"`) {
			t.Fatalf("create call = %s.%s %s", service, method, args)
		}
		return extension.Result{Value: []byte(`{"session_id":"created"}`)}, nil
	}
	result, err = api.operations["create"].Handler(context.Background(), call, []byte(`{}`))
	if err != nil || string(result.Value) != `{"session_id":"created"}` {
		t.Fatalf("create = %s, %v", result.Value, err)
	}
}

func TestAuthorModelUpdateTransform(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	call := extension.AuthorContext{Call: func(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.settings" || method != "update" {
			t.Fatalf("update = %s.%s", service, method)
		}
		var request struct {
			Service   string          `json:"service"`
			Method    string          `json:"method"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(args, &request); err != nil || request.Service != ConfigurationCapabilityID || request.Method != "set_model" {
			t.Fatalf("update args = %s %v", args, err)
		}
		transform, _ := json.Marshal(struct {
			Section   json.RawMessage `json:"section"`
			Arguments json.RawMessage `json:"arguments"`
		}{[]byte(`{"enabled":true,"other":42}`), request.Arguments})
		result, err := api.service.Methods[0].Handler(ctx, extension.AuthorContext{}, transform)
		if err != nil || !strings.Contains(string(result.Value), `"model":"p/m"`) || !strings.Contains(string(result.Value), `"other":42`) {
			t.Fatalf("transform = %s %v", result.Value, err)
		}
		return extension.Result{Value: []byte("null")}, nil
	}}
	result, err := api.operations["set_model"].Handler(context.Background(), call, []byte(`{"definition_id":"primary","model":"p/m"}`))
	if err != nil || string(result.Value) != "null" {
		t.Fatalf("set model = %s %v", result.Value, err)
	}
}

// TestPromptModesLiteralOutput pins the exact text each system-prompt mode
// supplies: full hands the default assembler one already-headed Instructions
// block, while simple and none carry their complete prompt literally.
func TestPromptModesLiteralOutput(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true,"definitions":{"full":{"system_prompt":"full","prompt":"role body"},"simple":{"system_prompt":"simple","prompt":"role body"},"none":{"system_prompt":"none","prompt":"role body"}}}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	input := extension.AdmissionInput{Prompt: extension.PromptAccess{
		Sections: func() []prompt.Section {
			return []prompt.Section{{Name: "identity", Text: "identity"}, {Name: "core_rules", Text: "core"}, {Name: "rules_file_guide", Text: "omit"}, {Name: "compaction_awareness", Text: "compact"}}
		},
		Environment: func() string { return "environment" },
		Rules: func() (string, []prompt.Section, error) {
			return "rules", []prompt.Section{{Name: "safety", Text: "safe"}}, nil
		},
	}}
	full, err := api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"full"}`)}, input)
	if err != nil || full.Candidate == nil || full.Candidate.Prompt != nil || full.Candidate.Instructions != "## Your Role and Instructions\n\nrole body" {
		t.Fatalf("full = %#v, %v", full, err)
	}
	assembled := prompt.Assemble(prompt.AssemblyInput{Instructions: full.Candidate.Instructions})
	if strings.Count(assembled, "## Your Role and Instructions") != 1 {
		t.Fatalf("assembled full prompt heading count = %d, want 1\n%s", strings.Count(assembled, "## Your Role and Instructions"), assembled)
	}
	simple, err := api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"simple"}`)}, input)
	if err != nil || simple.Candidate == nil || simple.Candidate.Prompt == nil || *simple.Candidate.Prompt != "identity\n\ncore\n\ncompact\n\nenvironment\n\nsafe\n\n## Your Role and Instructions\n\nrole body\n\nrules" {
		t.Fatalf("simple = %#v, %v", simple, err)
	}
	none, err := api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"none"}`)}, input)
	if err != nil || none.Candidate == nil || none.Candidate.Prompt == nil || *none.Candidate.Prompt != "role body" {
		t.Fatalf("none = %#v, %v", none, err)
	}
}
