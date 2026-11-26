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
	operations map[string]extension.Operation
	admission  extension.AdmissionHook
	request    extension.AuthorRequestHook
	arguments  extension.ArgumentsHook
	check      extension.ToolCheckHook
	settings   json.RawMessage
	service    extension.Service
	tool       extension.ToolFunc
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
func (a *authorAPI) RegisterTool(_ model.ToolDefinition, tool extension.ToolFunc, _ extension.Registration) (extension.Registration, error) {
	a.tool = tool
	return nil, nil
}
func (a *authorAPI) AttachAdmission(h extension.AdmissionHook, _ extension.Registration) (extension.Registration, error) {
	a.admission = h
	return nil, nil
}
func (a *authorAPI) AttachRequest(h extension.AuthorRequestHook, _ extension.Registration) (extension.Registration, error) {
	a.request = h
	return nil, nil
}
func (a *authorAPI) AttachArguments(h extension.ArgumentsHook, _ extension.Registration) (extension.Registration, error) {
	a.arguments = h
	return nil, nil
}

func TestAuthorAdmissionAppliesDefinition(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true,"definitions":{"writer":{"model":"profile/b","system_prompt":"none","prompt":"","tools":["read_file"]},"plain":{"system_prompt":"full"}}}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	base := extension.AdmissionCandidate{Model: model.ModelRef{Provider: "host", Model: "m"}, Instructions: "incoming", Tools: []model.ToolDefinition{{Name: "read_file"}, {Name: "write_file"}}}
	input := extension.AdmissionInput{Candidate: base, ToolCatalog: base.Tools}
	result, err := api.admission(context.Background(), extension.AuthorContext{}, input)
	if err != nil || result.Candidate != nil {
		t.Fatalf("unselected changed candidate: %#v, %v", result, err)
	}
	result, err = api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"writer"}`)}, input)
	if err != nil || result.Candidate == nil || result.Candidate.Model != (model.ModelRef{Provider: "profile", Model: "b"}) ||
		result.Candidate.Prompt == nil || *result.Candidate.Prompt != "" || len(result.Candidate.Tools) != 1 || result.Candidate.Tools[0].Name != "read_file" {
		t.Fatalf("definition over candidate: %#v, %v", result, err)
	}
	result, err = api.admission(context.Background(), extension.AuthorContext{State: []byte(`{"agent":"plain"}`)}, input)
	if err != nil || result.Candidate == nil || result.Candidate.Model != base.Model || len(result.Candidate.Tools) != len(base.Tools) {
		t.Fatalf("definition without model or tools changed the candidate: %#v, %v", result, err)
	}
}

func TestAuthorAdmissionUnavailableToolKeepsRestrictions(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true,"definitions":{"missing":{"tools":["ghost","run_command"],"readonly":true}}}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	call := extension.AuthorContext{State: []byte(`{"agent":"missing"}`)}
	result, err := api.admission(context.Background(), call, extension.AdmissionInput{ToolCatalog: []model.ToolDefinition{{Name: "run_command"}}, Prompt: extension.PromptAccess{
		Sections:    func() []prompt.Section { return nil },
		Environment: func() string { return "" },
		Rules:       func() (string, []prompt.Section, error) { return "", nil, nil },
	}})
	if err != nil || result.Candidate == nil || len(result.Candidate.Tools) != 1 || result.Candidate.Tools[0].Name != "run_command" {
		t.Fatalf("available tools: %#v %v", result, err)
	}
	refusal, err := api.check(context.Background(), call, model.ToolCall{Name: "write_file", Arguments: []byte(`{"path":"/file"}`)})
	if err != nil || refusal == nil {
		t.Fatalf("unavailable tool lifted write restriction: %v %v", refusal, err)
	}
	rewrite, err := api.arguments(context.Background(), call, model.ToolCall{Name: "run_command", Arguments: []byte(`{"command":"rm file"}`)})
	if err != nil || rewrite.Refusal == nil {
		t.Fatalf("unavailable tool lifted command restriction: %#v %v", rewrite, err)
	}
}

func TestAuthorTaskReadvertisement(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}, settings: []byte(`{"enabled":true}`)}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	call := extension.AuthorContext{Session: harness.SessionIdentity{OwnerSessionID: "owner"}, Call: func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
		t.Fatal("owned-session task started a Session")
		return extension.Result{}, nil
	}}
	input := extension.AdmissionInput{Session: harness.SessionIdentity{OwnerSessionID: "owner"}, Candidate: extension.AdmissionCandidate{Tools: []model.ToolDefinition{{Name: TaskCapabilityID}}}, ToolCatalog: []model.ToolDefinition{{Name: TaskCapabilityID}}}
	admitted, err := api.admission(context.Background(), call, input)
	if err != nil || admitted.Candidate == nil || len(admitted.Candidate.Tools) != 0 {
		t.Fatalf("owned-session admission = %#v, %v, want task removed", admitted, err)
	}
	_, err = api.tool(context.Background(), extension.ToolCall{AuthorContext: call, ID: "call", Name: TaskCapabilityID, Arguments: []byte(`{"prompt":"work","subagent_type":"secondary"}`)})
	if err == nil || !strings.Contains(err.Error(), "task is unavailable in an owned session") {
		t.Fatalf("re-advertised task error = %v, want the owned-session refusal", err)
	}
	if api.request != nil {
		t.Fatal("Agents still rewrites the advertised tool list")
	}
}
func (a *authorAPI) AttachToolCheck(h extension.ToolCheckHook, _ extension.Registration) (extension.Registration, error) {
	a.check = h
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
	args["background"] = false
	normalized, _ := json.Marshal(args)
	input := model.ToolCall{Name: "run_command", Arguments: normalized}
	refusal, err := api.check(context.Background(), call, input)
	if err != nil || refusal != nil {
		t.Fatalf("safe command refused: %v %v", refusal, err)
	}
	args["timeout"] = 121
	input.Arguments, _ = json.Marshal(args)
	refusal, err = api.check(context.Background(), call, input)
	if err != nil || refusal == nil {
		t.Fatalf("changed timeout accepted: %v %v", refusal, err)
	}
	delete(args, "timeout")
	args["command"] = "rm file"
	input.Arguments, _ = json.Marshal(args)
	refusal, err = api.check(context.Background(), call, input)
	if err != nil || refusal == nil {
		t.Fatalf("changed script accepted: %v %v", refusal, err)
	}
	background, err := api.arguments(context.Background(), call, model.ToolCall{Name: "run_command", Arguments: []byte(`{"command":"git status","timeout":777,"background":true}`)})
	if err != nil || background.Refusal != nil || !strings.Contains(string(background.Arguments), `"timeout":777`) {
		t.Fatalf("background override lost: %#v %v", background, err)
	}
	input.Arguments = background.Arguments
	refusal, err = api.check(context.Background(), call, input)
	if err != nil || refusal != nil {
		t.Fatalf("background override refused: %v %v", refusal, err)
	}
}

func TestAuthorWriteRoot(t *testing.T) {
	api := &authorAPI{operations: map[string]extension.Operation{}}
	if _, err := Author().Activate(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	calls := 0
	call := extension.AuthorContext{State: []byte(`{"agent":"explore"}`), Call: func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
		calls++
		return extension.Result{}, nil
	}}
	input := model.ToolCall{Name: "write_file", Arguments: []byte(`{"path":"/workspace/file"}`)}
	refusal, err := api.check(context.Background(), call, input)
	if err != nil || refusal == nil || *refusal != "read-only agent cannot write files" || calls != 0 {
		t.Fatalf("read-only write = %v, %v; host calls = %d", refusal, err, calls)
	}
	call.State = []byte(`{"agent":"primary"}`)
	refusal, err = api.check(context.Background(), call, input)
	if err != nil || refusal != nil || calls != 0 {
		t.Fatalf("unrestricted write = %v, %v; host calls = %d", refusal, err, calls)
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
	call := extension.AuthorContext{State: []byte(`{"agent":"writer"}`), Call: func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.workspace" || method != "stat" {
			t.Fatalf("unexpected binding: %s.%s %s", service, method, args)
		}
		var input struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			t.Fatal(err)
		}
		if input.Path == "allowed" {
			input.Path = "/workspace/allowed"
		}
		value, _ := json.Marshal(map[string]string{"canonical_path": input.Path})
		return extension.Result{Value: value}, nil
	}}
	action := model.ToolCall{Name: "write_file", Arguments: []byte(`{"path":"/workspace/allowed/a"}`)}
	refusal, err := api.check(context.Background(), call, action)
	if err != nil || refusal != nil {
		t.Fatalf("inside root: %v %v", refusal, err)
	}
	action.Arguments = []byte(`{"path":"/workspace/allowed-sibling/a"}`)
	refusal, err = api.check(context.Background(), call, action)
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
	call := extension.AuthorContext{Settings: []byte(`{"enabled":true,"other":42}`), Call: func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.settings" || method != "replace" {
			t.Fatalf("replace = %s.%s", service, method)
		}
		var request struct {
			Section json.RawMessage `json:"section"`
		}
		if err := json.Unmarshal(args, &request); err != nil {
			t.Fatalf("replace args = %s %v", args, err)
		}
		want := `{"definitions":{"primary":{"model":"p/m"}},"enabled":true,"other":42}`
		if string(request.Section) != want {
			t.Fatalf("replace section = %s, want %s", request.Section, want)
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
