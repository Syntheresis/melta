package runtime

// The shipped Runtime exercised through its public HTTP API: the
// shipped extension distribution is exercised as neither extension, each
// alone, and both together, plus one extension composed only from the public
// extension contract that registers, serves and retires a tool. Every
// composition runs the real owner, the real generated client and a scripted
// OpenAI-compatible endpoint.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/plugins/builtin"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/melta"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// shippedModelServer is the scripted OpenAI-compatible endpoint of the
// shipped compositions: every request body is recorded in arrival order
// and each scenario decides the streamed turn.
type shippedModelServer struct {
	*httptest.Server

	mu      sync.Mutex
	bodies  []string
	respond func(w http.ResponseWriter, body string)
}

func newShippedModelServer(t *testing.T) *shippedModelServer {
	t.Helper()
	server := &shippedModelServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(server.serve))
	t.Cleanup(server.Close)
	return server
}

func (s *shippedModelServer) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	body := string(raw)
	s.mu.Lock()
	s.bodies = append(s.bodies, body)
	respond := s.respond
	s.mu.Unlock()
	if respond != nil {
		respond(w, body)
		return
	}
	shippedTextTurn(w, "done")
}

func (s *shippedModelServer) setRespond(respond func(w http.ResponseWriter, body string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respond = respond
}

func (s *shippedModelServer) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.bodies...)
}

// shippedPlugins selects one composition of the shipped distribution list.
func shippedPlugins(ids ...string) []extension.Extension {
	selected := make(map[string]bool, len(ids))
	for _, id := range ids {
		selected[id] = true
	}
	plugins := make([]extension.Extension, 0, len(ids))
	for _, plugin := range builtin.Plugins() {
		if selected[plugin.Manifest.ID] {
			plugins = append(plugins, plugin)
		}
	}
	return plugins
}

// shippedDocument composes the provider endpoint and, when given, the
// installed extensions' sections.
func shippedDocument(endpoint string, models map[string]string, extensions map[string]any) []byte {
	rows := make(map[string]any, len(models))
	for id, name := range models {
		rows[id] = map[string]any{"name": name, "context_window": 65536}
	}
	document := map[string]any{
		"providers": map[string]any{
			"prov": map[string]any{
				"transport": map[string]any{"base_url": endpoint, "api_key_env": ""},
				"discovery": false,
				"models":    rows,
			},
		},
	}
	if extensions != nil {
		document["extensions"] = extensions
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return encoded
}

// shippedMount opens one real owner with its mounted authenticated HTTP
// host and generated client.
func shippedMount(t *testing.T, ctx context.Context, dataDir string, document []byte, extensions []extension.Extension) (*melta.Melta, *protocol.ClientWithResponses) {
	t.Helper()
	options := melta.Options{
		DataDir:       dataDir,
		Configuration: melta.NewValueConfigurationSource(document),
		Credentials:   melta.NewEnvironmentCredentialSource(filepath.Join(dataDir, ".env")),
		Extensions:    extensions,
	}
	owner, _, server := mountedOwner(t, ctx, options)
	return owner, mountedClient(t, server)
}

// shippedSubmit admits one regular message and waits for its Operation.
func shippedSubmit(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, sessionID, operationID, text string) {
	t.Helper()
	submitted, err := client.SubmitSessionWithResponse(ctx, sessionID, protocol.SubmitRequest{
		OperationId: operationID,
		Mode:        protocol.SubmitRequestModeRegular,
		Content:     []protocol.ContentPart{baselineTextPart(t, text)},
	})
	testassert.Require(t, err == nil && submitted.JSON200 != nil && submitted.JSON200.Operation != nil && submitted.JSON200.Operation.OperationId == operationID, "SubmitSession(%s) = (%+v, %v)", operationID, submitted, err)
	awaitMountedSuccess(t, ctx, client, sessionID, operationID)
}

// shippedSelectModel stores one Session model choice.
func shippedSelectModel(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, sessionID, ref string) {
	t.Helper()
	selected, err := client.SetSessionModelWithResponse(ctx, sessionID, protocol.SetSessionModelRequest{Model: protocol.ModelRef(ref)})
	testassert.Require(t, err == nil && selected.JSON200 != nil, "SetSessionModel(%s) = (%+v, %v)", ref, selected, err)
}

// shippedCreateSession creates one ordinary Session on a Workspace.
func shippedCreateSession(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, workspace string) string {
	t.Helper()
	created, err := client.CreateSessionWithResponse(ctx, protocol.CreateSessionRequest{Workspace: workspace})
	testassert.Require(t, err == nil && created.JSON201 != nil, "CreateSession = (%+v, %v)", created, err)
	return created.JSON201.SessionId
}

// shippedHydrate reads one Session's complete hydration.
func shippedHydrate(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, sessionID string) protocol.Hydration {
	t.Helper()
	hydration, err := client.GetSessionHydrationWithResponse(ctx, sessionID)
	testassert.Require(t, err == nil && hydration.JSON200 != nil, "GetSessionHydration(%s) = (%+v, %v)", sessionID, hydration, err)
	return *hydration.JSON200
}

// shippedAssistant returns the committed assistant item owned by one
// Operation.
func shippedAssistant(t *testing.T, items []protocol.ConversationItem, operationID string) protocol.AssistantItem {
	t.Helper()
	for _, item := range items {
		kind, err := item.Discriminator()
		if err != nil || kind != "assistant" {
			continue
		}
		assistant, err := item.AsAssistantItem()
		if err != nil || assistant.OperationId == nil || *assistant.OperationId != operationID {
			continue
		}
		return assistant
	}
	t.Fatalf("conversation has no assistant item for operation %q", operationID)
	return protocol.AssistantItem{}
}

// shippedToolCall returns one assistant item's committed call by identity.
func shippedToolCall(t *testing.T, assistant protocol.AssistantItem, callID string) protocol.ToolCallView {
	t.Helper()
	for _, call := range assistant.ToolCalls {
		if call.Id == callID {
			return call
		}
	}
	t.Fatalf("assistant item %s has no tool call %q: %+v", assistant.ItemId, callID, assistant.ToolCalls)
	return protocol.ToolCallView{}
}

// shippedAwaitCheckpoint waits for one committed context checkpoint.
func shippedAwaitCheckpoint(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, sessionID string) protocol.CheckpointItem {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		history, err := client.GetSessionHistoryWithResponse(ctx, sessionID, nil)
		if err == nil && history.JSON200 != nil {
			for _, item := range history.JSON200.Items {
				kind, err := item.Discriminator()
				if err != nil || kind != "context_checkpoint" {
					continue
				}
				checkpoint, err := item.AsCheckpointItem()
				if err == nil {
					return checkpoint
				}
			}
		}
		testassert.Require(t, !time.Now().After(deadline), "Session %s committed no context checkpoint (last read %v)", sessionID, err)
		time.Sleep(10 * time.Millisecond)
	}
}

// shippedAwaitRuntimeInput waits for one runtime-origin input carrying the
// given report text and returns its text.
func shippedAwaitRuntimeInput(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, sessionID, want string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		hydration := shippedHydrate(t, ctx, client, sessionID)
		for _, item := range hydration.Conversation.Items {
			kind, err := item.Discriminator()
			if err != nil || kind != "input" {
				continue
			}
			input, err := item.AsInputItem()
			if err != nil || input.Origin != protocol.InputOriginRuntime {
				continue
			}
			for _, part := range input.Content {
				text, err := part.AsTextPart()
				if err == nil && text.Text == want {
					return text.Text
				}
			}
		}
		testassert.Require(t, !time.Now().After(deadline), "Session %s never recorded the runtime report %q", sessionID, want)
		time.Sleep(10 * time.Millisecond)
	}
}

// shippedAwaitBackgroundSession waits for one owned Session member on the
// parent's background membership and returns its identity.
func shippedAwaitBackgroundSession(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, sessionID string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		hydration := shippedHydrate(t, ctx, client, sessionID)
		for _, member := range hydration.Background {
			if member.Kind == protocol.BackgroundMemberKindSession {
				return member.Id
			}
		}
		testassert.Require(t, !time.Now().After(deadline), "Session %s never carried an owned Session member: %+v", sessionID, hydration.Background)
		time.Sleep(10 * time.Millisecond)
	}
}

// shippedWireBody is one recorded request's decoded shape.
type shippedWireBody struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Tools []struct {
		Function struct {
			Name string `json:"name"`
		} `json:"function"`
	} `json:"tools"`
}

// shippedWire decodes one recorded request body. It never fails the test:
// the scripted endpoint's handler runs on another goroutine.
func shippedWire(body string) shippedWireBody {
	var decoded shippedWireBody
	_ = json.Unmarshal([]byte(body), &decoded)
	return decoded
}

// shippedWireUserText returns the last user message's plain text content.
func shippedWireUserText(body string) string {
	decoded := shippedWire(body)
	for i := len(decoded.Messages) - 1; i >= 0; i-- {
		if decoded.Messages[i].Role != "user" {
			continue
		}
		var text string
		if json.Unmarshal(decoded.Messages[i].Content, &text) == nil {
			return text
		}
		return ""
	}
	return ""
}

// shippedWireRole returns the final message's wire role.
func shippedWireRole(body string) string {
	decoded := shippedWire(body)
	if len(decoded.Messages) == 0 {
		return ""
	}
	return decoded.Messages[len(decoded.Messages)-1].Role
}

// shippedWireSystemText returns the request's concatenated system text.
func shippedWireSystemText(body string) string {
	decoded := shippedWire(body)
	var text strings.Builder
	for _, message := range decoded.Messages {
		if message.Role != "system" {
			continue
		}
		var content string
		if json.Unmarshal(message.Content, &content) == nil {
			text.WriteString(content)
		}
	}
	return text.String()
}

// shippedWireToolNames returns the advertised tool names in order.
func shippedWireToolNames(body string) []string {
	decoded := shippedWire(body)
	names := make([]string, 0, len(decoded.Tools))
	for _, tool := range decoded.Tools {
		names = append(names, tool.Function.Name)
	}
	return names
}

// shippedRequestWithUser returns the recorded request carrying the given
// user text.
func shippedRequestWithUser(t *testing.T, bodies []string, text string) string {
	t.Helper()
	for _, body := range bodies {
		if shippedWireUserText(body) == text {
			return body
		}
	}
	t.Fatalf("no recorded request carries the user text %q", text)
	return ""
}

func shippedSSE(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range events {
		fmt.Fprintf(w, "data: %s\n\n", event)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func shippedTextTurn(w http.ResponseWriter, text string) {
	shippedSSE(w,
		`{"choices":[{"delta":{"role":"assistant","content":`+strconv.Quote(text)+`},"finish_reason":null}]}`,
		`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
	)
}

func shippedToolTurn(w http.ResponseWriter, id, name, arguments string) {
	shippedSSE(w,
		`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":`+strconv.Quote(id)+`,"type":"function","function":{"name":`+strconv.Quote(name)+`,"arguments":`+strconv.Quote(arguments)+`}}]},"finish_reason":null}]}`,
		`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
	)
}

// shippedExternalExtension is an extension composed only from the public
// extension contract, the way an out-of-tree extension is: it declares the
// Workspace service and registers one ordinary Workspace-reading tool.
func shippedExternalExtension() extension.Extension {
	return extension.Extension{
		Manifest: extension.Manifest{
			ID:             "external-shipped",
			Version:        "1",
			APIVersion:     extension.APIVersion,
			SettingsSchema: json.RawMessage(`{"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"}},"additionalProperties":true}`),
			Services:       []string{"melta.workspace"},
		},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterTool(model.ToolDefinition{
				Name:        "external_read",
				Description: "Read one Workspace file and return its text.",
				Parameters:  json.RawMessage(`{"type":"object","required":["path"],"properties":{"path":{"type":"string"}},"additionalProperties":false}`),
			}, shippedExternalTool{}, nil)
			return nil, err
		},
	}
}

// shippedExternalTarget is one Workspace binding the external tool received.
type shippedExternalTarget struct {
	binding   string
	canonical string
}

func shippedExternalBind(ctx context.Context, author extension.AuthorContext, path string) (shippedExternalTarget, error) {
	arguments, err := json.Marshal(map[string]string{"path": path})
	if err != nil {
		return shippedExternalTarget{}, err
	}
	result, err := author.Call(ctx, "melta.workspace", "bind", arguments)
	if err != nil {
		return shippedExternalTarget{}, err
	}
	if result.Failure != nil {
		return shippedExternalTarget{}, fmt.Errorf("external_read: bind %s: %s", path, result.Failure.Message)
	}
	var bound struct {
		Binding       string `json:"binding"`
		CanonicalPath string `json:"canonical_path"`
	}
	if err := json.Unmarshal(result.Value, &bound); err != nil || bound.Binding == "" || bound.CanonicalPath == "" {
		return shippedExternalTarget{}, fmt.Errorf("external_read: bind %s: %v", path, err)
	}
	return shippedExternalTarget{binding: bound.Binding, canonical: bound.CanonicalPath}, nil
}

func shippedExternalFailure(callID, content string) extension.ToolPlan {
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: content}}}
}

type shippedExternalTool struct{}

func (shippedExternalTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	var input struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(call.Arguments, &input); err != nil || input.Path == "" {
		return nil, fmt.Errorf("external_read: path is required")
	}
	return call.Arguments, nil
}

func (shippedExternalTool) Prepare(ctx context.Context, author extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	var input struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(call.Arguments, &input); err != nil || input.Path == "" {
		return shippedExternalFailure(call.ID, "external_read: path is required")
	}
	root, err := shippedExternalBind(ctx, author, ".")
	if err != nil {
		return shippedExternalFailure(call.ID, err.Error())
	}
	target, err := shippedExternalBind(ctx, author, input.Path)
	if err != nil {
		return shippedExternalFailure(call.ID, err.Error())
	}
	data, err := json.Marshal(struct {
		CallID  string `json:"call_id"`
		Binding string `json:"binding"`
	}{CallID: call.ID, Binding: target.binding})
	if err != nil {
		return shippedExternalFailure(call.ID, err.Error())
	}
	return extension.ToolPlan{
		Permissions:        []harness.PermissionRequest{{Permission: harness.PermissionFileRead, Target: target.canonical}},
		CanonicalWorkspace: root.canonical,
		Data:               data,
	}
}

func (shippedExternalTool) Execute(ctx context.Context, author extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	var data struct {
		CallID  string `json:"call_id"`
		Binding string `json:"binding"`
	}
	if err := json.Unmarshal(plan.Data, &data); err != nil {
		return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}
	}
	arguments, err := json.Marshal(map[string]string{"binding": data.Binding})
	if err != nil {
		return harness.ToolOutcome{Result: model.ToolResult{CallID: data.CallID, Status: model.ResultError, Content: err.Error()}}
	}
	result, err := author.Call(ctx, "melta.workspace", "read", arguments)
	if err == nil && result.Failure != nil {
		err = fmt.Errorf("%s", result.Failure.Message)
	}
	if err != nil {
		return harness.ToolOutcome{Result: model.ToolResult{CallID: data.CallID, Status: model.ResultError, Content: err.Error()}}
	}
	var read struct {
		Bytes worker.JSONBytes `json:"bytes"`
	}
	if err := json.Unmarshal(result.Value, &read); err != nil {
		return harness.ToolOutcome{Result: model.ToolResult{CallID: data.CallID, Status: model.ResultError, Content: err.Error()}}
	}
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{
		CallID: data.CallID, Status: model.ResultSuccess, Content: "external result: " + string(read.Bytes),
	}}
}

// TestShippedRuntimeNeitherExtension proves the baseline distribution alone:
// one configured host model turn reaches the provider with the configured
// wire model, standard file mutations are captured and restored through
// public code reversion, and manual compaction commits a context checkpoint.
func TestShippedRuntimeNeitherExtension(t *testing.T) {
	modelServer := newShippedModelServer(t)
	modelServer.setRespond(func(w http.ResponseWriter, body string) {
		switch {
		case shippedWireRole(body) == "tool":
			shippedTextTurn(w, "done")
		case shippedWireUserText(body) == "write v1":
			shippedToolTurn(w, "shipped-write", "write_file", `{"path":"shipped.txt","content":"v1"}`)
		case shippedWireUserText(body) == "edit v2":
			shippedToolTurn(w, "shipped-edit", "edit_file", `{"path":"shipped.txt","old_string":"v1","new_string":"v2"}`)
		default:
			shippedTextTurn(w, "done")
		}
	})
	ctx := context.Background()
	workspace := t.TempDir()
	document := shippedDocument(modelServer.URL, map[string]string{"m": "M"}, nil)
	_, client := shippedMount(t, ctx, t.TempDir(), document, nil)
	sessionID := shippedCreateSession(t, ctx, client, workspace)
	shippedSelectModel(t, ctx, client, sessionID, "prov/m")

	shippedSubmit(t, ctx, client, sessionID, "op-message", "hello")
	requests := modelServer.recorded()
	testassert.Require(t, len(requests) != 0, "the configured message turn never reached the provider")
	testassert.Equal(t, shippedWire(requests[0]).Model, "m")

	shippedSubmit(t, ctx, client, sessionID, "op-write", "write v1")
	data, err := os.ReadFile(filepath.Join(workspace, "shipped.txt"))
	testassert.Require(t, err == nil && string(data) == "v1", "shipped.txt after write_file = (%q, %v), want v1", data, err)

	shippedSubmit(t, ctx, client, sessionID, "op-edit", "edit v2")
	data, err = os.ReadFile(filepath.Join(workspace, "shipped.txt"))
	testassert.Require(t, err == nil && string(data) == "v2", "shipped.txt after edit_file = (%q, %v), want v2", data, err)

	reverted, err := client.RevertSessionCodeWithResponse(ctx, sessionID, protocol.RevertCodeRequest{BoundaryOperationId: "op-edit"})
	testassert.Require(t, err == nil && reverted.JSON200 != nil, "RevertSessionCode = (%+v, %v)", reverted, err)
	wantRestored := filepath.Join(workspace, "shipped.txt")
	testassert.Require(t, len(reverted.JSON200.Restored) == 1 && reverted.JSON200.Restored[0] == wantRestored && len(reverted.JSON200.Skipped) == 0, "revert = %+v, want exactly the restored %s", reverted.JSON200, wantRestored)
	data, err = os.ReadFile(wantRestored)
	testassert.Require(t, err == nil && string(data) == "v1", "shipped.txt after reversion = (%q, %v), want the edit's preimage v1", data, err)

	compaction, err := client.CompactSessionWithResponse(ctx, sessionID, protocol.CompactRequest{OperationId: "op-compact"})
	testassert.Require(t, err == nil && compaction.JSON200 != nil && compaction.JSON200.OperationId == "op-compact", "CompactSession = (%+v, %v)", compaction, err)
	awaitMountedSuccess(t, ctx, client, sessionID, "op-compact")
	checkpoint := shippedAwaitCheckpoint(t, ctx, client, sessionID)
	testassert.Require(t, checkpoint.Strategy != "", "committed checkpoint = %+v, want its strategy", checkpoint)
}

// TestShippedRuntimeAgentsOnly proves the shipped Agents extension alone:
// the advertised Workspace-targeted create operation admits the parent
// Session, the parent's task call starts an owned Session carrying its own
// model turn, and the child's report is delivered to the parent.
func TestShippedRuntimeAgentsOnly(t *testing.T) {
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()

	modelServer := newShippedModelServer(t)
	modelServer.setRespond(func(w http.ResponseWriter, body string) {
		switch {
		case shippedWireUserText(body) == "child work":
			<-gate
			shippedTextTurn(w, "child report")
		case shippedWireRole(body) == "tool":
			shippedTextTurn(w, "parent done")
		default:
			shippedToolTurn(w, "shipped-task", "task", `{"prompt":"child work","subagent_type":"secondary"}`)
		}
	})
	ctx := context.Background()
	document := shippedDocument(modelServer.URL, map[string]string{"m": "M"}, map[string]any{
		"agents": map[string]any{
			"enabled":     true,
			"definitions": map[string]any{"primary": map[string]any{"model": "prov/m"}},
		},
	})
	_, client := shippedMount(t, ctx, t.TempDir(), document, shippedPlugins("agents"))

	catalog, err := client.ListExtensionsWithResponse(ctx)
	testassert.Require(t, err == nil && catalog.JSON200 != nil, "ListExtensions = (%+v, %v)", catalog, err)
	var agentsEntry *protocol.RegisteredExtension
	for i := range *catalog.JSON200 {
		if (*catalog.JSON200)[i].ExtensionId == "agents" {
			agentsEntry = &(*catalog.JSON200)[i]
		}
	}
	testassert.Require(t, agentsEntry != nil, "the catalog omits the shipped Agents extension: %+v", catalog.JSON200)
	advertised := false
	for _, operation := range agentsEntry.Operations {
		if operation.Method.Name == "create" && operation.Target == protocol.RegisteredExtensionOperationTargetWorkspace {
			advertised = true
		}
	}
	testassert.Require(t, advertised, "the Agents catalog omits the Workspace-targeted create operation: %+v", agentsEntry.Operations)
	sessionTools := make(map[string]bool)
	for _, tool := range agentsEntry.Tools {
		sessionTools[tool] = true
	}
	testassert.Require(t, sessionTools["task"], "the Agents catalog omits the task tool: %+v", agentsEntry.Tools)

	workspace := t.TempDir()
	arguments := json.RawMessage(`{}`)
	created, err := client.InvokeExtensionWithResponse(ctx, protocol.ExtensionInvocationRequest{
		ExtensionId: "agents",
		Operation:   "create",
		RequestId:   "shipped-agents-create",
		Target:      protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace},
		Arguments:   &arguments,
	})
	testassert.Require(t, err == nil && created.JSON200 != nil && created.JSON200.Result != nil, "Agents create = (%+v, %v)", created, err)
	var receipt struct {
		SessionID string `json:"session_id"`
	}
	testassert.NoError(t, json.Unmarshal(*created.JSON200.Result, &receipt))
	testassert.Require(t, receipt.SessionID != "", "Agents create receipt = %s, want a Session identity", *created.JSON200.Result)
	parentID := receipt.SessionID

	shippedSubmit(t, ctx, client, parentID, "op-delegate", "delegate work")
	childID := shippedAwaitBackgroundSession(t, ctx, client, parentID)
	child := shippedHydrate(t, ctx, client, childID)
	testassert.Require(t, child.Session.OwnerSessionId != nil && *child.Session.OwnerSessionId == parentID, "owned Session %s owner = %v, want %s", childID, child.Session.OwnerSessionId, parentID)

	parent := shippedHydrate(t, ctx, client, parentID)
	call := shippedToolCall(t, shippedAssistant(t, parent.Conversation.Items, "op-delegate"), "shipped-task")
	testassert.Require(t, call.Name == "task" && call.Status != nil && *call.Status == protocol.ToolCallStatusSuccess && call.Content != nil && strings.Contains(*call.Content, childID), "task call = %+v, want a settled success naming the owned Session %s", call, childID)

	release()
	want := "Session " + childID + ": success\nchild report"
	testassert.Equal(t, shippedAwaitRuntimeInput(t, ctx, client, parentID, want), want)
}

// TestShippedRuntimeAdaptationOnly proves the shipped Adaptation extension
// alone acts on the actual model request: a matching configured row changes
// the prompt and the advertised tools, including keeping apply_patch for an
// offer_patch row, while a non-matching model receives none of those changes.
func TestShippedRuntimeAdaptationOnly(t *testing.T) {
	modelServer := newShippedModelServer(t)
	ctx := context.Background()
	document := shippedDocument(modelServer.URL, map[string]string{"match-1": "Match One", "other": "Other"}, map[string]any{
		"adaptation": map[string]any{
			"enabled": true,
			"entries": []any{map[string]any{
				"pattern":       "match-*",
				"append":        []string{"ADAPTATION-ONLY-MARK"},
				"exclude_tools": []string{"write_file"},
				"offer_patch":   true,
			}},
		},
	})
	_, client := shippedMount(t, ctx, t.TempDir(), document, shippedPlugins("adaptation"))

	matchingID := shippedCreateSession(t, ctx, client, t.TempDir())
	shippedSelectModel(t, ctx, client, matchingID, "prov/match-1")
	shippedSubmit(t, ctx, client, matchingID, "op-matching", "matching turn")

	plainID := shippedCreateSession(t, ctx, client, t.TempDir())
	shippedSelectModel(t, ctx, client, plainID, "prov/other")
	shippedSubmit(t, ctx, client, plainID, "op-plain", "plain turn")

	requests := modelServer.recorded()
	matching := shippedRequestWithUser(t, requests, "matching turn")
	matchingTools := shippedWireToolNames(matching)
	testassert.Require(t, strings.Contains(shippedWireSystemText(matching), "ADAPTATION-ONLY-MARK"), "matching request system text = %q, want the configured appended prompt", shippedWireSystemText(matching))
	testassert.Require(t, containsString(matchingTools, "apply_patch"), "matching request tools = %v, want apply_patch advertised for the offer_patch row", matchingTools)
	testassert.Require(t, !containsString(matchingTools, "write_file") && containsString(matchingTools, "read_file"), "matching request tools = %v, want write_file excluded with read_file retained", matchingTools)

	plain := shippedRequestWithUser(t, requests, "plain turn")
	plainTools := shippedWireToolNames(plain)
	testassert.Require(t, !strings.Contains(shippedWireSystemText(plain), "ADAPTATION-ONLY-MARK"), "non-matching request system text = %q, want no appended prompt", shippedWireSystemText(plain))
	testassert.Require(t, !containsString(plainTools, "apply_patch") && containsString(plainTools, "write_file"), "non-matching request tools = %v, want apply_patch filtered out with write_file retained", plainTools)
}

// TestShippedRuntimeBothExtensions proves the shipped Agents and Adaptation
// extensions shape one Session's actual model request together: the selected
// Agent profile's instructions and Adaptation's configured prompt change both
// appear in the same provider request.
func TestShippedRuntimeBothExtensions(t *testing.T) {
	modelServer := newShippedModelServer(t)
	ctx := context.Background()
	document := shippedDocument(modelServer.URL, map[string]string{"m": "M"}, map[string]any{
		"agents": map[string]any{
			"enabled": true,
			"definitions": map[string]any{
				"profiled": map[string]any{"model": "prov/m", "system_prompt": "simple", "prompt": "AGENT-PROFILE-MARK"},
			},
		},
		"adaptation": map[string]any{
			"enabled": true,
			"entries": []any{map[string]any{"pattern": "*", "append": []string{"ADAPTATION-REQUEST-MARK"}}},
		},
	})
	_, client := shippedMount(t, ctx, t.TempDir(), document, shippedPlugins("agents", "adaptation"))

	workspace := t.TempDir()
	arguments := json.RawMessage(`{"agent":"profiled"}`)
	created, err := client.InvokeExtensionWithResponse(ctx, protocol.ExtensionInvocationRequest{
		ExtensionId: "agents",
		Operation:   "create",
		RequestId:   "shipped-both-create",
		Target:      protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace},
		Arguments:   &arguments,
	})
	testassert.Require(t, err == nil && created.JSON200 != nil && created.JSON200.Result != nil, "Agents create = (%+v, %v)", created, err)
	var receipt struct {
		SessionID string `json:"session_id"`
	}
	testassert.NoError(t, json.Unmarshal(*created.JSON200.Result, &receipt))
	testassert.Require(t, receipt.SessionID != "", "Agents create receipt = %s, want a Session identity", *created.JSON200.Result)

	shippedSubmit(t, ctx, client, receipt.SessionID, "op-both", "both turn")
	requests := modelServer.recorded()
	testassert.Require(t, len(requests) != 0, "the combined Session never reached the provider")
	system := shippedWireSystemText(requests[0])
	testassert.Require(t, strings.Contains(system, "AGENT-PROFILE-MARK") && strings.Contains(system, "ADAPTATION-REQUEST-MARK"), "combined request system text = %q, want the selected Agent profile and the Adaptation change", system)
	testassert.Equal(t, shippedWire(requests[0]).Model, "m")
}

// TestShippedRuntimeExternalExtension proves an extension composed only from
// the public contract registers a tool, the model invokes it, and its
// retirement leaves the Session history with the recorded call and result
// readable without the provider.
func TestShippedRuntimeExternalExtension(t *testing.T) {
	modelServer := newShippedModelServer(t)
	modelServer.setRespond(func(w http.ResponseWriter, body string) {
		switch {
		case shippedWireRole(body) == "tool":
			shippedTextTurn(w, "done")
		case shippedWireUserText(body) == "call external":
			shippedToolTurn(w, "shipped-external", "external_read", `{"path":"external.txt"}`)
		default:
			shippedTextTurn(w, "done")
		}
	})
	ctx := context.Background()
	dataDir := t.TempDir()
	document := shippedDocument(modelServer.URL, map[string]string{"m": "M"}, map[string]any{
		"external-shipped": map[string]any{"enabled": true},
	})
	owner, client := shippedMount(t, ctx, dataDir, document, []extension.Extension{shippedExternalExtension()})

	catalog, err := client.ListExtensionsWithResponse(ctx)
	testassert.Require(t, err == nil && catalog.JSON200 != nil, "ListExtensions = (%+v, %v)", catalog, err)
	found := false
	for _, registered := range *catalog.JSON200 {
		if registered.ExtensionId != "external-shipped" {
			continue
		}
		found = containsString(registered.Tools, "external_read")
	}
	testassert.Require(t, found, "the catalog omits the external extension's registered tool: %+v", catalog.JSON200)

	workspace := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(workspace, "external.txt"), []byte("external input"), 0o600))
	sessionID := shippedCreateSession(t, ctx, client, workspace)
	shippedSelectModel(t, ctx, client, sessionID, "prov/m")
	shippedSubmit(t, ctx, client, sessionID, "op-external", "call external")

	session := shippedHydrate(t, ctx, client, sessionID)
	call := shippedToolCall(t, shippedAssistant(t, session.Conversation.Items, "op-external"), "shipped-external")
	testassert.Require(t, call.Name == "external_read" && call.Status != nil && *call.Status == protocol.ToolCallStatusSuccess && call.Content != nil && *call.Content == "external result: external input", "external tool call = %+v, want the recorded success result", call)

	testassert.NoError(t, owner.Close(ctx))
	requestsBefore := len(modelServer.recorded())
	_, reopened := shippedMount(t, ctx, dataDir, document, nil)
	catalog, err = reopened.ListExtensionsWithResponse(ctx)
	testassert.Require(t, err == nil && catalog.JSON200 != nil && len(*catalog.JSON200) == 0, "reopened catalog = (%+v, %v), want the retired extension absent", catalog, err)

	history, err := reopened.GetSessionHistoryWithResponse(ctx, sessionID, nil)
	testassert.Require(t, err == nil && history.JSON200 != nil, "reopened GetSessionHistory = (%+v, %v)", history, err)
	recorded := shippedToolCall(t, shippedAssistant(t, history.JSON200.Items, "op-external"), "shipped-external")
	testassert.Require(t, recorded.Name == "external_read" && recorded.Status != nil && *recorded.Status == protocol.ToolCallStatusSuccess && recorded.Content != nil && *recorded.Content == "external result: external input", "reopened recorded call = %+v, want the call and result readable without the extension", recorded)
	testassert.Equal(t, len(modelServer.recorded()), requestsBefore)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
