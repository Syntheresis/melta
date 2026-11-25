package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// Preparation fixtures: the public Open over the existing owner environment
// drives the real Melta and its one concrete preparation over both stores
// through a keyless local model endpoint, registered admission, request and
// argument hooks, and registered test tools with observable behavior. HTTP
// scripts dispatch on actual wire inputs only.

var (
	errHook      = errors.New("test hook failure")
	errOpen      = errors.New("test open failure")
	errExecClose = errors.New("test execution cleanup failure")
)

var prepModelRef = model.ModelRef{Provider: "prov", Model: "m"}

// runtimeNormalize is the fixtures' required pure normalizer: it accepts
// exactly one non-null JSON object and returns its compact encoding.
func runtimeNormalize(call model.ToolCall) (json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(call.Arguments, &obj); err != nil || obj == nil {
		return nil, errors.New("arguments must be one non-null JSON object")
	}
	return json.Marshal(obj)
}

// prepConfigDocument points the configured provider at the fixture's keyless
// local endpoint with discovery disabled and a generous window, so ordinary
// rows never accidentally test compaction. The "zero" model keeps the
// zero-window rejection row. "m" carries the capture-pricing fixture rates.
func prepConfigDocument(endpoint, tag string) string {
	return `{"providers":{"prov":` + nominalProvMember(endpoint, "",
		`{"m":{"name":"M","context_window":262144,"max_output_tokens":4096,"cost":{"input":1.5,"output":3,"cache_read":0.5,"cache_write":0.25}},`+
			`"zero":{"name":"Z","context_window":0}}`) + `},"extensions":{"hooks":{"enabled":true,"tag":` + fixtureString(tag) + `},"caps":{"enabled":true}}}`
}

// "solo" with no selection, gives "ghostly" no model at all, selects the
// catalog model without a positive context window on "shallow", and names
// the tool pairs the visibility rows work with.
const prepAgentsDocument = `{
  "solo": {"model": "prov/m", "system_prompt": "simple", "tools": ["echo"]},
  "dual": {"model": "prov/m", "system_prompt": "simple", "tools": ["echo"]},
  "dup": {"model": "prov/m", "system_prompt": "simple", "tools": ["echo", "echo", "read"]},
  "wide": {"model": "prov/m", "system_prompt": "simple", "tools": ["echo", "read"]},
  "deep": {"model": "prov/m", "system_prompt": "simple", "tools": ["echo"]},
  "hooky": {"model": "prov/m", "system_prompt": "simple", "tools": ["echo"]},
  "ghostly": {"system_prompt": "simple", "tools": ["echo"]},
  "shallow": {"model": "prov/zero", "system_prompt": "simple", "tools": ["echo"]}
}`

// recordHook is one registered admission contribution: it retains the tool
// candidate it received and its provider settings, records the call, and
// answers with its configured consequence or error. One armed call may park
// until its test releases it, and one armed call may fail with a one-shot
// error.
type recordHook struct {
	name   string
	events *traceLog

	mu              sync.Mutex
	seen            []hookCall
	inputTools      []model.ToolDefinition
	inputDesc       string
	fail            error
	admissionMutate func(extension.AdmissionInput) (extension.AdmissionResult, error)
	block           bool
	blockSignal     chan struct{}
	parkRelease     chan struct{}
	parkArrived     chan struct{}
	nextFail        error
}

func (h *recordHook) Admit(ctx context.Context, call extension.AuthorContext, in extension.AdmissionInput) (extension.AdmissionResult, error) {
	h.events.add("hook:" + h.name)
	h.mu.Lock()
	h.inputTools = in.Candidate.Tools
	if len(in.Candidate.Tools) > 0 {
		h.inputDesc = in.Candidate.Tools[0].Description
	}
	block, blockSignal := h.block, h.blockSignal
	fail, mutate := h.fail, h.admissionMutate
	parkRelease, parkArrived := h.parkRelease, h.parkArrived
	h.parkRelease, h.parkArrived = nil, nil
	nextFail := h.nextFail
	h.nextFail = nil
	h.mu.Unlock()
	if parkRelease != nil {
		if parkArrived != nil {
			select {
			case parkArrived <- struct{}{}:
			default:
			}
		}
		select {
		case <-parkRelease:
		case <-ctx.Done():
			return extension.AdmissionResult{}, ctx.Err()
		}
	}
	if block {
		if blockSignal != nil {
			select {
			case blockSignal <- struct{}{}:
			default:
			}
		}
		<-ctx.Done()
		return extension.AdmissionResult{}, nil
	}
	var settings struct {
		Tag string `json:"tag"`
	}
	if len(call.Settings) != 0 {
		if err := json.Unmarshal(call.Settings, &settings); err != nil {
			return extension.AdmissionResult{}, err
		}
	}
	h.mu.Lock()
	h.seen = append(h.seen, hookCall{tag: settings.Tag})
	h.mu.Unlock()
	if nextFail != nil {
		return extension.AdmissionResult{}, nextFail
	}
	if fail != nil {
		return extension.AdmissionResult{}, fail
	}
	if mutate != nil {
		return mutate(in)
	}
	return extension.AdmissionResult{}, nil
}

type hookCall struct {
	tag string
}

func (h *recordHook) calls() []hookCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]hookCall(nil), h.seen...)
}

// retainedInputs returns the candidate tool slice and first description
// handed to the last Admit call. The candidate is the hook's own owned copy,
// so its in-place mutation is observable through it while the following hook
// and the capture see only accepted values.
func (h *recordHook) retainedInputs() ([]model.ToolDefinition, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inputTools, h.inputDesc
}

func (h *recordHook) setAdmission(fail error, mutate func(extension.AdmissionInput) (extension.AdmissionResult, error)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fail, h.admissionMutate = fail, mutate
}

// failNext arms exactly the next Admit call to fail with err after
// recording it; every later call answers normally again.
func (h *recordHook) failNext(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextFail = err
}

// parkNext arms exactly the next Admit call to park after recording its
// input, until the release channel closes; the arrived channel signals the
// parked call once.
func (h *recordHook) parkNext(release chan struct{}, arrived chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.parkRelease, h.parkArrived = release, arrived
}

// blockUntilCanceled parks the next Admit call until its call context is
// canceled, after signaling the buffered start channel.
func (h *recordHook) blockUntilCanceled(started chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.block = true
	h.blockSignal = started
}

// recordArgumentHook is one registered arguments contribution: it records
// every received call's argument bytes and owning extension identity and
// answers with its fixed replacement.
type recordArgumentHook struct {
	replace json.RawMessage

	mu   sync.Mutex
	seen []string
	revs []string
}

func (h *recordArgumentHook) AdmitArgs(ctx context.Context, author extension.AuthorContext, call model.ToolCall) (extension.ArgumentsResult, error) {
	if err := ctx.Err(); err != nil {
		return extension.ArgumentsResult{}, err
	}
	h.mu.Lock()
	h.seen = append(h.seen, string(call.Arguments))
	h.revs = append(h.revs, author.ExtensionID)
	h.mu.Unlock()
	return extension.ArgumentsResult{Arguments: h.replace}, nil
}

func (h *recordArgumentHook) received() ([]string, []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.seen...), append([]string{}, h.revs...)
}

// prepTool is the fixture's registered "echo" tool: pure object
// normalization, one declared file.write pair on the fixed canonical target,
// and observable counters, so the shared boundary's normalization, denial
// and argument-hook rows read real effects through the concrete opener. The
// failure flag is the one deliberate error-outcome row: false keeps the
// ordinary success result, true settles the executed call with a real error
// result.
type prepTool struct {
	mu         sync.Mutex
	normalized int
	executed   int
	input      []string
	fail       bool
}

func (t *prepTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	t.mu.Lock()
	t.normalized++
	t.mu.Unlock()
	return runtimeNormalize(call)
}

func (t *prepTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	t.mu.Lock()
	t.input = append(t.input, string(call.Arguments))
	t.mu.Unlock()
	return extension.ToolPlan{Data: []byte(call.ID)}
}

func (t *prepTool) Execute(_ context.Context, _ extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	t.mu.Lock()
	t.executed++
	fail := t.fail
	t.mu.Unlock()
	callID := string(plan.Data)
	if fail {
		return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: callID, Status: model.ResultError, Content: "echo failed"}}
	}
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: "ran"}}
}

func (t *prepTool) counters() (int, int, []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.normalized, t.executed, append([]string(nil), t.input...)
}

// textTurnEvents returns the SSE events of one completed text turn.
func textTurnEvents(text string) []string {
	return []string{
		`{"choices":[{"delta":{"role":"assistant","content":"` + text + `"},"finish_reason":null}]}`,
		`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
	}
}

// toolCallTurnEvents returns the SSE events of one completed tool-call turn.
func toolCallTurnEvents(id, name, args string) []string {
	return []string{
		`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"` + id + `","type":"function","function":{"name":"` + name + `","arguments":` + strconv.Quote(args) + `}}]},"finish_reason":null}]}`,
		`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
	}
}

// prepEnv hosts the preparation oracles on one real owner: the existing
// ownerEnv's isolated HOME/paths/store/server, the fixture's registered
// hooks and tools supplied to the public Open with the supplied Agents
// extension first, and the constructed Melta's actual Harness, Reload and
// Close. Every preparation, capture and admission observation runs through
// that owner; no second composition, scope registry or configuration service
// is assembled here.
type prepEnv struct {
	*ownerEnv
	r         *Melta
	store     harness.Storage
	workspace string
	echo      *prepTool
	hooks     [2]*recordHook
	argHooks  [4]*recordArgumentHook

	hookOpens, capOpens atomic.Int64
}

func newPrepEnv(t *testing.T, store harness.Storage) *prepEnv {
	t.Helper()
	e := &prepEnv{ownerEnv: newOwnerEnv(t), store: store, echo: &prepTool{}}
	e.workspace = e.dataDir
	writeServiceFile(t, e.configPath, prepConfigDocument(e.server.URL, "A"))
	writeAgentDefinitions(t, e.configPath, prepAgentsDocument)
	e.hooks[0] = &recordHook{name: "hook.first", events: e.events, admissionMutate: func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
		if in.Candidate.Prompt == nil {
			return extension.AdmissionResult{}, nil
		}
		prompt := *in.Candidate.Prompt + "|first"
		candidate := in.Candidate
		candidate.Prompt = &prompt
		return extension.AdmissionResult{Candidate: &candidate}, nil
	}}
	e.hooks[1] = &recordHook{name: "hook.second", events: e.events, admissionMutate: func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
		if in.Candidate.Prompt == nil {
			return extension.AdmissionResult{}, nil
		}
		prompt := *in.Candidate.Prompt + "|second"
		candidate := in.Candidate
		candidate.Prompt = &prompt
		return extension.AdmissionResult{Candidate: &candidate}, nil
	}}
	for i, name := range []string{"arg.runtime", "arg.workspace", "arg.operation", "arg.agent"} {
		e.argHooks[i] = &recordArgumentHook{replace: json.RawMessage(`{"repaired":"` + name + `"}`)}
	}
	e.fixtureStore(store)
	r, err := e.open(context.Background(), append([]extension.Extension{agentsTestNative()}, e.plugins()...)...)
	testassert.NoError(t, err)
	e.r = r
	t.Cleanup(func() { _ = e.converge() })
	return e
}

func (e *prepEnv) plugins() []extension.Extension {
	hooks := testNative("hooks", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		e.hookOpens.Add(1)
		if _, err := api.AttachAdmission(e.hooks[0].Admit, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachAdmission(e.hooks[1].Admit, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachArguments(e.argHooks[0].AdmitArgs, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachArguments(e.argHooks[1].AdmitArgs, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterTool(testDescribeTool("echo"), e.echo, nil); err != nil {
			return nil, err
		}
		_, err := api.RegisterTool(testDescribeTool("read"), noopNativeTool{}, nil)
		return nil, err
	})
	hooks.Manifest.SettingsSchema = json.RawMessage(`{"type":"object","required":["enabled","tag"],"properties":{"enabled":{"type":"boolean"},"tag":{"type":"string","minLength":1}},"additionalProperties":true}`)
	caps := testNative("caps", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		e.capOpens.Add(1)
		if _, err := api.RegisterService(testServiceCapability("cap.shared"), nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterService(testServiceCapability("cap.other"), nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachArguments(e.argHooks[2].AdmitArgs, nil); err != nil {
			return nil, err
		}
		_, err := api.AttachArguments(e.argHooks[3].AdmitArgs, nil)
		return nil, err
	})
	return []extension.Extension{hooks, caps}
}

// --- fixture control helpers ---

// session creates one root Session of the given agent type through the
// Melta's admitted-call gate.
func (e *prepEnv) session(agentType string) string {
	e.t.Helper()
	rec, err := e.r.createSession(context.Background(), e.workspace, agentType)
	if err != nil {
		e.t.Fatalf("createSession(%q): %v", agentType, err)
	}
	return rec.Identity.SessionID
}

// submit runs one public Submit through the Melta's actual Harness,
// returning the reported result even on error exactly as the Harness does.
func (e *prepEnv) submit(ctx context.Context, sessionID, operationID, text string, mode harness.MessageMode) (harness.SubmitResult, error) {
	return submitRawThroughRuntime(ctx, e.r, sessionID, operationID, text, mode)
}

// readOperation, sessionView and fork run their public Harness
// operations through the Melta's actual Harness.
func (e *prepEnv) readOperation(ctx context.Context, sessionID, operationID string) (harness.OperationRecord, error) {
	var rec harness.OperationRecord
	err := e.r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var err error
		rec, err = h.ReadOperation(ctx, sessionID, operationID)
		return err
	})
	return rec, err
}

func (e *prepEnv) sessionView(ctx context.Context, sessionID string) (harness.SessionReadView, error) {
	var view harness.SessionReadView
	err := e.r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var err error
		view, err = h.ReadSessionView(ctx, sessionID)
		return err
	})
	return view, err
}

func (e *prepEnv) fork(ctx context.Context, req harness.ForkRequest) (harness.ForkResult, error) {
	var res harness.ForkResult
	err := e.r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var err error
		res, err = h.Fork(ctx, req)
		return err
	})
	return res, err
}

// reload publishes the next configuration revision through the Melta's
// actual Reload.
func (e *prepEnv) Reload() error {
	_, err := e.r.Reload(context.Background(), testCaller(e.r))
	return err
}

func (e *prepEnv) admit(sessionID, operationID, text string) harness.SubmitResult {
	e.t.Helper()
	res, err := e.submit(context.Background(), sessionID, operationID, text, harness.MessageModeRegular)
	if err != nil || res.Disposition != harness.DispositionAdmitted || res.Operation == nil {
		e.t.Fatalf("submit %q = %+v err %v, want admitted", operationID, res, err)
	}
	return res
}

// admitOrBuffer submits the same way but tolerates the deterministic Harness
// routing while a predecessor's post-terminal drain is in flight: an idle
// Session admits immediately, and an active one buffers the message, which
// its drain then delivers through ordinary admission exactly once.
func (e *prepEnv) admitOrBuffer(sessionID, operationID, text string) {
	e.t.Helper()
	if _, err := e.submit(context.Background(), sessionID, operationID, text, harness.MessageModeRegular); err != nil {
		e.t.Fatalf("submit %q: %v", operationID, err)
	}
}

// awaitTerminal polls one Operation until it reaches the wanted terminal
// state.
func (e *prepEnv) awaitTerminal(sessionID, operationID string, want harness.OperationState) harness.OperationRecord {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		rec, err := e.readOperation(context.Background(), sessionID, operationID)
		if err == nil && rec.State.Status == want {
			return rec
		}
		if time.Now().After(deadline) {
			var detail string
			if rec.State.Terminal != nil {
				detail = rec.State.Terminal.Detail
			}
			e.t.Fatalf("operation %q never settled to %v (err %v, status %v, detail %q)", operationID, want, err, rec.State.Status, detail)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitEvent polls the ordered event log until every named event has run.
func (e *prepEnv) awaitEvent(want ...string) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		events := e.events.all()
		complete := true
		for _, name := range want {
			if !slices.Contains(events, name) {
				complete = false
			}
		}
		if complete {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("events %v never reached %v", events, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// converge joins the owner's one shared shutdown through its actual Close.
func (e *prepEnv) converge() error {
	e.t.Helper()
	return e.r.Close(context.Background())
}

func (e *prepEnv) inputEntry(sessionID, operationID string) string {
	e.t.Helper()
	entries, err := e.store.ReadEntries(context.Background(), sessionID, 0)
	if err != nil {
		e.t.Fatalf("ReadEntries: %v", err)
	}
	for _, entry := range entries {
		if entry.Kind == harness.EntryInput && entry.OperationID == operationID {
			return entry.ID
		}
	}
	e.t.Fatalf("no input entry owned by %q in session %q", operationID, sessionID)
	return ""
}

func countEvents(events []string, want string) int {
	n := 0
	for _, event := range events {
		if event == want {
			n++
		}
	}
	return n
}

// eachPrepStore runs one fixture against memory and a temporary SQLite store.
func eachPrepStore(t *testing.T, run func(t *testing.T, store harness.Storage)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		run(t, storage.NewMemory())
	})
	t.Run("sqlite", func(t *testing.T) {
		store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "melta.db"))
		testassert.NoError(t, err)
		defer store.Close()
		run(t, store)
	})
}

// eachPrepStoreOnce runs one fixture against the memory store alone: the
// scenarios that perform no Session admission, recovery, durable, or mounted
// work have no store axis to multiply.
func eachPrepStoreOnce(t *testing.T, run func(t *testing.T, store harness.Storage)) {
	t.Helper()
	run(t, storage.NewMemory())
}

// TestPreparationHappyPathThroughHarness drives one real admission end to end
// on both stores: the registered hooks run in attachment order and replace
// only the prompt, and the committed capture's selection matches the HTTP
// request's system prompt and tool advertisement.
func TestPreparationHappyPathThroughHarness(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		e := newPrepEnv(t, store)
		session := e.session("wide")
		res := e.admit(session, "op-1", "hello")

		got := res.Operation.Admission.Execution
		testassert.Require(t, slices.Equal(got.InitialToolNames, []string{"echo", "read"}) && got.Model == prepModelRef && got.ConfigurationRevision == "1" && strings.HasSuffix(got.SystemPrompt, "|first|second"), "committed capture = %+v, want the post-hook selection capture", got)
		wantCalls := []hookCall{{tag: "A"}}
		for _, hook := range e.hooks {
			{
				calls := hook.calls()
				testassert.Require(t, slices.Equal(calls, wantCalls), "hook %s calls = %v, want the captured revision %v", hook.name, calls, wantCalls)
			}
		}

		e.awaitTerminal(session, "op-1", harness.OperationSuccess)

		// The wire request carried the committed capture: the composed system
		// prompt and the advertised tool set.
		testassert.Equal(t, e.server.requests(), 1)
		doc := decodeWireChatBody(t, e.server.bodyAt(0))
		if doc.Model != "m" || !slices.Equal(wireToolNames(doc), []string{"echo", "read"}) {
			t.Fatalf("wire request = model %q tools %v, want the captured model and advertised tools", doc.Model, wireToolNames(doc))
		}
		testassert.Require(t, len(doc.Messages) != 0 && doc.Messages[0].Role == "system", "wire messages = %+v, want the leading system prompt", doc.Messages)
		var systemText string
		{
			err := json.Unmarshal(doc.Messages[0].Content, &systemText)
			testassert.Require(t, err == nil && systemText == got.SystemPrompt, "wire system prompt = %q (err %v), want the committed capture's %q", systemText, err, got.SystemPrompt)
		}
		testassert.Equal(t, e.hookOpens.Load(), 1)
		testassert.NoError(t, e.converge())
	})
}

// wireToolNames returns the advertised function names of one wire request.
func wireToolNames(doc wireChatBody) []string {
	out := make([]string, 0, len(doc.Tools))
	for _, tool := range doc.Tools {
		out = append(out, tool.Function.Name)
	}
	return out
}

// toolNames returns one tool list's names in order.
func toolNames(tools []model.ToolDefinition) []string {
	out := make([]string, 0, len(tools))
	for _, tool := range tools {
		out = append(out, tool.Name)
	}
	return out
}

// capturedTool returns the named definition from one capture's catalog.
func capturedTool(tools []model.ToolDefinition, name string) (model.ToolDefinition, bool) {
	for _, tool := range tools {
		if tool.Name == name {
			return tool, true
		}
	}
	return model.ToolDefinition{}, false
}

// setExtensionState stores one fixture extension's own Session state through
// the live owner's Harness: registered callbacks read only their activating
// extension's state.
func setExtensionState(t *testing.T, r *Melta, sessionID, extensionID, value string) {
	t.Helper()
	err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
		_, err := h.SetSessionExtensionState(ctx, sessionID, extensionID, harness.SessionStateValue(value))
		return err
	})
	testassert.NoError(t, err)
}

// TestPreparationReloadDoesNotRecaptureInFlightPreparation parks a real
// registered admission hook after its capture, publishes revision 2, releases
// it, and verifies the in-flight admission commits revision 1 with the old
// catalog price while the next capture uses revision 2 — for normal, drained
// and Fork admission alike, on both stores.
func TestPreparationReloadDoesNotRecaptureInFlightPreparation(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		mode harness.MessageMode
		want harness.SubmitDisposition
	}{
		{name: "normal", kind: "normal"},
		{name: "drained queued delivery", kind: "drained", mode: harness.MessageModeQueued, want: harness.DispositionQueued},
		{name: "drained regular delivery", kind: "drained", mode: harness.MessageModeRegular, want: harness.DispositionSteering},
		{name: "fork", kind: "fork"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eachPrepStore(t, func(t *testing.T, store harness.Storage) {
				e := newPrepEnv(t, store)
				session := e.session("dual")

				release := make(chan struct{})
				arrived := make(chan struct{}, 1)
				heldDone := make(chan struct{})
				var mu sync.Mutex
				var submitErr error
				var forked harness.SessionRecord

				switch tc.kind {
				case "drained":
					hold := make(chan struct{})
					e.server.setHold(hold)
					e.admit(session, "op-1", "one")
					<-e.server.arrived
					{
						res, err := e.submit(context.Background(), session, "op-2", "two", tc.mode)
						testassert.Require(t, err == nil && res.Disposition == tc.want, "buffered submit = %+v err %v, want disposition %s", res, err, tc.want)
					}
					e.hooks[0].parkNext(release, arrived)
					close(hold) // op-1 converges; the post-terminal drain reaches the parked hook
				case "normal":
					e.admit(session, "op-1", "one")
					e.awaitTerminal(session, "op-1", harness.OperationSuccess)
					e.hooks[0].parkNext(release, arrived)
					go func() {
						_, err := e.submit(context.Background(), session, "op-2", "two", harness.MessageModeRegular)
						mu.Lock()
						submitErr = err
						mu.Unlock()
						close(heldDone)
					}()
				case "fork":
					e.admit(session, "op-1", "one")
					e.awaitTerminal(session, "op-1", harness.OperationSuccess)
					e.hooks[0].parkNext(release, arrived)
					boundary := e.inputEntry(session, "op-1")
					go func() {
						res, err := e.fork(context.Background(), harness.ForkRequest{Origin: harness.InputOriginUser,
							SourceSessionID: session,
							BoundaryEntryID: boundary,
							OperationID:     "fork-op",
							Content:         []model.ContentPart{{Kind: model.PartText, Text: "fork input"}},
						})
						mu.Lock()
						if err == nil {
							forked = res.Session
						}
						submitErr = err
						mu.Unlock()
						close(heldDone)
					}()
				}
				<-arrived
				// The reload varies the selected model's catalog price, so
				// the revision-2 capture differs from the revision-1 one.
				writeServiceFile(t, e.configPath, strings.Replace(prepConfigDocument(e.server.URL, "A"),
					`"cost":{"input":1.5,"output":3,"cache_read":0.5,"cache_write":0.25}`,
					`"cost":{"input":9,"output":3,"cache_read":0.5,"cache_write":0.25}`, 1))
				writeAgentDefinitions(t, e.configPath, prepAgentsDocument)
				{
					err := e.Reload()
					testassert.NoError(t, err)
				}
				close(release)
				if tc.kind != "drained" {
					<-heldDone // the held admission's caller finished its submit
				}

				mu.Lock()
				err := submitErr
				mu.Unlock()
				testassert.NoError(t, err)

				// The in-flight admission committed revision 1 and the
				// revision-1 catalog price: the concurrent rate edit never
				// repriced it.
				var inFlight harness.OperationRecord
				if tc.kind == "fork" {
					mu.Lock()
					destination := forked.Identity.SessionID
					mu.Unlock()
					inFlight = e.awaitTerminal(destination, "fork-op", harness.OperationSuccess)
				} else {
					inFlight = e.awaitTerminal(session, "op-2", harness.OperationSuccess)
				}
				testassert.Require(t, inFlight.Admission.Execution.ConfigurationRevision == "1", "reloaded revision leaked into the committed capture: %v", inFlight.Admission.Execution.ConfigurationRevision)
				{
					captured := inFlight.Admission.Execution.Pricing
					testassert.Require(t, captured != nil && captured.Input != nil && *captured.Input == 1.5 && captured.Output != nil && *captured.Output == 3,
						"in-flight capture pricing = %+v, want the revision-1 catalog rates", captured)
				}

				// The next capture uses revision 2 and the reloaded rate.
				e.admitOrBuffer(session, "op-next", "next")
				next := e.awaitTerminal(session, "op-next", harness.OperationSuccess)
				testassert.Require(t, next.Admission.Execution.ConfigurationRevision == "2", "next admission revision = %v, want 2", next.Admission.Execution.ConfigurationRevision)
				{
					captured := next.Admission.Execution.Pricing
					testassert.Require(t, captured != nil && captured.Input != nil && *captured.Input == 9,
						"next capture pricing = %+v, want the reloaded revision-2 catalog rate", captured)
				}
				testassert.NoError(t, e.converge())
			})
		})
	}
}

// TestPreparationDeliveryIdentity proves on both stores that existing IDs skip
// preparation and buffered messages capture only on delivery: the counts are
// a real registered admission hook and the HTTP requests.
func TestPreparationDeliveryIdentity(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		e := newPrepEnv(t, store)
		session := e.session("dual")
		hold := make(chan struct{})
		e.server.setHold(hold)
		e.admit(session, "op-1", "one")
		<-e.server.arrived

		{
			res, err := e.submit(context.Background(), session, "op-1", "again", harness.MessageModeRegular)
			testassert.Require(t, err == nil && res.Disposition == harness.DispositionExisting, "reused id = %+v err %v, want existing without preparation", res, err)
		}
		{
			res, err := e.submit(context.Background(), session, "op-2", "two", harness.MessageModeQueued)
			testassert.Require(t, err == nil && res.Disposition == harness.DispositionQueued, "queued = %+v err %v, want queued", res, err)
		}
		testassert.Equal(t, len(e.hooks[0].calls()), 1)
		testassert.Equal(t, e.server.requests(), 1)
		close(hold)
		e.awaitTerminal(session, "op-1", harness.OperationSuccess)
		e.awaitTerminal(session, "op-2", harness.OperationSuccess)
		testassert.Equal(t, len(e.hooks[0].calls()), 2)
		testassert.Equal(t, e.server.requests(), 2)
		testassert.NoError(t, e.converge())
	})
}

// TestPreparationRejectsAdmissionAtTheBoundary proves on both stores that
// every pre-admission rejection reports its class at admission through the
// concrete Open path, prepares or opens nothing, and admits no Operation.
func TestPreparationRejectsAdmissionAtTheBoundary(t *testing.T) {
	cases := []struct {
		name      string
		agentType string
	}{
		{"unknown agent type", "ghost"},
		{"no model configured", "ghostly"},
		{"selected model without a positive context window", "shallow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eachPrepStore(t, func(t *testing.T, store harness.Storage) {
				e := newPrepEnv(t, store)
				t.Cleanup(func() { e.converge() })
				session := e.session(tc.agentType)
				res, err := e.submit(context.Background(), session, "op-x", "x", harness.MessageModeRegular)
				testassert.Require(t, errors.Is(err, harness.ErrInvalid), "submit error = %v, want harness.ErrInvalid", err)
				testassert.Require(t, res.Disposition != harness.DispositionAdmitted, "rejected preparation admitted an Operation")
				{
					_, err := e.readOperation(context.Background(), session, "op-x")
					testassert.Require(t, errors.Is(err, harness.ErrNotFound), "rejected admission left an Operation: %v", err)
				}
				testassert.Equal(t, e.server.requests(), 0)
				testassert.NoError(t, e.converge())
			})
		})
	}
}

// TestPreparationPreparationHookErrorOnDrainedAndForkDelivery proves on both
// stores that the queued-drain and Fork admission paths surface one
// admission-hook error with no partial consequence: the rejected delivery
// leaves no Operation or input entry while the next delivery still admits,
// and the failed Fork publishes no destination Session or prefix copy and
// leaves the source unchanged.
func TestPreparationPreparationHookErrorOnDrainedAndForkDelivery(t *testing.T) {
	t.Run("rejected queued delivery adds nothing and the next still admits", func(t *testing.T) {
		eachPrepStore(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			t.Cleanup(func() { e.converge() })
			session := e.session("dual")
			hold := make(chan struct{})
			e.server.setHold(hold)
			e.admit(session, "op-1", "one")
			<-e.server.arrived
			e.hooks[0].failNext(errHook)
			{
				res, err := e.submit(context.Background(), session, "op-2", "two", harness.MessageModeQueued)
				testassert.Require(t, err == nil && res.Disposition == harness.DispositionQueued, "queued submit = %+v err %v, want queued", res, err)
			}
			{
				res, err := e.submit(context.Background(), session, "op-3", "three", harness.MessageModeQueued)
				testassert.Require(t, err == nil && res.Disposition == harness.DispositionQueued, "queued submit = %+v err %v, want queued", res, err)
			}
			close(hold)
			e.awaitTerminal(session, "op-1", harness.OperationSuccess)
			e.awaitTerminal(session, "op-3", harness.OperationSuccess)
			{
				_, err := e.readOperation(context.Background(), session, "op-2")
				testassert.Require(t, errors.Is(err, harness.ErrNotFound), "rejected delivery left an Operation: %v", err)
			}
			entries, err := store.ReadEntries(context.Background(), session, 0)
			testassert.NoError(t, err)
			for _, entry := range entries {
				testassert.Require(t, entry.OperationID != "op-2", "rejected delivery left entry %s (%s) behind", entry.ID, entry.Kind)
			}
			testassert.NoError(t, e.converge())
		})
	})
	t.Run("failed Fork publishes no destination and leaves the source unchanged", func(t *testing.T) {
		eachPrepStore(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			t.Cleanup(func() { e.converge() })
			session := e.session("dual")
			e.admit(session, "op-1", "one")
			e.awaitTerminal(session, "op-1", harness.OperationSuccess)
			boundary := e.inputEntry(session, "op-1")
			sessionsBefore, err := store.ListSessionIDs(context.Background())
			testassert.NoError(t, err)
			entriesBefore, err := store.ReadEntries(context.Background(), session, 0)
			testassert.NoError(t, err)
			sourceBeforeView, err := e.sessionView(context.Background(), session)
			testassert.NoError(t, err)
			e.hooks[0].failNext(errHook)
			res, err := e.fork(context.Background(), harness.ForkRequest{Origin: harness.InputOriginUser,
				SourceSessionID: session,
				BoundaryEntryID: boundary,
				OperationID:     "fork-1",
				Content:         []model.ContentPart{{Kind: model.PartText, Text: "fork input"}},
			})
			testassert.Require(t, errors.Is(err, errHook), "fork with a failing hook = %+v err %v, want the hook's own error", res, err)
			sessionsAfter, err := store.ListSessionIDs(context.Background())
			testassert.NoError(t, err)
			testassert.Require(t, slices.Equal(sessionsAfter, sessionsBefore), "session listing after the failed fork = %v, want unchanged %v", sessionsAfter, sessionsBefore)
			entriesAfter, err := store.ReadEntries(context.Background(), session, 0)
			testassert.NoError(t, err)
			testassert.Equal(t, len(entriesAfter), len(entriesBefore))
			sourceAfterView, err := e.sessionView(context.Background(), session)
			testassert.NoError(t, err)
			// The run's own retirement publications advance the local counter
			// between the two reads; the durable metadata and the register
			// totals must stay exactly as they were.
			afterHeader, beforeHeader := sourceAfterView.Header, sourceBeforeView.Header
			afterHeader.Revision.LocalRevision, beforeHeader.Revision.LocalRevision = 0, 0
			testassert.Require(t, reflect.DeepEqual(afterHeader, beforeHeader) && reflect.DeepEqual(sourceAfterView.TotalUsage, sourceBeforeView.TotalUsage), "source session after the failed fork changed: %+v vs %+v", sourceAfterView, sourceBeforeView)
			testassert.Equal(t, len(e.hooks[0].calls()), 2)
			testassert.NoError(t, e.converge())
		})
	})
}

// TestPreparationAdmissionCandidateClosedValidation exercises the registered
// admission result rules through the real Harness: an accepted candidate may
// empty the advertisement or replace a definition, while unknown or duplicate
// tools, a partial model, and an invalid strategy reject the admission with
// no Operation published.
func TestPreparationAdmissionCandidateClosedValidation(t *testing.T) {
	t.Run("an empty candidate advertises nothing and a present empty prompt removes it", func(t *testing.T) {
		eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			t.Cleanup(func() { e.converge() })
			e.hooks[1].setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
				empty := ""
				candidate := in.Candidate
				candidate.Prompt = &empty
				candidate.Tools = []model.ToolDefinition{}
				return extension.AdmissionResult{Candidate: &candidate}, nil
			})
			session := e.session("wide")
			res := e.admit(session, "op-1", "x")
			e.awaitTerminal(session, "op-1", harness.OperationSuccess)
			got := res.Operation.Admission.Execution
			testassert.Equal(t, got.SystemPrompt, "")
			testassert.Require(t, got.InitialToolNames != nil && len(got.InitialToolNames) == 0, "initial names = %v, want present empty", got.InitialToolNames)
			testassert.Require(t, slices.Contains(toolNames(got.Tools), "echo") && slices.Contains(toolNames(got.Tools), "read"), "capture catalog = %v, want the registered definitions retained beside the empty selection", toolNames(got.Tools))
		})
	})
	violations := []struct {
		name   string
		mutate func(in extension.AdmissionInput) extension.AdmissionCandidate
	}{
		{"tool outside the catalog", func(in extension.AdmissionInput) extension.AdmissionCandidate {
			candidate := in.Candidate
			candidate.Tools = append(slices.Clone(candidate.Tools), model.ToolDefinition{Name: "extra", Parameters: json.RawMessage(`{}`)})
			return candidate
		}},
		{"duplicate tool", func(in extension.AdmissionInput) extension.AdmissionCandidate {
			candidate := in.Candidate
			candidate.Tools = append(slices.Clone(candidate.Tools), candidate.Tools[0])
			return candidate
		}},
		{"partial model", func(in extension.AdmissionInput) extension.AdmissionCandidate {
			candidate := in.Candidate
			candidate.Model = model.ModelRef{Provider: "prov"}
			return candidate
		}},
		{"invalid strategy", func(in extension.AdmissionInput) extension.AdmissionCandidate {
			candidate := in.Candidate
			candidate.Strategy.ID = "ghost.strategy"
			return candidate
		}},
		{"malformed strategy settings", func(in extension.AdmissionInput) extension.AdmissionCandidate {
			candidate := in.Candidate
			candidate.Strategy.Settings = json.RawMessage(`{`)
			return candidate
		}},
	}
	for _, v := range violations {
		t.Run(v.name, func(t *testing.T) {
			eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
				e := newPrepEnv(t, store)
				t.Cleanup(func() { e.converge() })
				e.hooks[1].setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
					candidate := v.mutate(in)
					return extension.AdmissionResult{Candidate: &candidate}, nil
				})
				session := e.session("wide")
				{
					_, err := e.submit(context.Background(), session, "op-1", "x", harness.MessageModeRegular)
					testassert.Require(t, errors.Is(err, harness.ErrInvalid), "submit error = %v, want harness.ErrInvalid", err)
				}
				{
					_, err := e.readOperation(context.Background(), session, "op-1")
					testassert.Require(t, errors.Is(err, harness.ErrNotFound), "rejected hook left an Operation: %v", err)
				}
			})
		})
	}
}

// TestPreparationHooksAreOrderedAndValidated exercises the registered
// admission boundary through the real Harness on both stores: input mutation
// is a non-effect, hook errors, cancellation while a hook is parked, and
// cancellation between hooks all abort admission with no partial consequence
// published.
func TestPreparationHooksAreOrderedAndValidated(t *testing.T) {
	t.Run("hook input mutation cannot change the core-owned capture", func(t *testing.T) {
		violations := []struct {
			name   string
			mutate func(in extension.AdmissionInput)
		}{
			{"model", func(in extension.AdmissionInput) {
				in.Candidate.Model = model.ModelRef{Provider: "prov", Model: "other"}
			}},
			{"instructions", func(in extension.AdmissionInput) {
				in.Candidate.Instructions = "mutated instructions"
			}},
			{"prompt", func(in extension.AdmissionInput) {
				mutated := "mutated prompt"
				in.Candidate.Prompt = &mutated
			}},
			{"tool definition", func(in extension.AdmissionInput) {
				in.Candidate.Tools[0].Description = "mutated description"
				in.Candidate.Tools[0].Name = "renamed"
			}},
			{"tool parameter bytes", func(in extension.AdmissionInput) {
				in.Candidate.Tools[0].Parameters[0] = 'X'
			}},
			{"model catalog entry", func(in extension.AdmissionInput) {
				in.ModelCatalog[0].Model = "mutated"
			}},
			{"tool catalog entry", func(in extension.AdmissionInput) {
				in.ToolCatalog[0].Description = "mutated description"
			}},
		}
		for _, v := range violations {
			t.Run(v.name, func(t *testing.T) {
				eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
					e := newPrepEnv(t, store)
					e.hooks[0].setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
						v.mutate(in)
						return extension.AdmissionResult{}, nil
					})
					session := e.session("wide")
					res := e.admit(session, "op-1", "x")
					e.awaitTerminal(session, "op-1", harness.OperationSuccess)
					got := res.Operation.Admission.Execution
					testassert.Require(t, got.Model == prepModelRef, "capture model = %s, want the admitted value", got.Model.String())
					testassert.Require(t, slices.Equal(got.InitialToolNames, []string{"echo", "read"}), "capture selection = %v, want the admitted selection", got.InitialToolNames)
					echo, ok := capturedTool(got.Tools, "echo")
					testassert.Require(t, ok && echo.Description == "echo" && echo.Parameters[0] == '{', "capture echo = %+v, want the definition's untouched metadata", echo)
					testassert.Require(t, got.Pricing != nil && got.Pricing.Input != nil && *got.Pricing.Input == 1.5 && got.Compact.Model == prepModelRef, "capture pricing/compact = %+v, want the catalog-owned values", got)
					testassert.Require(t, got.Strategy.ID == harness.IncludedStrategyID && got.ConfigurationRevision == "1", "capture strategy/revision = %+v/%q, want the core-owned values", got.Strategy, got.ConfigurationRevision)
					testassert.Require(t, got.SystemPrompt != "mutated prompt" && strings.HasSuffix(got.SystemPrompt, "|second"), "capture prompt = %.120s, want the admitted baseline with the accepted contribution only", got.SystemPrompt)
					testassert.NoError(t, e.converge())
				})
			})
		}
	})
	t.Run("hooks receive and return owned candidates", func(t *testing.T) {
		eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			e.hooks[0].setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
				candidate := in.Candidate
				candidate.Tools[0].Description = "tampered input" // in-place on the hook's own copy
				candidate.Tools[0].Description = "hook.first"
				return extension.AdmissionResult{Candidate: &candidate}, nil
			})
			e.hooks[1].setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
				candidate := in.Candidate
				candidate.Tools[0].Description = "hook.second"
				return extension.AdmissionResult{Candidate: &candidate}, nil
			})
			session := e.session("wide")
			res := e.admit(session, "op-1", "x")
			e.awaitTerminal(session, "op-1", harness.OperationSuccess)
			// Each hook's candidate carried the previous accepted description
			// (fresh owned copies chain), and mutating one hook's own copy
			// cannot change the value the following hook or the capture sees.
			{
				firstTools, firstDesc := e.hooks[0].retainedInputs()
				testassert.Require(t, firstDesc == "echo" && len(firstTools) != 0 && firstTools[0].Description == "hook.first", "hook.first input = %+v description %q, want its own accepted copy over the pre-callback definition", firstTools, firstDesc)
			}
			{
				_, secondDesc := e.hooks[1].retainedInputs()
				testassert.Equal(t, secondDesc, "hook.first")
			}
			// The committed capture carries the final accepted result.
			{
				echo, ok := capturedTool(res.Operation.Admission.Execution.Tools, "echo")
				testassert.Require(t, ok && echo.Description == "hook.second", "committed echo = %+v, want the final accepted description", echo)
			}
			testassert.NoError(t, e.converge())
		})
	})
	t.Run("contributions apply prompt and visibility in attachment order", func(t *testing.T) {
		eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			e.hooks[0].setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
				prompt := "FIRST"
				candidate := in.Candidate
				candidate.Prompt = &prompt
				candidate.Tools = []model.ToolDefinition{}
				candidate.Tools = append(candidate.Tools, model.ToolDefinition{
					Name:        "echo",
					Description: "first echo",
					Parameters:  json.RawMessage(`{"type":"object"}`),
				})
				return extension.AdmissionResult{Candidate: &candidate}, nil
			})
			e.hooks[1].setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
				testassert.Require(t, len(in.Candidate.Tools) == 1 && in.Candidate.Tools[0].Description == "first echo", "second hook candidate = %+v, want the first hook's accepted tool", in.Candidate.Tools)
				prompt := "SECOND"
				candidate := in.Candidate
				candidate.Prompt = &prompt
				return extension.AdmissionResult{Candidate: &candidate}, nil
			})
			session := e.session("wide")
			res := e.admit(session, "op-1", "x")
			e.awaitTerminal(session, "op-1", harness.OperationSuccess)
			got := res.Operation.Admission.Execution
			testassert.Equal(t, got.SystemPrompt, "SECOND")
			testassert.Require(t, slices.Equal(got.InitialToolNames, []string{"echo"}), "capture initial visibility = %v, want the accepted selection", got.InitialToolNames)
			echo, ok := capturedTool(got.Tools, "echo")
			testassert.Require(t, ok && echo.Description == "first echo", "capture echo = %+v, want the accepted definition", echo)
			testassert.NoError(t, e.converge())
		})
	})
	t.Run("hook error aborts admission", func(t *testing.T) {
		eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			e.hooks[0].setAdmission(errHook, nil)
			session := e.session("wide")
			{
				_, err := e.submit(context.Background(), session, "op-1", "x", harness.MessageModeRegular)
				testassert.Require(t, errors.Is(err, errHook), "submit error = %v, want %v", err, errHook)
			}
			{
				_, err := e.readOperation(context.Background(), session, "op-1")
				testassert.Require(t, errors.Is(err, harness.ErrNotFound), "failed hook left an Operation: %v", err)
			}
			testassert.NoError(t, e.converge())
		})
	})
	t.Run("cancellation while a hook is parked aborts admission", func(t *testing.T) {
		eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			started := make(chan struct{}, 1)
			e.hooks[0].blockUntilCanceled(started)
			session := e.session("dual")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := e.submit(ctx, session, "op-1", "x", harness.MessageModeRegular)
				done <- err
			}()
			<-started
			cancel()
			{
				err := <-done
				testassert.Require(t, errors.Is(err, context.Canceled), "canceled submit error = %v, want context.Canceled", err)
			}
			{
				_, err := e.readOperation(context.Background(), session, "op-1")
				testassert.Require(t, errors.Is(err, harness.ErrNotFound), "canceled preparation left an Operation: %v", err)
			}
			testassert.NoError(t, e.converge())
		})
	})
	t.Run("cancellation between hooks aborts admission", func(t *testing.T) {
		eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			started := make(chan struct{}, 1)
			e.hooks[0].blockUntilCanceled(started)
			session := e.session("wide")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := e.submit(ctx, session, "op-1", "x", harness.MessageModeRegular)
				done <- err
			}()
			<-started
			cancel()
			{
				err := <-done
				testassert.Require(t, errors.Is(err, context.Canceled), "canceled submit error = %v, want context.Canceled", err)
			}
			{
				_, err := e.readOperation(context.Background(), session, "op-1")
				testassert.Require(t, errors.Is(err, harness.ErrNotFound), "canceled preparation left an Operation: %v", err)
			}
			testassert.Equal(t, e.server.requests(), 0)
			_ = e.converge()
		})
	})
	t.Run("an admission refusal aborts with no Operation", func(t *testing.T) {
		eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
			e := newPrepEnv(t, store)
			e.hooks[0].setAdmission(nil, func(extension.AdmissionInput) (extension.AdmissionResult, error) {
				refusal := "fixture refusal"
				return extension.AdmissionResult{Refusal: &refusal}, nil
			})
			session := e.session("wide")
			{
				_, err := e.submit(context.Background(), session, "op-1", "x", harness.MessageModeRegular)
				testassert.Require(t, errors.Is(err, harness.ErrInvalid), "refused submit error = %v, want harness.ErrInvalid", err)
			}
			{
				_, err := e.readOperation(context.Background(), session, "op-1")
				testassert.Require(t, errors.Is(err, harness.ErrNotFound), "refused admission left an Operation: %v", err)
			}
			testassert.Equal(t, e.server.requests(), 0)
			testassert.NoError(t, e.converge())
		})
	})
}

// TestPreparationStoredChoicesMergeWithDefaults proves the defaults/Session
// choice merge through the real owner: host defaults select the model, a
// stored Session override wins per member, stored `tools:[]` advertises
// nothing while omitted tools keep the admitted set, a stored tool name that
// matches no registered tool is omitted, and a missing stored strategy keeps
// the included one.
func TestPreparationStoredChoicesMergeWithDefaults(t *testing.T) {
	eachPrepStoreOnce(t, func(t *testing.T, store harness.Storage) {
		e := newPrepEnv(t, store)
		t.Cleanup(func() { e.converge() })
		e.server.setScript(func(_ context.Context, _ string) []string { return textTurnEvents("done") })
		create := func(inputs harness.SessionInputs) string {
			var record harness.SessionRecord
			err := e.r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
				var err error
				record, err = h.CreateSession(ctx, harness.CreateSessionRequest{Workspace: e.workspace, Inputs: inputs.Owned()})
				return err
			})
			testassert.NoError(t, err)
			return record.Identity.SessionID
		}
		instructions := "stored instructions"
		ghost := "no_such_tool"

		// Omitted stored choices keep the full admitted catalog.
		omitted := create(harness.SessionInputs{Model: &prepModelRef})
		e.admit(omitted, "op-omitted", "one")
		omittedRec := e.awaitTerminal(omitted, "op-omitted", harness.OperationSuccess)
		testassert.Require(t, slices.Contains(omittedRec.Admission.Execution.InitialToolNames, "echo"), "omitted tools = %v, want the full admitted catalog", omittedRec.Admission.Execution.InitialToolNames)

		// tools:[] advertises nothing even though the hook chain still runs.
		none := create(harness.SessionInputs{Model: &prepModelRef, Tools: []string{}})
		e.admit(none, "op-none", "one")
		noneRec := e.awaitTerminal(none, "op-none", harness.OperationSuccess)
		testassert.Require(t, noneRec.Admission.Execution.InitialToolNames != nil && len(noneRec.Admission.Execution.InitialToolNames) == 0, "stored empty tools = %v, want present empty", noneRec.Admission.Execution.InitialToolNames)

		// A stored tool list keeps only names that exist and omits the rest.
		mixed := create(harness.SessionInputs{Model: &prepModelRef, Tools: []string{"echo", ghost}})
		e.admit(mixed, "op-mixed", "one")
		mixedRec := e.awaitTerminal(mixed, "op-mixed", harness.OperationSuccess)
		testassert.Require(t, slices.Equal(mixedRec.Admission.Execution.InitialToolNames, []string{"echo"}), "stored mixed tools = %v, want the ghost omitted", mixedRec.Admission.Execution.InitialToolNames)

		// A stored instructions override reaches the assembled prompt, and a
		// missing stored strategy keeps the included strategy.
		overridden := create(harness.SessionInputs{Instructions: &instructions, Model: &prepModelRef})
		e.admit(overridden, "op-over", "one")
		overRec := e.awaitTerminal(overridden, "op-over", harness.OperationSuccess)
		testassert.Require(t, strings.Contains(overRec.Admission.Execution.SystemPrompt, instructions), "capture prompt = %.120s, want the stored instructions", overRec.Admission.Execution.SystemPrompt)
		testassert.Require(t, !strings.Contains(omittedRec.Admission.Execution.SystemPrompt, instructions), "the unoverridden capture carried the override")
		for _, rec := range []harness.OperationRecord{omittedRec, noneRec, mixedRec, overRec} {
			testassert.Require(t, rec.Admission.Execution.Strategy.ID == harness.IncludedStrategyID, "capture strategy = %+v, want the included strategy", rec.Admission.Execution.Strategy)
		}
	})
}

// TestPreparationForwardsToolArgumentHooks proves the argument hook row
// through the real preparation, Harness and Agent on both stores: the
// registered argument hooks bind in attachment order, carry their owning
// extension identity, and the first hook receives the invalid raw arguments
// and repairs them; every replacement commits as its own durable
// hook_result effect; the final committed value reaches the concrete tool
// byte-identically; and the Operation settles success.
func TestPreparationForwardsToolArgumentHooks(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		e := newPrepEnv(t, store)
		t.Cleanup(func() { e.converge() })
		e.server.setScript(func(_ context.Context, body string) []string {
			if lastMessageRole(body) == "tool" {
				return nil // the default completed text turn
			}
			return toolCallTurnEvents("call-1", "echo", ` [not json `)
		})
		session := e.session("hooky")
		e.admit(session, "op-1", "one")
		{
			rec := e.awaitTerminal(session, "op-1", harness.OperationSuccess)
			testassert.Require(t, rec.State.Status == harness.OperationSuccess, "operation = %+v, want terminal success after the hook chain", rec.State)
		}

		normalized, executed, toolInput := e.echo.counters()
		testassert.Require(t, normalized == 1, "bound normalization ran %d times, want once at dispatch", normalized)
		testassert.Require(t, executed == 1 && len(toolInput) == 1 && toolInput[0] == `{"repaired":"arg.agent"}`, "concrete tool ran %d times on %q, want once with the final committed replacement", executed, toolInput)

		// The committed tool result records the minted implementation and
		// every accepted argument hook's identity in attachment order.
		entries, err := store.ReadEntries(context.Background(), session, 0)
		testassert.NoError(t, err)
		type resultWire struct {
			ToolCallID          string                     `json:"tool_call_id"`
			NormalizedArguments []byte                     `json:"normalized_arguments_base64"`
			Implementation      harness.ToolImplementation `json:"implementation"`
			HookIDs             []struct {
				ExtensionID string `json:"extension_id"`
				HookID      string `json:"hook_id"`
			} `json:"hook_ids"`
		}
		var result *resultWire
		for _, entry := range entries {
			if entry.Kind != harness.EntryToolResult {
				continue
			}
			var wire resultWire
			testassert.NoError(t, json.Unmarshal(entry.Payload, &wire))
			if wire.ToolCallID == "call-1" {
				result = &wire
			}
		}
		testassert.Require(t, result != nil, "no committed tool result for call-1")
		testassert.Require(t, result.Implementation.ExtensionID == "hooks" && result.Implementation.ID != "echo" && len(result.Implementation.ID) == 32, "committed implementation = %+v, want the minted binding", result.Implementation)
		wantExtensions := []string{"agents", "hooks", "hooks", "caps", "caps"} // the owning extension of each registered hook
		testassert.Require(t, len(result.HookIDs) == len(wantExtensions), "committed hook identities = %+v, want one per argument hook", result.HookIDs)
		seenIDs := make(map[string]bool)
		for i, want := range wantExtensions {
			got := result.HookIDs[i]
			testassert.Require(t, got.ExtensionID == want && got.HookID != "arg.runtime" && got.HookID != want && !seenIDs[got.HookID] && len(got.HookID) == 32, "hook identity %d = %+v, want a minted id from extension %q", i, got, want)
			seenIDs[got.HookID] = true
		}
		testassert.Require(t, string(result.NormalizedArguments) == `{"repaired":"arg.agent"}`, "last committed arguments = %s, want the final hook's replacement", result.NormalizedArguments)

		// The chain: the first hook received the invalid raw arguments and
		// every later one the preceding replacement; the owning extension
		// identity reached every hook.
		for i, hook := range e.argHooks {
			seen, owners := hook.received()
			testassert.Require(t, len(seen) == 1, "hook %d ran %d times, want once", i, len(seen))
			testassert.Require(t, owners[0] == wantExtensions[i+1], "hook %d owner = %q, want %q", i, owners[0], wantExtensions[i+1])
		}
		{
			first, _ := e.argHooks[0].received()
			testassert.Require(t, first[0] == ` [not json `, "first hook received %q, want the invalid raw arguments", first)
		}

		// The registered admission hooks ran at preparation and never bound
		// as an argument hook.
		testassert.Require(t, len(e.hooks[0].calls()) != 0, "admission hook never ran")
		for _, identity := range result.HookIDs {
			testassert.Require(t, identity.HookID != "hook.first", "admission hook bound as an argument hook")
		}
	})
}

type visibilityTool struct{ executes *atomic.Int64 }

func (t visibilityTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (t visibilityTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Data: []byte(call.ID)}
}
func (t visibilityTool) Execute(_ context.Context, _ extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	t.executes.Add(1)
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: string(plan.Data), Status: model.ResultSuccess, Content: "hidden-ok"}}
}

type visibilityRequestTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type visibilityRequest struct {
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Tools []struct {
		Function visibilityRequestTool `json:"function"`
	} `json:"tools"`
}

func parseVisibilityRequest(t *testing.T, body string) visibilityRequest {
	t.Helper()
	var doc visibilityRequest
	testassert.NoError(t, json.Unmarshal([]byte(body), &doc))
	return doc
}

func visibilityToolMessages(doc visibilityRequest) int {
	count := 0
	for _, message := range doc.Messages {
		if message.Role == "tool" {
			count++
		}
	}
	return count
}

// TestPreparationVisibilityRestoresThroughRequestHooks proves the separation
// between the full admitted catalog with its hook-transformed definition and
// the per-request advertisement: a request hook that advertises nothing makes
// the call refused, and the next request restores the tool with the
// transformed description; initial order is preserved; a present empty stored
// selection advertises nothing; an ineligible restore is refused; and the
// canonical capture survives a cold reopen.
func TestPreparationVisibilityRestoresThroughRequestHooks(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		e := newOwnerEnv(t)
		writeServiceFile(t, e.configPath, prepConfigDocument(e.server.URL, "A"))
		writeAgentDefinitions(t, e.configPath, `{
  "restorer": {"model": "prov/m", "system_prompt": "simple", "tools": ["hidden", "visible"]},
  "reorderer": {"model": "prov/m", "system_prompt": "simple", "tools": ["visible", "hidden"]},
  "empty": {"model": "prov/m", "system_prompt": "simple", "tools": []},
  "foreigner": {"model": "prov/m", "system_prompt": "simple", "tools": ["hidden", "visible"]}
}`)
		enableFixturePlugin(t, e.configPath, "gate")
		e.fixtureStore(store)
		var executes atomic.Int64
		gate := testNative("gate", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			for _, id := range []string{"hidden", "visible"} {
				if _, err := api.RegisterTool(testDescribeTool(id), visibilityTool{executes: &executes}, nil); err != nil {
					return nil, err
				}
			}
			if _, err := api.AttachAdmission(func(_ context.Context, call extension.AuthorContext, in extension.AdmissionInput) (extension.AdmissionResult, error) {
				var state struct {
					Agent string `json:"agent"`
				}
				if err := json.Unmarshal(call.State, &state); err != nil || state.Agent != "restorer" {
					return extension.AdmissionResult{}, nil
				}
				// The restorer's admission transforms the hidden description.
				candidate := in.Candidate
				tools := make([]model.ToolDefinition, len(candidate.Tools))
				for i, tool := range candidate.Tools {
					if tool.Name == "hidden" {
						tool.Description = "transformed hidden description"
					}
					tools[i] = tool
				}
				candidate.Tools = tools
				return extension.AdmissionResult{Candidate: &candidate}, nil
			}, nil); err != nil {
				return nil, err
			}
			_, err := api.AttachRequest(func(_ context.Context, call extension.AuthorContext, input extension.AuthorRequestInput) (harness.RequestConsequence, error) {
				var state struct {
					Agent string `json:"agent"`
				}
				if err := json.Unmarshal(call.State, &state); err != nil {
					return harness.RequestConsequence{}, nil
				}
				switch state.Agent {
				case "restorer":
					var hidden []model.ToolDefinition
					for _, tool := range input.Catalog {
						if tool.Name == "hidden" {
							hidden = append(hidden, tool)
						}
					}
					// The first request advertises nothing; the request after
					// the refused call restores the hidden tool, editing the
					// unavailable refusal text.
					for i, message := range input.Messages {
						if message.Role == model.RoleTool {
							if strings.Contains(message.TextContent(), "not available") {
								edits := make([]harness.RequestMessageEdit, len(input.Messages))
								for j := range edits {
									edits[j].Position = input.MessagePositions[j]
								}
								edits[i].Content = []model.ContentPart{{Kind: model.PartText, Text: "still unavailable"}}
								return harness.RequestConsequence{Edits: edits, Tools: hidden}, nil
							}
							return harness.RequestConsequence{Tools: hidden}, nil
						}
					}
					return harness.RequestConsequence{Tools: []model.ToolDefinition{}}, nil
				case "foreigner":
					return harness.RequestConsequence{Tools: []model.ToolDefinition{{Name: "foreign", Parameters: json.RawMessage(`{}`)}}}, nil
				}
				return harness.RequestConsequence{}, nil
			}, nil)
			return nil, err
		})
		e.server.setScript(func(_ context.Context, body string) []string {
			doc := visibilityRequest{Messages: nil}
			_ = json.Unmarshal([]byte(body), &doc)
			switch lastUserText(body) {
			case "restorer run":
				if visibilityToolMessages(doc) == 0 {
					return toolCallTurnEvents("call-hidden-1", "hidden", `{}`)
				}
				if visibilityToolMessages(doc) == 1 {
					return toolCallTurnEvents("call-hidden-2", "hidden", `{}`)
				}
				return textTurnEvents("done")
			case "reorder run":
				if visibilityToolMessages(doc) == 0 {
					return toolCallTurnEvents("call-reordered", "hidden", `{}`)
				}
				return textTurnEvents("done")
			default:
				return textTurnEvents("done")
			}
		})
		ctx := context.Background()
		r, err := e.open(ctx, agentsTestNative(), gate)
		testassert.NoError(t, err)

		restorerSession, err := r.createSession(ctx, e.dataDir, "restorer")
		testassert.NoError(t, err)
		restorer := restorerSession.Identity.SessionID
		setExtensionState(t, r, restorer, "gate", `{"agent":"restorer"}`)
		submitThroughRuntime(t, r, restorer, "op-restorer", "restorer run")
		restorerRec := awaitOperation(t, r, restorer, "op-restorer", harness.OperationSuccess)
		testassert.Require(t, slices.Equal(restorerRec.Admission.Execution.InitialToolNames, []string{"hidden", "visible"}), "restorer selection = %v, want the admitted selection", restorerRec.Admission.Execution.InitialToolNames)

		reordererSession, err := r.createSession(ctx, e.dataDir, "reorderer")
		testassert.NoError(t, err)
		reorderer := reordererSession.Identity.SessionID
		setExtensionState(t, r, reorderer, "gate", `{"agent":"reorderer"}`)
		submitThroughRuntime(t, r, reorderer, "op-reorder", "reorder run")
		reorderRec := awaitOperation(t, r, reorderer, "op-reorder", harness.OperationSuccess)
		testassert.Require(t, slices.Equal(reorderRec.Admission.Execution.InitialToolNames, []string{"visible", "hidden"}), "reordered initial visibility = %v, want the admitted order", reorderRec.Admission.Execution.InitialToolNames)

		emptySession, err := r.createSession(ctx, e.dataDir, "empty")
		testassert.NoError(t, err)
		empty := emptySession.Identity.SessionID
		setExtensionState(t, r, empty, "gate", `{"agent":"empty"}`)
		submitThroughRuntime(t, r, empty, "op-empty", "empty run")
		emptyRec := awaitOperation(t, r, empty, "op-empty", harness.OperationSuccess)
		testassert.Require(t, emptyRec.Admission.Execution.InitialToolNames != nil && len(emptyRec.Admission.Execution.InitialToolNames) == 0, "empty initial visibility = %v, want present empty", emptyRec.Admission.Execution.InitialToolNames)

		foreignerSession, err := r.createSession(ctx, e.dataDir, "foreigner")
		testassert.NoError(t, err)
		foreigner := foreignerSession.Identity.SessionID
		setExtensionState(t, r, foreigner, "gate", `{"agent":"foreigner"}`)
		beforeForeign := e.server.requests()
		submitThroughRuntime(t, r, foreigner, "op-foreign", "foreign run")
		awaitOperation(t, r, foreigner, "op-foreign", harness.OperationFailure)
		testassert.Equal(t, e.server.requests(), beforeForeign)
		testassert.Equal(t, executes.Load(), int64(2))

		// Find the restorer and reorderer request bodies by the actual wire
		// conversation and assert advertisement behavior.
		var restorerRequests, reorderRequests []visibilityRequest
		for i := 0; i < e.server.requests(); i++ {
			body := e.server.bodyAt(i)
			doc := parseVisibilityRequest(t, body)
			switch lastUserText(body) {
			case "restorer run":
				restorerRequests = append(restorerRequests, doc)
			case "reorder run":
				reorderRequests = append(reorderRequests, doc)
			}
		}
		testassert.Require(t, len(restorerRequests) >= 3, "restorer requests = %d, want the refused call, restored call, and follow-up", len(restorerRequests))
		testassert.Require(t, len(restorerRequests[0].Tools) == 0, "pre-restoration advertisement = %+v, want none", restorerRequests[0].Tools)
		testassert.Require(t, len(restorerRequests[1].Tools) == 1 && restorerRequests[1].Tools[0].Function.Name == "hidden" && restorerRequests[1].Tools[0].Function.Description == "transformed hidden description", "restored advertisement = %+v, want the hidden tool with the transformed description", restorerRequests[1].Tools)
		testassert.Require(t, strings.Contains(string(restorerRequests[1].Messages[len(restorerRequests[1].Messages)-1].Content), "still unavailable"), "pre-restoration tool result = %s, want the edited unavailable refusal", restorerRequests[1].Messages[len(restorerRequests[1].Messages)-1].Content)
		testassert.Require(t, strings.Contains(string(restorerRequests[2].Messages[len(restorerRequests[2].Messages)-1].Content), "hidden-ok"), "restored tool result = %s, want the executed content", restorerRequests[2].Messages[len(restorerRequests[2].Messages)-1].Content)
		testassert.Require(t, len(reorderRequests) >= 2 && len(reorderRequests[0].Tools) == 2 && reorderRequests[0].Tools[0].Function.Name == "visible" && reorderRequests[0].Tools[1].Function.Name == "hidden", "reordered advertisement = %+v, want visible then hidden", reorderRequests[0].Tools)

		testassert.NoError(t, r.Close(ctx))
		// Cold reopen: the canonical capture validates from durable bytes,
		// including the reordered admitted selection.
		r2, err := e.open(ctx, agentsTestNative(), gate)
		testassert.NoError(t, err)
		for _, row := range []struct {
			session, operation string
			want               []string
		}{
			{restorer, "op-restorer", []string{"hidden", "visible"}},
			{reorderer, "op-reorder", []string{"visible", "hidden"}},
			{empty, "op-empty", []string{}},
		} {
			err := r2.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
				rec, err := h.ReadOperation(ctx, row.session, row.operation)
				if err != nil {
					return err
				}
				if !slices.Equal(rec.Admission.Execution.InitialToolNames, row.want) {
					return fmt.Errorf("cold initial visibility = %v, want %v", rec.Admission.Execution.InitialToolNames, row.want)
				}
				return nil
			})
			testassert.NoError(t, err)
		}
		testassert.NoError(t, r2.Close(ctx))
	})
}

func TestRequestBodyParametersAcrossRetries(t *testing.T) {
	t.Setenv("PRODUCTION_TEST_KEY", "test-key")
	e := newOwnerEnv(t)
	config := strings.Replace(productionConfigDocument(e.server.URL, "A"), `"provider_side": "provider-value"`, `"provider_side":"provider-value","shared":"provider","_melta_hidden":true`, 1)
	config = strings.Replace(config, `"m": {"name":`, `"summary":{"name":"Summary","context_window":262144,"extra_body":{"model_side":"summary-value"}},"m": {"name":`, 1)
	config = strings.TrimSuffix(config, "}") + `,"compaction":{"model":"prov/summary"}}`
	writeServiceFile(t, e.configPath, config)
	writeAgentDefinitions(t, e.configPath, `{"body":{"model":"prov/m","system_prompt":"simple","tools":["prod_write","prod_read"]}}`)
	enableFixturePlugin(t, e.configPath, "gate")
	var attempts, hooks atomic.Int64
	gate := testNative("gate", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.AttachRequest(func(_ context.Context, _ extension.AuthorContext, input extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			hooks.Add(1)
			want := `"model-value"`
			if input.Purpose == harness.InvocationPurposeMaintenance {
				want = `"summary-value"`
			}
			if string(input.Parameters["shared"]) != `"provider"` || string(input.Parameters["model_side"]) != want || input.Parameters["_melta_hidden"] != nil {
				return harness.RequestConsequence{}, fmt.Errorf("defaults: %s", input.Parameters)
			}
			return harness.RequestConsequence{Parameters: model.Extra{"shared": json.RawMessage(`"first"`), "": json.RawMessage(`true`)}}, nil
		}, nil); err != nil {
			return nil, err
		}
		_, err := api.AttachRequest(func(_ context.Context, _ extension.AuthorContext, input extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			hooks.Add(1)
			if string(input.Parameters["shared"]) != `"first"` || string(input.Parameters[""]) != `true` {
				return harness.RequestConsequence{}, fmt.Errorf("preceding view: %s", input.Parameters)
			}
			return harness.RequestConsequence{Parameters: model.Extra{"shared": json.RawMessage(`"second"`)}}, nil
		}, nil)
		return nil, err
	})
	e.server.setScript(func(context.Context, string) []string {
		if attempts.Add(1)%2 == 1 {
			return []string{"HTTP 503", `{"error":{"message":"retry"}}`}
		}
		return textTurnEvents("done")
	})
	ctx := context.Background()
	r, err := e.open(ctx, productionToolsPlugin(), gate)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(ctx) })
	s, err := r.createSession(ctx, e.dataDir, "body")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, s.Identity.SessionID, "body", "hello")
	awaitOperation(t, r, s.Identity.SessionID, "body", harness.OperationSuccess)
	awaitRestorableSession(t, r, s.Identity.SessionID)
	_, err = r.CompactSession(ctx, testCaller(r), s.Identity.SessionID, protocol.CompactRequest{OperationId: "body-summary"})
	testassert.NoError(t, err)
	awaitOperation(t, r, s.Identity.SessionID, "body-summary", harness.OperationSuccess)
	testassert.Require(t, attempts.Load() == 4 && hooks.Load() == 4, "attempts=%d hooks=%d", attempts.Load(), hooks.Load())
	for i := 0; i < 4; i++ {
		var body map[string]json.RawMessage
		testassert.NoError(t, json.Unmarshal([]byte(e.server.bodyAt(i)), &body))
		wantModel, wantSide := `"m"`, `"model-value"`
		if i >= 2 {
			wantModel, wantSide = `"summary"`, `"summary-value"`
		}
		testassert.Require(t, string(body["shared"]) == `"second"` && string(body[""]) == `true` &&
			string(body["provider_number"]) == "9007199254740993" && string(body["model_side"]) == wantSide && string(body["model"]) == wantModel &&
			body["_melta_hidden"] == nil, "attempt %d: model=%s model_side=%s shared=%s empty=%s provider_number=%s private=%s",
			i+1, body["model"], body["model_side"], body["shared"], body[""], body["provider_number"], body["_melta_hidden"])
	}
}
