package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// Assembled-phase integration: one live scenario runs the concrete production
// preparation end to end over the real owner — real transports against a test
// model server, real tool dispatch through a dependent capability, configured
// hooks, a blocked Operation, a reload published over it, the queued next
// revision, independent Workspaces, observer saturation, shutdown joining the
// parked execution, and restart repair on the same store.

func assembledConfigDocument(endpoint, tag, label string) string {
	return `{"providers":{"prov":` + nominalProvMember(endpoint, "ASSEMBLED_TEST_KEY",
		`{"m":{"name":"M","context_window":262144}}`) + `},"extensions":{"hooks":{"enabled":true,"tag":` + fixtureString(tag) + `},"depper":{"enabled":true,"label":` + fixtureString(label) + `},"base":{"enabled":true},"probe":{"enabled":true}}}`
}

const assembledAgentsDocument = `{
  "integrated": {"model": "prov/m", "system_prompt": "simple", "tools": ["probe"]}
}`

// parkingHook parks exactly one armed admission until released, ignoring its
// context: the in-flight admitted call whose presence proves Close joins
// admitted work instead of abandoning it.
type parkingHook struct {
	mu      sync.Mutex
	armed   bool
	used    bool
	release chan struct{}
	usedSig chan struct{}
}

func (h *parkingHook) Prepare(_ context.Context, _ extension.AuthorContext, _ extension.AdmissionInput) (extension.AdmissionResult, error) {
	h.mu.Lock()
	park := h.armed && !h.used
	h.used = h.used || h.armed
	release := h.release
	h.mu.Unlock()
	if park {
		if h.usedSig != nil {
			select {
			case h.usedSig <- struct{}{}:
			default:
			}
		}
		<-release
	}
	return extension.AdmissionResult{}, nil
}

func (h *parkingHook) arm(release, used chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.armed, h.used, h.release, h.usedSig = true, false, release, used
}

// workspaceOpens is the assembled scenario's dependency park gate: one armed
// call parks the actual dependency invocation until its test releases it.
type workspaceOpens struct {
	mu      sync.Mutex
	gate    chan struct{}
	arrived chan struct{}
}

func (w *workspaceOpens) park() {
	w.mu.Lock()
	gate, arrived := w.gate, w.arrived
	w.gate = nil
	w.mu.Unlock()
	if gate == nil {
		return
	}
	if arrived != nil {
		select {
		case arrived <- struct{}{}:
		default:
		}
	}
	<-gate
}

// probeTool is the concrete registered tool of the assembled scenario: its
// preparation invokes the dependent service chain, so every real tool dispatch
// records one revision-scoped dependency invocation.
type probeTool struct {
	events *traceLog
}

func (t probeTool) run(ctx context.Context, tc extension.ToolCall) (extension.ToolResult, error) {
	result, err := tc.Call(ctx, "dep", "call", json.RawMessage(`{}`))
	if err != nil {
		return extension.ToolResult{}, err
	}
	var value struct {
		Result string `json:"result"`
	}
	if result.Failure != nil || json.Unmarshal(result.Value, &value) != nil {
		return extension.ToolResult{}, errors.New("undecodable dependency result")
	}
	t.events.add("work:" + value.Result)
	return extension.ToolResult{}, errors.New("probed")
}

// shPureService is one pure "call" service named name, answered by handler.
func shPureService(name string, handler extension.AuthorHandler) extension.Service {
	return extension.Service{Name: name, Methods: []extension.RegisteredMethod{{
		Method:  extension.Method{Name: "call", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{"type":"object"}`)},
		Handler: handler,
	}}}
}

// assembledPlugins composes the scenario's activation chain: base answers its
// own service, depper reads its own settings section and reaches base through
// that service, and probe's tool preparation reaches depper. Every activation
// records its own retirement, so the parked dependency call proves a retired
// activation closes only after the call holding it has finished.
func assembledPlugins(e *ownerEnv, store harness.Storage, hook *recordHook, ws *workspaceOpens) []extension.Extension {
	e.fixtureStore(store)
	base := testNative("base", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		closed := &atomic.Bool{}
		_, err := api.RegisterService(shPureService("dep.base", func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
			state := "base"
			if closed.Load() {
				state = "closed"
			}
			value, _ := json.Marshal(map[string]string{"state": state})
			return extension.Result{Value: value}, nil
		}), nil)
		return func(context.Context) error {
			closed.Store(true)
			e.events.add("close:base")
			return nil
		}, err
	})
	depper := testNative("depper", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		label := ""
		if section := api.Settings(); len(section) != 0 {
			var settings struct {
				Label string `json:"label"`
			}
			_ = json.Unmarshal(section, &settings)
			label = settings.Label
		}
		_, err := api.RegisterService(shPureService("dep", func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
			ws.park()
			if label == "" {
				return extension.Result{Value: json.RawMessage(`{"result":"undecodable"}`)}, nil
			}
			result, err := call.Call(ctx, "dep.base", "call", json.RawMessage(`{}`))
			if err != nil {
				return extension.Result{}, err
			}
			var base struct {
				State string `json:"state"`
			}
			if result.Failure != nil || json.Unmarshal(result.Value, &base) != nil {
				return extension.Result{Value: json.RawMessage(`{"result":"undecodable"}`)}, nil
			}
			value, _ := json.Marshal(map[string]string{"result": label + ":" + base.State})
			return extension.Result{Value: value}, nil
		}), nil)
		return func(context.Context) error {
			e.events.add("close:depper")
			return nil
		}, err
	})
	probe := testNative("probe", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(model.ToolDefinition{Name: "probe", Description: "records one dependency invocation", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}, (probeTool{events: e.events}).run, nil)
		return nil, err
	})
	return []extension.Extension{agentsTestNative(), hookPlugin(hook), base, depper, probe}
}

// lastMessageRole returns the wire role of the request body's last message.
func lastMessageRole(body string) string {
	var doc struct {
		Messages []struct {
			Role string `json:"role"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil || len(doc.Messages) == 0 {
		return ""
	}
	return doc.Messages[len(doc.Messages)-1].Role
}

// lastUserText returns the last user message's plain text content.
func lastUserText(body string) string {
	var doc struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		return ""
	}
	for i := len(doc.Messages) - 1; i >= 0; i-- {
		if doc.Messages[i].Role != "user" {
			continue
		}
		var text string
		if json.Unmarshal(doc.Messages[i].Content, &text) == nil {
			return text
		}
		return ""
	}
	return ""
}

func writeSSE(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range events {
		fmt.Fprintf(w, "data: %s\n\n", event)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func writeTextTurn(w http.ResponseWriter, text string) {
	writeSSE(w, textTurnEvents(text)...)
}

// writeToolCallTurn writes one completed assistant turn publishing one tool
// call under the given call identity.
func writeToolCallTurn(w http.ResponseWriter, id, name, args string) {
	writeSSE(w, toolCallTurnEvents(id, name, args)...)
}

// operationRegisterStatus reads one Operation register's state status
// directly from the store, the only reader that works after the owner closed.
func operationRegisterStatus(t *testing.T, store harness.Storage, sessionID, operationID string) harness.OperationState {
	t.Helper()
	status, _ := operationRegisterTerminal(t, store, sessionID, operationID)
	return status
}

// operationRegisterTerminal reads one Operation's register once and returns
// its status and terminal detail, failing the test on read or decode error.
func operationRegisterTerminal(t *testing.T, store harness.Storage, sessionID, operationID string) (harness.OperationState, string) {
	t.Helper()
	reg, err := store.ReadRegister(context.Background(), harness.RegisterKey{SessionID: sessionID, Kind: harness.RegisterOperation, OperationID: operationID})
	testassert.Require(t, err == nil, "ReadRegister(%s): %v", operationID, err)
	var wire struct {
		State struct {
			Status   harness.OperationState `json:"status"`
			Terminal *struct {
				Detail string `json:"detail"`
			} `json:"terminal"`
		} `json:"state"`
	}
	testassert.NoError(t, json.Unmarshal(reg.Payload, &wire))
	var detail string
	if wire.State.Terminal != nil {
		detail = wire.State.Terminal.Detail
	}
	return wire.State.Status, detail
}

// awaitOperation polls one Operation's register until it reaches the wanted
// state, bounding the wait: settlement is asynchronous after Submit returns,
// and a queued delivery admits later than its submit.
func awaitOperation(t *testing.T, r *Melta, sessionID, operationID string, want harness.OperationState) harness.OperationRecord {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var rec harness.OperationRecord
		err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
			var rerr error
			rec, rerr = h.ReadOperation(ctx, sessionID, operationID)
			return rerr
		})
		if err == nil && rec.State.Status == want {
			return rec
		}
		if time.Now().After(deadline) {
			var detail string
			if rec.State.Terminal != nil {
				detail = rec.State.Terminal.Detail
			}
			t.Fatalf("operation %q never settled to %v (read error %v, status %v, detail %q)", operationID, want, err, rec.State.Status, detail)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAssembledPhaseIntegration(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		ctx := context.Background()
		e := newOwnerEnv(t)
		server := newProductionModelServer(t, "probe", "{}")
		t.Setenv("ASSEMBLED_TEST_KEY", "assembled-key-1")
		writeServiceFile(t, e.configPath, assembledConfigDocument(server.URL, "T1", "L1"))
		writeAgentDefinitions(t, e.configPath, assembledAgentsDocument)
		hook := &recordHook{name: "hook.first", events: e.events, admissionMutate: func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
			var prompt string
			var err error
			if in.Candidate.Prompt != nil {
				prompt = *in.Candidate.Prompt
			} else {
				prompt, err = in.Prompt.Default()
				if err != nil {
					return extension.AdmissionResult{}, err
				}
			}
			prompt += "|hooked"
			candidate := in.Candidate
			candidate.Prompt = &prompt
			return extension.AdmissionResult{Candidate: &candidate}, nil
		}}
		ws := &workspaceOpens{}
		plugins := assembledPlugins(e, store, hook, ws)
		// The combined scenario runs through the public production
		// construction: concrete preparation, custom plugin slice.
		openAssembled := func() (*Melta, error) {
			return e.open(ctx, plugins...)
		}

		r, err := openAssembled()
		testassert.NoError(t, err)
		sat, err := r.Subscribe(1)
		testassert.NoError(t, err)
		healthy, err := r.Subscribe(64)
		testassert.NoError(t, err)

		// Two Sessions in two distinct Workspaces run over their own scopes.
		wsA := filepath.Join(e.home, "int-wsa")
		wsB := filepath.Join(e.home, "int-wsb")
		sessionA, err := r.createSession(ctx, wsA, "integrated")
		testassert.NoError(t, err)
		sessionB, err := r.createSession(ctx, wsB, "integrated")
		testassert.NoError(t, err)

		// Baseline turn on B: the composed production turn calls the real
		// probe tool through the transport — one model request for the tool
		// call and one for the follow-up — with the configured hook and the
		// dependency invoked at the tool boundary with its own revision's
		// settings.
		submitThroughRuntime(t, r, sessionB.Identity.SessionID, "op-b1", "hello")
		rec := awaitOperation(t, r, sessionB.Identity.SessionID, "op-b1", harness.OperationSuccess)
		testassert.Require(t, rec.Admission.Execution.ConfigurationRevision == "1" && rec.Admission.Execution.SystemPrompt != "" && strings.HasSuffix(rec.Admission.Execution.SystemPrompt, "|hooked"), "first turn record = %+v, want the hooked revision-1 success with the composed prompt", rec.Admission.Execution)
		if !slices.Equal(rec.Admission.Execution.InitialToolNames, []string{"probe"}) {
			t.Fatalf("first turn advertisement = %v, want the composed probe tool", rec.Admission.Execution.InitialToolNames)
		}
		if ad := wireToolNames(decodeWireChatBody(t, server.bodyAt(0))); !slices.Equal(ad, []string{"probe"}) {
			t.Fatalf("dispatched advertisement = %v, want the captured probe selection", ad)
		}
		if n := countEvents(e.events.all(), "work:L1:base"); n != 1 {
			t.Fatalf("dependency invocations reading revision-1 settings = %d, want 1: %v", n, e.events.all())
		}
		if server.requests() != 2 {
			t.Fatalf("model requests = %d, want the tool-call turn and the follow-up", server.requests())
		}
		testassert.Equal(t, server.authAt(0), "Bearer assembled-key-1")

		// The cleanup of the turn is asynchronous: consume it until the
		// Session's execution has disposed, retaining every committed event
		// for the final complete-sequence oracle.
		var observed []Event
		shConsumeUntilDisposed(t, r, healthy, &observed, sessionB.Identity.SessionID, sessionA.Identity.SessionID, sessionB.Identity.SessionID)

		// Park the actual dependency call, not its model request.
		ws.mu.Lock()
		ws.gate, ws.arrived = make(chan struct{}), make(chan struct{}, 1)
		gate, arrived := ws.gate, ws.arrived
		ws.mu.Unlock()
		submitThroughRuntime(t, r, sessionA.Identity.SessionID, "op-a1", "nested")
		server.awaitRequests(3)
		<-arrived
		// The saturated subscriber filled on the first publication — a
		// passive fact now precedes every scope opening — and is removed at
		// the second, while the healthy one continues.
		first, ok := nextEvent(t, sat)
		testassert.Require(t, ok, "saturated subscriber closed before its first buffered event")
		switch eventKind(t, first) {
		case "configuration_changed", "scope_opened", "scope_closed", "session_changed", "text_delta", "tool_started", "tool_finished", "warning_changed":
		default:
			t.Fatalf("saturated subscriber first event = %s, outside the closed event set", eventJSON(t, first))
		}
		{
			_, ok := nextEvent(t, sat)
			testassert.Require(t, !ok, "saturated subscriber still open at the second publication, want it removed and closed")
		}
		sat.Close()

		// The queued next revision is buffered while the Operation is blocked.
		submitQueuedThroughRuntime(t, r, sessionA.Identity.SessionID, "op-a2", "queued")

		// Reload publishes the next revision over the blocked Operation.
		writeServiceFile(t, e.configPath, assembledConfigDocument(server.URL, "T2", "L2"))
		writeAgentDefinitions(t, e.configPath, assembledAgentsDocument)
		{
			revision, err := r.Reload(ctx, testCaller(r))
			testassert.Require(t, err == nil && revision.Generation == "2", "Reload over the blocked Operation = (%q, %v), want 2", revision, err)
		}
		testassert.Require(t, countEvents(e.events.all(), "close:depper") == 0 && countEvents(e.events.all(), "close:base") == 0, "retired activations closed while the nested call was parked: %v", e.events.all())
		fresh, err := r.createSession(ctx, wsA, "integrated")
		testassert.NoError(t, err)
		submitThroughRuntime(t, r, fresh.Identity.SessionID, "op-fresh", "fresh")
		freshRec := awaitOperation(t, r, fresh.Identity.SessionID, "op-fresh", harness.OperationSuccess)
		testassert.Equal(t, freshRec.Admission.Execution.ConfigurationRevision, "2")
		testassert.Require(t, countEvents(e.events.all(), "work:L2:base") == 1 && countEvents(e.events.all(), "close:depper") == 0, "new work did not coexist with parked old work: %v", e.events.all())
		shConsumeUntilDisposed(t, r, healthy, &observed, fresh.Identity.SessionID, sessionA.Identity.SessionID, sessionB.Identity.SessionID, fresh.Identity.SessionID)

		// Release: the blocked Operation completes on its captured revision 1.
		close(gate)
		awaitOperation(t, r, sessionA.Identity.SessionID, "op-a1", harness.OperationSuccess)
		{
			rec := readOperation(t, r, sessionA.Identity.SessionID, "op-a1")
			testassert.Require(t, rec.Admission.Execution.ConfigurationRevision == "1" && strings.HasSuffix(rec.Admission.Execution.SystemPrompt, "|hooked"), "blocked Operation record = %+v, want the revision-1 hooked success", rec.Admission.Execution)
		}

		// The queued delivery captures the next revision: new hook settings
		// and the dependency's own settings from the same revision, while the
		// old capture keeps its content.
		recA2 := awaitOperation(t, r, sessionA.Identity.SessionID, "op-a2", harness.OperationSuccess)
		testassert.Require(t, countEvents(e.events.all(), "work:L1:base") == 2 && orderedSubset(e.events.all(), "work:L1:base", "close:depper"), "nested call and dependency closure order = %v", e.events.all())
		testassert.Require(t, countEvents(e.events.all(), "close:base") == 0, "a reload reactivated an extension whose own section is unchanged: %v", e.events.all())
		testassert.Require(t, recA2.Admission.Execution.ConfigurationRevision == "2", "queued Operation record = %+v, want the revision-2 success", recA2.Admission.Execution)
		{
			got := hook.calls()
			testassert.Require(t, slices.Equal(got, []hookCall{{tag: "T1"}, {tag: "T1"}, {tag: "T2"}, {tag: "T2"}}), "hook calls = %v, want the configured tag of each call", got)
		}
		if n := countEvents(e.events.all(), "work:L2:base"); n != 2 {
			t.Fatalf("dependency invocations reading revision-2 settings = %d, want 2: %v", n, e.events.all())
		}
		// The queued delivery's execution disposes before the shutdown phase:
		// consume its events so the remaining sequence stays in one order.
		shConsumeUntilDisposed(t, r, healthy, &observed, sessionA.Identity.SessionID, sessionA.Identity.SessionID, sessionB.Identity.SessionID, fresh.Identity.SessionID)

		// Shutdown joins the parked execution: the third admission's model
		// request parks at the server, Close cancels it, the server observes
		// the disconnect, and the interrupted settlement commits before Close
		// converges.
		server.armGate()
		before := server.requests()
		// A submit racing the predecessor's post-terminal drain is buffered
		// and delivered by that drain, so either disposition starts this
		// execution.
		parked, err := submitRawThroughRuntime(ctx, r, sessionA.Identity.SessionID, "op-a3", "parked", harness.MessageModeRegular)
		testassert.NoError(t, err)
		testassert.Require(t, parked.Disposition == harness.DispositionAdmitted || parked.Disposition == harness.DispositionSteering, "submit op-a3 = %+v, want admitted or buffered steering", parked.Disposition)
		server.awaitRequests(before + 1)
		closeDone := make(chan error, 1)
		go func() { closeDone <- r.Close(ctx) }()
		select {
		case <-server.gone:
		case <-time.After(10 * time.Second):
			t.Fatal("shutdown cancellation never reached the parked execution")
		}
		select {
		case err := <-closeDone:
			testassert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Close never converged after the parked execution settled")
		}
		testassert.Equal(t, operationRegisterStatus(t, store, sessionA.Identity.SessionID, "op-a3"), harness.OperationInterruption)
		server.releaseGate()
		// Owner shutdown retires what the reload left running, in reverse
		// activation order: the revision-2 dependency before the reused base.
		testassert.Require(t, countEvents(e.events.all(), "close:depper") == 2 && countEvents(e.events.all(), "close:base") == 1 && orderedSubset(e.events.all(), "close:depper", "close:base"), "owner shutdown closures = %v", e.events.all())

		// The healthy observer saw every committed transition in publication
		// order — invocation identities exact — and each Operation's own
		// opening and closure appear in it. The prefix consumed through the
		// executions' disposal precedes the remaining drain.
		sessionBID, sessionAID := sessionB.Identity.SessionID, sessionA.Identity.SessionID
		got := append(observed, committedTail(t, healthy, sessionAID, sessionBID, fresh.Identity.SessionID)...)
		shAssertCommittedOrder(t, got, r.instance, map[string]string{
			"op-b1": wsB, "op-a1": wsA, "op-fresh": wsA, "op-a2": wsA, "op-a3": wsA,
		})

		// Restart repair on the same store: recovery settles the abandoned
		// running state before any execution callback, then ordinary
		// admission proceeds on the repaired state.
		seededSession, _ := seedRunningOperation(t, store, filepath.Join(e.home, "int-reseed"))
		r2, err := openAssembled()
		testassert.NoError(t, err)
		{
			rec := readOperation(t, r2, seededSession, "seed-op")
			testassert.Require(t, rec.State.Status == harness.OperationInterruption && rec.State.Terminal != nil && rec.State.Terminal.Detail == "Operation interrupted by Runtime loss.", "restarted Operation state = %+v, want the terminal interruption repair", rec.State)
		}
		next, err := r2.createSession(ctx, wsB, "integrated")
		testassert.NoError(t, err)
		submitThroughRuntime(t, r2, next.Identity.SessionID, "op-after-repair", "continue")
		awaitOperation(t, r2, next.Identity.SessionID, "op-after-repair", harness.OperationSuccess)
		testassert.NoError(t, r2.Close(ctx))
	})
}

// shScopeMark is one committed invocation scope event reduced to its identity.
type shScopeMark struct {
	kind         protocol.ScopeEventKind
	invocationID string
	purpose      protocol.InvocationScopePurpose
	workspace    string
	operationID  string
}

// shInvocationPair collects one invocation's opening and closure marks by
// event position.
type shInvocationPair struct {
	purpose        protocol.InvocationScopePurpose
	opens, closes  int
	opened, closed int
}

func shScopeMarkOf(t *testing.T, kind protocol.ScopeEventKind, scope protocol.Scope) shScopeMark {
	t.Helper()
	testassert.Require(t, scopeKind(t, scope) == "invocation", "committed scope = %+v, want an invocation scope", scope)
	body, err := scope.AsInvocationScope()
	testassert.NoError(t, err)
	testassert.Require(t, body.InvocationId != "", "committed scope = %+v, want an invocation identity", body)
	return shScopeMark{kind: kind, invocationID: body.InvocationId, purpose: body.Purpose, workspace: deref(body.Workspace), operationID: body.OperationId}
}

// shAssertCommittedOrder proves the committed scope sequence structurally:
// exactly one configuration event for the reload's revision, and every
// Operation's one invocation opened and closed under its own identity and
// Workspace, the opening before the closure.
func shAssertCommittedOrder(t *testing.T, events []Event, instance string, operations map[string]string) {
	t.Helper()
	var marks []shScopeMark
	configs := 0
	for _, event := range events {
		switch eventKind(t, event) {
		case "configuration_changed":
			body, err := event.AsConfigurationChangedEvent()
			testassert.NoError(t, err)
			testassert.Require(t, body.ConfigurationRevision.InstanceId == instance && body.ConfigurationRevision.Generation == "2", "configuration event = %+v, want this owner's revision 2", body)
			configs++
		case "scope_opened", "scope_closed":
			body, err := event.AsScopeEvent()
			testassert.NoError(t, err)
			marks = append(marks, shScopeMarkOf(t, body.Kind, body.Scope))
		default:
			t.Fatalf("committed event kind %q, outside the scope and configuration set", eventKind(t, event))
		}
	}
	testassert.Equal(t, configs, 1)
	for _, operation := range []string{"op-b1", "op-a1", "op-fresh", "op-a2", "op-a3"} {
		workspace := operations[operation]
		pairs := make(map[string]*shInvocationPair)
		var ids []string
		for i, mark := range marks {
			if mark.operationID != operation {
				continue
			}
			testassert.Equal(t, mark.workspace, workspace)
			testassert.Equal(t, mark.purpose, protocol.Conversation)
			pair := pairs[mark.invocationID]
			if pair == nil {
				pair = &shInvocationPair{purpose: mark.purpose, opened: -1, closed: -1}
				pairs[mark.invocationID] = pair
				ids = append(ids, mark.invocationID)
			}
			if mark.kind == protocol.ScopeOpened {
				pair.opens++
				pair.opened = i
			} else {
				pair.closes++
				pair.closed = i
			}
		}
		testassert.Require(t, len(ids) == 1, "operation %q invocations = %v, want its one invocation: %v", operation, ids, marks)
		pair := pairs[ids[0]]
		testassert.Require(t, pair.opens == 1 && pair.closes == 1 && pair.opened < pair.closed, "invocation %q marks = %+v, want its one opening before its closure", ids[0], pair)
	}
}

// recordCommittedEvent appends one scope or configuration event to the
// committed-order oracle; every passive fact and warning event is accounted
// separately — its scope must name one of the real Sessions, never a foreign
// identity.
func recordCommittedEvent(t *testing.T, observed *[]Event, event Event, sessionIDs ...string) {
	t.Helper()
	switch eventKind(t, event) {
	case "configuration_changed", "scope_opened", "scope_closed":
		*observed = append(*observed, event)
	case "warning_changed":
		// The warning events the reloads' own commits and the real sessions'
		// prompt preparations require are accounted here on their
		// independent revision clock, never dropped silently: a global event
		// carries the runtime scope, a session event the real Session's
		// scope.
		body, err := event.AsWarningChangedEvent()
		testassert.NoError(t, err)
		if scopeKind(t, body.Scope) == "runtime" {
			// the global reload groups
		} else if scopeNamesRealSession(t, body.Scope, sessionIDs...) {
			// the session's own prompt group
		} else {
			t.Fatalf("warning event scope = %+v, want the runtime scope or a real session", body.Scope)
		}
		{
			_, err := strconv.ParseUint(body.WarningsRevision.Revision, 10, 64)
			testassert.Require(t, err == nil, "warning event revision %q is not a decimal integer", body.WarningsRevision.Revision)
		}
	case "session_changed":
		body, err := event.AsSessionChangedEvent()
		testassert.NoError(t, err)
		if !scopeNamesRealSession(t, body.Scope, sessionIDs...) {
			invalidation, _ := scopeSessionIdentity(t, body.Scope)
			t.Fatalf("passive invalidation for %v, want a real session", invalidation)
		}
	case "text_delta", "tool_started", "tool_finished":
		scope := progressEventScope(t, event)
		if !scopeNamesRealSession(t, scope, sessionIDs...) {
			progress, _ := scopeSessionIdentity(t, scope)
			t.Fatalf("passive progress for %v, want a real session", progress)
		}
	default:
		t.Fatalf("unexpected event kind %q", eventKind(t, event))
	}
}

// committedTail drains the remaining closed subscription, keeping the
// committed order exact while accounting the passive facts of the real
// Sessions — a foreign identity fatals inside the recorder.
func committedTail(t *testing.T, sub *Subscription, sessionIDs ...string) []Event {
	t.Helper()
	var out []Event
	for _, event := range drainClosed(t, sub) {
		recordCommittedEvent(t, &out, event, sessionIDs...)
	}
	return out
}

// shConsumeUntilDisposed consumes the subscription into the committed-order
// oracle through one Session's execution disposal, then drains what is still
// queued: an Engine invocation publishes no closure event, so the phase
// boundary is the Session view's idle execution.
func shConsumeUntilDisposed(t *testing.T, r *Melta, sub *Subscription, observed *[]Event, sessionID string, sessionIDs ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for sessionViewThroughRuntime(t, r, sessionID).History.ExecutionBusy {
		select {
		case event, ok := <-sub.Events():
			testassert.Require(t, ok, "the healthy subscription closed before session %q disposed its execution", sessionID)
			recordCommittedEvent(t, observed, event, sessionIDs...)
		case <-time.After(10 * time.Millisecond):
			testassert.Require(t, !time.Now().After(deadline), "session %q never disposed its execution", sessionID)
		}
	}
	for {
		select {
		case event, ok := <-sub.Events():
			testassert.Require(t, ok, "the healthy subscription closed before session %q disposed its execution", sessionID)
			recordCommittedEvent(t, observed, event, sessionIDs...)
			continue
		default:
		}
		break
	}
}

// scopeNamesRealSession reports whether one event scope names one of the
// given real Sessions through its Session-carrying union branches.
func scopeNamesRealSession(t *testing.T, scope protocol.Scope, sessionIDs ...string) bool {
	t.Helper()
	sessionID, ok := scopeSessionIdentity(t, scope)
	if !ok {
		return false
	}
	for _, real := range sessionIDs {
		if sessionID == real {
			return true
		}
	}
	return false
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
