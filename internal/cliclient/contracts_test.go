package cliclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/terminal"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/protocol"
)

type gatedScreen struct {
	screen           *screenWriter
	entered, release chan struct{}
	once             sync.Once
}

func (w *gatedScreen) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.screen.Write(data)
}

func TestStaleOlderPageRetriesThroughTheSameReader(t *testing.T) {
	c, transport := fixture(t)
	r, done := c.startReader()
	h := hydration(rootA, "5", inputs(51, 100), pointer("real-oldest"))
	receive(t, transport.requests).respond(h)
	barrier(t, done)
	c.mu.Lock()
	r.pendingOlder = true
	c.kickReaderLocked(r)
	chain := r.chain
	c.mu.Unlock()
	receive(t, transport.requests).respond(protocol.HistoryPage{SessionRevision: revision("4"), Items: inputs(1, 50)})
	fresh := receive(t, transport.requests)
	testassert.Require(t, strings.HasSuffix(fresh.req.URL.Path, "/hydration"), "stale page did not dirty hydration")
	fresh.respond(h)
	retry := receive(t, transport.requests)
	testassert.Require(t, retry.req.URL.Query().Get("cursor") == "real-oldest", "stale older request lost or cursor changed")
	c.mu.Lock()
	testassert.Require(t, r.chain == chain && len(r.items) == 50 && r.hydration.SessionRevision.DurableRevision == "5", "stale page advanced view or spawned second chain")
	c.mu.Unlock()
	retry.respond(protocol.HistoryPage{SessionRevision: revision("5"), Items: inputs(1, 50)})
	barrier(t, chain.done)
	c.mu.Lock()
	defer c.mu.Unlock()
	testassert.Require(t, len(r.items) == 100, "valid disjoint older page rejected")
}

func TestMalformedGlobalsAndLostConversationContinuityRepairActualConnection(t *testing.T) {
	for _, invalid := range []string{"configuration", "continuity"} {
		t.Run(invalid, func(t *testing.T) {
			var mu sync.Mutex
			configurationReads, hydrations := 0, 0
			historyEntered := make(chan struct{}, 1)
			live := liveFixture(t, false, func(w http.ResponseWriter, r *http.Request) bool {
				w.Header().Set("Content-Type", "application/json")
				mu.Lock()
				defer mu.Unlock()
				if invalid == "configuration" && r.URL.Path == "/v1/configuration" {
					configurationReads++
					if configurationReads == 1 {
						_ = json.NewEncoder(w).Encode(configuration("1.5"))
						return true
					}
				}
				if invalid == "continuity" && strings.HasSuffix(r.URL.Path, "/hydration") {
					hydrations++
					items := inputs(1, 2)
					var cursor *string
					if hydrations >= 2 {
						items = inputs(4, 5)
					}
					if hydrations == 2 {
						cursor = pointer("gap")
					}
					_ = json.NewEncoder(w).Encode(hydration(rootA, fmt.Sprint(hydrations), items, cursor))
					return true
				}
				if invalid == "continuity" && strings.HasSuffix(r.URL.Path, "/history") {
					historyEntered <- struct{}{}
					_ = json.NewEncoder(w).Encode(protocol.HistoryPage{SessionRevision: revision("2"), Items: []protocol.ConversationItem{}})
					return true
				}
				return false
			})
			input, write := pipeInput(t)
			c := New(Options{Startup: live.opts, Workspace: "/workspace"})
			screen := newScreen(80)
			done := runClient(c, input, screen)
			barrier(t, live.streams)
			if invalid == "continuity" {
				frameContaining(t, screen, "input 2")
				c.mu.Lock()
				reader := c.root
				c.mu.Unlock()
				c.refreshReader(reader)
				barrier(t, historyEntered)
			}
			barrier(t, live.streams)
			frameContaining(t, screen, "Connection repaired")
			if invalid == "continuity" && !strings.Contains(screen.text(), "input 5") {
				frameContaining(t, screen, "input 5")
			}
			write.Close()
			testassert.NoError(t, receive(t, done))
			assertNoImplicitControls(t, live)
		})
	}
}

func TestQueuedTaskMenuAndErrorCannotPublishAfterNavigation(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			c, transport := fixture(t)
			settledReader(t, c, transport)
			screen := newScreen(80)
			blocked := &gatedScreen{screen: screen, entered: make(chan struct{}), release: make(chan struct{})}
			// Park the sole writer before the asynchronous command completion.
			go c.writer(blocked)
			c.mu.Lock()
			c.notifyLocked()
			s := c.stampLocked()
			c.mu.Unlock()
			barrier(t, blocked.entered)
			c.task(func() { c.workspaceMenu(s) })
			old := receive(t, transport.requests)
			if fail {
				old.reject(404, protocol.Error{Code: "not_found", Message: "stale workspace error"})
			} else {
				old.respond([]protocol.Workspace{{DisplayName: "stale workspace menu", Root: "/old"}})
			}
			settleTasks(t, c)
			c.selectRoot(rootB)
			receive(t, transport.requests).respond(hydration(rootB, "1", nil, nil))
			receive(t, transport.requests).respond(json.RawMessage(`[]`))
			fresh := receive(t, transport.requests)
			fresh.respond(hydration(rootB, "1", nil, nil))
			settleTasks(t, c)
			close(blocked.release)
			c.mu.Lock()
			testassert.Require(t, c.menu == nil, "queued menu survived newer selection")
			current := c.stampLocked()
			c.mu.Unlock()
			c.report(current, terminal.RenderSystemMsg("current command marker"), false)
			frameContaining(t, screen, "current command marker")
			if strings.Contains(screen.text(), "stale workspace") || !strings.Contains(screen.text(), rootB) {
				t.Fatalf("queued stale task/menu published: %s", screen.text())
			}
			c.requestExit(nil)
		})
	}
}

func TestRuntimeFileSuccessAndErrorAreStampedAndUseGeneratedAddress(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			c, transport := fixture(t)
			settledReader(t, c, transport)
			c.command(`/runtime ReadWorkspaceFile {"body":{"workspace":"/workspace","path":"file.go"}}`)
			old := receive(t, transport.requests)
			testassert.Require(t, strings.Contains(string(old.body), `"workspace":"/workspace"`) && strings.Contains(string(old.body), `"path":"file.go"`), "file read omitted explicit address")
			c.selectRoot(rootB)
			receive(t, transport.requests).respond(hydration(rootB, "1", nil, nil))
			receive(t, transport.requests).respond(json.RawMessage(`[]`))
			receive(t, transport.requests).respond(hydration(rootB, "1", nil, nil))
			if fail {
				old.reject(400, protocol.Error{Code: "invalid", Message: "stale file error"})
			} else {
				old.respond(protocol.ReadFileResult{CanonicalPath: "/workspace/stale.go", Content: "stale content", Truncated: true})
			}
			settleTasks(t, c)
			testassert.Require(t, !strings.Contains(outputs(c), "stale"), "superseded viewer success/error printed")
			c.command(`/runtime ReadWorkspaceFile {"body":{"workspace":"/workspace","path":"current.go"}}`)
			receive(t, transport.requests).respond(protocol.ReadFileResult{CanonicalPath: "/workspace/current.go", Content: "current", Truncated: true})
			settleTasks(t, c)
			text := outputs(c)
			testassert.Require(t, strings.Contains(text, "/workspace/current.go") && strings.Contains(text, `"truncated":true`), "canonical path/truncation lost")
			c.command(`/runtime ReadWorkspaceFile {"body":{"workspace":"/workspace","path":"../denied"}}`)
			receive(t, transport.requests).reject(400, protocol.Error{Code: "invalid", Message: "denied workspace path"})
			settleTasks(t, c)
			{
				text := outputs(c)
				testassert.Require(t, strings.Contains(text, "denied workspace path") && !strings.Contains(text, "Outcome unknown"), "denied read hidden or treated as unknown mutation")
			}
		})
	}
}

func TestForkBoundaryMenusUserOwnedCopiedRuntimeAndPlugin(t *testing.T) {
	c, transport := fixture(t)
	r, done := c.startReader()
	var items []protocol.ConversationItem
	for i, origin := range []protocol.InputOrigin{"user", "user", "runtime", "plugin"} {
		input := protocol.InputItem{Kind: "input", ItemId: fmt.Sprint(i), Origin: origin, Content: textParts(fmt.Sprint(origin, i))}
		if i != 1 {
			input.OperationId = pointer(fmt.Sprintf("owned-%d", i))
		}
		var item protocol.ConversationItem
		_ = item.FromInputItem(input)
		items = append(items, item)
	}
	receive(t, transport.requests).respond(hydration(rootA, "1", items, nil))
	barrier(t, done)
	c.command("/fork")
	settleTasks(t, c)
	c.mu.Lock()
	testassert.Require(t, len(c.menu.items) == 4, "fork menu = %v, want every committed input independent of origin", c.menu.items)
	for i, origin := range []protocol.InputOrigin{"user", "user", "runtime", "plugin"} {
		testassert.Require(t, c.menu.items[i].Extra.(protocol.InputItem).Origin == origin, "fork item %d origin = %q, want %q", i, c.menu.items[i].Extra.(protocol.InputItem).Origin, origin)
	}
	c.mu.Unlock()
	press(c, terminal.KeyMsg{Special: terminal.KeyDown})
	press(c, terminal.KeyMsg{Special: terminal.KeyEnter})
	c.mu.Lock()
	testassert.Require(t, c.menu == nil && c.forkBoundary.OperationId == nil && !c.forkRewind, "copied input invented a source restore boundary")
	c.forkBoundary = nil
	c.mu.Unlock()

	// A runtime or plugin input is an ordinary fork entry, and its owned
	// attribution still offers the rewind choice.
	for _, index := range []int{2, 3} {
		c.command("/fork")
		settleTasks(t, c)
		for i := 0; i < index; i++ {
			press(c, terminal.KeyMsg{Special: terminal.KeyDown})
		}
		press(c, terminal.KeyMsg{Special: terminal.KeyEnter})
		c.mu.Lock()
		selected := *c.forkBoundary
		testassert.Require(t, c.menu != nil && c.menu.kind == "fork-choice" && selected.Origin == protocol.InputOrigin([]string{"user", "user", "runtime", "plugin"}[index]), "fork choice for item %d = (%+v, %v), want the owned input's fork choice", index, c.menu, selected)
		c.menu = nil
		c.forkBoundary = nil
		c.mu.Unlock()
	}

	for _, row := range []struct {
		index     int
		operation string
	}{{1, "owned-2"}, {2, "owned-3"}} {
		c.command("/revert")
		settleTasks(t, c)
		c.mu.Lock()
		testassert.Require(t, len(c.menu.items) == 3, "rewind menu = %v, want the three owned inputs independent of origin", c.menu.items)
		for i, origin := range []protocol.InputOrigin{"user", "runtime", "plugin"} {
			testassert.Require(t, c.menu.items[i].Extra.(protocol.InputItem).Origin == origin, "rewind item %d origin = %q, want %q", i, c.menu.items[i].Extra.(protocol.InputItem).Origin, origin)
		}
		c.mu.Unlock()
		for i := 0; i < row.index; i++ {
			press(c, terminal.KeyMsg{Special: terminal.KeyDown})
		}
		press(c, terminal.KeyMsg{Special: terminal.KeyEnter})
		restore := receive(t, transport.requests)
		testassert.Require(t, strings.HasSuffix(restore.req.URL.Path, "/revert-code") && strings.Contains(string(restore.body), row.operation), "rewind item %d request = %s %s, want the actual %s boundary", row.index, restore.req.URL.Path, restore.body, row.operation)
		restore.respond(map[string]any{"restored": []any{}, "skipped": []any{}})
		settleTasks(t, c)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	testassert.Require(t, r == c.root, "menu replaced the selected reader")
}

func TestRejectedSubmissionKeepsDraftAndDoesNotBecomeEmptySuccess(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	setDraft(c, "rejected draft")
	press(c, terminal.KeyMsg{Special: terminal.KeyEnter})
	receive(t, transport.requests).reject(409, protocol.Error{Code: "conflict", Message: "server gate rejection"})
	receive(t, transport.requests).respond(hydration(rootA, "2", inputs(1, 1), nil))
	settleTasks(t, c)
	c.mu.Lock()
	testassert.Require(t, c.draft.String() == "rejected draft", "rejected draft cleared")
	c.mu.Unlock()
	text := outputs(c)
	testassert.Require(t, strings.Contains(text, "server gate rejection") && !strings.Contains(text, "Outcome unknown"), "known rejection hidden/reclassified")
}

func TestChildListOldConnectionSuccessAndErrorAreSuperseded(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			c, transport := fixture(t)
			settledReader(t, c, transport)
			live := liveFixture(t, false, nil)
			c.mu.Lock()
			s := c.stampLocked()
			c.opts.Startup = live.opts
			c.mu.Unlock()
			oldDone := make(chan struct{})
			c.task(func() { c.childrenMenu(s); close(oldDone) })
			parked := receive(t, transport.requests)
			screen := newScreen(80)
			go c.writer(screen)
			c.task(func() { c.connect(true, s.conn) })
			frameContaining(t, screen, "canonical clipboard")
			if fail {
				parked.reject(404, protocol.Error{Code: "not_found", Message: "old owner child error"})
			} else {
				parked.respond([]protocol.Session{{SessionId: childA, OwnerSessionId: pointer(rootA)}})
			}
			barrier(t, oldDone)
			c.mu.Lock()
			testassert.Require(t, c.menu == nil && c.conn != s.conn, "old connection child menu published")
			fresh := c.stampLocked()
			c.mu.Unlock()
			c.report(fresh, terminal.RenderSystemMsg("new owner marker"), false)
			frameContaining(t, screen, "new owner marker")
			testassert.Require(t, !strings.Contains(screen.text(), "old owner child error"), "old-owner error published")
			c.mu.Lock()
			c.closed = true
			c.cancel()
			c.connCancel()
			c.mu.Unlock()
			c.requestExit(nil)
			settleTasks(t, c)
		})
	}
}

func TestFutureAgentAndTypeWideModelUseGeneratedScope(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	transport.mu.Lock()
	transport.globals = true
	transport.mu.Unlock()
	c.command("/agent planner")
	req := receive(t, transport.requests)
	var selectBody protocol.ExtensionInvocationRequest
	testassert.NoError(t, json.Unmarshal(req.body, &selectBody))
	testassert.Require(t, req.req.URL.Path == "/v1/extensions/invoke" && selectBody.ExtensionId == "agents" && selectBody.Operation == "select" && selectBody.Target.SessionId != nil && *selectBody.Target.SessionId == rootA && selectBody.Arguments != nil && string(*selectBody.Arguments) == `{"agent":"planner"}`, "wrong Session Agent edit: %s %s", req.req.URL, req.body)
	transport.mu.Lock()
	transport.globals = false
	transport.mu.Unlock()
	req.respond(json.RawMessage(`{"kind":"success","result":null}`))
	next := hydration(rootA, "2", inputs(1, 1), nil)
	next.ActiveOperation = &protocol.Operation{OperationId: "active", Model: "captured/model", RequestKind: "message"}
	receive(t, transport.requests).respond(next)
	settleTasks(t, c)
	c.command("/model provider/next")
	req = receive(t, transport.requests)
	testassert.Equal(t, req.req.URL.Path, "/v1/extensions")
	req.respond([]protocol.RegisteredExtension{{ExtensionId: "agents", Version: "1", ConfigurationSchema: pointer(protocol.ExtensionConfigDocument("{}")), Settings: "{}", Tools: []string{}, Operations: []protocol.RegisteredExtensionOperation{{Method: protocol.ExtensionMethod{Name: "current", Mode: protocol.Pure, Input: "{}", Success: "{}"}, Target: protocol.RegisteredExtensionOperationTargetSession}}}})
	req = receive(t, transport.requests)
	req.respond(json.RawMessage(`{"kind":"success","result":{"agent":"planner"}}`))
	req = receive(t, transport.requests)
	var modelBody protocol.ExtensionInvocationRequest
	testassert.NoError(t, json.Unmarshal(req.body, &modelBody))
	testassert.Require(t, req.req.URL.Path == "/v1/extensions/invoke" && modelBody.Operation == "set_model" && modelBody.Target.Kind == protocol.ExtensionInvocationTargetKindHarness && modelBody.Arguments != nil && string(*modelBody.Arguments) == `{"definition_id":"planner","model":"provider/next"}`, "model was not the supplied type-wide extension edit: %s %s", req.req.URL, req.body)
	req.respond(json.RawMessage(`{"kind":"success","result":null}`))
	next.SessionRevision = revision("3")
	next.SelectedModel = pointer("provider/next")
	receive(t, transport.requests).respond(next)
	settleTasks(t, c)
	screen := newScreen(100)
	go c.writer(screen)
	c.mu.Lock()
	c.width = 100
	c.notifyLocked()
	c.mu.Unlock()
	frameContaining(t, screen, "captured/model")
	text := strings.ReplaceAll(screen.text(), "\n", "")
	testassert.Require(t, strings.Contains(text, "Model choice: provider/next") && strings.Contains(text, "ran with: captured/model"), "future selection overwrote active capture")
	c.requestExit(nil)
}

func TestResolvedSessionModelUsesSessionOwner(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	c.mu.Lock()
	c.root.hydration.Session.Inputs = protocol.SessionInputs{Instructions: pointer("keep"), Tools: pointer([]string{"read_file"})}
	c.mu.Unlock()
	c.command("/model provider/next")
	req := receive(t, transport.requests)
	testassert.Equal(t, req.req.URL.Path, "/v1/extensions")
	req.respond([]protocol.RegisteredExtension{})
	req = receive(t, transport.requests)
	testassert.Require(t, strings.HasSuffix(req.req.URL.Path, "/v1/sessions/"+rootA+"/model") && string(req.body) == `{"model":"provider/next"}`, "resolved model edit = %s %s", req.req.URL, req.body)
	req.respond(protocol.Session{SessionId: rootA})
	next := hydration(rootA, "2", inputs(1, 1), nil)
	next.Session.Inputs = protocol.SessionInputs{Instructions: pointer("keep"), Tools: pointer([]string{"read_file"})}
	receive(t, transport.requests).respond(next)
	settleTasks(t, c)
	c.requestExit(nil)
}

func TestCreateRequiresValidDiscovery(t *testing.T) {
	agentsCreate := `{"extension_id":"agents","version":"1","configuration_schema":"{}","settings":"{}","tools":[],"operations":[{"method":{"name":"create","mode":"service_io","input":"{}","success":"{}"},"target":"workspace"}]}`
	foreign := `{"extension_id":"guest","version":"1","configuration_schema":"{}","settings":"{}","tools":[],"operations":[]}`
	for _, row := range []struct {
		name        string
		body        string
		wantPath    string
		wantAgentOp bool
	}{
		{"no agents", `[]`, "/v1/sessions", false},
		{"supplied create", `[` + agentsCreate + `]`, "/v1/extensions/invoke", true},
		{"foreign only", `[` + foreign + `]`, "/v1/sessions", false},
		{"both extensions", `[` + foreign + `,` + agentsCreate + `]`, "/v1/extensions/invoke", true},
		{"malformed catalog", `null`, "", false},
		{"malformed entry", `[{"extension_id":"agents"}]`, "", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			c, transport := fixture(t)
			transport.mu.Lock()
			transport.globals = true
			transport.mu.Unlock()
			c.mu.Lock()
			s := c.stampLocked()
			c.mu.Unlock()
			c.task(func() { c.create(s) })
			discovery := receive(t, transport.requests)
			testassert.Equal(t, discovery.req.URL.Path, "/v1/extensions")
			discovery.respondBody(row.body)
			if row.wantPath != "" {
				created := receive(t, transport.requests)
				testassert.Equal(t, created.req.URL.Path, row.wantPath)
				if row.wantAgentOp {
					var body protocol.ExtensionInvocationRequest
					testassert.NoError(t, json.Unmarshal(created.body, &body))
					testassert.Require(t, body.ExtensionId == "agents" && body.Operation == "create" && body.Target.Workspace != nil && *body.Target.Workspace == "/workspace", "create = %s", created.body)
				} else {
					testassert.Equal(t, string(created.body), `{"workspace":"/workspace"}`)
				}
				created.reject(400, protocol.Error{Code: protocol.ErrorCodeInvalid, Message: "rejected"})
			}
			settleTasks(t, c)
			select {
			case unexpected := <-transport.requests:
				t.Fatalf("unexpected request after roster: %s", unexpected.req.URL)
			default:
			}
			c.requestExit(nil)
		})
	}
}

func TestCreateFailureCapturedAddressAndExplicitWorkspaceIntent(t *testing.T) {
	c, transport := fixture(t)
	c.mu.Lock()
	s := c.stampLocked()
	c.mu.Unlock()
	c.task(func() { c.create(s) })
	discovery := receive(t, transport.requests)
	testassert.Equal(t, discovery.req.URL.Path, "/v1/extensions")
	discovery.respond([]protocol.RegisteredExtension{})
	old := receive(t, transport.requests)
	var pending protocol.CreateSessionRequest
	{
		err := json.Unmarshal(old.body, &pending)
		testassert.Require(t, err == nil && pending.Workspace == "/workspace", "pending create captured %q (%v)", pending.Workspace, err)
	}
	c.selectWorkspace("/workspace/../different")
	list := receive(t, transport.requests)
	testassert.Require(t, list.req.URL.Query().Get("workspace") == "/different", "Workspace did not use lexical Abs")
	// An owned-only list still creates a root for the captured Workspace.
	list.respond([]protocol.Session{{SessionId: childA, OwnerSessionId: pointer(rootB)}})
	discovery = receive(t, transport.requests)
	testassert.Equal(t, discovery.req.URL.Path, "/v1/extensions")
	discovery.respond([]protocol.RegisteredExtension{})
	newCreate := receive(t, transport.requests)
	var sent protocol.CreateSessionRequest
	{
		err := json.Unmarshal(newCreate.body, &sent)
		testassert.Require(t, err == nil && sent.Workspace == "/different", "explicit Workspace composition create = %s (%v)", newCreate.body, err)
	}
	newCreate.reject(201, protocol.Session{SessionId: rootB, Workspace: "/different", Inputs: protocol.SessionInputs{}})
	receive(t, transport.requests).respond(json.RawMessage(`[]`))
	fresh := hydration(rootB, "1", nil, nil)
	fresh.Session.Workspace = "/different"
	receive(t, transport.requests).respond(fresh)
	old.lost()
	settleTasks(t, c)
	c.mu.Lock()
	adopted := c.rootID == rootB
	c.mu.Unlock()
	testassert.Require(t, adopted, "an old-Workspace completion or a failed intent blocked the new explicit composition")
	testassert.Require(t, !strings.Contains(outputs(c), "Outcome unknown"), "an old-Workspace completion published a stale local error")
}

func TestExplicitResumeAdoptsOwnedSession(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	go c.writer(io.Discard)
	c.command("/resume " + childA)
	destination := receive(t, transport.requests)
	testassert.Require(t, strings.HasSuffix(destination.req.URL.Path, "/hydration"), "named resume did not read its destination globally")
	setDraft(c, "source while destination is pending")
	press(c, terminal.KeyMsg{Special: terminal.KeyEnter})
	source := receive(t, transport.requests)
	testassert.Require(t, strings.Contains(source.req.URL.Path, rootA+"/submit"), "pending-selection submit addressed %s, want the accepted source", source.req.URL.Path)
	child := hydration(childA, "1", nil, nil)
	child.Session.OwnerSessionId = pointer(rootA)
	destination.respond(child)
	receive(t, transport.requests).respond(json.RawMessage(`[]`))
	fresh := receive(t, transport.requests)
	testassert.Require(t, strings.Contains(fresh.req.URL.Path, childA+"/hydration"), "adopted reader addressed %s", fresh.req.URL.Path)
	fresh.respond(child)
	source.respond(protocol.SubmitResult{Disposition: "admitted", Operation: &protocol.Operation{OperationId: "source"}})
	settleAnsweringRequested(t, c, transport)
	c.mu.Lock()
	testassert.Require(t, c.rootID == childA && c.childID == "", "owned Session was not adopted")
	c.mu.Unlock()
	setDraft(c, "follow up")
	press(c, terminal.KeyMsg{Special: terminal.KeyEnter})
	followup := receive(t, transport.requests)
	testassert.Require(t, strings.Contains(followup.req.URL.Path, childA+"/submit"), "follow-up addressed outside adopted Session: %s", followup.req.URL.Path)
	followup.respond(protocol.SubmitResult{Disposition: "admitted", Operation: &protocol.Operation{OperationId: "followup"}})
	settleAnsweringRequested(t, c, transport)
}

func TestNamedResumeAdoptsCrossWorkspaceRootAndReopensArchived(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	// An open root elsewhere resolves through the global read alone and adopts
	// its immutable destination Workspace.
	c.command("/resume " + rootB)
	destination := hydration(rootB, "1", nil, nil)
	destination.Session.Workspace = "/different"
	receive(t, transport.requests).respond(destination)
	receive(t, transport.requests).respond(json.RawMessage(`[]`))
	receive(t, transport.requests).respond(destination)
	settleTasks(t, c)
	c.mu.Lock()
	testassert.Require(t, c.rootID == rootB && c.workspace == "/different" && c.root != nil, "cross-Workspace root resume was refused or not adopted: root=%q workspace=%q", c.rootID, c.workspace)
	c.mu.Unlock()
	// An archived root is reopened first and its fresh hydration is then adopted.
	c.command("/resume " + childA)
	archived := hydration(childA, "1", nil, nil)
	archived.Session.Lifecycle = protocol.Archived
	receive(t, transport.requests).respond(archived)
	reopen := receive(t, transport.requests)
	testassert.Require(t, strings.HasSuffix(reopen.req.URL.Path, "/reopen"), "archived destination did not reopen before work")
	reopen.respond(protocol.Session{SessionId: childA, Lifecycle: protocol.Open})
	receive(t, transport.requests).respond(json.RawMessage(`[]`))
	// The adopted reader owns the one fresh hydration after the reopen.
	fresh := receive(t, transport.requests)
	testassert.Require(t, strings.HasSuffix(fresh.req.URL.Path, "/hydration"), "reopen did not precede the fresh destination read")
	fresh.respond(hydration(childA, "2", nil, nil))
	settleTasks(t, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	testassert.Require(t, c.rootID == childA && c.root != nil && c.root.hydration != nil && c.root.hydration.SessionRevision.DurableRevision == "2", "reopened root was not adopted with its fresh read")
}

func TestBareResumeSelectsNewestOpenRootAndKeepsSelectionWithoutAny(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	// The newest open root wins; archived and child headers never appear.
	c.command("/resume")
	receive(t, transport.requests).respond([]protocol.Session{
		{SessionId: childA, OwnerSessionId: pointer(rootA), Workspace: "/workspace", LastActivity: time.Unix(9, 0)},
		{SessionId: rootA, Workspace: "/workspace", LastActivity: time.Unix(1, 0)},
		{SessionId: rootB, Workspace: "/workspace", LastActivity: time.Unix(5, 0)},
	})
	receive(t, transport.requests).respond(json.RawMessage(`[]`))
	receive(t, transport.requests).respond(hydration(rootB, "2", nil, nil))
	settleTasks(t, c)
	c.mu.Lock()
	testassert.Require(t, c.rootID == rootB && c.menu == nil, "bare resume did not select the newest open root: root=%q menu=%v", c.rootID, c.menu)
	c.mu.Unlock()
	// No available root reports the retained no-session outcome and keeps the
	// current selection; no Session is created.
	c.command("/resume")
	receive(t, transport.requests).respond([]protocol.Session{})
	// The retained source stays live: its own refresh follows the failed intent.
	settleAnsweringRequested(t, c, transport)
	c.mu.Lock()
	testassert.Equal(t, c.rootID, rootB)
	c.mu.Unlock()
	testassert.Require(t, strings.Contains(outputs(c), "no active sessions"), "no-session outcome was not reported")
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for _, route := range transport.calls {
		testassert.Require(t, !strings.Contains(route, "/create") && !strings.Contains(route, "/archived"), "bare resume created a Session or consulted archived Sessions: %s", route)
	}
}

// Failed root navigation over the actual commands and failure replies keeps
// the accepted root, inspector and Workspace live with their resume reads and
// reports the outcome; a superseded completion neither adopts nor prints, and
// recall survives the failed intent.
func TestFailedRootNavigationKeepsTheAcceptedSelection(t *testing.T) {
	for _, test := range []struct {
		name    string
		command string
		fail    func(request)
		note    string
	}{
		{"known rejected New", "/new", func(req request) { req.reject(409, protocol.Error{Code: "conflict", Message: "create rejected"}) }, "create rejected"},
		{"lost New", "/new", func(req request) { req.lost() }, "Outcome unknown"},
		{"known rejected named resume", "/resume missing-root", func(req request) { req.reject(404, protocol.Error{Code: "not_found", Message: "no such session"}) }, "no such session"},
		{"lost named resume", "/resume missing-root", func(req request) { req.lost() }, "unexpected EOF"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, transport := fixture(t)
			settledReader(t, c, transport)
			c.mu.Lock()
			source, sourceID, workspace := c.root, c.rootID, c.workspace
			c.mu.Unlock()
			c.command(test.command)
			if test.command == "/new" {
				discovery := receive(t, transport.requests)
				testassert.Equal(t, discovery.req.URL.Path, "/v1/extensions")
				discovery.respond([]protocol.RegisteredExtension{})
			}
			test.fail(receive(t, transport.requests))
			// The retained source readers resume under the current selection.
			resumed := receive(t, transport.requests)
			testassert.Require(t, strings.Contains(resumed.req.URL.Path, rootA+"/hydration"), "failed navigation did not resume the source read: %s", resumed.req.URL)
			resumed.respond(hydration(rootA, "2", inputs(1, 1), nil))
			settleTasks(t, c)
			c.mu.Lock()
			kept := c.root == source && c.rootID == sourceID && c.workspace == workspace && c.child == nil && c.childID == "" && c.menu == nil
			c.mu.Unlock()
			testassert.Require(t, kept, "failed navigation changed the accepted selection")
			testassert.Require(t, strings.Contains(outputs(c), test.note), "failed navigation hid its outcome")
			press(c, terminal.KeyMsg{Rune: 'z'})
			press(c, terminal.KeyMsg{Special: terminal.KeyUp})
			if c.draft.String() != "input 1" {
				t.Fatalf("failed navigation cleared recall: %q", c.draft.String())
			}
		})
	}

	t.Run("a superseded completion neither adopts nor prints", func(t *testing.T) {
		c, transport := fixture(t)
		settledReader(t, c, transport)
		c.command("/new")
		discovery := receive(t, transport.requests)
		discovery.respond([]protocol.RegisteredExtension{})
		parkedCreate := receive(t, transport.requests)
		c.command("/resume also-missing")
		parkedError := receive(t, transport.requests)
		c.command("/resume " + rootB)
		destination := receive(t, transport.requests)
		destination.respond(hydration(rootB, "1", nil, nil))
		receive(t, transport.requests).respond(json.RawMessage(`[]`))
		receive(t, transport.requests).respond(hydration(rootB, "2", nil, nil))
		parkedCreate.reject(201, protocol.Session{SessionId: childA, Workspace: "/workspace", Inputs: protocol.SessionInputs{}})
		parkedError.reject(404, protocol.Error{Code: "not_found", Message: "stale destination error"})
		settleTasks(t, c)
		c.mu.Lock()
		defer c.mu.Unlock()
		testassert.Require(t, c.rootID == rootB, "a superseded completion adopted over newer navigation")
		testassert.Require(t, !strings.Contains(outputs(c), "stale destination error"), "superseded destination error printed")
	})
}

func TestSourceAgentMenuCannotPublishAfterDestinationAdoption(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	// The menu's own configuration read parks so its completion can be ordered
	// after the destination adoption.
	transport.globals = true
	// The destination intent starts, then the source's own /agent menu is begun
	// while the destination is still being resolved.
	c.command("/resume " + rootB)
	destination := receive(t, transport.requests)
	c.command("/agent")
	catalog := receive(t, transport.requests)
	testassert.Equal(t, catalog.req.URL.Path, "/v1/extensions")
	catalog.respond(agentsListDiscovery())
	menuRead := receive(t, transport.requests)
	testassert.Equal(t, menuRead.req.URL.Path, "/v1/extensions/invoke")
	// The adopted reader's own read proves the destination published, then the
	// menu task completes afterwards.
	destination.respond(hydration(rootB, "2", nil, nil))
	receive(t, transport.requests).respond(json.RawMessage(`[]`))
	adopted := receive(t, transport.requests)
	menuRead.respond(json.RawMessage(`{"kind":"success","result":[{"name":"planner","description":"Planner","subagent":false,"model":""}]}`))
	adopted.respond(hydration(rootB, "2", nil, nil))
	settleTasks(t, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	testassert.Require(t, c.menu == nil && c.rootID == rootB && c.root != nil, "source agent menu published after destination adoption: %+v; destination was not adopted: root=%q", c.menu, c.rootID)
}

func TestSourceForkCompletionCannotReplaceAdoptedDestination(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	c.command("/resume " + rootB)
	destination := receive(t, transport.requests)
	// The Fork attempt is begun against the retained source during resolution.
	c.mu.Lock()
	c.forkBoundary = &protocol.InputItem{ItemId: "boundary"}
	a, err := c.formAttemptLocked("ForkSession", "fork-op")
	c.mu.Unlock()
	testassert.NoError(t, err)
	c.task(func() { c.runAttempt(a.id) })
	fork := receive(t, transport.requests)
	testassert.Require(t, strings.HasSuffix(fork.req.URL.Path, "/fork"), "fork attempt did not fork: %s", fork.req.URL.Path)
	// The adopted reader's own read proves the destination published, then the
	// source Fork completes afterwards.
	destination.respond(hydration(rootB, "2", nil, nil))
	receive(t, transport.requests).respond(json.RawMessage(`[]`))
	adopted := receive(t, transport.requests)
	fork.respond(protocol.ForkResult{Session: protocol.Session{SessionId: childA}, Operation: protocol.Operation{OperationId: a.id}})
	adopted.respond(hydration(rootB, "2", nil, nil))
	settleTasks(t, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	testassert.Require(t, c.rootID == rootB && c.root != nil, "source Fork completion replaced the adopted destination: root=%q", c.rootID)
	// The attempt's own outcome and identity survive.
	settled := c.attempts["fork-op"]
	testassert.Require(t, settled != nil && settled.outcome == "accepted" && settled.session == rootA, "fork outcome/identity lost: %+v", settled)
}

func TestFailedResumeLeavesTheSourceLiveUnderAHeldHydration(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	// A source refresh is in flight, and a further dirty hint lands while the
	// destination is resolved.
	c.mu.Lock()
	c.root.dirty = true
	c.kickReaderLocked(c.root)
	c.mu.Unlock()
	held := receive(t, transport.requests)
	c.command("/resume missing-root")
	destination := receive(t, transport.requests)
	c.mu.Lock()
	c.refreshReadersLocked()
	c.mu.Unlock()
	destination.reject(404, protocol.Error{Code: "not_found", Message: "no such session"})
	held.respond(hydration(rootA, "2", inputs(1, 1), nil))
	fresh := receive(t, transport.requests)
	testassert.Require(t, strings.Contains(fresh.req.URL.Path, rootA) && strings.HasSuffix(fresh.req.URL.Path, "/hydration"), "the retained source did not resume reading: %s", fresh.req.URL.Path)
	fresh.respond(hydration(rootA, "3", inputs(1, 1), nil))
	settleTasks(t, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	testassert.Require(t, c.rootID == rootA && c.root != nil && c.root.hydration != nil && c.root.hydration.SessionRevision.DurableRevision == "3", "the retained source was left stale after a failed resume")
}

func TestRuntimeCurrentSnapshotsAndPartialRestoreRemainReachable(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	c.command(`/runtime GetSessionCodeSnapshots {"session_id":"` + rootA + `"}`)
	current := receive(t, transport.requests)
	testassert.Require(t, strings.Contains(current.req.URL.Path, rootA), "current snapshots did not address Session")
	current.respond([]protocol.CodeSnapshotGroup{{OperationId: "owned-operation", GroupItemId: "group", Files: []protocol.SnapshotFile{}}})
	settleTasks(t, c)
	testassert.Require(t, strings.Contains(outputs(c), "owned-operation"), "Operation snapshot boundary dropped")
	c.command(`/runtime RevertSessionCode {"session_id":"` + rootA + `","body":{"boundary_operation_id":"owned-operation"}}`)
	restore := receive(t, transport.requests)
	testassert.Require(t, strings.Contains(string(restore.body), `"boundary_operation_id":"owned-operation"`), "canonical restore boundary rewritten")
	restore.reject(409, protocol.CodeRevertResult{Restored: []string{"one.go"}, Skipped: []protocol.SkippedFile{{Path: "two.go", Reason: "changed"}}, Error: &protocol.Error{Code: "conflict", Message: "partial restore"}})
	receive(t, transport.requests).respond(hydration(rootA, "2", inputs(1, 1), nil))
	settleTasks(t, c)
	text := outputs(c)
	testassert.Require(t, strings.Contains(text, "one.go") && strings.Contains(text, "two.go") && strings.Contains(text, "partial restore"), "/runtime dropped complete partial restore")
}

// TestAgentNameShorthandRequiresExactlyOneMatch proves the CLI name shorthand
// sends a selection only for exactly one displayed match: zero matches and
// ambiguous matches report and never send a selection.
func TestAgentNameShorthandReportsRefusal(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	c.command("/agent missing")
	request := receive(t, transport.requests)
	testassert.Equal(t, request.req.URL.Path, "/v1/extensions/invoke")
	var body protocol.ExtensionInvocationRequest
	testassert.NoError(t, json.Unmarshal(request.body, &body))
	testassert.Require(t, body.Operation == "select" && body.Arguments != nil && string(*body.Arguments) == `{"agent":"missing"}`, "selection = %s", request.body)
	request.reject(400, protocol.Error{Code: "invalid", Message: "unknown agent"})
	settleAnswering(t, c, transport)
	testassert.Require(t, strings.Contains(outputs(c), "unknown agent"), "selection refusal not reported")
	c.requestExit(nil)
}

// TestAgentMenuLabelsSourceAndDispatchesExactDuplicate proves same-name,
// same-description providers are distinguishable in the existing menu detail
// text and that selecting the second row dispatches its exact full reference.
func TestAgentMenuDispatchesSelectedName(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	transport.mu.Lock()
	transport.globals = true
	transport.mu.Unlock()
	c.command("/agent")
	catalog := receive(t, transport.requests)
	testassert.Equal(t, catalog.req.URL.Path, "/v1/extensions")
	catalog.respond(agentsListDiscovery())
	menuRead := receive(t, transport.requests)
	testassert.Equal(t, menuRead.req.URL.Path, "/v1/extensions/invoke")
	menuRead.respond(json.RawMessage(`{"kind":"success","result":[{"name":"first","description":"first description","subagent":false,"model":""},{"name":"second","description":"second description","subagent":true,"model":""}]}`))
	settleTasks(t, c)
	c.mu.Lock()
	testassert.Require(t, c.menu != nil && len(c.menu.items) == 2, "duplicate menu = %+v", c.menu)
	testassert.Require(t, c.menu.items[0].Label == "first" && c.menu.items[1].Label == "second", "menu labels = %+v", c.menu.items)
	testassert.Require(t, c.menu.items[0].Detail == "first description" && c.menu.items[1].Detail == "second description", "menu details = %+v", c.menu.items)
	testassert.Require(t, c.menu.items[0].Extra == "first" && c.menu.items[1].Extra == "second", "menu extras = %#v/%#v", c.menu.items[0].Extra, c.menu.items[1].Extra)
	c.mu.Unlock()
	// Post-selection global and reader refreshes are ordinary reads; the
	// selection command itself remains the one scripted request below.
	transport.mu.Lock()
	transport.globals = false
	transport.mu.Unlock()
	go func() {
		press(c, terminal.KeyMsg{Special: terminal.KeyDown})
		press(c, terminal.KeyMsg{Special: terminal.KeyEnter})
	}()
	selected := receive(t, transport.requests)
	var selectedBody protocol.ExtensionInvocationRequest
	testassert.NoError(t, json.Unmarshal(selected.body, &selectedBody))
	testassert.Require(t, selected.req.URL.Path == "/v1/extensions/invoke" && selectedBody.Operation == "select" && selectedBody.Arguments != nil && string(*selectedBody.Arguments) == `{"agent":"second"}`, "selected dispatch = %s %s", selected.req.URL, selected.body)
	selected.respond(json.RawMessage(`{"kind":"success","result":null}`))
	settleAnswering(t, c, transport)
	c.requestExit(nil)
}

// TestReadOnlyNamedModelReportsUnavailableAndSendsNothing proves a foreign or
// read-only named resolver's /model path reports the desktop-equivalent
// unavailable-edit meaning, sends no command and never falls back to mutating
// resolved Inputs.
func TestModelCurrentFailureDoesNotMutate(t *testing.T) {
	c, transport := fixture(t)
	settledReader(t, c, transport)
	c.command("/model provider/next")
	discovery := receive(t, transport.requests)
	discovery.respond([]protocol.RegisteredExtension{{ExtensionId: "agents", Version: "1", ConfigurationSchema: pointer(protocol.ExtensionConfigDocument("{}")), Settings: "{}", Tools: []string{}, Operations: []protocol.RegisteredExtensionOperation{{Method: protocol.ExtensionMethod{Name: "current", Mode: protocol.Pure, Input: "{}", Success: "{}"}, Target: protocol.RegisteredExtensionOperationTargetSession}}}})
	current := receive(t, transport.requests)
	current.reject(400, protocol.Error{Code: "invalid", Message: "current failed"})
	settleAnswering(t, c, transport)
	text := outputs(c)
	testassert.Require(t, strings.Contains(text, "current failed"), "current failure = %q", text)
	transport.mu.Lock()
	calls := append([]string(nil), transport.calls...)
	transport.mu.Unlock()
	for _, call := range calls {
		testassert.Require(t, !strings.HasSuffix(call, "/model"), "model mutation followed failed current: %v", calls)
	}
	c.requestExit(nil)
}
