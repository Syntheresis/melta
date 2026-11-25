package melta

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// The call-kind columns this test drives: the entry point, an admission hook, a
// request, arguments, authorization and result hook, the cleanup hook, a tool,
// and the three client-operation targets.
const (
	kindPhaseActivation    = "activation"
	kindPhaseAdmission     = "admission"
	kindPhaseRequest       = "request"
	kindPhaseArguments     = "arguments"
	kindPhaseAuthorization = "authorization"
	kindPhaseResult        = "result"
	kindPhaseCleanup       = "cleanup"
	kindPhaseTool          = "tool"
	kindPhaseOpSession     = "operation:session"
	kindPhaseOpWorkspace   = "operation:workspace"
	kindPhaseOpHarness     = "operation:harness"
)

// kindPhase describes one column's authority: which service-call kind it
// carries, whether it works for a host-issued Session, and, for an operation,
// its client target.
type kindPhase struct {
	kind       string
	hasSession bool
	target     extension.ExposedTargetKind
}

var kindPhases = map[string]kindPhase{
	kindPhaseActivation:    {kind: kindPhaseActivation},
	kindPhaseAdmission:     {kind: kindPhaseAdmission, hasSession: true},
	kindPhaseRequest:       {kind: kindPhaseRequest, hasSession: true},
	kindPhaseArguments:     {kind: kindPhaseArguments, hasSession: true},
	kindPhaseAuthorization: {kind: kindPhaseAuthorization, hasSession: true},
	kindPhaseResult:        {kind: kindPhaseResult, hasSession: true},
	kindPhaseCleanup:       {kind: kindPhaseCleanup},
	kindPhaseTool:          {kind: kindPhaseTool, hasSession: true},
	kindPhaseOpSession:     {kind: "operation", hasSession: true, target: extension.ExposedTargetSession},
	kindPhaseOpWorkspace:   {kind: "operation", target: extension.ExposedTargetWorkspace},
	kindPhaseOpHarness:     {kind: "operation", target: extension.ExposedTargetHarness},
}

// kindRow is one landed service method and the columns that may reach it.
type kindRow struct {
	name      string
	service   string
	method    string
	args      json.RawMessage
	columns   []string
	needsSess bool
	// harnessTarget marks a row whose operation cell holds for the
	// harness-target operation only.
	harnessTarget bool
}

func (r kindRow) allows(phase kindPhase) bool {
	allowed := false
	for _, column := range r.columns {
		allowed = allowed || column == phase.kind
	}
	if !allowed {
		return false
	}
	if r.needsSess && !phase.hasSession {
		return false
	}
	if phase.kind == "operation" && r.harnessTarget && phase.target != extension.ExposedTargetHarness {
		return false
	}
	return true
}

var kindRows = []kindRow{
	{name: "credentials", service: extension.ContractCredentials, method: "get", args: json.RawMessage(`{"field":"/key"}`),
		columns: []string{kindPhaseActivation, kindPhaseAdmission, kindPhaseRequest, kindPhaseArguments, kindPhaseAuthorization, kindPhaseResult, kindPhaseCleanup, kindPhaseTool, "operation"}},
	{name: "cache", service: extension.ContractData, method: "cache_namespace", args: json.RawMessage(`{}`),
		columns: []string{kindPhaseActivation, kindPhaseAdmission, kindPhaseRequest, kindPhaseArguments, kindPhaseAuthorization, kindPhaseResult, kindPhaseCleanup, kindPhaseTool, "operation"}},
	{name: "warnings", service: extension.ContractWarnings, method: "report", args: json.RawMessage(`{"kind":"call-kind","message":"probe"}`),
		columns: []string{kindPhaseActivation, kindPhaseAdmission, kindPhaseRequest, kindPhaseArguments, kindPhaseAuthorization, kindPhaseResult, kindPhaseCleanup, kindPhaseTool, "operation"}},
	{name: "members", service: extension.ContractSession, method: "members", args: json.RawMessage(`{}`),
		columns: []string{kindPhaseAdmission, kindPhaseRequest, kindPhaseArguments, kindPhaseAuthorization, kindPhaseResult, kindPhaseTool, "operation"}, needsSess: true},
	{name: "state", service: extension.ContractState, method: "set", args: json.RawMessage(`{"value":{"call_kind":true}}`),
		columns: []string{kindPhaseRequest, kindPhaseArguments, kindPhaseAuthorization, kindPhaseResult, kindPhaseTool, "operation"}, needsSess: true},
	{name: "session-namespace", service: extension.ContractData, method: "session_namespace", args: json.RawMessage(`{}`),
		columns: []string{kindPhaseRequest, kindPhaseArguments, kindPhaseAuthorization, kindPhaseResult, kindPhaseTool, "operation"}, needsSess: true},
	{name: "claim", service: extension.ContractData, method: "claim_external_data", args: json.RawMessage(`{}`),
		columns: []string{kindPhaseRequest, kindPhaseArguments, kindPhaseAuthorization, kindPhaseResult, kindPhaseTool, "operation"}, needsSess: true},
	{name: "settings", service: extension.ContractSettings, method: "replace",
		columns: []string{"operation"}, harnessTarget: true},
	{name: "commands", service: extension.ContractCommands,
		columns: []string{"operation"}},
}

// callKindProbe records every row's outcome for one call column. One extension
// carries it through every callback the columns cover.
type callKindProbe struct {
	mu       sync.Mutex
	outcomes map[string][]error

	peerMu      sync.Mutex
	peerCalls   map[string]error
	plainCalls  map[string]error
	precedence  [3]error
	harnessWork string

	requestOnce    sync.Once
	requestEntered chan struct{}
	requestRelease chan struct{}
}

func newCallKindProbe(harnessWork string) *callKindProbe {
	return &callKindProbe{
		outcomes:  make(map[string][]error),
		peerCalls: make(map[string]error), plainCalls: make(map[string]error),
		harnessWork: harnessWork,
	}
}

func kindOutcomeKey(phase, row string) string { return phase + "|" + row }

func (p *callKindProbe) record(phase, row string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := kindOutcomeKey(phase, row)
	p.outcomes[key] = append(p.outcomes[key], err)
}

func (p *callKindProbe) observe(call extension.AuthorContext, phase, commandMethod string, commandArgs json.RawMessage) {
	for _, row := range kindRows {
		service, method, args := row.service, row.method, row.args
		switch row.name {
		case "settings":
			raw, err := json.Marshal(map[string]json.RawMessage{"section": call.Settings})
			if err != nil {
				p.record(phase, row.name, err)
				continue
			}
			args = raw
		case "commands":
			method, args = commandMethod, commandArgs
		}
		_, err := call.Call(context.Background(), service, method, args)
		p.record(phase, row.name, err)
	}
}

func (p *callKindProbe) recordPeer(phase, service string, err error) {
	p.peerMu.Lock()
	defer p.peerMu.Unlock()
	if service == "kind.peer" {
		p.peerCalls[phase] = err
	} else {
		p.plainCalls[phase] = err
	}
}

// waitBeforeRequest parks the first request hook until the test releases it,
// so a denied admission call's side effects can be observed first.
func (p *callKindProbe) waitBeforeRequest() {
	if p.requestEntered == nil || p.requestRelease == nil {
		return
	}
	p.requestOnce.Do(func() {
		close(p.requestEntered)
		<-p.requestRelease
	})
}

// callSecondExtensions proves a second extension's own service acts with the
// kind of the call it serves and its own grants.
func (p *callKindProbe) callSecondExtensions(ctx context.Context, call extension.AuthorContext, phase string) {
	_, err := call.Call(ctx, "kind.peer", "set", json.RawMessage(`{}`))
	p.recordPeer(phase, "kind.peer", err)
	_, err = call.Call(ctx, "kind.plain", "set", json.RawMessage(`{}`))
	p.recordPeer(phase, "kind.plain", err)
}

func (p *callKindProbe) verify(t *testing.T) {
	t.Helper()
	for phase, info := range kindPhases {
		for _, row := range kindRows {
			key := kindOutcomeKey(phase, row.name)
			p.mu.Lock()
			outcomes := append([]error(nil), p.outcomes[key]...)
			p.mu.Unlock()
			if len(outcomes) == 0 {
				t.Errorf("%s/%s: no call was attempted", phase, row.name)
				continue
			}
			allowed := row.allows(info)
			for index, err := range outcomes {
				if allowed && err != nil {
					t.Errorf("%s/%s outcome %d = %v, want the handler reached", phase, row.name, index, err)
				}
				if !allowed && !errors.Is(err, ErrDenied) {
					t.Errorf("%s/%s outcome %d = %v, want denied", phase, row.name, index, err)
				}
			}
		}
	}
	p.peerMu.Lock()
	defer p.peerMu.Unlock()
	if !errors.Is(p.peerCalls[kindPhaseAdmission], ErrDenied) {
		t.Errorf("admission served state change = %v, want denied", p.peerCalls[kindPhaseAdmission])
	}
	if p.peerCalls[kindPhaseRequest] != nil {
		t.Errorf("request served state change = %v, want its own state write", p.peerCalls[kindPhaseRequest])
	}
	if !errors.Is(p.plainCalls[kindPhaseRequest], ErrDenied) {
		t.Errorf("ungranted server state change = %v, want denied although its caller has the grant", p.plainCalls[kindPhaseRequest])
	}
}

func (p *callKindProbe) extension() extension.Extension {
	manifest := registrarManifest("kindprobe")
	manifest.Services = []string{
		extension.ContractCredentials, extension.ContractData, extension.ContractState,
		extension.ContractSession, extension.ContractWarnings, extension.ContractSettings,
		extension.ContractCommands,
	}
	manifest.Secrets = []extension.SecretRef{{Field: "/key", Lifetime: extension.SecretActivation}}
	// One command method that succeeds on each operation target without
	// further setup: create_session on a whole-owner or Workspace call, the
	// argument-free interrupt on a Session call.
	createArgs := func() json.RawMessage {
		raw, _ := json.Marshal(map[string]string{"workspace": p.harnessWork})
		return raw
	}
	return extension.Extension{Manifest: manifest, Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		call := extension.AuthorContext{ExtensionID: manifest.ID, Settings: api.Settings(), Call: api.Call}
		p.observe(call, kindPhaseActivation, "interrupt", json.RawMessage(`{}`))
		// Precedence: an unknown method reports after the grant but before its
		// arguments.
		_, p.precedence[1] = api.Call(ctx, extension.ContractWarnings, "missing", json.RawMessage(`{"kind":7}`))

		if _, err := api.RegisterTool(model.ToolDefinition{Name: "kind_probe", Description: "call-kind probe", Parameters: json.RawMessage(`{"type":"object"}`)}, kindProbeTool{p}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachAdmission(func(ctx context.Context, call extension.AuthorContext, _ extension.AdmissionInput) (extension.AdmissionResult, error) {
			p.observe(call, kindPhaseAdmission, "interrupt", json.RawMessage(`{}`))
			// Precedence: a refused kind reports before invalid arguments; this
			// call has the admission hook's real Session.
			_, p.precedence[2] = call.Call(ctx, extension.ContractState, "set", json.RawMessage(`{"bogus":true}`))
			p.callSecondExtensions(ctx, call, kindPhaseAdmission)
			return extension.AdmissionResult{}, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachRequest(extension.HookModePure, func(ctx context.Context, call extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			p.waitBeforeRequest()
			p.observe(call, kindPhaseRequest, "interrupt", json.RawMessage(`{}`))
			p.callSecondExtensions(ctx, call, kindPhaseRequest)
			return harness.RequestConsequence{}, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachArguments(extension.HookModePure, func(ctx context.Context, call extension.AuthorContext, toolCall model.ToolCall) (extension.ArgumentsResult, error) {
			p.observe(call, kindPhaseArguments, "interrupt", json.RawMessage(`{}`))
			return extension.ArgumentsResult{Arguments: toolCall.Arguments}, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachAuthorization(func(ctx context.Context, call extension.AuthorContext, _ extension.AuthorizationInput) (*string, error) {
			p.observe(call, kindPhaseAuthorization, "interrupt", json.RawMessage(`{}`))
			return nil, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachResult(func(ctx context.Context, call extension.AuthorContext, input harness.ToolOutputInput) (string, error) {
			p.observe(call, kindPhaseResult, "interrupt", json.RawMessage(`{}`))
			return input.Content, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterCleanup(func(ctx context.Context, call extension.AuthorContext, _ harness.SessionIdentity) error {
			p.observe(call, kindPhaseCleanup, "interrupt", json.RawMessage(`{}`))
			return nil
		}, nil); err != nil {
			return nil, err
		}
		for _, operation := range []struct {
			name   string
			target extension.ExposedTargetKind
			phase  string
			method string
			args   json.RawMessage
		}{
			{"probe_harness", extension.ExposedTargetHarness, kindPhaseOpHarness, "create_session", createArgs()},
			{"probe_workspace", extension.ExposedTargetWorkspace, kindPhaseOpWorkspace, "create_session", createArgs()},
			{"probe_session", extension.ExposedTargetSession, kindPhaseOpSession, "interrupt", json.RawMessage(`{}`)},
		} {
			target, phase, command, commandArgs := operation.target, operation.phase, operation.method, operation.args
			if _, err := api.RegisterOperation(extension.Operation{
				Target: target,
				Method: extension.ServiceMethod(operation.name, `{}`, `{}`, "", ""),
				Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
					p.observe(call, phase, command, commandArgs)
					return extension.Result{Value: json.RawMessage(`null`)}, nil
				},
			}, nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}}
}

type kindProbeTool struct{ probe *callKindProbe }

func (t kindProbeTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t kindProbeTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionCommandRun, Target: "kind-probe"}}}
}

func (t kindProbeTool) Execute(ctx context.Context, call extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	t.probe.observe(call, kindPhaseTool, "interrupt", json.RawMessage(`{}`))
	return harness.ToolOutcome{Result: model.ToolResult{CallID: "kind-call", Status: model.ResultSuccess, Content: "ok"}, Evidence: harness.ExecutionObserved}
}

// kindPeerService is a second extension's service whose handler acts on its
// own Session state through the nested call scope it receives. granted says
// whether it requests the state service.
func kindPeerService(id, service string, granted bool) extension.Extension {
	manifest := registrarManifest(id)
	if granted {
		manifest.Services = []string{extension.ContractState}
	}
	return extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: service, Methods: []extension.RegisteredMethod{{
			Method: extension.ServiceMethod("set", `{}`, `{}`, "", ""),
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				return call.Call(ctx, extension.ContractState, "set", json.RawMessage(`{"value":{"peer":true}}`))
			},
		}}}, nil)
		return nil, err
	}}
}

// TestCallKindLimits proves every landed service method reaches its handler
// exactly from the columns that work for it, on the native and worker
// transports, and that a call breaking two rules reports the earlier one.
func TestCallKindLimits(t *testing.T) {
	for _, transport := range []struct {
		name   string
		worker bool
	}{
		{"native", false},
		{"worker", true},
	} {
		transport := transport
		t.Run(transport.name, func(t *testing.T) {
			runCallKindLimits(t, transport.worker)
		})
	}
}

func runCallKindLimits(t *testing.T, workerHost bool) {
	t.Setenv("KIND_PROBE_KEY", "kind-secret")
	e := newOwnerEnv(t)
	workspace := filepath.Join(e.dataDir, "kind-workspace")
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))
	writeServiceFile(t, e.configPath, strings.TrimSuffix(ownerConfigDocument(e.server.URL), "}")+
		`,"extensions":{"agents":{"enabled":true},"kindprobe":{"enabled":true,"key":"KIND_PROBE_KEY"},"kindpeer":{"enabled":true},"kindplain":{"enabled":true},"kindnogrant":{"enabled":true}}}`)
	writeAgentDefinitions(t, e.configPath, projectionAgentsDocument)
	store := storage.NewMemory()
	e.fixtureStore(store)
	probe := newCallKindProbe(workspace)
	noGrant := extension.Extension{Manifest: registrarManifest("kindnogrant"), Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		_, probe.precedence[0] = api.Call(ctx, extension.ContractWarnings, "missing", json.RawMessage(`{"kind":7}`))
		return nil, nil
	}}
	probeNative := probe.extension()
	probeExtension := probeNative
	var spawner *adapterSpawner
	if workerHost {
		spawner = &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
			return probeNative, nil
		}}
		spawner.install()
		probeExtension = executableAuthorExtension(probeNative.Manifest, "kindprobe", adapterEnvironment)
	}
	ctx := context.Background()
	r, err := e.open(ctx, probeExtension, kindPeerService("kindpeer", "kind.peer", true), kindPeerService("kindplain", "kind.plain", false), noGrant)
	testassert.NoError(t, err)
	defer closeProjectionRuntime(r)
	if spawner != nil {
		testassert.Require(t, spawner.count() == 1, "probe spawned %d worker processes, want one", spawner.count())
	}

	session := projectionSession(t, r, workspace, "solo").Identity.SessionID
	e.server.setScript(func(_ context.Context, body string) []string {
		if lastMessageRole(body) == "tool" {
			return textTurnEvents("done")
		}
		return toolCallTurnEvents("kind-call", "kind_probe", "{}")
	})
	probe.requestEntered, probe.requestRelease = make(chan struct{}), make(chan struct{})
	if _, err := r.SubmitSession(ctx, testCaller(r), session, commandSubmitRequest(t, "kind-op", "probe")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.requestEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first request hook never parked")
	}
	var statesBefore map[string]harness.SessionStateValue
	testassert.NoError(t, r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var err error
		statesBefore, err = h.SessionExtensionStates(ctx, session)
		return err
	}))
	if state, ok := statesBefore["kindprobe"]; ok {
		t.Errorf("denied admission call changed probe Session state: %s", state)
	}
	if owners := sessionOwners(t, store, session); len(owners) != 0 {
		t.Errorf("denied admission call recorded an external-data claim: %v", owners)
	}
	close(probe.requestRelease)
	awaitOperation(t, r, session, "kind-op", harness.OperationSuccess)

	for _, operation := range []struct {
		name   string
		target protocol.ExtensionInvocationTarget
	}{
		{"probe_workspace", workspaceTarget(workspace)},
		{"probe_harness", harnessTarget()},
		{"probe_session", sessionTarget(session)},
	} {
		_, err := r.invokeRegisteredOperation(ctx, testCaller(r), "kindprobe", operation.name, operation.target, []byte(`{}`), "req-"+operation.name)
		if err != nil {
			t.Fatalf("%s: %v", operation.name, err)
		}
	}

	var states map[string]harness.SessionStateValue
	testassert.NoError(t, r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var err error
		states, err = h.SessionExtensionStates(ctx, session)
		return err
	}))
	testassert.Check(t, string(states["kindpeer"]) == `{"peer":true}`, "served state change = %q, want the peer extension's write", states["kindpeer"])
	if _, ok := states["kindplain"]; ok {
		t.Errorf("ungranted serving extension wrote state: %s", states["kindplain"])
	}

	// The cleanup hook runs through real Session deletion; the claims the
	// permitted columns recorded make this extension one of its owners.
	_, err = r.ArchiveSession(ctx, testCaller(r), session)
	testassert.NoError(t, err)
	if _, err := r.DeleteSession(ctx, testCaller(r), session); err != nil {
		t.Fatal(err)
	}

	probe.verify(t)
	testassert.Require(t, errors.Is(probe.precedence[0], ErrDenied), "no grant and an unknown method = %v, want denied first", probe.precedence[0])
	testassert.Require(t, errors.Is(probe.precedence[1], harness.ErrNotFound), "an unknown method and invalid arguments = %v, want not-found first", probe.precedence[1])
	testassert.Require(t, errors.Is(probe.precedence[2], ErrDenied), "a refused kind and invalid arguments = %v, want denied first", probe.precedence[2])
}
