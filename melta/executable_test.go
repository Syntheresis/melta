package melta

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

const executableTestTimeout = 10 * time.Second

// testExecutablePeer is one in-process worker speaking the protocol over pipes.
type testExecutablePeer struct {
	t       *testing.T
	auto    bool
	handler func(*testExecutablePeer, worker.Frame)
	input   *io.PipeReader
	output  *io.PipeWriter
	reader  *worker.Reader
	writer  *worker.Writer
	frames  chan worker.Frame
	allow   chan struct{}
	done    chan struct{}
	finish  sync.Once
}

// testExecutableProcess is one peer's host-visible process.
type testExecutableProcess struct {
	stdin  io.WriteCloser
	stdout io.Reader
	peer   *testExecutablePeer
}

func (p *testExecutableProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *testExecutableProcess) Stdout() io.Reader     { return p.stdout }
func (p *testExecutableProcess) Wait() error           { <-p.peer.done; return nil }
func (p *testExecutableProcess) Kill() error           { p.peer.exit(); return nil }

// closeSignal closes the peer when the host closes its stdin end.
type closeSignal struct {
	io.WriteCloser
	once sync.Once
	exit func()
}

func (w *closeSignal) Close() error {
	w.once.Do(w.exit)
	return w.WriteCloser.Close()
}

func newTestExecutablePeer(t *testing.T, auto bool, handler func(*testExecutablePeer, worker.Frame)) *testExecutablePeer {
	return &testExecutablePeer{
		t:       t,
		auto:    auto,
		handler: handler,
		frames:  make(chan worker.Frame, 64),
		allow:   make(chan struct{}, 64),
		done:    make(chan struct{}),
	}
}

func (p *testExecutablePeer) process() *testExecutableProcess {
	peerIn, hostIn := io.Pipe()
	hostOut, peerOut := io.Pipe()
	p.input = peerIn
	p.output = peerOut
	p.reader = worker.NewReader(peerIn)
	p.writer = worker.NewWriter(peerOut)
	go p.run()
	return &testExecutableProcess{
		stdin:  &closeSignal{WriteCloser: hostIn, exit: p.exit},
		stdout: hostOut,
		peer:   p,
	}
}

func (p *testExecutablePeer) run() {
	defer p.exit()
	for {
		if !p.auto {
			select {
			case <-p.allow:
			case <-p.done:
				return
			}
		}
		frame, err := p.reader.ReadFrame()
		if err != nil {
			return
		}
		if p.handler != nil {
			p.handler(p, frame)
			continue
		}
		select {
		case p.frames <- frame:
		case <-p.done:
			return
		}
	}
}

// exit ends the simulated process. It is idempotent.
func (p *testExecutablePeer) exit() {
	p.finish.Do(func() {
		close(p.done)
		if p.input != nil {
			_ = p.input.Close()
		}
		if p.output != nil {
			_ = p.output.Close()
		}
	})
}

// allowOne permits one inbound frame in manual mode.
func (p *testExecutablePeer) allowOne() {
	select {
	case p.allow <- struct{}{}:
	default:
		p.t.Fatal("peer allow queue is full")
	}
}

// next returns the next frame in manual mode after permitting it.
func (p *testExecutablePeer) next() worker.Frame {
	p.t.Helper()
	p.allowOne()
	return p.await()
}

// await returns the next frame without permitting a read.
func (p *testExecutablePeer) await() worker.Frame {
	p.t.Helper()
	select {
	case frame := <-p.frames:
		return frame
	case <-time.After(executableTestTimeout):
		p.t.Fatal("timed out waiting for a worker frame")
		return worker.Frame{}
	}
}

// expectRequest asserts the next frame is a request with the given method.
func (p *testExecutablePeer) expectRequest(method string) worker.Frame {
	p.t.Helper()
	frame := p.next()
	if frame.Kind != worker.KindRequest || frame.Method != method {
		p.t.Fatalf("frame = %q/%q, want request/%q", frame.Kind, frame.Method, method)
	}
	return frame
}

func (p *testExecutablePeer) send(frame worker.Frame) {
	p.t.Helper()
	if err := p.writer.WriteFrame(frame); err != nil {
		p.t.Fatalf("writing %q frame: %v", frame.Kind, err)
	}
}

func (p *testExecutablePeer) sendValue(id string, value any) {
	p.t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		p.t.Fatalf("marshaling response: %v", err)
	}
	p.send(worker.Frame{Kind: worker.KindResponse, ID: id, Result: raw})
}

func (p *testExecutablePeer) sendNull(id string) {
	p.t.Helper()
	p.send(worker.Frame{Kind: worker.KindResponse, ID: id, Result: json.RawMessage("null")})
}

// testPeerSpawner replaces the process seam with a factory of in-process
// peers, one per activation.
type testPeerSpawner struct {
	t       *testing.T
	factory func() *testExecutablePeer
	mu      sync.Mutex
	spawned []*testExecutablePeer
}

func (s *testPeerSpawner) install() {
	old := spawnExecutable
	spawnExecutable = func(_ context.Context, _ string, environment []string) (executableProcess, error) {
		s.mu.Lock()
		peer := s.factory()
		s.spawned = append(s.spawned, peer)
		s.mu.Unlock()
		return peer.process(), nil
	}
	s.t.Cleanup(func() { spawnExecutable = old })
}

func (s *testPeerSpawner) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.spawned)
}

func (s *testPeerSpawner) last() *testExecutablePeer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.spawned) == 0 {
		s.t.Fatal("no peer was spawned")
	}
	return s.spawned[len(s.spawned)-1]
}

// executableTestManifest is the shared executable fixture manifest: one
// declared activation credential resolved through the credentials service.
func executableTestManifest() extension.Manifest {
	return extension.Manifest{
		ID:             "peer",
		Version:        "1",
		APIVersion:     extension.APIVersion,
		SettingsSchema: json.RawMessage(`{"type":"object","properties":{"enabled":{"type":"boolean"},"token":{"type":"string"}},"additionalProperties":false}`),
		Services:       []string{extension.ContractCredentials},
		Secrets:        []extension.SecretRef{{Field: "/token", Lifetime: extension.SecretActivation}},
	}
}

// executableTestToolName is the one tool the scripted peers register.
const executableTestToolName = "peer.tool"

// peerAuthorAPI is the author API handed to a directly-started activation:
// only the section and the activation call are exercised.
type peerAuthorAPI struct{ extension.API }

func (peerAuthorAPI) Settings() json.RawMessage { return json.RawMessage(`{}`) }

func (peerAuthorAPI) Call(context.Context, string, string, json.RawMessage) (extension.Result, error) {
	return extension.Result{}, extension.ErrClosed
}

func (peerAuthorAPI) Command(context.Context, extension.CommandTarget, string, json.RawMessage) (extension.Result, error) {
	return extension.Result{}, extension.ErrClosed
}

// activatePeer answers one worker's hello and activate handshake.
func activatePeer(t *testing.T, peer *testExecutablePeer, manifest extension.Manifest) {
	t.Helper()
	hello := peer.expectRequest(worker.MethodHello)
	peer.sendValue(hello.ID, worker.HelloResult{Manifest: worker.NewAuthorMetadata(manifest)})
	respondNull(peer, worker.MethodActivate)
}

func respondNull(peer *testExecutablePeer, method string) worker.Frame {
	frame := peer.expectRequest(method)
	peer.sendNull(frame.ID)
	return frame
}

// startPeerActivation spawns one activation over a scripted peer and returns
// it once its handshake has completed.
func startPeerActivation(t *testing.T, peer *testExecutablePeer, manifest extension.Manifest) *executableActivation {
	t.Helper()
	activation := newExecutableActivation("peer", func(context.Context, []string) ([]string, error) { return []string{}, nil })
	activation.author = &executableAuthor{manifest: manifest, api: peerAuthorAPI{}, registrations: make(map[string]extension.Registration)}
	started := make(chan error, 1)
	go func() { started <- activation.start(context.Background(), nil) }()
	activatePeer(t, peer, manifest)
	if err := <-started; err != nil {
		t.Fatalf("start: %v", err)
	}
	return activation
}

// registerToolParams is the register request one scripted peer issues for the
// shared fixture tool.
func registerToolParams(handle string) json.RawMessage {
	params, _ := json.Marshal(worker.AuthorRegisterParams{
		Kind:   "tool",
		Handle: handle,
		Spec:   worker.JSONBytes(`{"name":"` + executableTestToolName + `","parameters":{"type":"object"}}`),
	})
	return params
}

// authoringPeer scripts one worker entry: the handshake, one tool
// registration, and whatever the call handler does with each call frame.
func authoringPeer(manifest extension.Manifest, call func(*testExecutablePeer, worker.Frame)) func(*testExecutablePeer, worker.Frame) {
	return func(peer *testExecutablePeer, frame worker.Frame) {
		switch frame.Method {
		case worker.MethodHello:
			peer.sendValue(frame.ID, worker.HelloResult{Manifest: worker.NewAuthorMetadata(manifest)})
		case worker.MethodActivate:
			peer.sendNull(frame.ID)
			peer.send(worker.Frame{Kind: worker.KindRequest, ID: "reg-1", Method: "register", Params: registerToolParams("peer")})
		case "register":
			peer.sendNull(frame.ID)
		case worker.MethodCall:
			if call != nil {
				call(peer, frame)
			}
		}
	}
}

func eventually(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(executableTestTimeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// awaitRegistered waits until one of the candidate's activations has
// registered the named contribution.
func awaitRegistered(t *testing.T, candidate *nativeCandidate, name string) {
	t.Helper()
	owner := candidate.owner
	eventually(t, "registration "+name, func() bool {
		owner.nativeMu.Lock()
		defer owner.nativeMu.Unlock()
		for _, activation := range candidate.activations {
			if activation == nil {
				continue
			}
			for _, entry := range activation.entries {
				if entry.name == name {
					return true
				}
			}
		}
		return false
	})
}

func activationPending(activation *executableActivation) (sent int, total int) {
	activation.mu.Lock()
	defer activation.mu.Unlock()
	for _, request := range activation.pending {
		request.mu.Lock()
		if request.sent {
			sent++
		}
		request.mu.Unlock()
		total++
	}
	return sent, total
}

func runToolExecute(t *testing.T, tool extension.RegisteredTool, call extension.AuthorContext, plan extension.ToolPlan) chan harness.ToolOutcome {
	outcome := make(chan harness.ToolOutcome, 1)
	go func() { outcome <- tool.Execute(context.Background(), call, plan) }()
	return outcome
}

// encodeValue runs one worker encoder with a single-value result for tests.
func encodeValue[T any](t *testing.T, encode func(T) (json.RawMessage, error), value T) json.RawMessage {
	t.Helper()
	raw, err := encode(value)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	return raw
}

func TestExecutableCallEvidence(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	plan := extension.ToolPlan{}

	// The writer is blocked on the first frame, the second frame never leaves
	// the queue, and the activation then ends.
	first := runToolExecute(t, tool, extension.AuthorContext{}, plan)
	peer.allowOne()
	eventually(t, "first call to leave the writer queue", func() bool {
		sent, _ := activationPending(activation)
		return sent == 1
	})
	second := runToolExecute(t, tool, extension.AuthorContext{}, plan)
	eventually(t, "second call to start its write", func() bool {
		sent, _ := activationPending(activation)
		return sent == 2
	})
	third := runToolExecute(t, tool, extension.AuthorContext{}, plan)
	eventually(t, "third call to queue behind the blocked writer", func() bool {
		_, total := activationPending(activation)
		return total == 3 && len(activation.out) >= 1
	})
	peer.exit()
	firstOutcome := <-first
	secondOutcome := <-second
	thirdOutcome := <-third
	testassert.Equal(t, firstOutcome.Evidence, harness.ExecutionUnknown)
	testassert.Equal(t, secondOutcome.Evidence, harness.ExecutionUnknown)
	testassert.Equal(t, thirdOutcome.Evidence, harness.ExecutionNotStarted)
}

// TestExecutableCommandRequestBindsActivation proves the distinct worker
// command request is bound to its process's activation: the host answers with
// the ordinary typed error frame and the worker stays live.
func TestExecutableCommandRequestBindsActivation(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	params, err := json.Marshal(worker.AuthorCommandParams{
		Target: extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: "session"},
		Method: "submit", Arguments: worker.JSONBytes(`{"operation_id":"op-1"}`),
	})
	testassert.NoError(t, err)
	peer.send(worker.Frame{Kind: worker.KindRequest, ID: "w-command", Method: worker.MethodCommand, Params: params})
	reply := peer.next()
	testassert.Require(t, reply.ID == "w-command" && reply.Error != nil && reply.Error.Class == worker.ClassClosed, "command reply = %#v, want the activation's closed class", reply)
	activation.mu.Lock()
	failed := activation.failed
	activation.mu.Unlock()
	testassert.Require(t, failed == nil, "a typed command error failed the activation: %v", failed)
	peer.exit()
}

func TestExecutableLostResult(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}

	outcome := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	peer.next() // the call left the queue
	peer.exit()
	got := <-outcome
	testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
}

func TestExecutableMalformedTerminalFailsActivationWithoutRestart(t *testing.T) {
	manifest := executableTestManifest()
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer {
		return newTestExecutablePeer(t, true, authoringPeer(manifest, func(peer *testExecutablePeer, frame worker.Frame) {
			peer.send(worker.Frame{Kind: worker.KindResponse, ID: frame.ID, Result: json.RawMessage(`[]`)})
		}))
	}}
	spawner.install()
	owner := &configurationService{}
	guest := executableAuthorExtension(manifest, "peer", func(context.Context, []string) ([]string, error) { return []string{}, nil })
	candidate := func() *nativeCandidate {
		t.Helper()
		c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: guest, section: json.RawMessage(`{"enabled":true}`), installed: true}})
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		return c
	}
	first := candidate()
	if first.inactive[manifest.ID] != nil {
		t.Fatalf("activation inactive: %v", first.inactive)
	}
	if err := first.publishForTest(); err != nil {
		t.Fatalf("publish: %v", err)
	}
	awaitRegistered(t, first, executableTestToolName)
	stage := first.snapshotForTest()
	binding := stage.bindTool(executableTestToolName)
	if binding == nil {
		t.Fatal("peer.tool is not registered")
	}
	tool := binding.entry.value.(nativeTool).tool
	outcome := tool.Execute(context.Background(), extension.AuthorContext{}, extension.ToolPlan{})
	testassert.Equal(t, outcome.Evidence, harness.ExecutionUnknown)
	binding.release()
	stage.release()

	// The failed activation is retained without respawn while the selection
	// stays enabled with the same section.
	retained := candidate()
	if retained.inactive[manifest.ID] == nil {
		t.Fatal("a failed activation was retried")
	}
	if count := spawner.count(); count != 1 {
		t.Fatalf("failed activation spawned %d processes, want 1", count)
	}
	retained.release()

	// A published disable clears the retained failure: the next enable spawns fresh.
	disabled, err := owner.prepareNativeCandidate(context.Background(), nil)
	if err != nil {
		t.Fatalf("prepare disable: %v", err)
	}
	if err := disabled.publishForTest(); err != nil {
		t.Fatalf("disable publish: %v", err)
	}
	disabled.release()
	fresh := candidate()
	if fresh.inactive[manifest.ID] != nil {
		t.Fatalf("re-enabled activation stayed inactive: %v", fresh.inactive)
	}
	awaitRegistered(t, fresh, executableTestToolName)
	if err := fresh.publishForTest(); err != nil {
		t.Fatalf("re-enable publish: %v", err)
	}
	fresh.release()
	if count := spawner.count(); count != 2 {
		t.Fatalf("re-enabled activation spawned %d processes in total, want 2", count)
	}
	first.release()
}

func TestExecutableNestedCallLimits(t *testing.T) {
	manifest := executableTestManifest()
	plan := extension.ToolPlan{}

	// The live-call cap refuses one more call without terminating the
	// activation, and every call already admitted settles as unknown.
	peer := newTestExecutablePeer(t, true, nil) // reads every frame, never answers
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	admitted := make(chan harness.ToolOutcome, executableLiveCalls)
	for range executableLiveCalls {
		go func() { admitted <- tool.Execute(context.Background(), extension.AuthorContext{}, plan) }()
	}
	eventually(t, "live calls to fill the cap", func() bool {
		_, total := activationPending(activation)
		return total == executableLiveCalls
	})
	over := tool.Execute(context.Background(), extension.AuthorContext{}, plan)
	testassert.Equal(t, over.Evidence, harness.ExecutionNotStarted)
	testassert.Require(t, strings.Contains(over.Result.Content, "too many live calls"), "refused call = %q", over.Result.Content)
	activation.mu.Lock()
	failed := activation.failed
	activation.mu.Unlock()
	if failed != nil {
		t.Fatalf("the live-call cap failed the activation: %v", failed)
	}
	eventually(t, "every admitted call to be claimed by the writer", func() bool {
		sent, _ := activationPending(activation)
		return sent == executableLiveCalls
	})
	peer.exit()
	for range executableLiveCalls {
		testassert.Equal(t, (<-admitted).Evidence, harness.ExecutionUnknown)
	}

	// One live hostcall more than the cap is a protocol failure: it ends the
	// activation and the call it was nesting.
	blocked := newTestExecutablePeer(t, false, nil)
	spawner.factory = func() *testExecutablePeer { return blocked }
	blockedActivation := startPeerActivation(t, blocked, manifest)
	entered := make(chan struct{}, 1)
	outcome := make(chan harness.ToolOutcome, 1)
	go func() {
		outcome <- (&executableAuthorTool{activation: blockedActivation, handle: "peer"}).Execute(context.Background(), extension.AuthorContext{
			Call: func(ctx context.Context, _, _ string, _ json.RawMessage) (extension.Result, error) {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return extension.Result{}, ctx.Err()
			},
		}, plan)
	}()
	frame := blocked.next()
	var params worker.AuthorCallParams
	if err := json.Unmarshal(frame.Params, &params); err != nil {
		t.Fatalf("call params: %v", err)
	}
	hostcall := func(id string) worker.Frame {
		next := worker.Frame{Kind: worker.KindRequest, ID: id, Method: worker.MethodHostcall}
		next.Params, _ = json.Marshal(worker.AuthorHostcallParams{
			Token: params.Context.Token, Service: extension.ContractCredentials, Method: "get", Arguments: worker.JSONBytes(`{"field":"/token"}`),
		})
		return next
	}
	for i := range executableLiveCalls {
		blocked.send(hostcall("w" + strconv.Itoa(i)))
	}
	<-entered
	blocked.send(hostcall("over-limit"))
	got := <-outcome
	testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
	testassert.Require(t, strings.Contains(got.Result.Content, "live hostcalls"), "capped activation = %q", got.Result.Content)
}

func TestExecutableEnvironmentUsesSecretExcludedProducer(t *testing.T) {
	manifest := executableTestManifest()
	t.Setenv("EXEC_PROVIDER_KEY", "provider-secret")
	t.Setenv("EXEC_DECLARED_SECRET", "declared-secret")
	t.Setenv("EXEC_UNRELATED", "kept")
	provider := `{"providers":{"prov":` + nominalProvMember("http://prov.test/v1", "EXEC_PROVIDER_KEY", `{"m":{"context_window":4096}}`) + `},"extensions":{"peer":`
	var service *configurationService
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }), nil
	}}
	spawner.install()
	h := newServiceHarness(t)
	writeServiceFile(t, h.configPath, provider+`{"enabled":false}}}`)
	service = h.preparedService(context.Background(), executableAuthorExtension(manifest, "peer", func(ctx context.Context, excluded []string) ([]string, error) {
		return service.executableEnvironment(ctx, excluded)
	}))
	if _, err := service.publish(context.Background()); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	writeServiceFile(t, h.configPath, provider+`{"enabled":true,"token":"EXEC_DECLARED_SECRET"}}}`)
	if _, err := service.publish(context.Background()); err != nil {
		t.Fatalf("enable publish: %v", err)
	}
	defer service.current().resources.release()
	environment := spawner.environment(0)
	for _, name := range []string{"EXEC_PROVIDER_KEY", "EXEC_DECLARED_SECRET"} {
		testassert.Require(t, !slices.Contains(environment, name+"="), "worker environment inherited %s", name)
	}
	testassert.Require(t, slices.Contains(environment, "EXEC_UNRELATED=kept"), "worker environment dropped an ordinary entry")
}

func TestExecutableRejectsInvalidToolSchemaWithoutReplacement(t *testing.T) {
	manifest := executableTestManifest()
	rejected := make(chan [2]error, 1)
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			definition := model.ToolDefinition{Name: "probe", Parameters: []byte(`{"type":"object"}`)}
			old, err := api.RegisterTool(definition, registrarTestTool{}, nil)
			if err != nil {
				return nil, err
			}
			definition.Parameters = []byte(`{"type":7}`)
			_, first := api.RegisterTool(definition, registrarTestTool{}, nil)
			_, replacement := api.RegisterTool(definition, registrarTestTool{}, old)
			rejected <- [2]error{first, replacement}
			return nil, nil
		}), nil
	}}
	spawner.install()
	owner := &configurationService{}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: executableAuthorExtension(manifest, "peer", func(context.Context, []string) ([]string, error) { return []string{}, nil }), section: []byte(`{}`), installed: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	for _, err := range <-rejected {
		if !errors.Is(err, harness.ErrInvalid) {
			t.Fatalf("invalid schema = %v", err)
		}
	}
	stage := candidate.snapshotForTest()
	defer stage.release()
	if stage.tools["probe"] == nil {
		t.Fatal("invalid replacement removed the original registration")
	}
}

func TestExecutableDisableKeepsParkedCall(t *testing.T) {
	manifest := executableTestManifest()
	blocking := newAdapterBlockingTool()
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterTool(testDescribeTool(executableTestToolName), blocking, nil)
			return nil, err
		}), blocking.close
	}}
	spawner.install()
	var service *configurationService
	h := newServiceHarness(t)
	writeServiceFile(t, h.configPath, `{"extensions":{"peer":{"enabled":true}}}`)
	service = h.preparedService(context.Background(), executableAuthorExtension(manifest, "peer", func(ctx context.Context, excluded []string) ([]string, error) {
		return service.executableEnvironment(ctx, excluded)
	}))
	if _, err := service.publish(context.Background()); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	captured := service.current().native.capture()
	if captured == nil {
		t.Fatal("no admission capture")
	}
	stage, err := captured.stage()
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	binding := stage.bindTool(executableTestToolName)
	if binding == nil {
		t.Fatal("peer.tool is not registered")
	}
	tool := binding.entry.value.(nativeTool).tool
	outcome := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	select {
	case <-blocking.entered:
	case got := <-outcome:
		t.Fatalf("parked call settled before it parked: %+v", got)
	case <-time.After(2 * time.Second):
		t.Fatal("parked call did not start")
	}
	writeServiceFile(t, h.configPath, `{"extensions":{"peer":{"enabled":false}}}`)
	if _, err := service.publish(context.Background()); err != nil {
		t.Fatalf("disable publish: %v", err)
	}
	select {
	case got := <-outcome:
		t.Fatalf("parked call settled during disable: %+v", got)
	default:
	}
	select {
	case <-spawner.last().leaderDone:
		t.Fatal("worker stopped while a captured set still held it")
	default:
	}
	blocking.close()
	select {
	case got := <-outcome:
		if got.Result.Content != "released" || got.Evidence != harness.ExecutionObserved {
			t.Fatalf("parked call outcome = %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked call did not finish")
	}
	binding.release()
	stage.release()
	captured.release()
	select {
	case <-spawner.last().leaderDone:
	case <-time.After(executableTestTimeout):
		t.Fatal("worker did not stop after the last release")
	}
	service.current().resources.release()
}

func TestExecutableWorkerCredentialHostService(t *testing.T) {
	const name = "EXEC_WORKER_SECRET"
	t.Setenv(name, "activation-secret")
	manifest := executableTestManifest()
	var service *configurationService
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterService(extension.Service{Name: "peer.service", Methods: []extension.RegisteredMethod{{
				Method: extension.ServiceMethod("echo", `{"type":"object"}`, `{}`, "", ""),
				Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
					return call.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/token"}`))
				},
			}}}, nil)
			return nil, err
		}), nil
	}}
	spawner.install()
	h := newServiceHarness(t)
	writeServiceFile(t, h.configPath, `{"extensions":{"peer":{"enabled":true,"token":"`+name+`"}}}`)
	service = h.preparedService(context.Background(), executableAuthorExtension(manifest, "peer", func(ctx context.Context, excluded []string) ([]string, error) {
		return service.executableEnvironment(ctx, excluded)
	}))
	if _, err := service.publish(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer service.current().resources.release()
	result, err := service.current().native.callServiceScoped(context.Background(), "peer.service", "echo", json.RawMessage(`{"query":1}`), &nativeCallScope{kind: callKindHook, host: service.current().composition})
	if err != nil {
		t.Fatalf("service: %v", err)
	}
	if string(result.Value) != `{"value":"activation-secret"}` {
		t.Fatalf("credential value = %q", result.Value)
	}
}

func TestExecutableOwnerCloseBoundedUnresponsiveCall(t *testing.T) {
	manifest := executableTestManifest()
	called := make(chan struct{}, 1)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer {
		return newTestExecutablePeer(t, true, authoringPeer(manifest, func(peer *testExecutablePeer, _ worker.Frame) {
			select {
			case called <- struct{}{}:
			default:
			}
		}))
	}}
	spawner.install()
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, `{"providers":{},"extensions":{"peer":{"enabled":true}}}`)
	installExecutablePackage(t, dataDir, manifest, "peer")
	owner, err := Open(context.Background(), Options{
		DataDir:       dataDir,
		Configuration: NewFileConfigurationSource(configPath),
		Credentials:   NewEnvironmentCredentialSource(filepath.Join(dataDir, ".env")),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	captured := owner.config.current().native.capture()
	if captured == nil {
		t.Fatal("no admission capture")
	}
	awaitRegistered(t, owner.config.current().native, executableTestToolName)
	stage, err := captured.stage()
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	binding := stage.bindTool(executableTestToolName)
	if binding == nil {
		t.Fatal("peer.tool is not registered")
	}
	tool := binding.entry.value.(nativeTool).tool
	binding.release()
	stage.release()
	captured.release()
	outcome := make(chan harness.ToolOutcome, 1)
	go func() { outcome <- tool.Execute(context.Background(), extension.AuthorContext{}, extension.ToolPlan{}) }()
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("the call never reached the worker")
	}
	closed := make(chan error, 1)
	go func() { closed <- owner.Close(context.Background()) }()
	select {
	case <-closed:
	case <-time.After(executableShutdownGrace + time.Second):
		t.Fatal("owner close did not complete within the grace window")
	}
	select {
	case got := <-outcome:
		testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
	case <-time.After(executableShutdownGrace + time.Second):
		t.Fatal("in-flight call did not settle")
	}
}

// adapterWorker is one in-process author-side worker.Serve process over pipes.
type adapterWorker struct {
	stdin      *io.PipeWriter
	stdout     *io.PipeReader
	serveIn    *io.PipeReader
	serveOut   *io.PipeWriter
	cancel     context.CancelFunc
	leaderDone chan struct{}
	leaderOnce sync.Once
	release    func()
}

// adapterAuthor is the in-process author entry one adapter worker serves.
func adapterAuthor(manifest extension.Manifest, activate func(context.Context, extension.API) (func(context.Context) error, error)) extension.Extension {
	return extension.Extension{Manifest: manifest, Activate: activate}
}

func newAdapterWorker(t *testing.T, author extension.Extension, release func()) *adapterWorker {
	t.Helper()
	serveIn, hostIn := io.Pipe()
	hostOut, serveOut := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &adapterWorker{
		stdin:      hostIn,
		stdout:     hostOut,
		serveIn:    serveIn,
		serveOut:   serveOut,
		cancel:     cancel,
		leaderDone: make(chan struct{}),
		release:    release,
	}
	go func() {
		_ = worker.Serve(ctx, author, serveIn, serveOut)
		_ = serveOut.Close()
		_ = serveIn.Close()
		adapter.finishLeader()
	}()
	return adapter
}

func (w *adapterWorker) finishLeader() {
	w.leaderOnce.Do(func() { close(w.leaderDone) })
}

func (w *adapterWorker) Stdin() io.WriteCloser { return w.stdin }
func (w *adapterWorker) Stdout() io.Reader     { return w.stdout }
func (w *adapterWorker) Wait() error           { <-w.leaderDone; return nil }
func (w *adapterWorker) Kill() error {
	w.cancel()
	w.finishLeader()
	_ = w.stdin.Close()
	_ = w.stdout.Close()
	_ = w.serveIn.Close()
	_ = w.serveOut.Close()
	if w.release != nil {
		w.release()
	}
	return nil
}

// adapterSpawner replaces the process seam with in-process adapter workers.
type adapterSpawner struct {
	t       *testing.T
	factory func() (extension.Extension, func())
	mu      sync.Mutex
	workers []*adapterWorker
	envs    [][]string
}

func (s *adapterSpawner) install() {
	old := spawnExecutable
	spawnExecutable = func(_ context.Context, _ string, environment []string) (executableProcess, error) {
		author, release := s.factory()
		adapter := newAdapterWorker(s.t, author, release)
		s.mu.Lock()
		s.workers = append(s.workers, adapter)
		s.envs = append(s.envs, append([]string(nil), environment...))
		s.mu.Unlock()
		return adapter, nil
	}
	s.t.Cleanup(func() { spawnExecutable = old })
}

func (s *adapterSpawner) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.workers)
}

func (s *adapterSpawner) last() *adapterWorker {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.workers) == 0 {
		s.t.Fatal("no adapter worker was spawned")
	}
	return s.workers[len(s.workers)-1]
}

func (s *adapterSpawner) environment(index int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.envs[index]
}

// adapterBlockingTool blocks Execute until release, ignoring cancellation.
type adapterBlockingTool struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newAdapterBlockingTool() *adapterBlockingTool {
	return &adapterBlockingTool{entered: make(chan struct{}), release: make(chan struct{})}
}

func (t *adapterBlockingTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *adapterBlockingTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{}
}

func (t *adapterBlockingTool) Execute(_ context.Context, _ extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	t.once.Do(func() { close(t.entered) })
	<-t.release
	return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess, Content: "released"}, Evidence: harness.ExecutionObserved}
}

func (t *adapterBlockingTool) close() {
	select {
	case <-t.release:
	default:
		close(t.release)
	}
}

func TestExecutableInvalidServiceResultFailsActivation(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	type callResult struct {
		value extension.Result
		err   error
	}
	called := make(chan callResult, 1)
	go func() {
		value, err := activation.authorServiceCall(context.Background(), "peer", "echo", extension.AuthorContext{}, json.RawMessage(`{}`))
		called <- callResult{value: value, err: err}
	}()
	frame := peer.next()
	peer.send(worker.Frame{Kind: worker.KindResponse, ID: frame.ID, Result: json.RawMessage(`{}`)})
	got := <-called
	testassert.Require(t, got.err != nil && strings.Contains(got.err.Error(), "exactly one"), "invalid service terminal = %v", got.err)
	activation.mu.Lock()
	failed := activation.failed
	activation.mu.Unlock()
	if failed == nil {
		t.Fatal("invalid service terminal did not fail the activation")
	}
	testassert.Equal(t, tool.Execute(context.Background(), extension.AuthorContext{}, extension.ToolPlan{}).Evidence, harness.ExecutionNotStarted)
}

func TestExecutableTerminalBeforeFailureKeepsOutcome(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	plan := extension.ToolPlan{}
	first := runToolExecute(t, tool, extension.AuthorContext{RequestID: "first"}, plan)
	second := runToolExecute(t, tool, extension.AuthorContext{RequestID: "second"}, plan)
	frames := make(map[string]worker.Frame, 2)
	for len(frames) < 2 {
		frame := peer.next()
		var params worker.AuthorCallParams
		if err := json.Unmarshal(frame.Params, &params); err != nil {
			t.Fatalf("decode call params: %v", err)
		}
		frames[params.Context.RequestID] = frame
	}
	peer.sendValue(frames["first"].ID, encodeValue(t, worker.EncodeExecuteResult, harness.ToolOutcome{
		Result:   model.ToolResult{CallID: "first", Status: model.ResultSuccess, Content: "accepted"},
		Evidence: harness.ExecutionObserved,
	}))
	peer.send(worker.Frame{Kind: worker.KindResponse, ID: frames["second"].ID, Result: json.RawMessage(`[]`)})
	if got := <-first; got.Result.Content != "accepted" || got.Evidence != harness.ExecutionObserved {
		t.Fatalf("accepted terminal was overwritten: %+v", got)
	}
	if got := <-second; got.Evidence != harness.ExecutionUnknown {
		t.Fatalf("malformed terminal outcome = %+v", got)
	}
}

// leaderExitProcess reports leader exit independently of the stdout pipe and
// takes the descendant down with Kill.
type leaderExitProcess struct {
	inner  *testExecutableProcess
	leader chan struct{}
	killed chan struct{}
	once   sync.Once
}

func (p *leaderExitProcess) Stdin() io.WriteCloser { return p.inner.Stdin() }
func (p *leaderExitProcess) Stdout() io.Reader     { return p.inner.Stdout() }
func (p *leaderExitProcess) Wait() error           { <-p.leader; return nil }
func (p *leaderExitProcess) Kill() error {
	p.once.Do(func() { close(p.killed) })
	return p.inner.Kill()
}

func TestExecutableLeaderExitWithDescendantHoldingStdout(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	process := &leaderExitProcess{inner: peer.process(), leader: make(chan struct{}), killed: make(chan struct{})}
	old := spawnExecutable
	spawnExecutable = func(context.Context, string, []string) (executableProcess, error) { return process, nil }
	t.Cleanup(func() { spawnExecutable = old })

	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	outcome := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	peer.next() // the call was written
	eventually(t, "call write acknowledgement", func() bool {
		sent, _ := activationPending(activation)
		return sent == 1
	})
	close(process.leader)
	select {
	case <-process.killed:
	case <-time.After(2 * time.Second):
		t.Fatal("descendant process group was not killed after leader exit")
	}
	select {
	case got := <-outcome:
		testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
	case <-time.After(2 * time.Second):
		t.Fatal("lost-result call did not settle after leader exit")
	}
}

// gatedKillProcess delays physical termination so tests can observe that
// callers are not released before the process group is gone.
type gatedKillProcess struct {
	inner       *testExecutableProcess
	killStarted chan struct{}
	releaseKill chan struct{}
	once        sync.Once
}

func (p *gatedKillProcess) Stdin() io.WriteCloser { return p.inner.Stdin() }
func (p *gatedKillProcess) Stdout() io.Reader     { return p.inner.Stdout() }
func (p *gatedKillProcess) Wait() error           { return p.inner.Wait() }
func (p *gatedKillProcess) Kill() error {
	p.once.Do(func() { close(p.killStarted) })
	<-p.releaseKill
	return p.inner.Kill()
}

func TestExecutableFailureReleasesAfterProcessGone(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	process := &gatedKillProcess{inner: peer.process(), killStarted: make(chan struct{}), releaseKill: make(chan struct{})}
	old := spawnExecutable
	spawnExecutable = func(context.Context, string, []string) (executableProcess, error) { return process, nil }
	t.Cleanup(func() { spawnExecutable = old })

	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	outcome := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	call := peer.next()
	peer.send(worker.Frame{Kind: worker.KindResponse, ID: call.ID, Result: json.RawMessage(`[]`)})
	select {
	case <-process.killStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("failure did not start process-group termination")
	}
	select {
	case got := <-outcome:
		t.Fatalf("caller released before the process group was gone: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
	close(process.releaseKill)
	got := <-outcome
	testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
}

func TestExecutableHostcallResponseDoesNotBlockTerminalIntake(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)

	result := make(chan extension.Result, 1)
	go func() {
		value, err := activation.authorServiceCall(context.Background(), "peer", "echo", extension.AuthorContext{
			Call: func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: json.RawMessage(`"hostcall"`)}, nil
			},
		}, json.RawMessage(`{}`))
		if err != nil {
			t.Errorf("service: %v", err)
		}
		result <- value
	}()
	serviceFrame := peer.next()
	var params worker.AuthorCallParams
	if err := json.Unmarshal(serviceFrame.Params, &params); err != nil {
		t.Fatalf("service params: %v", err)
	}
	hostcall := worker.Frame{Kind: worker.KindRequest, ID: "w1", Method: worker.MethodHostcall}
	hostcall.Params, _ = json.Marshal(worker.AuthorHostcallParams{
		Token: params.Context.Token, Service: extension.ContractCredentials, Method: "get", Arguments: worker.JSONBytes(`{"field":"/token"}`),
	})
	peer.send(hostcall)
	// The peer no longer reads stdin, so the queued hostcall response blocks
	// the writer. The service terminal on stdout must still be consumed.
	peer.sendValue(serviceFrame.ID, encodeValue(t, worker.EncodeServiceResult, extension.Result{Value: json.RawMessage(`"done"`)}))
	select {
	case got := <-result:
		if string(got.Value) != `"done"` {
			t.Fatalf("service result = %q", got.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal intake blocked behind a hostcall response")
	}
	peer.exit()
}

// adapterEnvironment is the empty spawn environment the in-process adapter
// workers use.
func adapterEnvironment(context.Context, []string) ([]string, error) { return []string{}, nil }

// publishAdapterWorker publishes one in-process adapter author as the enabled
// peer extension and returns its service and published snapshot.
func publishAdapterWorker(t *testing.T, activate func(context.Context, extension.API) (func(context.Context) error, error), release func()) (*configurationService, *nativeSnapshot) {
	t.Helper()
	manifest := executableTestManifest()
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, activate), release
	}}
	spawner.install()
	h := newServiceHarness(t)
	writeServiceFile(t, h.configPath, `{"extensions":{"peer":{"enabled":true}}}`)
	service := h.preparedService(context.Background(), executableAuthorExtension(manifest, "peer", adapterEnvironment))
	if _, err := service.publish(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Cleanup(func() { service.current().resources.release() })
	stage := service.current().native.snapshotForTest()
	if stage == nil {
		t.Fatal("no published snapshot")
	}
	t.Cleanup(func() { stage.release() })
	return service, stage
}

// stagedExecutableTool binds one published snapshot's executable tool.
func stagedExecutableTool(t *testing.T, stage *nativeSnapshot) extension.RegisteredTool {
	t.Helper()
	binding := stage.bindTool(executableTestToolName)
	if binding == nil {
		t.Fatalf("%s is not registered", executableTestToolName)
	}
	t.Cleanup(func() { binding.release() })
	return binding.entry.value.(nativeTool).tool
}

func TestExecutableCancelledCallUnresponsiveWorker(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	ctx, cancel := context.WithCancel(context.Background())
	outcome := make(chan harness.ToolOutcome, 1)
	go func() {
		outcome <- tool.Execute(ctx, extension.AuthorContext{}, extension.ToolPlan{})
	}()
	peer.next() // the call was delivered; the worker never answers
	cancel()
	select {
	case got := <-outcome:
		testassert.Equal(t, got.Result.Status, model.ResultInterrupted)
		testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
	case <-time.After(executableTestTimeout):
		t.Fatal("cancelled call did not settle after the grace window")
	}
	select {
	case <-activation.terminated:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled call did not finish terminating the activation")
	}
}

// adapterCancelTool blocks one execution until its callback context ends and
// can be switched to an immediate result for the late-cancellation case.
type adapterCancelTool struct {
	block   bool
	entered chan struct{}
	once    sync.Once
}

func (t *adapterCancelTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (t *adapterCancelTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{}
}
func (t *adapterCancelTool) Execute(ctx context.Context, _ extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	if t.block {
		t.once.Do(func() { close(t.entered) })
		<-ctx.Done()
	}
	return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess, Content: "done"}, Evidence: harness.ExecutionObserved}
}

func TestExecutableCooperativeAndLateCancellation(t *testing.T) {
	tool := &adapterCancelTool{block: true, entered: make(chan struct{})}
	_, stage := publishAdapterWorker(t, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool(executableTestToolName), tool, nil)
		return nil, err
	}, nil)
	executableTool := stagedExecutableTool(t, stage)
	plan := extension.ToolPlan{}
	ctx, cancel := context.WithCancel(context.Background())
	outcome := make(chan harness.ToolOutcome, 1)
	go func() { outcome <- executableTool.Execute(ctx, extension.AuthorContext{}, plan) }()
	select {
	case <-tool.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cooperative call did not start")
	}
	cancel()
	select {
	case got := <-outcome:
		if got.Result.Content != "done" || got.Evidence != harness.ExecutionObserved {
			t.Fatalf("cooperative cancellation outcome = %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cooperative cancellation did not settle")
	}

	tool.block = false
	lateCtx, lateCancel := context.WithCancel(context.Background())
	late := make(chan harness.ToolOutcome, 1)
	go func() { late <- executableTool.Execute(lateCtx, extension.AuthorContext{}, plan) }()
	var lateOutcome harness.ToolOutcome
	select {
	case lateOutcome = <-late:
	case <-time.After(2 * time.Second):
		t.Fatal("late-cancellation call did not settle")
	}
	lateCancel()
	if lateOutcome.Result.Content != "done" || lateOutcome.Evidence != harness.ExecutionObserved {
		t.Fatalf("late cancellation outcome = %+v", lateOutcome)
	}
}

func TestExecutableOversizeCallFailsItselfOnly(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	defer activation.stop()

	oversize := extension.ToolPlan{
		Data: make([]byte, worker.FrameLimit),
	}
	outcome := tool.Execute(context.Background(), extension.AuthorContext{}, oversize)
	testassert.Equal(t, outcome.Evidence, harness.ExecutionNotStarted)
	testassert.Equal(t, outcome.Result.Status, model.ResultError)

	follow := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	call := peer.next()
	peer.sendValue(call.ID, encodeValue(t, worker.EncodeExecuteResult, harness.ToolOutcome{
		Result:   model.ToolResult{CallID: "call-1", Status: model.ResultSuccess, Content: "ok"},
		Evidence: harness.ExecutionObserved,
	}))
	got := <-follow
	if got.Result.Content != "ok" || got.Evidence != harness.ExecutionObserved {
		t.Fatalf("follow-up outcome = %+v", got)
	}
}

func TestExecutableTerminalRacingActivationFailure(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	process := &leaderExitProcess{inner: peer.process(), leader: make(chan struct{}), killed: make(chan struct{})}
	old := spawnExecutable
	spawnExecutable = func(context.Context, string, []string) (executableProcess, error) { return process, nil }
	t.Cleanup(func() { spawnExecutable = old })

	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	outcome := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	call := peer.next()
	peer.sendValue(call.ID, encodeValue(t, worker.EncodeExecuteResult, harness.ToolOutcome{
		Result:   model.ToolResult{CallID: "call-1", Status: model.ResultSuccess, Content: "accepted"},
		Evidence: harness.ExecutionObserved,
	}))
	// The leader exits as the terminal is in flight; termination must drain the
	// reader before it settles anything.
	close(process.leader)
	select {
	case got := <-outcome:
		if got.Result.Content != "accepted" || got.Evidence != harness.ExecutionObserved {
			t.Fatalf("terminal racing failure was overwritten: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal racing failure did not settle")
	}
	select {
	case <-activation.terminated:
	case <-time.After(executableTestTimeout):
		t.Fatal("leader exit did not finish terminating the activation")
	}
}

// toggleFailWriter fails writes after the activation is established.
type toggleFailWriter struct {
	io.WriteCloser
	fail atomic.Bool
}

func (w *toggleFailWriter) Write(data []byte) (int, error) {
	if w.fail.Load() {
		return 0, errors.New("write failed")
	}
	return w.WriteCloser.Write(data)
}

type writeFailProcess struct {
	inner  *testExecutableProcess
	writer *toggleFailWriter
}

func (p *writeFailProcess) Stdin() io.WriteCloser { return p.writer }
func (p *writeFailProcess) Stdout() io.Reader     { return p.inner.Stdout() }
func (p *writeFailProcess) Wait() error           { return p.inner.Wait() }
func (p *writeFailProcess) Kill() error           { return p.inner.Kill() }

func TestExecutableWriteFailureTerminatesWithoutEarlySettlement(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	process := &writeFailProcess{inner: peer.process(), writer: &toggleFailWriter{WriteCloser: nil}}
	process.writer.WriteCloser = process.inner.Stdin()
	old := spawnExecutable
	spawnExecutable = func(context.Context, string, []string) (executableProcess, error) { return process, nil }
	t.Cleanup(func() { spawnExecutable = old })

	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	process.writer.fail.Store(true)
	outcome := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	select {
	case got := <-outcome:
		testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
	case <-time.After(2 * time.Second):
		t.Fatal("write failure did not settle the call")
	}
	activation.mu.Lock()
	failed := activation.failed
	activation.mu.Unlock()
	if failed == nil {
		t.Fatal("write failure did not terminate the activation")
	}
	select {
	case <-activation.terminated:
	case <-time.After(executableTestTimeout):
		t.Fatal("write failure did not finish terminating the activation")
	}
}

func TestExecutableUnsentCancelSettlesNotStarted(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	plan := extension.ToolPlan{}
	first := runToolExecute(t, tool, extension.AuthorContext{}, plan)
	eventually(t, "first call to block the writer", func() bool {
		sent, _ := activationPending(activation)
		return sent == 1
	})
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan harness.ToolOutcome, 1)
	go func() { second <- tool.Execute(ctx, extension.AuthorContext{}, plan) }()
	eventually(t, "second call to queue unclaimed", func() bool {
		_, total := activationPending(activation)
		return total == 2
	})
	cancel()
	select {
	case got := <-second:
		testassert.Equal(t, got.Evidence, harness.ExecutionNotStarted)
		testassert.Equal(t, got.Result.Status, model.ResultInterrupted)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("unsent cancellation did not settle immediately")
	}
	activation.mu.Lock()
	failed := activation.failed
	activation.mu.Unlock()
	if failed != nil {
		t.Fatalf("unsent cancellation terminated the activation: %v", failed)
	}
	peer.exit()
	<-first
	select {
	case <-activation.terminated:
	case <-time.After(executableTestTimeout):
		t.Fatal("worker exit did not finish terminating the activation")
	}
}

func TestExecutableCancelledClassTerminalWithoutHostCancellationIsAuthorError(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	spawner := &testPeerSpawner{t: t, factory: func() *testExecutablePeer { return peer }}
	spawner.install()
	activation := startPeerActivation(t, peer, manifest)
	tool := &executableAuthorTool{activation: activation, handle: "peer"}
	defer activation.stop()
	outcome := runToolExecute(t, tool, extension.AuthorContext{}, extension.ToolPlan{})
	call := peer.next()
	peer.send(worker.Frame{Kind: worker.KindResponse, ID: call.ID, Error: &worker.Error{Class: worker.ClassCancelled, Message: "cancelled"}})
	got := <-outcome
	testassert.Equal(t, got.Result.Status, model.ResultError)
	testassert.Equal(t, got.Evidence, harness.ExecutionUnknown)
	testassert.Require(t, strings.Contains(got.Result.Content, "cancelled"), "tool error = %q, want the author's cancelled-class error", got.Result.Content)
}

// closeRecordingProcess records the parent-pipe close.
type closeRecordingProcess struct {
	inner  *testExecutableProcess
	closed chan struct{}
	once   sync.Once
}

func (p *closeRecordingProcess) Stdin() io.WriteCloser { return p.inner.Stdin() }
func (p *closeRecordingProcess) Stdout() io.Reader     { return p.inner.Stdout() }
func (p *closeRecordingProcess) Wait() error           { return p.inner.Wait() }
func (p *closeRecordingProcess) Kill() error           { return p.inner.Kill() }
func (p *closeRecordingProcess) CloseParent() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

func TestExecutableTerminationClosesParentPipes(t *testing.T) {
	manifest := executableTestManifest()
	peer := newTestExecutablePeer(t, false, nil)
	process := &closeRecordingProcess{inner: peer.process(), closed: make(chan struct{})}
	old := spawnExecutable
	spawnExecutable = func(context.Context, string, []string) (executableProcess, error) { return process, nil }
	t.Cleanup(func() { spawnExecutable = old })

	activation := startPeerActivation(t, peer, manifest)
	activation.stop()
	select {
	case <-process.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("termination did not close the parent pipe ends")
	}
}

func TestExecutableInvalidManifestRejectedAtInstall(t *testing.T) {
	manifest := executableTestManifest()
	manifest.APIVersion = "1"
	dataDir := t.TempDir()
	source := executablePackageSource(t, manifest, "peer")
	_, err := newPackageManager(dataDir).install(source)
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "install invalid manifest = %v, want invalid", err)
	mustNotExist(t, filepath.Join(dataDir, "extensions", extensionDataHash(manifest.ID), "selected"), "an invalid manifest created a selection")
}

func TestExecutableOwnerCloseRunsWorkerClose(t *testing.T) {
	manifest := executableTestManifest()
	closed := make(chan struct{})
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			if _, err := api.RegisterTool(testDescribeTool(executableTestToolName), testRegisteredTool{}, nil); err != nil {
				return nil, err
			}
			return func(context.Context) error {
				select {
				case <-closed:
				default:
					close(closed)
				}
				return nil
			}, nil
		}), nil
	}}
	spawner.install()
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, `{"extensions":{"peer":{"enabled":true}}}`)
	installExecutablePackage(t, dataDir, manifest, "peer")
	owner, err := Open(context.Background(), Options{
		DataDir:       dataDir,
		Configuration: NewFileConfigurationSource(configPath),
		Credentials:   NewEnvironmentCredentialSource(filepath.Join(dataDir, ".env")),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("owner close skipped the worker Close callback")
	}
}

func TestExecutableHookCancelledAuthorEvidence(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	callback := func(ctx context.Context, _ extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return harness.RequestConsequence{}, &harness.EffectError{Interrupted: true, Err: context.Canceled}
	}
	_, stage := publishAdapterWorker(t, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachRequest(callback, nil)
		return nil, err
	}, nil)
	if len(stage.hooks["request"]) != 1 {
		t.Fatal("effectful request hook is not registered")
	}
	hook := stage.hooks["request"][0].value.(nativeHook).handler.(extension.AuthorRequestHook)
	input := extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{RequestID: "request-1", Purpose: harness.InvocationPurposeConversation, Model: model.ModelRef{Provider: "provider", Model: "model"}}}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := hook(ctx, extension.AuthorContext{}, input)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker hook never started")
	}
	cancel()
	var got error
	select {
	case got = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("worker hook did not settle after cancellation")
	}
	testassert.Equal(t, harness.EffectInterrupted(got), true)
	testassert.Require(t, errors.Is(got, context.Canceled), "worker hook error = %v, want the author's cancellation cause", got)
}

func TestWorkerServiceErrorClassReachesClassifier(t *testing.T) {
	service, _ := publishAdapterWorker(t, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "peer.service", Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "echo", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{}, &worker.Error{Class: worker.ClassDenied, Message: "denied by worker"}
			},
		}}}, nil)
		return nil, err
	}, nil)
	_, err := service.current().native.callServiceScoped(context.Background(), "peer.service", "echo", json.RawMessage(`{}`), &nativeCallScope{kind: callKindHook, host: service.current().composition})
	if err == nil {
		t.Fatal("worker service denial returned no error")
	}
	testassert.Equal(t, ErrorCode(err), protocol.ErrorCodeDenied)
}

// TestExecutableCredentialRotationRetiresWorkerKeepsCapture proves a healthy
// worker activation is replaced when its declared activation credential value
// rotates, while work captured before the rotation keeps the original worker
// and its captured credential binding until that capture releases. An
// unrelated settings change with the unchanged section and credential reuses
// the activation without spawning another worker.
func TestExecutableCredentialRotationRetiresWorkerKeepsCapture(t *testing.T) {
	const name = "EXEC_ROTATING_CREDENTIAL"
	t.Setenv(name, "first")
	manifest := executableTestManifest()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
		return adapterAuthor(manifest, func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterService(extension.Service{Name: "peer.service", Methods: []extension.RegisteredMethod{{
				Method: extension.ServiceMethod("echo", `{}`, `{}`, "", ""),
				Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					if string(args) == `{"park":true}` {
						enteredOnce.Do(func() { close(entered) })
						<-release
					}
					return call.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/token"}`))
				},
			}}}, nil)
			return nil, err
		}), nil
	}}
	spawner.install()
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	config := func(settings string) string {
		return settings + `,"extensions":{"peer":{"enabled":true,"token":"` + name + `"}}}`
	}
	writeServiceFile(t, configPath, config(`{"providers":{}`))
	installExecutablePackage(t, dataDir, manifest, "author-worker")
	owner, err := Open(context.Background(), Options{
		DataDir:       dataDir,
		Configuration: NewFileConfigurationSource(configPath),
		Credentials:   NewEnvironmentCredentialSource(filepath.Join(dataDir, ".env")),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = owner.Close(context.Background()) }()
	if count := spawner.count(); count != 1 {
		t.Fatalf("initial spawns = %d, want 1", count)
	}
	original := spawner.last()
	host := owner.config.current().composition
	first := owner.config.current().native
	captured := first.capture()
	if captured == nil {
		t.Fatal("no admission capture")
	}
	defer captured.release()
	defer unblock()

	// An unrelated settings change leaves the section and credential unchanged,
	// so the published activation is reused instead of spawning another worker.
	writeServiceFile(t, configPath, config(`{"providers":{},"sessions":{"archive_after_days":3}`))
	if _, err := owner.Reload(context.Background(), testCaller(owner)); err != nil {
		t.Fatalf("settings publish: %v", err)
	}
	if count := spawner.count(); count != 1 {
		t.Fatalf("an unrelated settings change spawned %d workers, want 1", count)
	}

	// Park one call on the healthy activation.
	type serviceOutcome struct {
		value extension.Result
		err   error
	}
	parked := make(chan serviceOutcome, 1)
	go func() {
		value, err := first.callServiceScoped(context.Background(), "peer.service", "echo", json.RawMessage(`{"park":true}`), &nativeCallScope{kind: callKindHook, host: host})
		parked <- serviceOutcome{value: value, err: err}
	}()
	select {
	case <-entered:
	case got := <-parked:
		t.Fatalf("parked call settled before it parked: %+v", got)
	case <-time.After(2 * time.Second):
		t.Fatal("parked call did not start")
	}

	// Rotating the declared credential value publishes a new activation and
	// spawns its worker; the capture keeps the original worker alive.
	t.Setenv(name, "second")
	writeServiceFile(t, configPath, config(`{"providers":{},"sessions":{"archive_after_days":3}`))
	if _, err := owner.Reload(context.Background(), testCaller(owner)); err != nil {
		t.Fatalf("rotation publish: %v", err)
	}
	if count := spawner.count(); count != 2 {
		t.Fatalf("credential rotation spawned %d workers, want 2", count)
	}
	awaitRegistered(t, owner.config.current().native, "peer.service")
	select {
	case got := <-parked:
		t.Fatalf("parked call settled during rotation: %+v", got)
	default:
	}
	select {
	case <-original.leaderDone:
		t.Fatal("the original worker stopped while its capture still held it")
	default:
	}

	// New work is admitted on the new activation with the rotated value.
	result, err := owner.config.current().native.callServiceScoped(context.Background(), "peer.service", "echo", json.RawMessage(`{}`), &nativeCallScope{kind: callKindHook, host: host})
	if err != nil {
		t.Fatalf("rotated service call: %v", err)
	}
	testassert.Equal(t, string(result.Value), `{"value":"second"}`)

	// The parked call completes on the original worker with its captured
	// binding.
	unblock()
	select {
	case got := <-parked:
		testassert.NoError(t, got.err)
		testassert.Equal(t, string(got.value.Value), `{"value":"first"}`)
	case <-time.After(executableTestTimeout):
		t.Fatal("parked call did not finish")
	}

	// The original worker stops only after the capture releases.
	testassert.NoError(t, captured.release())
	select {
	case <-original.leaderDone:
	case <-time.After(executableTestTimeout):
		t.Fatal("the original worker did not stop after its capture released")
	}
}
