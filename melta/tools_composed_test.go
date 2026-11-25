package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/jobs"
	"github.com/Syntheresis/melta/internal/snapshot"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// The composed standard-tools tests: the directly constructed baseline tools
// run through a real Harness over real temporary SQLite — file effects,
// code-group creation on disk, snapshot reversion, and interruption recovery.
// The tools are built from owner state over one data root with a scripted or
// real jobs service; no optional registration is involved in opening them.

const toolsComposedDocument = `{"providers":{"prov":{"transport":{"base_url":"https://prov.test/v1","api_key_env":""},"discovery":false,"models":{"m":{"name":"M","context_window":4096}}}},"tools":{"read_max_lines":77}}`

const toolsComposedInvalidDocument = `{"providers":{"prov":{"transport":{"base_url":"https://prov.test/v1","api_key_env":""},"discovery":false,"models":{"m":{"name":"M","context_window":4096}}}},"tools":{"max_output_bytes":0}}`

const toolsComposedNullDocument = `{"providers":{"prov":{"transport":{"base_url":"https://prov.test/v1","api_key_env":""},"discovery":false,"models":{"m":{"name":"M","context_window":4096}}}},"tools":null}`

func writeToolsConfig(t *testing.T, path, doc string) {
	t.Helper()
	testassert.Require(t, json.Valid([]byte(doc)), "fixture document is not valid JSON: %s", doc)
	testassert.NoError(t, os.WriteFile(path, []byte(doc), 0o600))
}

// TestComposedToolsSettingsPublication proves the top-level tools section
// publication matrix over the directly composed baseline: a valid section
// publishes, a consumed-limit violation and a null section fail publication
// without replacing the live revision, and the next success continues the
// generation sequence. No optional registration is involved.
func TestComposedToolsSettingsPublication(t *testing.T) {
	ctx := context.Background()
	e := newComposeEnv(t)
	writeToolsConfig(t, e.configPath, toolsComposedDocument)
	r, err := Open(ctx, e.options())
	testassert.NoError(t, err)
	if rev, err := r.Reload(ctx, testCaller(r)); err != nil || rev.Generation != "2" {
		_ = r.Close(ctx)
		t.Fatalf("first Reload = (%q, %v), want revision 2", rev, err)
	}
	writeToolsConfig(t, e.configPath, toolsComposedInvalidDocument)
	if _, err := r.Reload(ctx, testCaller(r)); err == nil || !errors.Is(err, ErrConfiguration) {
		_ = r.Close(ctx)
		t.Fatalf("Reload with a nonpositive consumed limit = %v, want a complete publication failure", err)
	}
	writeToolsConfig(t, e.configPath, toolsComposedNullDocument)
	if _, err := r.Reload(ctx, testCaller(r)); err == nil || !errors.Is(err, ErrConfiguration) {
		_ = r.Close(ctx)
		t.Fatalf("Reload with a null tools section = %v, want the publisher's non-object rejection", err)
	}
	if current := r.config.current(); current == nil || current.generation != 2 {
		_ = r.Close(ctx)
		t.Fatalf("current revision after the null tools rejection = %+v, want the unchanged generation 2", current)
	}
	if data, err := os.ReadFile(e.configPath); err != nil || string(data) != toolsComposedNullDocument {
		_ = r.Close(ctx)
		t.Fatalf("config file after the null tools rejection = %q (%v), want the unchanged null fixture", data, err)
	}
	writeToolsConfig(t, e.configPath, toolsComposedDocument)
	rev, err := r.Reload(ctx, testCaller(r))
	if err != nil || rev.Generation != "3" {
		_ = r.Close(ctx)
		t.Fatalf("Reload after the failures = (%q, %v), want generation 3: failed publications published nothing", rev, err)
	}
	testassert.NoError(t, r.Close(ctx))
}

// fakeJobs is the scriptable jobs.Jobs test double: it records every call
// and serves scripted errors, reservations, and live results.
type fakeJobs struct {
	reserveErr error
	startErr   error
	live       []string

	reserves int
	aborted  []string
	started  []jobs.StartRequest
	reads    int
	kills    int
	lists    int
}

func (f *fakeJobs) Reserve() (string, error) {
	f.reserves++
	if f.reserveErr != nil {
		return "", f.reserveErr
	}
	return "b0b0b0b0", nil
}

func (f *fakeJobs) Abort(jobID string) { f.aborted = append(f.aborted, jobID) }

func (f *fakeJobs) Start(req jobs.StartRequest) error {
	f.started = append(f.started, req)
	return f.startErr
}

func (f *fakeJobs) Read(_, _ string) (string, error) {
	f.reads++
	return "", nil
}

func (f *fakeJobs) Preview(_, _ string) string { return "" }

func (f *fakeJobs) Kill(_, _ string) error {
	f.kills++
	return nil
}

func (f *fakeJobs) List(string) string {
	f.lists++
	return ""
}

func (f *fakeJobs) Live(string) []string { return f.live }

func (f *fakeJobs) StopJob(string, string) {}

func (f *fakeJobs) Close() error { return nil }

// realJobs constructs the real jobs service over one data root and registers
// its Close with the test.
func realJobs(t *testing.T, dataDir string) *jobs.Manager {
	t.Helper()
	manager := jobs.New(dataDir)
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

// openToolsComposed constructs the baseline standard tool values directly over
// the given data root and jobs service, optionally composing one extra plugin
// as an isolated Melta scope (the zero Plugin composes none) and returning
// its bound capability values.
func openToolsComposed(t *testing.T, dataDir string, jobsValue jobs.Jobs, _ any) (map[string]Tool, map[string]any) {
	t.Helper()
	return newStandardTools(dataDir, jobsValue, newLiveToolOutput(newObservation())), nil
}

func openTools(t *testing.T, dataDir string) map[string]Tool {
	t.Helper()
	return newStandardTools(dataDir, &fakeJobs{}, newLiveToolOutput(newObservation()))
}

// TestStandardToolsReservedAgainstShadowing proves the baseline tool universe:
// the five standard tool values are constructed directly with no
// registration, and a registration naming one standard tool is refused while
// the extension's own names still register.
func TestStandardToolsReservedAgainstShadowing(t *testing.T) {
	owner := &configurationService{}
	e := testNative("shadow", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterTool(model.ToolDefinition{Name: "read_file", Parameters: json.RawMessage(`{}`)}, testRegisteredTool{}, nil); err == nil {
			return nil, errors.New("standard tool shadow accepted")
		}
		_, err := api.RegisterTool(model.ToolDefinition{Name: "custom_tool", Parameters: json.RawMessage(`{}`)}, testRegisteredTool{}, nil)
		return nil, err
	})
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	testassert.NoError(t, err)
	defer candidate.release()
	stage := candidate.snapshotForTest()
	defer stage.release()
	testassert.Require(t, stage.tools["custom_tool"] != nil && stage.tools["read_file"] == nil, "registrations = %v, want only the extension's own name", stage.tools)
	byID, _ := openToolsComposed(t, t.TempDir(), &fakeJobs{}, nil)
	testassert.Equal(t, len(byID), 5)
}

// stubManagedEnv returns one real manager bound to a throwaway .env whose
// managed set the test controls; its SubprocessEnv method is the fixture's
// call-time environment producer.
func stubManagedEnv(t *testing.T) *config.ManagedEnv {
	t.Helper()
	return config.NewManagedEnv(filepath.Join(t.TempDir(), ".env"))
}

// envCallContext is a callToolContext whose call-time environment producer
// is one empty real manager: the ambient environment passes through
// unfiltered.
func envCallContext(t *testing.T, workspace string) ToolContext {
	t.Helper()
	tc := callToolContext(workspace)
	tc.SubprocessEnv = testSubprocessEnv(stubManagedEnv(t))
	return tc
}

// scriptBackground is the test BackgroundServices over no Harness: StartJob
// runs the spawn inline (or surfaces the scripted failure) with a no-op
// deliver.
type scriptBackground struct {
	startErr error
}

func (b scriptBackground) StartJob(ctx context.Context, _ string, spawn func(context.Context, func(string) error) error) error {
	if b.startErr != nil {
		return b.startErr
	}
	return spawn(ctx, func(string) error { return nil })
}

// harnessBackground is the production-shaped BackgroundServices over the
// fixture Harness; the delivered signal fires when the job's deliver closure
// reaches the Harness.
type harnessBackground struct {
	h         *harness.Harness
	sessionID string
	delivered chan struct{}
}

func (b *harnessBackground) StartJob(ctx context.Context, jobID string, spawn func(context.Context, func(string) error) error) error {
	return b.h.StartJob(ctx, b.sessionID, jobID, func(ctx context.Context, completionID string) error {
		return spawn(ctx, func(content string) error {
			err := b.h.DeliverBackgroundCompletion(context.Background(), b.sessionID, completionID, content)
			if err == nil {
				select {
				case b.delivered <- struct{}{}:
				default:
				}
			}
			return err
		})
	})
}

// backgroundImmediatePattern is the retained complete immediate template;
// the captured group is the job ID.
var backgroundImmediatePattern = regexp.MustCompile("^Command running in the background with ID: `([0-9a-f]{8})`\\. You will be notified when it finishes\\. If your next steps do not depend on its output, continue with them; otherwise use process to read the output\\.\\nRunning in the background: `[^`]*`\\.$")

// assertBackgroundImmediate validates one complete retained immediate
// template and returns its job ID.
func assertBackgroundImmediate(t *testing.T, content string) string {
	t.Helper()
	m := backgroundImmediatePattern.FindStringSubmatch(content)
	testassert.Require(t, m != nil, "result %q does not match the retained background template", content)
	return m[1]
}

// deltaStream is the scripted accepted model stream of the composed tools
// fixtures.
type deltaStream struct {
	deltas []model.StreamDelta
	i      int
}

func (s *deltaStream) Recv() (model.StreamDelta, error) {
	if s.i >= len(s.deltas) {
		return model.StreamDelta{}, io.EOF
	}
	d := s.deltas[s.i]
	s.i++
	return d, nil
}

func (s *deltaStream) Close() error { return nil }

func streamOf(deltas ...model.StreamDelta) model.Stream {
	return &deltaStream{deltas: deltas}
}

// toolCallStream is one completed assistant turn publishing one tool call.
func toolCallStream(id, name, args string) model.Stream {
	return streamOf(
		model.StreamDelta{HasChoice: true, ToolFragments: []model.ToolCallFragment{{ID: id, Name: name, ArgumentFragment: args}}},
		model.StreamDelta{HasChoice: true, FinishReason: "tool_calls"},
	)
}

// stopStream is one completed assistant turn with plain text and no calls.
func stopStream(text string) model.Stream {
	return streamOf(
		model.StreamDelta{HasChoice: true, ContentFragments: []model.ContentFragment{{Position: 0, Kind: model.PartText, Text: text}}},
		model.StreamDelta{HasChoice: true, FinishReason: "stop"},
	)
}

// toolsCapture is the execution capture advertising the given tool names.
func toolsCapture(names []string) harness.ExecutionCapture {
	definitions := make([]model.ToolDefinition, 0, len(names))
	for _, name := range names {
		definitions = append(definitions, model.ToolDefinition{Name: name, Parameters: json.RawMessage(`{"type":"object","properties":{}}`)})
	}
	return harness.ExecutionCapture{
		ConfigurationRevision: "1",
		Model:                 model.ModelRef{Provider: "prov", Model: "m"},
		ContextWindow:         4096,
		OutputReserve:         2048,
		SystemPrompt:          "composed tools",
		Tools:                 definitions,
		Compact: harness.CompactCapture{
			Model:        model.ModelRef{Provider: "prov", Model: "m"},
			SystemPrompt: "summarize",
		},
		Strategy: harness.StrategyCapture{ID: harness.IncludedStrategyID, RequiresContextWindow: true},
	}
}

// toolsHarnessOpts varies one composed tools Harness: the jobs provider (a
// fake or the real service), the advertised tool set, the background bridge,
// the captured invocation, the check hooks, and the call-time
// subprocess environment producer.
type toolsHarnessOpts struct {
	jobsValue     jobs.Jobs // the tool-facing jobs provider; realJobs wins when set
	realJobs      bool      // construct the real jobs service over the fixture data root
	advertise     []string
	background    BackgroundServices
	invocation    Invocation
	subprocessEnv func(context.Context) ([]string, error)
	checkHooks    []harness.ToolCheckHook
	// store, when non-nil, is the durable store behind the fixture's
	// Harness; the zero value opens a temporary SQLite store.
	store harness.Storage
}

// toolsHarness is one real Harness over one durable store whose
// execution's normalization and preparation callbacks are the real plugin
// tool values, wired with the admitted identity and the durable capture's
// constraints exactly as production preparation wires them.
type toolsHarness struct {
	t         *testing.T
	h         *harness.Harness
	dataDir   string
	workspace string
	store     harness.Storage

	mu        sync.Mutex
	admission harness.EntryRef
}

func newToolsHarness(t *testing.T, modelFn func(context.Context, model.Request, model.Extra) (model.Stream, error)) *toolsHarness {
	t.Helper()
	return newToolsHarnessWith(t, modelFn, toolsHarnessOpts{})
}

func newToolsHarnessWith(t *testing.T, modelFn func(context.Context, model.Request, model.Extra) (model.Stream, error), opts toolsHarnessOpts) *toolsHarness {
	t.Helper()
	ctx := context.Background()
	dataDir := t.TempDir()
	workspace := t.TempDir()

	store := opts.store
	if store == nil {
		opened, err := storage.OpenSQLite(filepath.Join(dataDir, "melta.db"))
		testassert.NoError(t, err)
		t.Cleanup(func() { _ = opened.Close() })
		store = opened
	}

	var jobsValue jobs.Jobs
	switch {
	case opts.realJobs:
		jobsValue = realJobs(t, dataDir)
	case opts.jobsValue != nil:
		jobsValue = opts.jobsValue
	default:
		jobsValue = &fakeJobs{}
	}
	byID, _ := openToolsComposed(t, dataDir, jobsValue, nil)
	advertise := opts.advertise
	if advertise == nil {
		advertise = []string{"read_file", "write_file", "edit_file"}
	}

	owner, cancel := context.WithCancel(ctx)
	th := &toolsHarness{t: t, dataDir: dataDir, workspace: workspace, store: store}
	deps := harness.Dependencies{
		Storage: store,
		Prepare: func(_ context.Context, _ harness.PreparationRequest) (harness.PreparedExecution, error) {
			return harness.PreparedExecution{
				Capture: toolsCapture(advertise),
				Open: func(_ context.Context, admission harness.OperationAdmission) (harness.Execution, error) {
					th.mu.Lock()
					th.admission = admission.AdmittedEntry
					th.mu.Unlock()
					background := opts.background
					if bg, ok := background.(*harnessBackground); ok {
						copied := *bg
						copied.sessionID = admission.AdmittedEntry.SessionID
						background = &copied
					}
					tc := ToolContext{
						Workspace:     workspace,
						AdmittedEntry: admission.AdmittedEntry,
						Invocation:    opts.invocation,
						Background:    background,
						SubprocessEnv: opts.subprocessEnv,
					}
					return harness.Execution{
						OpenInvocation: func(context.Context, harness.InvocationRequest) (harness.InvocationExecution, error) {
							return harness.InvocationExecution{
								Model:      modelFn,
								CheckStage: func() harness.CheckStage { return harness.CheckStage{Hooks: opts.checkHooks} },
								LookupTool: func(name string) (harness.ToolBinding, bool, error) {
									run, ok := byID[name]
									if !ok {
										return harness.ToolBinding{}, false, nil
									}
									var validate func(json.RawMessage) error
									for _, d := range standardToolDeclarations() {
										if d.id == name {
											validate, _ = compileToolValidator(d.definition)
											break
										}
									}
									return harness.ToolBinding{Identity: harness.ToolImplementation{ID: name}, Validate: validate, Run: func(ctx context.Context, call model.ToolCall) harness.ToolOutcome { return run(ctx, tc, call) }}, true, nil
								},
							}, nil
						},
					}, nil
				},
			}, nil
		},
	}
	// The directly constructed jobs service is also the Harness job-stop
	// seam; wire it exactly as production wires it so background starts
	// admit.
	if stopper, ok := jobsValue.(harness.JobStopper); ok {
		deps.Jobs = stopper
	}
	h, err := harness.New(owner, deps)
	if err != nil {
		cancel()
		t.Fatalf("harness.New: %v", err)
	}
	th.h = h
	t.Cleanup(cancel)
	if bg, ok := opts.background.(*harnessBackground); ok {
		bg.h = h
	}
	return th
}

func (th *toolsHarness) createSession() string {
	th.t.Helper()
	rec, err := th.h.CreateSession(context.Background(), harness.CreateSessionRequest{Workspace: th.workspace, ExtensionState: agentState("coder")})
	if err != nil {
		th.t.Fatalf("CreateSession: %v", err)
	}
	return rec.Identity.SessionID
}

func (th *toolsHarness) submit(sessionID, operationID, text string) {
	th.t.Helper()
	res, err := th.h.Submit(context.Background(), harness.SubmitRequest{
		SessionID: sessionID, OperationID: operationID, Origin: harness.InputOriginUser,
		Content: []model.ContentPart{{Kind: model.PartText, Text: text}}, Mode: harness.MessageModeRegular,
	})
	if err != nil || res.Disposition != harness.DispositionAdmitted {
		th.t.Fatalf("Submit = (%+v, %v), want admission", res, err)
	}
}

// submitAllowBuffered submits one message tolerating the buffered
// dispositions: the retiring-run window of a just-settled Operation routes
// the admission to the steering or queued buffer, and the post-terminal
// drain delivers it, so the caller's immediately following awaitSettled
// converges both paths. Only a Submit error — or a disposition outside the
// admitted-and-buffered set — is fatal. A buffered disposition creates the
// operation's register asynchronously — the retiring run's drain admits the
// item after the submit returns — so the helper converges before returning:
// it polls the register treating not-found as not-yet-converged within the
// same bounded deadline awaitSettled uses, so the immediately-following
// awaitSettled never first-polls the legal transient. An admitted
// disposition creates the register synchronously and needs no polling; a
// drain drop has no other admission to race and surfaces through the
// deadline expiry.
func (th *toolsHarness) submitAllowBuffered(sessionID, operationID, text string) {
	th.t.Helper()
	res, err := th.h.Submit(context.Background(), harness.SubmitRequest{
		SessionID: sessionID, OperationID: operationID, Origin: harness.InputOriginUser,
		Content: []model.ContentPart{{Kind: model.PartText, Text: text}}, Mode: harness.MessageModeRegular,
	})
	if err != nil {
		th.t.Fatalf("Submit(%s): %v", operationID, err)
	}
	switch res.Disposition {
	case harness.DispositionAdmitted:
		return
	case harness.DispositionSteering, harness.DispositionQueued:
	default:
		th.t.Fatalf("Submit(%s) = %+v, want an admitted or buffered disposition", operationID, res)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := th.h.ReadOperation(context.Background(), sessionID, operationID)
		if err == nil {
			return
		}
		if !errors.Is(err, harness.ErrNotFound) {
			th.t.Fatalf("ReadOperation(%s): %v", operationID, err)
		}
		if time.Now().After(deadline) {
			th.t.Fatalf("operation %q stayed unadmitted within the wait bound", operationID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitSettled waits for the operation's terminal settlement and returns it.
func (th *toolsHarness) awaitSettled(sessionID, operationID string) harness.OperationRecord {
	th.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		rec, err := th.h.ReadOperation(context.Background(), sessionID, operationID)
		if err != nil {
			th.t.Fatalf("ReadOperation: %v", err)
		}
		switch rec.State.Status {
		case harness.OperationSuccess, harness.OperationFailure, harness.OperationInterruption:
			return rec
		}
		if time.Now().After(deadline) {
			th.t.Fatalf("operation %q never settled: %+v", operationID, rec.State)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// readToolResults decodes every committed tool_result entry of one session,
// keyed by tool call ID.
func (th *toolsHarness) readToolResults(sessionID string) map[string]model.ToolResult {
	th.t.Helper()
	entries, err := th.store.ReadEntries(context.Background(), sessionID, 0)
	if err != nil {
		th.t.Fatalf("ReadEntries: %v", err)
	}
	results := make(map[string]model.ToolResult)
	for _, entry := range entries {
		if entry.Kind != harness.EntryToolResult {
			continue
		}
		var wire struct {
			ToolCallID string                 `json:"tool_call_id"`
			Status     model.ToolResultStatus `json:"status"`
			Content    string                 `json:"content"`
			Metadata   []byte                 `json:"metadata_base64"`
		}
		if err := json.Unmarshal(entry.Payload, &wire); err != nil {
			th.t.Fatalf("decode tool_result payload: %v", err)
		}
		results[wire.ToolCallID] = model.ToolResult{CallID: wire.ToolCallID, Status: wire.Status, Content: wire.Content}
	}
	return results
}

// TestComposedToolsHarnessAllowDenyEffect runs concrete plugin tool calls
// through the real Harness boundary with nothing refusing them: the
// in-Workspace read, the sensitive-basename read, an absolute read outside the
// Workspace, and the in-Workspace and outside-Workspace writes all reach their
// files. Both mutations are captured in the Session's snapshot group, and the
// outside write reverts with it.
func TestComposedToolsHarnessAllowDenyEffect(t *testing.T) {
	outside := t.TempDir()
	outsideRead := filepath.Join(outside, "outside.txt")
	outsideWrite := filepath.Join(outside, "written.txt")
	pathArgs := func(path string) string {
		raw, err := json.Marshal(map[string]string{"path": path})
		testassert.NoError(t, err)
		return string(raw)
	}
	writeArgs := func(path, content string) string {
		raw, err := json.Marshal(map[string]string{"path": path, "content": content})
		testassert.NoError(t, err)
		return string(raw)
	}
	attempt := 0
	var mu sync.Mutex
	modelFn := func(_ context.Context, _ model.Request, _ model.Extra) (model.Stream, error) {
		mu.Lock()
		attempt++
		n := attempt
		mu.Unlock()
		switch n {
		case 1:
			return toolCallStream("call-allowed", "read_file", pathArgs("notes.txt")), nil
		case 2:
			return toolCallStream("call-sensitive", "read_file", pathArgs(".env")), nil
		case 3:
			return toolCallStream("call-outside", "read_file", pathArgs(outsideRead)), nil
		case 4:
			return toolCallStream("call-write", "write_file", writeArgs("out/new.txt", "written")), nil
		case 5:
			return toolCallStream("call-outside-write", "write_file", writeArgs(outsideWrite, "outside-written")), nil
		default:
			return stopStream("done"), nil
		}
	}
	th := newToolsHarness(t, modelFn)
	testassert.NoError(t, os.WriteFile(filepath.Join(th.workspace, "notes.txt"), []byte("hello\n"), 0o644))
	testassert.NoError(t, os.WriteFile(filepath.Join(th.workspace, ".env"), []byte("SECRET=1\n"), 0o600))
	testassert.NoError(t, os.WriteFile(outsideRead, []byte("outside-read-marker\n"), 0o600))
	session := th.createSession()
	th.submit(session, "op-1", "run the tools")
	rec := th.awaitSettled(session, "op-1")
	testassert.Require(t, rec.State.Status == harness.OperationSuccess, "operation settled %q, want success: %+v", rec.State.Status, rec.State.Terminal)

	results := th.readToolResults(session)
	testassert.Equal(t, len(results), 5)
	allowed := results["call-allowed"]
	testassert.Require(t, allowed.Status == model.ResultSuccess && strings.Contains(allowed.Content, "1\thello"), "allowed read = %+v, want the bounded numbered content", allowed)
	sensitive := results["call-sensitive"]
	testassert.Require(t, sensitive.Status == model.ResultSuccess && strings.Contains(sensitive.Content, "SECRET=1"), "sensitive read = %+v, want the file content", sensitive)
	outsideResult := results["call-outside"]
	testassert.Require(t, outsideResult.Status == model.ResultSuccess && strings.Contains(outsideResult.Content, "outside-read-marker"), "outside read = %+v, want the file content", outsideResult)
	written := results["call-write"]
	testassert.Require(t, written.Status == model.ResultSuccess && strings.Contains(written.Content, "Wrote"), "write = %+v, want the write success", written)
	outsideWritten := results["call-outside-write"]
	testassert.Require(t, outsideWritten.Status == model.ResultSuccess && strings.Contains(outsideWritten.Content, "Wrote"), "outside write = %+v, want the write success", outsideWritten)
	data, err := os.ReadFile(filepath.Join(th.workspace, "out", "new.txt"))
	testassert.Require(t, err == nil && string(data) == "written", "written file = (%q, %v), want the effect on disk", data, err)
	data, err = os.ReadFile(outsideWrite)
	testassert.Require(t, err == nil && string(data) == "outside-written", "outside written file = (%q, %v), want the effect on disk", data, err)

	// Both mutating calls' code group lives at DataDir/code/<SessionID>/<admitted input EntryID>/snapshots/1/.
	th.mu.Lock()
	admitted := th.admission
	th.mu.Unlock()
	groupRoot := groupDir(th.dataDir, session, admitted.EntryID)
	codeStore, err := snapshot.OpenCodeStore(groupRoot)
	testassert.NoError(t, err)
	entries, err := codeStore.ListEntries()
	testassert.NoError(t, err)
	testassert.Equal(t, len(entries), 2)
	recorded := map[string]bool{}
	for _, entry := range entries {
		testassert.Require(t, !entry.Existed, "snapshot entry = %+v, want the new-file preimage of the declared target", entry)
		for _, want := range []string{filepath.Join("out", "new.txt"), outsideWrite} {
			if strings.HasSuffix(entry.OriginalPath, want) {
				recorded[want] = true
			}
		}
	}
	testassert.Require(t, recorded[filepath.Join("out", "new.txt")] && recorded[outsideWrite], "snapshot inventory = %v, want both written targets", recorded)
	{
		_, err := os.Stat(filepath.Join(th.dataDir, "code", session, "missing"))
		testassert.Require(t, errors.Is(err, os.ErrNotExist), "an unexpected second group directory exists")
	}

	reverted, err := snapshot.RevertCodeGroups([]string{groupRoot})
	testassert.NoError(t, err)
	testassert.Require(t, len(reverted.Skipped) == 0 && len(reverted.Restored) == 2, "revert = %+v, want both written targets restored", reverted)
	{
		_, err := os.Stat(outsideWrite)
		testassert.Require(t, errors.Is(err, os.ErrNotExist), "the outside write survived its revert (%v)", err)
	}
}

// TestComposedToolsHarnessRecovery proves the recovery sibling through the
// real plugin wiring: a running Operation whose execution never completes is
// settled as the terminal interruption when a fresh Melta composes over
// the same storage, with the admitted input and tool evidence intact and no
// new execution started.
func TestComposedToolsHarnessRecovery(t *testing.T) {
	ctx := context.Background()
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	modelFn := func(_ context.Context, _ model.Request, _ model.Extra) (model.Stream, error) {
		select {
		case arrived <- struct{}{}:
		default:
		}
		<-release
		return nil, errors.New("released after convergence")
	}
	th := newToolsHarness(t, modelFn)
	session := th.createSession()
	th.submit(session, "op-1", "park")

	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("the execution never reached its model effect")
	}

	// A fresh Melta composition over the same data root repairs the
	// abandoned running state before any execution can start.
	e := newComposeEnv(t)
	r, err := Open(ctx, testOptions(th.dataDir, e.configPath, e.home))
	testassert.NoError(t, err)
	testassert.NoError(t, r.Close(ctx))
	key := harness.RegisterKey{SessionID: session, Kind: harness.RegisterOperation, OperationID: "op-1"}
	reg, err := th.store.ReadRegister(ctx, key)
	testassert.NoError(t, err)
	testassert.Equal(t, registerStatus(t, reg.Payload), harness.OperationInterruption)
	entries, err := th.store.ReadEntries(ctx, session, 0)
	testassert.NoError(t, err)
	var kinds []string
	for _, entry := range entries {
		kinds = append(kinds, string(entry.Kind))
	}
	testassert.Require(t, strings.Contains(strings.Join(kinds, ","), string(harness.EntryInput)), "recovered entries = %v, want the admitted input intact", kinds)
}

func TestRunCommandTextOutcomes(t *testing.T) {
	dataDir := t.TempDir()
	byID := openTools(t, dataDir)
	ws := t.TempDir()

	t.Run("shell commands run each segment", func(t *testing.T) {
		outcome := byID["run_command"](context.Background(), envCallContext(t, ws), makeCall(t, "run_command", `{"command":"echo one; echo two && echo three"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "one\ntwo\nthree"), "outcome = %+v", outcome)
	})

	t.Run("all-blank text runs as a no-op", func(t *testing.T) {
		outcome := byID["run_command"](context.Background(), envCallContext(t, ws), makeCall(t, "run_command", `{"command":"   "}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == "(No output)", "result = %+v, want the retained no-op run", outcome.Result)
	})

	t.Run("empty command text is an argument error", func(t *testing.T) {
		outcome := byID["run_command"](context.Background(), envCallContext(t, ws), makeCall(t, "run_command", `{"command":""}`))
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == "run_command: command is required", "result = %+v", outcome.Result)
	})
}

func TestRunCommandExecuteOutcomes(t *testing.T) {
	dataDir := t.TempDir()
	byID := openTools(t, dataDir)
	ws := t.TempDir()
	tc := callToolContext(ws)
	tc.SubprocessEnv = testSubprocessEnv(stubManagedEnv(t))

	t.Run("success carries the captured output", func(t *testing.T) {
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"printf ok"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == "ok", "result = %+v, want the command output", outcome.Result)
	})

	t.Run("nonzero exit settles exactly as the legacy engine settles the ExitError", func(t *testing.T) {
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"printf fail && exit 7"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == "Error: Exit code 7\nfail", "result = %+v, want the engine's error settlement with the ExitError output", outcome.Result)
	})

	t.Run("a configured timeout settles as an error result with the retained text", func(t *testing.T) {
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"sleep 5","timeout":1}`))
		testassert.Require(t, outcome.Result.Status == model.ResultError && strings.HasPrefix(outcome.Result.Content, "Error: Exit code -1 (timeout)\n"), "result = %+v, want the retained timeout text as an error result", outcome.Result)
	})

	t.Run("cancellation settles as an interrupted result with the retained text", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		outcomeCh := make(chan struct {
			Status model.ToolResultStatus
			Body   string
		}, 1)
		go func() {
			outcome := byID["run_command"](ctx, tc, makeCall(t, "run_command", `{"command":"sleep 5"}`))
			outcomeCh <- struct {
				Status model.ToolResultStatus
				Body   string
			}{outcome.Result.Status, outcome.Result.Content}
		}()
		time.Sleep(100 * time.Millisecond)
		cancel()
		select {
		case outcome := <-outcomeCh:
			testassert.Require(t, outcome.Status == model.ResultInterrupted && outcome.Body == "command cancelled", "outcome = (%v, %q), want the retained cancellation text as interrupted", outcome.Status, outcome.Body)
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled command did not return")
		}
	})

	t.Run("output spills under the calling session's output directory", func(t *testing.T) {
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"yes overflowing-output | head -c 20000"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "result = %+v, want success with the spill marker", outcome.Result)
		wantPrefix := filepath.Join(dataDir, "code", testSessionID, "output", "cmd_output_")
		testassert.Require(t, strings.Contains(outcome.Result.Content, "saved to: "+wantPrefix), "result = %q, want the spill marker under %q", outcome.Result.Content, wantPrefix)
		marker := "saved to: "
		idx := strings.LastIndex(outcome.Result.Content, marker)
		path := outcome.Result.Content[idx+len(marker):]
		if end := strings.IndexAny(path, "]\n"); end >= 0 {
			path = path[:end]
		}
		data, err := os.ReadFile(strings.TrimSpace(path))
		testassert.Require(t, err == nil, "ReadFile(%q): %v", path, err)
		testassert.Equal(t, len(data), 20000)
	})

	t.Run("the managed key connected after Open is scrubbed while an unlisted sibling and the workspace cwd remain", func(t *testing.T) {
		t.Setenv("MELTA_TOOLS_PLUGIN_TEST", "env-value")
		t.Setenv("MELTA_TOOLS_PLUGIN_MANAGED", "managed-secret") // cleanup reservation only
		testassert.NoError(t, os.Unsetenv("MELTA_TOOLS_PLUGIN_MANAGED"))
		byID, _ := openToolsComposed(t, dataDir, &fakeJobs{}, nil)
		runCommand := byID["run_command"]
		// The key connects AFTER the tools plugin opened: the call-time
		// subprocess-environment producer, not any Open-captured copy, must
		// scrub it from the child.
		managed := config.NewManagedEnv(filepath.Join(t.TempDir(), ".env"))
		testassert.NoError(t, managed.TrySet("MELTA_TOOLS_PLUGIN_MANAGED", "managed-secret"))
		callTC := tc
		callTC.SubprocessEnv = testSubprocessEnv(managed)
		outcome := runCommand(context.Background(), callTC, makeCall(t, "run_command", `{"command":"printf '%s|%s' \"$MELTA_TOOLS_PLUGIN_TEST\" \"$MELTA_TOOLS_PLUGIN_MANAGED\"; pwd"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "result = %+v, want success", outcome.Result)
		content := outcome.Result.Content
		testassert.Require(t, strings.Contains(content, "env-value|") && !strings.Contains(content, "managed-secret") && strings.Contains(content, ws), "result = %q, want the unlisted key visible and the managed key scrubbed; result = %q, want the workspace cwd", content, content)
	})
}

func TestStandardToolArgumentsAndPathErrors(t *testing.T) {
	byID := openTools(t, t.TempDir())
	ws := t.TempDir()
	testassert.NoError(t, os.WriteFile(filepath.Join(ws, "f.txt"), []byte("one\ntwo\nthree"), 0o644))
	outcome := byID["read_file"](context.Background(), callToolContext(ws), makeCall(t, "read_file", `{"path":"f.txt","offset":1e0,"limit":1.0}`))
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.HasPrefix(outcome.Result.Content, "1\tone\n") && strings.Contains(outcome.Result.Content, "(Showing lines 1-1 of 3. Use offset=2 to continue.)"), "result = %+v", outcome.Result)
	invalid := model.ToolCall{ID: "call-1", Name: "read_file", Arguments: json.RawMessage(`{"path":`)}
	outcome = byID["read_file"](context.Background(), callToolContext(ws), invalid)
	testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.CallID == "call-1" && outcome.Result.Content != "", "result = %+v", outcome.Result)
	testassert.NoError(t, os.Symlink("loop", filepath.Join(ws, "loop")))
	outcome = byID["write_file"](context.Background(), callToolContext(ws), makeCall(t, "write_file", `{"path":"loop/out.txt","content":"x"}`))
	testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, "resolve path"), "result = %+v", outcome.Result)
}

func TestExecuteFileEffects(t *testing.T) {
	dataDir := t.TempDir()
	byID := openTools(t, dataDir)

	t.Run("read_file returns bounded numbered content and no metadata", func(t *testing.T) {
		ws := t.TempDir()
		testassert.NoError(t, os.WriteFile(filepath.Join(ws, "notes.txt"), []byte("one\ntwo\nthree\n"), 0o644))
		outcome := byID["read_file"](context.Background(), callToolContext(ws), makeCall(t, "read_file", `{"path":"notes.txt","offset":2,"limit":2}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.CallID == "call-1" && strings.Contains(outcome.Result.Content, "2\ttwo") && strings.Contains(outcome.Result.Content, "3\tthree") && outcome.Metadata == nil, "result = %+v, want success for call-1; content = %q, want the numbered requested window; metadata = %s, want none for read_file", outcome.Result, outcome.Result.Content, outcome.Metadata)
	})
	t.Run("read_file of a missing file returns the suggestion outcome", func(t *testing.T) {
		ws := t.TempDir()
		testassert.NoError(t, os.WriteFile(filepath.Join(ws, "notes.txt"), []byte("x\n"), 0o644))
		outcome := byID["read_file"](context.Background(), callToolContext(ws), makeCall(t, "read_file", `{"path":"notes.tx"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "not found") && strings.Contains(outcome.Result.Content, "Did you mean:") && strings.Contains(outcome.Result.Content, "notes.txt"), "result = %+v, want not-found text suggesting notes.txt", outcome)
	})
	t.Run("write_file creates parents, writes the file and captures the preimage group", func(t *testing.T) {
		dataDir := t.TempDir()
		byID := openTools(t, dataDir)
		ws := t.TempDir()
		outcome := byID["write_file"](context.Background(), callToolContext(ws), makeCall(t, "write_file", `{"path":"sub/dir/new.txt","content":"body"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "Wrote"), "result = %+v, want the write success", outcome.Result)
		data, err := os.ReadFile(filepath.Join(ws, "sub", "dir", "new.txt"))
		testassert.Require(t, err == nil && string(data) == "body", "written file = (%q, %v), want the new content", data, err)
		testassert.Require(t, outcome.Metadata == nil, "metadata = %s, want none for write_file", outcome.Metadata)
		// The preimage group lives at DataDir/code/<SessionID>/<EntryID>/snapshots/1/.
		turnDir := filepath.Join(groupDir(dataDir, testSessionID, testEntryID), "snapshots", "1")
		entries, err := os.ReadDir(turnDir)
		if err != nil || len(entries) != 1 {
			t.Fatalf("group turn dir = (%d entries, %v), want one snapshot entry", len(entries), err)
		}
		meta, err := os.ReadFile(filepath.Join(turnDir, entries[0].Name(), "meta.json"))
		testassert.NoError(t, err)
		var wire struct {
			OriginalPath string `json:"original_path"`
			Existed      bool   `json:"existed"`
		}
		testassert.NoError(t, json.Unmarshal(meta, &wire))
		testassert.Require(t, !wire.Existed && strings.HasSuffix(wire.OriginalPath, "new.txt"), "meta = %s, want the new-file preimage of the declared target", meta)
	})
	t.Run("edit_file mutates the file and emits the edit_preview metadata shape", func(t *testing.T) {
		ws := t.TempDir()
		testassert.NoError(t, os.WriteFile(filepath.Join(ws, "notes.txt"), []byte("alpha\nbeta\ngamma\n"), 0o644))
		outcome := byID["edit_file"](context.Background(), callToolContext(ws), makeCall(t, "edit_file", `{"path":"notes.txt","old_string":"beta","new_string":"BETA"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "lines 2"), "result = %+v, want the edit summary", outcome.Result)
		data, err := os.ReadFile(filepath.Join(ws, "notes.txt"))
		testassert.Require(t, err == nil && string(data) == "alpha\nBETA\ngamma\n", "edited file = (%q, %v), want the replacement", data, err)
		testassert.Require(t, outcome.Metadata != nil, "edit_file emitted no metadata")
		var metadata struct {
			EditPreview *struct {
				Hunks []struct {
					Rows []struct {
						Kind string `json:"kind"`
						Text string `json:"text"`
					} `json:"rows"`
				} `json:"hunks"`
			} `json:"edit_preview"`
		}
		{
			err := json.Unmarshal(outcome.Metadata, &metadata)
			testassert.Require(t, err == nil, "metadata %s is not the documented shape: %v", outcome.Metadata, err)
		}
		testassert.Require(t, metadata.EditPreview != nil && len(metadata.EditPreview.Hunks) == 1 && len(metadata.EditPreview.Hunks[0].Rows) == 2, "metadata = %s, want one hunk with the remove/add diff rows", outcome.Metadata)
	})
	t.Run("successive calls of one operation share the disk group through fresh handles", func(t *testing.T) {
		dataDir := t.TempDir()
		byID := openTools(t, dataDir)
		ws := t.TempDir()
		{
			outcome := byID["write_file"](context.Background(), callToolContext(ws), makeCall(t, "write_file", `{"path":"one.txt","content":"1"}`))
			testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "first write = %+v", outcome.Result)
		}
		{
			outcome := byID["write_file"](context.Background(), callToolContext(ws), makeCall(t, "write_file", `{"path":"two.txt","content":"2"}`))
			testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "second write = %+v", outcome.Result)
		}
		turnDir := filepath.Join(groupDir(dataDir, testSessionID, testEntryID), "snapshots", "1")
		entries, err := os.ReadDir(turnDir)
		if err != nil || len(entries) != 2 {
			t.Fatalf("group turn dir = (%d entries, %v), want both calls' preimages in the one group", len(entries), err)
		}
	})
	t.Run("another operation's admitted input opens its own group", func(t *testing.T) {
		dataDir := t.TempDir()
		byID := openTools(t, dataDir)
		ws := t.TempDir()
		tc := callToolContext(ws)
		tc.AdmittedEntry = harness.EntryRef{SessionID: testSessionID, EntryID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		{
			outcome := byID["write_file"](context.Background(), tc, makeCall(t, "write_file", `{"path":"other.txt","content":"x"}`))
			testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "write = %+v", outcome.Result)
		}
		turnDir := filepath.Join(groupDir(dataDir, testSessionID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), "snapshots", "1")
		if entries, err := os.ReadDir(turnDir); err != nil || len(entries) != 1 {
			t.Fatalf("other group turn dir = (%d entries, %v), want exactly one entry", len(entries), err)
		}
	})
}

// TestFailedCallRetainsEarlierSnapshot proves the failed-call retention
// sibling at the plugin boundary: a failed mutating call in the SAME
// Operation's code group — reopened through a fresh handle per call —
// discards only its own pre-mutation claim and never the earlier
// successful call's retained preimage.
func TestFailedCallRetainsEarlierSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	byID := openTools(t, dataDir)
	ws := t.TempDir()
	tc := callToolContext(ws)
	testassert.NoError(t, os.WriteFile(filepath.Join(ws, "a.txt"), []byte("alpha\n"), 0o644))
	testassert.NoError(t, os.WriteFile(filepath.Join(ws, "b.txt"), []byte("beta\n"), 0o644))

	// First call: a successful write retains its preimage in the group.
	{
		outcome := byID["write_file"](context.Background(), tc, makeCall(t, "write_file", `{"path":"a.txt","content":"ALPHA\n"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "first write = %+v, want success", outcome.Result)
	}

	// Second call: a failing edit in the same group — the shared body
	// captures b.txt's preimage, fails before any mutation, and discards
	// its own claim through the same fresh-handle group.
	{
		outcome := byID["edit_file"](context.Background(), tc, makeCall(t, "edit_file", `{"path":"b.txt","old_string":"missing","new_string":"x"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultError, "failing edit = %+v, want the error outcome", outcome.Result)
	}

	// The first call's retained preimage survives on disk; the failing
	// call discarded its own claim, so the turn holds exactly that one
	// entry.
	turnDir := filepath.Join(groupDir(dataDir, testSessionID, testEntryID), "snapshots", "1")
	entries, err := os.ReadDir(turnDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("group turn dir = (%d entries, %v), want only the first call's retained preimage", len(entries), err)
	}
	metaData, err := os.ReadFile(filepath.Join(turnDir, entries[0].Name(), "meta.json"))
	testassert.NoError(t, err)
	var meta struct {
		OriginalPath string `json:"original_path"`
	}
	testassert.NoError(t, json.Unmarshal(metaData, &meta))
	testassert.Require(t, strings.HasSuffix(meta.OriginalPath, "a.txt"), "retained entry = %q, want the first call's a.txt preimage", meta.OriginalPath)
	preimage, err := os.ReadFile(filepath.Join(turnDir, entries[0].Name(), "original"))
	testassert.Require(t, err == nil && string(preimage) == "alpha\n", "retained preimage = (%q, %v), want the first call's content", preimage, err)
	{
		data, err := os.ReadFile(filepath.Join(ws, "a.txt"))
		testassert.Require(t, err == nil && string(data) == "ALPHA\n", "written file = (%q, %v), want the first call's mutation intact", data, err)
	}
}

// TestBackgroundStartFailuresRenderOneWrapperAndAbortReservation pins the one
// error wrapper for every background start failure and the reservation abort
// on the StartJob error path.
func TestBackgroundStartFailuresRenderOneWrapperAndAbortReservation(t *testing.T) {
	ws := t.TempDir()

	t.Run("handoff rejection renders through the wrapper and aborts the reservation", func(t *testing.T) {
		fake := &fakeJobs{}
		byID, _ := openToolsComposed(t, t.TempDir(), fake, nil)
		tc := callToolContext(ws)
		tc.SubprocessEnv = testSubprocessEnv(stubManagedEnv(t))
		tc.Background = &scriptBackground{startErr: errors.New("background group is closed or stopping")}
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"true","background":true}`))
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == "run_command: background start: background group is closed or stopping", "result = %+v, want the one wrapper over the handoff rejection", outcome.Result)
		testassert.Require(t, len(fake.aborted) == 1 && fake.aborted[0] == "b0b0b0b0" && len(fake.started) == 0, "aborted = %v, want the reservation aborted once; got %v, want %v", fake.aborted, len(fake.started), 0)
	})

	t.Run("spawn failure renders through the wrapper and aborts the reservation", func(t *testing.T) {
		fake := &fakeJobs{startErr: errors.New("spawn boom")}
		toolOutput := newLiveToolOutput(newObservation())
		byID := newStandardTools(t.TempDir(), fake, toolOutput)
		tc := callToolContext(ws)
		tc.SubprocessEnv = testSubprocessEnv(stubManagedEnv(t))
		tc.Background = &scriptBackground{}
		call := makeCall(t, "run_command", `{"command":"true","background":true}`)
		outcome := byID["run_command"](context.Background(), tc, call)
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == "run_command: background start: spawn boom", "result = %+v, want the one wrapper over the spawn failure", outcome.Result)
		testassert.Require(t, len(fake.aborted) == 1 && fake.aborted[0] == "b0b0b0b0" && len(fake.started) == 1, "aborted = %v, want the reservation aborted once; got %v, want %v", fake.aborted, len(fake.started), 1)
		_, live := toolOutput.preview(tc.AdmittedEntry.SessionID, tc.OperationID, call.ID)
		testassert.Require(t, !live, "failed background call still has a registered output preview")
	})

	t.Run("reserve failure renders through the wrapper without an abort", func(t *testing.T) {
		fake := &fakeJobs{reserveErr: errors.New("process: manager is closed")}
		byID, _ := openToolsComposed(t, t.TempDir(), fake, nil)
		tc := callToolContext(ws)
		tc.SubprocessEnv = testSubprocessEnv(stubManagedEnv(t))
		tc.Background = &scriptBackground{}
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"true","background":true}`))
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == "run_command: background start: process: manager is closed", "result = %+v, want the one wrapper over the reserve failure", outcome.Result)
		testassert.Require(t, len(fake.aborted) == 0 && fake.reserves == 1, "aborted = %v, want no abort when the reservation never existed; got %v, want %v", fake.aborted, fake.reserves, 1)
	})
}

func TestRunCommandConcurrentManagedWritesSpawnChildren(t *testing.T) {
	const (
		externalKey = "MELTA_CONCURRENT_EXTERNAL"
		managedKey  = "MELTA_CONCURRENT_MANAGED"
	)
	t.Setenv(externalKey, "sibling-value")
	t.Setenv(managedKey, "") // cleanup reservation only
	testassert.NoError(t, os.Unsetenv(managedKey))
	managed := stubManagedEnv(t)
	jobsDir := t.TempDir()
	jobsInst := realJobs(t, jobsDir)
	byID := newStandardTools(jobsDir, jobsInst, newLiveToolOutput(newObservation()))
	ws := t.TempDir()
	tc := callToolContext(ws)
	tc.SubprocessEnv = testSubprocessEnv(managed)
	tc.Background = &scriptBackground{}

	// One synchronous managed write proves the writer path works before any
	// child spawns, so a failed or unscheduled writer cannot pass vacuously.
	testassert.NoError(t, managed.TrySet(managedKey, "managed-secret"))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWriter := func() { stopOnce.Do(func() { close(stop) }) }
	defer func() { stopWriter(); wg.Wait() }()
	writerErrs := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var err error
			if i%2 == 0 {
				err = managed.TrySet(managedKey, "managed-secret")
			} else {
				err = managed.TryRemove(managedKey)
			}
			if err != nil {
				select {
				case writerErrs <- err:
				default:
				}
				return
			}
		}
	}()

	checkOutput := func(kind, output string) {
		t.Helper()
		output = strings.TrimSpace(output)
		testassert.Require(t, strings.HasPrefix(output, "sibling-value|"), "%s child lost the external sibling: %q", kind, output)
		{
			rest := strings.TrimPrefix(output, "sibling-value|")
			testassert.Require(t, rest == "", "%s child saw a managed value %q, want none", kind, rest)
		}
	}
	command := `printf '%s|%s' "$MELTA_CONCURRENT_EXTERNAL" "$MELTA_CONCURRENT_MANAGED"`
	fgArgs, err := json.Marshal(map[string]any{"command": command})
	testassert.NoError(t, err)
	bgArgs, err := json.Marshal(map[string]any{"command": command + "; sleep 30", "background": true})
	testassert.NoError(t, err)

	for i := 0; i < 20; i++ {
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", string(fgArgs)))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "foreground child %d = %+v, want success", i, outcome.Result)
		checkOutput("foreground", outcome.Result.Content)
	}
	for i := 0; i < 5; i++ {
		outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", string(bgArgs)))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "background child %d = %+v, want the immediate success", i, outcome.Result)
		jobID := assertBackgroundImmediate(t, outcome.Result.Content)
		deadline := time.Now().Add(15 * time.Second)
		output := ""
		for time.Now().Before(deadline) {
			got, rerr := jobsInst.Read(testSessionID, jobID)
			if rerr == nil && strings.Contains(got, "sibling-value|") {
				output = got
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		testassert.Require(t, output != "", "the background child %d never produced its output", i)
		checkOutput("background", output)
		{
			rerr := jobsInst.Kill(testSessionID, jobID)
			testassert.Require(t, rerr == nil, "kill background child %d: %v", i, rerr)
		}
	}
	stopWriter()
	wg.Wait()
	select {
	case err := <-writerErrs:
		t.Fatalf("concurrent managed writer: %v", err)
	default:
	}
}

// TestBackgroundJobEnvironmentScrubsManagedKey proves the background job's
// environment carries the unlisted user key and never the managed key —
// connected through a real manager AFTER the call was prepared and before the
// actual child start, so the call-time producer, not any Prepare-time copy,
// does the scrubbing.
func TestBackgroundJobEnvironmentScrubsManagedKey(t *testing.T) {
	t.Setenv("MELTA_BG_UNLISTED", "early-value")
	t.Setenv("MELTA_BG_MANAGED", "managed-secret") // cleanup reservation only
	testassert.NoError(t, os.Unsetenv("MELTA_BG_MANAGED"))
	dataDir := t.TempDir()
	jobsInst := realJobs(t, dataDir)
	byID := newStandardTools(dataDir, jobsInst, newLiveToolOutput(newObservation()))
	managed := stubManagedEnv(t)
	ws := t.TempDir()
	tc := callToolContext(ws)
	tc.SubprocessEnv = testSubprocessEnv(managed)
	tc.Background = &scriptBackground{}
	// The environment changes before the child starts: the
	// external sibling changes and the key connects. The job must see the
	// later sibling and no managed key.
	t.Setenv("MELTA_BG_UNLISTED", "visible-value")
	testassert.NoError(t, managed.TrySet("MELTA_BG_MANAGED", "managed-secret"))
	outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"printf '%s|%s' \"$MELTA_BG_UNLISTED\" \"$MELTA_BG_MANAGED\"; sleep 30","background":true}`))
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "result = %+v, want the background start to succeed", outcome.Result)
	jobID := assertBackgroundImmediate(t, outcome.Result.Content)
	deadline := time.Now().Add(15 * time.Second)
	var output string
	for time.Now().Before(deadline) {
		got, err := jobsInst.Read(testSessionID, jobID)
		if err == nil && strings.Contains(got, "visible-value|") {
			output = got
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	testassert.Require(t, output != "", "the background job never produced its output")
	testassert.Require(t, !strings.Contains(output, "managed-secret"), "job output %q carries the managed key", output)
}

// completionTextIn returns the first completion text carried by a model
// request's conversation messages.
func completionTextIn(req model.Request) string {
	for _, msg := range req.Messages {
		for _, part := range msg.Content {
			if part.Kind == model.PartText && strings.Contains(part.Text, "Background process ") && strings.Contains(part.Text, " finished: ") {
				return part.Text
			}
		}
	}
	return ""
}

// TestBackgroundStartRealProcessDeliversTruncatedSteeringCompletion drives a
// background start against a real short-lived process through the real
// Harness bridge: the immediate result is the retained template, the
// completion is delivered as steering input into the running operation, and
// the composed completion text is truncated at the configured bound.
func TestBackgroundStartRealProcessDeliversTruncatedSteeringCompletion(t *testing.T) {
	delivered := make(chan struct{}, 1)
	bg := &harnessBackground{delivered: delivered}
	invocation, err := ConfiguredInvocationForTest("", `{"max_output_bytes":1024}`, "", nil)
	testassert.NoError(t, err)
	var mu sync.Mutex
	attempt := 0
	completion := ""
	modelFn := func(_ context.Context, req model.Request, _ model.Extra) (model.Stream, error) {
		if text := completionTextIn(req); text != "" {
			mu.Lock()
			completion = text
			mu.Unlock()
		}
		mu.Lock()
		attempt++
		n := attempt
		seen := completion != ""
		mu.Unlock()
		switch {
		case n == 1:
			return toolCallStream("call-bg", "run_command", `{"command":"printf '%1000d' 0","background":true}`), nil
		case seen:
			return stopStream("done"), nil
		case n == 2:
			select {
			case <-delivered:
			case <-time.After(15 * time.Second):
				return nil, errors.New("the background completion was never delivered")
			}
			return toolCallStream("call-followup", "run_command", `{"command":"true"}`), nil
		default:
			return nil, errors.New("the background completion never reached a model boundary")
		}
	}
	th := newToolsHarnessWith(t, modelFn, toolsHarnessOpts{
		realJobs:      true,
		advertise:     []string{"run_command"},
		background:    bg,
		invocation:    invocation,
		subprocessEnv: testSubprocessEnv(stubManagedEnv(t)),
	})
	session := th.createSession()
	th.submit(session, "op-1", "run the background command")
	rec := th.awaitSettled(session, "op-1")
	testassert.Require(t, rec.State.Status == harness.OperationSuccess, "operation settled %q, want success: %+v", rec.State.Status, rec.State.Terminal)

	results := th.readToolResults(session)
	immediate := results["call-bg"]
	testassert.Require(t, immediate.Status == model.ResultSuccess, "immediate result = %+v, want success", immediate)
	jobID := assertBackgroundImmediate(t, immediate.Content)

	mu.Lock()
	gotCompletion := completion
	mu.Unlock()
	deadline := time.Now().Add(15 * time.Second)
	for gotCompletion == "" && time.Now().Before(deadline) { // the completion's own Operation carries it after the handoff
		time.Sleep(time.Millisecond)
		mu.Lock()
		gotCompletion = completion
		mu.Unlock()
	}
	testassert.Require(t, gotCompletion != "", "the completion never reached a model boundary as steering input")
	if len(gotCompletion) > 1024 || !strings.HasSuffix(gotCompletion, "\n[truncated]") || !utf8.ValidString(gotCompletion) {
		t.Fatalf("completion length = %d, want the text capped at 1024 bytes with the trailing marker and a complete UTF-8 prefix", len(gotCompletion))
	}
	testassert.Require(t, strings.Contains(gotCompletion, "Background process "+jobID), "completion = %q, want it to name the started job %q", gotCompletion, jobID)

	entries, err := th.store.ReadEntries(context.Background(), session, 0)
	testassert.NoError(t, err)
	found := false
	for _, entry := range entries {
		if entry.Kind != harness.EntryInput || !strings.Contains(string(entry.Payload), "Background process ") {
			continue
		}
		var wire struct {
			Origin string `json:"origin"`
		}
		testassert.NoError(t, json.Unmarshal(entry.Payload, &wire))
		testassert.Equal(t, wire.Origin, "runtime")
		found = true
	}
	testassert.Require(t, found, "no runtime-origin input entry carries the delivered completion")
}

// reserveAndStart seeds one real background job on the composed capability.
func reserveAndStart(t *testing.T, jobsInst jobs.Jobs, workspace, command string) string {
	t.Helper()
	id, err := jobsInst.Reserve()
	testassert.NoError(t, err)
	testassert.NoError(t, jobsInst.Start(jobs.StartRequest{
		JobID:     id,
		SessionID: testSessionID,
		Workspace: workspace,
		Command:   command,
		Env:       os.Environ(),
	}))
	return id
}

// TestProcessToolReturnsRetainedActionResults reproduces every legacy
// process action's result text against a real jobs capability, including
// session ownership and the capability's unknown-ID text for
// well-shaped unknown and foreign identities.
func TestProcessToolReturnsRetainedActionResults(t *testing.T) {
	jobsDir := t.TempDir()
	jobsInst := realJobs(t, jobsDir)
	byID := newStandardTools(jobsDir, jobsInst, newLiveToolOutput(newObservation()))
	process := byID["process"]
	ws := t.TempDir()
	tc := callToolContext(ws)

	t.Run("empty list retains the empty text", func(t *testing.T) {
		outcome := process(context.Background(), tc, makeCall(t, "process", `{"action":"list"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == "No background processes.", "result = %+v, want the retained empty list text", outcome.Result)
	})

	runningID := reserveAndStart(t, jobsInst, ws, "sleep 30")
	outID := reserveAndStart(t, jobsInst, ws, "printf hello; sleep 30")

	t.Run("list carries the retained line format", func(t *testing.T) {
		outcome := process(context.Background(), tc, makeCall(t, "process", `{"action":"list"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, runningID) && strings.Contains(outcome.Result.Content, "sleep 30") && strings.Contains(outcome.Result.Content, "(running for"), "result = %+v, want success; list = %q, want the retained running line for the seeded job", outcome.Result, outcome.Result.Content)
	})

	t.Run("read of a silent running job retains the placeholder", func(t *testing.T) {
		outcome := process(context.Background(), tc, makeCall(t, "process", fmt.Sprintf(`{"action":"read","id":"%s"}`, runningID)))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == "(No output yet)", "result = %+v, want the retained empty-output placeholder", outcome.Result)
	})

	t.Run("read carries the job's captured output", func(t *testing.T) {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			got, err := jobsInst.Read(testSessionID, outID)
			if err == nil && strings.Contains(got, "hello") {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		outcome := process(context.Background(), tc, makeCall(t, "process", fmt.Sprintf(`{"action":"read","id":"%s"}`, outID)))
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.Contains(outcome.Result.Content, "hello"), "result = %+v, want the captured job output", outcome.Result)
	})

	t.Run("a foreign session id reaches the capability and returns the same text", func(t *testing.T) {
		tcForeign := callToolContext(ws)
		tcForeign.AdmittedEntry.SessionID = "ffffffffffffffffffffffffffffffff"
		outcome := process(context.Background(), tcForeign, makeCall(t, "process", fmt.Sprintf(`{"action":"kill","id":"%s"}`, runningID)))
		want := fmt.Sprintf("process: no process with ID %q", runningID)
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == want, "result = %+v, want %q", outcome.Result, want)
	})

	t.Run("kill retains the terminated text", func(t *testing.T) {
		outcome := process(context.Background(), tc, makeCall(t, "process", fmt.Sprintf(`{"action":"kill","id":"%s"}`, runningID)))
		want := fmt.Sprintf("Process %s terminated.", runningID)
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == want, "result = %+v, want %q", outcome.Result, want)
		outcome = process(context.Background(), tc, makeCall(t, "process", fmt.Sprintf(`{"action":"kill","id":"%s"}`, outID)))
		want = fmt.Sprintf("Process %s terminated.", outID)
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == want, "result = %+v, want %q", outcome.Result, want)
	})

	t.Run("a well-shaped unknown id reaches the capability and returns the retained text", func(t *testing.T) {
		outcome := process(context.Background(), tc, makeCall(t, "process", `{"action":"read","id":"deadbeef"}`))
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == `process: no process with ID "deadbeef"`, "result = %+v, want the retained unknown-ID text", outcome.Result)
	})
}

// TestProcessToolInvalidArgumentsReturnErrors pins the validation outcomes:
// missing ids, unknown actions, and ids outside the 8-lowercase-hex
// namespace return error results without calling the jobs capability.
func TestProcessToolInvalidArgumentsReturnErrors(t *testing.T) {
	fake := &fakeJobs{}
	byID, _ := openToolsComposed(t, t.TempDir(), fake, nil)
	process := byID["process"]
	ws := t.TempDir()
	cases := []struct {
		args string
		want string
	}{
		{`{"action":"read"}`, "process: id is required for read"},
		{`{"action":"kill"}`, "process: id is required for kill"},
		{`{"action":"drop","id":"deadbeef"}`, `process: unknown action "drop"`},
		{`{}`, `process: unknown action ""`},
		{`{"action":"read","id":"xyz"}`, `process: no process with ID "xyz"`},
		{`{"action":"kill","id":"DEADBEEF"}`, `process: no process with ID "DEADBEEF"`},
		{`{"action":"read","id":"deadbee"}`, `process: no process with ID "deadbee"`},
		{`{"action":"read","id":"deadbeef0"}`, `process: no process with ID "deadbeef0"`},
		{`{"action":"kill","id":"deadbeeg"}`, `process: no process with ID "deadbeeg"`},
	}
	for _, tc := range cases {
		outcome := process(context.Background(), callToolContext(ws), makeCall(t, "process", tc.args))
		testassert.Require(t, outcome.Result.Status == model.ResultError && outcome.Result.Content == tc.want, "%s: result = %+v, want error %q", tc.args, outcome.Result, tc.want)
	}
	testassert.Require(t, fake.reserves == 0 && fake.reads == 0 && fake.kills == 0 && fake.lists == 0, "capability calls = reserves %d reads %d kills %d lists %d, want none", fake.reserves, fake.reads, fake.kills, fake.lists)
}

// TestComposedCheckDenialSettlesOnlyTheCall refuses the background command and
// process call without reaching jobs, while a later allowed write still runs.
func TestComposedCheckDenialSettlesOnlyTheCall(t *testing.T) {
	fake := &fakeJobs{}
	hook := harness.ToolCheckHook{ID: "restrict", ExtensionID: "test", Run: func(_ context.Context, call model.ToolCall) (*string, error) {
		if call.Name == "run_command" || call.Name == "process" {
			reason := "restricted"
			return &reason, nil
		}
		return nil, nil
	}}
	attempt := 0
	var mu sync.Mutex
	modelFn := func(_ context.Context, _ model.Request, _ model.Extra) (model.Stream, error) {
		mu.Lock()
		attempt++
		n := attempt
		mu.Unlock()
		switch n {
		case 1:
			return toolCallStream("call-bg", "run_command", `{"command":"true","background":true}`), nil
		case 2:
			return toolCallStream("call-list", "process", `{"action":"list"}`), nil
		case 3:
			return toolCallStream("call-write", "write_file", `{"path":"out.txt","content":"written"}`), nil
		default:
			return stopStream("done"), nil
		}
	}
	th := newToolsHarnessWith(t, modelFn, toolsHarnessOpts{
		jobsValue:  fake,
		advertise:  []string{"run_command", "process", "write_file"},
		checkHooks: []harness.ToolCheckHook{hook},
	})
	session := th.createSession()
	th.submit(session, "op-1", "exercise the denied calls")
	rec := th.awaitSettled(session, "op-1")
	testassert.Require(t, rec.State.Status == harness.OperationSuccess, "operation settled %q, want success: %+v", rec.State.Status, rec.State.Terminal)
	results := th.readToolResults(session)
	testassert.Equal(t, len(results), 3)
	for _, id := range []string{"call-bg", "call-list"} {
		got := results[id]
		testassert.Require(t, got.Status == model.ResultDenied && got.Content == "restricted", "%s = %+v, want the hook's denial", id, got)
	}
	write := results["call-write"]
	testassert.Require(t, write.Status == model.ResultSuccess && strings.Contains(write.Content, "Wrote"), "call-write = %+v, want the later allowed call to run", write)
	{
		data, err := os.ReadFile(filepath.Join(th.workspace, "out.txt"))
		testassert.Require(t, err == nil && string(data) == "written", "written file = (%q, %v), want the later write on disk", data, err)
	}
	if fake.reserves != 0 || len(fake.started) != 0 || fake.lists != 0 {
		t.Fatalf("capability calls = reserves %d started %d lists %d, want none behind the denials", fake.reserves, len(fake.started), fake.lists)
	}
}

// TestRunCommandNilSubprocessEnvNoStart proves the foreground command
// refuses when the call-time environment producer is missing or reports no
// environment: the ordinary error outcome, and no process started.
func TestRunCommandNilSubprocessEnvNoStart(t *testing.T) {
	byID := openTools(t, t.TempDir())
	ws := t.TempDir()
	for _, testCase := range []struct {
		name     string
		producer func(context.Context) ([]string, error)
		want     string
	}{
		{name: "nil callback", producer: nil, want: "no subprocess environment producer"},
		{name: "nil snapshot", producer: func(context.Context) ([]string, error) { return nil, nil }, want: "returned no environment"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			marker := filepath.Join(ws, strings.ReplaceAll(testCase.name, " ", "-")+"-marker")
			tc := callToolContext(ws)
			tc.SubprocessEnv = testCase.producer
			outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"touch `+marker+`"}`))
			testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, testCase.want), "result = %+v, want the %q refusal", outcome.Result, testCase.want)
			{
				_, err := os.Stat(marker)
				testassert.Require(t, os.IsNotExist(err), "the command started without an environment producer (%v)", err)
			}
		})
	}
}

// TestBackgroundNilSubprocessEnvNoReservation proves the background start
// refuses before the Jobs reservation when the producer is missing: no
// reservation, no member, no start.
func TestBackgroundNilSubprocessEnvNoReservation(t *testing.T) {
	ws := t.TempDir()
	fake := &fakeJobs{}
	byID, _ := openToolsComposed(t, t.TempDir(), fake, nil)
	tc := callToolContext(ws)
	tc.Background = &scriptBackground{}
	outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"true","background":true}`))
	testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, "no subprocess environment producer"), "result = %+v, want the missing-producer error", outcome.Result)
	if fake.reserves != 0 || len(fake.started) != 0 {
		t.Fatalf("reserves = %d, started = %d, want no reservation and no start", fake.reserves, len(fake.started))
	}
}

// TestRunCommandUsesCurrentSubprocessEnvironment checks that a key connected
// before the call is scrubbed and the current unrelated value is visible.
func TestRunCommandUsesCurrentSubprocessEnvironment(t *testing.T) {
	managed := stubManagedEnv(t)
	key := "MELTA_SCRUB_LATE"
	external := "MELTA_TIMING_EXTERNAL"
	t.Setenv(key, "late-secret") // cleanup reservation only
	testassert.NoError(t, os.Unsetenv(key))
	t.Setenv(external, "early-value")
	runCommand := openTools(t, t.TempDir())["run_command"]
	ws := t.TempDir()
	tc := callToolContext(ws)
	tc.SubprocessEnv = testSubprocessEnv(managed)
	// Before the call starts, the external sibling changes and the
	// key connects. An earlier capture would freeze the sibling and
	// miss the late managed key; the start-time resolution sees the later
	// sibling and scrubs the key.
	t.Setenv(external, "later-value")
	testassert.NoError(t, managed.TrySet(key, "late-secret"))
	outcome := runCommand(context.Background(), tc, makeCall(t, "run_command", `{"command":"printf '%s|%s' \"$MELTA_TIMING_EXTERNAL\" \"$MELTA_SCRUB_LATE\""}`))
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess && strings.TrimSpace(outcome.Result.Content) == "later-value|", "result = %+v, want success; result = %q, want the updated sibling and the scrubbed key", outcome.Result, outcome.Result.Content)
}

// TestBackgroundNilEnvSnapshotAbortsBeforeStart proves the forbidden-sibling
// row of the background handoff: a producer reporting no environment fails
// the start with no process and no leaked reservation or member.
func TestBackgroundNilEnvSnapshotAbortsBeforeStart(t *testing.T) {
	ws := t.TempDir()
	fake := &fakeJobs{}
	byID, _ := openToolsComposed(t, t.TempDir(), fake, nil)
	tc := callToolContext(ws)
	tc.SubprocessEnv = func(context.Context) ([]string, error) { return nil, nil } // a nil snapshot is never an ambient fallback
	tc.Background = &scriptBackground{}
	outcome := byID["run_command"](context.Background(), tc, makeCall(t, "run_command", `{"command":"true","background":true}`))
	testassert.Require(t, outcome.Result.Status == model.ResultError && strings.Contains(outcome.Result.Content, "returned no environment"), "result = %+v, want the nil-snapshot refusal", outcome.Result)
	testassert.Require(t, len(fake.started) == 0 && len(fake.aborted) == 1, "got %v, want %v; aborted = %v, want the reservation aborted once", len(fake.started), 0, fake.aborted)
}
