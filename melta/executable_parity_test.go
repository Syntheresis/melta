package melta

// Native/worker transport-boundary parity: one author fixture's manifest,
// registrations, and handler semantics run directly and through the
// in-process pipe adapter. Every case compares the semantic values both
// transports produce at the owner boundary; the filesystem, lifecycle, and
// client cases belong to their owners and are not reproduced in both forms.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// parityObservation collects the transport-independent semantic values one
// parity run produced. Keys are stable descriptions, values are digests,
// statuses, or counts, never transport mechanics.
type parityObservation struct {
	values map[string]string
}

func newParityObservation() parityObservation {
	return parityObservation{values: map[string]string{}}
}

func (o parityObservation) set(key, value string) {
	o.values[key] = value
}

func hashHex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestToolReturnTransportParity(t *testing.T) {
	jsonString := func(n int) json.RawMessage {
		return json.RawMessage(`"` + strings.Repeat("m", n-2) + `"`)
	}
	const marker = "\n[truncated]"
	for _, workerHost := range []bool{false, true} {
		for _, row := range []struct {
			name         string
			cancel       bool
			err          error
			status       model.ToolResultStatus
			evidence     harness.ExecutionEvidence
			callID       string
			content      string
			metadata     json.RawMessage
			wantContent  string
			wantMetadata json.RawMessage
		}{
			{name: "success", status: model.ResultSuccess, evidence: harness.ExecutionObserved},
			{name: "error", err: errors.New("failed"), status: model.ResultError, evidence: harness.ExecutionObserved},
			{name: "unknown", err: extension.ErrOutcomeUnknown, status: model.ResultError, evidence: harness.ExecutionUnknown},
			{name: "cancel unknown", cancel: true, err: fmt.Errorf("%w: %w", extension.ErrOutcomeUnknown, context.Canceled), status: model.ResultInterrupted, evidence: harness.ExecutionUnknown},
			{name: "cancel", cancel: true, err: context.Canceled, status: model.ResultInterrupted, evidence: harness.ExecutionObserved},
			{name: "late success", cancel: true, status: model.ResultSuccess, evidence: harness.ExecutionObserved},
			{name: "late unrelated error", cancel: true, err: errors.New("failed"), status: model.ResultError, evidence: harness.ExecutionObserved},
			{name: "live cancellation error", err: context.Canceled, status: model.ResultError, evidence: harness.ExecutionObserved},
			{
				name: "content over the ceiling", status: model.ResultSuccess, evidence: harness.ExecutionObserved,
				content: strings.Repeat("<", 600000), metadata: jsonString(1048576),
				wantContent: strings.Repeat("<", 458740) + marker, wantMetadata: jsonString(1048576),
			},
			{
				name: "largest response", status: model.ResultSuccess, evidence: harness.ExecutionObserved,
				callID: strings.Repeat("a", 43000), content: strings.Repeat("<", 458752), metadata: jsonString(1048576),
				wantContent: strings.Repeat("<", 458752), wantMetadata: jsonString(1048576),
			},
			{
				name: "metadata over the bound", status: model.ResultSuccess, evidence: harness.ExecutionObserved,
				content: "ok", metadata: jsonString(4194304),
				wantContent: "ok",
			},
		} {
			name := "native"
			if workerHost {
				name = "worker"
			}
			t.Run(name+"/"+row.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				content := row.content
				if content == "" {
					content = "ok"
				}
				callID := row.callID
				if callID == "" {
					callID = "call"
				}
				manifest := registrarManifest("tool-parity")
				author := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
					_, err := api.RegisterTool(testDescribeTool("tool"), func(callCtx context.Context, _ extension.ToolCall) (extension.ToolResult, error) {
						if row.cancel {
							cancel()
							<-callCtx.Done()
						}
						return extension.ToolResult{Content: content, Metadata: row.metadata}, row.err
					}, nil)
					return nil, err
				}}
				if workerHost {
					workerAuthor := author
					spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) { return workerAuthor, nil }}
					spawner.install()
					author = executableAuthorExtension(manifest, "tool-parity", adapterEnvironment)
				}
				owner := &configurationService{}
				candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: author, section: json.RawMessage(`{}`)}})
				testassert.NoError(t, err)
				defer candidate.release()
				stage := candidate.snapshotForTest()
				defer stage.release()
				binding := stage.bindTool("tool")
				testassert.Require(t, binding != nil, "tool not registered")
				defer binding.release()
				outcome := binding.entry.value.(nativeTool).tool(ctx, extension.AuthorContext{}, model.ToolCall{ID: callID, Name: "tool", Arguments: json.RawMessage(`{}`)})
				if outcome.Result.Status != row.status || outcome.Evidence != row.evidence {
					t.Fatalf("outcome = %+v, want %s/%s", outcome, row.status, row.evidence)
				}
				if row.wantContent != "" {
					testassert.Require(t, outcome.Result.Content == row.wantContent && bytes.Equal(outcome.Metadata, row.wantMetadata) && outcome.Result.CallID == callID,
						"outcome carries %d content bytes, %d metadata bytes and call id %q, want %d content bytes, %d metadata bytes and call id %q",
						len(outcome.Result.Content), len(outcome.Metadata), outcome.Result.CallID, len(row.wantContent), len(row.wantMetadata), callID)
				}
			})
		}
	}
}

// eachAuthorTransport runs one case through the in-process native fixture and
// the pipe adapter over the same candidate fixture, then compares the two
// semantic observations.
func eachAuthorTransport(t *testing.T, run func(t *testing.T, workerHost bool) parityObservation) {
	t.Helper()
	observed := make([]parityObservation, 2)
	complete := true
	for index, workerHost := range []bool{false, true} {
		name := "native"
		if workerHost {
			name = "worker"
		}
		index, workerHost := index, workerHost
		if !t.Run(name, func(t *testing.T) {
			if workerHost {
				spawner := &adapterSpawner{t: t, factory: func() (extension.Extension, func()) {
					return nativeAuthorFixture(), nil
				}}
				spawner.install()
			}
			observed[index] = run(t, workerHost)
		}) {
			complete = false
		}
	}
	if !complete {
		return
	}
	if !reflect.DeepEqual(observed[0].values, observed[1].values) {
		t.Fatalf("worker semantic observation differs across transports:\nnative: %v\nworker: %v", observed[0].values, observed[1].values)
	}
}

// parityFixtureTool is the native counterpart of the worker fixture's
// registration-replacing fixture tool, kept semantically identical to
// extension/worker/testdata/author's fixtureTool.
type parityFixtureTool struct {
	api    extension.API
	handle *extension.Registration
	fresh  bool
	remove bool
}

func (t *parityFixtureTool) run(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
	if !t.fresh {
		if _, err := call.Call(ctx, "gate", "park", json.RawMessage(`{}`)); err != nil {
			return extension.ToolResult{}, err
		}
		if t.remove {
			if err := t.api.Remove(*t.handle); err != nil {
				return extension.ToolResult{}, err
			}
			return extension.ToolResult{Content: "old handler"}, nil
		}
		if _, err := t.api.RegisterTool(model.ToolDefinition{Name: "read_file", Parameters: json.RawMessage(`{}`)}, (&parityFixtureTool{fresh: true}).run, *t.handle); err == nil {
			return extension.ToolResult{}, errors.New("reserved replacement accepted")
		}
		replacement, err := t.api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, (&parityFixtureTool{fresh: true}).run, *t.handle)
		if err != nil {
			return extension.ToolResult{}, err
		}
		*t.handle = replacement
		return extension.ToolResult{Content: "old handler"}, nil
	}
	return extension.ToolResult{}, errors.New("incorrect handler")
}

// nativeAuthorFixture is the in-process counterpart of the worker fixture's
// default entry: same manifest, same registrations, same handler semantics.
// Its "crash" method is the worker fixture's process exit; the parity script
// never dispatches it.
func nativeAuthorFixture() extension.Extension {
	return extension.Extension{
		Manifest: authorFixtureManifest(),
		Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
			if string(api.Settings()) == `{"fail":true}` {
				return nil, errors.New("activation refused")
			}
			result, err := api.Call(ctx, "host-service", "echo", json.RawMessage(`{"entry":true}`))
			if err != nil {
				return nil, err
			}
			if string(result.Value) != `{"entry":true}` {
				return nil, errors.New("entry service returned unexpected value")
			}
			_, err = api.RegisterService(extension.Service{Name: "worker-service", Methods: []extension.RegisteredMethod{
				{
					Method: extension.Method{Name: "relay", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
					Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
						return call.Call(ctx, "host-service", "echo", args)
					},
				},
				{
					Method: extension.Method{Name: "crash", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
					Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
						return extension.Result{}, errors.New("native crash stand-in")
					},
				},
			}}, nil)
			if err != nil {
				return nil, err
			}
			var handle extension.Registration
			tool := &parityFixtureTool{api: api, handle: &handle}
			handle, err = api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, tool.run, nil)
			if err != nil {
				return nil, err
			}
			var removable extension.Registration
			remover := &parityFixtureTool{api: api, handle: &removable, remove: true}
			removable, err = api.RegisterTool(model.ToolDefinition{Name: "removable-tool", Parameters: json.RawMessage(`{}`)}, remover.run, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterStrategy(harness.StrategyBinding{ID: "worker-strategy", Strategy: parityStrategy{}}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachArguments(func(_ context.Context, _ extension.AuthorContext, call model.ToolCall) (extension.ArgumentsResult, error) {
				return extension.ArgumentsResult{Arguments: append(json.RawMessage(nil), call.Arguments...)}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachRequest(func(_ context.Context, _ extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
				prompt := "worker prompt"
				return harness.RequestConsequence{Prompt: &prompt}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachResult(func(_ context.Context, _ extension.AuthorContext, input harness.ToolOutputInput) (string, error) {
				return "worker: " + input.Content, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachToolCheck(func(_ context.Context, _ extension.AuthorContext, input model.ToolCall) (*string, error) {
				if string(input.Arguments) != string([]byte{0xff, '{'}) {
					return nil, errors.New("check arguments changed")
				}
				refusal := "denied by worker"
				return &refusal, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterOperation(extension.Operation{
				Target: extension.ExposedTargetHarness,
				Method: extension.Method{Name: "operation", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
				Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return extension.Result{Value: args}, nil
				},
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachAdmission(func(_ context.Context, _ extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
				if input.Candidate.Instructions == "skip" {
					value := "unread"
					return extension.AdmissionResult{Refusal: &value}, nil
				}
				prompt, err := input.Prompt.Default()
				if err != nil {
					return extension.AdmissionResult{}, err
				}
				return extension.AdmissionResult{Refusal: &prompt}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterCleanup(func(_ context.Context, _ extension.AuthorContext, session harness.SessionIdentity) error {
				if session.SessionID != "session" {
					return errors.New("unexpected session")
				}
				return nil
			}, nil)
			return nil, err
		},
	}
}

// parityStrategy is the native counterpart of the worker fixture's PlanNone strategy.
type parityStrategy struct{}

func (parityStrategy) Plan(harness.PlanInput) (harness.PlanOutput, error) {
	return harness.PlanOutput{Kind: harness.PlanNone}, nil
}

func (parityStrategy) Consume(harness.ConsumeInput) (harness.PlanOutput, error) {
	return harness.PlanOutput{Kind: harness.PlanNone}, nil
}

// TestAuthorTransportBoundaryParity runs the author fixture's registered
// service values, tool handler chain, strategy, hooks, operation, activation
// hostcall and activation failure through both transports and compares the
// semantic observations.
func TestAuthorTransportBoundaryParity(t *testing.T) {
	corpus := []string{
		"{\n  \"a\": 1,\n  \"nested\": {\"b\": [true, null]}\n}",
		"[1, 2, 3, \"four\", {\"five\": 5}]",
		"{\"big\":9007199254740993,\"precise\":1.0000000000000002}",
		"{\"text\":\"line\\nbreak\\ttab <>& \\u2028 \\u00e9 \\\"quoted\\\"\"}",
	}
	eachAuthorTransport(t, func(t *testing.T, workerHost bool) parityObservation {
		manifest := authorFixtureManifest()
		guest := nativeAuthorFixture()
		if workerHost {
			guest = executableAuthorExtension(manifest, "author-worker", adapterEnvironment)
		}
		owner := &configurationService{}
		ctx := context.Background()
		build := func(section string) *nativeCandidate {
			t.Helper()
			candidate, err := owner.prepareNativeCandidate(ctx, []nativeActivationSpec{
				{extension: shHostServicePlugin(), section: json.RawMessage(`{}`)},
				{extension: guest, section: json.RawMessage(section), installed: workerHost},
			})
			testassert.NoError(t, err)
			return candidate
		}
		observation := newParityObservation()

		// An activation that refuses its section stays inactive on both
		// transports, and the rest of the candidate publishes.
		failed := build(`{"fail":true}`)
		testassert.Require(t, failed.inactive[manifest.ID] != nil, "refused activation was not inactive: %v", failed.inactive)
		failed.release()

		candidate := build(`{}`)
		defer candidate.release()
		testassert.Require(t, len(candidate.inactive) == 0, "activation inactive: %v", candidate.inactive)
		testassert.NoError(t, candidate.publishForTest())
		observation.set("activation", "ok")

		callScoped := func(service, method string, args json.RawMessage) (extension.Result, error) {
			return candidate.callServiceScoped(ctx, service, method, args, &nativeCallScope{})
		}
		for index, payload := range corpus {
			result, err := callScoped("worker-service", "relay", json.RawMessage(payload))
			testassert.NoError(t, err)
			testassert.Require(t, string(result.Value) == payload, "service relay %d = %q, want the exact arguments %q", index, result.Value, payload)
			observation.set(fmt.Sprintf("relay-%d", index), hashHex(result.Value))
		}
		_, err := callScoped("worker-service", "crash", json.RawMessage(`{}`))
		testassert.Require(t, err != nil, "crash service = %v, want its handler's error", err)
		observation.set("crash", string(ErrorCode(err)))

		stage := candidate.snapshotForTest()
		defer stage.release()
		binding := stage.bindTool("worker-tool")
		if binding == nil {
			t.Fatal("worker-tool is not registered")
		}
		defer binding.release()
		tool := binding.entry.value.(nativeTool).tool
		gate := func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}
		outcome := tool(ctx, extension.AuthorContext{Call: gate}, model.ToolCall{ID: "call", Name: "worker-tool", Arguments: json.RawMessage(`{}`)})
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess && outcome.Result.Content == "old handler" && outcome.Evidence == harness.ExecutionObserved, "execute = %+v, want the old handler's observed success", outcome)
		observation.set("execute", outcome.Result.Content+":"+string(outcome.Evidence))

		strategy := stage.strategies["worker-strategy"]
		if strategy == nil {
			t.Fatal("worker-strategy is not registered")
		}
		strategyImpl := strategy.value.(harness.StrategyBinding).Strategy
		planned, err := strategyImpl.Plan(harness.PlanInput{Trigger: "manual"})
		testassert.NoError(t, err)
		consumed, err := strategyImpl.Consume(harness.ConsumeInput{Input: harness.PlanInput{Trigger: "manual"}, Output: model.Output{Source: model.ModelRef{Provider: "p", Model: "m"}}})
		testassert.NoError(t, err)
		observation.set("strategy", string(planned.Kind)+":"+string(consumed.Kind))

		argumentsHook := stage.hooks["arguments"][0].value.(nativeHook).handler.(extension.ArgumentsHook)
		repaired, err := argumentsHook(ctx, extension.AuthorContext{}, model.ToolCall{ID: "bad", Name: "worker-tool", Arguments: json.RawMessage{0xff, '{'}})
		testassert.NoError(t, err)
		observation.set("hook-arguments", hashHex(repaired.Arguments))
		requestHook := stage.hooks["request"][0].value.(nativeHook).handler.(extension.AuthorRequestHook)
		consequence, err := requestHook(ctx, extension.AuthorContext{}, extension.AuthorRequestInput{RequestHookInput: harness.RequestHookInput{Model: model.ModelRef{Provider: "p", Model: "m"}}})
		testassert.NoError(t, err)
		testassert.Require(t, consequence.Prompt != nil && *consequence.Prompt == "worker prompt", "request hook = %+v", consequence)
		observation.set("hook-request", *consequence.Prompt)
		resultHook := stage.hooks["result"][0].value.(nativeHook).handler.(extension.ResultHook)
		content, err := resultHook(ctx, extension.AuthorContext{}, harness.ToolOutputInput{Content: "output"})
		testassert.NoError(t, err)
		observation.set("hook-result", content)
		checkHook := stage.hooks["check"][0].value.(nativeHook).handler.(extension.ToolCheckHook)
		refusal, err := checkHook(ctx, extension.AuthorContext{}, model.ToolCall{Arguments: json.RawMessage{0xff, '{'}})
		testassert.NoError(t, err)
		testassert.Require(t, refusal != nil && *refusal == "denied by worker", "check = %v", refusal)
		kept, err := checkHook(ctx, extension.AuthorContext{}, model.ToolCall{Arguments: json.RawMessage(`{}`)})
		testassert.Require(t, err != nil && kept == nil, "check with changed arguments = (%v, %v), want the refusal error", kept, err)
		observation.set("hook-check", *refusal+":refused")

		admissionHook := stage.hooks["admission"][0].value.(nativeHook).handler.(extension.AdmissionHook)
		promptReads := 0
		admissionResult, err := admissionHook(ctx, extension.AuthorContext{}, extension.AdmissionInput{Candidate: extension.AdmissionCandidate{Model: model.ModelRef{Provider: "p", Model: "m"}}, Prompt: extension.PromptAccess{Default: func() (string, error) {
			promptReads++
			return "lazy prompt", nil
		}}})
		testassert.NoError(t, err)
		testassert.Require(t, admissionResult.Refusal != nil && *admissionResult.Refusal == "lazy prompt" && promptReads == 1, "admission = %+v, reads %d", admissionResult, promptReads)
		promptReads = 0
		admissionResult, err = admissionHook(ctx, extension.AuthorContext{}, extension.AdmissionInput{Candidate: extension.AdmissionCandidate{
			Model: model.ModelRef{Provider: "p", Model: "m"}, Instructions: "skip",
		}, Prompt: extension.PromptAccess{Default: func() (string, error) {
			promptReads++
			return "unexpected", nil
		}}})
		testassert.NoError(t, err)
		testassert.Require(t, admissionResult.Refusal != nil && *admissionResult.Refusal == "unread" && promptReads == 0, "unread prompt = %+v, reads %d", admissionResult, promptReads)
		observation.set("hook-admission", "lazy:1,unread:0")

		cleanupHook := stage.hooks["cleanup"][0].value.(extension.CleanupHook)
		testassert.NoError(t, cleanupHook(ctx, extension.AuthorContext{ExtensionID: manifest.ID}, harness.SessionIdentity{SessionID: "session"}))
		testassert.Require(t, cleanupHook(ctx, extension.AuthorContext{ExtensionID: manifest.ID}, harness.SessionIdentity{SessionID: "other"}) != nil, "unexpected session was accepted")
		observation.set("hook-cleanup", "ok:refused")

		operation := stage.operations[nativeOperationKey{manifest.ID, "operation"}]
		if operation == nil {
			t.Fatal("operation is not registered")
		}
		operationMethod := operation.value.(struct {
			target extension.ExposedTargetKind
			method nativeServiceMethod
		}).method
		operationResult, err := operationMethod.handler(ctx, extension.AuthorContext{}, json.RawMessage(`{"number":9007199254740993}`))
		testassert.NoError(t, err)
		observation.set("operation", hashHex(operationResult.Value))
		return observation
	})
}
