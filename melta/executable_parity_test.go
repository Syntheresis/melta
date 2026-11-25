package melta

// Native/worker transport-boundary parity: one author fixture's manifest,
// registrations, and handler semantics run directly and through the
// in-process pipe adapter. Every case compares the semantic values both
// transports produce at the owner boundary; the filesystem, lifecycle, and
// client cases belong to their owners and are not reproduced in both forms.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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

func (t *parityFixtureTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *parityFixtureTool) Prepare(ctx context.Context, call extension.AuthorContext, _ model.ToolCall) extension.ToolPlan {
	if !t.fresh {
		if _, err := call.Call(ctx, "gate", "park", json.RawMessage(`{}`)); err != nil {
			return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}}
		}
		if t.remove {
			if err := t.api.Remove(*t.handle); err != nil {
				return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}}
			}
			return extension.ToolPlan{Data: []byte("old-plan"), Start: &extension.SessionStart{Target: "raw/name", InitialState: []byte(`{"large":9007199254740993}`), Report: true}}
		}
		if _, err := t.api.RegisterTool(model.ToolDefinition{Name: "read_file", Parameters: json.RawMessage(`{}`)}, &parityFixtureTool{fresh: true}, *t.handle); err == nil {
			return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: "reserved replacement accepted"}}}
		}
		replacement, err := t.api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, &parityFixtureTool{fresh: true}, *t.handle)
		if err != nil {
			return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: err.Error()}}}
		}
		*t.handle = replacement
	}
	return extension.ToolPlan{Data: []byte("old-plan"), Start: &extension.SessionStart{Target: "raw/name", InitialState: []byte(`{"large":9007199254740993}`), Report: true}}
}

func (t *parityFixtureTool) Execute(_ context.Context, _ extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	if t.fresh || string(plan.Data) != "old-plan" || plan.Start == nil || plan.Start.Target != "raw/name" || string(plan.Start.InitialState) != `{"large":9007199254740993}` || !plan.Start.Report {
		return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultError, Content: "incorrect handler or plan"}}
	}
	return harness.ToolOutcome{Result: model.ToolResult{Status: model.ResultSuccess, Content: "old handler"}, Evidence: harness.ExecutionObserved}
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
			handle, err = api.RegisterTool(model.ToolDefinition{Name: "worker-tool", Parameters: json.RawMessage(`{}`)}, tool, nil)
			if err != nil {
				return nil, err
			}
			var removable extension.Registration
			remover := &parityFixtureTool{api: api, handle: &removable, remove: true}
			removable, err = api.RegisterTool(model.ToolDefinition{Name: "removable-tool", Parameters: json.RawMessage(`{}`)}, remover, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.RegisterStrategy(harness.StrategyBinding{ID: "worker-strategy", Strategy: parityStrategy{}}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachArguments(extension.HookModePure, func(_ context.Context, _ extension.AuthorContext, call model.ToolCall) (extension.ArgumentsResult, error) {
				return extension.ArgumentsResult{Arguments: append(json.RawMessage(nil), call.Arguments...)}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachRequest(extension.HookModePure, func(_ context.Context, _ extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
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
			_, err = api.AttachAuthorization(func(_ context.Context, _ extension.AuthorContext, input extension.AuthorizationInput) (*string, error) {
				if string(input.Arguments) != string([]byte{0xff, '{'}) {
					return nil, errors.New("authorization arguments changed")
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
		for index, payload := range corpus {
			normalized, err := tool.Normalize(extension.AuthorContext{}, model.ToolCall{ID: fmt.Sprintf("call-%d", index), Name: "worker-tool", Arguments: json.RawMessage(payload)})
			testassert.NoError(t, err)
			testassert.Require(t, string(normalized) == payload, "normalized arguments %d = %q, want the exact call arguments", index, normalized)
			observation.set(fmt.Sprintf("normalize-%d", index), hashHex(normalized))
		}
		gate := func(context.Context, string, string, json.RawMessage) (extension.Result, error) {
			return extension.Result{Value: json.RawMessage(`{}`)}, nil
		}
		plan := tool.Prepare(ctx, extension.AuthorContext{Call: gate}, model.ToolCall{ID: "call", Name: "worker-tool", Arguments: json.RawMessage(`{}`)})
		if plan.Immediate != nil {
			t.Fatalf("prepare: %+v", plan.Immediate)
		}
		start, err := json.Marshal(plan.Start)
		testassert.NoError(t, err)
		observation.set("plan", hashHex(plan.Data)+":"+hashHex(start))
		outcome := tool.Execute(ctx, extension.AuthorContext{}, plan)
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
		authorizationHook := stage.hooks["authorization"][0].value.(nativeHook).handler.(extension.AuthorizationHook)
		refusal, err := authorizationHook(ctx, extension.AuthorContext{}, extension.AuthorizationInput{Arguments: json.RawMessage{0xff, '{'}})
		testassert.NoError(t, err)
		testassert.Require(t, refusal != nil && *refusal == "denied by worker", "authorization = %v", refusal)
		kept, err := authorizationHook(ctx, extension.AuthorContext{}, extension.AuthorizationInput{Arguments: json.RawMessage(`{}`)})
		testassert.Require(t, err != nil && kept == nil, "authorization with changed arguments = (%v, %v), want the refusal error", kept, err)
		observation.set("hook-authorization", *refusal+":refused")

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
