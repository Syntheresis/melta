package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestStrategyAuthoredSummaryRequestShapedOnce proves the maintenance request
// boundary: the strategy's frozen request is the one shaped request, its own
// leading system prompt is exposed to the hooks, the catalog and advertisement
// are empty, and an accepted no-op preserves every authored message — extras,
// an empty additional system message, and a request with no leading system
// message alike. The configured conversation prompt never enters this request.
func TestStrategyAuthoredSummaryRequestShapedOnce(t *testing.T) {
	t.Run("leading system prompt and authored messages are preserved", func(t *testing.T) {
		authored := []model.Message{
			{
				Role: model.RoleSystem,
				Content: []model.ContentPart{{Kind: model.PartText, Text: "authored system", Extra: model.Extra{
					"part": json.RawMessage(`{"keep":true}`),
				}}},
				Extra: model.Extra{"sys": json.RawMessage(`"keep"`)},
			},
			{Role: model.RoleSystem, Content: []model.ContentPart{}},
			{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "body"}}, Extra: model.Extra{"u": json.RawMessage(`1`)}},
		}
		runAuthoredSummaryRound(t, authored, false, func(t *testing.T, sent model.Request, fact maintenanceFact) {
			requireAuthoredMessages(t, sent.Messages, authored)
			requireAuthoredMessages(t, fact.Input, authored)
		}, func(t *testing.T, in RequestHookInput) {
			testassert.Require(t, in.Purpose == InvocationPurposeMaintenance, "hook purpose = %q, want maintenance", in.Purpose)
			testassert.Require(t, in.Model == testCompactCapture().Model, "hook model = %+v, want the actual compact model", in.Model)
			testassert.Require(t, in.Prompt == "authored system" && in.CapturedPrompt == "authored system",
				"hook prompts = %q/%q, want the request's own leading system prompt", in.CapturedPrompt, in.Prompt)
			testassert.Require(t, len(in.Catalog) == 0 && len(in.Tools) == 0, "hook catalog/tools = %d/%d, want an empty tool-free view", len(in.Catalog), len(in.Tools))
			testassert.Require(t, len(in.CanonicalMessages) == 2 && in.CanonicalMessages[0].Role == model.RoleSystem && in.CanonicalMessages[0].TextContent() == "" && in.CanonicalMessages[1].TextContent() == "body",
				"hook canonical view = %+v, want the authored messages without the leading prompt", in.CanonicalMessages)
		})
	})

	t.Run("a request without a leading system message exposes no prompt", func(t *testing.T) {
		authored := []model.Message{
			{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "only body"}}},
			{Role: model.RoleSystem, Content: []model.ContentPart{{Kind: model.PartText, Text: "late system"}}},
		}
		runAuthoredSummaryRound(t, authored, false, func(t *testing.T, sent model.Request, fact maintenanceFact) {
			requireAuthoredMessages(t, sent.Messages, authored)
			requireAuthoredMessages(t, fact.Input, authored)
		}, func(t *testing.T, in RequestHookInput) {
			testassert.Require(t, in.Prompt == "" && in.CapturedPrompt == "", "hook prompts = %q/%q, want no leading prompt", in.CapturedPrompt, in.Prompt)
		})
	})

	t.Run("an echoed prompt keeps the authored first system message and part extras", func(t *testing.T) {
		authored := []model.Message{
			{
				Role: model.RoleSystem,
				Content: []model.ContentPart{{Kind: model.PartText, Text: "authored system", Extra: model.Extra{
					"part": json.RawMessage(`{"keep":true}`),
				}}},
				Extra: model.Extra{"sys": json.RawMessage(`"keep"`)},
			},
			{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "body"}}},
		}
		runAuthoredSummaryRound(t, authored, true, func(t *testing.T, sent model.Request, fact maintenanceFact) {
			requireAuthoredMessages(t, sent.Messages, authored)
			requireAuthoredMessages(t, fact.Input, authored)
		}, func(*testing.T, RequestHookInput) {})
	})

	t.Run("an echoed present-empty prompt keeps the authored empty system message", func(t *testing.T) {
		authored := []model.Message{
			{Role: model.RoleSystem, Content: []model.ContentPart{}},
			{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "body"}}},
		}
		runAuthoredSummaryRound(t, authored, true, func(t *testing.T, sent model.Request, fact maintenanceFact) {
			requireAuthoredMessages(t, sent.Messages, authored)
			requireAuthoredMessages(t, fact.Input, authored)
		}, func(*testing.T, RequestHookInput) {})
	})
}

// runAuthoredSummaryRound runs one automatic round over a fixed strategy
// request, with a pure inspecting hook on both invocations and a checkpoint
// Consume outcome.
func runAuthoredSummaryRound(t *testing.T, authored []model.Message, echo bool, checkRequest func(*testing.T, model.Request, maintenanceFact), checkHook func(*testing.T, RequestHookInput)) {
	t.Helper()
	seed := UsageCount{InputTokens: 13}
	h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
	var sent model.Request
	exec := purposeExecution(quietConversation(), func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		return summaryStream("summary", model.Usage{InputTokens: 1}), nil
	})
	exec.ResolveStrategy = fixedStrategyResolver(&StrategyBinding{ID: IncludedStrategyID, RequiresContextWindow: true, Strategy: requestShapeStrategy{request: authored}})
	opened := exec.OpenInvocation
	exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
		inv, err := opened(ctx, request)
		if err != nil {
			return inv, err
		}
		inv.Model = func(ctx context.Context, req model.Request, _ model.Extra) (model.Stream, error) {
			if request.Purpose == InvocationPurposeMaintenance {
				sent = req
				return summaryStream("summary", model.Usage{InputTokens: 1}), nil
			}
			return completedTurnStream(), nil
		}
		inv.RequestStage = fixtureRequestStage([]RequestHook{{
			ID: "req.inspect", ExtensionID: "req-ext",
			Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
				if in.Purpose == InvocationPurposeMaintenance {
					checkHook(t, in)
					if echo {
						prompt := in.Prompt
						return RequestConsequence{Prompt: &prompt}, nil
					}
				}
				return RequestConsequence{}, nil
			},
		}}...)
		return inv, nil
	}
	testassert.NoError(t, runAutomaticMaintenance(t, h, c, exec))
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	comp := compactedEntryOf(t, graph)
	testassert.Require(t, len(comp.Facts) == 1, "checkpoint facts = %+v, want the one summary producer", comp.Facts)
	checkRequest(t, sent, comp.Facts[0])
}

func TestRequestHookSummaryPositionDoesNotAddressPrompt(t *testing.T) {
	authored := []model.Message{
		{Role: model.RoleSystem, Content: admissionContent("authored")},
		{Role: model.RoleUser, Content: admissionContent("body")},
	}
	for _, prompt := range []string{"authored", ""} {
		t.Run(fmt.Sprintf("prompt=%q", prompt), func(t *testing.T) {
			view := newStrategyRequestView(testCapture(), "", "", model.Request{Messages: authored})
			position := 0
			accepted, err := view.acceptConsequence(RequestConsequence{
				Prompt: &prompt, Edits: []RequestMessageEdit{{Position: &position, Content: admissionContent("edited body")}},
			})
			testassert.NoError(t, err)
			view.applyConsequence(accepted)
			request, err := shapeStrategyRequest(view, model.Request{Messages: authored})
			testassert.NoError(t, err)
			want := []model.Message{{Role: model.RoleUser, Content: admissionContent("edited body")}}
			if prompt != "" {
				want = append([]model.Message{authored[0]}, want...)
			}
			requireAuthoredMessages(t, request.Messages, want)
		})
	}
}

// TestSummaryRunningRequestClearedBeforeConsume proves the one summary request
// occupies the existing running-request slot while it is dispatched and is
// cleared by the model settlement before the pure folding stage sees it; the
// durable producer identity is the maintenance invocation's own issued ID on
// the fact and its hook results.
func TestSummaryRunningRequestClearedBeforeConsume(t *testing.T) {
	seed := UsageCount{InputTokens: 13}
	h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
	var dispatchedID, hookID string
	var sawModelState, sawConsumeState OperationCurrentState
	exec := purposeExecution(quietConversation(), func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		return summaryStream("summary", model.Usage{InputTokens: 1}), nil
	})
	exec.ResolveStrategy = fixedStrategyResolver(&StrategyBinding{ID: IncludedStrategyID, RequiresContextWindow: true, Strategy: &maintenanceObserveStrategy{
		request: []model.Message{{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "pass"}}}},
		onConsume: func() {
			rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
			testassert.NoError(t, err)
			sawConsumeState = rec.State
		},
	}})
	opened := exec.OpenInvocation
	exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
		inv, err := opened(ctx, request)
		if err != nil {
			return inv, err
		}
		inv.Model = func(ctx context.Context, req model.Request, _ model.Extra) (model.Stream, error) {
			if request.Purpose == InvocationPurposeMaintenance {
				dispatchedID = request.ID
				rec, rerr := h.ReadOperation(context.Background(), sessionID, testOpID)
				testassert.NoError(t, rerr)
				sawModelState = rec.State
				return summaryStream("summary", model.Usage{InputTokens: 1}), nil
			}
			return completedTurnStream(), nil
		}
		if request.Purpose == InvocationPurposeMaintenance {
			inv.RequestStage = fixtureRequestStage([]RequestHook{{
				ID: "req.effectful", ExtensionID: "req-ext",
				Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
					hookID = in.RequestID
					return RequestConsequence{}, nil
				},
			}}...)
		}
		return inv, nil
	}
	testassert.NoError(t, runAutomaticMaintenance(t, h, c, exec))
	testassert.Require(t, dispatchedID != "" && hookID == dispatchedID, "summary hook/model identities = %q/%q, want the one issued invocation identity", hookID, dispatchedID)
	testassert.Require(t, sawModelState.Request != nil && sawModelState.Request.RequestID == dispatchedID && sawModelState.Request.BoundaryEntryID != "",
		"summary model state = %+v, want the one request occupying the running-request slot", sawModelState)
	testassert.Require(t, sawConsumeState.Request == nil && sawConsumeState.ActiveEffect == nil,
		"summary consume state = %+v, want the request cleared by the model settlement before folding", sawConsumeState)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	comp := compactedEntryOf(t, graph)
	testassert.Require(t, comp.Facts[0].RequestID == dispatchedID, "checkpoint fact producer = %q, want the issued summary identity %q", comp.Facts[0].RequestID, dispatchedID)
	rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, err)
	testassert.Require(t, rec.State.Status == OperationSuccess && rec.State.Request == nil, "operation after the round = %+v, want the success terminal with no request left", rec.State)
}

type maintenanceObserveStrategy struct {
	request   []model.Message
	onConsume func()
}

func (s *maintenanceObserveStrategy) Plan(PlanInput) (PlanOutput, error) {
	return PlanOutput{Kind: PlanModel, Model: &ModelPlan{Request: model.Request{Messages: s.request}}}, nil
}

func (s *maintenanceObserveStrategy) Consume(in ConsumeInput) (PlanOutput, error) {
	if s.onConsume != nil {
		s.onConsume()
	}
	return PlanOutput{Kind: PlanCheckpoint, Checkpoint: &CheckpointPlan{
		BoundaryEntryID: in.Input.BoundaryEntryID,
		Items:           authoredItems(model.Message{Role: model.RoleUser, Content: admissionContent("kept")}),
	}}, nil
}

// TestStrategyReplacementBeforeProcedure proves the replacement binding is
// accepted once at procedure start: a matching replacement that requires a
// window runs when the captured inputs carry one even though the capture's own
// requirement bit differs, a replacement whose required window the captured
// inputs lack fails before Plan, and a missing implementation fails before
// Plan as well.
func TestStrategyReplacementBeforeProcedure(t *testing.T) {
	type row struct {
		name       string
		window     int
		requires   bool
		absent     bool
		wantPlans  int
		wantStatus OperationState
		wantDetail string
	}
	for _, tc := range []row{
		{"required window present despite a changed requirement bit", 4096, true, false, 1, OperationSuccess, ""},
		{"required window absent fails before plan", 0, true, false, 0, OperationFailure, "requires a known context window"},
		{"absent implementation fails", 4096, false, true, 0, OperationFailure, "carries no implementation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := captureForHandoff()
			capture.ContextWindow = tc.window
			plans, modelCalls := 0, 0
			strategy := &countingCheckpointStrategy{plans: &plans}
			exec := Execution{OpenInvocation: func(_ context.Context, request InvocationRequest) (InvocationExecution, error) {
				if request.Purpose == InvocationPurposeMaintenance {
					modelCalls++
					return invocationExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
						return summaryStream("summary", model.Usage{InputTokens: 1}), nil
					}, func(context.Context, model.ToolCall) ToolOutcome { return ToolOutcome{} }), nil
				}
				return invocationExecution(quietConversation(), func(context.Context, model.ToolCall) ToolOutcome { return ToolOutcome{} }), nil
			}}
			binding := &StrategyBinding{ID: capture.Strategy.ID, RequiresContextWindow: tc.requires, Strategy: strategy}
			if tc.absent {
				binding.Strategy = nil
			}
			exec.ResolveStrategy = fixedStrategyResolver(binding)
			prepared := preparedExecutionFor(capture, func(context.Context, OperationAdmission) (Execution, error) { return exec, nil })
			h, store, c, sessionID := newAdmittedHarness(t, context.Background(), prepared)
			err := executeDirect(h, c, h.ctx, testOpID, prepared)
			if tc.wantStatus == OperationSuccess {
				testassert.NoError(t, err)
			} else {
				testassert.Require(t, err != nil && strings.Contains(err.Error(), tc.wantDetail), "run = %v, want %q", err, tc.wantDetail)
			}
			testassert.Equal(t, plans, tc.wantPlans)
			graph, gerr := validateFixture(t, store, sessionID)
			testassert.NoError(t, gerr)
			state := operationStateOf(t, graph, testOpID)
			testassert.Require(t, state.Status == tc.wantStatus, "operation = %+v, want %s", state, tc.wantStatus)
			if tc.wantStatus == OperationFailure {
				testassert.Equal(t, modelCalls, 0)
			}
		})
	}
}

type countingCheckpointStrategy struct{ plans *int }

func (s *countingCheckpointStrategy) Plan(PlanInput) (PlanOutput, error) {
	(*s.plans)++
	return PlanOutput{Kind: PlanModel, Model: &ModelPlan{Request: model.Request{Messages: []model.Message{
		{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "pass"}}},
	}}}}, nil
}

func (s *countingCheckpointStrategy) Consume(in ConsumeInput) (PlanOutput, error) {
	return PlanOutput{Kind: PlanCheckpoint, Checkpoint: &CheckpointPlan{
		BoundaryEntryID: in.Input.BoundaryEntryID,
		Items:           authoredItems(model.Message{Role: model.RoleUser, Content: admissionContent("kept")}),
	}}, nil
}

// TestMaintenanceSummaryLossRecovery proves the crash row on both summary
// phases through the real recovery path: loss while the summary request hook
// is active settles that hook under its reservation and witnesses the summary
// producer, while loss after the summary model ran but before its result
// committed settles the interrupted model without inventing usage or facts;
// neither replays work, dispatches the conversation, or survives a second
// recovery with any additional write.
func TestMaintenanceSummaryLossRecovery(t *testing.T) {
	t.Run("loss after the summary model ran but before its result committed", func(t *testing.T) {
		seed := UsageCount{InputTokens: 13}
		h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
		var summaryID string
		var sent model.Request
		var parameters model.Extra
		modelCalls, conversationCalls := 0, 0
		exec := purposeExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			conversationCalls++
			return completedTurnStream(), nil
		}, func(_ context.Context, req model.Request, body model.Extra) (model.Stream, error) {
			modelCalls++
			sent, parameters = req, body
			return summaryStream("summary", model.Usage{InputTokens: 7}), nil
		})
		exec.ResolveStrategy = fixedStrategyResolver(&StrategyBinding{ID: IncludedStrategyID, RequiresContextWindow: true, Strategy: requestShapeStrategy{request: []model.Message{
			{Role: model.RoleSystem, Content: []model.ContentPart{{Kind: model.PartText, Text: "first"}, {Kind: model.PartText, Text: " second"}}, Extra: model.Extra{"custom": json.RawMessage(`1`)}},
			{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "pass"}}},
		}}})
		opened := exec.OpenInvocation
		exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
			inv, err := opened(ctx, request)
			if err != nil {
				return inv, err
			}
			if request.Purpose == InvocationPurposeMaintenance {
				summaryID = request.ID
				inv.RequestStage = fixtureRequestStage([]RequestHook{{ID: "req.summary", ExtensionID: "req-ext",
					Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
						return RequestConsequence{Prompt: &in.Prompt, Parameters: model.Extra{"temperature": json.RawMessage(`0.3`)}}, nil
					}}}...)
			}
			return inv, nil
		}
		failed := false
		store.replaceHook = func(key RegisterKey, payload json.RawMessage) error {
			if failed || key.Kind != RegisterOperation || !bytes.Contains(payload, []byte(`"unreported_maintenance":true`)) ||
				bytes.Contains(payload, []byte(`"active_effect"`)) {
				return nil
			}
			failed = true
			return fmt.Errorf("%w: injected summary result failure", ErrStorage)
		}
		err := runAutomaticMaintenance(t, h, c, exec)
		testassert.Require(t, errors.Is(err, ErrStorage), "run = %v, want the injected storage failure", err)
		testassert.Require(t, modelCalls == 1 && conversationCalls == 0, "model calls = maintenance %d / conversation %d, want the summary model only", modelCalls, conversationCalls)
		rec, rerr := h.ReadOperation(context.Background(), sessionID, testOpID)
		testassert.NoError(t, rerr)
		testassert.Require(t, rec.State.Status == OperationRunning && rec.State.ActiveEffect != nil && rec.State.ActiveEffect.Kind == EffectModel &&
			rec.State.ActiveEffect.Purpose == InvocationPurposeMaintenance && rec.State.ActiveEffect.RequestID == summaryID &&
			rec.State.Request != nil && rec.State.Request.RequestID == summaryID && !rec.State.UnreportedMaintenance,
			"state at the lost summary model = %+v, want the committed intent and summary request with no fabricated marker", rec.State)
		reserved := rec.State.ActiveEffect.ResultEntryID
		{
			err := Recover(context.Background(), store)
			testassert.NoError(t, err)
		}
		graph, gerr := validateFixture(t, store, sessionID)
		testassert.NoError(t, gerr)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Status == OperationInterruption && settlement.RequestID == summaryID &&
			settlement.EntryID == reserved &&
			settlement.Model == nil && settlement.Usage == nil && len(settlement.Facts) == 1 && settlement.Evidence == nil,
			"settlement = %+v, want exact input under the reserved result with no synthesized usage", settlement)
		fact := settlement.Facts[0]
		requireAuthoredMessages(t, fact.Input, sent.Messages)
		testassert.Require(t, fact.Model == testCompactCapture().Model && fact.Usage == nil && reflect.DeepEqual(fact.Parameters, parameters) &&
			reflect.DeepEqual(fact.HookIDs, []HookIdentity{{ExtensionID: "req-ext", HookID: "req.summary"}}), "recovered fact = %+v", fact)
		state := operationStateOf(t, graph, testOpID)
		testassert.Require(t, state.Status == OperationInterruption && state.Request == nil && !state.UnreportedMaintenance, "recovered operation = %+v", state)
		for _, modelUsage := range state.Usage.ByModel {
			testassert.Require(t, modelUsage.Model != testCompactCapture().Model, "recovery charged the compact model usage %+v", modelUsage)
		}
		assertMaintenanceRecoverIdempotent(t, store, sessionID, testOpID)
		testassert.Require(t, modelCalls == 1 && conversationCalls == 0, "recovery calls = summary %d / conversation %d, want no replay", modelCalls, conversationCalls)
	})
}

// assertMaintenanceRecoverIdempotent proves a second recovery writes nothing:
// the Operation register keeps its revision and payload and the entry count is
// unchanged.
func assertMaintenanceRecoverIdempotent(t *testing.T, store *graphStorage, sessionID, operationID string) {
	t.Helper()
	key := RegisterKey{SessionID: sessionID, Kind: RegisterOperation, OperationID: operationID}
	before, err := store.ReadRegister(context.Background(), key)
	testassert.NoError(t, err)
	entriesBefore, err := store.ReadEntries(context.Background(), sessionID, 0)
	testassert.NoError(t, err)
	testassert.NoError(t, Recover(context.Background(), store))
	after, err := store.ReadRegister(context.Background(), key)
	testassert.NoError(t, err)
	entriesAfter, err := store.ReadEntries(context.Background(), sessionID, 0)
	testassert.NoError(t, err)
	testassert.Require(t, after.Revision == before.Revision && bytes.Equal(after.Payload, before.Payload), "second recovery changed the operation register")
	testassert.Equal(t, len(entriesAfter), len(entriesBefore))
}

// requireAuthoredMessages compares a shaped request against the strategy's
// authored messages by value, tolerating the codec's nil-versus-empty extras
// normalization while rejecting any lost field or changed content.
func requireAuthoredMessages(t *testing.T, got, want []model.Message) {
	t.Helper()
	testassert.Require(t, len(got) == len(want), "messages = %+v, want %d authored messages", got, len(want))
	for i := range want {
		testassert.Require(t, got[i].Role == want[i].Role && got[i].TextContent() == want[i].TextContent() && got[i].ToolCallID == want[i].ToolCallID,
			"message %d = %+v, want %+v", i, got[i], want[i])
		testassert.Require(t, len(got[i].Content) == len(want[i].Content), "message %d content = %+v, want %+v", i, got[i].Content, want[i].Content)
		for j := range want[i].Content {
			testassert.Require(t, got[i].Content[j].Kind == want[i].Content[j].Kind && got[i].Content[j].Text == want[i].Content[j].Text,
				"message %d content %d = %+v, want %+v", i, j, got[i].Content[j], want[i].Content[j])
			for key, value := range want[i].Content[j].Extra {
				testassert.Require(t, reflect.DeepEqual(got[i].Content[j].Extra[key], value), "message %d content %d extra %q = %s, want %s", i, j, key, got[i].Content[j].Extra[key], value)
			}
		}
		for key, value := range want[i].Extra {
			testassert.Require(t, reflect.DeepEqual(got[i].Extra[key], value), "message %d extra %q = %s, want %s", i, key, got[i].Extra[key], value)
		}
	}
}

// TestSummaryRequestRejectsToolDefinitions proves the summary request advertises
// nothing: a maintenance hook selecting any tool is rejected against the empty
// catalogue before the summary is dispatched, and the operation settles its
// strategy failure without a checkpoint.
func TestSummaryRequestRejectsToolDefinitions(t *testing.T) {
	seed := UsageCount{InputTokens: 13}
	h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
	modelCalls := 0
	exec := purposeExecution(quietConversation(), func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		modelCalls++
		return summaryStream("summary", model.Usage{InputTokens: 1}), nil
	})
	exec.ResolveStrategy = fixedStrategyResolver(&StrategyBinding{ID: IncludedStrategyID, RequiresContextWindow: true, Strategy: requestShapeStrategy{request: []model.Message{
		{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "pass"}}},
	}}})
	opened := exec.OpenInvocation
	exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
		inv, err := opened(ctx, request)
		if err != nil {
			return inv, err
		}
		if request.Purpose == InvocationPurposeMaintenance {
			inv.RequestStage = fixtureRequestStage([]RequestHook{{
				ID: "req.tools", ExtensionID: "req-ext",
				Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
					return RequestConsequence{Tools: fixtureDefinitions("echo")}, nil
				},
			}}...)
		}
		return inv, nil
	}
	err := runAutomaticMaintenance(t, h, c, exec)
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "outside the stage tool catalog"), "run = %v, want the empty-catalog rejection", err)
	testassert.Equal(t, modelCalls, 0)
	graph, gerr := validateFixture(t, store, sessionID)
	testassert.NoError(t, gerr)
	testassert.Require(t, operationStateOf(t, graph, testOpID).Status == OperationFailure, "operation = %+v, want the strategy failure", operationStateOf(t, graph, testOpID))
	for _, entry := range graph.Entries {
		testassert.Require(t, entry.Compaction == nil, "a rejected summary tool selection committed a checkpoint")
	}
}

// countingProcedureStrategy records each Plan and Consume and either plans one
// model pass that folds to none or ends at none directly.
type countingProcedureStrategy struct {
	plans     int
	consumes  int
	modelOnce bool
}

func (s *countingProcedureStrategy) Plan(PlanInput) (PlanOutput, error) {
	s.plans++
	if !s.modelOnce {
		return PlanOutput{Kind: PlanNone}, nil
	}
	return PlanOutput{Kind: PlanModel, Model: &ModelPlan{Request: model.Request{Messages: []model.Message{
		{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "pass"}}},
	}}}}, nil
}

func (s *countingProcedureStrategy) Consume(ConsumeInput) (PlanOutput, error) {
	s.consumes++
	return PlanOutput{Kind: PlanNone}, nil
}

// TestStrategyLookupResolvedForEachMaintenanceProcedure proves the execution
// open no longer fixes one strategy for the Operation: the first procedure
// accepts the first lookup result and holds it through Plan and Consume, and
// the next procedure after the conversation continues accepts the replacement
// returned by the same execution's lookup.
func TestStrategyLookupResolvedForEachMaintenanceProcedure(t *testing.T) {
	first := &countingProcedureStrategy{modelOnce: true}
	second := &countingProcedureStrategy{}
	opens, lookups := 0, 0
	exec := Execution{
		ResolveStrategy: func(_ context.Context, id string) (*StrategyBinding, error) {
			lookups++
			testassert.Require(t, id == gapStrategyID, "resolved strategy id = %q, want the captured identity %q", id, gapStrategyID)
			if lookups == 1 {
				return &StrategyBinding{ID: gapStrategyID, Strategy: first}, nil
			}
			return &StrategyBinding{ID: gapStrategyID, Strategy: second}, nil
		},
		OpenInvocation: func(_ context.Context, request InvocationRequest) (InvocationExecution, error) {
			if request.Purpose == InvocationPurposeMaintenance {
				return invocationExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
					return summaryStream("summary", model.Usage{InputTokens: 1}), nil
				}, func(context.Context, model.ToolCall) ToolOutcome { return ToolOutcome{} }), nil
			}
			opens++
			round := 0
			return invocationExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				round++
				if round == 1 {
					return completedTurnStream(testToolCall("call-1")), nil
				}
				return completedTurnStream(), nil
			}, func(_ context.Context, call model.ToolCall) ToolOutcome {
				return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
			}), nil
		},
	}
	prepared := preparedExecutionFor(gapCapture(), func(context.Context, OperationAdmission) (Execution, error) { return exec, nil })
	h, store, c, sessionID := newAdmittedHarness(t, context.Background(), prepared)
	testassert.NoError(t, executeDirect(h, c, h.ctx, testOpID, prepared))
	testassert.Equal(t, opens, 1) // one conversation invocation over the same opened execution
	testassert.Equal(t, lookups, 2)
	testassert.Require(t, first.plans == 1 && first.consumes == 1, "first procedure = %d plans/%d consumes, want the held accepted binding", first.plans, first.consumes)
	testassert.Require(t, second.plans == 1 && second.consumes == 0, "second procedure = %d plans/%d consumes, want the replacement lookup result", second.plans, second.consumes)
	if _, err := validateFixture(t, store, sessionID); err != nil {
		t.Fatalf("graph validation: %v", err)
	}
}

// TestMaintenanceSummaryHookCancellationInterrupts proves a summary request
// hook canceled under a done execution context settles the procedure as an
// interruption — the token runRequestHooks returns is not an EffectInterrupted
// error — with no summary model dispatch and no checkpoint.
func TestMaintenanceSummaryHookCancellationInterrupts(t *testing.T) {
	seed := UsageCount{InputTokens: 13}
	h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
	modelCalls := 0
	exec := purposeExecution(quietConversation(), func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		modelCalls++
		return summaryStream("summary", model.Usage{InputTokens: 1}), nil
	})
	exec.ResolveStrategy = fixedStrategyResolver(&StrategyBinding{ID: IncludedStrategyID, RequiresContextWindow: true, Strategy: requestShapeStrategy{request: []model.Message{
		{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "pass"}}},
	}}})
	entered := make(chan struct{})
	var once sync.Once
	hook := RequestHook{ID: "req.cancel", ExtensionID: "req-ext", Run: func(ctx context.Context, _ RequestHookInput) (RequestConsequence, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return RequestConsequence{}, ctx.Err()
	}}
	opened := exec.OpenInvocation
	exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
		inv, err := opened(ctx, request)
		if err != nil {
			return inv, err
		}
		if request.Purpose == InvocationPurposeMaintenance {
			inv.RequestStage = fixtureRequestStage([]RequestHook{hook}...)
		}
		return inv, nil
	}
	execCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runAutomaticMaintenanceCtx(t, h, c, execCtx, exec) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the summary hook never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the canceled summary hook did not settle")
	}
	testassert.Equal(t, modelCalls, 0)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	state := operationStateOf(t, graph, testOpID)
	testassert.Require(t, state.Status == OperationInterruption && state.Request == nil && state.ActiveEffect == nil,
		"operation = %+v, want the summary-hook cancellation interruption", state)
	for _, entry := range graph.Entries {
		testassert.Require(t, entry.Compaction == nil, "a canceled summary hook committed a checkpoint")
	}
}

// TestEmptyHistorySummarySlotDispatchAndRecovery proves a history-free manual
// Session still dispatches a strategy-authored summary through a request hook
// and model under a purpose-bound running-request slot with absent canonical
// references.
func TestEmptyHistorySummarySlotDispatchAndRecovery(t *testing.T) {
	t.Run("dispatch and model-free settlement", func(t *testing.T) {
		store := emptyStore(t)
		var sawSlot OperationCurrentState
		var sessionID string
		exec := purposeExecution(forbiddenConversationModel(t), func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return summaryStream("summary", model.Usage{InputTokens: 1}), nil
		})
		exec.ResolveStrategy = fixedStrategyResolver(&StrategyBinding{ID: "test.empty/v1", Strategy: requestShapeStrategy{
			request: []model.Message{{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "pass"}}}},
			outcome: "none-success",
		}})
		capture := captureForHandoff()
		capture.Strategy = StrategyCapture{ID: "test.empty/v1"}
		opened := exec.OpenInvocation
		var h *Harness
		exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
			inv, err := opened(ctx, request)
			if err != nil {
				return inv, err
			}
			if request.Purpose == InvocationPurposeMaintenance {
				inv.RequestStage = fixtureRequestStage([]RequestHook{{
					ID: "req.effectful", ExtensionID: "req-ext",
					Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
						return RequestConsequence{}, nil
					},
				}}...)
				inv.Model = func(context.Context, model.Request, model.Extra) (model.Stream, error) {
					rec, rerr := h.ReadOperation(context.Background(), sessionID, "compact-1")
					testassert.NoError(t, rerr)
					sawSlot = rec.State
					return summaryStream("summary", model.Usage{InputTokens: 1}), nil
				}
			}
			return inv, nil
		}
		prepared := compactPrepared(capture, exec)
		h = newCompactHarness(t, store, prepared)
		session, err := h.CreateSession(context.Background(), CreateSessionRequest{Workspace: "/tmp/works", Inputs: testInputs("coder")})
		testassert.NoError(t, err)
		sessionID = session.Identity.SessionID
		c := leaseCoordinator(t, h, session.Identity.SessionID)
		testassert.NoError(t, runManualCompact(t, h, c, session.Identity.SessionID, "compact-1"))
		testassert.Require(t, sawSlot.Request != nil && sawSlot.Request.Purpose == InvocationPurposeMaintenance && sawSlot.Request.BoundaryEntryID == "",
			"summary slot during dispatch = %+v, want the purpose-bound empty-base request", sawSlot.Request)
		graph, gerr := validateFixture(t, store, session.Identity.SessionID)
		testassert.NoError(t, gerr)
		settlement := settlementEntryOf(t, graph)
		testassert.Require(t, settlement.Status == OperationSuccess && settlement.CompactionUsage && settlement.RequestID != "" &&
			len(settlement.Facts) == 1 && settlement.Facts[0].RequestID == settlement.RequestID && settlement.Evidence == nil,
			"settlement = %+v, want the actual summary producer with carried facts and no invented boundary", settlement)
	})

}

// planErrorStrategy fails its first Plan before any model request opens.
type planErrorStrategy struct{ err error }

func (s planErrorStrategy) Plan(PlanInput) (PlanOutput, error) { return PlanOutput{}, s.err }
func (planErrorStrategy) Consume(ConsumeInput) (PlanOutput, error) {
	return PlanOutput{Kind: PlanNone}, nil
}

// TestCarriedFactsModelFreeTerminalSettlesOnce proves an earlier maintenance
// model's facts can be carried into a later terminal with no request at all:
// each fact owns its producer, the terminal needs no invented request identity,
// and the facts are recorded and charged exactly once.
func TestCarriedFactsModelFreeTerminalSettlesOnce(t *testing.T) {
	first := &countingProcedureStrategy{modelOnce: true}
	second := planErrorStrategy{err: errors.New("second plan broke before any model")}
	lookups := 0
	exec := Execution{
		ResolveStrategy: func(_ context.Context, id string) (*StrategyBinding, error) {
			lookups++
			testassert.Require(t, id == gapStrategyID, "resolved id = %q, want %q", id, gapStrategyID)
			if lookups == 1 {
				return &StrategyBinding{ID: gapStrategyID, Strategy: first}, nil
			}
			return &StrategyBinding{ID: gapStrategyID, Strategy: second}, nil
		},
		OpenInvocation: func(_ context.Context, request InvocationRequest) (InvocationExecution, error) {
			if request.Purpose == InvocationPurposeMaintenance {
				return invocationExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
					return summaryStream("summary", model.Usage{InputTokens: 5}), nil
				}, func(context.Context, model.ToolCall) ToolOutcome { return ToolOutcome{} }), nil
			}
			round := 0
			return invocationExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				round++
				if round == 1 {
					return completedTurnStream(testToolCall("call-1")), nil
				}
				return completedTurnStream(), nil
			}, func(_ context.Context, call model.ToolCall) ToolOutcome {
				return ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}
			}), nil
		},
	}
	prepared := preparedExecutionFor(gapCapture(), func(context.Context, OperationAdmission) (Execution, error) { return exec, nil })
	h, store, c, sessionID := newAdmittedHarness(t, context.Background(), prepared)
	runErr := executeDirect(h, c, h.ctx, testOpID, prepared)
	testassert.Require(t, runErr != nil && strings.Contains(runErr.Error(), "second plan broke"), "run = %v, want the second procedure's failure", runErr)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	settlement := settlementEntryOf(t, graph)
	testassert.Require(t, settlement.Status == OperationFailure && strings.Contains(settlement.Detail, "second plan broke"),
		"settlement = %+v, want the second procedure's model-free failure", settlement)
	testassert.Require(t, settlement.RequestID == "" && len(settlement.Facts) == 1 && settlement.Facts[0].RequestID != "" &&
		settlement.Facts[0].Usage != nil && *settlement.Facts[0].Usage == (UsageCount{InputTokens: 5}),
		"settlement = %+v, want the carried fact with its own producer and no terminal request identity", settlement)
	usage := UsageByModelForTest(operationStateOf(t, graph, testOpID).Usage, testCompactCapture().Model)
	testassert.Require(t, usage.ByModel != nil && len(usage.ByModel) == 1 && usage.ByModel[0].Usage == (UsageCount{InputTokens: 5}),
		"operation maintenance usage = %+v, want the carried fact charged exactly once", usage)
}
