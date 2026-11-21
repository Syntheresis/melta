package harness

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// emptyText returns a pointer to the empty string for present-empty
// consequence members.
func emptyText() *string {
	text := ""
	return &text
}

// compositionRun drives one real admitted execution and returns the first
// conversation request the physical model received.
func compositionRun(t *testing.T, h *Harness, c *coordinator, exec Execution) (model.Request, error) {
	t.Helper()
	return runAdmittedExecution(t, h, c, context.Background(), testOpID, testCapture(), exec)
}

// compositionModelEffect returns the committed assistant payload of one
// settled conversation request.
func compositionAssistant(t *testing.T, store *graphStorage, sessionID string) *assistantEntry {
	t.Helper()
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var out *assistantEntry
	for _, entry := range graph.Entries {
		if entry.Assistant != nil {
			out = entry.Assistant
		}
	}
	testassert.Require(t, out != nil, "no committed assistant entry")
	return out
}

// TestRequestHookPurePromptRemoval proves the pure request-boundary rows: a
// present-empty Prompt removes the captured prompt from the actual dispatch,
// the contribution is recorded as durable evidence on the answering
// assistant, and the pure hook commits no durable effect of its own.
func TestRequestHookPurePromptRemoval(t *testing.T) {
	h, store, c, sessionID := newEffectHarness(t, nil)
	remover := RequestHook{ID: "req.prompt.remove", ExtensionID: "req-ext", Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
		return RequestConsequence{Prompt: emptyText()}, nil
	}}
	inv := InvocationExecution{LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{remover}...)}
	sent, err := compositionRun(t, h, c, executionForInvocation(inv))
	testassert.NoError(t, err)
	testassert.Require(t, len(sent.Messages) > 0 && sent.Messages[0].Role != model.RoleSystem, "dispatch request = %+v, want no system prompt message after a present-empty prompt", sent.Messages)

	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, assistant.Evidence != nil && assistant.Evidence.Prompt == "", "assistant evidence = %+v, want the recorded empty-prompt contribution", assistant.Evidence)
	testassert.Require(t, len(assistant.Evidence.HookIDs) == 1 && assistant.Evidence.HookIDs[0].HookID == "req.prompt.remove" && assistant.Evidence.HookIDs[0].ExtensionID == "req-ext", "assistant evidence hook identities = %+v, want the pure contributor recorded", assistant.Evidence.HookIDs)

	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	for _, entry := range graph.Entries {
		testassert.Require(t, entry.HookResult == nil, "pure request hook committed a durable hook result: %+v", entry.HookResult)
	}
}

// TestRequestHookPureToolView proves the pure tool-view rows: a present
// selection narrows the advertised catalog, a present-empty selection
// advertises none while the actual captured catalog rides the request, an
// out-of-catalog name rejects the round before any model callback, and a
// later selection restoring the full catalog keeps every accepted identity
// while recording no durable tool-view delta.
func TestRequestHookPureToolView(t *testing.T) {
	t.Run("narrowed view advertised", func(t *testing.T) {
		h, store, c, sessionID := newEffectHarness(t, nil)
		empty := RequestHook{ID: "req.tools.empty", ExtensionID: "req-ext", Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
			return RequestConsequence{Tools: fixtureDefinitions()}, nil
		}}
		inv := InvocationExecution{LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{empty}...)}
		sent, err := compositionRun(t, h, c, executionForInvocation(inv))
		testassert.NoError(t, err)
		testassert.Require(t, len(sent.Tools) == 0, "dispatch tools = %+v, want the actual request narrowed to the empty advertised view", sent.Tools)
		assistant := compositionAssistant(t, store, sessionID)
		testassert.Require(t, assistant.Evidence != nil && len(assistant.Evidence.Tools) == 0 && assistant.Evidence.Tools != nil, "assistant evidence = %+v, want the recorded empty advertised view", assistant.Evidence)
		testassert.Require(t, len(assistant.Evidence.HookIDs) == 1 && assistant.Evidence.HookIDs[0].HookID == "req.tools.empty", "assistant evidence hook identities = %+v, want the narrowing pure contributor retained", assistant.Evidence.HookIDs)
	})
	t.Run("outside catalog rejects before dispatch", func(t *testing.T) {
		h, _, c, _ := newEffectHarness(t, nil)
		calls := 0
		bad := RequestHook{ID: "req.tools.bad", ExtensionID: "req-ext", Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
			return RequestConsequence{Tools: fixtureDefinitions("not-captured")}, nil
		}}
		inv := InvocationExecution{LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{bad}...)}
		inv.Model = func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			calls++
			return completedTurnStream(), nil
		}
		_, err := compositionRun(t, h, c, executionForInvocation(inv))
		testassert.Require(t, err != nil && strings.Contains(err.Error(), "outside the stage tool catalog"), "round = %v, want the out-of-catalog rejection", err)
		testassert.Equal(t, calls, 0)
	})
	t.Run("restored view keeps accepted identities with no delta", func(t *testing.T) {
		h, store, c, sessionID := newEffectHarness(t, nil)
		narrow := RequestHook{ID: "req.tools.narrow", ExtensionID: "req-ext", Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
			return RequestConsequence{Tools: fixtureDefinitions()}, nil
		}}
		restore := RequestHook{ID: "req.tools.restore", ExtensionID: "req-ext", Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
			return RequestConsequence{Tools: fixtureDefinitions("echo")}, nil // a selection covering the captured catalog restores the full view
		}}
		inv := InvocationExecution{LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{narrow, restore}...)}
		sent, err := compositionRun(t, h, c, executionForInvocation(inv))
		testassert.NoError(t, err)
		testassert.Require(t, len(sent.Tools) == 1 && sent.Tools[0].Name == "echo", "dispatch tools = %+v, want the restored full catalog", sent.Tools)
		assistant := compositionAssistant(t, store, sessionID)
		testassert.Require(t, assistant.Evidence != nil && reflect.DeepEqual(fixtureDefinitionNames(assistant.Evidence.Tools), []string{"echo"}), "assistant evidence = %+v, want restored tool view", assistant.Evidence)
		testassert.Require(t, len(assistant.Evidence.HookIDs) == 2 && assistant.Evidence.HookIDs[0].HookID == "req.tools.narrow" && assistant.Evidence.HookIDs[1].HookID == "req.tools.restore", "assistant evidence hook identities = %+v, want both accepted hooks in configured order", assistant.Evidence.HookIDs)
	})
}

// TestRequestHookFrozenInputOwnership proves the input-ownership rows: the
// hook receives the frozen canonical projection with its canonical
// references, and mutating the delivered views cannot reach the dispatch or
// the durable state.
func TestRequestHookFrozenInputOwnership(t *testing.T) {
	h, store, c, sessionID := newEffectHarness(t, nil)
	var saw RequestHookInput
	observer := RequestHook{ID: "req.observe", ExtensionID: "req-ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		saw = in
		for i := range in.Messages {
			for j := range in.Messages[i].Content {
				in.Messages[i].Content[j].Text = "mutated"
			}
		}
		in.Tools = append(in.Tools, model.ToolDefinition{Name: "extra", Parameters: json.RawMessage(`{"type":"object"}`)})
		in.Prompt += " mutated"
		return RequestConsequence{}, nil
	}}
	inv := InvocationExecution{LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{observer}...)}
	sent, err := compositionRun(t, h, c, executionForInvocation(inv))
	testassert.NoError(t, err)
	testassert.Require(t, saw.RequestID != "" && saw.BoundaryEntryID != "", "hook input = %+v, want the bound request identity and canonical boundary", saw)
	testassert.Require(t, len(saw.Messages) == 1 && saw.Messages[0].TextContent() == "mutated", "hook mutation did not reach the delivered copy")
	testassert.Require(t, len(sent.Messages) == 2 && sent.Messages[1].TextContent() == "hello", "dispatch messages = %+v, want the frozen canonical input unchanged by hook mutation", sent.Messages)
	testassert.Require(t, len(sent.Tools) == 1 && sent.Tools[0].Name == "echo", "dispatch tools = %+v, want the captured catalog unchanged by hook mutation", sent.Tools)
	testassert.Require(t, strings.Contains(sent.Messages[0].TextContent(), "system") && !strings.Contains(sent.Messages[0].TextContent(), "mutated"), "dispatch prompt = %q, want the captured prompt unchanged", sent.Messages[0].TextContent())
	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, len(assistant.Evidence.HookIDs) == 1 && assistant.Evidence.HookIDs[0].HookID == "req.observe" && assistant.Evidence.Prompt == testCapture().SystemPrompt && assistant.Evidence.Edits == nil, "assistant evidence = %+v, want the accepted observer identity and captured prompt", assistant.Evidence)
}

// TestHookFamiliesDisjointForSameIdentity proves the binding-isolation rows:
// a request-bound hook and a tool-bound hook configured with identical
// extension and hook identities never cross-bind — the request family records
// consumed evidence with no argument members, the tool family records consumed
// bytes with no evidence, and each family's provenance lands only on its own
// record.
func TestHookFamiliesDisjointForSameIdentity(t *testing.T) {
	sharedID := "shared.hook"
	sharedExt := "shared-ext"
	hookToolCall := model.ToolCall{ID: "call-1", Name: "echo", Arguments: json.RawMessage(`{"x":1}`)}
	spy := &hookToolSpy{}
	rounds := 0
	exec := Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
		prompt := "shared request prompt"
		return InvocationExecution{
			Model: func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				rounds++
				if rounds == 1 {
					return completedTurnStream(hookToolCall), nil
				}
				return completedTurnStream(), nil
			},
			LookupTool: fixtureToolLookup(spy.tool, objectNormalize),
			RequestStage: fixtureRequestStage([]RequestHook{{ID: sharedID, ExtensionID: sharedExt, Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
				return RequestConsequence{Prompt: &prompt}, nil
			}}}...),
			ToolHooks: []ToolArgumentsHook{{ID: sharedID, ExtensionID: sharedExt, Mode: HookModeEffectful, Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
				return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":2}`)}, nil
			}}},
		}, nil
	}}
	h, store, c, sessionID := newHookHarness(t, exec)
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))

	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, assistant.Evidence != nil && len(assistant.Evidence.HookIDs) == 1 && assistant.Evidence.HookIDs[0].HookID == sharedID && assistant.Evidence.HookIDs[0].ExtensionID == sharedExt, "assistant evidence hook identities = %+v, want only the request-bound family record", assistant.Evidence.HookIDs)
	testassert.Require(t, assistant.Evidence.Prompt == "shared request prompt", "assistant evidence = %+v, want the request family's consumed prompt contribution", assistant.Evidence)

	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var hookResult *hookResultEntry
	var toolResult *toolResultEntry
	for _, entry := range graph.Entries {
		if entry.HookResult != nil {
			hookResult = entry.HookResult
		}
		if entry.ToolResult != nil {
			toolResult = entry.ToolResult
		}
	}
	testassert.Require(t, hookResult != nil && hookResult.RequestID == "" && hookResult.ToolCallID == "call-1" && hookResult.InputEvidence == nil, "tool-bound hook result carries a request binding or consumed evidence: %+v", hookResult)
	testassert.Require(t, hookResult.InputArgumentsBase64 != "", "tool-bound hook result carries no consumed input bytes: %+v", hookResult)
	testassert.Require(t, toolResult != nil && len(toolResult.HookIDs) == 1 && toolResult.HookIDs[0].HookID == sharedID && toolResult.HookIDs[0].ExtensionID == sharedExt, "tool result hook provenance = %+v, want only the tool-bound family record", toolResult.HookIDs)
}

// TestArgumentHookChainPureRestoreKeepsAcceptedIdentities proves the chain
// provenance rule: every successfully accepted hook identity — pure
// replacements and an effectful hook alike, changed or restored — stays in the
// tool settlement's provenance in configured order, while an effectful hook
// always records its own result and retains the consumed intermediate bytes
// it actually consumed.
func TestArgumentHookChainPureRestoreKeepsAcceptedIdentities(t *testing.T) {
	original := json.RawMessage(`{"x":1}`)
	spy := &hookToolSpy{}
	exec := hookFixtureExecution(spy.tool, objectNormalize,
		ToolArgumentsHook{ID: "cap.pure.one", ExtensionID: "cap-a", Mode: HookModePure, Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
			return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":9}`)}, nil
		}},
		ToolArgumentsHook{ID: "cap.pure.two", ExtensionID: "cap-a", Mode: HookModePure, Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
			return ToolArgumentsResult{Arguments: model.CloneRaw(original)}, nil // restore the original, not the preceding hook's replacement
		}},
		ToolArgumentsHook{ID: "cap.effectful", ExtensionID: "cap-a", Mode: HookModeEffectful, Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
			return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":7}`)}, nil
		}},
	)
	call := model.ToolCall{ID: "call-1", Name: "echo", Arguments: original}
	_, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, call)
	testassert.NoError(t, err)
	testassert.Require(t, results[0].Status == model.ResultSuccess, "tool result = %+v, want the chain settled", results[0])

	graph, gerr := validateFixture(t, store, sessionID)
	testassert.NoError(t, gerr)
	var hookResults []hookResultEntry
	var toolResult *toolResultEntry
	for _, entry := range graph.Entries {
		if entry.HookResult != nil {
			hookResults = append(hookResults, *entry.HookResult)
		}
		if entry.ToolResult != nil {
			toolResult = entry.ToolResult
		}
	}
	testassert.Require(t, len(hookResults) == 1 && hookResults[0].HookID == "cap.effectful", "committed hook results = %+v, want only the effectful hook's own result", hookResults)
	consumed, derr := base64.StdEncoding.DecodeString(hookResults[0].InputArgumentsBase64)
	testassert.Require(t, derr == nil && string(consumed) == string(original), "effectful hook result consumed input = %q, want the restored original bytes", consumed)
	testassert.Require(t, toolResult != nil && len(toolResult.HookIDs) == 3 && toolResult.HookIDs[0].HookID == "cap.pure.one" && toolResult.HookIDs[1].HookID == "cap.pure.two" && toolResult.HookIDs[2].HookID == "cap.effectful", "tool result hook provenance = %+v, want every accepted identity in configured order", toolResult.HookIDs)
	prepared := spy.received()
	testassert.Require(t, len(prepared) == 1 && string(prepared[0].Arguments) == `{"x":7}`, "preparer received %+v, want the effectful final value", prepared)
}

// TestMaintenanceFactsSettleOnce proves the maintenance provenance rows: a
// committed checkpoint carries the actual summary request input and the
// consumed evidence under the maintenance identity, and its Operation then
// carries no settlement-carried facts; a failed maintenance settles the same
// facts exactly once on the terminal instead.
func TestMaintenanceFactsSettleOnce(t *testing.T) {
	t.Run("committed checkpoint carries the facts", func(t *testing.T) {
		seed := UsageCount{InputTokens: 13}
		h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
		var summaryRequest model.Request
		exec := purposeExecution(
			func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				return completedTurnStream(), nil
			},
			func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
				summaryRequest = r
				return summaryStream("maintained", model.Usage{InputTokens: 5, OutputTokens: 2}), nil
			},
		)
		testassert.NoError(t, runAutomaticMaintenance(t, h, c, exec))
		graph, err := validateFixture(t, store, sessionID)
		testassert.NoError(t, err)
		var compaction *contextCheckpointEntry
		var settlement *operationSettlementEntry
		for _, entry := range graph.Entries {
			if entry.Compaction != nil {
				compaction = entry.Compaction
			}
			if entry.Settlement != nil {
				settlement = entry.Settlement
			}
		}
		testassert.Require(t, compaction != nil, "no committed compaction entry")
		testassert.Require(t, len(compaction.Facts) == 1 && compaction.Facts[0].RequestID != "" && compaction.Facts[0].Input != nil && compaction.Evidence != nil && compaction.Evidence.BoundaryEntryID != "", "compaction entry provenance = %+v, want the maintenance identity, actual input, and consumed evidence", compaction)
		testassert.Require(t, reflect.DeepEqual(compaction.Facts[0].Input, summaryRequest.Messages), "compaction fact input = %+v, want the exact tool-free summary request the effect consumed", compaction.Facts[0].Input)
		for _, message := range compaction.Facts[0].Input {
			testassert.Require(t, len(message.ToolCalls) == 0, "compaction input carries tool members: %+v", message)
		}
		testassert.Require(t, settlement != nil && len(settlement.Facts) == 0, "terminal settlement = %+v, want the checkpoint to carry the maintenance facts", settlement)
	})
	t.Run("failed maintenance settles facts on the terminal", func(t *testing.T) {
		seed := UsageCount{InputTokens: 13}
		h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
		exec := purposeExecution(
			func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				t.Fatal("maintenance failure dispatched the conversation request")
				return nil, nil
			},
			func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				return nil, errMaintenanceSummary
			},
		)
		err := runAutomaticMaintenance(t, h, c, exec)
		testassert.Require(t, err != nil && err.Error() == errMaintenanceSummary.Error(), "round error = %v, want the nonnil owner-reconstructed summary transport detail", err)
		graph, gerr := validateFixture(t, store, sessionID)
		testassert.NoError(t, gerr)
		var settlement *operationSettlementEntry
		for _, entry := range graph.Entries {
			testassert.Require(t, entry.Compaction == nil, "maintenance failure committed a checkpoint: %+v", entry.Compaction)
			if entry.Settlement != nil {
				settlement = entry.Settlement
			}
		}
		testassert.Require(t, settlement != nil && len(settlement.Facts) > 0 && settlement.RequestID != "" && settlement.Evidence != nil, "settlement = %+v, want the maintenance facts settled on the terminal", settlement)
	})
	t.Run("checkpointed maintenance then failed maintenance attributes each producer once", func(t *testing.T) {
		seed := UsageCount{InputTokens: 13}
		h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
		var summaryInputs [][]model.Message
		summaryCalls, dispatchCalls := 0, 0
		rounds := 0
		exec := purposeExecution(
			func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				dispatchCalls++
				rounds++
				if rounds == 1 {
					return completedTurnStream(testToolCall("call-1")), nil // reports {3,1,2}: above threshold 4
				}
				return completedTurnStream(), nil
			},
			func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
				summaryCalls++
				summaryInputs = append(summaryInputs, append([]model.Message(nil), r.Messages...))
				if summaryCalls == 1 {
					return summaryStream("M1", model.Usage{InputTokens: 1, OutputTokens: 1}), nil
				}
				return nil, errMaintenanceSummary
			},
		)
		opened := exec.OpenInvocation
		exec.OpenInvocation = func(ctx context.Context, request InvocationRequest) (InvocationExecution, error) {
			inv, err := opened(ctx, request)
			if err != nil {
				return inv, err
			}
			if request.Purpose == InvocationPurposeConversation {
				inv.LookupTool = fixtureToolLookup(func(ctx context.Context, call model.ToolCall) PreparedTool {
					return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}}
				}, objectNormalize)
			}
			return inv, nil
		}
		err := runAutomaticMaintenance(t, h, c, exec)
		testassert.Require(t, err != nil && err.Error() == errMaintenanceSummary.Error(), "run = %v, want the nonnil owner-reconstructed summary transport detail", err)
		testassert.Equal(t, summaryCalls, 2)
		testassert.Equal(t, dispatchCalls, 1) // the failed maintenance dispatched no conversation request
		graph, gerr := validateFixture(t, store, sessionID)
		testassert.NoError(t, gerr)
		var compaction *contextCheckpointEntry
		var settlement *operationSettlementEntry
		compactions, settlements := 0, 0
		for i := range graph.Entries {
			if graph.Entries[i].Compaction != nil {
				compaction = graph.Entries[i].Compaction
				compactions++
			}
			if graph.Entries[i].Settlement != nil {
				settlement = graph.Entries[i].Settlement
				settlements++
			}
		}
		testassert.Require(t, compactions == 1 && settlements == 1, "facts carriers = %d checkpoints, %d terminals, want each maintenance settled once", compactions, settlements)
		testassert.Require(t, compaction.Facts[0].RequestID != "" && settlement.RequestID != "" && compaction.Facts[0].RequestID != settlement.RequestID,
			"producers = checkpoint %q terminal %q, want two distinct maintenance identities", compaction.Facts[0].RequestID, settlement.RequestID)
		testassert.Require(t, reflect.DeepEqual(compaction.Facts[0].Input, summaryInputs[0]) && reflect.DeepEqual(settlement.Facts[0].Input, summaryInputs[1]),
			"facts carry summary inputs of the wrong producers: checkpoint %+v terminal %+v", compaction.Facts[0].Input, settlement.Facts[0].Input)
		testassert.Require(t, compaction.Facts[0].Usage != nil && compaction.Facts[0].Usage.InputTokens == 1,
			"checkpoint fact usage = %+v, want the producing summary's usage billed on its own entry", compaction.Facts[0].Usage)
		testassert.Require(t, graph.Operations[0].State.Status == OperationFailure && graph.Operations[0].State.Terminal != nil,
			"operation = %+v, want the cold graph terminal on the maintenance failure", graph.Operations[0].State)
	})
}

var (
	errMaintenanceSummary = errors.New("summary transport failure")
	errMaintenanceOpener  = errors.New("maintenance opener broke")
)

// TestRunningRequestSuspendAndClear proves the running-request lifecycle:
// the first conversation intent publishes the frozen request state with its
// canonical references, a pending tool batch keeps that request suspended
// across its tool settlement, and the quiet conversation result clears it.
func TestRunningRequestSuspendAndClear(t *testing.T) {
	type observation struct {
		requestNil bool
		pending    int
		requestID  string
	}
	var hookSaw []observation
	var toolSaw []observation
	var h *Harness
	var store *graphStorage
	var c *coordinator
	var sessionID string
	observe := func() observation {
		rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
		testassert.NoError(t, err)
		out := observation{requestNil: rec.State.Request == nil, pending: len(rec.State.PendingToolCalls)}
		if rec.State.Request != nil {
			out.requestID = rec.State.Request.RequestID
		}
		return out
	}
	rounds := 0
	exec := Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
		return InvocationExecution{
			Model: func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				rounds++
				switch rounds {
				case 1:
					return completedTurnStream(model.ToolCall{ID: "call-1", Name: "echo", Arguments: json.RawMessage(`{"x":1}`)}), nil
				case 2:
					return completedTurnStream(model.ToolCall{ID: "call-2", Name: "echo", Arguments: json.RawMessage(`{"x":2}`)}), nil
				}
				return completedTurnStream(), nil
			},
			LookupTool: fixtureToolLookup(func(ctx context.Context, call model.ToolCall) PreparedTool {
				saw := observe()
				toolSaw = append(toolSaw, saw)
				testassert.Require(t, saw.requestID != "", "tool prepare observed no producing request")
				rec, rerr := h.ReadOperation(context.Background(), sessionID, testOpID)
				testassert.NoError(t, rerr)
				testassert.Require(t, len(rec.State.PendingToolCalls) == 1 && rec.State.PendingToolCalls[0].AssistantEntry.SessionID == sessionID && rec.State.PendingToolCalls[0].AssistantEntry.EntryID != "",
					"pending reservation = %+v, want the producing assistant identity", rec.State.PendingToolCalls)
				graph, gerr := validateFixture(t, store, sessionID)
				testassert.NoError(t, gerr)
				found := false
				for i := range graph.Entries {
					if graph.Entries[i].Assistant != nil {
						for _, tc := range graph.Entries[i].Assistant.ToolCalls {
							if tc.ID == call.ID {
								found = true
								testassert.Require(t, graph.Entries[i].Envelope.ID == rec.State.PendingToolCalls[0].AssistantEntry.EntryID,
									"pending assistant = %q, want the committed assistant %q", rec.State.PendingToolCalls[0].AssistantEntry.EntryID, graph.Entries[i].Envelope.ID)
							}
						}
					}
				}
				testassert.Require(t, found, "no committed assistant carries the preparing call %q", call.ID)
				return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "done"}}}
			}, objectNormalize),
			RequestStage: fixtureRequestStage([]RequestHook{{ID: "req.observe", ExtensionID: "req-ext", Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
				hookSaw = append(hookSaw, observe())
				return RequestConsequence{}, nil
			}}}...),
		}, nil
	}}
	h, store, c, sessionID = newHookHarness(t, exec)
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))

	testassert.Require(t, len(hookSaw) == 3, "hook observations = %+v, want one per request round", hookSaw)
	for i, saw := range hookSaw {
		testassert.Require(t, saw.requestNil && saw.pending == 0, "request round %d observation = %+v, want the prior batch drained with no published request before the next intent", i, saw)
	}
	testassert.Require(t, len(toolSaw) == 2, "tool observations = %+v, want one per executed call", toolSaw)
	for i, saw := range toolSaw {
		testassert.Require(t, !saw.requestNil && saw.pending == 1 && saw.requestID != "", "tool settlement %d observation = %+v, want the published request suspending its pending batch", i, saw)
	}
	rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, err)
	testassert.Require(t, rec.State.Status == OperationSuccess && rec.State.Request == nil && len(rec.State.PendingToolCalls) == 0,
		"operation state after the final batch = %+v, want the quiet terminal and the request cleared at the last settlement", rec.State)
	{
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	}
}

// TestModelAttemptDeepOwnedRequestCopies proves the per-attempt ownership
// rule through the real effect pipeline: every physical attempt receives its
// own deep-owned copy of the logical dispatch request, a callback that
// mutates its copy (message text, tool list, extras) cannot reach the next
// attempt's view or the retained evidence, and the settled assistant still
// answers the untouched logical request.
func TestModelAttemptDeepOwnedRequestCopies(t *testing.T) {
	var seen []string
	calls := 0
	modelFn := func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
		calls++
		fingerprint := fmt.Sprintf("%d/%d/%s/%s/%s", len(r.Messages), len(r.Tools), r.Messages[len(r.Messages)-1].TextContent(), r.Messages[len(r.Messages)-1].Content[0].Extra["raw"], r.Tools[0].Parameters)
		seen = append(seen, fingerprint)
		if calls == 1 {
			r.Messages[len(r.Messages)-1].Content[0].Text = "mutated"
			r.Messages[len(r.Messages)-1].Content[0].Extra["raw"][0] = 'X'
			r.Tools[0].Parameters[0] = 'X'
			r.Tools = append(r.Tools, model.ToolDefinition{Name: "extra", Parameters: json.RawMessage(`{"type":"object"}`)})
			return nil, errors.New("first attempt failed")
		}
		return completedTurnStream(), nil
	}
	h, store, c, sessionID := newEffectHarness(t, nil)
	exec := executionForInvocation(InvocationExecution{Model: modelFn, LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{{ID: "req.copy", ExtensionID: "req-ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		return RequestConsequence{Edits: appendRequestFixture(in, model.Message{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "extra", Extra: model.Extra{"raw": json.RawMessage(` { "x" : "<" } `)}}}})}, nil
	}}}...)})
	exec.Retry = (&retrySpy{limit: 1}).retry
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	testassert.Equal(t, calls, 2)
	testassert.Require(t, len(seen) == 2 && seen[0] == seen[1], "attempt views differ after mutation: %v", seen)
	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, assistant.Evidence != nil && assistant.Evidence.BoundaryEntryID != "", "assistant evidence = %+v, want the untouched logical request evidence", assistant.Evidence)
	testassert.Require(t, string(assistant.Evidence.Edits[len(assistant.Evidence.Edits)-1].Message.Content[0].Extra["raw"]) == ` { "x" : "<" } ` && reflect.DeepEqual(fixtureDefinitionNames(assistant.Evidence.Tools), []string{"echo"}), "mutation reached evidence or advertisement")
	{
		if _, err := validateFixture(t, store, sessionID); err != nil {
			t.Fatalf("graph validation: %v", err)
		}
	}
}

func TestPhysicalDispatchMatchesSourceRequest(t *testing.T) {
	h, store, c, sessionID := newEffectHarness(t, nil)
	contribution := model.Message{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "added", Extra: model.Extra{"raw": json.RawMessage(` { "value" : "<raw>" } `)}}}}
	hook := RequestHook{ID: "req.extra", ExtensionID: "req-ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		return RequestConsequence{Edits: appendRequestFixture(in, contribution)}, nil
	}}
	var sent model.Request
	var intentEvidence RequestEvidence
	modelFn := func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
		sent = r
		c.mu.Lock()
		op, ok := c.graph.Operation(testOpID)
		if !ok || op.State.ActiveEffect == nil || op.State.ActiveEffect.Evidence == nil {
			c.mu.Unlock()
			t.Fatal("model callback has no committed intent")
		}
		intentEvidence = copyRequestEvidence(*op.State.ActiveEffect.Evidence)
		c.mu.Unlock()
		return completedTurnStream(), nil
	}
	exec := executionForInvocation(InvocationExecution{Model: modelFn, LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{hook}...)})
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	want := model.Request{
		Messages: []model.Message{
			{Role: model.RoleSystem, Content: []model.ContentPart{{Kind: model.PartText, Text: "system"}}},
			{Role: model.RoleUser, Content: []model.ContentPart{{Kind: model.PartText, Text: "hello"}}},
			contribution,
		},
		Tools: []model.ToolDefinition{testToolDefinition()},
	}
	testassert.Require(t, reflect.DeepEqual(sent, want), "physical request = %+v, want independently authored %+v", sent, want)
	assistant := compositionAssistant(t, store, sessionID)
	testassert.Require(t, reflect.DeepEqual(intentEvidence, *assistant.Evidence), "intent evidence %+v differs from cold assistant evidence %+v", intentEvidence, *assistant.Evidence)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	testassert.Require(t, assistant.Evidence.BoundaryEntryID == graph.Entries[0].Envelope.ID && assistant.Evidence.Prompt == "system" && reflect.DeepEqual(fixtureDefinitionNames(assistant.Evidence.Tools), []string{"echo"}) && reflect.DeepEqual(*assistant.Evidence.Edits[len(assistant.Evidence.Edits)-1].Message, contribution) && reflect.DeepEqual(graph.Operations[0].Admission.Execution.Tools, want.Tools), "cold evidence differs from accepted request: %+v", assistant.Evidence)
}

func TestUnadvertisedCallSettlesUnavailable(t *testing.T) {
	h, store, c, sessionID := newEffectHarness(t, nil)
	modelCalls, normalizeCalls, prepareCalls := 0, 0, 0
	modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		modelCalls++
		if modelCalls == 1 {
			return completedTurnStream(model.ToolCall{ID: "ghost-call", Name: "ghost", Arguments: json.RawMessage(`{"x":1}`)}), nil
		}
		return completedTurnStream(), nil
	}
	normalize := func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
		normalizeCalls++
		return objectNormalize(ctx, call)
	}
	prepare := func(context.Context, model.ToolCall) PreparedTool { prepareCalls++; return PreparedTool{} }
	exec := executionForInvocation(InvocationExecution{Model: modelFn, LookupTool: fixtureToolLookup(prepare, normalize)})
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedExecutionFor(testCapture(), func(context.Context, OperationAdmission) (Execution, error) {
		return exec, nil
	})))
	rec, err := h.ReadOperation(context.Background(), sessionID, testOpID)
	testassert.NoError(t, err)
	testassert.Require(t, rec.State.Status == OperationSuccess && modelCalls == 2 && normalizeCalls == 0 && prepareCalls == 0, "state=%+v model=%d normalize=%d prepare=%d", rec.State, modelCalls, normalizeCalls, prepareCalls)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var call *toolCallRecord
	var toolResult *toolResultEntry
	for _, entry := range graph.Entries {
		if entry.Assistant != nil && len(entry.Assistant.ToolCalls) != 0 {
			call = &entry.Assistant.ToolCalls[0]
		}
		if entry.ToolResult != nil {
			toolResult = entry.ToolResult
		}
	}
	testassert.Require(t, call != nil && toolResult != nil && toolResult.Status == model.ResultError && toolResult.Content == `Tool "ghost" is not available.`, "call=%+v result=%+v", call, toolResult)
}

// TestRequestHookConsequenceToolMembersRejectBeforeDispatch proves the shared
// message rule at the hook consequence gate: a consequence carrying tool
// execution members rejects before any view advance, the pure chain commits
// nothing, and the round dispatches nothing.
func TestRequestHookConsequenceToolMembersRejectBeforeDispatch(t *testing.T) {
	sneaky := model.ToolCall{ID: "call-9", Name: "echo", Arguments: json.RawMessage(`{}`)}
	hook := RequestHook{ID: "req.bad", ExtensionID: "req-ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		return RequestConsequence{Edits: appendRequestFixture(in, model.Message{Role: model.RoleAssistant, Content: []model.ContentPart{{Kind: model.PartText, Text: "x"}}, ToolCalls: []model.ToolCall{sneaky}})}, nil
	}}
	inv := InvocationExecution{LookupTool: fixtureToolLookup(nil, objectNormalize), RequestStage: fixtureRequestStage([]RequestHook{hook}...)}
	calls := 0
	inv.Model = func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		calls++
		return completedTurnStream(), nil
	}
	h, store, c, sessionID := newEffectHarness(t, nil)
	_, err := compositionRun(t, h, c, executionForInvocation(inv))
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "tool calls"), "round = %v, want the consequence rejection", err)
	testassert.Equal(t, calls, 0)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	for _, entry := range graph.Entries {
		testassert.Require(t, entry.Assistant == nil, "a rejected consequence still committed an assistant entry")
	}
}

func TestRequestMessageCopyOwnsHistoricalToolCalls(t *testing.T) {
	original := model.Message{
		Role: model.RoleAssistant, Source: testModelRef(),
		ToolCalls: []model.ToolCall{{ID: "call-1", Name: "echo", Arguments: json.RawMessage("not json"), Extra: model.Extra{"meta": json.RawMessage(`{"n":1e0}`)}}},
	}
	owned := cloneMessageValue(original)
	owned.ToolCalls[0].Name = "changed"
	owned.ToolCalls[0].Arguments[0] = 'X'
	owned.ToolCalls[0].Extra["meta"][0] = '['
	owned.ToolCalls[0].Extra["injected"] = json.RawMessage(`true`)
	testassert.Check(t, original.ToolCalls[0].Name == "echo", "copied call scalar aliases the original")
	testassert.Check(t, string(original.ToolCalls[0].Arguments) == "not json", "copied raw arguments alias the original")
	testassert.Check(t, string(original.ToolCalls[0].Extra["meta"]) == `{"n":1e0}`, "copied extra bytes alias the original")
	testassert.Check(t, len(original.ToolCalls[0].Extra) == 1, "copied extra map aliases the original")
}

func TestMaterializedEmptyToolSelectionStaysEmpty(t *testing.T) {
	capture := testCapture()
	testassert.Require(t, len(capture.Tools) > 0, "the captured catalog must be nonempty")
	req := buildDispatchRequest(nil, RequestEvidence{BoundaryEntryID: testEntryID, Tools: fixtureDefinitions()})
	testassert.Require(t, len(req.Tools) == 0, "present-empty selection became the full catalog")
}

func TestAcceptedPureRequestIdentitiesRecordInOrder(t *testing.T) {
	capture := testCapture()
	for _, restored := range []bool{false, true} {
		view := newRequestView(capture, "", testEntryID, nil)
		testassert.NoError(t, view.captureStage(InvocationExecution{}))
		reasserted := capture.SystemPrompt
		view.recordAccepted(HookIdentity{ExtensionID: "ext.test", HookID: "pure.noop"}, RequestConsequence{Prompt: &reasserted})
		if restored {
			changed, baseline := "changed", capture.SystemPrompt
			view.recordAccepted(HookIdentity{ExtensionID: "ext.test", HookID: "pure.change"}, RequestConsequence{Prompt: &changed})
			view.recordAccepted(HookIdentity{ExtensionID: "ext.test", HookID: "pure.restore"}, RequestConsequence{Prompt: &baseline})
		}
		evidence := view.evidenceFor()
		want := []HookIdentity{{ExtensionID: "ext.test", HookID: "pure.noop"}}
		if restored {
			want = append(want, HookIdentity{ExtensionID: "ext.test", HookID: "pure.change"}, HookIdentity{ExtensionID: "ext.test", HookID: "pure.restore"})
		}
		testassert.Check(t, reflect.DeepEqual(evidence.HookIDs, want), "restored=%v contributions = %+v, want every accepted identity in order", restored, evidence.HookIDs)
		testassert.Check(t, evidence.Prompt == capture.SystemPrompt && evidence.Edits == nil && reflect.DeepEqual(fixtureDefinitionNames(evidence.Tools), []string{capture.Tools[0].Name}), "restored=%v evidence = %+v, want full captured view", restored, evidence)
	}
}

func TestArgumentRefusalDoesNotAcceptInvalidOriginal(t *testing.T) {
	spy := &hookToolSpy{}
	normalize := func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
		if string(call.Arguments) == "not json" {
			return nil, errors.New("invalid original")
		}
		return objectNormalize(ctx, call)
	}
	hook := ToolArgumentsHook{ID: "pure.refuse", ExtensionID: "ext.test", Mode: HookModePure, Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		if call.ID == "call-1" {
			return ToolArgumentsResult{}, errors.New("refused")
		}
		return ToolArgumentsResult{Arguments: model.CloneRaw(call.Arguments)}, nil
	}}
	exec := hookFixtureExecution(spy.tool, normalize, hook)
	first, second := testToolCall("call-1"), testToolCall("call-2")
	first.Arguments = json.RawMessage("not json")
	_, store, _, sessionID, results, err := freshExecutionToolCalls(t, testCapture(), exec, first, second)
	testassert.NoError(t, err)
	testassert.Require(t, results[0].Status == model.ResultError, "refusal = %+v", results[0])
	testassert.Require(t, results[1].Status == model.ResultSuccess, "later sibling = %+v", results[1])
	graph, gerr := validateFixture(t, store, sessionID)
	testassert.NoError(t, gerr)
	for _, entry := range graph.Entries {
		if entry.ToolResult != nil && entry.ToolResult.ToolCallID == first.ID {
			testassert.Require(t, len(entry.ToolResult.NormalizedArguments) == 0, "rejected raw bytes became accepted arguments")
		}
	}
	testassert.Require(t, len(spy.received()) == 1, "only the valid sibling may reach preparation")
}

func TestFinalArgumentHookCancellationPreventsPreparation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spy := &hookToolSpy{}
	hook := ToolArgumentsHook{ID: "pure.cancel", ExtensionID: "ext.test", Mode: HookModePure, Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
		cancel()
		return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":9}`)}, nil
	}}
	exec := hookFixtureExecution(spy.tool, objectNormalize, hook)
	call := testToolCall("call-1")
	_, store, _, sessionID, results, err := freshExecutionToolCallsCtx(t, ctx, testCapture(), exec, call)
	testassert.NoError(t, err)
	testassert.Check(t, len(spy.received()) == 0, "preparer ran after cancellation")
	testassert.Check(t, results[0].Status == model.ResultInterrupted, "result = %+v, want interruption", results[0])
	graph, gerr := validateFixture(t, store, sessionID)
	testassert.NoError(t, gerr)
	for _, entry := range graph.Entries {
		if entry.ToolResult != nil {
			testassert.Require(t, len(entry.ToolResult.NormalizedArguments) == 0, "interrupted before normalization carried effective arguments")
		}
	}
}

func TestEffectfulNoopKeepsSurvivingPureArgumentIdentity(t *testing.T) {
	spy := &hookToolSpy{}
	pure := ToolArgumentsHook{ID: "pure.repair", ExtensionID: "ext.test", Mode: HookModePure, Run: func(context.Context, model.ToolCall) (ToolArgumentsResult, error) {
		return ToolArgumentsResult{Arguments: json.RawMessage(`{"x":9}`)}, nil
	}}
	effect := ToolArgumentsHook{ID: "effect.noop", ExtensionID: "ext.test", Mode: HookModeEffectful, Run: func(ctx context.Context, call model.ToolCall) (ToolArgumentsResult, error) {
		return ToolArgumentsResult{Arguments: model.CloneRaw(call.Arguments)}, nil
	}}
	exec := hookFixtureExecution(spy.tool, objectNormalize, pure, effect)
	call := testToolCall("call-1")
	_, _, _, _, results, err := freshExecutionToolCalls(t, testCapture(), exec, call)
	testassert.NoError(t, err)
	want := []HookIdentity{{ExtensionID: pure.ExtensionID, HookID: pure.ID}, {ExtensionID: effect.ExtensionID, HookID: effect.ID}}
	testassert.Require(t, reflect.DeepEqual(results[0].HookIDs, want), "surviving contributors = %+v, want %+v", results[0].HookIDs, want)
}

func TestRequestEvidenceRequiresOrderedEffectfulIdentities(t *testing.T) {
	a := HookIdentity{ExtensionID: "ext.test", HookID: "effect.a"}
	b := HookIdentity{ExtensionID: "ext.test", HookID: "effect.b"}
	pure := HookIdentity{ExtensionID: "ext.test", HookID: "pure"}
	results := []graphEntry{
		{Envelope: Entry{ID: hexID(10), Sequence: 2}, HookResult: &hookResultEntry{ExtensionID: a.ExtensionID, HookID: a.HookID}},
		{Envelope: Entry{ID: hexID(11), Sequence: 3}, HookResult: &hookResultEntry{ExtensionID: b.ExtensionID, HookID: b.HookID}},
	}
	for _, tc := range []struct {
		name string
		ids  []HookIdentity
		seq  int64
		bad  bool
	}{
		{"complete", []HookIdentity{a, b}, 4, false},
		{"pure identities may interleave", []HookIdentity{pure, a, b}, 4, false},
		{"missing all", nil, 4, true},
		{"missing earlier", []HookIdentity{b}, 4, true},
		{"reversed", []HookIdentity{b, a}, 4, true},
		{"consumed prefix", []HookIdentity{a}, 3, false},
		{"future identity", []HookIdentity{a, b}, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &graphValidation{}
			err := v.validateEvidenceHookOrder(tc.ids, results, graphEntry{Envelope: Entry{Sequence: tc.seq}})
			testassert.Require(t, (err != nil) == tc.bad, "order validation = %v, want bad=%v", err, tc.bad)
		})
	}
}

func TestContributionMessageEmptyNameIsUnnamed(t *testing.T) {
	m, err := decodeMessageValue(json.RawMessage(`{"role":"user","name":"","content":[{"kind":"text","text":"hello"}]}`))
	testassert.NoError(t, err)
	testassert.Equal(t, m.Name, "")
}

func TestEvidenceBoundaryDoesNotPredateItsCheckpoint(t *testing.T) {
	inputID, checkpointID, ownerID := hexID(1), hexID(2), hexID(3)
	v := &graphValidation{sessionID: testSessionID, entryByID: map[string]graphEntry{
		inputID:      {Envelope: Entry{ID: inputID, Sequence: 1}, Input: &inputEntry{}},
		checkpointID: {Envelope: Entry{ID: checkpointID, Sequence: 2}, Compaction: &contextCheckpointEntry{BoundaryEntryID: inputID}},
	}}
	owner := graphEntry{Envelope: Entry{ID: ownerID, Sequence: 3}}
	evidence := &RequestEvidence{CheckpointEntryID: checkpointID, BoundaryEntryID: inputID}
	testassert.Require(t, v.validateEvidenceRefs("request", evidence, owner) != nil, "a covered pre-checkpoint boundary was accepted")
	evidence.BoundaryEntryID = checkpointID
	testassert.NoError(t, v.validateEvidenceRefs("request", evidence, owner))
}

// historicalProbe renders one stable fingerprint of a message list: role and
// text per message plus each tool call's raw argument bytes and marshalled
// tool-call extras, so a fixture can compare views byte-for-byte.
func historicalProbe(messages []model.Message) string {
	var sb strings.Builder
	for _, m := range messages {
		sb.WriteString(string(m.Role))
		sb.WriteString("/")
		sb.WriteString(m.TextContent())
		for _, tc := range m.ToolCalls {
			extra, _ := json.Marshal(tc.Extra)
			fmt.Fprintf(&sb, " [%s %s %s]", tc.ID, string(tc.Arguments), extra)
		}
	}
	return sb.String()
}

// TestRequestHookHistoricalToolCallMutationStaysLocal proves the actual
// pipeline's deep ownership of historical conversation state: a pure request
// hook that rewrites the nested ToolCalls (arguments, extras) and text of the
// messages it receives cannot reach the later effectful hook's materialized
// view, the actual model dispatch request, or the canonical-only maintenance
// summary input, and the committed assistant record keeps its original bytes.
func TestRequestHookHistoricalToolCallMutationStaysLocal(t *testing.T) {
	originalPair := string(model.RoleAssistant) + "/done [call-1 " + `{"x":1}` + ` {"keep":1}` + "]"
	zero := 0
	pairStream := streamOf(model.StreamDelta{
		HasChoice: true,
		Role:      "assistant",
		ContentFragments: []model.ContentFragment{{
			Position: zero,
			Kind:     model.PartText,
			Text:     "done",
		}},
		ToolFragments: []model.ToolCallFragment{{
			Position:         &zero,
			ID:               "call-1",
			Name:             "echo",
			ArgumentFragment: `{"x":1}`,
			Extra:            model.Extra{"keep": json.RawMessage(`1`)},
		}},
		FinishReason: "tool_calls",
	}, usageDelta(model.Usage{InputTokens: 3, CachedInputTokens: 1, OutputTokens: 2}))
	roundA := Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
		return InvocationExecution{
			Model: func(context.Context, model.Request, model.Extra) (model.Stream, error) { return pairStream, nil },
			LookupTool: fixtureToolLookup(func(ctx context.Context, call model.ToolCall) PreparedTool {
				return PreparedTool{Permissions: fixturePermission, Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "tool done"}}}
			}, objectNormalize),
		}, nil
	}}

	var mutatorSawOriginal string
	var checkerView string
	var dispatchView string
	var summaryInput []model.Message
	mutator := RequestHook{ID: "req.mutator", ExtensionID: "req-ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		mutatorSawOriginal = historicalProbe(in.Messages)
		for i := range in.Messages {
			m := &in.Messages[i]
			for j := range m.ToolCalls {
				m.ToolCalls[j].Arguments = json.RawMessage(`{"mutated":true}`)
				m.ToolCalls[j].Extra = model.Extra{"poisoned": json.RawMessage(`"leak"`)}
			}
			for k := range m.Content {
				m.Content[k].Text = "content mutated"
			}
		}
		return RequestConsequence{}, nil
	}}
	checker := RequestHook{ID: "req.checker", ExtensionID: "req-ext", Mode: HookModeEffectful, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		checkerView = historicalProbe(in.Messages)
		return RequestConsequence{}, nil
	}}
	roundB := Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
		return InvocationExecution{
			Model: func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
				dispatchView = historicalProbe(r.Messages)
				return streamOf(turnDelta("stop", "mid"), usageDelta(model.Usage{InputTokens: 13})), nil
			},
			LookupTool:   fixtureToolLookup(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize),
			RequestStage: fixtureRequestStage([]RequestHook{mutator, checker}...),
		}, nil
	}}
	roundC := compactionExecution(t, func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
		summaryInput = append([]model.Message(nil), r.Messages...)
		return summaryStream("M-hist", model.Usage{InputTokens: 1, OutputTokens: 1}), nil
	}, nil)

	store := emptyStore(t)
	preparedA := PreparedExecution{Capture: testCapture(), Open: func(context.Context, OperationAdmission) (Execution, error) {
		return roundA, nil
	}}
	preparedB := PreparedExecution{Capture: testCapture(), Open: func(context.Context, OperationAdmission) (Execution, error) {
		return roundB, nil
	}}
	preparedC := compactPrepared(testCapture(), roundC)
	queue := []PreparedExecution{preparedA, preparedB, preparedC}
	h := newTestHarness(t, store, func(context.Context, PreparationRequest) (PreparedExecution, error) {
		next := queue[0]
		queue = queue[1:]
		return next, nil
	})
	session, err := h.CreateSession(context.Background(), CreateSessionRequest{Workspace: "/tmp/works", Inputs: testInputs("coder")})
	testassert.NoError(t, err)
	sessionID := session.Identity.SessionID
	c := leaseCoordinator(t, h, sessionID)
	mustAdmitWithoutExecution(t, h, sessionID, testOpID, admissionContent("hello"))
	testassert.NoError(t, executeDirect(h, c, context.Background(), testOpID, preparedA))
	mustAdmitWithoutExecution(t, h, sessionID, "op-2", admissionContent("later"))
	testassert.NoError(t, executeDirect(h, c, context.Background(), "op-2", preparedB))

	// The mutating hook really received the seeded pair, then poisoned only
	// its own copy; the effectful hook's materialized view and the actual
	// dispatch still carry the committed bytes exactly.
	testassert.Require(t, strings.Contains(mutatorSawOriginal, originalPair), "mutator view = %q, want the seeded pair before mutating", mutatorSawOriginal)
	testassert.Require(t, strings.Contains(checkerView, originalPair), "checker view = %q, want the seeded pair byte-identical after the earlier hook's mutation", checkerView)
	testassert.Require(t, strings.Contains(dispatchView, originalPair), "dispatch view = %q, want the seeded pair byte-identical in the actual model request", dispatchView)

	// A later manual maintenance summarizes the canonical-only projection of
	// the same pair; the summary input stays original for every field the
	// maintenance snapshot carries (tool execution members are stripped).
	testassert.NoError(t, runManualCompact(t, h, c, sessionID, "compact-1"))
	testassert.Require(t, len(summaryInput) >= 2, "summary input = %+v, want the canonical projection of the seeded conversation", summaryInput)
	var transcript strings.Builder
	for _, m := range summaryInput {
		testassert.Require(t, len(m.ToolCalls) == 0, "summary input carries tool members: %+v", m)
		transcript.WriteString(m.TextContent())
	}
	testassert.Require(t, strings.Contains(transcript.String(), `"done"`) && strings.Contains(transcript.String(), `"tool done"`) && strings.Contains(transcript.String(), `keep`),
		"summary transcript = %q, want the seeded conversation's original bytes", transcript.String())
	testassert.Require(t, !strings.Contains(transcript.String(), "mutated") && !strings.Contains(transcript.String(), "poisoned") && !strings.Contains(transcript.String(), "leak"),
		"summary transcript leaked a hook mutation: %q", transcript.String())

	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	seededKept := false
	for i := range graph.Entries {
		assistant := graph.Entries[i].Assistant
		if assistant == nil || len(assistant.ToolCalls) == 0 {
			continue
		}
		call := assistant.ToolCalls[0]
		raw, derr := base64.StdEncoding.DecodeString(call.ArgumentsBase64)
		testassert.NoError(t, derr)
		testassert.Require(t, string(raw) == `{"x":1}` && string(call.Extra["keep"]) == `1` &&
			len(assistant.Content) > 0 && assistant.Content[0].Text == "done",
			"committed tool call = %+v, want the original bytes and extras preserved", call)
		seededKept = true
	}
	testassert.Require(t, seededKept, "the graph carries no committed assistant pair")
	{
		committed := hookGraphHookResults(t, store, sessionID)
		testassert.Require(t, len(committed) == 1 && committed[0].HookID == "req.checker" && committed[0].Status == hookSucceeded,
			"hook results = %+v, want only the effectful checker's own committed effect", committed)
	}
}

// TestRequestHookPureRestoreMatchesBaselineTransactions proves the D2 pure
// request boundary through the actual pipeline: a pure change restored to the
// captured original by a later pure hook performs exactly the durable mutation
// steps of the identical hookless round — no hook_result entries, no extra
// register or entry transactions — and the dispatch keeps the original prompt.
func TestRequestHookPureRestoreMatchesBaselineTransactions(t *testing.T) {
	drive := func(t *testing.T, hooks ...RequestHook) (int, int, string) {
		exec := requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			return completedTurnStream(), nil
		}, nil, hooks...)
		h, store, c, sessionID := newHookHarness(t, exec)
		steps := 0
		store.txHook = func(string) error { steps++; return nil }
		sent, err := runAdmittedExecution(t, h, c, context.Background(), testOpID, testCapture(), exec)
		testassert.NoError(t, err)
		prompt := ""
		for _, m := range sent.Messages {
			if m.Role == model.RoleSystem {
				prompt = m.TextContent()
			}
		}
		return steps, len(hookGraphHookResults(t, store, sessionID)), prompt
	}
	baseSteps, baseHooks, basePrompt := drive(t)
	var capturedOriginal string
	pureA := RequestHook{ID: "req.pure.one", ExtensionID: "req-ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		capturedOriginal = in.Prompt
		changed := "pure changed " + in.Prompt
		return RequestConsequence{Prompt: &changed}, nil
	}}
	pureB := RequestHook{ID: "req.pure.two", ExtensionID: "req-ext", Mode: HookModePure, Run: func(context.Context, RequestHookInput) (RequestConsequence, error) {
		restored := capturedOriginal // the captured original bytes, not an echo of the previous replacement
		return RequestConsequence{Prompt: &restored}, nil
	}}
	pureSteps, pureHooks, purePrompt := drive(t, pureA, pureB)
	testassert.Require(t, baseHooks == 0 && pureHooks == 0, "hook entries = baseline %d pure %d, want none", baseHooks, pureHooks)
	testassert.Require(t, pureSteps == baseSteps, "pure restore round performed %d durable mutation steps, want the baseline %d", pureSteps, baseSteps)
	testassert.Require(t, purePrompt == basePrompt && basePrompt != "", "dispatch prompt = %q, want the restored original %q", purePrompt, basePrompt)
}

// TestRequestHooksRunBeforeCanonicalOnlyMaintenance proves the installed-hook
// ordering through the full trigger pipeline: the strategy's summary request
// is shaped once by the maintenance invocation's own hooks, its model runs,
// the checkpoint commits, and only then the rebuilt conversation request is
// shaped once by the conversation invocation's hooks before its model. The
// summary request exposes its own leading system prompt — never a second copy
// of the configured summary instructions — and the durable entries commit in
// summary-hook → checkpoint → conversation-hook → assistant order.
func TestRequestHooksRunBeforeCanonicalOnlyMaintenance(t *testing.T) {
	seed := UsageCount{InputTokens: 13}
	h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
	var events []string
	var summaryConsumedPrompt, conversationConsumedPrompt string
	var summarySerialized, dispatchPrompt string
	var dispatchHooked int
	pure := RequestHook{ID: "req.pure", ExtensionID: "req-ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		events = append(events, "pure-"+string(in.Purpose))
		changed := "hooked " + in.Prompt
		return RequestConsequence{Prompt: &changed}, nil
	}}
	noop := RequestHook{ID: "req.effectful.noop", ExtensionID: "req-ext", Mode: HookModeEffectful, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		events = append(events, "effectful-"+string(in.Purpose))
		if in.Purpose == InvocationPurposeMaintenance {
			summaryConsumedPrompt = in.Prompt
		} else {
			conversationConsumedPrompt = in.Prompt
		}
		reasserted := in.Prompt // the exact value the hook received: an effectful no-op
		return RequestConsequence{Prompt: &reasserted}, nil
	}}
	exec := Execution{OpenInvocation: func(_ context.Context, request InvocationRequest) (InvocationExecution, error) {
		modelFn := func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
			events = append(events, "model-"+string(request.Purpose))
			for _, m := range r.Messages {
				if m.Role == model.RoleSystem {
					dispatchPrompt = m.TextContent()
				}
				dispatchHooked += strings.Count(m.TextContent(), "hooked")
			}
			return completedTurnStream(), nil
		}
		if request.Purpose == InvocationPurposeMaintenance {
			modelFn = func(_ context.Context, r model.Request, _ model.Extra) (model.Stream, error) {
				events = append(events, "model-"+string(request.Purpose))
				var sb strings.Builder
				for _, m := range r.Messages {
					sb.WriteString(m.TextContent())
				}
				summarySerialized = sb.String()
				return summaryStream("M-hooks", model.Usage{InputTokens: 1, OutputTokens: 1}), nil
			}
		}
		return InvocationExecution{
			Model:        modelFn,
			LookupTool:   fixtureToolLookup(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize),
			RequestStage: fixtureRequestStage([]RequestHook{pure, noop}...),
		}, nil
	}}
	testassert.NoError(t, runAutomaticMaintenance(t, h, c, exec))
	testassert.Require(t, reflect.DeepEqual(events, []string{
		"pure-maintenance", "effectful-maintenance", "model-maintenance",
		"pure-conversation", "effectful-conversation", "model-conversation",
	}), "pipeline events = %v, want the summary hooks and model before the rebuilt conversation hooks and model", events)
	// The summary request is shaped from the strategy's own leading prompt:
	// the configured instructions appear once and the hook contribution once.
	testassert.Require(t, strings.Count(summarySerialized, "hooked") == 1 && strings.Count(summarySerialized, "summarize") == 1,
		"maintenance input = %q, want one hooked strategy prompt over the configured instructions", summarySerialized)
	// The effectful summary no-op consumed the pure intermediate.
	testassert.Require(t, summaryConsumedPrompt == "hooked summarize", "summarized consumed prompt = %q, want the pure intermediate", summaryConsumedPrompt)
	// The conversation request was shaped after the rebuilt projection.
	testassert.Require(t, dispatchPrompt == conversationConsumedPrompt && strings.HasPrefix(dispatchPrompt, "hooked "), "dispatch prompt = %q, want the conversation hook contribution", dispatchPrompt)
	testassert.Require(t, dispatchHooked == 1, "dispatch carries the contribution %d times, want exactly once", dispatchHooked)
	committed := hookGraphHookResults(t, store, sessionID)
	testassert.Require(t, len(committed) == 2, "hook results = %+v, want one effectful summary hook and one effectful conversation hook", committed)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	var checkpoint *contextCheckpointEntry
	var answering *assistantEntry
	for _, entry := range graph.Entries {
		switch {
		case entry.Compaction != nil:
			checkpoint = entry.Compaction
		case entry.Assistant != nil:
			answering = entry.Assistant
		}
	}
	testassert.Require(t, checkpoint != nil && answering != nil && len(checkpoint.Facts) == 1, "missing checkpoint, assistant or summary producer")
	summaryRequestID := checkpoint.Facts[0].RequestID
	var summaryHookSeq, conversationHookSeq, checkpointSeq, assistantSeq int64 = -1, -1, -1, -1
	for _, entry := range graph.Entries {
		switch {
		case entry.HookResult != nil && entry.HookResult.RequestID == summaryRequestID:
			summaryHookSeq = entry.Envelope.Sequence
			testassert.Require(t, entry.HookResult.InputEvidence != nil && entry.HookResult.InputEvidence.Prompt == "hooked summarize",
				"summary hook consumed evidence = %+v, want the strategy prompt it actually received", entry.HookResult.InputEvidence)
		case entry.HookResult != nil && entry.HookResult.RequestID == answering.RequestID:
			conversationHookSeq = entry.Envelope.Sequence
			testassert.Require(t, entry.HookResult.InputEvidence != nil && len(entry.HookResult.InputEvidence.HookIDs) == 1 &&
				entry.HookResult.InputEvidence.HookIDs[0].HookID == "req.pure",
				"conversation hook consumed evidence = %+v, want the pure intermediate it consumed", entry.HookResult.InputEvidence)
		case entry.Compaction != nil:
			checkpointSeq = entry.Envelope.Sequence
		case entry.Assistant != nil && entry.Assistant.RequestID == answering.RequestID:
			assistantSeq = entry.Envelope.Sequence
		}
	}
	testassert.Require(t, summaryHookSeq >= 0 && checkpointSeq >= 0 && conversationHookSeq >= 0 && assistantSeq >= 0 &&
		summaryHookSeq < checkpointSeq && checkpointSeq < conversationHookSeq && conversationHookSeq < assistantSeq,
		"committed sequences = summary hook %d checkpoint %d conversation hook %d assistant %d", summaryHookSeq, checkpointSeq, conversationHookSeq, assistantSeq)
	// Maintenance evidence names only the frozen canonical references; the
	// conversation request did not exist when it committed.
	testassert.Require(t, checkpoint.Evidence != nil && len(checkpoint.Evidence.HookIDs) == 0 && checkpoint.Evidence.Prompt == "" && len(checkpoint.Evidence.Tools) == 0,
		"checkpoint evidence = %+v, want only the frozen canonical references", checkpoint.Evidence)
	// The conversation request's final evidence carries the accepted chain.
	testassert.Require(t, answering.Evidence != nil && len(answering.Evidence.HookIDs) == 2 &&
		answering.Evidence.HookIDs[0].HookID == "req.pure" && answering.Evidence.HookIDs[1].HookID == "req.effectful.noop",
		"assistant evidence = %+v, want both accepted conversation hook identities", answering.Evidence)
}

// maintenanceHookedRound runs one hooked real automatic-maintenance round with
// two effectful request hooks and returns the storage the durable rows live in
// plus how many times the conversation transport ran. A non-nil summaryErr
// fails the maintenance summary; a non-nil openerErr fails the maintenance
// OpenInvocation before the summary effect is ever attempted.
func maintenanceHookedRound(t *testing.T, summaryErr, openerErr error) (*Harness, *graphStorage, string, int) {
	t.Helper()
	seed := UsageCount{InputTokens: 13}
	h, store, c, sessionID := seededTriggerHarness(t, triggerUsageCapture(4), &seed)
	e1 := RequestHook{ID: "req.effectful.one", ExtensionID: "req-ext", Mode: HookModeEffectful, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		prompt := "effect one " + in.Prompt
		return RequestConsequence{Prompt: &prompt}, nil
	}}
	e2 := RequestHook{ID: "req.effectful.two", ExtensionID: "req-ext", Mode: HookModeEffectful, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		prompt := "effect two " + in.Prompt
		return RequestConsequence{Prompt: &prompt}, nil
	}}
	conversationCalls := 0
	exec := Execution{OpenInvocation: func(_ context.Context, request InvocationRequest) (InvocationExecution, error) {
		modelFn := func(context.Context, model.Request, model.Extra) (model.Stream, error) {
			conversationCalls++
			return completedTurnStream(), nil
		}
		if request.Purpose == InvocationPurposeMaintenance {
			if openerErr != nil {
				return InvocationExecution{}, openerErr
			}
			modelFn = func(context.Context, model.Request, model.Extra) (model.Stream, error) {
				if summaryErr != nil {
					return nil, summaryErr
				}
				return summaryStream("M-reconcile", model.Usage{InputTokens: 1, OutputTokens: 1}), nil
			}
		}
		return InvocationExecution{
			Model:        modelFn,
			LookupTool:   fixtureToolLookup(func(context.Context, model.ToolCall) PreparedTool { return PreparedTool{} }, objectNormalize),
			RequestStage: fixtureRequestStage([]RequestHook{e1, e2}...),
		}, nil
	}}
	_ = runAutomaticMaintenance(t, h, c, exec)
	return h, store, sessionID, conversationCalls
}

// mutateStoredEvidenceHookIDs rewrites the hook identities recorded in one
// stored entry payload's consumed evidence, simulating durable evidence that
// no longer matches the committed effect executions it names its view from.
func mutateStoredEvidenceHookIDs(t *testing.T, store *graphStorage, sessionID string, kind EntryKind, hookIDs json.RawMessage) {
	t.Helper()
	entries := store.entries[sessionID]
	for i := range entries {
		if entries[i].Kind != kind {
			continue
		}
		var obj map[string]json.RawMessage
		testassert.NoError(t, json.Unmarshal(entries[i].Payload, &obj))
		var evidence map[string]json.RawMessage
		testassert.NoError(t, json.Unmarshal(obj["evidence"], &evidence))
		evidence["hook_ids"] = hookIDs
		evidenceRaw, err := json.Marshal(evidence)
		testassert.NoError(t, err)
		obj["evidence"] = evidenceRaw
		payload, err := json.Marshal(obj)
		testassert.NoError(t, err)
		entries[i].Payload = payload
		return
	}
	t.Fatalf("no stored %s entry to mutate", kind)
}

// removeStoredMember drops one member from a stored entry payload of the
// given kind, simulating a durable record written without it.
func removeStoredMember(t *testing.T, store *graphStorage, sessionID string, kind EntryKind, member string) {
	t.Helper()
	entries := store.entries[sessionID]
	for i := range entries {
		if entries[i].Kind != kind {
			continue
		}
		var obj map[string]json.RawMessage
		testassert.NoError(t, json.Unmarshal(entries[i].Payload, &obj))
		if _, ok := obj[member]; !ok {
			t.Fatalf("stored %s entry carries no %s member", kind, member)
		}
		delete(obj, member)
		payload, err := json.Marshal(obj)
		testassert.NoError(t, err)
		entries[i].Payload = payload
		return
	}
	t.Fatalf("no stored %s entry", kind)
}

// TestAutomaticMaintenanceEvidenceNamesFrozenInputAndSummaryProducer proves
// the new maintenance boundary: the durable checkpoint evidence names only
// the frozen canonical input references — no conversation request existed
// before the procedure ran — while the summary request's own effectful hook
// executions name the per-plan producer recorded on the maintenance facts.
func TestAutomaticMaintenanceEvidenceNamesFrozenInputAndSummaryProducer(t *testing.T) {
	_, store, sessionID, _ := maintenanceHookedRound(t, nil, nil)
	graph, err := validateFixture(t, store, sessionID)
	testassert.NoError(t, err)
	comp := compactedEntryOf(t, graph)
	testassert.Require(t, len(comp.Facts) == 1 && comp.Facts[0].RequestID != "", "checkpoint facts = %+v, want one per-plan producer", comp.Facts)
	testassert.Require(t, comp.Evidence != nil && comp.Evidence.BoundaryEntryID != "", "checkpoint evidence = %+v, want the frozen canonical boundary", comp.Evidence)
	testassert.Require(t, len(comp.Evidence.HookIDs) == 0 && comp.Evidence.Prompt == "" && len(comp.Evidence.Tools) == 0,
		"checkpoint evidence = %+v, want only the frozen canonical references", comp.Evidence)
	var summaryHooks int
	for _, entry := range graph.Entries {
		if entry.HookResult == nil || entry.HookResult.RequestID != comp.Facts[0].RequestID {
			continue
		}
		summaryHooks++
	}
	testassert.Equal(t, summaryHooks, 2) // the two effectful hooks ran on the summary producer's own request
	// The failure terminal keeps the same frozen-input evidence and facts.
	_, failStore, failSession, _ := maintenanceHookedRound(t, errMaintenanceSummary, nil)
	failGraph, gerr := validateFixture(t, failStore, failSession)
	testassert.NoError(t, gerr)
	settlement := settlementEntryOf(t, failGraph)
	testassert.Require(t, len(settlement.Facts) > 0 && settlement.Evidence != nil, "failure settlement = %+v, want the retained facts and frozen input", settlement)
	testassert.Require(t, len(settlement.Evidence.HookIDs) == 0 && settlement.Evidence.Prompt == "" && len(settlement.Evidence.Tools) == 0,
		"failure evidence = %+v, want only the frozen canonical references", settlement.Evidence)
}
