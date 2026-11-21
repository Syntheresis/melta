package harness

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/agent"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

func TestRequestHookChainedEdits(t *testing.T) {
	zero := 0
	hook := func(id string, mode HookMode, run func(RequestHookInput) RequestConsequence) RequestHook {
		return RequestHook{ID: id, ExtensionID: "req-ext", Mode: mode, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
			return run(in), nil
		}}
	}
	retain := func(content []model.ContentPart) RequestConsequence {
		return RequestConsequence{Edits: []RequestMessageEdit{{Position: &zero, Content: content}}}
	}
	first := hook("req.rename", HookModePure, func(in RequestHookInput) RequestConsequence {
		testassert.Require(t, *in.MessagePositions[0] == 0, "positions: %v", in.MessagePositions)
		return retain(admissionContent("edited"))
	})
	second := hook("req.retain", HookModeEffectful, func(in RequestHookInput) RequestConsequence {
		testassert.Equal(t, in.Messages[0].TextContent(), "edited")
		in.Messages[0].Content[0].Text = "mutation"
		return retain(nil)
	})
	third := hook("req.check", HookModePure, func(in RequestHookInput) RequestConsequence {
		testassert.Equal(t, in.Messages[0].TextContent(), "edited")
		return RequestConsequence{}
	})
	exec := requestHookFixtureExecution(func(context.Context, model.Request, model.Extra) (model.Stream, error) {
		return completedTurnStream(), nil
	}, nil, first, second, third)
	h, store, c, sessionID := newHookHarness(t, exec)
	sent, err := runRequestHookRound(t, h, c, exec, testCapture())
	testassert.NoError(t, err)
	testassert.Equal(t, sent.Messages[1].TextContent(), "edited")
	committed := hookGraphHookResults(t, store, sessionID)
	testassert.Require(t, len(committed) == 1 && reflect.DeepEqual(committed[0].Consequence.Edits[0].Content, admissionContent("edited")), "effectful consequence: %+v", committed)
	cold := compositionAssistant(t, store, sessionID).Evidence
	testassert.Require(t, len(cold.Edits) == 1 && *cold.Edits[0].Position == 0 && reflect.DeepEqual(cold.Edits[0].Content, admissionContent("edited")), "cold evidence: %+v", cold)
}

func TestRequestHookReorderAndDefinitionChain(t *testing.T) {
	first := RequestHook{ID: "req.reorder", ExtensionID: "ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		in.Tools[0].Description = "redefined"
		return RequestConsequence{Edits: []RequestMessageEdit{{Position: new(2)}, {Position: new(0)}}, Tools: in.Tools}, nil
	}}
	second := RequestHook{ID: "req.select", ExtensionID: "ext", Mode: HookModePure, Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
		testassert.Equal(t, len(in.Messages), 2)
		testassert.Equal(t, *in.MessagePositions[0], 2)
		testassert.Equal(t, *in.MessagePositions[1], 0)
		testassert.Equal(t, in.Tools[0].Description, "redefined")
		return RequestConsequence{Edits: appendRequestFixture(in, model.Message{Role: model.RoleUser}), Tools: in.Tools[:1]}, nil
	}}
	view := newRequestView(testCapture(), "", "", []model.Message{{Role: model.RoleUser}, {Role: model.RoleUser}, {Role: model.RoleUser}})
	testassert.NoError(t, view.captureStage(InvocationExecution{RequestStage: fixtureRequestStage(first, second)}))
	driver := &executionDriver{}
	testassert.NoError(t, driver.runRequestHooks(context.Background(), &openedInvocation{}, "request", view))
	evidence, err := acceptRequestEvidence(view.evidenceFor())
	testassert.NoError(t, err)
	sent := buildDispatchRequest(view.canonical, evidence)
	testassert.Equal(t, sent.Tools[0].Description, "redefined")
	testassert.Equal(t, len(evidence.Edits), 3)
	for _, positions := range [][]int{{-1}, {3}, {0, 0}} {
		edits := make([]RequestMessageEdit, 0, len(positions))
		for _, position := range positions {
			edits = append(edits, RequestMessageEdit{Position: &position})
		}
		if _, err := view.acceptConsequence(RequestConsequence{Edits: edits}); err == nil {
			t.Errorf("accepted positions %v", positions)
		}
	}
}

func TestRequestHookStageChangesBetweenRequests(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wantErr string
		models  int
		stages  int
		hooks   int
	}{
		{"late tool accepted", "", 2, 2, 2},
		{"admission-only tool refused", `tool name "echo" is outside the stage tool catalog`, 1, 2, 2},
		{"missing initial name refused", `advertisement names unregistered tool "late"`, 0, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls, stages, hooks int
			echo := testCapture().Tools[0]
			late := echo
			late.Name = "late"
			exec := Execution{OpenInvocation: func(context.Context, InvocationRequest) (InvocationExecution, error) {
				inv := invocationExecution(func(_ context.Context, req model.Request, _ model.Extra) (model.Stream, error) {
					calls++
					if calls > tc.models {
						return completedTurnStream(), nil
					}
					want := []string{"echo", "late"}[calls-1]
					testassert.Require(t, len(req.Tools) == 1 && req.Tools[0].Name == want, "dispatch %d: %+v", calls, req.Tools)
					if calls == 1 {
						return completedTurnStream(testToolCall("call-1")), nil
					}
					return completedTurnStream(), nil
				}, func(_ context.Context, call model.ToolCall) PreparedTool {
					return PreparedTool{Immediate: &ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "ok"}}}
				})
				inv.RequestStage = func() RequestStage {
					stages++
					def := []model.ToolDefinition{echo, late}[stages-1]
					stage := RequestStage{Catalog: []model.ToolDefinition{def}, InitialToolNames: []string{}}
					if tc.models == 0 {
						stage.InitialToolNames = []string{late.Name}
					}
					stage.Hooks = []RequestHook{{ID: "req." + def.Name, ExtensionID: "req-ext", Mode: HookModePure,
						Run: func(_ context.Context, in RequestHookInput) (RequestConsequence, error) {
							hooks++
							testassert.Require(t, len(in.Tools) == 0 && len(in.Catalog) == 1 && in.Catalog[0].Name == def.Name, "catalog/advertisement: %+v / %+v", in.Catalog, in.Tools)
							if stages == 2 && tc.models == 1 {
								return RequestConsequence{Tools: []model.ToolDefinition{echo}}, nil
							}
							return RequestConsequence{Tools: []model.ToolDefinition{def}}, nil
						}}}
					return stage
				}
				return inv, nil
			}}
			h, store, c, sessionID := newHookHarness(t, exec)
			_, err := runRequestHookRound(t, h, c, exec, testCapture())
			testassert.Require(t, calls == tc.models && stages == tc.stages && hooks == tc.hooks, "counts: model=%d stage=%d hook=%d", calls, stages, hooks)
			if tc.wantErr == "" {
				testassert.NoError(t, err)
			} else {
				testassert.Require(t, err != nil && strings.Contains(err.Error(), tc.wantErr), "round = %v, want %q", err, tc.wantErr)
			}
			_, err = validateFixture(t, store, sessionID)
			testassert.NoError(t, err)
		})
	}
}

func TestRequestBodyAttemptIsolation(t *testing.T) {
	params := model.Extra{"temperature": json.RawMessage(`0.2`), "": json.RawMessage(`true`)}
	calls := 0
	res := runModelAttempts(context.Background(), model.Request{}, params, func(_ context.Context, _ model.Request, got model.Extra) (model.Stream, error) {
		calls++
		if string(got["temperature"]) != "0.2" || string(got[""]) != "true" || got["leak"] != nil {
			t.Errorf("attempt %d received %s", calls, got)
		}
		got["temperature"][0] = '9'
		got["leak"] = json.RawMessage(`true`)
		if calls == 1 {
			return nil, errors.New("retry")
		}
		return completedTurnStream(), nil
	}, func(stream model.Stream) (model.Output, error) {
		return agent.Assemble(context.Background(), testModelRef(), stream)
	}, func(_ error, failed int) (time.Duration, bool) { return 0, failed == 1 })
	if res.kind != attemptSettled || calls != 2 || string(params["temperature"]) != "0.2" || params["leak"] != nil {
		t.Fatalf("kind=%v calls=%d retained=%s", res.kind, calls, params)
	}
}

func TestRequestHookAuthenticToolPairings(t *testing.T) {
	assistant := func() model.Message {
		return model.Message{Role: model.RoleAssistant, Source: testCapture().Model, ToolCalls: []model.ToolCall{testToolCall("reused")}}
	}
	result := model.Message{Role: model.RoleTool, ToolCallID: "reused"}
	frozen := []model.Message{assistant(), {Role: model.RoleUser}, result, assistant(), result}
	for _, tc := range []struct {
		name      string
		positions []int
		pass      bool
	}{
		{"unchanged with intervening context", []int{0, 1, 2, 3, 4}, true},
		{"reordered complete exchanges", []int{3, 4, 0, 1, 2}, true},
		{"missing", []int{0}, false},
		{"orphan", []int{2}, false},
		{"cross-wired", []int{0, 4}, false},
		{"reopened before result", []int{0, 3, 2, 4}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var edits []RequestMessageEdit
			for _, p := range tc.positions {
				edits = append(edits, RequestMessageEdit{Position: &p})
			}
			v := newRequestView(testCapture(), "", "", frozen)
			_, err := v.acceptConsequence(RequestConsequence{Edits: edits})
			if (err == nil) != tc.pass {
				t.Fatalf("accept positions %v: %v", tc.positions, err)
			}
		})
	}
}

func TestRequestHookDroppedPositionAndEmptyContent(t *testing.T) {
	v := newRequestView(testCapture(), "", "", []model.Message{{Role: model.RoleUser, Content: admissionContent("original")}})
	p := 0
	apply := func(edits []RequestMessageEdit) {
		t.Helper()
		con, err := v.acceptConsequence(RequestConsequence{Edits: edits})
		testassert.NoError(t, err)
		v.applyConsequence(con)
	}
	apply([]RequestMessageEdit{{Position: &p, Content: []model.ContentPart{}}})
	apply([]RequestMessageEdit{{Position: &p}})
	testassert.Equal(t, v.currentCanonical()[0].TextContent(), "")
	apply([]RequestMessageEdit{})
	testassert.Equal(t, len(v.currentCanonical()), 0)
	apply([]RequestMessageEdit{{Position: &p}})
	testassert.Equal(t, v.currentCanonical()[0].TextContent(), "original")
}

func TestRequestHookRejectsPrivateAndReservedBodyParameters(t *testing.T) {
	v := newRequestView(testCapture(), "", "", nil)
	for _, name := range append(model.ReservedBodyKeys(), "_melta_private") {
		if _, err := v.acceptConsequence(RequestConsequence{Parameters: model.Extra{name: json.RawMessage(`1`)}}); err == nil {
			t.Errorf("accepted %q", name)
		}
	}
}

func TestRequestHookColdEditPresence(t *testing.T) {
	_, err := decodeRequestEvidence(json.RawMessage(`{"boundary_entry_id":"","prompt":"","tools":[],"edits":[{"position":null}]}`))
	testassert.Require(t, err != nil && strings.Contains(err.Error(), "position"), "null position: %v", err)
	evidence, err := decodeRequestEvidence(json.RawMessage(`{"boundary_entry_id":"","prompt":"","tools":[],"edits":[{"position":0,"content":[]}]}`))
	testassert.NoError(t, err)
	edit := evidence.Edits[0]
	testassert.Require(t, edit.Position != nil && *edit.Position == 0 && edit.Content != nil && len(edit.Content) == 0, "cold edit: %#v", edit)
}

func TestPartialCutRequestEvidenceIncludesPreCheckpointSignal(t *testing.T) {
	fixture := partialCutFixture()
	fixture.ops[0].Admission.Execution.Strategy = fixtureStrategyCapture()
	store := fixture.storage(t)
	h := newTestHarness(t, store, nil)
	c := leaseCoordinator(t, h, testSessionID)
	var got []model.Message
	exec := purposeExecution(func(_ context.Context, req model.Request, _ model.Extra) (model.Stream, error) {
		got = req.Messages
		return completedTurnStream(), nil
	}, forbiddenMaintenanceModel(t))
	exec.ResolveStrategy = fixedStrategyResolver(fixtureStrategyBinding(&noneStrategy{}))
	exec = requestHookExecution(exec, RequestHook{
		ID: "req.edit", ExtensionID: "req-ext", Mode: HookModePure,
		Run: func(_ context.Context, input RequestHookInput) (RequestConsequence, error) {
			checkpoint := len(validCompactionEntry(testOpID).Messages)
			preCheckpoint := checkpoint + 1
			testassert.Require(t, strings.Contains(input.Messages[preCheckpoint].TextContent(), fixture.entries[2].signal.Content),
				"pre-checkpoint signal missing from frozen request: %+v", input.Messages)
			last := len(input.Messages) - 1
			return RequestConsequence{Edits: []RequestMessageEdit{{Position: &last}, {Position: &checkpoint}, {Position: &preCheckpoint, Content: admissionContent("signal edited")}}}, nil
		},
	})
	testassert.NoError(t, runAutomaticMaintenance(t, h, c, exec))
	testassert.Equal(t, got[len(got)-1].TextContent(), "signal edited")
	graph, err := validateFixture(t, store, testSessionID)
	testassert.NoError(t, err)
	for i := len(graph.Entries) - 1; i >= 0; i-- {
		if a := graph.Entries[i].Assistant; a != nil {
			testassert.Require(t, a.Evidence.CheckpointEntryID == hexID(3) && a.Evidence.BoundaryEntryID == hexID(6), "request evidence: %+v", a.Evidence)
			outOfBounds := len(validCompactionEntry(testOpID).Messages) + 3
			a.Evidence.Edits[0].Position = &outOfBounds
			err := validateGraphInvariants(testSessionID, graph)
			testassert.Require(t, err != nil && errors.Is(err, ErrCorrupt), "accepted out-of-bounds cold evidence: %v", err)
			return
		}
	}
	t.Fatal("missing assistant evidence")
}
