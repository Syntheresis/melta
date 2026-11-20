package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// Native compaction rows over the real preparation: the compaction observer
// oracle through configured conversation and compact endpoints, the captured
// compaction configuration through real hooks, the raw durable-state builders
// the paging and recovery rows seed with, and the exported-surface pin.

// compactObservationConfigDocument configures the conversation model with a
// window whose trigger threshold the seed turn's reported usage overflows,
// and the top-level compaction model as a distinct catalog identity, so the
// wire distinguishes the transports by model ID.
func compactObservationConfigDocument(endpoint string) string {
	return `{"providers":{"prov":` + nominalProvMember(endpoint, "",
		`{"m":{"name":"M","context_window":8192,"max_output_tokens":4096},`+
			`"k":{"name":"K","context_window":32768,"max_output_tokens":4096,"usage_in_stream":true}}`) + `},
		"compaction":{"model":"prov/k"}}`
}

// compactObservationAgentsDocument selects the conversation model for the
// working type.
const compactObservationAgentsDocument = `{
	"tight": {"model": "prov/m", "system_prompt": "simple"}
}`

// TestCompactLifecycleNativeCompactionObservation keeps the Melta observer
// oracle through the native pipeline: the first turn's reported conversation
// usage overflows the captured window, the next admission's checkpoint runs
// one automatic compaction whose summary request reaches the compact model's
// transport — distinguished by its model ID, with the captured compact
// prompt and no tools — the committed summary turns the conversation
// projection, and the observation bus carries only well-scoped facts: the
// session invalidation at session granularity and the text progress at the
// real admitted Operations.
func TestCompactLifecycleNativeCompactionObservation(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		e := newOwnerEnv(t)
		writeServiceFile(t, e.configPath, compactObservationConfigDocument(e.server.URL))
		writeAgentDefinitions(t, e.configPath, compactObservationAgentsDocument)
		e.fixtureStore(store)
		r, err := e.open(context.Background())
		testassert.NoError(t, err)
		ctx := context.Background()

		e.server.setScript(func(_ context.Context, body string) []string {
			if !strings.Contains(body, `"model":"k"`) && !strings.Contains(body, `"model": "k"`) {
				if lastUserText(body) == "first question" {
					// The seed report: {5000,10} overflows the window 8192
					// minus the reserve 4096.
					return append(textTurnEvents("seeded answer"),
						`{"usage":{"prompt_tokens":5000,"completion_tokens":10,"total_tokens":5010}}`)
				}
				return nil // conversation turns: the default completed text turn
			}
			return append(textTurnEvents("native summary one"),
				`{"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`)
		})

		sub, err := r.Subscribe(256)
		testassert.NoError(t, err)
		session, err := r.createSession(ctx, e.dataDir, "tight")
		testassert.NoError(t, err)
		sessionID := session.Identity.SessionID

		submitThroughRuntime(t, r, sessionID, "op-1", "first question")
		awaitOperation(t, r, sessionID, "op-1", harness.OperationSuccess) // the seed turn commits the report
		submitThroughRuntime(t, r, sessionID, "op-2", "the overflowing checkpoint")
		op2 := awaitOperation(t, r, sessionID, "op-2", harness.OperationSuccess)
		testassert.Require(t, op2.State.Status == harness.OperationSuccess, "overflowing operation = %+v, want the compacted success", op2.State)
		submitConvergedThroughRuntime(t, r, sessionID, "op-3", "second question", harness.OperationSuccess)
		testassert.NoError(t, r.Close(ctx))

		// The wire: the seed conversation request, one compact summary on
		// the compact model, then the two conversation requests against the
		// summary projection.
		var compactSummary string
		var conversation []string
		for i := 0; i < e.server.requests(); i++ {
			body := e.server.bodyAt(i)
			doc := decodeWireChatBody(t, body)
			if doc.Model == "k" {
				compactSummary = body
				continue
			}
			testassert.Require(t, doc.Model == "m", "wire request %d names model %q, want one of the configured identities", i, doc.Model)
			conversation = append(conversation, body)
		}
		testassert.Require(t, compactSummary != "", "the compaction never reached the compact model's transport")
		testassert.Equal(t, len(conversation), 3)
		seed := decodeWireChatBody(t, conversation[0])
		{
			roles := wireRoles(seed)
			testassert.Require(t, slices.Equal(roles, []string{"system", "user"}), "seed request roles = %v, want the prompt and the first message", roles)
		}
		summary := decodeWireChatBody(t, compactSummary)
		testassert.Require(t, len(summary.Tools) == 0 && len(summary.Messages) != 0 && summary.Messages[0].Role == "system", "got %v, want %v; compact summary request = %+v, want the leading compact system prompt", len(summary.Tools), 0, summary.Messages)
		var systemText string
		{
			err := json.Unmarshal(summary.Messages[0].Content, &systemText)
			testassert.Require(t, err == nil && systemText == op2.Admission.Execution.Compact.SystemPrompt, "compact summary system = %q (err %v), want the captured compact prompt %q", systemText, err, op2.Admission.Execution.Compact.SystemPrompt)
		}
		testassert.Require(t, op2.Admission.Execution.Compact.Model == (model.ModelRef{Provider: "prov", Model: "k"}), "captured compact model = %+v, want the configured prov/k", op2.Admission.Execution.Compact.Model)
		first := decodeWireChatBody(t, conversation[1])
		{
			roles := wireRoles(first)
			testassert.Require(t, slices.Equal(roles, []string{"system", "assistant"}), "post-compaction request roles = %v, want the prompt and the summary alone", roles)
		}
		var summaryText string
		if len(first.Messages) > 1 {
			_ = json.Unmarshal(first.Messages[1].Content, &summaryText)
		}
		testassert.Require(t, strings.Contains(summaryText, "native summary one"), "post-compaction assistant = %q, want the committed summary", summaryText)
		second := decodeWireChatBody(t, conversation[2])
		{
			roles := wireRoles(second)
			testassert.Require(t, slices.Equal(roles, []string{"system", "assistant", "assistant", "user"}), "second request roles = %v, want the prompt, the summary, the first closing turn and the new message", roles)
		}

		// The durable state: exactly one committed compaction entry owned by
		// the overflowing Operation.
		entries, err := store.ReadEntries(ctx, sessionID, 0)
		testassert.NoError(t, err)
		compactions := 0
		for _, entry := range entries {
			if entry.Kind != harness.EntryCompaction {
				continue
			}
			compactions++
			var wire struct {
				OperationID string `json:"operation_id"`
				Messages    []struct {
					Content []struct {
						Text string `json:"text"`
					} `json:"content"`
				} `json:"messages"`
			}
			testassert.NoError(t, json.Unmarshal(entry.Payload, &wire))
			wireSummary := ""
			if len(wire.Messages) > 0 && len(wire.Messages[0].Content) > 0 {
				wireSummary = wire.Messages[0].Content[0].Text
			}
			testassert.Require(t, wire.OperationID == "op-2" && wireSummary == "[Previous conversation summary]\n\nnative summary one\n\n[End of summary. Continue from here.]", "compaction entry = %+v, want op-2's committed summary", wire)
		}
		testassert.Equal(t, compactions, 1)

		// The observation set: every session invalidation names the real
		// Session, every text progress the real running Operation, and no
		// other fact kind fires.
		sawInvalidation, sawDelta := false, false
		for event := range sub.Events() {
			switch eventKind(t, event) {
			case "session_changed":
				body, err := event.AsSessionChangedEvent()
				testassert.NoError(t, err)
				scope, err := body.Scope.AsSessionScope()
				testassert.NoError(t, err)
				testassert.Equal(t, scope.SessionId, sessionID)
				sawInvalidation = true
			case "text_delta":
				testassert.NoError(t, assertProgressScopedToOperations(t, event, sessionID, "op-1", "op-2", "op-3"))
				sawDelta = true
			case "scope_opened", "scope_closed", "warning_changed", "configuration_changed":
				// scope lifecycle and publication facts stay allowed beside the passive set
			default:
				t.Fatalf("unexpected event %s", eventJSON(t, event))
			}
		}
		testassert.Require(t, sawInvalidation && sawDelta, "observation set: invalidation %v delta %v, want both", sawInvalidation, sawDelta)
	})
}

// TestCompactLifecycleNativeCompactionAgentScopeCount checks request hooks run
// once for the conversation request and once for the automatic summary request
// that conversation triggers.
func TestCompactLifecycleNativeCompactionAgentScopeCount(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		e := newOwnerEnv(t)
		writeServiceFile(t, e.configPath, compactObservationConfigDocument(e.server.URL))
		writeAgentDefinitions(t, e.configPath, compactObservationAgentsDocument)
		enableFixturePlugin(t, e.configPath, "probe-agent")
		var agOpens atomic.Int64
		e.fixtureStore(store)
		r, err := e.open(context.Background(),
			shortScopeProbe("probe-agent", &agOpens))
		testassert.NoError(t, err)
		ctx := context.Background()

		e.server.setScript(func(_ context.Context, body string) []string {
			if !strings.Contains(body, `"model":"k"`) && !strings.Contains(body, `"model": "k"`) {
				if lastUserText(body) == "first question" {
					// The seed report: {5000,10} overflows the window 8192
					// minus the reserve 4096.
					return append(textTurnEvents("seeded answer"),
						`{"usage":{"prompt_tokens":5000,"completion_tokens":10,"total_tokens":5010}}`)
				}
				return nil // conversation turns: the default completed text turn
			}
			return append(textTurnEvents("native summary one"),
				`{"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`)
		})

		session, err := r.createSession(ctx, e.dataDir, "tight")
		testassert.NoError(t, err)
		sessionID := session.Identity.SessionID

		submitThroughRuntime(t, r, sessionID, "op-1", "first question")
		awaitOperation(t, r, sessionID, "op-1", harness.OperationSuccess) // the seed turn commits the overflowing report

		before := agOpens.Load()
		submitThroughRuntime(t, r, sessionID, "op-2", "the overflowing checkpoint")
		op2 := awaitOperation(t, r, sessionID, "op-2", harness.OperationSuccess)
		testassert.Require(t, op2.State.Status == harness.OperationSuccess, "overflowing operation = %+v, want the compacted success", op2.State)
		after := agOpens.Load()
		testassert.Require(t, after-before == 2, "request hooks during the pressure-triggered submission = %d, want the conversation request and its automatic summary request", after-before)

		submitConvergedThroughRuntime(t, r, sessionID, "op-3", "second question", harness.OperationSuccess)
		testassert.NoError(t, r.Close(ctx))
	})
}

// wireRoles returns one wire request's message roles in order.
func wireRoles(doc wireChatBody) []string {
	out := make([]string, 0, len(doc.Messages))
	for _, msg := range doc.Messages {
		out = append(out, msg.Role)
	}
	return out
}

// assertProgressScopedToOperations checks one progress event carries the
// real Session and one of the named Operations.
func assertProgressScopedToOperations(t *testing.T, event Event, sessionID string, operationIDs ...string) error {
	t.Helper()
	scope := progressEventScope(t, event)
	if scopeKind(t, scope) != "operation" {
		return errors.New("progress carries a non-operation scope")
	}
	body, err := scope.AsOperationScope()
	testassert.NoError(t, err)
	if body.SessionId != sessionID {
		return errors.New("progress carries a foreign session identity")
	}
	for _, id := range operationIDs {
		if body.OperationId == id {
			return nil
		}
	}
	return fmt.Errorf("progress carries operation %q outside %v", body.OperationId, operationIDs)
}

// rawAdmissionValues carries the authored execution values of one raw
// message-kind admission seed.
type rawAdmissionValues struct {
	agentType     string
	model         usageWireRef
	outputReserve int
	systemPrompt  string
	tools         string
	compactModel  usageWireRef
	compactPrompt string
}

// rawMessageAdmission builds one message-kind admission payload from the
// caller's authored execution values.
func rawMessageAdmission(sessionID, operationID, entryID string, v rawAdmissionValues) string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var defs []struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal([]byte(v.tools), &defs)
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	initialNames, _ := json.Marshal(names)
	return fmt.Sprintf(
		`{"session_id":%q,"operation_id":%q,"request_kind":"message",`+
			`"admitted_entry":{"session_id":%q,"entry_id":%q},"inputs":{},`+
			`"execution":{"configuration_revision":"rev-1","model":{"provider":%q,"model":%q},`+
			`"context_window":4096,"output_reserve":%d,`+
			`"system_prompt":%q,"tools":%s,"initial_tool_names":%s,`+
			`"compact":{"model":{"provider":%q,"model":%q},"system_prompt":%q},`+
			`"strategy":{"id":"melta.summary/v1","requires_context_window":true}},"admitted_at":%q}`,
		sessionID, operationID, sessionID, entryID,
		v.model.Provider, v.model.Model, v.outputReserve,
		v.systemPrompt, v.tools, initialNames,
		v.compactModel.Provider, v.compactModel.Model, v.compactPrompt, now)
}

// compactRawMessageAdmission builds the compact and paging fixtures'
// message-kind admission payload.
func compactRawMessageAdmission(sessionID, operationID, entryID string) string {
	return rawMessageAdmission(sessionID, operationID, entryID, rawAdmissionValues{
		agentType:     "solo",
		model:         usageWireRef{Provider: "prov", Model: "m"},
		outputReserve: 1024,
		systemPrompt:  "compact-sys",
		tools:         `[{"name":"noop","description":"tool noop","parameters_base64":"eyJ0eXBlIjoib2JqZWN0In0="}]`,
		compactModel:  usageWireRef{Provider: "prov", Model: "m"},
		compactPrompt: "summarize",
	})
}

func compactRawOperationRegister(admission, state string) string {
	return fmt.Sprintf(`{"admission":%s,"state":%s}`, admission, state)
}

func compactRawRunningModelState(now, reserved, boundary string) string {
	return fmt.Sprintf(
		`{"status":"running","started_at":%q,`+
			`"active_effect":{"kind":"model","result_entry_id":%q,"request_id":"request-live-%s","model":{"provider":"prov","model":"m"},"purpose":"conversation","evidence":{"boundary_entry_id":%q,"prompt":"compact-sys","tools":[]}},`+
			`"running_request":{"request_id":"request-live-%s","purpose":"conversation","boundary_entry_id":%q,"hook_results":[]},`+
			`"pending_tool_calls":[],"usage":{"by_model":[]}}`,
		now, reserved, reserved, boundary, reserved, boundary)
}

func compactRawSuccessState(now, settlementEntry, sessionID, usage string) string {
	return fmt.Sprintf(
		`{"status":"success","started_at":%q,"settled_at":%q,"pending_tool_calls":[],"usage":%s,"terminal":{"settlement_entry":{"session_id":%q,"entry_id":%q}}}`,
		now, now, usage, sessionID, settlementEntry)
}

func compactRawInputEntry(sessionID, entryID, operationID, origin, text string) string {
	provenance := ""
	if origin == "plugin" {
		provenance = `,"extension_id":"lifecycle.extension"`
	}
	return fmt.Sprintf(
		`{"session_id":%q,"entry_id":%q,"operation_id":%q,"origin":%q%s,"content":[{"kind":"text","text":%q}]}`,
		sessionID, entryID, operationID, origin, provenance, text)
}

func compactRawAssistantEntry(sessionID, entryID, operationID, boundaryID, text string) string {
	return fmt.Sprintf(
		`{"session_id":%q,"entry_id":%q,"operation_id":%q,"status":"completed",`+
			`"source":{"provider":"prov","model":"m"},"content":[{"kind":"text","text":%q}],"tool_calls":[],"request_id":%q,"evidence":{"boundary_entry_id":%q,"prompt":"compact-sys","tools":[]}}`,
		sessionID, entryID, operationID, text, "request-a-"+entryID, boundaryID)
}

func compactRawSettlementEntry(sessionID, entryID, operationID, extra string) string {
	return fmt.Sprintf(
		`{"session_id":%q,"entry_id":%q,"operation_id":%q,"status":"success"%s}`,
		sessionID, entryID, operationID, extra)
}

func compactRawCompactionEntry(sessionID, entryID, operationID, summary, boundaryID string) string {
	return fmt.Sprintf(
		`{"session_id":%q,"entry_id":%q,"operation_id":%q,"strategy_id":"melta.summary/v1",`+
			`"messages":[{"role":"assistant","source":{"provider":"prov","model":"m"},"content":[{"kind":"text","text":%q}]}],`+
			`"boundary_entry_id":%q,"configuration_revision":"rev-1",`+
			`"facts":[{"request_id":%q,"model":{"provider":"prov","model":"m"},"input":[{"role":"user","content":[{"kind":"text","text":"transcript seed"}]}]}],`+
			`"evidence":{"boundary_entry_id":%q,"prompt":"","tools":[]}}`,
		sessionID, entryID, operationID, "[Previous conversation summary]\n\n"+summary+"\n\n[End of summary. Continue from here.]", boundaryID, "maintainer-"+entryID, boundaryID)
}

// compactRawSuccessOperation seeds one settled success message Operation:
// its input and assistant entries plus the settlement entry are inserted in
// order and the register carries the matching terminal section. It returns
// the assistant entry's identity; the input and settlement identities stay
// local to the seeding.
func compactRawSuccessOperation(t *testing.T, store harness.Storage, sessionID, operationID, inputText, assistantText string) string {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	inputID, assistantID, settlementID := newLifecycleID(t), newLifecycleID(t), newLifecycleID(t)
	lifecycleInsertEntry(t, store, sessionID, inputID, operationID, harness.EntryInput, compactRawInputEntry(sessionID, inputID, operationID, "user", inputText))
	lifecycleInsertEntry(t, store, sessionID, assistantID, operationID, harness.EntryAssistant, compactRawAssistantEntry(sessionID, assistantID, operationID, inputID, assistantText))
	lifecycleInsertEntry(t, store, sessionID, settlementID, operationID, harness.EntryOperationSettlement, compactRawSettlementEntry(sessionID, settlementID, operationID, ""))
	lifecycleInsertRegister(t, store, harness.RegisterKey{SessionID: sessionID, Kind: harness.RegisterOperation, OperationID: operationID},
		compactRawOperationRegister(compactRawMessageAdmission(sessionID, operationID, inputID), compactRawSuccessState(now, settlementID, sessionID, `{"by_model":[]}`)))
	return assistantID
}

// TestCompactLifecycleCapturedCompactionConfiguration proves the R6 row end
// to end over the real Open path: the prepared capture carries the compact
// configuration from one revision, and the prompt and tool-description
// replacement freedoms still hold through the admitted candidate. The compact
// members are Core-owned capture fields a contribution cannot name at all.
func TestCompactLifecycleCapturedCompactionConfiguration(t *testing.T) {
	eachProductionStore(t, func(t *testing.T, e *productionEnv) {
		ctx := context.Background()
		var seenMu sync.Mutex
		var seenPrompt string
		hook := &recordHook{name: "hook.first", events: &traceLog{}, admissionMutate: func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
			seenMu.Lock()
			seenPrompt = ""
			if in.Candidate.Prompt != nil {
				seenPrompt = *in.Candidate.Prompt
			}
			seenMu.Unlock()
			return extension.AdmissionResult{}, nil
		}}
		// The freed phase runs on its own hook instance, so the first
		// instance's visible contribution is never overwritten.
		freedHook := &recordHook{name: "hook.freed", events: &traceLog{}}
		writeAgentDefinitions(t, e.configPath, productionAgentsWithFreedHook())
		r, err := e.openWithPlugins(ctx, hook, &parkingHook{}, freedHookPlugin(freedHook))
		testassert.NoError(t, err)
		defer r.Close(ctx)
		session, err := r.createSession(ctx, e.workspace("compact-capture"), "worker")
		testassert.NoError(t, err)
		sessionID := session.Identity.SessionID

		// The capture carries the compact configuration from one revision.
		submitThroughRuntime(t, r, sessionID, "op-1", "please write")
		awaitOperation(t, r, sessionID, "op-1", harness.OperationSuccess)
		awaitIdleSession(t, r, sessionID)
		durable := readOperation(t, r, sessionID, "op-1").Admission.Execution
		seenMu.Lock()
		seen := seenPrompt
		seenMu.Unlock()
		testassert.Equal(t, seen, durable.SystemPrompt)
		testassert.Require(t, durable.Compact.Model.Provider != "" && durable.Compact.Model.Model != "" && durable.Compact.SystemPrompt != "", "captured compact configuration incomplete: %+v", durable.Compact)

		// The prompt and tool-description replacement freedoms hold on a
		// fresh session under the second hook instance.
		freedHook.setAdmission(nil, func(in extension.AdmissionInput) (extension.AdmissionResult, error) {
			prompt := ""
			if in.Candidate.Prompt != nil {
				prompt = *in.Candidate.Prompt
			} else {
				text, err := in.Prompt.Default()
				if err != nil {
					return extension.AdmissionResult{}, err
				}
				prompt = text
			}
			prompt += "|freed"
			candidate := in.Candidate
			candidate.Prompt = &prompt
			tools := make([]model.ToolDefinition, len(candidate.Tools))
			for i, tool := range candidate.Tools {
				tool.Description = "freed description"
				tools[i] = tool
			}
			candidate.Tools = tools
			return extension.AdmissionResult{Candidate: &candidate}, nil
		})
		freedSession, err := r.createSession(ctx, e.workspace("compact-freed"), "freedworker")
		testassert.NoError(t, err)
		freedID := freedSession.Identity.SessionID
		setExtensionState(t, r, freedID, "freedhooks", `{"agent":"freedworker"}`)
		submitThroughRuntime(t, r, freedID, "op-3", "please write")
		awaitOperation(t, r, freedID, "op-3", harness.OperationSuccess)
		rec := readOperation(t, r, freedID, "op-3")
		testassert.Require(t, strings.Contains(rec.Admission.Execution.SystemPrompt, "|freed"), "capture prompt = %q, want the hook's replacement", rec.Admission.Execution.SystemPrompt)
		for _, name := range rec.Admission.Execution.InitialToolNames {
			tool, ok := capturedTool(rec.Admission.Execution.Tools, name)
			testassert.Require(t, ok && tool.Description == "freed description", "tool %q description = %q, want the hook's replacement", name, tool.Description)
		}
	})
}

// freedHookPlugin composes the second hook instance under its own extension
// identity: it contributes only for Sessions whose Agents state selects the
// "freedworker" definition, so the first instance's contribution is never
// overwritten by the freed phase.
func freedHookPlugin(hook *recordHook) extension.Extension {
	return testNative("freedhooks", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachAdmission(func(ctx context.Context, call extension.AuthorContext, in extension.AdmissionInput) (extension.AdmissionResult, error) {
			var state struct {
				Agent string `json:"agent"`
			}
			if err := json.Unmarshal(call.State, &state); err != nil || state.Agent != "freedworker" {
				return extension.AdmissionResult{}, nil
			}
			return hook.Admit(ctx, call, in)
		}, nil)
		return nil, err
	})
}

// productionAgentsWithFreedHook extends the production agents document with
// one definition the freed hook phase contributes for.
func productionAgentsWithFreedHook() string {
	return strings.TrimSuffix(productionAgentsDocument, "}") + `,
  "freedworker": {"model": "prov/m", "system_prompt": "simple", "tools": ["prod_write", "prod_read"]}}`
}
