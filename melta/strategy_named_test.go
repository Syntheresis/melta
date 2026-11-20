package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// recordingStrategy delegates to the included summary policy while recording
// the exact Settings document the Harness supplied to Plan and Consume.
type recordingStrategy struct {
	mu       sync.Mutex
	plans    []json.RawMessage
	consumes []json.RawMessage
}

func (s *recordingStrategy) Plan(input harness.PlanInput) (harness.PlanOutput, error) {
	s.mu.Lock()
	s.plans = append(s.plans, append(json.RawMessage(nil), input.Settings...))
	s.mu.Unlock()
	return harness.IncludedStrategyBinding().Strategy.Plan(input)
}

func (s *recordingStrategy) Consume(input harness.ConsumeInput) (harness.PlanOutput, error) {
	s.mu.Lock()
	s.consumes = append(s.consumes, append(json.RawMessage(nil), input.Input.Settings...))
	s.mu.Unlock()
	return harness.IncludedStrategyBinding().Strategy.Consume(input)
}

func (s *recordingStrategy) observed() ([]json.RawMessage, []json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]json.RawMessage(nil), s.plans...), append([]json.RawMessage(nil), s.consumes...)
}

// strategyExtension registers one context strategy under its supplied
// extension identity/version through the activation API.
func strategyExtension(id, version, capabilityID string, requiresWindow bool, implementation harness.ContextStrategy) extension.Extension {
	manifest := registrarManifest(id)
	manifest.Version = version
	manifest.SettingsSchema = []byte(`{"type":"object","properties":{"enabled":{"type":"boolean"}},"required":["enabled"],"additionalProperties":false}`)
	return extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterStrategy(harness.StrategyBinding{ID: capabilityID, RequiresContextWindow: requiresWindow, Strategy: implementation}, nil)
		return nil, err
	}}
}

func TestSelectedStrategyWindowAndCapture(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(fmt.Sprintf("selected-requires-%t", required), func(t *testing.T) {
			e := newOwnerEnv(t)
			writeServiceFile(t, e.configPath, `{"model":"p/m","providers":{"p":{"transport":{"base_url":"`+e.server.URL+`","api_key_env":""},"discovery":false,"models":{"m":{}}}},"extensions":{"strategyext":{"enabled":true}}}`)
			e.fixtureStore(storage.NewMemory())
			e.server.setScript(func(_ context.Context, _ string) []string { return textTurnEvents("done") })
			native := strategyExtension("strategyext", "1", "selected.strategy", required, &recordingStrategy{})
			opts := e.options(native)
			opts.Strategy = &harness.StrategyBinding{ID: "host", RequiresContextWindow: !required, Strategy: harness.IncludedStrategyBinding().Strategy}
			r, err := open(context.Background(), opts)
			testassert.NoError(t, err)
			defer r.Close(context.Background())
			ctx := context.Background()
			inputs := protocol.SessionInputs{Strategy: &protocol.StrategySelection{Id: "selected.strategy", Settings: `{"keep":1}`}}
			session, err := r.CreateSession(ctx, testCaller(r), e.dataDir, &inputs)
			testassert.NoError(t, err)
			var part protocol.ContentPart
			testassert.NoError(t, part.FromTextPart(protocol.TextPart{Kind: protocol.TextPartKindText, Text: "hello"}))
			result, err := r.SubmitSession(ctx, testCaller(r), session.SessionId, protocol.SubmitRequest{OperationId: "selected", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{part}})
			if required {
				testassert.Require(t, err != nil && result.Operation == nil && errors.Is(readOperationError(ctx, r, session.SessionId, "selected"), harness.ErrNotFound), "required window admitted: %+v %v", result, err)
				return
			}
			testassert.NoError(t, err)
			record := awaitOperation(t, r, session.SessionId, "selected", harness.OperationSuccess)
			capture := record.Admission.Execution.Strategy
			testassert.Require(t, capture.ID == "selected.strategy" && !capture.RequiresContextWindow && string(capture.Settings) == `{"keep":1}`, "strategy capture = %+v", capture)
		})
	}
}

func namedStrategyConfig(endpoint string) string {
	return `{"providers":{"prov":` + nominalProvMember(endpoint, "",
		`{"m":{"name":"M","context_window":8192,"max_output_tokens":4096},`+
			`"k":{"name":"K","context_window":32768,"max_output_tokens":4096}}`) + `},
		"compaction":{"model":"prov/k"},
		"extensions":{"strategyext":{"enabled":true}}}`
}

// createStrategySession creates one Session whose stored choices select the
// model, instructions and strategy settings directly.
func createStrategySession(t *testing.T, owner *Melta, workspace string, strategyID string, settings json.RawMessage) harness.SessionRecord {
	t.Helper()
	var record harness.SessionRecord
	err := owner.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
		instructions := "named prompt"
		var createErr error
		record, createErr = h.CreateSession(ctx, harness.CreateSessionRequest{
			Workspace: workspace,
			Inputs: harness.SessionInputs{
				Model:        &model.ModelRef{Provider: "prov", Model: "m"},
				Instructions: &instructions,
				Strategy:     &harness.StrategySelection{ID: strategyID, Settings: settings},
			},
		})
		return createErr
	})
	testassert.NoError(t, err)
	return record
}

// TestStoredStrategySettingsReachPlanConsumeAndCapture proves a stored
// strategy selection's exact Settings document is the one execution value:
// the captured strategy carries the same bytes, Plan and Consume receive
// them, and provenance names the strategy's declaring extension.
func TestStoredStrategySettingsReachPlanConsumeAndCapture(t *testing.T) {
	eachPrepStore(t, func(t *testing.T, store harness.Storage) {
		e := newOwnerEnv(t)
		writeServiceFile(t, e.configPath, namedStrategyConfig(e.server.URL))
		e.fixtureStore(store)
		recorder := &recordingStrategy{}
		strategy := strategyExtension("strategyext", "7", "recorder.strategy", true, recorder)
		owner, err := e.open(context.Background(), strategy)
		testassert.NoError(t, err)
		defer closeProjectionRuntime(owner)

		e.server.setScript(func(_ context.Context, body string) []string {
			if strings.Contains(body, `"model":"k"`) || strings.Contains(body, `"model": "k"`) {
				return append(textTurnEvents("summary"), `{"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`)
			}
			if lastUserText(body) == "seed" {
				return append(textTurnEvents("seeded answer"), `{"usage":{"prompt_tokens":5000,"completion_tokens":10,"total_tokens":5010}}`)
			}
			return nil
		})

		session := createStrategySession(t, owner, e.dataDir, "recorder.strategy", json.RawMessage(`{"exact":9007199254740993}`))
		submitThroughRuntime(t, owner, session.Identity.SessionID, "op-1", "seed")
		awaitOperation(t, owner, session.Identity.SessionID, "op-1", harness.OperationSuccess)
		submitThroughRuntime(t, owner, session.Identity.SessionID, "op-2", "trigger")
		record := awaitOperation(t, owner, session.Identity.SessionID, "op-2", harness.OperationSuccess)

		wantSettings := json.RawMessage(`{"exact":9007199254740993}`)
		capture := record.Admission.Execution
		testassert.Require(t, capture.Strategy.ID == "recorder.strategy" && capture.Strategy.RequiresContextWindow && reflect.DeepEqual(capture.Strategy.Settings, wantSettings), "captured strategy = %+v, want the stored strategy and exact settings", capture.Strategy)
		testassert.Require(t, capture.Model == (model.ModelRef{Provider: "prov", Model: "m"}), "capture model = %+v, want the stored model choice", capture)

		plans, consumes := recorder.observed()
		testassert.Require(t, len(plans) != 0 && len(consumes) != 0, "strategy calls = %d plans/%d consumes, want both", len(plans), len(consumes))
		for _, settings := range plans {
			testassert.Require(t, reflect.DeepEqual(settings, wantSettings), "Plan settings = %s, want exact %s", settings, wantSettings)
		}
		for _, settings := range consumes {
			testassert.Require(t, reflect.DeepEqual(settings, wantSettings), "Consume settings = %s, want exact %s", settings, wantSettings)
		}

		foundStrategy, foundAgents := false, false
		for _, implementation := range capture.Implementations {
			switch implementation.ExtensionID {
			case "strategyext":
				foundStrategy = implementation.Version == "7" && string(implementation.Settings) == `{"enabled":true}`
			case "agents":
				foundAgents = true
			}
		}
		testassert.Require(t, foundStrategy && !foundAgents, "provenance = %+v, want the strategy root and no unused Agents root", capture.Implementations)
	})
}

// TestStoredUnavailableStrategyFallsBackToIncluded proves the stored-choice
// fallback: a strategy ID that names an unknown strategy or a strategy whose
// declaring extension is inactive substitutes the included strategy, and the
// admission completes with the included capture.
func TestStoredUnavailableStrategyFallsBackToIncluded(t *testing.T) {
	disabled := strategyExtension("inactiveext", "1", "inactive.strategy", true, &recordingStrategy{})
	for _, row := range []struct {
		name       string
		strategyID string
	}{
		{"unknown strategy", "ghost.strategy"},
		{"inactive strategy", "inactive.strategy"},
	} {
		t.Run(row.name, func(t *testing.T) {
			eachPrepStore(t, func(t *testing.T, store harness.Storage) {
				e := newOwnerEnv(t)
				writeServiceFile(t, e.configPath, namedStrategyConfig(e.server.URL))
				e.fixtureStore(store)
				e.server.setScript(func(_ context.Context, _ string) []string { return textTurnEvents("done") })
				owner, err := e.open(context.Background(), disabled)
				testassert.NoError(t, err)
				defer closeProjectionRuntime(owner)
				session := createStrategySession(t, owner, e.dataDir, row.strategyID, nil)
				submitThroughRuntime(t, owner, session.Identity.SessionID, "op-fallback", "hello")
				record := awaitOperation(t, owner, session.Identity.SessionID, "op-fallback", harness.OperationSuccess)
				capture := record.Admission.Execution.Strategy
				testassert.Require(t, capture.ID == harness.IncludedStrategyID && capture.RequiresContextWindow, "%s: capture = %+v, want the included strategy substituted", row.name, capture)
			})
		})
	}
}
