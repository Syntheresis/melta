package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

func TestBaselineModelConfiguration(t *testing.T) {
	for _, row := range []struct {
		raw   string
		want  string
		valid bool
	}{
		{`{}`, "", true},
		{`{"default_model":"provider/model"}`, "", true},
		{`{"model":""}`, "", true},
		{`{"model":null}`, "", true},
		{`{"model":"provider/org/model"}`, "provider/org/model", true},
		{`{"model":"invalid"}`, "", false},
		{`{"model":"/model"}`, "", false},
		{`{"model":1}`, "", false},
		{`{"model":{}}`, "", false},
	} {
		t.Run(row.raw, func(t *testing.T) {
			cfg, err := config.Parse([]byte(row.raw))
			if (err == nil) != row.valid {
				t.Fatalf("doctor reader: %v", err)
			}
			doc, err := decodeCapturedConfig([]byte(row.raw))
			if err == nil {
				_, err = config.ParseOptionalModel(doc.Model)
			}
			if (err == nil) != row.valid {
				t.Fatalf("owner reader: %v", err)
			}
			if row.valid && cfg.Model.String() != row.want {
				t.Fatalf("model %q, want %q", cfg.Model.String(), row.want)
			}
		})
	}
}

func TestBaselineModelWindowAdmission(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(fmt.Sprintf("requires-%t", required), func(t *testing.T) {
			e := newOwnerEnv(t)
			writeServiceFile(t, e.configPath, `{"model":"p/m","providers":{"p":{"transport":{"base_url":"`+e.server.URL+`","api_key_env":""},"discovery":false,"models":{"m":{}}}}}`)
			e.fixtureStore(storage.NewMemory())
			e.server.setScript(func(_ context.Context, _ string) []string { return textTurnEvents("done") })
			opts := e.options()
			opts.Strategy = &harness.StrategyBinding{ID: "host", RequiresContextWindow: required, Strategy: harness.IncludedStrategyBinding().Strategy}
			r, err := open(context.Background(), opts)
			testassert.NoError(t, err)
			defer r.Close(context.Background())
			ctx := context.Background()
			session, err := r.CreateSession(ctx, testCaller(r), e.dataDir, nil)
			testassert.NoError(t, err)
			var part protocol.ContentPart
			testassert.NoError(t, part.FromTextPart(protocol.TextPart{Kind: protocol.TextPartKindText, Text: "hello"}))
			result, err := r.SubmitSession(ctx, testCaller(r), session.SessionId, protocol.SubmitRequest{OperationId: "window", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{part}})
			if required {
				testassert.Require(t, err != nil && result.Operation == nil, "required window admitted: %+v %v", result, err)
				testassert.Require(t, errors.Is(readOperationError(ctx, r, session.SessionId, "window"), harness.ErrNotFound), "refusal persisted an Operation")
				return
			}
			testassert.NoError(t, err)
			record := awaitOperation(t, r, session.SessionId, "window", harness.OperationSuccess)
			testassert.Require(t, record.Admission.Execution.ContextWindow == 0 && !record.Admission.Execution.Strategy.RequiresContextWindow, "capture: %+v", record.Admission.Execution)
		})
	}
}

func TestHostAndDefinitionModelAdmission(t *testing.T) {
	e := newOwnerEnv(t)
	writeServiceFile(t, e.configPath, `{"model":"p/c","providers":{"p":{"transport":{"base_url":"`+e.server.URL+`","api_key_env":""},"discovery":false,"models":{"a":{"context_window":8192},"b":{"context_window":16384},"c":{"context_window":32768}}}}}`)
	writeAgentDefinitions(t, e.configPath, `{"primary":{"model":"p/b"}}`)
	e.fixtureStore(storage.NewMemory())
	e.server.setScript(func(_ context.Context, _ string) []string { return textTurnEvents("done") })
	opts := e.options()
	host := model.ModelRef{Provider: "p", Model: "a"}
	opts.Defaults = harness.SessionInputs{Model: &host}
	opts.Extensions = []extension.Extension{agents.Author()}
	ctx := context.Background()
	r, err := open(ctx, opts)
	testassert.NoError(t, err)
	defer r.Close(ctx)
	createAndSubmit := func(inputs *protocol.SessionInputs, selected bool, op, want string) string {
		t.Helper()
		var sessionID string
		if selected {
			args := json.RawMessage(`{}`)
			workspace := e.dataDir
			result, err := r.InvokeExtension(ctx, testCaller(r), protocol.ExtensionInvocationRequest{
				ExtensionId: "agents", Operation: "create", RequestId: op + "-create",
				Target: protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace}, Arguments: &args,
			})
			testassert.NoError(t, err)
			testassert.Require(t, result.Failure == nil, "Agents create failure: %+v", result.Failure)
			var receipt struct {
				SessionID string `json:"session_id"`
			}
			testassert.NoError(t, json.Unmarshal(result.Value, &receipt))
			sessionID = receipt.SessionID
		} else {
			session, err := r.CreateSession(ctx, testCaller(r), e.dataDir, inputs)
			testassert.NoError(t, err)
			sessionID = session.SessionId
		}
		var part protocol.ContentPart
		testassert.NoError(t, part.FromTextPart(protocol.TextPart{Kind: protocol.TextPartKindText, Text: "hello"}))
		_, err = r.SubmitSession(ctx, testCaller(r), sessionID, protocol.SubmitRequest{OperationId: op, Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{part}})
		testassert.NoError(t, err)
		record := awaitOperation(t, r, sessionID, op, harness.OperationSuccess)
		testassert.Require(t, record.Admission.Execution.Model.String() == want, "captured model %s, want %s", record.Admission.Execution.Model.String(), want)
		return sessionID
	}
	first := createAndSubmit(nil, false, "host", "p/a")
	createAndSubmit(nil, true, "named", "p/b")
	missing := protocol.ModelRef("p/missing")
	inputs := protocol.SessionInputs{Model: &missing}
	createAndSubmit(&inputs, false, "host-fallback-before-settings", "p/a")
	view, err := r.GetConfiguration(ctx, testCaller(r))
	testassert.NoError(t, err)
	view.Settings.Model = nil
	_, err = r.UpdateSettings(ctx, testCaller(r), protocol.UpdateSettingsRequest{Settings: view.Settings})
	testassert.NoError(t, err)
	view, err = r.GetConfiguration(ctx, testCaller(r))
	testassert.NoError(t, err)
	testassert.Require(t, view.Settings.Model == nil, "host model leaked into settings: %+v", view.Settings.Model)
	createAndSubmit(&inputs, false, "host-fallback", "p/a")
	record := awaitOperation(t, r, first, "host", harness.OperationSuccess)
	testassert.Require(t, record.Admission.Execution.Model.String() == "p/a", "reload changed prior capture: %+v", record.Admission.Execution)
}

func TestBaselineModelBinding(t *testing.T) {
	for _, row := range []struct {
		name, chosen, baseline, want string
		requiresWindow, cancel       bool
		wantErr                      bool
	}{
		{"selected", "prov/m", "prov/m2", "prov/m", true, false, false},
		{"missing", "prov/missing", "prov/m2", "prov/m2", true, false, false},
		{"credential", "p2/m3", "prov/m2", "prov/m2", true, false, false},
		{"window required", "prov/mzero", "prov/m2", "prov/m2", true, false, false},
		{"window not required", "prov/mzero", "prov/m2", "prov/mzero", false, false, false},
		{"neither", "prov/missing", "p2/m3", "", true, false, true},
		{"cancelled", "prov/missing", "prov/m2", "", true, true, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			t.Setenv("PREP_COMPACT_ABSENT_KEY", "")
			doc := strings.TrimSuffix(compactPrepConfigDocument("https://prov.test/v1"), "}") + `,"model":"` + row.baseline + `"}`
			captured, prep := compactPrepWith(t, compactAgentsDocument, doc, "https://prov.test/v1")
			sel := compactPrepSelection(t, captured, "solo")
			sel.strategy.RequiresContextWindow = row.requiresWindow
			var err error
			sel.candidate.Model, err = model.Parse(row.chosen)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if row.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			entry, transport, err := prep.bindModel(ctx, &sel)
			if (err != nil) != row.wantErr {
				t.Fatalf("bind: %v", err)
			}
			if row.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			if row.name == "neither" && (!errors.Is(err, config.ErrMissingEnvVar) || strings.Contains(err.Error(), "prov/missing")) {
				t.Fatalf("final refusal reason: %v", err)
			}
			if !row.wantErr && (entry == nil || transport.transport == nil || sel.candidate.Model.String() != row.want) {
				t.Fatalf("binding: %v %v %s", entry, transport, sel.candidate.Model.String())
			}
		})
	}
}

func TestBaselineModelCaptureAndIndependentCompact(t *testing.T) {
	doc := compactPrepConfigDocumentWith("https://prov.test/v1", `{"model":"prov/m"}`)
	doc = strings.TrimSuffix(doc, "}") + `,"model":"prov/m2"}`
	captured, prep := compactPrepWith(t, compactAgentsDocument, doc, "https://prov.test/v1")
	sel := compactPrepSelection(t, captured, "solo")
	sel.candidate.Model = model.ModelRef{Provider: "prov", Model: "missing"}
	entry, transport, err := prep.bindModel(context.Background(), &sel)
	testassert.NoError(t, err)
	capture, _, err := prep.concretePrepare(context.Background(), compactPrepRequest(), sel, entry, transport)
	testassert.NoError(t, err)
	testassert.Require(t, capture.Model.String() == "prov/m2" && capture.ContextWindow == 2048 && capture.OutputReserve == 1000, "effective capture: %+v", capture)
	testassert.Require(t, capture.Pricing != nil && capture.Pricing.Input != nil && *capture.Pricing.Input == 0.5, "effective pricing: %+v", capture.Pricing)
	testassert.Require(t, capture.Compact.Model.String() == "prov/m" && capture.Compact.Pricing != nil && capture.Compact.Pricing.Input != nil && *capture.Compact.Pricing.Input == 1.5, "independent compact capture: %+v", capture.Compact)
}

func TestBaselineModelSettingsRoundTrip(t *testing.T) {
	h := newServiceHarness(t)
	writeServiceFile(t, h.configPath, `{"model":"prov/m","default_model":"ignored/value","other":{"number":9007199254740993}}`)
	svc := h.service(context.Background())
	captured, err := svc.publish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := captured.snapshot
	settings := projectSettings(s)
	if settings.Model == nil || *settings.Model != "prov/m" {
		t.Fatalf("settings model: %+v", settings.Model)
	}
	ref := protocol.ModelRef("other/new")
	settings.Model = &ref
	captured, err = svc.mutate(context.Background(), editSettings(settings))
	if err != nil || captured.snapshot.model.String() != string(ref) {
		t.Fatalf("save: %v", err)
	}
	bad := protocol.ModelRef("malformed")
	settings.Model = &bad
	before, err := os.ReadFile(h.configPath)
	if err != nil {
		t.Fatal(err)
	}
	revision := captured.snapshot
	if _, err := svc.mutate(context.Background(), editSettings(settings)); err == nil {
		t.Fatal("invalid model accepted")
	}
	after, err := os.ReadFile(h.configPath)
	if err != nil || string(after) != string(before) || svc.current() != revision {
		t.Fatalf("rejected save changed configuration: %v", err)
	}
	settings.Model = nil
	if _, err := svc.mutate(context.Background(), editSettings(settings)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(h.configPath)
	if err != nil {
		t.Fatal(err)
	}
	var roots map[string]json.RawMessage
	if err := json.Unmarshal(raw, &roots); err != nil {
		t.Fatal(err)
	}
	if _, ok := roots["model"]; ok || string(roots["default_model"]) != `"ignored/value"` || !strings.Contains(string(roots["other"]), "9007199254740993") {
		t.Fatalf("saved roots: %s", raw)
	}
	captured, err = svc.publish(context.Background())
	if err != nil || !captured.snapshot.model.IsZero() {
		t.Fatalf("reload: %v", err)
	}
}
