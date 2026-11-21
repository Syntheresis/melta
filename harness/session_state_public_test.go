package harness_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/model"
)

func TestFirstAdmissionReadsStoredInputsAndState(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	refused := errors.New("stop after preparation")
	var observed harness.PreparationSession
	h, err := harness.New(ctx, harness.Dependencies{
		Storage: store,
		Prepare: func(_ context.Context, req harness.PreparationRequest) (harness.PreparedExecution, error) {
			observed = req.Session
			return harness.PreparedExecution{}, refused
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := h.CreateSession(ctx, harness.CreateSessionRequest{
		Workspace:      t.TempDir(),
		Inputs:         harness.SessionInputs{Tools: []string{}},
		ExtensionState: map[string]harness.SessionStateValue{"dormant": []byte(` {"v":1} `)},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Submit(ctx, harness.SubmitRequest{
		SessionID: created.Identity.SessionID, OperationID: "first",
		Origin: harness.InputOriginUser, Mode: harness.MessageModeRegular,
		Content: []model.ContentPart{{Kind: model.PartText, Text: "hello"}},
	})
	if !errors.Is(err, refused) {
		t.Fatalf("submission error = %v", err)
	}
	if observed.Inputs.Tools == nil || len(observed.Inputs.Tools) != 0 || string(observed.ExtensionState["dormant"]) != ` {"v":1} ` {
		t.Fatalf("first admission = %#v", observed)
	}
	if _, err := h.SetSessionInputs(ctx, created.Identity.SessionID, harness.SessionInputs{}); err != nil {
		t.Fatal(err)
	}
	value := harness.SessionStateValue(` {"v":2} `)
	if _, err := h.SetSessionExtensionState(ctx, created.Identity.SessionID, "dormant", value); err != nil {
		t.Fatal(err)
	}
	value[1] = 'x'
	_, err = h.Submit(ctx, harness.SubmitRequest{
		SessionID: created.Identity.SessionID, OperationID: "second",
		Origin: harness.InputOriginUser, Mode: harness.MessageModeRegular,
		Content: []model.ContentPart{{Kind: model.PartText, Text: "hello again"}},
	})
	if !errors.Is(err, refused) {
		t.Fatalf("second submission error = %v", err)
	}
	if observed.Inputs.Tools != nil || string(observed.ExtensionState["dormant"]) != ` {"v":2} ` {
		t.Fatalf("updated admission = %#v", observed)
	}
}
