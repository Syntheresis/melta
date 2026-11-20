package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Syntheresis/melta/client"
	"github.com/Syntheresis/melta/protocol"
)

func TestRuntimeBaselineModelAdmission(t *testing.T) {
	requests := make(chan string, 10)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		requests <- body.Model
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer provider.Close()
	t.Setenv("HOME", t.TempDir())
	for _, row := range []struct {
		name, configured, selected, effective string
		admit                                 bool
	}{
		{"default", "p/good", "", "good", true},
		{"selected", "p/good", "p/other", "other", true},
		{"fallback missing", "p/good", "p/missing", "good", true},
		{"refusal", "p/missing", "", "", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.json")
			doc := fmt.Sprintf(`{"model":%q,"providers":{"p":{"transport":{"base_url":%q,"api_key_env":""},"discovery":false,"models":{"good":{"context_window":262144},"other":{"context_window":8192}}}}}`, row.configured, provider.URL)
			if err := os.WriteFile(configPath, []byte(doc), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- runRuntime(ctx, dir, configPath) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			var c *client.Client
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				var err error
				c, err = client.Connect(ctx, filepath.Join(dir, "runtime.json"))
				if err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if c == nil {
				t.Fatal("runtime discovery not ready")
			}
			created, err := c.CreateSessionWithResponse(ctx, protocol.CreateSessionRequest{Workspace: dir})
			if err != nil || created.JSON201 == nil {
				t.Fatalf("create: %v %+v", err, created)
			}
			id := created.JSON201.SessionId
			if row.selected != "" {
				changed, err := c.SetSessionModelWithResponse(ctx, id, protocol.SetSessionModelRequest{Model: protocol.ModelRef(row.selected)})
				if err != nil || changed.JSON200 == nil {
					t.Fatalf("set model: %v %+v", err, changed)
				}
			}
			var part protocol.ContentPart
			if err := part.FromTextPart(protocol.TextPart{Kind: protocol.TextPartKindText, Text: "hello"}); err != nil {
				t.Fatal(err)
			}
			result, err := c.SubmitSessionWithResponse(ctx, id, protocol.SubmitRequest{OperationId: "op-1", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{part}})
			if err != nil {
				t.Fatal(err)
			}
			if row.admit {
				if result.JSON200 == nil || result.JSON200.Operation == nil {
					t.Fatalf("submit: %d %s", result.StatusCode(), result.Body)
				}
				if result.JSON200.Operation.Model != protocol.ModelRef("p/"+row.effective) {
					t.Fatalf("captured model: %+v", result.JSON200.Operation)
				}
				select {
				case model := <-requests:
					if model != row.effective {
						t.Fatalf("model %q", model)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("no provider request")
				}
			} else if result.JSON200 != nil {
				t.Fatalf("unexpected admission: %s", result.Body)
			}
			hydration, err := c.GetSessionHydrationWithResponse(ctx, id)
			if err != nil || hydration.JSON200 == nil {
				t.Fatalf("hydrate: %v %+v", err, hydration)
			}
			if len(hydration.JSON200.Operations) != btoi(row.admit) {
				t.Fatalf("operations after submit: %+v", hydration.JSON200.Operations)
			}
			stored := hydration.JSON200.Session.Inputs.Model
			if row.selected == "" {
				if stored != nil {
					t.Fatalf("stored model: %+v, want none", stored)
				}
			} else if stored == nil || string(*stored) != row.selected {
				t.Fatalf("stored model: %+v, want %q", stored, row.selected)
			}
			for _, warning := range hydration.JSON200.Warnings {
				if warning.SessionId != nil && (warning.Source != "runtime:prompt" || warning.Kind != "rules_not_found") {
					t.Fatalf("unexpected Session warning: %+v", warning)
				}
			}
		})
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}
