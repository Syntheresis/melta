package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/adaptation"
	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/melta"
	"github.com/Syntheresis/melta/protocol"
)

// baselineModel is the supplied-Runtime fixture's local OpenAI-compatible
// endpoint: it records the resolved request model and answers every turn with
// plain text.
type baselineModel struct {
	*httptest.Server

	mu     sync.Mutex
	models []string
}

func newBaselineModel(t *testing.T) *baselineModel {
	t.Helper()
	server := &baselineModel{}
	server.Server = httptest.NewServer(http.HandlerFunc(server.serve))
	t.Cleanup(server.Close)
	return server
}

func (s *baselineModel) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(raw, &body)
	s.mu.Lock()
	s.models = append(s.models, body.Model)
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprintf(w, "data: %s\n\n", `{"choices":[{"delta":{"role":"assistant","content":"runtime done"},"finish_reason":null}]}`)
	fmt.Fprintf(w, "data: %s\n\n", `{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func (s *baselineModel) requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.models...)
}

// baselineConfigDocument points the configured provider at the scripted local
// endpoint and optionally carries the installed extensions' inactive sections.
func baselineConfigDocument(endpoint string, extensionsDisabled bool) []byte {
	document := map[string]any{
		"providers": map[string]any{
			"prov": map[string]any{
				"transport": map[string]any{"base_url": endpoint, "api_key_env": ""},
				"discovery": false,
				"models": map[string]any{"m": map[string]any{
					"name": "M", "context_window": 65536,
				}},
			},
		},
	}
	if extensionsDisabled {
		document["extensions"] = map[string]any{
			"agents":     map[string]any{"enabled": false},
			"adaptation": map[string]any{"enabled": false},
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		panic(err)
	}
	return encoded
}

func baselineTextPart(t *testing.T, text string) protocol.ContentPart {
	t.Helper()
	var part protocol.ContentPart
	if err := part.FromTextPart(protocol.TextPart{Text: text}); err != nil {
		t.Fatalf("text part: %v", err)
	}
	return part
}

func awaitMountedSuccess(t *testing.T, ctx context.Context, client *protocol.ClientWithResponses, sessionID, operationID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last protocol.OperationStatus
	for {
		hydration, err := client.GetSessionHydrationWithResponse(ctx, sessionID)
		if err == nil && hydration.JSON200 != nil {
			for _, operation := range hydration.JSON200.Operations {
				if operation.OperationId != operationID {
					continue
				}
				last = operation.Status
				if operation.Status == protocol.OperationStatusSuccess {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation %q never succeeded (last status %q, last err %v)", operationID, last, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestProtocolSuppliedRuntimeBaselineControls proves the HTTP host over the
// supplied no-extension Runtime: no extension registers when Agents and
// Adaptation are absent or installed but inactive, model controls operate on
// the resolved inputs and reach the later admission, and Session creation
// refuses a missing or malformed Workspace.
func TestProtocolSuppliedRuntimeBaselineControls(t *testing.T) {
	rows := []struct {
		name       string
		disabled   bool
		extensions []extension.Extension
	}{
		{name: "absent"},
		{
			name:     "installed but disabled",
			disabled: true,
			extensions: []extension.Extension{
				agents.Author(),
				adaptation.Author(),
			},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			modelEndpoint := newBaselineModel(t)
			dataDir := t.TempDir()
			options := melta.Options{
				DataDir:       dataDir,
				Configuration: melta.NewValueConfigurationSource(baselineConfigDocument(modelEndpoint.URL, row.disabled)),
				Credentials:   melta.NewEnvironmentCredentialSource(filepath.Join(dataDir, ".env")),
				Extensions:    row.extensions,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, _, server := mountedOwner(t, ctx, options)
			client := mountedClient(t, server)
			workspace := t.TempDir()

			discovery, err := client.ListExtensionsWithResponse(ctx)
			if err != nil || discovery.JSON200 == nil {
				t.Fatalf("ListExtensions = (%+v, %v)", discovery, err)
			}
			if len(*discovery.JSON200) != 0 {
				t.Fatalf("extensions = %+v, want none", discovery.JSON200)
			}

			missing, err := client.CreateSessionWithResponse(ctx, protocol.CreateSessionRequest{Workspace: ""})
			if err != nil {
				t.Fatalf("CreateSession without Workspace: %v", err)
			}
			if missing.StatusCode() != http.StatusBadRequest || missing.JSONDefault == nil || missing.JSONDefault.Code != protocol.ErrorCodeInvalid {
				t.Fatalf("CreateSession without Workspace = (%d, %+v), want the typed invalid refusal", missing.StatusCode(), missing.JSONDefault)
			}
			invalid := rawMounted(t, http.MethodPost, server.Endpoint()+"/v1/sessions", server.credential, `{"workspace":7}`)
			if typed := decodeMountedError(t, invalid); invalid.StatusCode != http.StatusBadRequest || typed.Code != protocol.ErrorCodeInvalid {
				t.Fatalf("CreateSession with a malformed Workspace = (%d, %+v), want the typed invalid refusal", invalid.StatusCode, typed)
			}

			created, err := client.CreateSessionWithResponse(ctx, protocol.CreateSessionRequest{Workspace: workspace})
			if err != nil || created.JSON201 == nil {
				t.Fatalf("CreateSession = (%+v, %v)", created, err)
			}
			sessionID := created.JSON201.SessionId
			selected, err := client.SetSessionModelWithResponse(ctx, sessionID, protocol.SetSessionModelRequest{Model: "prov/m"})
			if err != nil || selected.JSON200 == nil {
				t.Fatalf("SetSessionModel = (%+v, %v)", selected, err)
			}
			if selected.JSON200.Inputs.Model == nil || *selected.JSON200.Inputs.Model != "prov/m" {
				t.Fatalf("SetSessionModel response = %+v, want the stored model", selected.JSON200.Inputs)
			}
			models, err := client.ListModelsWithResponse(ctx, &protocol.ListModelsParams{All: false})
			if err != nil || models.JSON200 == nil || len(models.JSON200.Models) != 1 || models.JSON200.Models[0].Ref != "prov/m" {
				t.Fatalf("ListModels = (%+v, %v), want the configured model", models, err)
			}

			submitted, err := client.SubmitSessionWithResponse(ctx, sessionID, protocol.SubmitRequest{
				OperationId: "op-runtime",
				Mode:        protocol.SubmitRequestModeRegular,
				Content:     []protocol.ContentPart{baselineTextPart(t, "hello")},
			})
			if err != nil || submitted.JSON200 == nil || submitted.JSON200.Operation == nil || submitted.JSON200.Operation.OperationId != "op-runtime" {
				t.Fatalf("SubmitSession = (%+v, %v)", submitted, err)
			}
			awaitMountedSuccess(t, ctx, client, sessionID, "op-runtime")
			requested := modelEndpoint.requested()
			if len(requested) != 1 || requested[0] != "m" {
				t.Fatalf("model requests = %v, want the later admission's bare wire model", requested)
			}
			hydration, err := client.GetSessionHydrationWithResponse(ctx, sessionID)
			if err != nil || hydration.JSON200 == nil || hydration.JSON200.SelectedModel == nil || *hydration.JSON200.SelectedModel != "prov/m" {
				t.Fatalf("hydration = (%+v, %v), want the selected prov/m identity", hydration, err)
			}
		})
	}
}

// TestProtocolMountedSessionOperationRead proves the generated Operation read
// returns the captured inputs under Session authority and maps another
// Session's Operation and an unknown ID to the same not-found read.
func TestProtocolMountedSessionOperationRead(t *testing.T) {
	modelEndpoint := newBaselineModel(t)
	dataDir := t.TempDir()
	options := melta.Options{
		DataDir:       dataDir,
		Configuration: melta.NewValueConfigurationSource(baselineConfigDocument(modelEndpoint.URL, false)),
		Credentials:   melta.NewEnvironmentCredentialSource(filepath.Join(dataDir, ".env")),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, server := mountedOwner(t, ctx, options)
	client := mountedClient(t, server)
	workspace := t.TempDir()
	created, err := client.CreateSessionWithResponse(ctx, protocol.CreateSessionRequest{Workspace: workspace})
	testassert.Require(t, err == nil && created.JSON201 != nil, "CreateSession = (%+v, %v)", created, err)
	sessionID := created.JSON201.SessionId
	tools := []string{"read_file"}
	ref := protocol.ModelRef("prov/m")
	selected, err := client.SetSessionInputsWithResponse(ctx, sessionID, protocol.SetSessionInputsRequest{Inputs: protocol.SessionInputs{Model: &ref, Tools: &tools}})
	testassert.Require(t, err == nil && selected.JSON200 != nil, "SetSessionInputs = (%+v, %v)", selected, err)
	submitted, err := client.SubmitSessionWithResponse(ctx, sessionID, protocol.SubmitRequest{
		OperationId: "op-read", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{baselineTextPart(t, "hello")},
	})
	testassert.Require(t, err == nil && submitted.JSON200 != nil, "SubmitSession = (%+v, %v)", submitted, err)
	awaitMountedSuccess(t, ctx, client, sessionID, "op-read")

	read, err := client.GetSessionOperationWithResponse(ctx, sessionID, "op-read")
	testassert.Require(t, err == nil && read.JSON200 != nil, "GetSessionOperation = (%+v, %v)", read, err)
	testassert.Require(t, string(read.JSON200.Inputs.Model) == "prov/m" && read.JSON200.Inputs.Prompt != "" && read.JSON200.Inputs.Strategy.Id == harness.IncludedStrategyID, "operation inputs = %+v, want the captured model/strategy", read.JSON200.Inputs)
	testassert.Require(t, len(read.JSON200.Inputs.Tools) == 1 && read.JSON200.Inputs.Tools[0].Name == "read_file", "operation tools = %+v, want the admitted selection", read.JSON200.Inputs.Tools)

	other, err := client.CreateSessionWithResponse(ctx, protocol.CreateSessionRequest{Workspace: workspace})
	testassert.Require(t, err == nil && other.JSON201 != nil, "second CreateSession = (%+v, %v)", other, err)
	wrongSession, err := client.GetSessionOperationWithResponse(ctx, other.JSON201.SessionId, "op-read")
	testassert.Require(t, err == nil && wrongSession.StatusCode() == http.StatusNotFound && wrongSession.JSONDefault != nil && wrongSession.JSONDefault.Code == protocol.ErrorCodeNotFound, "other Session's Operation = (%+v, %v), want typed not found", wrongSession, err)
	unknown, err := client.GetSessionOperationWithResponse(ctx, sessionID, "op-missing")
	testassert.Require(t, err == nil && unknown.StatusCode() == http.StatusNotFound && unknown.JSONDefault != nil && unknown.JSONDefault.Code == protocol.ErrorCodeNotFound, "unknown Operation = (%+v, %v), want typed not found", unknown, err)
}
