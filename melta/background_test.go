package melta

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// backgroundProbe records the author call view of every prepared call.
type backgroundProbe struct {
	seen []extension.AuthorContext
}

func (p *backgroundProbe) record(tc extension.AuthorContext) {
	p.seen = append(p.seen, tc)
}

type backgroundProbeTool struct {
	probe *backgroundProbe
}

func (t backgroundProbeTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return runtimeNormalize(call)
}

func (t backgroundProbeTool) Prepare(_ context.Context, tc extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	t.probe.record(tc)
	return extension.ToolPlan{Immediate: &harness.ToolOutcome{Result: model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: "probed"}}}
}

func (t backgroundProbeTool) Execute(context.Context, extension.AuthorContext, extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{Status: model.ResultSuccess, Content: "probed"}}
}

func backgroundProbePlugin(probe *backgroundProbe) extension.Extension {
	tool := backgroundProbeTool{probe: probe}
	return testNative("bgprobe", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(testDescribeTool("bg_probe"), tool, nil)
		return nil, err
	})
}

// TestBackgroundBridgeArmingAcrossPublication pins the arming pattern on the
// production preparation path: the bridge is created before harness.New and
// armed before the owner is published, so construction and publication
// produce no observation of it, while every prepared call afterward carries
// the complete armed bridge whose methods reach the Harness.
func TestBackgroundBridgeArmingAcrossPublication(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()
	server := newProductionModelServer(t, "bg_probe", "{}")
	home := t.TempDir()
	isolateBundledCredentials(t)
	t.Setenv("PRODUCTION_TEST_KEY", "production-secret-1")
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, chainCommandConfigDocument(server.URL))
	writeAgentDefinitions(t, configPath,
		`{"prober": {"model": "prov/m", "system_prompt": "simple", "tools": ["bg_probe"]}}`)
	enableFixturePlugin(t, configPath, "bgprobe")
	workspace := filepath.Join(home, "probe-ws")
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))

	probe := &backgroundProbe{}
	useFixtureStore(t, store)
	r, err := Open(ctx, testOptions(dataDir, configPath, home, backgroundProbePlugin(probe), fixtureAgentPlugin()))
	testassert.NoError(t, err)
	// The unarmed-to-armed window ends before publication: construction
	// and publication observe nothing.
	testassert.Equal(t, len(probe.seen), 0)

	session, err := r.createSession(ctx, workspace, "prober")
	testassert.NoError(t, err)
	submitThroughRuntime(t, r, session.Identity.SessionID, "op-1", "probe")
	awaitOperation(t, r, session.Identity.SessionID, "op-1", harness.OperationSuccess)

	testassert.Require(t, len(probe.seen) != 0, "no prepared call observed the background services object")
	for i, call := range probe.seen {
		testassert.Require(t, call.Call != nil, "prepared call %d carried no call binding", i)
	}

	testassert.NoError(t, r.Close(ctx))
}
