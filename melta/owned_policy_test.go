package melta

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
)

// TestOwnedSessionExtensionPoliciesBeforeAndAfterHandoff proves the two
// extension policies coexist in the same owned Session: Agents hides and
// refuses task by lineage, while another extension may nest Sessions, both
// before the completion handoff and after the Session accepts ordinary input
// again.
func TestOwnedSessionExtensionPoliciesBeforeAndAfterHandoff(t *testing.T) {
	e := newOwnerEnv(t)
	writeAgentDefinitions(t, e.configPath, `{"lead":{"model":"prov/m","system_prompt":"simple","tools":["task","alpha_start"]},"helper":{"model":"prov/m","system_prompt":"simple","tools":["alpha_start","task"],"subagent":true},"nested":{"model":"prov/m","system_prompt":"simple","tools":[]}}`)
	enableFixturePlugin(t, e.configPath, "alpha")
	e.fixtureStore(storage.NewMemory())
	probe := &ownedStartProbe{target: "nested", content: "nested work"}
	e.server.setScript(func(_ context.Context, body string) []string {
		switch lastUserText(body) {
		case "lead work":
			if strings.Contains(body, "call-parent-task") {
				return textTurnEvents("parent done")
			}
			return toolCallTurnEvents("call-parent-task", "task", `{"prompt":"helper work","subagent_type":"helper"}`)
		case "helper work":
			switch {
			case strings.Contains(body, "call-child-task"):
				return textTurnEvents("helper done")
			case strings.Contains(body, "call-alpha-before"):
				return toolCallTurnEvents("call-child-task", "task", `{"prompt":"nested task","subagent_type":"nested"}`)
			default:
				return toolCallTurnEvents("call-alpha-before", "alpha_start", `{}`)
			}
		case "after handoff":
			if strings.Contains(body, "call-alpha-after") {
				return textTurnEvents("after done")
			}
			return toolCallTurnEvents("call-alpha-after", "alpha_start", `{}`)
		case "nested work":
			return textTurnEvents("nested done")
		}
		return textTurnEvents("seen")
	})
	ctx := context.Background()
	r, err := e.open(ctx, agents.Author(), ownedStartNative("alpha", "alpha_start", probe))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	lead, err := r.createSession(ctx, e.dataDir, "lead")
	testassert.NoError(t, err)
	leadID := lead.Identity.SessionID
	submitThroughRuntime(t, r, leadID, "op-lead", "lead work")
	awaitOperation(t, r, leadID, "op-lead", harness.OperationSuccess)

	owned := awaitOwnedSession(t, r, leadID)
	deadline := time.Now().Add(10 * time.Second)
	for probe.childID == "" {
		testassert.Require(t, time.Now().Before(deadline), "before-handoff alpha start returned no child (executes %d, launch err %v); requests=%v; advertised=%v parent=%v; tool=%.400q", probe.executes.Load(), probe.launchErr, requestUserTexts(e), advertisedTools(t, e.server.bodyAt(1)), advertisedTools(t, e.server.bodyAt(0)), lastToolContent(e.server.bodyAt(3)))
		time.Sleep(10 * time.Millisecond)
	}
	testassert.Require(t, probe.executes.Load() == 1, "before-handoff alpha executes = %d, want one", probe.executes.Load())
	before := awaitBody(t, e, func(body string) bool {
		return lastUserText(body) == "helper work" && !strings.Contains(body, "call-alpha-before")
	})
	testassert.Require(t, !advertisesTool(t, before, "task") && advertisesTool(t, before, "alpha_start"), "owned Session advertisement = %v, want task hidden and alpha_start visible", advertisedTools(t, before))
	testassert.Require(t, modelSawRefusal(t, e, `Tool \"task\" is not available.`) || modelSawRefusal(t, e, "task is unavailable in an owned session"), "owned Session never received a task refusal")

	awaitIdleSession(t, r, owned)
	submitThroughRuntime(t, r, owned, "op-after", "after handoff")
	awaitOperation(t, r, owned, "op-after", harness.OperationSuccess)
	testassert.Require(t, probe.executes.Load() == 2, "alpha starts = %d, want a nested start before and after handoff", probe.executes.Load())
	after := awaitBody(t, e, func(body string) bool {
		return lastUserText(body) == "after handoff" && !strings.Contains(body, "call-alpha-after")
	})
	testassert.Require(t, !advertisesTool(t, after, "task") && advertisesTool(t, after, "alpha_start"), "post-handoff advertisement = %v, want task still hidden and alpha_start visible", advertisedTools(t, after))

	ids, err := e.storeIDs(ctx, r)
	testassert.NoError(t, err)
	testassert.Require(t, len(ids) == 4, "sessions = %v, want lead, owned, and two nested children", ids)
}

// requestUserTexts lists each recorded request's last user text.
func requestUserTexts(e *ownerEnv) []string {
	out := make([]string, 0, e.server.requests())
	for i := 0; i < e.server.requests(); i++ {
		out = append(out, lastUserText(e.server.bodyAt(i)))
	}
	return out
}

// lastToolContent returns the content of the newest tool message in one
// recorded request.
func lastToolContent(body string) string {
	var doc struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal([]byte(body), &doc) != nil {
		return ""
	}
	for i := len(doc.Messages) - 1; i >= 0; i-- {
		if doc.Messages[i].Role == "tool" {
			return string(doc.Messages[i].Content)
		}
	}
	return ""
}

// awaitOwnedSession waits for the one Session whose owner is the given root.
func awaitOwnedSession(t *testing.T, r *Melta, owner string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var ids []string
		err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
			rows, err := h.ListSessions(ctx)
			if err != nil {
				return err
			}
			for _, row := range rows {
				if row.Identity.OwnerSessionID == owner {
					ids = append(ids, row.Identity.SessionID)
				}
			}
			return nil
		})
		testassert.NoError(t, err)
		testassert.Require(t, len(ids) <= 1, "owner %q has %d Sessions, want one", owner, len(ids))
		if len(ids) == 1 {
			return ids[0]
		}
		testassert.Require(t, time.Now().Before(deadline), "owner %q never admitted an owned Session", owner)
		time.Sleep(10 * time.Millisecond)
	}
}

// advertisedTools returns the tool names carried by one recorded request.
func advertisedTools(t *testing.T, body string) []string {
	t.Helper()
	var doc struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	testassert.NoError(t, json.Unmarshal([]byte(body), &doc))
	names := make([]string, 0, len(doc.Tools))
	for _, tool := range doc.Tools {
		names = append(names, tool.Function.Name)
	}
	return names
}

// advertisesTool reports whether one recorded request advertised name.
func advertisesTool(t *testing.T, body, name string) bool {
	t.Helper()
	for _, advertised := range advertisedTools(t, body) {
		if advertised == name {
			return true
		}
	}
	return false
}

// modelSawRefusal reports whether any recorded request carried text.
func modelSawRefusal(t *testing.T, e *ownerEnv, text string) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		for i := 0; i < e.server.requests(); i++ {
			if strings.Contains(e.server.bodyAt(i), text) {
				return true
			}
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// storeIDs lists every durable Session identity.
func (e *ownerEnv) storeIDs(ctx context.Context, r *Melta) ([]string, error) {
	var rows []harness.SessionHeader
	err := r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var err error
		rows, err = h.ListSessions(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.Identity.SessionID)
	}
	return ids, nil
}
