package melta

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

func warningIdentityConfig(endpoint string) string {
	return `{"providers":{"prov":` + nominalProvMember(endpoint, "", `{"m":{"name":"M","context_window":4096}}`) +
		`,"foreign":` + nominalProvMember(endpoint, "", `{"f":{"name":"F","context_window":4096}}`) + `}}`
}

func setupWarningsOf(t *testing.T, store *warningStore) []protocol.Warning {
	t.Helper()
	_, warnings := store.snapshot()
	out := make([]protocol.Warning, 0, len(warnings))
	for _, warning := range warnings {
		if warning.Source == setupSource {
			out = append(out, warning)
		}
	}
	return out
}

func TestSetupWarningsDoNotUseBaselineModelWithoutAgents(t *testing.T) {
	for _, doc := range []string{`{}`, `{"model":"missing/model"}`} {
		t.Run(doc, func(t *testing.T) {
			h := newServiceHarness(t)
			writeServiceFile(t, h.configPath, doc)
			svc := h.preparedService(context.Background())
			store := newWarningStore()
			svc.attachWarnings(store)
			_, err := svc.publish(context.Background())
			testassert.NoError(t, err)
			if warnings := setupWarningsOf(t, store); len(warnings) != 0 {
				t.Fatalf("unexpected setup warnings without Agents: %+v", warnings)
			}
		})
	}
}

func TestSetupWarningsUseAgentModelInsteadOfBaseline(t *testing.T) {
	for _, row := range []struct {
		baseline, primary string
		wantWarning       bool
	}{
		{"prov/missing", "prov/m", false},
		{"prov/m", "prov/missing", true},
	} {
		t.Run(row.primary, func(t *testing.T) {
			h := newServiceHarness(t)
			doc := strings.Replace(warningIdentityConfig("https://prov.test/v1"), "{", `{"model":"`+row.baseline+`",`, 1)
			writeServiceFile(t, h.configPath, doc)
			writeAgentDefinitions(t, h.configPath, `{"primary":{"model":"`+row.primary+`"}}`)
			svc := h.preparedService(context.Background(), agentsTestNative())
			store := newWarningStore()
			svc.attachWarnings(store)
			_, err := svc.publish(context.Background())
			testassert.NoError(t, err)
			warnings := setupWarningsOf(t, store)
			if row.wantWarning {
				testassert.Require(t, len(warnings) == 1 && warnings[0].Kind == setupModelUnavailable && strings.Contains(warnings[0].Message, row.primary), "warnings = %+v", warnings)
			} else {
				testassert.Require(t, len(warnings) == 0, "warnings = %+v", warnings)
			}
		})
	}
}

// TestSetupWarningsUseSuppliedPrimaryReference proves the named-model warning
// addresses only the supplied agents/agents.definitions/primary projection:
// a foreign resolver's first/only primary name never substitutes, while an
// opposing supplied model readiness still drives the warning.
func TestSetupWarningsUseSuppliedPrimaryReference(t *testing.T) {
	for _, row := range []struct {
		name          string
		suppliedModel string
		installAgents bool
		foreignModel  string
		wantKinds     []string
		wantMessage   string
	}{
		{"foreign-first ready supplied unavailable", "prov/missing", true, "foreign/f", []string{setupModelUnavailable}, "prov/missing"},
		{"foreign-first unavailable supplied ready", "prov/m", true, "foreign/missing", nil, ""},
		{"foreign-only ready", "", false, "foreign/f", nil, ""},
		{"foreign-only unavailable", "", false, "foreign/missing", nil, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			h := newServiceHarness(t)
			writeServiceFile(t, h.configPath, warningIdentityConfig("https://prov.test/v1"))
			if row.installAgents {
				writeAgentDefinitions(t, h.configPath, `{"primary":{"model":"`+row.suppliedModel+`"}}`)
			}
			enableFixturePlugin(t, h.configPath, "guestdefs")
			guest := namedResolverExtension("guestdefs", map[string]harness.AgentInputs{
				"primary": {
					Model:        model.ModelRef{Provider: "foreign", Model: strings.TrimPrefix(row.foreignModel, "foreign/")},
					Instructions: "foreign primary",
					AssemblyMode: "simple",
				},
			})
			plugins := []extension.Native{guest}
			if row.installAgents {
				plugins = append(plugins, agentsTestNative(guest))
			}
			svc := h.preparedService(context.Background(), plugins...)
			store := newWarningStore()
			svc.attachWarnings(store)
			_, err := svc.publish(context.Background())
			testassert.NoError(t, err)
			warnings := setupWarningsOf(t, store)
			var kinds []string
			for _, warning := range warnings {
				kinds = append(kinds, warning.Kind)
				if row.wantMessage != "" {
					testassert.Require(t, strings.Contains(warning.Message, row.wantMessage), "warning = %+v, want the supplied model %q", warning, row.wantMessage)
				}
			}
			testassert.Require(t, reflect.DeepEqual(kinds, row.wantKinds), "setup warnings = %+v, want kinds %v", warnings, row.wantKinds)
		})
	}
}
