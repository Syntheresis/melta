package melta

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
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

// TestSetupWarningsFollowTheBaselineWithoutExtensions proves the host setup
// group reads only the captured baseline choice: an unconfigured baseline
// reports no model (the fixture's keyless bundled provider stays connected),
// and a configured unavailable model reports its own kind without any
// extension present.
func TestSetupWarningsFollowTheBaselineWithoutExtensions(t *testing.T) {
	for _, row := range []struct {
		doc       string
		wantKinds []string
	}{
		{`{}`, []string{setupNoModelKind}},
		{`{"model":"missing/model"}`, []string{setupModelUnavailable}},
	} {
		t.Run(row.doc, func(t *testing.T) {
			h := newServiceHarness(t)
			writeServiceFile(t, h.configPath, row.doc)
			svc := h.preparedService(context.Background())
			store := newWarningStore()
			svc.attachWarnings(store)
			_, err := svc.publish(context.Background())
			testassert.NoError(t, err)
			var kinds []string
			for _, warning := range setupWarningsOf(t, store) {
				kinds = append(kinds, warning.Kind)
			}
			testassert.Require(t, reflect.DeepEqual(kinds, row.wantKinds), "setup warnings = %+v, want kinds %v", kinds, row.wantKinds)
		})
	}
}

// TestSetupWarningsUseBaselineModel proves the baseline model choice drives
// the unavailable warning: a usable model of a connected provider stays
// silent, and an unavailable baseline reference names itself in the warning.
func TestSetupWarningsUseBaselineModel(t *testing.T) {
	for _, row := range []struct {
		baseline  string
		wantModel bool
	}{
		{"prov/m", false},
		{"prov/missing", true},
	} {
		t.Run(row.baseline, func(t *testing.T) {
			h := newServiceHarness(t)
			doc := strings.Replace(warningIdentityConfig("https://prov.test/v1"), "{", `{"model":"`+row.baseline+`",`, 1)
			writeServiceFile(t, h.configPath, doc)
			svc := h.preparedService(context.Background())
			store := newWarningStore()
			svc.attachWarnings(store)
			_, err := svc.publish(context.Background())
			testassert.NoError(t, err)
			warnings := setupWarningsOf(t, store)
			if row.wantModel {
				testassert.Require(t, len(warnings) == 1 && warnings[0].Kind == setupModelUnavailable && strings.Contains(warnings[0].Message, row.baseline), "warnings = %+v", warnings)
			} else {
				testassert.Require(t, len(warnings) == 0, "warnings = %+v", warnings)
			}
		})
	}
}
