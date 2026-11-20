package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
)

// TestInitializeFirstRunConfigurationCreatesFirstRunDocument pins the
// Runtime host's first-run initialization: a missing document is created
// exclusively with the enabled-extension members derived from the supplied
// ordered distribution list, and an existing user document is never rewritten.
func TestInitializeFirstRunConfigurationCreatesFirstRunDocument(t *testing.T) {
	t.Run("missing document", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.json")
		testassert.NoError(t, InitializeFirstRunConfiguration(path, []string{"adaptation", "agents"}))
		data, err := os.ReadFile(path)
		testassert.NoError(t, err)
		var document struct {
			Providers  map[string]json.RawMessage `json:"providers"`
			Extensions map[string]struct {
				Enabled bool `json:"enabled"`
			} `json:"extensions"`
		}
		testassert.NoError(t, json.Unmarshal(data, &document))
		testassert.Require(t, len(document.Providers) == 0 && len(document.Extensions) == 2 &&
			document.Extensions["adaptation"].Enabled && document.Extensions["agents"].Enabled,
			"first-run document = %q, want the derived skeleton", data)
		info, err := os.Stat(path)
		testassert.NoError(t, err)
		testassert.Require(t, info.Mode().Perm() == 0o600, "first-run mode = %o, want 0600", info.Mode().Perm())
	})

	t.Run("no extensions", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.json")
		testassert.NoError(t, InitializeFirstRunConfiguration(path, nil))
		data, err := os.ReadFile(path)
		testassert.NoError(t, err)
		var document struct {
			Providers  map[string]json.RawMessage `json:"providers"`
			Extensions map[string]json.RawMessage `json:"extensions"`
		}
		testassert.NoError(t, json.Unmarshal(data, &document))
		testassert.Require(t, len(document.Providers) == 0 && len(document.Extensions) == 0, "first-run document = %q, want an empty extensions member", data)
	})

	t.Run("existing document", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.json")
		custom := []byte(`{"providers":{"custom":{}}}`)
		testassert.NoError(t, os.WriteFile(path, custom, 0o600))
		testassert.NoError(t, InitializeFirstRunConfiguration(path, []string{"adaptation", "agents"}))
		data, err := os.ReadFile(path)
		testassert.NoError(t, err)
		testassert.Require(t, string(data) == string(custom), "existing document = %q, want it untouched", data)
	})
}
