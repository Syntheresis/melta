package runtime

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"

	"github.com/Syntheresis/melta/internal/atomicfs"
)

// InitializeFirstRunConfiguration creates the application's first-run
// configuration at path when it is absent, exclusively, and leaves any
// existing document untouched. The enabled-extension members are derived
// from the same ordered distribution list the owner's Open receives; the
// document chooses no model. Callers invoke it before Open; the owner itself
// never creates a missing document.
func InitializeFirstRunConfiguration(path string, extensionIDs []string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	type extensionSettings struct {
		Enabled bool `json:"enabled"`
	}
	type firstRunDocument struct {
		Providers  map[string]json.RawMessage   `json:"providers"`
		Extensions map[string]extensionSettings `json:"extensions"`
	}
	document := firstRunDocument{
		Providers:  map[string]json.RawMessage{},
		Extensions: make(map[string]extensionSettings, len(extensionIDs)),
	}
	for _, id := range extensionIDs {
		document.Extensions[id] = extensionSettings{Enabled: true}
	}
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	_, err = atomicfs.CreateExclusive(path, append(encoded, '\n'), 0o600)
	return err
}
