package worker

import (
	"encoding/json"

	"github.com/Syntheresis/melta/extension"
)

// Manifest is the install metadata for an author worker.
type Manifest struct {
	EntryPoint string             `json:"entry_point"`
	Manifest   extension.Manifest `json:"manifest"`
}

// EncodeManifest renders author metadata and an install-relative entry.
func EncodeManifest(manifest extension.Manifest, entryPoint string) ([]byte, error) {
	return json.Marshal(Manifest{EntryPoint: entryPoint, Manifest: manifest})
}
