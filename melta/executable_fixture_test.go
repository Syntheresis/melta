package melta

// Executable fixture helpers: the shipped author worker fixture at
// extension/worker/testdata/author is installed as one package source (manifest
// and entry point) through the package manager.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/internal/testassert"
)

// authorFixtureManifest matches the manifest of the shipped author worker
// fixture's default entry.
func authorFixtureManifest() extension.Manifest {
	return extension.Manifest{ID: "author-fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`), Services: []string{extension.ContractCommands}}
}

// executablePackageSource writes one local source package: exactly its
// manifest and the contained placeholder executable, because the process
// spawn is faked.
func executablePackageSource(t *testing.T, manifest extension.Manifest, entryPoint string) string {
	t.Helper()
	source := t.TempDir()
	name := filepath.Base(entryPoint)
	if err := os.WriteFile(filepath.Join(source, name), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("source entry point: %v", err)
	}
	encoded, err := worker.EncodeManifest(manifest, name)
	testassert.NoError(t, err)
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), encoded, 0o600); err != nil {
		t.Fatalf("source manifest: %v", err)
	}
	return source
}

// installExecutablePackage installs one executable selection through the
// package manager.
func installExecutablePackage(t *testing.T, dataDir string, manifest extension.Manifest, entryPoint string) {
	t.Helper()
	if _, err := newPackageManager(dataDir).install(executablePackageSource(t, manifest, entryPoint)); err != nil {
		t.Fatalf("install package %q: %v", manifest.ID, err)
	}
}
