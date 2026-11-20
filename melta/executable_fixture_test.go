package melta

// Executable fixture helpers: the shipped author worker fixture at
// extension/worker/testdata/author is built once and installed as one
// package source (manifest and entry point) through the package manager.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/internal/testassert"
)

// authorFixtureManifest matches the manifest of the shipped author worker
// fixture's default entry.
func authorFixtureManifest() extension.Manifest {
	return extension.Manifest{ID: "author-fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
}

// buildAuthorWorker builds the shipped author worker fixture and returns the
// binary path.
func buildAuthorWorker(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "author-worker")
	command := exec.Command("go", "build", "-o", binary, "./testdata/author")
	command.Dir = filepath.Join("..", "extension", "worker")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("building author fixture: %v\n%s", err, output)
	}
	return binary
}

// executablePackageSource writes one local source package: exactly its
// manifest and entry point. An unreadable entry-point name gets a contained
// placeholder executable because the process spawn is faked.
func executablePackageSource(t *testing.T, manifest extension.Manifest, entryPoint string) string {
	t.Helper()
	source := t.TempDir()
	name := filepath.Base(entryPoint)
	data, err := os.ReadFile(entryPoint)
	if err != nil {
		data = []byte("#!/bin/sh\nexit 0\n")
	}
	if err := os.WriteFile(filepath.Join(source, name), data, 0o700); err != nil {
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
