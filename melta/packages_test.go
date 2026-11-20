package melta

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/atomicfs"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/protocol"
)

func openPackageOwner(t *testing.T, dataDir, configPath string, authors ...extension.Extension) *Melta {
	t.Helper()
	owner, err := open(context.Background(), options{
		DataDir:       dataDir,
		Configuration: NewFileConfigurationSource(configPath),
		Credentials:   NewEnvironmentCredentialSource(filepath.Join(dataDir, ".env")),
		Extensions:    authors,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return owner
}

func packageCaller(t *testing.T, owner *Melta) Caller {
	t.Helper()
	caller, err := owner.Caller(CallerContext{Origin: harness.InputOriginUser, Scope: harnessScope})
	if err != nil {
		t.Fatalf("caller: %v", err)
	}
	return caller
}

func readSelectedRecord(t *testing.T, dataDir, extensionID string) packageSelection {
	t.Helper()
	selected, ok, err := readSelection(filepath.Join(dataDir, "extensions", extensionDataHash(extensionID)))
	if err != nil || !ok {
		t.Fatalf("read selected record = %+v, %v, want a selection", selected, err)
	}
	return selected
}

func packageRow(t *testing.T, owner *Melta, caller Caller, extensionID string) protocol.ExtensionPackage {
	t.Helper()
	rows, err := owner.ListExtensionPackages(context.Background(), caller)
	if err != nil {
		t.Fatalf("list packages: %v", err)
	}
	for _, row := range rows {
		if row.Id == extensionID {
			return row
		}
	}
	t.Fatalf("package %q is not listed: %+v", extensionID, rows)
	return protocol.ExtensionPackage{}
}

func packageTestManifest() extension.Manifest {
	return extension.Manifest{ID: "peer", Version: "1.0.0", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
}

// shAuthorFixtureSource builds the real author worker fixture into one
// self-contained package source directory with the given manifest version.
func shAuthorFixtureSource(t *testing.T, version string) (string, extension.Manifest) {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "worker")
	cmd := exec.Command("go", "build", "-o", binary, "./testdata/author")
	cmd.Dir = filepath.Join("..", "extension", "worker")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build author fixture: %v: %s", err, output)
	}
	manifest := extension.Manifest{ID: "author-fixture", Version: version, APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	source := t.TempDir()
	data, err := os.ReadFile(binary)
	testassert.NoError(t, err)
	testassert.NoError(t, os.WriteFile(filepath.Join(source, "worker"), data, 0o700))
	manifestBytes, err := worker.EncodeManifest(manifest, "worker")
	testassert.NoError(t, err)
	testassert.NoError(t, os.WriteFile(filepath.Join(source, "manifest.json"), manifestBytes, 0o600))
	return source, manifest
}

// shHostServicePlugin registers the host-service companion the author
// fixture calls during its activation.
func shHostServicePlugin() extension.Extension {
	return testNative("host-fixture", func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "host-service", Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "echo", Mode: extension.ModePure, Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: append(json.RawMessage(nil), args...)}, nil
			},
		}}}, nil)
		return nil, err
	})
}

// TestPackageInstallRejectsEscapingAndSymlinkedEntryPoints proves installation
// reads only a contained regular executable entry point and never writes the
// selected record for an escaping, absolute or symlinked source leaf.
func TestPackageInstallRejectsEscapingAndSymlinkedEntryPoints(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("package paths use the Linux protected filesystem traversal")
	}
	declaration := packageTestManifest()
	writeSource := func(t *testing.T, entryPoint string) string {
		t.Helper()
		source := t.TempDir()
		if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifestWithEntry(t, declaration, entryPoint), 0o600); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		return source
	}

	for _, test := range []struct {
		name       string
		entryPoint string
	}{
		{name: "parent escape", entryPoint: "../escaped-worker"},
		{name: "absolute", entryPoint: "/tmp/escaped-worker"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataDir := t.TempDir()
			source := writeSource(t, test.entryPoint)
			outside := filepath.Join(filepath.Dir(source), "escaped-worker")
			if err := os.WriteFile(outside, []byte("outside"), 0o700); err != nil {
				t.Fatalf("outside sentinel: %v", err)
			}
			_, err := newPackageManager(dataDir).install(source)
			testassert.Require(t, errors.Is(err, harness.ErrInvalid), "install entry point %q = %v, want invalid", test.entryPoint, err)
			mustNotExist(t, filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected"), "an escaping entry point created a selection")
			if data, err := os.ReadFile(outside); err != nil || string(data) != "outside" {
				t.Fatalf("outside sentinel = %q, %v", data, err)
			}
		})
	}

	t.Run("symlinked entry point", func(t *testing.T) {
		dataDir := t.TempDir()
		source := writeSource(t, "worker")
		outsideDir := t.TempDir()
		outside := filepath.Join(outsideDir, "outside-worker")
		if err := os.WriteFile(outside, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
			t.Fatalf("outside executable: %v", err)
		}
		if err := os.Symlink(outside, filepath.Join(source, "worker")); err != nil {
			t.Fatalf("symlink entry point: %v", err)
		}
		_, err := newPackageManager(dataDir).install(source)
		testassert.Require(t, errors.Is(err, harness.ErrInvalid), "install symlinked entry point = %v, want invalid", err)
		mustNotExist(t, filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected"), "a symlinked entry point created a selection")
		if data, err := os.ReadFile(outside); err != nil || string(data) != "#!/bin/sh\nexit 0\n" {
			t.Fatalf("outside executable = %q, %v", data, err)
		}
	})

	t.Run("symlinked intermediate directory outside", func(t *testing.T) {
		dataDir := t.TempDir()
		source := writeSource(t, filepath.Join("bin", "worker"))
		outsideDir := t.TempDir()
		outside := filepath.Join(outsideDir, "outside-worker")
		if err := os.WriteFile(outside, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
			t.Fatalf("outside executable: %v", err)
		}
		if err := os.Symlink(outsideDir, filepath.Join(source, "bin")); err != nil {
			t.Fatalf("symlink intermediate directory: %v", err)
		}
		_, err := newPackageManager(dataDir).install(source)
		testassert.Require(t, errors.Is(err, harness.ErrInvalid), "install symlinked intermediate directory = %v, want invalid", err)
		mustNotExist(t, filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected"), "a symlinked intermediate directory created a selection")
		if data, err := os.ReadFile(outside); err != nil || string(data) != "#!/bin/sh\nexit 0\n" {
			t.Fatalf("outside executable = %q, %v", data, err)
		}
	})

	t.Run("not executable", func(t *testing.T) {
		dataDir := t.TempDir()
		source := writeSource(t, "worker")
		if err := os.WriteFile(filepath.Join(source, "worker"), []byte("plain"), 0o600); err != nil {
			t.Fatalf("write non-executable: %v", err)
		}
		_, err := newPackageManager(dataDir).install(source)
		testassert.Require(t, errors.Is(err, harness.ErrInvalid), "install non-executable entry point = %v, want invalid", err)
		mustNotExist(t, filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected"), "a non-executable entry point created a selection")
	})
}

// manifestWithEntry encodes one deliberately explicit entry point.
func manifestWithEntry(t *testing.T, manifest extension.Manifest, entryPoint string) []byte {
	t.Helper()
	encoded, err := worker.EncodeManifest(manifest, entryPoint)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return encoded
}

// TestPackageInstallAcceptsSymlinkedSourceAncestor proves the operator's
// source root is opened once as a root, so a symlinked path to the package
// directory is accepted while the two consumed files stay root-confined.
func TestPackageInstallAcceptsSymlinkedSourceAncestor(t *testing.T) {
	declaration := packageTestManifest()
	realRoot := t.TempDir()
	source := filepath.Join(realRoot, "package")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if err := writePackageSourceFiles(t, source, declaration, "1.0.0", "worker"); err != nil {
		t.Fatalf("write source: %v", err)
	}
	aliasRoot := t.TempDir()
	alias := filepath.Join(aliasRoot, "alias")
	if err := os.Symlink(realRoot, alias); err != nil {
		t.Fatalf("symlink source ancestor: %v", err)
	}
	dataDir := t.TempDir()
	if _, err := newPackageManager(dataDir).install(filepath.Join(alias, "package")); err != nil {
		t.Fatalf("install through a symlinked source ancestor: %v", err)
	}
	testassert.Equal(t, readSelectedRecord(t, dataDir, declaration.ID).Version, "1.0.0")
}

// TestPackageInstallAcceptsHardlinkedEntryPoint proves a hardlinked source
// entry point is not given a second refusal rule beyond the root's path
// containment.
func TestPackageInstallAcceptsHardlinkedEntryPoint(t *testing.T) {
	declaration := packageTestManifest()
	outside := filepath.Join(t.TempDir(), "outside-worker")
	if err := os.WriteFile(outside, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("outside executable: %v", err)
	}
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifestWithEntry(t, declaration, "worker"), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.Link(outside, filepath.Join(source, "worker")); err != nil {
		t.Fatalf("hardlink entry point: %v", err)
	}
	dataDir := t.TempDir()
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install hardlinked entry point: %v", err)
	}
	testassert.Equal(t, readSelectedRecord(t, dataDir, declaration.ID).Version, "1.0.0")
}

// TestPackageInstallAcceptsEntrySymlinkInsideSource proves a source entry
// symlink that resolves inside the opened root is accepted.
func TestPackageInstallAcceptsEntrySymlinkInsideSource(t *testing.T) {
	declaration := packageTestManifest()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifestWithEntry(t, declaration, "worker"), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "real-worker"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("real entry point: %v", err)
	}
	if err := os.Symlink("real-worker", filepath.Join(source, "worker")); err != nil {
		t.Fatalf("inside entry symlink: %v", err)
	}
	dataDir := t.TempDir()
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install entry symlink inside source: %v", err)
	}
	testassert.Equal(t, readSelectedRecord(t, dataDir, declaration.ID).Version, "1.0.0")
}

// TestPackageInstallRejectsBindingSchemaExternalReference proves installation
// compiles every binding-definition schema, so a non-local $ref is refused
// before any selection exists.
func TestPackageInstallRejectsSettingsSchemaExternalReference(t *testing.T) {
	declaration := packageTestManifest()
	declaration.ID = "bindingref"
	declaration.SettingsSchema = json.RawMessage(`{"$ref":"https://example.invalid/schema.json"}`)
	dataDir := t.TempDir()
	source := t.TempDir()
	testassert.NoError(t, writePackageSourceFiles(t, source, declaration, declaration.Version, "worker"))
	_, err := newPackageManager(dataDir).install(source)
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "install settings schema external reference = %v, want invalid", err)
	mustNotExist(t, filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected"), "a settings schema external reference created a selection")
}

func TestPackageInstallAcceptsRequestedServiceWithoutProvider(t *testing.T) {
	declaration := packageTestManifest()
	declaration.ID = "consumer"
	declaration.Services = []string{"remote"}
	dataDir := t.TempDir()
	source := t.TempDir()
	testassert.NoError(t, writePackageSourceFiles(t, source, declaration, declaration.Version, "worker"))
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install: %v", err)
	}
	testassert.Equal(t, readSelectedRecord(t, dataDir, declaration.ID).Version, "1.0.0")
}

// TestPackageSelectedDeclarationFailureIsUnusable proves a selected copy
// whose declaration fails compilation at load is recorded unusable: an
// enabled section rejects the build and a disabled one is ignored.
func TestPackageSelectedDeclarationFailureIsUnusable(t *testing.T) {
	declaration := packageTestManifest()
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	source := t.TempDir()
	if err := writePackageSourceFiles(t, source, declaration, "1.0.0", "worker"); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install: %v", err)
	}
	selected := readSelectedRecord(t, dataDir, declaration.ID)
	manifestPath := filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "packages", selected.Copy, "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read installed manifest: %v", err)
	}
	var manifest worker.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("decode installed manifest: %v", err)
	}
	manifest.Manifest.SettingsSchema = json.RawMessage(`"invalid"`)
	corrupt, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode corrupt manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, corrupt, 0o600); err != nil {
		t.Fatalf("write corrupt manifest: %v", err)
	}

	writeServiceFile(t, configPath, `{"extensions":{"peer":{"enabled":true}}}`)
	owner := openPackageOwner(t, dataDir, configPath)
	caller := packageCaller(t, owner)
	catalog, err := owner.ListExtensions(context.Background(), caller)
	testassert.NoError(t, err)
	testassert.Require(t, len(catalog) == 1 && catalog[0].ExtensionId == declaration.ID && catalog[0].InactiveReason != nil, "enabled invalid package = %+v, want inactive reason", catalog)
	_, err = owner.Health(context.Background(), caller)
	testassert.NoError(t, err)
	testassert.NoError(t, owner.Close(context.Background()))

	writeServiceFile(t, configPath, `{"extensions":{"peer":{"enabled":false}}}`)
	owner = openPackageOwner(t, dataDir, configPath)
	caller = packageCaller(t, owner)
	row := packageRow(t, owner, caller, declaration.ID)
	testassert.Equal(t, row.LoadedVersion, "")
	testassert.Equal(t, row.SelectedVersion, "1.0.0")
	catalog, err = owner.ListExtensions(context.Background(), caller)
	testassert.NoError(t, err)
	testassert.Require(t, len(catalog) == 0, "disabled invalid package = %+v, want no active or inactive entry", catalog)
	testassert.NoError(t, owner.Close(context.Background()))
}

// TestPackageInstallRejectsPlantedDestinationSymlink proves a symlinked
// package destination is refused instead of followed into an outside tree.
func TestPackageInstallRejectsPlantedDestinationSymlink(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("package paths use the Linux protected filesystem traversal")
	}
	declaration := packageTestManifest()
	dataDir := t.TempDir()
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifestWithEntry(t, declaration, "worker"), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "worker"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write entry point: %v", err)
	}
	extensionDir := filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID))
	if err := os.MkdirAll(extensionDir, 0o700); err != nil {
		t.Fatalf("create extension directory: %v", err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(extensionDir, "packages")); err != nil {
		t.Fatalf("plant destination symlink: %v", err)
	}
	_, err := newPackageManager(dataDir).install(source)
	testassert.Require(t, err != nil, "install with a symlinked destination succeeded")
	entries, readErr := os.ReadDir(outside)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("outside destination = %v, %v, want untouched", entries, readErr)
	}
	mustNotExist(t, filepath.Join(extensionDir, "selected"), "a symlinked destination created a selection")
}

// TestPackageInstallPublicationFailureKeepsPreviousSelection proves a failed
// selected-record publication leaves the previous selection intact and removes
// the partial copy.
func TestPackageInstallPublicationFailureKeepsPreviousSelection(t *testing.T) {
	declaration := packageTestManifest()
	dataDir := t.TempDir()
	v1 := t.TempDir()
	if err := writePackageSourceFiles(t, v1, declaration, "1.0.0", "v1-worker"); err != nil {
		t.Fatalf("write v1 source: %v", err)
	}
	if _, err := newPackageManager(dataDir).install(v1); err != nil {
		t.Fatalf("install v1: %v", err)
	}
	selectedPath := filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected")
	before, err := os.ReadFile(selectedPath)
	if err != nil {
		t.Fatalf("read selected: %v", err)
	}
	v2 := t.TempDir()
	if err := writePackageSourceFiles(t, v2, declaration, "2.0.0", "v2-worker"); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}

	previousSync := atomicfs.SyncFileFunc
	atomicfs.SyncFileFunc = func(*os.File) error { return errors.New("injected publication failure") }
	_, err = newPackageManager(dataDir).install(v2)
	atomicfs.SyncFileFunc = previousSync
	testassert.Require(t, err != nil, "install with a failed publication succeeded")
	after, err := os.ReadFile(selectedPath)
	if err != nil {
		t.Fatalf("read selected after failure: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("selected record = %s, want the previous %s", after, before)
	}
	first := readSelectedRecord(t, dataDir, declaration.ID)
	testassert.Equal(t, first.Version, "1.0.0")
	entries, readErr := os.ReadDir(filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "packages"))
	testassert.NoError(t, readErr)
	testassert.Require(t, len(entries) == 1 && entries[0].Name() == first.Copy, "package copies = %v, want only the selected copy %q", entries, first.Copy)

	if _, err := newPackageManager(dataDir).install(v2); err != nil {
		t.Fatalf("install v2 after restoring publication: %v", err)
	}
	testassert.Equal(t, readSelectedRecord(t, dataDir, declaration.ID).Version, "2.0.0")
}

// TestPackageInstallCreatesDistinctCopies proves install, update and
// reinstall each write their own copy and select the latest one.
func TestPackageInstallCreatesDistinctCopies(t *testing.T) {
	declaration := packageTestManifest()
	dataDir := t.TempDir()
	v1 := t.TempDir()
	if err := writePackageSourceFiles(t, v1, declaration, "1.0.0", "v1-worker"); err != nil {
		t.Fatalf("write v1 source: %v", err)
	}
	v2 := t.TempDir()
	if err := writePackageSourceFiles(t, v2, declaration, "2.0.0", "v2-worker"); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}
	manager := newPackageManager(dataDir)
	copies := map[string]bool{}
	for _, step := range []struct {
		source  string
		version string
	}{
		{v1, "1.0.0"},
		{v2, "2.0.0"},
		{v2, "2.0.0"},
	} {
		if _, err := manager.install(step.source); err != nil {
			t.Fatalf("install %s: %v", step.version, err)
		}
		selected := readSelectedRecord(t, dataDir, declaration.ID)
		testassert.Equal(t, selected.Version, step.version)
		testassert.Require(t, !copies[selected.Copy], "install reused copy %q", selected.Copy)
		copies[selected.Copy] = true
	}
	entries, err := os.ReadDir(filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "packages"))
	testassert.NoError(t, err)
	testassert.Require(t, len(entries) == len(copies), "package copies = %v, want one per install", entries)
	for _, entry := range entries {
		testassert.Require(t, copies[entry.Name()], "unexpected copy directory %q", entry.Name())
	}
}

// TestPackageInstallSyncsCopyDirectoriesAndParentChain proves a nested entry
// point's created directories and its copy directory are each synced before
// the parent chain and the selection write.
func TestPackageInstallSyncsCopyDirectoriesAndParentChain(t *testing.T) {
	declaration := packageTestManifest()
	dataDir := t.TempDir()
	source := t.TempDir()
	if err := writePackageSourceFiles(t, source, declaration, "1.0.0", filepath.Join("bin", "nested", "worker")); err != nil {
		t.Fatalf("write source: %v", err)
	}
	var mu sync.Mutex
	var synced []string
	previous := atomicfs.SyncDirFunc
	atomicfs.SyncDirFunc = func(dir string) error {
		mu.Lock()
		synced = append(synced, dir)
		mu.Unlock()
		return nil
	}
	defer func() { atomicfs.SyncDirFunc = previous }()
	check := func(t *testing.T, trace []string, copyID string) {
		t.Helper()
		extensionDir := filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID))
		packagesDir := filepath.Join(extensionDir, "packages")
		packageDir := filepath.Join(packagesDir, copyID)
		testassert.Require(t, len(trace) == 8, "syncs = %v, want the entry point's directories, the copy directory, the parent chain, and the selection directory", trace)
		testassert.Equal(t, trace[0], filepath.Join(packageDir, "bin", "nested"))
		testassert.Equal(t, trace[1], filepath.Join(packageDir, "bin"))
		testassert.Equal(t, trace[2], packageDir)
		testassert.Equal(t, trace[3], packagesDir)
		testassert.Equal(t, trace[4], extensionDir)
		testassert.Equal(t, trace[5], filepath.Join(dataDir, "extensions"))
		testassert.Equal(t, trace[6], dataDir)
		testassert.Equal(t, trace[7], extensionDir)
	}
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install: %v", err)
	}
	first := readSelectedRecord(t, dataDir, declaration.ID)
	mu.Lock()
	trace := append([]string(nil), synced...)
	mu.Unlock()
	check(t, trace, first.Copy)

	mu.Lock()
	synced = nil
	mu.Unlock()
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("reinstall: %v", err)
	}
	second := readSelectedRecord(t, dataDir, declaration.ID)
	mu.Lock()
	trace = append([]string(nil), synced...)
	mu.Unlock()
	testassert.Require(t, first.Copy != second.Copy, "reinstall reused copy %q", second.Copy)
	check(t, trace, second.Copy)
}

// TestPackageRemoveSyncsExtensionDirectory proves a successful selection
// unlink is followed by the extension directory sync.
func TestPackageRemoveSyncsExtensionDirectory(t *testing.T) {
	declaration := packageTestManifest()
	dataDir := t.TempDir()
	source := t.TempDir()
	if err := writePackageSourceFiles(t, source, declaration, "1.0.0", "worker"); err != nil {
		t.Fatalf("write source: %v", err)
	}
	manager := newPackageManager(dataDir)
	if _, err := manager.install(source); err != nil {
		t.Fatalf("install: %v", err)
	}
	var synced []string
	previous := atomicfs.SyncDirFunc
	atomicfs.SyncDirFunc = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	defer func() { atomicfs.SyncDirFunc = previous }()
	if err := manager.remove(declaration.ID); err != nil {
		t.Fatalf("remove: %v", err)
	}
	extensionDir := filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID))
	testassert.Require(t, len(synced) == 1 && synced[0] == extensionDir, "syncs = %v, want the extension directory", synced)
	mustNotExist(t, filepath.Join(extensionDir, "selected"), "remove retained the selected record")
}

// TestInstalledWorkerPublicExtensionAcceptance exercises an actually installed
// worker through the public extension surface: discovery, concrete Harness- and
// Session-target invocations, exact declared results, package removal, a
// deliberate restart over the same store and configuration, retained history
// without replay, and Session-data cleanup that preserves the extension cache
// and configuration bytes.
func TestInstalledWorkerPublicExtensionAcceptance(t *testing.T) {
	source, manifest := shAuthorFixtureSource(t, "1")
	eachProductionStore(t, func(t *testing.T, e *productionEnv) {
		ctx := context.Background()
		if _, err := newPackageManager(e.dataDir).install(source); err != nil {
			t.Fatalf("install author fixture package: %v", err)
		}
		// Seed retained history while the installed author stays disabled: the
		// author fixture refuses every admission while active.
		seed, err := e.openPlugins(ctx, shHostServicePlugin())
		if err != nil {
			t.Fatalf("open seeded owner: %v", err)
		}
		seedCaller := testCaller(seed)
		ref := protocol.ModelRef("prov/m")
		prompt := "author public extension"
		tools := []string{}
		inputs := &protocol.SessionInputs{Model: &ref, Prompt: &prompt, Tools: &tools}
		session, err := seed.CreateSession(ctx, seedCaller, e.workspace("author-public"), inputs)
		testassert.NoError(t, err)
		e.server.setScript(func(context.Context, string) []string { return textTurnEvents("author done") })
		_, err = seed.SubmitSession(ctx, seedCaller, session.SessionId, commandSubmitRequest(t, "op-author-public", "author work"))
		testassert.NoError(t, err)
		awaitOperation(t, seed, session.SessionId, "op-author-public", harness.OperationSuccess)
		testassert.NoError(t, seed.Close(ctx))

		enableFixturePlugin(t, e.configPath, manifest.ID)
		owner, err := e.openPlugins(ctx, shHostServicePlugin())
		if err != nil {
			t.Fatalf("open installed author: %v", err)
		}
		defer func() { _ = owner.Close(ctx) }()
		caller := testCaller(owner)

		catalog, err := owner.ListExtensions(ctx, caller)
		testassert.NoError(t, err)
		var entry *protocol.RegisteredExtension
		for i := range catalog {
			if catalog[i].ExtensionId == manifest.ID {
				entry = &catalog[i]
			}
		}
		testassert.Require(t, entry != nil && entry.InactiveReason == nil && entry.Version == "1", "catalog omitted the installed author: %+v", catalog)
		var sawOperation bool
		for _, exposed := range entry.Operations {
			sawOperation = sawOperation || (exposed.Method.Name == "operation" && exposed.Target == protocol.RegisteredExtensionOperationTargetHarness)
		}
		testassert.Require(t, sawOperation && slices.Contains(entry.Tools, "worker-tool"), "catalog = operations %+v tools %v, want the registered operation and tool projected", entry.Operations, entry.Tools)

		rawValue := "{\n  \"big\": 9007199254740993,\n  \"escaped\": \"line\\n\\t\\\"quote\\\" \\\\ slash\"\n}"
		raw, err := owner.InvokeExtension(ctx, caller, fixtureRequest(manifest.ID, "operation", harnessTarget(), rawValue, "req-author-raw"))
		testassert.NoError(t, err)
		testassert.Require(t, raw.Failure == nil && string(raw.Value) == rawValue, "raw author result = %+v, want the exact raw bytes", raw)

		configBefore, err := os.ReadFile(e.configPath)
		testassert.NoError(t, err)
		testassert.NoError(t, owner.RemoveExtensionPackage(ctx, caller, manifest.ID))
		configAfterRemoval, err := os.ReadFile(e.configPath)
		testassert.NoError(t, err)
		testassert.Require(t, string(configAfterRemoval) == string(configBefore), "package removal rewrote the configuration")
		testassert.NoError(t, owner.Close(ctx))

		restarted, err := e.openPlugins(ctx, shHostServicePlugin())
		if err != nil {
			t.Fatalf("reopen after package removal: %v", err)
		}
		defer func() { _ = restarted.Close(ctx) }()
		restartedCaller := testCaller(restarted)
		catalog, err = restarted.ListExtensions(ctx, restartedCaller)
		testassert.NoError(t, err)
		for _, candidate := range catalog {
			testassert.Require(t, candidate.ExtensionId != manifest.ID, "removed author is still cataloged: %+v", candidate)
		}
		_, err = restarted.InvokeExtension(ctx, restartedCaller, fixtureRequest(manifest.ID, "operation", sessionTarget(session.SessionId), `{}`, "req-author-removed"))
		testassert.Require(t, errors.Is(err, harness.ErrNotFound), "invocation after package removal = %v, want not-found", err)

		page, err := restarted.GetHistory(ctx, restartedCaller, session.SessionId, nil)
		testassert.NoError(t, err)
		testassert.Require(t, len(page.Items) != 0, "reopened history is empty")
		_, err = restarted.ArchiveSession(ctx, restartedCaller, session.SessionId)
		testassert.NoError(t, err)
		deletion, err := restarted.DeleteSession(ctx, restartedCaller, session.SessionId)
		testassert.NoError(t, err)
		testassert.Require(t, len(deletion.CleanupFailures) == 0, "deletion cleanup failures = %+v", deletion.CleanupFailures)
		mustNotExist(t, filepath.Join(e.dataDir, "code", session.SessionId), "the deleted Session tree")
		configAfter, err := os.ReadFile(e.configPath)
		testassert.NoError(t, err)
		testassert.Require(t, string(configAfter) == string(configBefore), "Session deletion or restart rewrote the configuration")
		testassert.NoError(t, restarted.Close(ctx))
	})
}

// workerExtensionInput finds the admitted input of one Operation in a history
// page.
func workerExtensionInput(page protocol.HistoryPage, operationID string) (protocol.InputItem, bool) {
	for _, item := range page.Items {
		input, err := item.AsInputItem()
		if err != nil {
			continue
		}
		if input.OperationId != nil && *input.OperationId == operationID {
			return input, true
		}
	}
	return protocol.InputItem{}, false
}

// writePackageSourceFiles writes one self-contained source package with a
// distinct declaration version and an executable placeholder entry point.
func writePackageSourceFiles(t *testing.T, dir string, declaration extension.Manifest, version, entry string) error {
	t.Helper()
	versioned := declaration
	versioned.Version = version
	manifest, err := worker.EncodeManifest(versioned, entry)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), manifest, 0o600); err != nil {
		return err
	}
	entryPath := filepath.Join(dir, entry)
	if err := os.MkdirAll(filepath.Dir(entryPath), 0o700); err != nil {
		return err
	}
	return os.WriteFile(entryPath, []byte("#!/bin/sh\nexit 0\n"), 0o700)
}

// TestPackageInstallRemoveReinstallPreservesConfiguration proves package
// lifecycle operations never rewrite the configuration document.
func TestPackageInstallRemoveReinstallPreservesConfiguration(t *testing.T) {
	declaration := packageTestManifest()
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, `{"extensions":{"peer":{"enabled":false},"uninstalled":{"enabled":true,"unknown":9007199254740993}}}`)
	v1 := t.TempDir()
	if err := writePackageSourceFiles(t, v1, declaration, "1.0.0", "v1-worker"); err != nil {
		t.Fatalf("write v1 source: %v", err)
	}
	if _, err := newPackageManager(dataDir).install(v1); err != nil {
		t.Fatalf("install v1: %v", err)
	}
	owner := openPackageOwner(t, dataDir, configPath)
	caller := packageCaller(t, owner)
	before, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read configuration: %v", err)
	}
	testassert.NoError(t, owner.RemoveExtensionPackage(context.Background(), caller, declaration.ID))
	afterRemove, err := os.ReadFile(configPath)
	if err != nil || string(before) != string(afterRemove) {
		t.Fatalf("configuration after remove = %q, %v, want %q", afterRemove, err, before)
	}
	v2 := t.TempDir()
	if err := writePackageSourceFiles(t, v2, declaration, "2.0.0", "v2-worker"); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}
	row, err := owner.InstallExtensionPackage(context.Background(), caller, v2)
	if err != nil {
		t.Fatalf("install v2: %v", err)
	}
	testassert.Equal(t, row.Id, declaration.ID)
	afterInstall, err := os.ReadFile(configPath)
	if err != nil || string(before) != string(afterInstall) {
		t.Fatalf("configuration after install = %q, %v, want %q", afterInstall, err, before)
	}
	testassert.NoError(t, owner.Close(context.Background()))
}

// TestPackageLoadedSelectedVersionsAcrossRestart proves installation changes
// only the next-start selection; restart selects the installed code.
func TestPackageLoadedSelectedVersionsAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, `{"extensions":{"fixture":{"enabled":false}}}`)
	declaration := extension.Manifest{ID: "fixture", Version: "1.0.0", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	v1 := t.TempDir()
	if err := writePackageSourceFiles(t, v1, declaration, "1.0.0", "v1-worker"); err != nil {
		t.Fatalf("write v1 source: %v", err)
	}
	if _, err := newPackageManager(dataDir).install(v1); err != nil {
		t.Fatalf("install v1: %v", err)
	}
	owner := openPackageOwner(t, dataDir, configPath)
	caller := packageCaller(t, owner)
	row := packageRow(t, owner, caller, "fixture")
	testassert.Equal(t, row.LoadedVersion, "1.0.0")
	testassert.Equal(t, row.SelectedVersion, "1.0.0")
	v2 := t.TempDir()
	if err := writePackageSourceFiles(t, v2, declaration, "2.0.0", "v2-worker"); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}
	installed, err := owner.InstallExtensionPackage(context.Background(), caller, v2)
	if err != nil {
		t.Fatalf("install v2: %v", err)
	}
	testassert.Equal(t, installed.LoadedVersion, "1.0.0")
	testassert.Equal(t, installed.SelectedVersion, "2.0.0")
	row = packageRow(t, owner, caller, "fixture")
	testassert.Equal(t, row.LoadedVersion, "1.0.0")
	testassert.Equal(t, row.SelectedVersion, "2.0.0")
	testassert.NoError(t, owner.Close(context.Background()))

	restarted := openPackageOwner(t, dataDir, configPath)
	restartedCaller := packageCaller(t, restarted)
	row = packageRow(t, restarted, restartedCaller, "fixture")
	testassert.Equal(t, row.LoadedVersion, "2.0.0")
	testassert.Equal(t, row.SelectedVersion, "2.0.0")
	testassert.NoError(t, restarted.Close(context.Background()))
}

// TestPackageLifecycleUsesCanonicalDataRoot proves package load, install,
// list, restart and remove all use the canonical root captured at open when
// the operator-supplied DataDir is a symlink: the real and alias paths contend
// for one owner, and repointing the alias cannot redirect later selections.
func TestPackageLifecycleUsesCanonicalDataRoot(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("package paths use the Linux protected filesystem traversal")
	}
	realRoot := t.TempDir()
	alias := filepath.Join(t.TempDir(), "data")
	testassert.NoError(t, os.Symlink(realRoot, alias))
	replacement := t.TempDir()
	configPath := filepath.Join(realRoot, "config.json")
	writeServiceFile(t, configPath, `{"extensions":{"fixture":{"enabled":false}}}`)
	declaration := extension.Manifest{ID: "fixture", Version: "1.0.0", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	v1 := t.TempDir()
	if err := writePackageSourceFiles(t, v1, declaration, "1.0.0", "v1-worker"); err != nil {
		t.Fatalf("write v1 source: %v", err)
	}
	if _, err := newPackageManager(realRoot).install(v1); err != nil {
		t.Fatalf("install v1: %v", err)
	}

	owner := openPackageOwner(t, alias, configPath)
	want := canonicalFixturePath(t, realRoot)
	testassert.Equal(t, owner.dataDir, want)
	caller := packageCaller(t, owner)
	row := packageRow(t, owner, caller, "fixture")
	testassert.Equal(t, row.LoadedVersion, "1.0.0")
	testassert.Equal(t, row.SelectedVersion, "1.0.0")

	// The real path and the alias are one ownership identity.
	{
		_, err := open(context.Background(), options{
			DataDir:       realRoot,
			Configuration: NewFileConfigurationSource(configPath),
			Credentials:   NewEnvironmentCredentialSource(filepath.Join(realRoot, ".env")),
		})
		testassert.Require(t, errors.Is(err, ErrOwned), "real-path contender open = %v, want ErrOwned", err)
	}

	// Repointing the operator's alias cannot redirect the open owner's writes.
	testassert.NoError(t, os.Remove(alias))
	testassert.NoError(t, os.Symlink(replacement, alias))
	v2 := t.TempDir()
	if err := writePackageSourceFiles(t, v2, declaration, "2.0.0", "v2-worker"); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}
	installed, err := owner.InstallExtensionPackage(context.Background(), caller, v2)
	testassert.NoError(t, err)
	testassert.Equal(t, installed.LoadedVersion, "1.0.0")
	testassert.Equal(t, installed.SelectedVersion, "2.0.0")
	testassert.Equal(t, owner.dataDir, want)
	testassert.Equal(t, readSelectedRecord(t, realRoot, "fixture").Version, "2.0.0")
	{
		entries, err := os.ReadDir(replacement)
		testassert.NoError(t, err)
		testassert.Require(t, len(entries) == 0, "repointed alias target = %v, want no redirected package writes", entries)
	}
	testassert.NoError(t, owner.Close(context.Background()))

	restarted := openPackageOwner(t, realRoot, configPath)
	restartedCaller := packageCaller(t, restarted)
	row = packageRow(t, restarted, restartedCaller, "fixture")
	testassert.Equal(t, row.LoadedVersion, "2.0.0")
	testassert.Equal(t, row.SelectedVersion, "2.0.0")
	testassert.NoError(t, restarted.RemoveExtensionPackage(context.Background(), restartedCaller, "fixture"))
	mustNotExist(t, filepath.Join(realRoot, "extensions", extensionDataHash("fixture"), "selected"), "remove retained the selected record")
	{
		entries, err := os.ReadDir(replacement)
		testassert.NoError(t, err)
		testassert.Require(t, len(entries) == 0, "repointed alias target = %v, want no redirected selection writes", entries)
	}
	testassert.NoError(t, restarted.Close(context.Background()))
}

// TestPackageDisabledLoadedUpdateThenReloadUsesLoadedCopy proves a reload
// after an update activates the loaded code, not the newly selected bytes.
func TestPackageDisabledLoadedUpdateThenReloadUsesLoadedCopy(t *testing.T) {
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, `{"extensions":{"author-fixture":{"enabled":false},"host-fixture":{"enabled":true}}}`)
	v1Source, manifest := shAuthorFixtureSource(t, "1")
	if _, err := newPackageManager(dataDir).install(v1Source); err != nil {
		t.Fatalf("install v1: %v", err)
	}
	owner := openPackageOwner(t, dataDir, configPath, shHostServicePlugin())
	caller := packageCaller(t, owner)
	v2Source := t.TempDir()
	if err := writePackageSourceFiles(t, v2Source, extension.Manifest{ID: "author-fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}, "2.0.0", "v2-worker"); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}
	if _, err := owner.InstallExtensionPackage(context.Background(), caller, v2Source); err != nil {
		t.Fatalf("install v2: %v", err)
	}
	writeServiceFile(t, configPath, `{"extensions":{"author-fixture":{"enabled":true},"host-fixture":{"enabled":true}}}`)
	if _, err := owner.Reload(context.Background(), caller); err != nil {
		t.Fatalf("reload: %v", err)
	}
	row := packageRow(t, owner, caller, manifest.ID)
	testassert.Equal(t, row.LoadedVersion, "1")
	testassert.Equal(t, row.SelectedVersion, "2.0.0")
	snapshot := owner.config.current()
	var activated *nativeActivation
	for _, activation := range snapshot.native.activations {
		if activation != nil && activation.id == manifest.ID {
			activated = activation
		}
	}
	testassert.Require(t, activated != nil && activated.version == "1", "reload activated %+v, want the loaded copy version 1; inactive=%+v", activated, snapshot.native.inactive)
	catalog, err := owner.ListExtensions(context.Background(), caller)
	testassert.NoError(t, err)
	var entry *protocol.RegisteredExtension
	for i := range catalog {
		if catalog[i].ExtensionId == manifest.ID {
			entry = &catalog[i]
		}
	}
	testassert.Require(t, entry != nil && entry.InactiveReason == nil && slices.Contains(entry.Tools, "worker-tool"), "reload catalog = %+v, want the loaded copy active with its registered tool", entry)
	testassert.NoError(t, owner.Close(context.Background()))
}

// TestPackageUnusableSelectionRejectsEnabledBuild proves a selected package
// that fails to load is ignored while disabled and rejects an enabled build.
func TestPackageUnusableSelectionRejectsEnabledBuild(t *testing.T) {
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	declaration := packageTestManifest()
	source := t.TempDir()
	if err := writePackageSourceFiles(t, source, declaration, "1.0.0", "worker"); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install: %v", err)
	}
	selected := readSelectedRecord(t, dataDir, declaration.ID)
	entryPath := filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "packages", selected.Copy, "worker")
	if err := os.Remove(entryPath); err != nil {
		t.Fatalf("remove package entry point: %v", err)
	}

	writeServiceFile(t, configPath, `{"extensions":{"peer":{"enabled":true}}}`)
	owner := openPackageOwner(t, dataDir, configPath)
	caller := packageCaller(t, owner)
	catalog, err := owner.ListExtensions(context.Background(), caller)
	testassert.NoError(t, err)
	testassert.Require(t, len(catalog) == 1 && catalog[0].ExtensionId == declaration.ID && catalog[0].InactiveReason != nil && *catalog[0].InactiveReason != "", "enabled missing executable = %+v, want inactive reason", catalog)
	testassert.Equal(t, catalog[0].Version, selected.Version)
	testassert.Require(t, catalog[0].ConfigurationSchema == nil, "inactive entry fabricated a configuration schema: %v", catalog[0].ConfigurationSchema)
	_, err = owner.Health(context.Background(), caller)
	testassert.NoError(t, err)
	session, err := owner.CreateSession(context.Background(), caller, dataDir, nil)
	testassert.Require(t, err == nil && session.SessionId != "", "the inactive package broke the baseline owner: %v", err)
	testassert.NoError(t, owner.Close(context.Background()))

	writeServiceFile(t, configPath, `{"extensions":{"peer":{"enabled":false}}}`)
	owner = openPackageOwner(t, dataDir, configPath)
	caller = packageCaller(t, owner)
	row := packageRow(t, owner, caller, declaration.ID)
	testassert.Equal(t, row.LoadedVersion, "")
	testassert.Equal(t, row.SelectedVersion, "1.0.0")
	catalog, err = owner.ListExtensions(context.Background(), caller)
	testassert.NoError(t, err)
	testassert.Require(t, len(catalog) == 0, "disabled missing executable = %+v, want no inactive entry", catalog)
	testassert.NoError(t, owner.Close(context.Background()))
}

// TestPackageListReadFailureIsError proves only a missing selection is the
// ordinary not-selected state; another read failure is an error.
func TestPackageListReadFailureIsError(t *testing.T) {
	dataDir := t.TempDir()
	declaration := packageTestManifest()
	source := t.TempDir()
	if err := writePackageSourceFiles(t, source, declaration, "1.0.0", "worker"); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install: %v", err)
	}
	selectedPath := filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected")
	if err := os.WriteFile(selectedPath, []byte(`{"id":"peer"}`), 0o600); err != nil {
		t.Fatalf("corrupt selection: %v", err)
	}
	_, err := newPackageManager(dataDir).list()
	testassert.Require(t, err != nil, "list with a corrupt selection succeeded")
}

// TestPackageRemovePathShapedIDHasNoDeletionAuthority proves the extension ID
// is hashed for every path, so a path-shaped ID cannot delete outside the
// extension directory or another package's selection.
func TestPackageRemovePathShapedIDHasNoDeletionAuthority(t *testing.T) {
	dataDir := t.TempDir()
	declaration := packageTestManifest()
	source := t.TempDir()
	if err := writePackageSourceFiles(t, source, declaration, "1.0.0", "worker"); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if _, err := newPackageManager(dataDir).install(source); err != nil {
		t.Fatalf("install: %v", err)
	}
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, `{"extensions":{"peer":{"enabled":false}}}`)
	owner := openPackageOwner(t, dataDir, configPath)
	caller := packageCaller(t, owner)
	outside := filepath.Join(dataDir, "extensions", "sentinel")
	if err := os.WriteFile(outside, []byte("sentinel"), 0o600); err != nil {
		t.Fatalf("outside sentinel: %v", err)
	}
	testassert.NoError(t, owner.RemoveExtensionPackage(context.Background(), caller, "../../sentinel"))
	if data, err := os.ReadFile(outside); err != nil || string(data) != "sentinel" {
		t.Fatalf("outside sentinel = %q, %v", data, err)
	}
	testassert.Equal(t, readSelectedRecord(t, dataDir, declaration.ID).Version, "1.0.0")
	testassert.NoError(t, owner.RemoveExtensionPackage(context.Background(), caller, declaration.ID))
	mustNotExist(t, filepath.Join(dataDir, "extensions", extensionDataHash(declaration.ID), "selected"), "remove retained the selected record")
	row := packageRow(t, owner, caller, declaration.ID)
	testassert.Equal(t, row.LoadedVersion, "1.0.0")
	testassert.Equal(t, row.SelectedVersion, "")
	testassert.NoError(t, owner.Close(context.Background()))
}

// TestPackageCommandsRequireOwnerScope proves the three package commands are
// owner-scoped and reject empty caller inputs as invalid.
func TestPackageCommandsRequireOwnerScope(t *testing.T) {
	dataDir := t.TempDir()
	configPath := filepath.Join(dataDir, "config.json")
	writeServiceFile(t, configPath, `{}`)
	owner := openPackageOwner(t, dataDir, configPath)
	narrow, err := owner.Caller(CallerContext{Origin: harness.InputOriginUser, Scope: CallerScope{Kind: CallerScopeSession, SessionID: canonicalTestSessionID}})
	if err != nil {
		t.Fatalf("narrow caller: %v", err)
	}
	_, err = owner.ListExtensionPackages(context.Background(), narrow)
	testassert.Require(t, errors.Is(err, ErrDenied), "narrow list = %v, want denied", err)
	_, err = owner.InstallExtensionPackage(context.Background(), narrow, t.TempDir())
	testassert.Require(t, errors.Is(err, ErrDenied), "narrow install = %v, want denied", err)
	err = owner.RemoveExtensionPackage(context.Background(), narrow, "peer")
	testassert.Require(t, errors.Is(err, ErrDenied), "narrow remove = %v, want denied", err)
	caller := packageCaller(t, owner)
	_, err = owner.InstallExtensionPackage(context.Background(), caller, "")
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "empty source install = %v, want invalid", err)
	err = owner.RemoveExtensionPackage(context.Background(), caller, "")
	testassert.Require(t, errors.Is(err, harness.ErrInvalid), "empty ID remove = %v, want invalid", err)
	testassert.NoError(t, owner.Close(context.Background()))
}

// canonicalTestSessionID is one shape-valid Session identity for narrow
// caller-grant tests.
const canonicalTestSessionID = "0123456789abcdef0123456789abcdef"
