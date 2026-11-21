package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/atomicfs"
	"github.com/Syntheresis/melta/internal/safefs"
	"github.com/Syntheresis/melta/protocol"
)

// packageSelection is the atomic selected-package record: the extension ID,
// its implementation version, and the copy directory that holds its bytes.
type packageSelection struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Copy    string `json:"copy"`
}

// packageManager owns the local executable-package layout beneath the one
// data directory. One loaded code set is fixed for the owner lifetime;
// install, list and remove only read or replace next-start selections. Every
// method serializes on mu, so package access is serialized under the owner.
type packageManager struct {
	dataDir string

	mu             sync.Mutex
	loaded         map[string]string
	selected       map[string]string
	unusable       map[string]error
	authorFailures map[string]error
}

func (m *packageManager) loadAuthors(environment func(context.Context, []string) ([]string, error)) ([]nativeActivationSpec, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	root := filepath.Join(m.dataDir, "extensions")
	directory, err := safefs.OpenDirectory(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := directory.ReadDir(-1)
	_ = directory.Close()
	if err != nil {
		return nil, err
	}
	m.authorFailures = make(map[string]error)
	var specs []nativeActivationSpec
	for _, entry := range entries {
		hash := entry.Name()
		if !entry.IsDir() || !canonicalPackageHash(hash) {
			continue
		}
		selected, ok, err := readSelection(filepath.Join(root, hash))
		if err != nil {
			m.unusable[hash] = err
			continue
		}
		if !ok {
			continue
		}
		if extensionDataHash(selected.ID) != hash {
			m.unusable[hash] = fmt.Errorf("selected package identity %q does not match its directory", selected.ID)
			continue
		}
		m.selected[selected.ID] = selected.Version
		manifest, path, err := loadAuthorPackage(m.dataDir, hash, selected)
		if err != nil {
			m.authorFailures[selected.ID] = err
			continue
		}
		m.loaded[selected.ID] = selected.Version
		specs = append(specs, nativeActivationSpec{extension: executableAuthorExtension(manifest, path, environment), installed: true})
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].extension.Manifest.ID < specs[j].extension.Manifest.ID })
	return specs, nil
}

// newPackageManager constructs the manager before the first load. The loaded
// and unusable maps are populated by load from the selected records.
func newPackageManager(dataDir string) *packageManager {
	return &packageManager{
		dataDir:  dataDir,
		loaded:   make(map[string]string),
		selected: make(map[string]string),
		unusable: make(map[string]error),
	}
}

// selectedVersion returns one installed extension's selected implementation
// version as read from its selection record at startup; an uninstalled
// extension has none.
func (m *packageManager) selectedVersion(extensionID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.selected[extensionID]
}

// install reads one local source package's manifest and entry point, writes
// exactly those bytes into a new copy directory, syncs it, and atomically
// replaces the selected record. It never edits configuration or deletes an
// earlier package copy.
func (m *packageManager) install(sourceDir string) (protocol.ExtensionPackage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sourceDir == "" {
		return protocol.ExtensionPackage{}, fmt.Errorf("package source directory must be non-empty: %w", harness.ErrInvalid)
	}
	sourceRoot, err := os.OpenRoot(sourceDir)
	if err != nil {
		return protocol.ExtensionPackage{}, fmt.Errorf("open package source directory: %v: %w", err, harness.ErrInvalid)
	}
	defer func() { _ = sourceRoot.Close() }()
	manifestBytes, err := sourceRoot.ReadFile("manifest.json")
	if err != nil {
		return protocol.ExtensionPackage{}, fmt.Errorf("read package manifest: %v: %w", err, harness.ErrInvalid)
	}
	var manifest worker.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return protocol.ExtensionPackage{}, fmt.Errorf("decode package manifest: %v: %w", err, harness.ErrInvalid)
	}
	if err := extension.ValidateManifest(manifest.Manifest); err != nil {
		return protocol.ExtensionPackage{}, fmt.Errorf("package manifest: %v: %w", err, harness.ErrInvalid)
	}
	id, version, entryPoint := manifest.Manifest.ID, manifest.Manifest.Version, manifest.EntryPoint
	if !containedPackageEntryPoint(entryPoint) {
		return protocol.ExtensionPackage{}, fmt.Errorf("entry point %q is not a contained package-relative path: %w", entryPoint, harness.ErrInvalid)
	}
	entryFile, err := sourceRoot.Open(entryPoint)
	if err != nil {
		return protocol.ExtensionPackage{}, fmt.Errorf("open package entry point: %v: %w", err, harness.ErrInvalid)
	}
	if err := validatePackageEntryFile(entryFile, entryPoint); err != nil {
		_ = entryFile.Close()
		return protocol.ExtensionPackage{}, err
	}
	entryBytes, err := io.ReadAll(entryFile)
	_ = entryFile.Close()
	if err != nil {
		return protocol.ExtensionPackage{}, fmt.Errorf("read package entry point: %v: %w", err, harness.ErrInvalid)
	}

	extensionDir := filepath.Join(m.dataDir, "extensions", extensionDataHash(id))
	packagesDir := filepath.Join(extensionDir, "packages")
	if err := safefs.EnsureDirectory(packagesDir, 0o700); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	copyID, err := newInstanceID()
	if err != nil {
		return protocol.ExtensionPackage{}, err
	}
	packageDir := filepath.Join(packagesDir, copyID)
	if err := safefs.EnsureDirectory(packageDir, 0o700); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	installed := false
	defer func() {
		if !installed {
			_ = os.RemoveAll(packageDir)
		}
	}()
	if err := writePackageFile(filepath.Join(packageDir, "manifest.json"), manifestBytes, 0o600); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	if err := writePackageFile(filepath.Join(packageDir, entryPoint), entryBytes, 0o700); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	if err := syncPackageDirectories(packageDir, entryPoint); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	if err := syncParentChain(m.dataDir, packagesDir); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	selected, err := json.Marshal(packageSelection{ID: id, Version: version, Copy: copyID})
	if err != nil {
		return protocol.ExtensionPackage{}, err
	}
	if err := atomicfs.Write(filepath.Join(extensionDir, "selected"), selected, 0o600); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	installed = true
	return protocol.ExtensionPackage{
		Id:              id,
		LoadedVersion:   m.loaded[id],
		SelectedVersion: version,
	}, nil
}

// remove deletes only the selected record. Package copies and loaded code are
// never deleted, and an absent selection is already removed.
func (m *packageManager) remove(extensionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if extensionID == "" {
		return fmt.Errorf("extension ID must be non-empty: %w", harness.ErrInvalid)
	}
	extensionDir := filepath.Join(m.dataDir, "extensions", extensionDataHash(extensionID))
	err := safefs.RemoveLeaf(filepath.Join(extensionDir, "selected"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return atomicfs.SyncDir(extensionDir)
}

// list reports every loaded extension and every selected next-start package.
// A missing selected record is the normal not-selected state; any other read
// failure is an error.
func (m *packageManager) list() ([]protocol.ExtensionPackage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	selected := make(map[string]string)
	root := filepath.Join(m.dataDir, "extensions")
	directory, err := safefs.OpenDirectory(root)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if directory != nil {
		entries, err := directory.ReadDir(-1)
		_ = directory.Close()
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			name := entry.Name()
			if !entry.IsDir() || !canonicalPackageHash(name) {
				continue
			}
			selection, ok, err := readSelection(filepath.Join(root, name))
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			if extensionDataHash(selection.ID) != name {
				return nil, fmt.Errorf("selected package identity %q does not match its directory", selection.ID)
			}
			selected[selection.ID] = selection.Version
		}
	}
	ids := make([]string, 0, len(m.loaded)+len(selected))
	for id := range m.loaded {
		ids = append(ids, id)
	}
	for id := range selected {
		if _, ok := m.loaded[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	rows := make([]protocol.ExtensionPackage, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, protocol.ExtensionPackage{
			Id:              id,
			LoadedVersion:   m.loaded[id],
			SelectedVersion: selected[id],
		})
	}
	return rows, nil
}

// ListExtensionPackages serves the loaded and selected versions. It is
// Caller-scoped to the whole owner and serialized under it.
func (r *Melta) ListExtensionPackages(ctx context.Context, c Caller) ([]protocol.ExtensionPackage, error) {
	release, err := r.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := r.authorize(c, ctx, nil, harnessScope); err != nil {
		return nil, err
	}
	return r.packages.list()
}

// InstallExtensionPackage installs one local source package for the next
// start. It is Caller-scoped to the whole owner and never changes the live
// loaded set.
func (r *Melta) InstallExtensionPackage(ctx context.Context, c Caller, sourceDir string) (protocol.ExtensionPackage, error) {
	release, err := r.enter(ctx)
	if err != nil {
		return protocol.ExtensionPackage{}, err
	}
	defer release()
	if err := r.authorize(c, ctx, nil, harnessScope); err != nil {
		return protocol.ExtensionPackage{}, err
	}
	return r.packages.install(sourceDir)
}

// RemoveExtensionPackage removes the next-start selection. It is Caller-scoped
// to the whole owner and deletes no package copy.
func (r *Melta) RemoveExtensionPackage(ctx context.Context, c Caller, extensionID string) error {
	release, err := r.enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := r.authorize(c, ctx, nil, harnessScope); err != nil {
		return err
	}
	return r.packages.remove(extensionID)
}

func loadAuthorPackage(dataDir, hash string, selected packageSelection) (extension.Manifest, string, error) {
	packageDir := filepath.Join(dataDir, "extensions", hash, "packages", selected.Copy)
	file, err := safefs.OpenExisting(filepath.Join(packageDir, "manifest.json"), os.O_RDONLY)
	if err != nil {
		return extension.Manifest{}, "", err
	}
	raw, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil {
		return extension.Manifest{}, "", err
	}
	var manifest worker.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return extension.Manifest{}, "", err
	}
	if err := extension.ValidateManifest(manifest.Manifest); err != nil {
		return extension.Manifest{}, "", err
	}
	if manifest.Manifest.ID != selected.ID || manifest.Manifest.Version != selected.Version {
		return extension.Manifest{}, "", errors.New("package manifest does not match selected identity")
	}
	entry, err := openPackageEntry(packageDir, manifest.EntryPoint)
	if err != nil {
		return extension.Manifest{}, "", err
	}
	_ = entry.Close()
	return manifest.Manifest, filepath.Join(packageDir, manifest.EntryPoint), nil
}

// openPackageEntry opens one contained package-relative entry point without
// following a symlink in any component, and applies the shared regular
// executable check.
func openPackageEntry(packageDir, entryPoint string) (*os.File, error) {
	if !containedPackageEntryPoint(entryPoint) {
		return nil, fmt.Errorf("entry point %q is not a contained package-relative path: %w", entryPoint, harness.ErrInvalid)
	}
	file, err := safefs.OpenExisting(filepath.Join(packageDir, entryPoint), os.O_RDONLY)
	if err != nil {
		return nil, fmt.Errorf("open package entry point: %v: %w", err, harness.ErrInvalid)
	}
	if err := validatePackageEntryFile(file, entryPoint); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

// validatePackageEntryFile applies the one entry-point shape rule to an
// already-opened file from either the install source root or the owned
// package directory: it must be a regular executable file.
func validatePackageEntryFile(file *os.File, entryPoint string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("entry point %q is not a regular file: %w", entryPoint, harness.ErrInvalid)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("entry point %q is not executable: %w", entryPoint, harness.ErrInvalid)
	}
	return nil
}

// containedPackageEntryPoint reports whether one manifest entry point is a
// nonempty local path that cannot escape the package directory.
func containedPackageEntryPoint(value string) bool {
	return value != "" && value != "manifest.json" && filepath.IsLocal(value)
}

// readSelection reads one extension's selected record. A missing record is
// the normal not-selected state.
func readSelection(extensionDir string) (packageSelection, bool, error) {
	file, err := safefs.OpenExisting(filepath.Join(extensionDir, "selected"), os.O_RDONLY)
	if err != nil {
		if os.IsNotExist(err) {
			return packageSelection{}, false, nil
		}
		return packageSelection{}, false, fmt.Errorf("open selected package: %w", err)
	}
	defer file.Close()
	var selected packageSelection
	if err := json.NewDecoder(file).Decode(&selected); err != nil {
		return packageSelection{}, false, fmt.Errorf("decode selected package: %w", err)
	}
	if selected.ID == "" || selected.Version == "" || !canonicalCopyID(selected.Copy) {
		return packageSelection{}, false, errors.New("selected package record is incomplete")
	}
	return selected, true, nil
}

// writePackageFile writes and syncs one package file through the checked
// no-follow traversal.
func writePackageFile(path string, data []byte, perm os.FileMode) error {
	file, _, err := safefs.OpenForWrite(path, perm)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// syncPackageDirectories syncs one copy's directory and every directory
// created for the entry point's parents, deepest first, so a nested entry
// point's directory chain is durable before the selection write.
func syncPackageDirectories(packageDir, entryPoint string) error {
	for dir := filepath.Dir(filepath.Join(packageDir, entryPoint)); ; dir = filepath.Dir(dir) {
		if err := atomicfs.SyncDir(dir); err != nil {
			return err
		}
		if dir == packageDir {
			return nil
		}
	}
}

// syncParentChain makes one published package directory entry durable by
// syncing every directory from dir up to and including dataDir.
func syncParentChain(dataDir, dir string) error {
	for {
		if err := atomicfs.SyncDir(dir); err != nil {
			return err
		}
		if dir == dataDir || dir == filepath.Dir(dir) {
			return nil
		}
		dir = filepath.Dir(dir)
	}
}

// canonicalPackageHash reports whether one extension directory name is the
// lowercase hex SHA-256 identity.
func canonicalPackageHash(value string) bool {
	return len(value) == 64 && strings.Trim(value, "0123456789abcdef") == ""
}

// canonicalCopyID reports whether one selected copy ID is the lowercase hex
// identity minted for a package copy.
func canonicalCopyID(value string) bool {
	return len(value) == 32 && strings.Trim(value, "0123456789abcdef") == ""
}
