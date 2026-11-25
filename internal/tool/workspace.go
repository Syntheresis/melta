package tool

import (
	"fmt"
	"io"
	"os"

	"github.com/Syntheresis/melta/internal/safefs"
)

// WorkspaceBinding retains the lexical and canonical witnesses for one target.
type WorkspaceBinding struct{ bound boundCall }

// MutationOutcome reports what the producer knows independently of its error.
type MutationOutcome = mutationOutcome

const (
	MutationUntouched = mutationUntouched
	MutationCompleted = mutationCompleted
	MutationUncertain = mutationUncertain
)

// BindWorkspace resolves a target without accessing its contents.
func BindWorkspace(root, path string) (WorkspaceBinding, string, bool, error) {
	bound, err := bindCanonicalTarget(root, "workspace", path)
	if err != nil {
		return WorkspaceBinding{}, "", false, err
	}
	return WorkspaceBinding{bound}, bound.canonical, bound.leafExists, nil
}

// Read revalidates the witnesses before opening the bound target.
func (b WorkspaceBinding) Read(recheck func() error) ([]byte, error) {
	b.bound.recheck = recheck
	if err := b.bound.revalidate(); err != nil {
		return nil, err
	}
	return readFileBytes(b.bound.canonical)
}

// Write uses the shared snapshot-aware file mutation producer.
func (b WorkspaceBinding) Write(store SnapshotStore, content []byte, createOnly bool, recheck func() error) (MutationOutcome, error) {
	b.bound.recheck = recheck
	return writeBound(b.bound, store, content, createOnly)
}

// Delete uses the shared snapshot-aware file mutation producer.
func (b WorkspaceBinding) Delete(store SnapshotStore, recheck func() error) (MutationOutcome, error) {
	b.bound.recheck = recheck
	return deleteBound(b.bound, store)
}

func deleteBound(bound boundCall, store SnapshotStore) (mutationOutcome, error) {
	return mutateBound(bound, store, nil, func() (mutationOutcome, []byte, bool, error) {
		if err := safefs.RemoveLeaf(bound.canonical); err != nil {
			return mutationUntouched, nil, false, fmt.Errorf("remove: %w", err)
		}
		return mutationCompleted, nil, true, nil
	})
}

// readFileBytes reads the entire file at canonicalPath with O_NOFOLLOW and
// returns the raw bytes (including \r) unchanged.
func readFileBytes(canonicalPath string) ([]byte, error) {
	f, err := safefs.OpenExisting(canonicalPath, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
