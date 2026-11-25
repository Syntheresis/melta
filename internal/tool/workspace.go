package tool

import (
	"fmt"
	"io"
	"os"

	"github.com/Syntheresis/melta/internal/safefs"
)

// MutationOutcome reports what the producer knows independently of its error.
type MutationOutcome = mutationOutcome

const (
	MutationUntouched = mutationUntouched
	MutationCompleted = mutationCompleted
	MutationUncertain = mutationUncertain
)

// WorkspaceStat resolves a target without accessing its contents.
func WorkspaceStat(root, path string) (string, bool, error) {
	bound, err := bindCanonicalTarget(root, "workspace", path)
	if err != nil {
		return "", false, err
	}
	return bound.canonical, bound.leafExists, nil
}

// WorkspaceRead resolves and reads a target.
func WorkspaceRead(root, path string) ([]byte, error) {
	bound, err := bindCanonicalTarget(root, "workspace", path)
	if err != nil {
		return nil, err
	}
	return readFileBytes(bound.canonical)
}

// WorkspaceWrite resolves a target and captures a snapshot before writing.
func WorkspaceWrite(root, path string, store SnapshotStore, content []byte, createOnly bool) (MutationOutcome, error) {
	bound, err := bindCanonicalTarget(root, "workspace", path)
	if err != nil {
		return MutationUntouched, err
	}
	return writeBound(bound, store, content, createOnly)
}

// WorkspaceDelete resolves a target and captures a snapshot before deletion.
func WorkspaceDelete(root, path string, store SnapshotStore) (MutationOutcome, error) {
	bound, err := bindCanonicalTarget(root, "workspace", path)
	if err != nil {
		return MutationUntouched, err
	}
	return deleteBound(bound, store)
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
