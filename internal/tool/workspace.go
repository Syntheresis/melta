package tool

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
	rootCanonical, err := bindApproved(root, "workspace", "")
	if err != nil {
		return WorkspaceBinding{}, "", false, err
	}
	bound, err := bindCanonicalTarget(root, "workspace", path, rootCanonical)
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
