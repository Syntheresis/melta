package tool

import (
	"fmt"
	"io"
	"os"

	"github.com/Syntheresis/melta/internal/safefs"
	"github.com/Syntheresis/melta/internal/snapshot"
)

// SnapshotStore is the complete internal snapshot surface the prepared write,
// edit and patch mutations require. It is mandatory: mutation never runs
// against a missing or partial store.
type SnapshotStore interface {
	SnapshotResolvedEntry(originalPath, canonicalPath string) (entryID string, created bool, err error)
	DiscardSnapshotEntry(entryID string) error
	RecordSnapshotContent(entryID string, content []byte) error
	RecordSnapshotAbsence(entryID string) error
	ReplaceSnapshotIdentity(entryID string, identity *snapshot.SnapshotContentIdentity) (*snapshot.SnapshotContentIdentity, error)
}

type mutationFile interface {
	io.Reader
	io.Seeker
	io.Writer
	Close() error
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
}

var (
	openWriteTargetForMutationFunc = openWriteTargetForMutationReal
	openExistingMutationFile       = func(path string, flag int) (mutationFile, error) {
		return safefs.OpenExisting(path, flag)
	}
)

// snapshotEntry is one captured mutation snapshot: the entry id, whether this
// call created the preimage, and the LastWrite proof the entry owned before
// the pre-mutation clear.
type snapshotEntry struct {
	store   SnapshotStore
	entryID string
	created bool
	prior   *snapshot.SnapshotContentIdentity
}

type mutationOutcome uint8

const (
	mutationUntouched mutationOutcome = iota
	mutationCompleted
	mutationUncertain
)

// mutateBound revalidates around capture and keeps the mutation outcome separate from errors.
func mutateBound(bound boundCall, store SnapshotStore, preflight func() error, mutate func() (mutationOutcome, []byte, bool, error)) (mutationOutcome, error) {
	if err := bound.revalidate(); err != nil {
		return mutationUntouched, fmt.Errorf("%s: resolve path: %w", bound.toolName, err)
	}
	if preflight != nil {
		if err := preflight(); err != nil {
			return mutationUntouched, err
		}
	}
	entry, err := captureSnapshotEntry(store, bound.displayAbs, bound.canonical)
	if err != nil {
		return mutationUntouched, fmt.Errorf("%s: snapshot: %w", bound.toolName, err)
	}
	if err := bound.revalidate(); err != nil {
		err = fmt.Errorf("%s: resolve path: %w", bound.toolName, err)
		return mutationUntouched, abandonMutation(entry, err)
	}
	outcome, produced, absent, err := mutate()
	if outcome == mutationUntouched {
		return outcome, abandonMutation(entry, err)
	}
	if outcome == mutationCompleted {
		var recordErr error
		if absent {
			recordErr = entry.store.RecordSnapshotAbsence(entry.entryID)
		} else {
			recordErr = entry.store.RecordSnapshotContent(entry.entryID, produced)
		}
		if recordErr != nil {
			if err != nil {
				return outcome, fmt.Errorf("%w; additionally failed to record snapshot identity: %v", err, recordErr)
			}
			return outcome, fmt.Errorf("%s: record snapshot identity: %w", bound.toolName, recordErr)
		}
	}
	return outcome, err
}

func writeBound(bound boundCall, store SnapshotStore, content []byte, createOnly bool) (mutationOutcome, error) {
	return mutateBound(bound, store, func() error {
		return preflightWriteSnapshotTarget(bound.canonical, bound.toolName)
	}, func() (mutationOutcome, []byte, bool, error) {
		f, existed, err := openWriteTargetForMutation(bound.canonical)
		if err != nil {
			return mutationUntouched, nil, false, fmt.Errorf("%s: open: %w", bound.toolName, err)
		}
		defer f.Close()
		if createOnly && existed {
			return mutationUntouched, nil, false, fmt.Errorf("path %s appeared during apply (Add requires the path to not exist)", bound.canonical)
		}
		complete, err := writeReplacement(f, content)
		if err != nil {
			err = fmt.Errorf("%s: %w", bound.toolName, err)
		}
		if complete {
			return mutationCompleted, content, false, err
		}
		return mutationUncertain, nil, false, err
	})
}

// captureSnapshotEntry captures the preimage and persists the cleared
// LastWrite proof under the existing execution/artifact ownership. A failed
// clear abandons the capture (a newly created preimage is discarded) and the
// caller must not mutate.
func captureSnapshotEntry(store SnapshotStore, originalPath, canonicalPath string) (snapshotEntry, error) {
	entryID, created, err := store.SnapshotResolvedEntry(originalPath, canonicalPath)
	if err != nil {
		return snapshotEntry{}, err
	}
	prior, err := store.ReplaceSnapshotIdentity(entryID, nil)
	if err != nil {
		if created {
			if discardErr := store.DiscardSnapshotEntry(entryID); discardErr != nil {
				return snapshotEntry{}, fmt.Errorf("%w; additionally failed to discard snapshot: %v", err, discardErr)
			}
		}
		return snapshotEntry{}, err
	}
	return snapshotEntry{store: store, entryID: entryID, created: created, prior: prior}, nil
}

// abandonUnmutatedSnapshot resolves a proven non-mutation failure: this call's
// newly created preimage is discarded, and an existing entry gets its owned
// prior proof restored. A failed restoration leaves the cleared nil proof,
// reports its error, and preserves the preimage for a later restore skip.
func abandonUnmutatedSnapshot(entry snapshotEntry) error {
	if entry.created {
		return entry.store.DiscardSnapshotEntry(entry.entryID)
	}
	if entry.prior == nil {
		return nil
	}
	if _, err := entry.store.ReplaceSnapshotIdentity(entry.entryID, entry.prior); err != nil {
		return fmt.Errorf("restore snapshot proof: %w", err)
	}
	return nil
}

// writeReplacement performs the truncate/seek/write/sync content replacement
// and reports, independently of the returned error, whether the intended
// bytes are known to have been fully produced: only a byte count that covers
// the whole content claims a complete identity. A truncate, seek, or short
// write leaves the produced state uncertain.
func writeReplacement(f mutationFile, content []byte) (complete bool, err error) {
	if err := f.Truncate(0); err != nil {
		return false, fmt.Errorf("truncate: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("seek: %w", err)
	}
	n, writeErr := f.Write(content)
	if n != len(content) {
		if writeErr != nil {
			return false, fmt.Errorf("write: incomplete %d of %d bytes: %w", n, len(content), writeErr)
		}
		return false, fmt.Errorf("write: incomplete %d of %d bytes", n, len(content))
	}
	if writeErr != nil {
		return true, fmt.Errorf("write: %w", writeErr)
	}
	if err := f.Sync(); err != nil {
		return true, fmt.Errorf("sync: %w", err)
	}
	return true, nil
}

func abandonMutation(entry snapshotEntry, err error) error {
	if abandonErr := abandonUnmutatedSnapshot(entry); abandonErr != nil {
		return fmt.Errorf("%w; additionally failed to abandon snapshot: %v", err, abandonErr)
	}
	return err
}

// openWriteTargetForMutation opens the canonical write target through the
// shared safefs primitives.

func openWriteTargetForMutation(absPath string) (mutationFile, bool, error) {
	return openWriteTargetForMutationFunc(absPath)
}

func openWriteTargetForMutationReal(absPath string) (mutationFile, bool, error) {
	_, err := ensureRegularExistingTarget(absPath)
	if err != nil {
		return nil, false, err
	}

	f, openedExisting, err := safefs.OpenForWrite(absPath, 0o644)
	if err != nil {
		return nil, false, err
	}
	if openedExisting {
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, false, fmt.Errorf("stat: %w", err)
		}
		if err := ensureRegularFileInfo(absPath, info); err != nil {
			_ = f.Close()
			return nil, false, err
		}
	}
	return f, openedExisting, nil
}

func preflightWriteSnapshotTarget(absPath, toolName string) error {
	existed, err := ensureRegularExistingTarget(absPath)
	if err != nil {
		return fmt.Errorf("%s: %w", toolName, err)
	}
	if !existed {
		return nil
	}
	f, err := safefs.OpenExisting(absPath, os.O_RDWR)
	if err != nil {
		return fmt.Errorf("%s: %w", toolName, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("%s: stat: %w", toolName, err)
	}
	if err := ensureRegularFileInfo(absPath, info); err != nil {
		return fmt.Errorf("%s: %w", toolName, err)
	}
	return nil
}
