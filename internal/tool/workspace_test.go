package tool

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Syntheresis/melta/internal/snapshot"
)

func TestWorkspaceBoundProducer(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file")
	if err := os.WriteFile(path, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := snapshot.OpenCodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	canonical, exists, err := WorkspaceStat(root, "file")
	if err != nil || !exists || canonical != path {
		t.Fatalf("bind: %q %v %v", canonical, exists, err)
	}
	content, err := WorkspaceRead(root, "file")
	if err != nil || string(content) != "before" {
		t.Fatalf("read: %q %v", content, err)
	}
	if outcome, err := WorkspaceWrite(root, "file", store, []byte("after"), true); err == nil || outcome != MutationUntouched {
		t.Fatalf("create-only: %v %v", outcome, err)
	}
	if outcome, err := WorkspaceWrite(root, "file", store, []byte("after"), false); err != nil || outcome != MutationCompleted {
		t.Fatalf("write: %v %v", outcome, err)
	}
	if outcome, err := WorkspaceDelete(root, "file", store); err != nil || outcome != MutationCompleted {
		t.Fatalf("delete: %v %v", outcome, err)
	}
}

func TestWorkspaceBoundMutationOutcomes(t *testing.T) {
	t.Run("completed record failure", func(t *testing.T) {
		root := t.TempDir()
		store := newTempCodeGroupStore(t)
		withSnapshotMetaPublishFailure(t, 3)
		outcome, err := WorkspaceWrite(root, "file", store, []byte("after"), false)
		if err == nil || outcome != MutationCompleted {
			t.Fatalf("record failure outcome: %v %v", outcome, err)
		}
	})
	t.Run("short write", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "file")
		if err := os.WriteFile(path, []byte("before"), 0600); err != nil {
			t.Fatal(err)
		}
		store := newTempCodeGroupStore(t)
		withMutationFailure(t, path, failShortWriteError)
		outcome, err := WorkspaceWrite(root, "file", store, []byte("after"), false)
		if err == nil || outcome != MutationUncertain {
			t.Fatalf("short write outcome: %v %v", outcome, err)
		}
	})
}

func TestWorkspacePathsAndRevert(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		t.Run(fmt.Sprintf("absolute=%v", absolute), func(t *testing.T) {
			root := t.TempDir()
			existing := snapshotLifecycleFile(t, root, "existing", "before")
			created := filepath.Join(root, "created")
			path := func(value string) string {
				if absolute {
					return filepath.Join(root, value)
				}
				return value
			}
			canonical, exists, err := WorkspaceStat(root, path("existing"))
			if err != nil || !exists || canonical != existing {
				t.Fatalf("stat = %q %v %v", canonical, exists, err)
			}
			content, err := WorkspaceRead(root, path("existing"))
			if err != nil || string(content) != "before" {
				t.Fatalf("read = %q %v", content, err)
			}
			store := newSnapshotLifecycleStore(t)
			if outcome, err := WorkspaceWrite(root, path("created"), store, []byte("new"), true); err != nil || outcome != MutationCompleted {
				t.Fatalf("create = %v %v", outcome, err)
			}
			if outcome, err := WorkspaceWrite(root, path("existing"), store, []byte("changed"), false); err != nil || outcome != MutationCompleted {
				t.Fatalf("write = %v %v", outcome, err)
			}
			if outcome, err := WorkspaceDelete(root, path("existing"), store); err != nil || outcome != MutationCompleted {
				t.Fatalf("delete = %v %v", outcome, err)
			}
			if _, err := revertLifecycleGroup(t, store); err != nil {
				t.Fatal(err)
			}
			assertFileContent(t, existing, "before")
			if _, err := os.Stat(created); !os.IsNotExist(err) {
				t.Fatalf("created path after revert: %v", err)
			}
		})
	}
}
