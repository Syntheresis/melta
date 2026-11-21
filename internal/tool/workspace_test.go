package tool

import (
	"errors"
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
	bound, canonical, exists, err := BindWorkspace(root, "file")
	if err != nil || !exists || canonical != path {
		t.Fatalf("bind: %q %v %v", canonical, exists, err)
	}
	content, err := bound.Read(nil)
	if err != nil || string(content) != "before" {
		t.Fatalf("read: %q %v", content, err)
	}
	if outcome, err := bound.Write(store, []byte("after"), true, nil); err == nil || outcome != MutationUntouched {
		t.Fatalf("create-only: %v %v", outcome, err)
	}
	if outcome, err := bound.Write(store, []byte("after"), false, nil); err != nil || outcome != MutationCompleted {
		t.Fatalf("write: %v %v", outcome, err)
	}
	if outcome, err := bound.Delete(store, nil); err != nil || outcome != MutationCompleted {
		t.Fatalf("delete: %v %v", outcome, err)
	}
}

func TestWorkspaceBoundRevalidatesWitness(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink("first", link); err != nil {
		t.Fatal(err)
	}
	bound, _, _, err := BindWorkspace(root, "link")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("second", link); err != nil {
		t.Fatal(err)
	}
	if _, err := bound.Read(nil); err == nil {
		t.Fatal("read accepted repointed witness")
	}
	store, err := snapshot.OpenCodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := bound.Write(store, []byte("changed"), false, nil); err == nil || outcome != MutationUntouched {
		t.Fatalf("write after repoint: %v %v", outcome, err)
	}
	check := errors.New("authorization witness changed")
	other, _, _, err := BindWorkspace(root, "second")
	if err != nil {
		t.Fatal(err)
	}
	if outcome, err := other.Delete(store, func() error { return check }); !errors.Is(err, check) || outcome != MutationUntouched {
		t.Fatalf("delete after authorization witness changed: %v %v", outcome, err)
	}
}

func TestWorkspaceBoundMutationOutcomes(t *testing.T) {
	t.Run("completed record failure", func(t *testing.T) {
		root := t.TempDir()
		bound, _, _, err := BindWorkspace(root, "file")
		if err != nil {
			t.Fatal(err)
		}
		store := newTempCodeGroupStore(t)
		withSnapshotMetaPublishFailure(t, 3)
		outcome, err := bound.Write(store, []byte("after"), false, nil)
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
		bound, _, _, err := BindWorkspace(root, "file")
		if err != nil {
			t.Fatal(err)
		}
		store := newTempCodeGroupStore(t)
		withMutationFailure(t, path, failShortWriteError)
		outcome, err := bound.Write(store, []byte("after"), false, nil)
		if err == nil || outcome != MutationUncertain {
			t.Fatalf("short write outcome: %v %v", outcome, err)
		}
	})
}
