package tool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdditionalRecheckAtEveryMutationCapture(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flipAt int
	}{
		{"write", 1},
		{"edit", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			old := filepath.Join(root, "old.txt")
			if err := os.WriteFile(old, []byte("before"), 0o600); err != nil {
				t.Fatal(err)
			}
			store := newRecordingCodeGroupStore(t)
			changed := false
			store.onSnapshot = func(n int) {
				if n == tc.flipAt {
					changed = true
				}
			}
			check := func() error {
				if changed {
					return errors.New("witness changed")
				}
				return nil
			}
			var err error
			switch tc.name {
			case "write":
				var call *PreparedCall
				call, err = PrepareWriteCall(root, root, store, map[string]any{"path": "old.txt", "content": "after"})
				if err == nil {
					_, err = call.Execute(context.Background(), check)
				}
			case "edit":
				var call *PreparedCall
				call, err = PrepareEditCall(root, root, store, map[string]any{"path": "old.txt", "old_string": "before", "new_string": "after"})
				if err == nil {
					_, err = call.Execute(context.Background(), check)
				}
			}
			if err == nil || !strings.Contains(err.Error(), "witness changed") || !changed {
				t.Fatalf("err=%v changed=%v", err, changed)
			}
			assertFileContent(t, old, "before")
			if _, err := os.Stat(filepath.Join(root, "new.txt")); !os.IsNotExist(err) {
				t.Fatalf("destination exists: %v", err)
			}
			if len(store.discards) != 1 {
				t.Fatalf("discards: %v", store.discards)
			}
		})
	}
}
