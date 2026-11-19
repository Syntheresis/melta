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
		name, patch string
		flipAt      int
	}{
		{"write", "", 1},
		{"edit", "", 1},
		{"add", "*** Add File: new.txt\n+after", 1},
		{"update", "*** Update File: old.txt\n@@\n-before\n+after", 1},
		{"delete", "*** Delete File: old.txt", 1},
		{"move destination", "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after", 1},
		{"move source", "*** Update File: old.txt\n*** Move to: new.txt\n@@\n-before\n+after", 2},
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
				call, err = PrepareWriteCall(root, root, "", CapabilityOptions{}, store, map[string]any{"path": "old.txt", "content": "after"})
				if err == nil {
					_, err = call.Execute(context.Background(), check)
				}
			case "edit":
				var call *PreparedCall
				call, err = PrepareEditCall(root, root, "", CapabilityOptions{}, store, map[string]any{"path": "old.txt", "old_string": "before", "new_string": "after"})
				if err == nil {
					_, err = call.Execute(context.Background(), check)
				}
			default:
				var call *PreparedPatchCall
				call, err = PreparePatchCall(root, root, "", CapabilityOptions{}, store, map[string]any{"input": applyPatchInput(t, tc.patch)})
				if err == nil {
					_, err = call.Execute(context.Background(), check)
				}
			}
			if err == nil || !strings.Contains(err.Error(), "witness changed") || !changed {
				t.Fatalf("err=%v changed=%v", err, changed)
			}
			assertFileContent(t, old, "before")
			dst := filepath.Join(root, "new.txt")
			if tc.name == "move source" {
				assertFileContent(t, dst, "after")
				if len(store.identityRecords) != 1 {
					t.Fatalf("identity records: %+v", store.identityRecords)
				}
			} else if _, err := os.Stat(dst); !os.IsNotExist(err) {
				t.Fatalf("destination exists: %v", err)
			}
			if len(store.discards) != 1 {
				t.Fatalf("discards: %v", store.discards)
			}
		})
	}
}
