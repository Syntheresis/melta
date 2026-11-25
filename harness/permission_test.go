package harness

import (
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
)

// TestPermissionNamesKeepPublishedLiterals pins the exported permission
// vocabulary to its exact published strings: these names cross the plugin
// boundary as literals, so the constants may never drift from them.
func TestPermissionNamesKeepPublishedLiterals(t *testing.T) {
	want := map[string]string{
		PermissionFileRead:   "file.read",
		PermissionFileWrite:  "file.write",
		PermissionCommandRun: "command.run",
		PermissionSessionOwn: "session.own",
		PermissionProcessOwn: "process.own",
	}
	// distinct literals: a duplicated value would collapse the map.
	testassert.Equal(t, len(want), 5)
	for name, literal := range want {
		testassert.Check(t, name == literal, "permission constant %q = %q, want the published literal", name, literal)
	}
	testassert.Check(t, PermissionDeniedContent == "Permission denied.", "PermissionDeniedContent = %q, want the published denial content", PermissionDeniedContent)
}
