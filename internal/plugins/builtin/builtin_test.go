package builtin

import (
	"github.com/Syntheresis/melta/internal/testassert"
	"testing"
)

// TestShippedRegistrationHoldsTwoInOrder proves the shipped registration set
// is exactly the two declared plugins in registration order: the bundled
// adaptation and the child-session task tool. The session store, the standard
// tools, and the background jobs service are constructed directly by the
// Runtime owner and are not plugin declarations. The full composition open over
// this set is exercised by the runtime integration suite.
func TestShippedRegistrationHoldsTwoInOrder(t *testing.T) {
	plugins := Plugins()
	testassert.Equal(t, len(plugins), 2)
	want := []string{"adaptation", "agents"}
	for i, p := range plugins {
		testassert.Check(t, p.Manifest.ID == want[i], "plugin %d ID = %q, want %q", i, p.Manifest.ID, want[i])
		testassert.Check(t, p.Activate != nil, "plugin %q has no activation entry", p.Manifest.ID)
	}
}
