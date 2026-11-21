package agents

import "testing"

func TestReadOnlyPreparedCommand(t *testing.T) {
	for _, raw := range []string{"git diff -- file", "cat file | grep foo"} {
		script, err := ReadOnlyCommand(raw)
		if err != nil || !ReadOnlyPreparedCommand(script) {
			t.Fatalf("safe rewritten %q: %q %v", raw, script, err)
		}
	}
	for _, script := range []string{"git -c core.fsmonitor=false status", "git status", "rm file", "'git' '--no-pager' '--no-optional-locks' '-c' 'core.fsmonitor=true' '-c' 'diff.external=' '-c' 'core.pager=cat' 'status'"} {
		if ReadOnlyPreparedCommand(script) {
			t.Fatalf("accepted %q", script)
		}
	}
}
