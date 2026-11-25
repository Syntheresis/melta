package worker_test

import (
	"encoding/json"
	"testing"

	"github.com/Syntheresis/melta/extension/worker"
)

func TestDecodeToolOutcome(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"result":{"call_id":"c","status":"success","content":"ok"},"evidence":"observed"}`, true},
		{`{"result":{"call_id":"c","status":"error","content":"no"},"evidence":""}`, true},
		{`{"result":{"call_id":"c","status":"interrupted","content":"no"},"evidence":"unknown"}`, true},
		{`{"result":{"call_id":"c","status":"denied","content":"no"},"evidence":"observed"}`, false},
		{`{"result":{"call_id":"c","status":"error","content":"no"},"evidence":"not_started"}`, false},
	} {
		_, err := worker.DecodeExecuteResult(json.RawMessage(tc.raw))
		if (err == nil) != tc.valid {
			t.Fatalf("decode %s: %v", tc.raw, err)
		}
	}
}
