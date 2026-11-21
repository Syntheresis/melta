package harness

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestStrategySelectionRetainsSettingsBytes(t *testing.T) {
	settings := json.RawMessage{'"', 0xff, '"'}
	if !json.Valid(settings) {
		t.Fatal("fixture is not accepted JSON")
	}
	wire, err := json.Marshal(StrategySelection{ID: "s", Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	var decoded StrategySelection
	if err := json.Unmarshal(wire, &decoded); err != nil || decoded.ID != "s" || !bytes.Equal(decoded.Settings, settings) {
		t.Fatalf("strategy settings = %x, want %x (error %v)", decoded.Settings, settings, err)
	}
	for _, bad := range []string{`{"id":"s","extra":1}`, `{}`, `{"id":"s","settings_base64":null}`} {
		if err := json.Unmarshal([]byte(bad), &decoded); err == nil {
			t.Fatalf("%s accepted", bad)
		}
	}
}
