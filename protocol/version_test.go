package protocol

import "testing"

func TestHealthAcceptsOnlyCurrentProtocolVersion(t *testing.T) {
	if !HealthProtocolVersion("19").Valid() {
		t.Fatal("current protocol version rejected")
	}
	if HealthProtocolVersion("18").Valid() {
		t.Fatal("previous protocol version accepted")
	}
}
