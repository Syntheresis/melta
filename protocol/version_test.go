package protocol

import "testing"

func TestHealthAcceptsOnlyCurrentProtocolVersion(t *testing.T) {
	if !HealthProtocolVersion("21").Valid() {
		t.Fatal("current protocol version rejected")
	}
	if HealthProtocolVersion("20").Valid() {
		t.Fatal("previous protocol version accepted")
	}
}
