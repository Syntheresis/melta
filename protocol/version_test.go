package protocol

import "testing"

func TestHealthAcceptsOnlyCurrentProtocolVersion(t *testing.T) {
	if !HealthProtocolVersion("24").Valid() {
		t.Fatal("current protocol version rejected")
	}
	if HealthProtocolVersion("23").Valid() {
		t.Fatal("previous protocol version accepted")
	}
}
