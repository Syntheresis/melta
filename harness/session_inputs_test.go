package harness

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/model"
)

func TestSessionInputsCodecPresenceOwnership(t *testing.T) {
	empty := ""
	input := SessionInputs{
		Model: &model.ModelRef{Provider: "p", Model: "m"}, Prompt: &empty,
		Tools:    []string{"b", "a", "b"},
		Strategy: &StrategySelection{ID: "s", Settings: json.RawMessage(" { \"n\": 123456789012345678901 } ")},
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var got SessionInputs
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Prompt == nil || *got.Prompt != "" || !reflect.DeepEqual(got.Tools, []string{"b", "a"}) || !bytes.Equal(got.Strategy.Settings, input.Strategy.Settings) {
		t.Fatalf("round trip: %#v", got)
	}
	owned := got.Owned()
	owned.Tools[0] = "changed"
	owned.Strategy.Settings[0] = 'x'
	if got.Tools[0] != "b" || got.Strategy.Settings[0] != ' ' {
		t.Fatal("owned inputs alias source")
	}
	for _, raw := range []string{`{}`, `{"tools":[]}`} {
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			t.Fatal(err)
		}
		if (raw == `{}`) != (got.Tools == nil) || got.Prompt != nil {
			t.Fatalf("presence lost: %s: %#v", raw, got)
		}
		for _, value := range []SessionInputs{got, got.Owned()} {
			encoded, err := json.Marshal(value)
			if err != nil || string(encoded) != raw {
				t.Fatalf("presence round trip %s: %s (%v)", raw, encoded, err)
			}
		}
	}
	for _, raw := range []string{`{"tools":null}`, `{"tools":[""]}`, `{"prompt":"","instructions":""}`, `{"model":{}}`, `{"strategy":{"id":""}}`, `{"unknown":1}`, `{} {}`} {
		if err := json.Unmarshal([]byte(raw), &got); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestSessionStateValueCodec(t *testing.T) {
	type holder struct {
		State SessionStateValue `json:"state,omitempty"`
	}
	absent, err := json.Marshal(holder{})
	if err != nil || string(absent) != `{}` {
		t.Fatalf("absent state = %s (%v)", absent, err)
	}
	present, err := json.Marshal(holder{State: SessionStateValue("null")})
	if err != nil || string(present) != `{"state":"bnVsbA=="}` {
		t.Fatalf("JSON null state = %s (%v)", present, err)
	}
	values := map[string]SessionStateValue{"ext": []byte("null")}
	rawMap, err := json.Marshal(values)
	if err != nil || string(rawMap) != `{"ext":"bnVsbA=="}` {
		t.Fatalf("present state = %s (%v)", rawMap, err)
	}
	var decoded map[string]SessionStateValue
	if err := json.Unmarshal(rawMap, &decoded); err != nil || string(decoded["ext"]) != "null" {
		t.Fatalf("decoded present state = %q (%v)", decoded["ext"], err)
	}
	delete(values, "ext")
	rawMap, err = json.Marshal(values)
	if err != nil || string(rawMap) != `{}` {
		t.Fatalf("cleared state = %s (%v)", rawMap, err)
	}
	values["ext"] = nil
	if _, err := json.Marshal(values); err == nil {
		t.Fatal("accepted nil present state")
	}
	if err := json.Unmarshal([]byte(`{"ext":null}`), &values); err == nil {
		t.Fatal("accepted unframed null member")
	}
	for _, value := range []SessionStateValue{[]byte("null"), []byte("  {\"n\":123456789012345678901}  "), []byte(`"` + strings.Repeat("x", (1<<20)-2) + `"`)} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var got SessionStateValue
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(value, got) || (value == nil) != (got == nil) {
			t.Fatalf("round trip failed for length %d", len(value))
		}
		if len(value) > 0 {
			got[0] ^= 1
			if bytes.Equal(value, got) {
				t.Fatal("decoded bytes alias source")
			}
		}
	}
	for _, value := range []SessionStateValue{[]byte(""), []byte("{"), []byte(`"` + strings.Repeat("x", (1<<20)-1) + `"`)} {
		if _, err := json.Marshal(value); err == nil {
			t.Errorf("accepted invalid state of size %d", len(value))
		}
	}
	for _, raw := range []string{`123`, `"!!!!"`, `"e30=\n"`, `"ew=="`} {
		var got SessionStateValue
		if err := json.Unmarshal([]byte(raw), &got); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	oversize := []byte(`"` + strings.Repeat("x", (1<<20)-1) + `"`)
	var got SessionStateValue
	if err := json.Unmarshal([]byte(`"`+base64.StdEncoding.EncodeToString(oversize)+`"`), &got); err == nil {
		t.Fatal("accepted decoded state over 1 MiB")
	}
}
