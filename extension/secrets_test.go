package extension

import (
	"encoding/json"
	"testing"
)

func TestSecretNameResolvesJSONPointerValues(t *testing.T) {
	section := json.RawMessage(`{"api":{"key":"FIRST_KEY"},"keys":["ZERO_KEY","SECOND_KEY"],"slash/key":"SLASH_KEY","tilde~key":"TILDE_KEY","tilde~2key":"TILDE_TWO_KEY","trailing~":"TRAILING_KEY","numeric":{"01":"ZERO_ONE_KEY","+1":"PLUS_KEY","-1":"MINUS_KEY","1":"ONE_KEY"},"empty":"","number":5}`)
	for _, row := range []struct {
		name   string
		field  string
		want   string
		wantOK bool
	}{
		{"nested object", "/api/key", "FIRST_KEY", true},
		{"canonical array index", "/keys/1", "SECOND_KEY", true},
		{"zero array index", "/keys/0", "ZERO_KEY", true},
		{"leading-zero index", "/keys/01", "", false},
		{"signed index", "/keys/+1", "", false},
		{"negative index", "/keys/-1", "", false},
		{"dash index", "/keys/-", "", false},
		{"space index", "/keys/ 1", "", false},
		{"fractional index", "/keys/1.0", "", false},
		{"out-of-range index", "/keys/2", "", false},
		{"numeric object key stays exact", "/numeric/01", "ZERO_ONE_KEY", true},
		{"signed object key stays exact", "/numeric/+1", "PLUS_KEY", true},
		{"negative object key stays exact", "/numeric/-1", "MINUS_KEY", true},
		{"numeric object key one", "/numeric/1", "ONE_KEY", true},
		{"escaped slash", "/slash~1key", "SLASH_KEY", true},
		{"escaped tilde", "/tilde~0key", "TILDE_KEY", true},
		{"escaped tilde then literal two", "/tilde~02key", "TILDE_TWO_KEY", true},
		{"invalid tilde escape", "/tilde~2key", "", false},
		{"trailing tilde escape", "/trailing~", "", false},
		{"escaped trailing tilde", "/trailing~0", "TRAILING_KEY", true},
		{"missing member", "/api/other", "", false},
		{"non-string value", "/number", "", false},
		{"empty value", "/empty", "", false},
		{"traversal through a string", "/api/key/deeper", "", false},
		{"empty pointer", "", "", false},
		{"malformed section", "/api", "", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			input := section
			if row.name == "malformed section" {
				input = json.RawMessage(`{`)
			}
			got, ok := SecretName(input, row.field)
			if got != row.want || ok != row.wantOK {
				t.Fatalf("SecretName(%q) = (%q, %v), want (%q, %v)", row.field, got, ok, row.want, row.wantOK)
			}
		})
	}
}

func TestSecretRefPointerGrammarAtManifestValidation(t *testing.T) {
	valid := Manifest{ID: "valid", Version: "1", APIVersion: APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`), Secrets: []SecretRef{
		{Field: "/unresolved", Lifetime: SecretActivation},
		{Field: "/array/0", Lifetime: SecretOperation},
		{Field: "/object/-1", Lifetime: SecretActivation},
	}}
	if err := ValidateManifest(valid); err != nil {
		t.Fatalf("ValidateManifest valid unresolved SecretRef fields: %v", err)
	}
	for _, field := range []string{"/bad~2", "/bad~", "no-slash", ""} {
		malformed := valid
		malformed.Secrets = []SecretRef{{Field: field, Lifetime: SecretActivation}}
		if err := ValidateManifest(malformed); err == nil {
			t.Fatalf("ValidateManifest SecretRef field %q accepted", field)
		}
	}
}

func TestCredentialsHostContract(t *testing.T) {
	methods, ok := HostService(ContractCredentials)
	if !ok || len(methods) != 1 || methods[0].Name != "get" {
		t.Fatalf("host credentials methods = %+v, want the one get method", methods)
	}
}
