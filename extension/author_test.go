package extension

import (
	"encoding/json"
	"testing"
)

func TestRegistrationMethodAndManifestValidation(t *testing.T) {
	manifest := Manifest{ID: "example", Version: "1", APIVersion: APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	mismatch := manifest
	mismatch.APIVersion = "unsupported"
	if err := ValidateManifest(mismatch); err == nil {
		t.Fatal("API mismatch accepted")
	}
	reserved := manifest
	reserved.ID = HostProvider
	if err := ValidateManifest(reserved); err == nil {
		t.Fatal("reserved host identity accepted")
	}
	if err := ValidateManifestSection(manifest, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManifestSection(manifest, json.RawMessage(`[]`)); err == nil {
		t.Fatal("invalid section accepted")
	}
	method := Method{Name: "get", Input: json.RawMessage(`{"type":"object"}`), Success: json.RawMessage(`{"type":"string"}`)}
	bound, err := CompileMethod(method)
	if err != nil {
		t.Fatal(err)
	}
	method.Input[0] = 'x'
	if err := bound.ValidateArguments(json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := bound.ValidateArguments(json.RawMessage(`[]`)); err == nil {
		t.Fatal("invalid arguments accepted")
	}
	if _, err := CompileMethod(Method{}); err == nil {
		t.Fatal("invalid method accepted")
	}
}
