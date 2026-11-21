package extension

import (
	"encoding/json"
	"errors"
	"testing"
)

var sectionManifest = Manifest{ID: "ext", Version: "1", APIVersion: APIVersion,
	SettingsSchema: json.RawMessage(`{"type":"object","properties":{"enabled":{"type":"boolean"},"value":{"type":"integer"}},"required":["enabled"],"additionalProperties":false}`)}

func compiled(t *testing.T, method Method) *CompiledMethod {
	t.Helper()
	contract, err := CompileMethod(method)
	if err != nil {
		t.Fatalf("CompileMethod: %v", err)
	}
	return contract
}

func TestSectionAndMethodValidatorsKeepExactValues(t *testing.T) {
	exact := Manifest{ID: "isolated", Version: "1", APIVersion: APIVersion, SettingsSchema: json.RawMessage(`{"type":"integer","const":9007199254740993}`)}
	if err := ValidateManifestSection(exact, json.RawMessage(`9007199254740993`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateManifestSection(exact, json.RawMessage(`9007199254740992`)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("section error = %v", err)
	}
	method := compiled(t, PureMethod("read", `{"type":"integer"}`, `{"type":"string"}`))
	if err := method.ValidateArguments(json.RawMessage(`12`)); err != nil {
		t.Fatal(err)
	}
	if err := method.ValidateArguments(json.RawMessage(`"12"`)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("argument error = %v", err)
	}
	if err := method.ValidateResult(Result{Value: json.RawMessage(`"ok"`)}); err != nil {
		t.Fatal(err)
	}
	if err := method.ValidateResult(Result{Value: json.RawMessage(`12`)}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("result error = %v", err)
	}
	if err := ValidateManifestSection(sectionManifest, json.RawMessage(`{"enabled":true,"value":9007199254740993}`)); err != nil {
		t.Fatalf("exact integer rejected: %v", err)
	}
	if err := ValidateManifestSection(sectionManifest, json.RawMessage(`{"enabled":true,"value":"1"}`)); err == nil {
		t.Fatal("string was coerced into an integer value")
	}
	if err := ValidateManifestSection(sectionManifest, json.RawMessage(`{"enabled":true,"extra":1}`)); err == nil {
		t.Fatal("undeclared section member accepted")
	}
}

func TestValidateResultDeclaredFailureUnion(t *testing.T) {
	method := compiled(t, ServiceMethod("call", ModeServiceIO, `{}`, `{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"]}`, `{"type":"object","properties":{"reason":{"type":"string"}},"required":["reason"]}`, `{"type":"string"}`))
	if err := method.ValidateResult(Result{Value: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatalf("valid success: %v", err)
	}
	failure := Result{Failure: &Failure{Code: "invalid", Message: "bad", Data: json.RawMessage(`{"reason":"x"}`), Partial: json.RawMessage(`"partial"`)}}
	if err := method.ValidateResult(failure); err != nil {
		t.Fatalf("valid failure: %v", err)
	}
	for _, row := range []struct {
		name   string
		result Result
	}{
		{"both", Result{Value: json.RawMessage(`{}`), Failure: &Failure{Code: "x", Message: "y"}}},
		{"neither", Result{}},
		{"empty failure identity", Result{Failure: &Failure{Code: "", Message: "y"}}},
		{"wrong success shape", Result{Value: json.RawMessage(`{"bad":true}`)}},
		{"wrong partial", Result{Failure: &Failure{Code: "x", Message: "y", Partial: json.RawMessage(`{}`)}}},
	} {
		if err := method.ValidateResult(row.result); err == nil {
			t.Fatalf("%s: ValidateResult accepted an invalid union", row.name)
		}
	}
}

func TestFailurePayloadPresenceSemantics(t *testing.T) {
	bare := compiled(t, ServiceMethod("bare", ModeServiceIO, `{}`, `{}`, "", ""))
	data := compiled(t, ServiceMethod("data", ModeServiceIO, `{}`, `{}`, `{"type":"object","required":["reason"],"properties":{"reason":{"type":"string"}},"additionalProperties":false}`, ""))
	partial := compiled(t, ServiceMethod("partial", ModeServiceIO, `{}`, `{}`, "", `{"type":"string"}`))
	accepted := []struct {
		name   string
		method *CompiledMethod
		result Result
	}{
		{"bare failure", bare, Result{Failure: &Failure{Code: "invalid", Message: "bad"}}},
		{"zero-length data is absent", data, Result{Failure: &Failure{Code: "invalid", Message: "bad", Data: json.RawMessage{}}}},
		{"zero-length partial is absent", partial, Result{Failure: &Failure{Code: "invalid", Message: "bad", Partial: json.RawMessage{}}}},
		{"declared data", data, Result{Failure: &Failure{Code: "invalid", Message: "bad", Data: json.RawMessage(`{"reason":"x"}`)}}},
		{"partial without error schema", partial, Result{Failure: &Failure{Code: "invalid", Message: "bad", Partial: json.RawMessage(`"p"`)}}},
	}
	for _, row := range accepted {
		if err := row.method.ValidateResult(row.result); err != nil {
			t.Fatalf("%s: %v", row.name, err)
		}
	}
	rejected := []struct {
		name   string
		method *CompiledMethod
		result Result
	}{
		{"undeclared data", bare, Result{Failure: &Failure{Code: "x", Message: "y", Data: json.RawMessage(`{}`)}}},
		{"whitespace data on bare", bare, Result{Failure: &Failure{Code: "x", Message: "y", Data: json.RawMessage("  ")}}},
		{"undeclared partial", bare, Result{Failure: &Failure{Code: "x", Message: "y", Partial: json.RawMessage(`"p"`)}}},
		{"whitespace partial on bare", bare, Result{Failure: &Failure{Code: "x", Message: "y", Partial: json.RawMessage("  ")}}},
		{"whitespace data on declared", data, Result{Failure: &Failure{Code: "x", Message: "y", Data: json.RawMessage("  ")}}},
		{"wrong declared data", data, Result{Failure: &Failure{Code: "x", Message: "y", Data: json.RawMessage(`{"reason":5}`)}}},
		{"whitespace partial on declared", partial, Result{Failure: &Failure{Code: "x", Message: "y", Partial: json.RawMessage("  ")}}},
		{"wrong declared partial", partial, Result{Failure: &Failure{Code: "x", Message: "y", Partial: json.RawMessage(`5`)}}},
		{"whitespace success", bare, Result{Value: json.RawMessage("  ")}},
		{"empty code", bare, Result{Failure: &Failure{Code: "", Message: "y"}}},
		{"empty message", bare, Result{Failure: &Failure{Code: "x", Message: ""}}},
	}
	for _, row := range rejected {
		if err := row.method.ValidateResult(row.result); err == nil {
			t.Fatalf("%s accepted", row.name)
		}
	}
}

// TestWhitespaceOptionalSchemaDocumentsAreMalformed proves zero raw length is
// absence while nonempty whitespace is a present, malformed schema document.
func TestWhitespaceOptionalSchemaDocumentsAreMalformed(t *testing.T) {
	if _, err := CompileMethod(ServiceMethod("call", ModeServiceIO, `{}`, `{}`, "  ", "")); err == nil {
		t.Fatal("CompileMethod accepted a whitespace error schema document")
	}
	if _, err := CompileMethod(ServiceMethod("call", ModeServiceIO, `{}`, `{}`, "", "  ")); err == nil {
		t.Fatal("CompileMethod accepted a whitespace partial schema document")
	}
	if _, err := CompileMethod(ServiceMethod("call", ModeServiceIO, `{}`, `{}`, "", `{"type":"string"}`)); err != nil {
		t.Fatalf("CompileMethod refused partial without an error schema: %v", err)
	}
}

func TestJSONAcceptanceRequiresSingleValueEOF(t *testing.T) {
	method := compiled(t, ServiceMethod("call", ModeServiceIO, `{"type":"object"}`, `{"type":"object"}`, `{"type":"object"}`, `{"type":"object"}`))
	for _, tail := range []string{" {}", " null", ` 1`, " ]", `{"x":`} {
		if err := ValidateManifestSection(sectionManifest, json.RawMessage(`{"enabled":true}`+tail)); err == nil {
			t.Fatalf("section accepted %q after the first value", tail)
		}
		value := json.RawMessage(`{}` + tail)
		if err := method.ValidateArguments(value); err == nil {
			t.Fatalf("arguments accepted %q after the first value", tail)
		}
		for _, result := range []Result{
			{Value: value},
			{Failure: &Failure{Code: "x", Message: "y", Data: value}},
			{Failure: &Failure{Code: "x", Message: "y", Partial: value}},
		} {
			if err := method.ValidateResult(result); err == nil {
				t.Fatalf("result accepted %q after the first value", tail)
			}
		}
	}
	if err := ValidateManifestSection(sectionManifest, json.RawMessage("{\"enabled\":true}  \n\t")); err != nil {
		t.Fatalf("trailing whitespace-only section rejected: %v", err)
	}
	if err := method.ValidateArguments(json.RawMessage("{} \n")); err != nil {
		t.Fatalf("trailing whitespace-only arguments rejected: %v", err)
	}
}

func TestCompiledSchemasAreReusedOnlyForIdenticalBytes(t *testing.T) {
	input := `{"type":"object","required":["name"],"properties":{"name":{"type":"string"}},"additionalProperties":false}`
	first := compiled(t, ServiceIOMethod("call", input, `{"type":"object"}`))
	second := compiled(t, ServiceIOMethod("call", input, `{"type":"object"}`))
	other := compiled(t, ServiceIOMethod("other", input, `{"type":"object"}`))
	if first.input != second.input {
		t.Fatal("identical method recompiled its input schema")
	}
	if first.input == other.input {
		t.Fatal("identical bytes under another method label served the first label's schema")
	}
	stringInput := compiled(t, ServiceIOMethod("call", `{"type":"string"}`, `{"type":"object"}`))
	if stringInput.input == first.input {
		t.Fatal("same label with different bytes served the first compilation's schema")
	}
	if err := stringInput.ValidateArguments(json.RawMessage(`{}`)); err == nil {
		t.Fatal("string input schema accepted {}")
	}
	if err := first.ValidateArguments(json.RawMessage(`{"name":"x"}`)); err != nil {
		t.Fatalf("valid arguments rejected: %v", err)
	}
	if err := first.ValidateArguments(json.RawMessage(`{"name":5}`)); err == nil {
		t.Fatal("wrong argument type accepted")
	}
}

func TestValidateResultEnvelopeShared(t *testing.T) {
	for _, row := range []struct {
		name   string
		result Result
		ok     bool
	}{
		{"success", Result{Value: json.RawMessage("null")}, true},
		{"present empty success", Result{Value: json.RawMessage{}}, true},
		{"failure", Result{Failure: &Failure{Code: "x", Message: "y"}}, true},
		{"both", Result{Value: json.RawMessage("null"), Failure: &Failure{Code: "x", Message: "y"}}, false},
		{"neither", Result{}, false},
		{"empty code", Result{Failure: &Failure{Code: "", Message: "y"}}, false},
		{"empty message", Result{Failure: &Failure{Code: "x", Message: ""}}, false},
	} {
		if err := ValidateResultEnvelope(row.result); (err == nil) != row.ok {
			t.Fatalf("%s: ValidateResultEnvelope = %v, want ok=%v", row.name, err, row.ok)
		}
	}
}

func TestHostServiceContracts(t *testing.T) {
	for _, name := range []string{ContractData, ContractCredentials, ContractCommands, ContractWorkspace, ContractSettings, ContractWarnings, ContractState, ContractSession} {
		methods, ok := HostService(name)
		if !ok || len(methods) == 0 {
			t.Fatalf("%s: missing host contract", name)
		}
		for _, method := range methods {
			if _, err := CompileMethod(method); err != nil {
				t.Fatalf("%s/%s: %v", name, method.Name, err)
			}
		}
	}
	if _, ok := HostService("melta.child_launch"); ok {
		t.Fatal("removed host service resolved")
	}
	data, _ := HostService(ContractData)
	claim := compiled(t, data[2])
	if claim.Name != "claim_external_data" || claim.ValidateArguments(json.RawMessage(`{}`)) != nil || claim.ValidateArguments(json.RawMessage(`{"session_id":"x"}`)) == nil {
		t.Fatal("claim_external_data accepts only the empty object")
	}
}
