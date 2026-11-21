package extension

import "encoding/json"

// Melta service names an extension reaches through its API Call. Each is
// granted on enablement from the manifest's requested services.
const (
	ContractPrompt      = "melta.prompt"
	ContractData        = "melta.data"
	ContractCredentials = "melta.credentials" // #nosec G101 -- Service identifier, not a credential value.
	ContractCommands    = "melta.commands"
	ContractWorkspace   = "melta.workspace"
	ContractSettings    = "melta.settings"
	ContractWarnings    = "melta.warnings"
	ContractState       = "melta.state"
	ContractSession     = "melta.session"
)

// HostProvider is the reserved provider identity of Melta's own services; no
// extension may use it as its ID.
const HostProvider = "host"

// Mode is one method's semantic mode: pure or with service I/O.
type Mode string

const (
	ModePure      Mode = "pure"
	ModeServiceIO Mode = "service_io"
)

func (m Mode) valid() bool { return m == ModePure || m == ModeServiceIO }

// HookMode is one hook attachment's semantic mode. It is separate from the
// method Mode domain.
type HookMode string

const (
	HookModePure      HookMode = "pure"
	HookModeEffectful HookMode = "effectful"
)

// ExposedTargetKind is the closed target-scope kind a client operation
// accepts.
type ExposedTargetKind string

const (
	ExposedTargetHarness   ExposedTargetKind = "harness"
	ExposedTargetWorkspace ExposedTargetKind = "workspace"
	ExposedTargetSession   ExposedTargetKind = "session"
)

// SecretLifetime names when one declared credential reference resolves.
type SecretLifetime string

const (
	SecretActivation SecretLifetime = "activation"
	SecretOperation  SecretLifetime = "operation"
)

// SecretRef declares one JSON-pointer field in the active section whose value
// names a credential. Metadata is inspected without executing the extension.
type SecretRef struct {
	Field    string         `json:"field"`
	Lifetime SecretLifetime `json:"lifetime"`
}

// Method declares one method and its closed input/result domain. A present
// Error schema declares failure data, a present Partial schema declares an
// optional partial result.
type Method struct {
	Name    string          `json:"name"`
	Mode    Mode            `json:"mode"`
	Input   json.RawMessage `json:"input"`
	Success json.RawMessage `json:"success"`
	Error   json.RawMessage `json:"error,omitempty"`
	Partial json.RawMessage `json:"partial,omitempty"`
}

// ServiceMethod constructs one method declaration.
func ServiceMethod(name string, mode Mode, input, success, failure, partial string) Method {
	out := Method{Name: name, Mode: mode, Input: json.RawMessage(input), Success: json.RawMessage(success)}
	if failure != "" {
		out.Error = json.RawMessage(failure)
	}
	if partial != "" {
		out.Partial = json.RawMessage(partial)
	}
	return out
}

// PureMethod and ServiceIOMethod are the two shorthand constructors for the
// common no-failure method shape.
func PureMethod(name, input, success string) Method {
	return ServiceMethod(name, ModePure, input, success, "", "")
}

func ServiceIOMethod(name, input, success string) Method {
	return ServiceMethod(name, ModeServiceIO, input, success, "", "")
}

// HostService returns the method contracts of one of Melta's own services
// whose arguments Melta validates by schema.
func HostService(name string) ([]Method, bool) {
	switch name {
	case ContractData:
		return hostDataMethods(), true
	case ContractCredentials:
		return hostCredentialsMethods(), true
	case ContractCommands:
		return hostCommandsMethods(), true
	case ContractWorkspace:
		return hostWorkspaceMethods(), true
	case ContractSettings:
		return hostSettingsMethods(), true
	case ContractWarnings:
		return hostWarningsMethods(), true
	case ContractState:
		return hostStateMethods(), true
	case ContractSession:
		return hostSessionMethods(), true
	}
	return nil, false
}

func hostWorkspaceMethods() []Method {
	outcome := `{"type":"object","required":["outcome"],"properties":{"outcome":{"const":"completed"}},"additionalProperties":false}`
	partial := `{"type":"object","required":["outcome"],"properties":{"outcome":{"enum":["completed","uncertain"]}},"additionalProperties":false}`
	return []Method{
		ServiceMethod("bind", ModePure, `{"type":"object","required":["path"],"properties":{"path":{"type":"string","minLength":1}},"additionalProperties":false}`, `{"type":"object","required":["binding","canonical_path","exists"],"properties":{"binding":{"type":"string"},"canonical_path":{"type":"string"},"exists":{"type":"boolean"}},"additionalProperties":false}`, "", ""),
		ServiceMethod("read", ModeServiceIO, `{"type":"object","required":["binding"],"properties":{"binding":{"type":"string","minLength":1}},"additionalProperties":false}`, `{"type":"object","required":["bytes"],"properties":{"bytes":{"type":"string"}},"additionalProperties":false}`, "", ""),
		ServiceMethod("write", ModeServiceIO, `{"type":"object","required":["binding","bytes","create_only"],"properties":{"binding":{"type":"string","minLength":1},"bytes":{"type":"string"},"create_only":{"type":"boolean"}},"additionalProperties":false}`, outcome, "", partial),
		ServiceMethod("delete", ModeServiceIO, `{"type":"object","required":["binding"],"properties":{"binding":{"type":"string","minLength":1}},"additionalProperties":false}`, outcome, "", partial),
	}
}

func hostSettingsMethods() []Method {
	return []Method{
		ServiceIOMethod("update", `{"type":"object","required":["service","method","arguments"],"properties":{"service":{"type":"string","minLength":1},"method":{"type":"string","minLength":1},"arguments":{}},"additionalProperties":false}`, `{"type":"null"}`),
	}
}

func hostWarningsMethods() []Method {
	return []Method{
		PureMethod("report", `{"type":"object","required":["kind","message"],"properties":{"kind":{"type":"string"},"message":{"type":"string"}},"additionalProperties":false}`, `{"type":"null"}`),
	}
}

// hostDataMethods are the extension's own Session and cache namespaces plus
// the durable external-data claim.
func hostDataMethods() []Method {
	directory := `{"type":"object","required":["directory"],"properties":{"directory":{"type":"string","minLength":1}},"additionalProperties":false}`
	return []Method{
		ServiceIOMethod("session_namespace", `{"type":"object","additionalProperties":false}`, directory),
		ServiceIOMethod("cache_namespace", `{"type":"object","additionalProperties":false}`, directory),
		ServiceIOMethod("claim_external_data", `{"type":"object","additionalProperties":false}`, `{"type":"null"}`),
	}
}

// hostCredentialsMethods return one declared credential value addressed by
// its declared section field.
func hostCredentialsMethods() []Method {
	return []Method{ServiceIOMethod("get",
		`{"type":"object","required":["field"],"properties":{"field":{"type":"string","minLength":1}},"additionalProperties":false}`,
		`{"type":"object","required":["value"],"properties":{"value":{"type":"string"}},"additionalProperties":false}`)}
}

// hostCommandsMethods are one method per ordinary command producer. Request
// and result schemas remain object-shaped because the accepting handler
// decodes and validates the exact generated protocol value itself; controls
// take one closed empty object and return the existing null or generated
// receipt.
func hostCommandsMethods() []Method {
	const (
		request = `{"type":"object"}`
		control = `{"type":"object","additionalProperties":false}`
		value   = `{"type":"object"}`
		null    = `{"type":"null"}`
	)
	return []Method{
		ServiceIOMethod("create_session", request, value),
		ServiceIOMethod("submit", request, value),
		ServiceIOMethod("fork", request, value),
		ServiceIOMethod("compact", request, value),
		ServiceIOMethod("set_inputs", request, value),
		ServiceIOMethod("set_model", request, value),
		ServiceIOMethod("interrupt", control, null),
		ServiceIOMethod("stop", control, null),
		ServiceIOMethod("archive", control, value),
		ServiceIOMethod("reopen", control, value),
		ServiceIOMethod("delete", control, value),
		ServiceIOMethod("revert_code", request, value),
	}
}

// hostStateMethods are the scoped Session-state writes: one arbitrary JSON
// value at a time under the calling extension's own Session state.
func hostStateMethods() []Method {
	return []Method{
		ServiceIOMethod("set", `{"type":"object","required":["value"],"properties":{"value":{}},"additionalProperties":false}`, `{"type":"null"}`),
		ServiceIOMethod("clear", `{"type":"object","additionalProperties":false}`, `{"type":"null"}`),
	}
}

// hostSessionMethods expose the calling Session's live ownership group and
// its one prepared owned-Session start.
func hostSessionMethods() []Method {
	const empty = `{"type":"object","additionalProperties":false}`
	const started = `{"type":"object","required":["session_id"],"properties":{"session_id":{"type":"string","minLength":1}},"additionalProperties":false}`
	members := `{"type":"array","items":{"type":"object","required":["kind","id","starter"],"properties":{"kind":{"enum":["session","job"]},"id":{"type":"string"},"starter":{"type":"string"}},"additionalProperties":false}}`
	return []Method{
		PureMethod("members", empty, members),
		ServiceMethod("start", ModeServiceIO, empty, started, "", started),
	}
}
