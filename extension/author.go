package extension

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// APIVersion is the exact activation authoring contract version.
const APIVersion = "9"

// Manifest is the metadata available before an extension is activated.
type Manifest struct {
	ID             string          `json:"id"`
	Version        string          `json:"version"`
	APIVersion     string          `json:"api_version"`
	SettingsSchema json.RawMessage `json:"settings_schema"`
	Services       []string        `json:"services,omitempty"`
	Secrets        []SecretRef     `json:"secrets,omitempty"`
}

// Extension is a native entry point. Registrations belong to its activation.
type Extension struct {
	Manifest Manifest
	Activate func(context.Context, API) (func(context.Context) error, error)
}

// ValidateManifest validates metadata without requiring a settings section.
func ValidateManifest(m Manifest) error {
	_, err := compileManifest(m)
	return err
}

// ValidateManifestSection validates one extension's captured settings.
func ValidateManifestSection(m Manifest, section json.RawMessage) error {
	schema, err := compileManifest(m)
	if err != nil {
		return err
	}
	return validateSection(m.ID, schema, section)
}

func compileManifest(m Manifest) (*jsonschema.Schema, error) {
	if m.ID == "" || m.ID == HostProvider || m.Version == "" || m.APIVersion != APIVersion {
		return nil, invalid("invalid extension identity or API version")
	}
	seen := make(map[string]bool)
	for _, ref := range m.Secrets {
		if !ValidSecretField(ref.Field) || seen[ref.Field] || (ref.Lifetime != SecretActivation && ref.Lifetime != SecretOperation) {
			return nil, invalid("extension %q: invalid secret reference %q", m.ID, ref.Field)
		}
		seen[ref.Field] = true
	}
	seen = make(map[string]bool)
	for _, service := range m.Services {
		if service == "" || seen[service] {
			return nil, invalid("extension %q: invalid requested service %q", m.ID, service)
		}
		seen[service] = true
	}
	schema, err := compileSchema(m.SettingsSchema, m.ID+"/settings")
	if err != nil {
		return nil, invalid("extension %q settings schema: %v", m.ID, err)
	}
	return schema, nil
}

// CompileMethod compiles one independently registered method.
func CompileMethod(m Method) (*CompiledMethod, error) {
	if m.Name == "" || !m.Mode.valid() {
		return nil, invalid("invalid method name or mode")
	}
	m.Input = append(json.RawMessage(nil), m.Input...)
	m.Success = append(json.RawMessage(nil), m.Success...)
	m.Error = append(json.RawMessage(nil), m.Error...)
	m.Partial = append(json.RawMessage(nil), m.Partial...)
	return compileMethod(m)
}

// Registration identifies one immutable registration.
type Registration interface{}

// API is the activation's registration and service-lookup entry point.
type API interface {
	Settings() json.RawMessage
	Remove(Registration) error
	RegisterTool(model.ToolDefinition, RegisteredTool, Registration) (Registration, error)
	RegisterStrategy(harness.StrategyBinding, Registration) (Registration, error)
	RegisterService(Service, Registration) (Registration, error)
	RegisterOperation(Operation, Registration) (Registration, error)
	AttachAdmission(AdmissionHook, Registration) (Registration, error)
	AttachRequest(HookMode, AuthorRequestHook, Registration) (Registration, error)
	AttachArguments(HookMode, ArgumentsHook, Registration) (Registration, error)
	AttachAuthorization(AuthorizationHook, Registration) (Registration, error)
	AttachResult(ResultHook, Registration) (Registration, error)
	RegisterCleanup(CleanupHook, Registration) (Registration, error)
	AttachObserver(Observer, Registration) (Registration, error)
	Call(context.Context, string, string, json.RawMessage) (Result, error)
	// Command issues one independent attributed command for work the extension
	// does on its own. Work done for a client or Operation goes through that
	// callback's own AuthorContext.Call, within its limits. A context Melta
	// handed to a callback is denied; a nil context is independent.
	Command(context.Context, CommandTarget, string, json.RawMessage) (Result, error)
}

// Observer receives one committed fact. It receives no AuthorContext and no
// Call: observation grants no command or effect authority.
type Observer func(context.Context, Fact) error

// The closed committed fact kinds an observer receives.
const (
	FactOperationAdmitted = "operation_admitted"
	FactOperationSettled  = "operation_settled"
)

// Fact is one committed passive observation carrying its Session and
// Operation identity. Status is the closed terminal OperationState and is
// present only for a settled Operation.
type Fact struct {
	Kind        string `json:"kind"`
	SessionID   string `json:"session_id"`
	OperationID string `json:"operation_id"`
	Status      string `json:"status,omitempty"`
}

// CommandTarget is one independent command's bound target: the whole owner,
// one Workspace, or one Session.
type CommandTarget struct {
	Kind      ExposedTargetKind `json:"kind"`
	Workspace string            `json:"workspace,omitempty"`
	SessionID string            `json:"session_id,omitempty"`
}

// AuthorContext supplies the values and scoped service calls of one callback.
type AuthorContext struct {
	RequestID   string
	ExtensionID string
	Settings    json.RawMessage
	Session     harness.SessionIdentity
	State       harness.SessionStateValue
	Call        func(context.Context, string, string, json.RawMessage) (Result, error)
}

// RegisteredTool is the typed implementation paired with a tool definition.
type RegisteredTool interface {
	Normalize(AuthorContext, model.ToolCall) (json.RawMessage, error)
	Prepare(context.Context, AuthorContext, model.ToolCall) ToolPlan
	Execute(context.Context, AuthorContext, ToolPlan) harness.ToolOutcome
}

// RegisteredMethod pairs a method definition with its implementation.
type RegisteredMethod struct {
	Method  Method
	Handler AuthorHandler
}

// Service is a named set of methods available to extensions.
type Service struct {
	Name    string
	Methods []RegisteredMethod
}

// Operation is a method exposed to clients under its extension's identity.
type Operation struct {
	Target  ExposedTargetKind
	Method  Method
	Handler AuthorHandler
}

type AuthorHandler func(context.Context, AuthorContext, json.RawMessage) (Result, error)

// AdmissionInput is the captured input and candidate execution.
type AdmissionInput struct {
	Session      harness.SessionIdentity
	Input        []model.ContentPart
	Candidate    AdmissionCandidate
	ModelCatalog []model.ModelRef
	ToolCatalog  []model.ToolDefinition
	Prompt       PromptAccess
}

// PromptAccess supplies prompt facts on demand without assembling an unread prompt.
type PromptAccess struct {
	Default     func() (string, error)
	Sections    func() []prompt.Section
	Rules       func() (string, []prompt.Section, error)
	Environment func() string
}

// AdmissionCandidate is the execution input being composed.
type AdmissionCandidate struct {
	Model        model.ModelRef
	Prompt       *string
	Instructions string
	Tools        []model.ToolDefinition
	Strategy     harness.StrategySelection
}

type AdmissionResult struct {
	Candidate *AdmissionCandidate
	Refusal   *string
}

type AdmissionHook func(context.Context, AuthorContext, AdmissionInput) (AdmissionResult, error)

// AuthorRequestInput adds the admitted prompt choice to the request view.
type AuthorRequestInput struct {
	harness.RequestHookInput
	PromptAssembled bool
}

type AuthorRequestHook func(context.Context, AuthorContext, AuthorRequestInput) (harness.RequestConsequence, error)

// ArgumentsResult distinguishes unchanged arguments, replacement and refusal.
type ArgumentsResult struct {
	Arguments json.RawMessage
	Refusal   *string
}

type ArgumentsHook func(context.Context, AuthorContext, model.ToolCall) (ArgumentsResult, error)

// AuthorizationInput carries the policy root and prepared call; location
// binding remains a separate scoped operation.
type AuthorizationInput struct {
	ToolName           string
	Arguments          json.RawMessage
	CanonicalWorkspace string
	ToolSettings       json.RawMessage // effective captured baseline settings, including defaults
	Permissions        []harness.PermissionRequest
}

type AuthorizationHook func(context.Context, AuthorContext, AuthorizationInput) (*string, error)
type ResultHook func(context.Context, AuthorContext, harness.ToolOutputInput) (string, error)
type CleanupHook func(context.Context, AuthorContext, harness.SessionIdentity) error

// SessionStart is data in a tool plan, not authority to start a Session.
type SessionStart struct {
	Target       string
	Inputs       harness.SessionInputs
	InitialState harness.SessionStateValue
	Input        []model.ContentPart
	Report       bool
	OutputLimit  int
}
