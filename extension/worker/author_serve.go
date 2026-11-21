package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
)

type HelloParams struct {
	APIVersion string `json:"api_version"`
}

type HelloResult struct {
	Manifest AuthorMetadata `json:"manifest"`
}

type AuthorMetadata struct {
	ID             string                `json:"id"`
	Version        string                `json:"version"`
	APIVersion     string                `json:"api_version"`
	SettingsSchema JSONBytes             `json:"settings_schema"`
	Services       []string              `json:"services,omitempty"`
	Secrets        []extension.SecretRef `json:"secrets,omitempty"`
}

func NewAuthorMetadata(manifest extension.Manifest) AuthorMetadata {
	return AuthorMetadata{ID: manifest.ID, Version: manifest.Version, APIVersion: manifest.APIVersion,
		SettingsSchema: JSONBytes(manifest.SettingsSchema), Services: manifest.Services, Secrets: manifest.Secrets}
}

func (metadata AuthorMetadata) Value() extension.Manifest {
	return extension.Manifest{ID: metadata.ID, Version: metadata.Version, APIVersion: metadata.APIVersion,
		SettingsSchema: append(json.RawMessage(nil), metadata.SettingsSchema...), Services: metadata.Services, Secrets: metadata.Secrets}
}

type AuthorActivateParams struct {
	Section JSONBytes `json:"section"`
	Token   string    `json:"token"`
}

type AuthorCallContext struct {
	Token       string                  `json:"token"`
	RequestID   string                  `json:"request_id,omitempty"`
	ExtensionID string                  `json:"extension_id"`
	Settings    JSONBytes               `json:"settings"`
	Session     harness.SessionIdentity `json:"session"`
	State       JSONBytes               `json:"state"`
}

type AuthorCallParams struct {
	Handle    string            `json:"handle"`
	Method    string            `json:"method"`
	Context   AuthorCallContext `json:"context"`
	Arguments JSONBytes         `json:"arguments"`
}

type AuthorHostcallParams struct {
	Token     string    `json:"token"`
	Service   string    `json:"service"`
	Method    string    `json:"method"`
	Arguments JSONBytes `json:"arguments"`
}

// AuthorCommandParams is one independent command request: the fixed target,
// the ordinary command method name and its generated argument bytes. It
// carries no call context token.
type AuthorCommandParams struct {
	Target    extension.CommandTarget `json:"target"`
	Method    string                  `json:"method"`
	Arguments JSONBytes               `json:"arguments"`
}

type authorServer struct {
	extension extension.Extension
	started   bool
	close     func(context.Context) error
	pending   map[string]chan Frame
	handlers  map[string]*authorRegistration
}

// Serve serves an extension entry over the author worker protocol.
func Serve(ctx context.Context, author extension.Extension, in io.Reader, out io.Writer) error {
	if in == nil || out == nil || author.Activate == nil {
		return errors.New("worker: author requires streams and an entry point")
	}
	if err := extension.ValidateManifest(author.Manifest); err != nil {
		return err
	}
	s := &server{
		ctx: ctx, author: &authorServer{extension: author, pending: make(map[string]chan Frame), handlers: make(map[string]*authorRegistration)},
		reader: NewReader(in), writer: NewWriter(out),
		requests: make(map[string]context.CancelFunc), pending: make(map[string]chan hostcallOutcome),
		done: make(chan struct{}), fatal: make(chan error, 1),
	}
	return s.run()
}

func (s *server) authorHello(frame Frame) error {
	if frame.Kind != KindRequest || frame.ID == "" || frame.Method != MethodHello {
		return errors.New("worker: first frame must be hello")
	}
	var params HelloParams
	if err := json.Unmarshal(frame.Params, &params); err != nil {
		return s.rejectActivation(frame.ID, err)
	}
	if params.APIVersion != s.author.extension.Manifest.APIVersion {
		return s.rejectActivation(frame.ID, errors.New("worker: incompatible protocol or API version"))
	}
	result, err := json.Marshal(HelloResult{Manifest: NewAuthorMetadata(s.author.extension.Manifest)})
	if err != nil {
		return err
	}
	return s.write(Frame{Kind: KindResponse, ID: frame.ID, Result: result})
}

// authorHostCallKey marks the context of one host call the worker is serving.
// The worker's independent command route refuses such a context, except for
// observer delivery, which carries no call authority.
type authorHostCallKey struct{}

func (s *server) admitAuthorRequest(frame Frame, ctx context.Context, cancel context.CancelFunc) error {
	if frame.Method == "release" && s.author.started {
		var params struct {
			Handle string `json:"handle"`
		}
		if err := json.Unmarshal(frame.Params, &params); err != nil || params.Handle == "" {
			cancel()
			return badRoute("worker: invalid release")
		}
		s.mu.Lock()
		_, ok := s.author.handlers[params.Handle]
		delete(s.author.handlers, params.Handle)
		s.mu.Unlock()
		cancel()
		if !ok {
			return badRoute("worker: unknown author registration")
		}
		s.respond(frame.ID, json.RawMessage("null"), nil)
		return nil
	}
	if frame.Method == MethodCall && s.author.started {
		var params AuthorCallParams
		if err := json.Unmarshal(frame.Params, &params); err != nil || params.Handle == "" || params.Context.Token == "" {
			cancel()
			return badRoute("worker: invalid author call")
		}
		s.mu.Lock()
		registration := s.author.handlers[params.Handle]
		s.mu.Unlock()
		if registration == nil {
			cancel()
			return badRoute("worker: unknown author registration")
		}
		s.launch(frame.ID, ctx, cancel, func(ctx context.Context) (json.RawMessage, error) {
			if registration.kind != "observer" {
				ctx = context.WithValue(ctx, authorHostCallKey{}, struct{}{})
			}
			call := extension.AuthorContext{
				ExtensionID: params.Context.ExtensionID, Settings: bytes.Clone(params.Context.Settings),
				RequestID: params.Context.RequestID,
				Session:   params.Context.Session, State: bytes.Clone(params.Context.State),
				Call: s.callFunc(ctx, params.Context.Token),
			}
			switch registration.kind {
			case "admission":
				handler := registration.value.(extension.AdmissionHook)
				if params.Method != "admission" {
					return nil, badRoute("worker: invalid admission hook method")
				}
				input, err := DecodeAuthorAdmission(json.RawMessage(params.Arguments))
				if err != nil {
					return nil, err
				}
				var promptMu sync.Mutex
				var promptErr error
				read := func(method string, value any) error {
					result, err := call.Call(ctx, extension.ContractPrompt, method, json.RawMessage(`{}`))
					if err == nil && result.Failure != nil {
						err = errors.New(result.Failure.Message)
					}
					if err != nil {
						return err
					}
					return json.Unmarshal(result.Value, value)
				}
				record := func(err error) {
					if err != nil {
						promptMu.Lock()
						promptErr = errors.Join(promptErr, err)
						promptMu.Unlock()
					}
				}
				input.Prompt = extension.PromptAccess{
					Default: func() (string, error) {
						var value string
						err := read("default", &value)
						return value, err
					},
					Sections: func() []prompt.Section {
						var value []prompt.Section
						record(read("sections", &value))
						return value
					},
					Rules: func() (string, []prompt.Section, error) {
						var value struct {
							Text     string           `json:"text"`
							Sections []prompt.Section `json:"sections"`
						}
						err := read("rules", &value)
						return value.Text, value.Sections, err
					},
					Environment: func() string {
						var value string
						record(read("environment", &value))
						return value
					},
				}
				result, err := handler(ctx, call, input)
				promptMu.Lock()
				err = errors.Join(err, promptErr)
				promptMu.Unlock()
				if err != nil {
					return nil, err
				}
				return EncodeAuthorAdmissionResult(result)
			case "authorization":
				handler := registration.value.(extension.AuthorizationHook)
				if params.Method != "authorization" {
					return nil, badRoute("worker: invalid authorization hook method")
				}
				input, err := DecodeAuthorAuthorization(json.RawMessage(params.Arguments))
				if err != nil {
					return nil, err
				}
				result, err := handler(ctx, call, input)
				if err != nil {
					return nil, err
				}
				return EncodeAuthorRefusal(result)
			case "request":
				handler := registration.value.(extension.AuthorRequestHook)
				if params.Method != "request" {
					return nil, badRoute("worker: invalid request hook method")
				}
				input, err := DecodeAuthorRequest(json.RawMessage(params.Arguments))
				if err != nil {
					return nil, err
				}
				result, err := handler(ctx, call, input)
				if err != nil {
					return nil, err
				}
				return EncodeRequestHookResult(result)
			case "result":
				handler := registration.value.(extension.ResultHook)
				if params.Method != "result" {
					return nil, badRoute("worker: invalid result hook method")
				}
				input, err := DecodeAfterToolHookParams(json.RawMessage(params.Arguments))
				if err != nil {
					return nil, err
				}
				result, err := handler(ctx, call, input)
				if err != nil {
					return nil, err
				}
				return EncodeAfterToolHookResult(result)
			case "arguments":
				handler := registration.value.(extension.ArgumentsHook)
				if params.Method != "arguments" {
					return nil, badRoute("worker: invalid arguments hook method")
				}
				input, err := DecodeBeforeToolHookParams(json.RawMessage(params.Arguments))
				if err != nil {
					return nil, err
				}
				result, err := handler(ctx, call, input)
				if err != nil {
					return nil, err
				}
				return EncodeAuthorArgumentsResult(result)
			case "strategy":
				handler := registration.value.(harness.ContextStrategy)
				var result harness.PlanOutput
				var err error
				switch params.Method {
				case "plan":
					var input harness.PlanInput
					input, err = DecodePlanParams(json.RawMessage(params.Arguments))
					if err == nil {
						result, err = handler.Plan(input)
					}
				case "consume":
					var input harness.ConsumeInput
					input, err = DecodeConsumeParams(json.RawMessage(params.Arguments))
					if err == nil {
						result, err = handler.Consume(input)
					}
				default:
					return nil, badRoute("worker: invalid strategy method")
				}
				if err != nil {
					return nil, err
				}
				return EncodePlanResult(result)
			case "tool":
				handler := registration.value.(extension.RegisteredTool)
				switch params.Method {
				case "normalize", "prepare":
					input, err := DecodeBeforeToolHookParams(json.RawMessage(params.Arguments))
					if err != nil {
						return nil, badRoute("worker: invalid tool input: %v", err)
					}
					if params.Method == "normalize" {
						result, err := handler.Normalize(call, input)
						if err != nil {
							return nil, err
						}
						return EncodeArguments(result)
					}
					return EncodeAuthorPlan(handler.Prepare(ctx, call, input))
				case "execute":
					plan, err := DecodeAuthorPlan(json.RawMessage(params.Arguments))
					if err != nil {
						return nil, badRoute("worker: invalid tool plan: %v", err)
					}
					return EncodeExecuteResult(handler.Execute(ctx, call, plan))
				}
			case "cleanup":
				handler := registration.value.(extension.CleanupHook)
				var session harness.SessionIdentity
				if err := json.Unmarshal(params.Arguments, &session); err != nil || params.Method != "cleanup" {
					return nil, badRoute("worker: invalid cleanup call")
				}
				return json.RawMessage("null"), handler(ctx, call, session)
			case "observer":
				handler := registration.value.(extension.Observer)
				if params.Method != "observe" {
					return nil, badRoute("worker: invalid observer method")
				}
				fact, err := DecodeAuthorFact(json.RawMessage(params.Arguments))
				if err != nil {
					return nil, err
				}
				if err := handler(ctx, fact); err != nil {
					return nil, err
				}
				return json.RawMessage("null"), nil
			case "operation":
				handler := registration.value.(extension.Operation)
				if handler.Method.Name != params.Method || handler.Handler == nil {
					return nil, badRoute("worker: invalid operation method")
				}
				result, err := handler.Handler(ctx, call, json.RawMessage(params.Arguments))
				if err != nil {
					return nil, err
				}
				return EncodeServiceResult(result)
			case "service":
				handler := registration.value.(extension.Service)
				for _, method := range handler.Methods {
					if method.Method.Name == params.Method && method.Handler != nil {
						result, err := method.Handler(ctx, call, json.RawMessage(params.Arguments))
						if err != nil {
							return nil, err
						}
						return EncodeServiceResult(result)
					}
				}
			}
			return nil, badRoute("worker: invalid author method")
		})
		return nil
	}
	if frame.Method != MethodActivate || s.author.started {
		cancel()
		return badRoute("worker: unexpected author request %q", frame.Method)
	}
	var params AuthorActivateParams
	if err := json.Unmarshal(frame.Params, &params); err != nil || params.Token == "" {
		cancel()
		return badRoute("worker: invalid activation params")
	}
	if err := extension.ValidateManifestSection(s.author.extension.Manifest, json.RawMessage(params.Section)); err != nil {
		cancel()
		s.respond(frame.ID, nil, err)
		return nil
	}
	s.author.started = true
	s.launch(frame.ID, ctx, cancel, func(ctx context.Context) (json.RawMessage, error) {
		api := &authorAPI{server: s, section: bytes.Clone(params.Section), token: params.Token, entry: ctx}
		closeFn, err := s.author.extension.Activate(ctx, api)
		s.author.close = closeFn
		return json.RawMessage("null"), err
	})
	return nil
}

type authorAPI struct {
	server  *server
	section json.RawMessage
	token   string
	entry   context.Context
}

func (a *authorAPI) Settings() json.RawMessage { return bytes.Clone(a.section) }
func (a *authorAPI) Call(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	return a.server.hostcall(ctx, a.entry, a.token, service, method, args)
}

type authorRegistration struct {
	server *server
	handle string
	kind   string
	value  any
}

type AuthorRegisterParams struct {
	Kind     string    `json:"kind"`
	Handle   string    `json:"handle"`
	Spec     JSONBytes `json:"spec"`
	Replaces string    `json:"replaces,omitempty"`
}

func (a *authorAPI) request(method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	s := a.server
	id := s.nextRequestID()
	result := make(chan Frame, 1)
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return extension.ErrClosed
	}
	s.author.pending[id] = result
	s.mu.Unlock()
	if err := s.write(Frame{Kind: KindRequest, ID: id, Method: method, Params: raw}); err != nil {
		s.mu.Lock()
		delete(s.author.pending, id)
		s.mu.Unlock()
		return err
	}
	select {
	case reply := <-result:
		if reply.Error != nil {
			return reply.Error
		}
		if string(reply.Result) != "null" {
			return errors.New("worker: expected null registration response")
		}
		return nil
	case <-s.done:
		return extension.ErrClosed
	}
}

func (a *authorAPI) register(kind string, spec any, value any, replace extension.Registration) (extension.Registration, error) {
	var previous string
	if replace != nil {
		r, ok := replace.(*authorRegistration)
		if !ok || r == nil || r.server != a.server {
			return nil, errors.New("worker: invalid registration handle")
		}
		previous = r.handle
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	s := a.server
	r := &authorRegistration{server: s, handle: s.nextRequestID(), kind: kind, value: value}
	s.mu.Lock()
	s.author.handlers[r.handle] = r
	s.mu.Unlock()
	err = a.request("register", AuthorRegisterParams{Kind: kind, Handle: r.handle, Spec: raw, Replaces: previous})
	if err != nil {
		s.mu.Lock()
		delete(s.author.handlers, r.handle)
		s.mu.Unlock()
		return nil, err
	}
	return r, nil
}

func (a *authorAPI) Remove(handle extension.Registration) error {
	r, ok := handle.(*authorRegistration)
	if !ok || r == nil || r.server != a.server {
		return errors.New("worker: invalid registration handle")
	}
	return a.request("remove", struct {
		Handle string `json:"handle"`
	}{r.handle})
}
func (a *authorAPI) RegisterTool(def model.ToolDefinition, tool extension.RegisteredTool, replace extension.Registration) (extension.Registration, error) {
	if tool == nil {
		return nil, errors.New("missing tool implementation")
	}
	return a.register("tool", def, tool, replace)
}
func (a *authorAPI) RegisterStrategy(binding harness.StrategyBinding, replace extension.Registration) (extension.Registration, error) {
	if binding.Strategy == nil {
		return nil, errors.New("invalid strategy")
	}
	return a.register("strategy", struct {
		ID                    string `json:"id"`
		RequiresContextWindow bool   `json:"requires_context_window"`
	}{binding.ID, binding.RequiresContextWindow}, binding.Strategy, replace)
}
func (a *authorAPI) RegisterService(service extension.Service, replace extension.Registration) (extension.Registration, error) {
	methods := make([]extension.Method, len(service.Methods))
	for i := range service.Methods {
		if service.Methods[i].Handler == nil {
			return nil, errors.New("invalid service method")
		}
		methods[i] = service.Methods[i].Method
	}
	return a.register("service", struct {
		Name    string             `json:"name"`
		Methods []extension.Method `json:"methods"`
	}{service.Name, methods}, service, replace)
}
func (a *authorAPI) RegisterOperation(op extension.Operation, replace extension.Registration) (extension.Registration, error) {
	if op.Handler == nil {
		return nil, errors.New("invalid operation")
	}
	return a.register("operation", struct {
		Target extension.ExposedTargetKind `json:"target"`
		Method extension.Method            `json:"method"`
	}{op.Target, op.Method}, op, replace)
}
func (a *authorAPI) AttachAdmission(h extension.AdmissionHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing hook")
	}
	return a.register("admission", struct{}{}, h, r)
}
func (a *authorAPI) AttachRequest(mode extension.HookMode, h extension.AuthorRequestHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("invalid hook")
	}
	return a.register("request", mode, h, r)
}
func (a *authorAPI) AttachArguments(mode extension.HookMode, h extension.ArgumentsHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("invalid hook")
	}
	return a.register("arguments", mode, h, r)
}
func (a *authorAPI) AttachAuthorization(h extension.AuthorizationHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing hook")
	}
	return a.register("authorization", struct{}{}, h, r)
}
func (a *authorAPI) AttachResult(h extension.ResultHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing hook")
	}
	return a.register("result", struct{}{}, h, r)
}
func (a *authorAPI) RegisterCleanup(h extension.CleanupHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing cleanup")
	}
	return a.register("cleanup", struct{}{}, h, r)
}

func (a *authorAPI) AttachObserver(observer extension.Observer, r extension.Registration) (extension.Registration, error) {
	if observer == nil {
		return nil, errors.New("missing observer")
	}
	return a.register("observer", struct{}{}, observer, r)
}

// Command issues one independent command through the host-bound activation.
// It carries no callback token: the host resolves the command against the
// activation of this worker process. A command issued with the context of a
// host call this worker is serving is refused locally; observer delivery
// carries no such mark.
func (a *authorAPI) Command(ctx context.Context, target extension.CommandTarget, method string, args json.RawMessage) (extension.Result, error) {
	if method == "" {
		return extension.Result{}, errors.New("worker: command requires a method")
	}
	if ctx != nil && ctx.Value(authorHostCallKey{}) != nil {
		return extension.Result{}, &Error{Class: ClassDenied, Message: "worker: command issued from a host callback"}
	}
	params, err := json.Marshal(AuthorCommandParams{Target: target, Method: method, Arguments: JSONBytes(args)})
	if err != nil {
		return extension.Result{}, err
	}
	return a.server.requestHost(ctx, nil, "command", MethodCommand, params)
}
