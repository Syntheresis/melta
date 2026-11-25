package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
)

type executableAuthor struct {
	manifest      extension.Manifest
	api           extension.API
	handshaken    bool
	registrations map[string]extension.Registration
	native        *nativeActivation
}

// executableAuthorExtension binds one process to one registrar activation.
func executableAuthorExtension(manifest extension.Manifest, entryPoint string, environment func(context.Context, []string) ([]string, error)) extension.Extension {
	return extension.Extension{Manifest: manifest, Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		a := newExecutableActivation(entryPoint, environment)
		a.author = &executableAuthor{manifest: manifest, api: api, registrations: make(map[string]extension.Registration)}
		var excluded []string
		if native, ok := api.(nativeAPI); ok {
			excluded = native.activation.owner.heldCredentialNames()
			a.author.native = native.activation
		}
		if err := a.start(ctx, excluded); err != nil {
			a.startTermination(err, false)
			<-a.terminated
			return nil, err
		}
		return func(context.Context) error {
			a.stop()
			return nil
		}, nil
	}}
}

func (a *executableActivation) authorHandshake(ctx context.Context) requestOutcome {
	hello := a.request(ctx, worker.MethodHello, func(string) (any, error) {
		return worker.HelloParams{APIVersion: a.author.manifest.APIVersion}, nil
	}, nil, func(raw json.RawMessage) (any, error) {
		var result worker.HelloResult
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, err
		}
		want, err := json.Marshal(a.author.manifest)
		if err != nil {
			return nil, err
		}
		got, err := json.Marshal(result.Manifest.Value())
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(got, want) {
			return nil, errors.New("worker manifest differs from selected manifest")
		}
		return nil, nil
	})
	if hello.err != nil || hello.failure != nil {
		return hello
	}
	a.mu.Lock()
	a.author.handshaken = true
	a.mu.Unlock()
	return a.request(ctx, worker.MethodActivate, func(token string) (any, error) {
		return worker.AuthorActivateParams{Section: worker.JSONBytes(a.author.api.Settings()), Token: token}, nil
	}, executableCall(a.author.api.Call), validateNull)
}

func authorPromptGrant(ctx context.Context, call extension.AuthorContext, access extension.PromptAccess) (executableCall, func()) {
	origin, cancel := context.WithCancel(ctx)
	lifetime := &nativeCallLifetime{ctx: origin, cancel: cancel}
	return func(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		scoped, finish, err := lifetime.admit(ctx)
		if err != nil {
			return extension.Result{}, err
		}
		defer finish()
		if service != extension.ContractPrompt {
			if call.Call == nil {
				return extension.Result{}, extension.ErrClosed
			}
			return call.Call(scoped, service, method, args)
		}
		if string(args) != "{}" {
			return extension.Result{}, harness.ErrInvalid
		}
		var value any
		switch method {
		case "default":
			if access.Default == nil {
				err = extension.ErrClosed
			} else {
				value, err = access.Default()
			}
		case "sections":
			if access.Sections == nil {
				err = extension.ErrClosed
			} else {
				value = access.Sections()
			}
		case "rules":
			if access.Rules == nil {
				err = extension.ErrClosed
			} else {
				var text string
				var sections []prompt.Section
				text, sections, err = access.Rules()
				value = struct {
					Text     string           `json:"text"`
					Sections []prompt.Section `json:"sections"`
				}{text, sections}
			}
		case "environment":
			if access.Environment == nil {
				err = extension.ErrClosed
			} else {
				value = access.Environment()
			}
		default:
			err = errors.New("worker: unknown prompt method")
		}
		if err != nil {
			return extension.Result{}, err
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return extension.Result{}, err
		}
		return extension.Result{Value: raw}, nil
	}, lifetime.end
}

func (a *executableActivation) authorRegistration(frame worker.Frame) error {
	if frame.ID == "" {
		return errors.New("worker: registration request without id")
	}
	a.mu.Lock()
	ready := a.author.handshaken
	a.mu.Unlock()
	if !ready {
		return errors.New("worker: registration before handshake")
	}
	if frame.Method == "remove" {
		var params struct {
			Handle string `json:"handle"`
		}
		if err := json.Unmarshal(frame.Params, &params); err != nil || params.Handle == "" {
			return errors.New("worker: invalid remove")
		}
		a.mu.Lock()
		handle := a.author.registrations[params.Handle]
		a.mu.Unlock()
		if handle == nil {
			a.respondError(frame.ID, &worker.Error{Class: worker.ClassInvalid, Message: "invalid registration handle"})
			return nil
		}
		if err := a.author.api.Remove(handle); err != nil {
			a.respondError(frame.ID, executableFailure(err))
		} else {
			a.respondFrame(worker.Frame{Kind: worker.KindResponse, ID: frame.ID, Result: json.RawMessage("null")})
		}
		return nil
	}
	var params worker.AuthorRegisterParams
	if err := json.Unmarshal(frame.Params, &params); err != nil || params.Kind == "" || params.Handle == "" {
		return errors.New("worker: invalid registration")
	}
	a.mu.Lock()
	_, duplicate := a.author.registrations[params.Handle]
	replacement := a.author.registrations[params.Replaces]
	a.mu.Unlock()
	if duplicate || (params.Replaces != "" && replacement == nil) {
		a.respondError(frame.ID, &worker.Error{Class: worker.ClassInvalid, Message: "invalid registration handle"})
		return nil
	}
	var handle extension.Registration
	var err error
	switch params.Kind {
	case "admission":
		handle, err = a.author.api.AttachAdmission(func(ctx context.Context, call extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
			args, err := worker.EncodeAuthorAdmission(input)
			if err != nil {
				return extension.AdmissionResult{}, err
			}
			grant, end := authorPromptGrant(ctx, call, input.Prompt)
			defer end()
			outcome := a.request(ctx, worker.MethodCall, func(issued string) (any, error) {
				return worker.AuthorCallParams{Handle: params.Handle, Method: "admission", Arguments: worker.JSONBytes(args), Context: worker.AuthorCallContext{
					Token: issued, RequestID: call.RequestID, ExtensionID: call.ExtensionID, Settings: worker.JSONBytes(call.Settings), Session: call.Session, State: worker.JSONBytes(call.State),
				}}, nil
			}, grant, typed(worker.DecodeAuthorAdmissionResult))
			value, err := decodeOutcome(outcome)
			if err != nil {
				return extension.AdmissionResult{}, err
			}
			return value.(extension.AdmissionResult), nil
		}, replacement)
	case "check":
		handle, err = a.author.api.AttachToolCheck(func(ctx context.Context, call extension.AuthorContext, input model.ToolCall) (*string, error) {
			args, err := worker.EncodeBeforeToolHookParams(input)
			if err != nil {
				return nil, err
			}
			value, err := a.authorCall(ctx, params.Handle, "check", call, args, typed(worker.DecodeAuthorRefusal))
			if err != nil {
				return nil, err
			}
			return value.(*string), nil
		}, replacement)
	case "request":
		handle, err = a.author.api.AttachRequest(func(ctx context.Context, call extension.AuthorContext, input extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			args, err := worker.EncodeAuthorRequest(input)
			if err != nil {
				return harness.RequestConsequence{}, err
			}
			value, err := a.authorCall(ctx, params.Handle, "request", call, args, typed(worker.DecodeRequestHookResult))
			if err != nil {
				return harness.RequestConsequence{}, err
			}
			return value.(harness.RequestConsequence), nil
		}, replacement)
	case "result":
		handle, err = a.author.api.AttachResult(func(ctx context.Context, call extension.AuthorContext, input harness.ToolOutputInput) (string, error) {
			args, err := worker.EncodeAfterToolHookParams(input)
			if err != nil {
				return "", err
			}
			value, err := a.authorCall(ctx, params.Handle, "result", call, args, typed(worker.DecodeAfterToolHookResult))
			if err != nil {
				return "", err
			}
			return value.(string), nil
		}, replacement)
	case "arguments":
		handle, err = a.author.api.AttachArguments(func(ctx context.Context, call extension.AuthorContext, input model.ToolCall) (extension.ArgumentsResult, error) {
			args, err := worker.EncodeBeforeToolHookParams(input)
			if err != nil {
				return extension.ArgumentsResult{}, err
			}
			value, err := a.authorCall(ctx, params.Handle, "arguments", call, args, typed(worker.DecodeAuthorArgumentsResult))
			if err != nil {
				return extension.ArgumentsResult{}, err
			}
			return value.(extension.ArgumentsResult), nil
		}, replacement)
	case "strategy":
		var spec struct {
			ID                    string `json:"id"`
			RequiresContextWindow bool   `json:"requires_context_window"`
		}
		if err = json.Unmarshal(params.Spec, &spec); err == nil {
			handle, err = a.author.api.RegisterStrategy(harness.StrategyBinding{
				ID: spec.ID, RequiresContextWindow: spec.RequiresContextWindow,
				Strategy: &executableAuthorStrategy{activation: a, handle: params.Handle},
			}, replacement)
		}
	case "tool":
		var def model.ToolDefinition
		if err = json.Unmarshal(params.Spec, &def); err == nil {
			handle, err = (nativeAPI{activation: a.author.native}).registerTool(def, func(ctx context.Context, call extension.AuthorContext, input model.ToolCall) harness.ToolOutcome {
				return a.authorToolRun(ctx, params.Handle, call, input)
			}, replacement)
		}
	case "service":
		var spec struct {
			Name    string             `json:"name"`
			Methods []extension.Method `json:"methods"`
		}
		if err = json.Unmarshal(params.Spec, &spec); err == nil {
			service := extension.Service{Name: spec.Name}
			for _, method := range spec.Methods {
				name := method.Name
				service.Methods = append(service.Methods, extension.RegisteredMethod{Method: method, Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return a.authorServiceCall(ctx, params.Handle, name, call, args)
				}})
			}
			handle, err = a.author.api.RegisterService(service, replacement)
		}
	case "operation":
		var spec struct {
			Target extension.ExposedTargetKind `json:"target"`
			Method extension.Method            `json:"method"`
		}
		if err = json.Unmarshal(params.Spec, &spec); err == nil {
			handle, err = a.author.api.RegisterOperation(extension.Operation{
				Target: spec.Target, Method: spec.Method,
				Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return a.authorServiceCall(ctx, params.Handle, spec.Method.Name, call, args)
				},
			}, replacement)
		}
	case "cleanup":
		handle, err = a.author.api.RegisterCleanup(func(ctx context.Context, call extension.AuthorContext, session harness.SessionIdentity) error {
			args, err := json.Marshal(session)
			if err != nil {
				return err
			}
			_, err = a.authorCall(ctx, params.Handle, "cleanup", call, args, validateNull)
			return err
		}, replacement)
	case "observer":
		handle, err = a.author.api.AttachObserver(func(ctx context.Context, fact extension.Fact) error {
			args, err := json.Marshal(fact)
			if err != nil {
				return err
			}
			_, err = a.authorCall(ctx, params.Handle, "observe", extension.AuthorContext{}, args, validateNull)
			return err
		}, replacement)
	default:
		err = fmt.Errorf("worker: unsupported registration kind %q", params.Kind)
	}
	if err != nil {
		a.respondError(frame.ID, executableFailure(err))
		return nil
	}
	if registration, ok := handle.(*nativeRegistration); ok {
		owner := registration.activation.owner
		owner.nativeMu.Lock()
		registration.release = func() {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), nativeCloseDeadline)
				defer cancel()
				a.request(ctx, "release", func(string) (any, error) {
					return struct {
						Handle string `json:"handle"`
					}{params.Handle}, nil
				}, nil, validateNull)
			}()
		}
		owner.nativeMu.Unlock()
	}
	a.mu.Lock()
	a.author.registrations[params.Handle] = handle
	a.mu.Unlock()
	a.respondFrame(worker.Frame{Kind: worker.KindResponse, ID: frame.ID, Result: json.RawMessage("null")})
	return nil
}

func (a *executableActivation) authorToolRun(ctx context.Context, handle string, call extension.AuthorContext, input model.ToolCall) harness.ToolOutcome {
	args, err := worker.EncodeBeforeToolHookParams(input)
	if err != nil {
		return harness.ToolOutcome{Result: bindingToolResult(input.ID, err, false), Evidence: harness.ExecutionNotStarted}
	}
	outcome := a.authorRequest(ctx, handle, "run", call, args, typed(worker.DecodeExecuteResult))
	value, err := decodeOutcome(outcome)
	if err != nil {
		return harness.ToolOutcome{Result: bindingToolResult(input.ID, err, harness.EffectInterrupted(err)), Evidence: outcome.evidence}
	}
	return value.(harness.ToolOutcome)
}

type executableAuthorStrategy struct {
	activation *executableActivation
	handle     string
}

func (s *executableAuthorStrategy) Plan(input harness.PlanInput) (harness.PlanOutput, error) {
	args, err := worker.EncodePlanParams(input)
	if err != nil {
		return harness.PlanOutput{}, err
	}
	value, err := s.activation.authorCall(context.Background(), s.handle, "plan", extension.AuthorContext{}, args, typed(worker.DecodePlanResult))
	if err != nil {
		return harness.PlanOutput{}, err
	}
	return value.(harness.PlanOutput), nil
}

func (s *executableAuthorStrategy) Consume(input harness.ConsumeInput) (harness.PlanOutput, error) {
	args, err := worker.EncodeConsumeParams(input)
	if err != nil {
		return harness.PlanOutput{}, err
	}
	value, err := s.activation.authorCall(context.Background(), s.handle, "consume", extension.AuthorContext{}, args, typed(worker.DecodePlanResult))
	if err != nil {
		return harness.PlanOutput{}, err
	}
	return value.(harness.PlanOutput), nil
}

func (a *executableActivation) authorServiceCall(ctx context.Context, handle, method string, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
	value, err := a.authorCall(ctx, handle, method, call, args, typed(worker.DecodeServiceResult))
	if err != nil {
		return extension.Result{}, err
	}
	return value.(extension.Result), nil
}

func (a *executableActivation) authorCall(ctx context.Context, handle, method string, call extension.AuthorContext, args json.RawMessage, decode func(json.RawMessage) (any, error)) (any, error) {
	return decodeOutcome(a.authorRequest(ctx, handle, method, call, args, decode))
}

func (a *executableActivation) authorRequest(ctx context.Context, handle, method string, call extension.AuthorContext, args json.RawMessage, decode func(json.RawMessage) (any, error)) requestOutcome {
	grant := executableCall(call.Call)
	if grant == nil {
		grant = closedCall
	}
	return a.request(ctx, worker.MethodCall, func(token string) (any, error) {
		return worker.AuthorCallParams{
			Handle: handle, Method: method, Arguments: worker.JSONBytes(args),
			Context: worker.AuthorCallContext{Token: token, RequestID: call.RequestID, ExtensionID: call.ExtensionID, Settings: worker.JSONBytes(call.Settings), Session: call.Session, State: worker.JSONBytes(call.State)},
		}, nil
	}, grant, decode)
}
