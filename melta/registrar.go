package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type nativeActivationSpec struct {
	extension extension.Extension
	section   json.RawMessage
	bindings  map[string]declaredBinding
	installed bool
}

type nativeCandidate struct {
	owner       *configurationService
	activations []*nativeActivation
	ready       map[*nativeActivation]bool
	inactive    map[string]error
	holds       int
}

type nativeActivation struct {
	owner     *configurationService
	id        string
	version   string
	section   json.RawMessage
	bindings  map[string]declaredBinding
	entries   []*nativeRegistration
	close     func(context.Context) error
	holds     int
	closed    bool
	construct *nativeCandidate
	entry     nativeCallLifetime
}

type nativeRegistration struct {
	activation *nativeActivation
	kind       string
	name       string
	value      any
	current    bool
}

type nativeAPI struct{ activation *nativeActivation }

func (api nativeAPI) Settings() json.RawMessage {
	return bytes.Clone(api.activation.section)
}

type nativeServiceMethod struct {
	contract *extension.CompiledMethod
	handler  extension.AuthorHandler
}

type registeredService struct {
	methods map[string]nativeServiceMethod
}

type nativeTool struct {
	definition model.ToolDefinition
	tool       extension.RegisteredTool
}

type nativeHook struct {
	mode    extension.HookMode
	handler any
}

type nativeSnapshot struct {
	nativeCaptureHold
	tools      map[string]*nativeRegistration
	strategies map[string]*nativeRegistration
	services   map[string]*nativeRegistration
	operations map[nativeOperationKey]*nativeRegistration
	hooks      map[string][]*nativeRegistration
}

type nativeCaptureHold struct {
	candidate *nativeCandidate
	released  bool
}

type nativeOperationKey struct {
	extension string
	method    string
}

type nativeToolBinding struct {
	nativeCaptureHold
	entry *nativeRegistration
}

func (s *nativeSnapshot) bindTool(name string) *nativeToolBinding {
	owner := s.candidate.owner
	owner.nativeMu.Lock()
	defer owner.nativeMu.Unlock()
	if s.released {
		return nil
	}
	entry := s.tools[name]
	if entry == nil {
		return nil
	}
	s.candidate.holds++
	return &nativeToolBinding{nativeCaptureHold: nativeCaptureHold{candidate: s.candidate}, entry: entry}
}

func (h *nativeCaptureHold) release() error {
	owner := h.candidate.owner
	owner.nativeMu.Lock()
	if h.released {
		owner.nativeMu.Unlock()
		return nil
	}
	h.released = true
	owner.nativeMu.Unlock()
	return h.candidate.release()
}

// prepareNativeCandidate holds exactly the activations selected by this
// candidate. Entrypoints execute without the owner's registration mutex.
func (s *configurationService) prepareNativeCandidate(ctx context.Context, specs []nativeActivationSpec) (*nativeCandidate, error) {
	specs = slices.Clone(specs)
	slices.SortStableFunc(specs, func(a, b nativeActivationSpec) int {
		if a.installed != b.installed {
			if a.installed {
				return 1
			}
			return -1
		}
		if a.installed {
			return strings.Compare(a.extension.Manifest.ID, b.extension.Manifest.ID)
		}
		return 0
	})
	c := &nativeCandidate{owner: s, ready: make(map[*nativeActivation]bool), inactive: make(map[string]error), holds: 1}
	s.nativeMu.Lock()
	if s.nativeCandidates == nil {
		s.nativeCandidates = make(map[*nativeCandidate]bool)
	}
	previous := s.nativeCurrent
	s.nativeCandidates[c] = true
	s.nativeMu.Unlock()

	counts := make(map[string]int)
	for _, spec := range specs {
		counts[spec.extension.Manifest.ID]++
	}
	reused := make(map[*nativeActivation]bool)
	entries := make(map[*nativeActivation]extension.Extension)
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, c.release())
		}
		id := spec.extension.Manifest.ID
		if counts[id] > 1 {
			c.inactive[id] = fmt.Errorf("duplicate extension ID %q", id)
			continue
		}
		manifest := spec.extension.Manifest
		err := extension.ValidateManifestSection(manifest, spec.section)
		if err == nil && spec.extension.Activate == nil {
			err = errors.New("missing extension entry point")
		}
		if err != nil {
			c.inactive[id] = err
			continue
		}
		s.nativeMu.Lock()
		var activation *nativeActivation
		if previous != nil {
			for _, a := range previous.activations {
				if a != nil && previous.ready[a] && !a.closed && a.id == id && a.version == manifest.Version &&
					bytes.Equal(a.section, spec.section) && maps.Equal(a.bindings, spec.bindings) {
					activation = a
					break
				}
			}
		}
		if activation == nil {
			activation = &nativeActivation{owner: s, id: id, version: manifest.Version, section: bytes.Clone(spec.section), bindings: maps.Clone(spec.bindings), construct: c}
			activation.entry.ctx, activation.entry.cancel = context.WithCancel(ctx)
		}
		activation.holds++
		c.activations = append(c.activations, activation)
		reused[activation] = activation.construct != c
		entries[activation] = spec.extension
		s.nativeMu.Unlock()
	}
	for i, activation := range c.activations {
		if activation == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, c.release())
		}
		if reused[activation] {
			s.nativeMu.Lock()
			c.ready[activation] = true
			s.nativeMu.Unlock()
			continue
		}
		closeFn, err := entries[activation].Activate(ctx, nativeAPI{activation})
		s.nativeMu.Lock()
		activation.construct = nil
		s.nativeMu.Unlock()
		activation.entry.end()
		s.nativeMu.Lock()
		if err == nil && ctx.Err() == nil {
			activation.close = closeFn
			c.ready[activation] = true
			s.nativeMu.Unlock()
			continue
		}
		c.activations[i] = nil
		activation.holds--
		activation.closed = true
		s.nativeMu.Unlock()
		activation.close = closeFn
		err = errors.Join(err, closeNativeActivation(activation))
		if ctx.Err() != nil {
			return nil, errors.Join(ctx.Err(), err, c.release())
		}
		c.inactive[activation.id] = err
	}
	return c, nil
}

func (c *nativeCandidate) publish() error {
	c.owner.nativeMu.Lock()
	defer c.owner.nativeMu.Unlock()
	if c.holds == 0 {
		return extension.ErrClosed
	}
	c.owner.nativeCurrent = c
	return nil
}

func closeNativeActivation(a *nativeActivation) error {
	if a.entry.cancel != nil {
		a.entry.end()
	}
	if a.close == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), nativeCloseDeadline)
	defer cancel()
	return a.close(ctx)
}

func (c *nativeCandidate) release() error {
	s := c.owner
	s.nativeMu.Lock()
	c.holds--
	if c.holds != 0 {
		s.nativeMu.Unlock()
		return nil
	}
	delete(s.nativeCandidates, c)
	if s.nativeCurrent == c {
		s.nativeCurrent = nil
	}
	var closing []*nativeActivation
	for i := len(c.activations) - 1; i >= 0; i-- {
		a := c.activations[i]
		if a == nil {
			continue
		}
		a.holds--
		if a.holds == 0 {
			a.closed = true
			closing = append(closing, a)
		}
	}
	s.nativeMu.Unlock()
	var err error
	for _, a := range closing {
		err = errors.Join(err, closeNativeActivation(a))
	}
	if err != nil {
		s.cleanupMu.Lock()
		s.cleanupErr = errors.Join(s.cleanupErr, err)
		s.cleanupMu.Unlock()
	}
	return err
}

func (c *nativeCandidate) snapshot() *nativeSnapshot {
	s := c.owner
	s.nativeMu.Lock()
	defer s.nativeMu.Unlock()
	if c.holds == 0 {
		return nil
	}
	c.holds++
	out := &nativeSnapshot{nativeCaptureHold: nativeCaptureHold{candidate: c}, tools: make(map[string]*nativeRegistration), strategies: make(map[string]*nativeRegistration), services: make(map[string]*nativeRegistration), operations: make(map[nativeOperationKey]*nativeRegistration), hooks: make(map[string][]*nativeRegistration)}
	for _, a := range c.activations {
		if a == nil || !c.ready[a] {
			continue
		}
		for _, r := range a.entries {
			switch r.kind {
			case "tool":
				out.tools[r.name] = r
			case "strategy":
				out.strategies[r.name] = r
			case "service":
				out.services[r.name] = r
			case "operation":
				out.operations[nativeOperationKey{a.id, r.name}] = r
			default:
				out.hooks[r.kind] = append(out.hooks[r.kind], r)
			}
		}
	}
	return out
}

func (api nativeAPI) Remove(handle extension.Registration) error {
	a := api.activation
	a.owner.nativeMu.Lock()
	defer a.owner.nativeMu.Unlock()
	if a.closed {
		return extension.ErrClosed
	}
	r, ok := handle.(*nativeRegistration)
	if !ok || r == nil || r.activation != a {
		return errors.New("invalid registration handle")
	}
	if !r.current {
		return nil
	}
	for i, entry := range a.entries {
		if entry == r {
			a.entries = append(a.entries[:i], a.entries[i+1:]...)
			r.current = false
			break
		}
	}
	return nil
}

func (api nativeAPI) register(kind, name string, value any, replace extension.Registration) (extension.Registration, error) {
	a := api.activation
	s := a.owner
	s.nativeMu.Lock()
	defer s.nativeMu.Unlock()
	if a.closed {
		return nil, extension.ErrClosed
	}
	var old *nativeRegistration
	if replace != nil {
		var ok bool
		old, ok = replace.(*nativeRegistration)
		if !ok || old == nil || old.activation != a || !old.current {
			return nil, errors.New("invalid replacement handle")
		}
	}
	if kind == "tool" {
		for _, standard := range standardToolDeclarations() {
			if standard.id == name {
				return nil, fmt.Errorf("standard tool %q is reserved", name)
			}
		}
	}
	if kind == "tool" || kind == "strategy" || kind == "service" || kind == "operation" {
		for candidate := range s.nativeCandidates {
			member := false
			for _, item := range candidate.activations {
				if item == a {
					member = true
					break
				}
			}
			if !member {
				continue
			}
			for _, item := range candidate.activations {
				if item == nil {
					continue
				}
				for _, entry := range item.entries {
					if entry == old || entry.kind != kind || entry.name != name {
						continue
					}
					if kind != "operation" || item.id == a.id {
						return nil, fmt.Errorf("%s %q already registered", kind, name)
					}
				}
			}
		}
	}
	r := &nativeRegistration{activation: a, kind: kind, name: name, value: value, current: true}
	if old != nil {
		for i, entry := range a.entries {
			if entry == old {
				a.entries = append(a.entries[:i], a.entries[i+1:]...)
				old.current = false
				break
			}
		}
	}
	a.entries = append(a.entries, r)
	return r, nil
}

func (api nativeAPI) RegisterTool(def model.ToolDefinition, tool extension.RegisteredTool, replace extension.Registration) (extension.Registration, error) {
	def, err := model.NewToolDefinition(def)
	if err != nil {
		return nil, err
	}
	if tool == nil {
		return nil, errors.New("missing tool implementation")
	}
	return api.register("tool", def.Name, nativeTool{def, tool}, replace)
}

func (api nativeAPI) RegisterStrategy(binding harness.StrategyBinding, replace extension.Registration) (extension.Registration, error) {
	if binding.ID == "" || binding.Strategy == nil {
		return nil, errors.New("invalid strategy")
	}
	return api.register("strategy", binding.ID, binding, replace)
}

func (api nativeAPI) RegisterService(service extension.Service, replace extension.Registration) (extension.Registration, error) {
	if service.Name == "" || len(service.Methods) == 0 {
		return nil, errors.New("invalid service")
	}
	compiled := registeredService{methods: make(map[string]nativeServiceMethod, len(service.Methods))}
	for _, method := range service.Methods {
		if method.Handler == nil || compiled.methods[method.Method.Name].contract != nil {
			return nil, errors.New("invalid service method")
		}
		contract, err := extension.CompileMethod(method.Method)
		if err != nil {
			return nil, err
		}
		compiled.methods[method.Method.Name] = nativeServiceMethod{contract, method.Handler}
	}
	return api.register("service", service.Name, compiled, replace)
}

func (api nativeAPI) RegisterOperation(op extension.Operation, replace extension.Registration) (extension.Registration, error) {
	if op.Handler == nil ||
		(op.Target != extension.ExposedTargetHarness && op.Target != extension.ExposedTargetWorkspace && op.Target != extension.ExposedTargetSession) {
		return nil, errors.New("invalid operation")
	}
	contract, err := extension.CompileMethod(op.Method)
	if err != nil {
		return nil, err
	}
	return api.register("operation", op.Method.Name, struct {
		target extension.ExposedTargetKind
		method nativeServiceMethod
	}{op.Target, nativeServiceMethod{contract, op.Handler}}, replace)
}

func (api nativeAPI) AttachAdmission(h extension.AdmissionHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing hook")
	}
	return api.register("admission", "", nativeHook{extension.HookModePure, h}, r)
}
func (api nativeAPI) AttachRequest(mode extension.HookMode, h extension.AuthorRequestHook, r extension.Registration) (extension.Registration, error) {
	if h == nil || (mode != extension.HookModePure && mode != extension.HookModeEffectful) {
		return nil, errors.New("invalid hook")
	}
	return api.register("request", "", nativeHook{mode, h}, r)
}
func (api nativeAPI) AttachArguments(mode extension.HookMode, h extension.ArgumentsHook, r extension.Registration) (extension.Registration, error) {
	if h == nil || (mode != extension.HookModePure && mode != extension.HookModeEffectful) {
		return nil, errors.New("invalid hook")
	}
	return api.register("arguments", "", nativeHook{mode, h}, r)
}
func (api nativeAPI) AttachAuthorization(h extension.AuthorizationHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing hook")
	}
	return api.register("authorization", "", nativeHook{extension.HookModePure, h}, r)
}
func (api nativeAPI) AttachResult(h extension.ResultHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing hook")
	}
	return api.register("result", "", nativeHook{extension.HookModePure, h}, r)
}
func (api nativeAPI) RegisterCleanup(h extension.CleanupHook, r extension.Registration) (extension.Registration, error) {
	if h == nil {
		return nil, errors.New("missing cleanup")
	}
	return api.register("cleanup", "", h, r)
}

func (api nativeAPI) Call(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	a := api.activation
	s := a.owner
	s.nativeMu.Lock()
	c := a.construct
	closed := a.closed
	s.nativeMu.Unlock()
	if closed || c == nil {
		return extension.Result{}, extension.ErrClosed
	}
	callCtx, finish, err := a.entry.admit(ctx)
	if err != nil {
		return extension.Result{}, err
	}
	defer finish()
	return c.callService(callCtx, service, method, args, false)
}

type nativeCallScope struct {
	nativeCallLifetime
	candidate *nativeCandidate
	pure      bool
}

func (scope *nativeCallScope) call(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	callCtx, finish, err := scope.nativeCallLifetime.admit(ctx)
	if err != nil {
		return extension.Result{}, err
	}
	defer finish()
	return scope.candidate.callService(callCtx, service, method, args, scope.pure)
}

func (c *nativeCandidate) callService(ctx context.Context, service, method string, args json.RawMessage, pure bool) (extension.Result, error) {
	s := c.owner
	s.nativeMu.Lock()
	var target *nativeActivation
	var binding nativeServiceMethod
	for _, other := range c.activations {
		if other == nil || !c.ready[other] {
			continue
		}
		for _, r := range other.entries {
			if r.kind == "service" && r.name == service {
				target = other
				binding = r.value.(registeredService).methods[method]
			}
		}
	}
	s.nativeMu.Unlock()
	if target == nil || binding.contract == nil {
		return extension.Result{}, errors.New("service unavailable")
	}
	args, err := acceptNativeMethod(binding.contract, args, pure)
	if err != nil {
		return extension.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return extension.Result{}, err
	}
	childCtx, cancel := context.WithCancel(ctx)
	scope := &nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: childCtx, cancel: cancel}, candidate: c, pure: pure || binding.contract.Mode == extension.ModePure}
	return executeNativeProvider(childCtx, extension.AuthorContext{ExtensionID: target.id, Settings: bytes.Clone(target.section), Call: scope.call}, binding.contract, func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
		defer scope.end()
		return binding.handler(ctx, call, args)
	}, "service", service, args)
}
