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
	"sync"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type nativeActivationSpec struct {
	extension        extension.Extension
	section          json.RawMessage
	bindings         map[string]declaredBinding
	approvedServices []string
	scope            CallerScope
	installed        bool
}

type nativeCandidate struct {
	owner             *configurationService
	activations       []*nativeActivation
	ready             map[*nativeActivation]bool
	inactive          map[string]error
	holds             int
	heldNames         []string
	enabled           map[string]bool
	attemptedFailures map[string]error
}

type nativeActivation struct {
	owner        *configurationService
	id           string
	version      string
	manifest     extension.Manifest
	section      json.RawMessage
	bindings     map[string]declaredBinding
	services     map[string]bool
	scope        CallerScope
	warnings     *resourceSet
	entries      []*nativeRegistration
	close        func(context.Context) error
	holds        int
	closed       bool
	observerWait []*observerDelivery
	construct    *nativeCandidate
	entry        nativeCallLifetime
	lost         error
	installed    bool
}

type nativeRegistration struct {
	id         string
	activation *nativeActivation
	kind       string
	name       string
	value      any
	current    bool
	holds      int
	release    func()
	observer   *observerDelivery
}

type nativeAPI struct{ activation *nativeActivation }

func (a *nativeActivation) unavailableLocked() error {
	if a.closed {
		return extension.ErrClosed
	}
	return a.lost
}

func (a *nativeActivation) unavailable() error {
	a.owner.nativeMu.Lock()
	defer a.owner.nativeMu.Unlock()
	return a.unavailableLocked()
}

// observerDeliveries returns the activation's current observer registrations.
func (a *nativeActivation) observerDeliveries() []*observerDelivery {
	if a == nil || a.owner == nil {
		return nil
	}
	a.owner.nativeMu.Lock()
	defer a.owner.nativeMu.Unlock()
	if a.closed || a.lost != nil {
		return nil
	}
	var deliveries []*observerDelivery
	for _, entry := range a.entries {
		if entry.kind == "observer" && entry.current && entry.observer != nil {
			deliveries = append(deliveries, entry.observer)
		}
	}
	return deliveries
}

// endRegistrationLocked stops one current registration: it removes the entry,
// marks it non-current, ends an owned observer delivery and returns the
// deferred release to run after nativeMu is released. It is the one end path
// for Remove, replacement, activation close/loss, owner close and observer
// self-end.
func endRegistrationLocked(a *nativeActivation, r *nativeRegistration) func() {
	if r == nil || !r.current {
		return nil
	}
	for i, entry := range a.entries {
		if entry == r {
			a.entries = append(a.entries[:i], a.entries[i+1:]...)
			break
		}
	}
	r.current = false
	if r.observer != nil {
		r.observer.close()
		r.observer = nil
	}
	return r.takeRelease()
}

// endObserversLocked ends every observer registration this activation owns and
// records their deliveries for the close-side in-flight wait. The caller holds
// nativeMu; releases run after it is released.
func (a *nativeActivation) endObserversLocked() []func() {
	var releases []func()
	for _, entry := range append([]*nativeRegistration(nil), a.entries...) {
		if entry.kind != "observer" {
			continue
		}
		if entry.observer != nil {
			a.observerWait = append(a.observerWait, entry.observer)
		}
		if release := endRegistrationLocked(a, entry); release != nil {
			releases = append(releases, release)
		}
	}
	return releases
}

// endObserver ends one delivery's registration through the one end path.
func (a *nativeActivation) endObserver(d *observerDelivery) {
	s := a.owner
	s.nativeMu.Lock()
	release := endRegistrationLocked(a, d.entry)
	s.nativeMu.Unlock()
	if release != nil {
		release()
	}
}

// waitObservers waits, bounded by nativeCloseDeadline, for every observer call
// ended by activation close or loss to return.
func (a *nativeActivation) waitObservers() {
	s := a.owner
	s.nativeMu.Lock()
	deliveries := a.observerWait
	a.observerWait = nil
	s.nativeMu.Unlock()
	if len(deliveries) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), nativeCloseDeadline)
	defer cancel()
	for _, delivery := range deliveries {
		delivery.wait(ctx)
	}
}

func (a *nativeActivation) markLost(cause error) {
	s := a.owner
	s.nativeMu.Lock()
	if a.lost != nil || a.closed {
		s.nativeMu.Unlock()
		return
	}
	a.lost = cause
	releases := a.endObserversLocked()
	current := s.nativeCurrent
	if current != nil {
		for _, member := range current.activations {
			if member == a && current.ready[a] {
				if s.nativeFailures == nil {
					s.nativeFailures = make(map[string]error)
				}
				s.nativeFailures[a.id] = cause
				current.inactive[a.id] = cause
				break
			}
		}
	}
	s.nativeMu.Unlock()
	for _, release := range releases {
		release()
	}
}

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

type nativeAdmissionCapture struct {
	nativeCaptureHold
	activations []*nativeActivation
}

type nativeCaptureHold struct {
	candidate *nativeCandidate
	released  bool
	entries   []*nativeRegistration
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
	entry.holds++
	return &nativeToolBinding{nativeCaptureHold: nativeCaptureHold{candidate: s.candidate, entries: []*nativeRegistration{entry}}, entry: entry}
}

func (h *nativeCaptureHold) release() error {
	owner := h.candidate.owner
	owner.nativeMu.Lock()
	if h.released {
		owner.nativeMu.Unlock()
		return nil
	}
	h.released = true
	var releases []func()
	for _, entry := range h.entries {
		entry.holds--
		if release := entry.takeRelease(); release != nil {
			releases = append(releases, release)
		}
	}
	owner.nativeMu.Unlock()
	for _, release := range releases {
		release()
	}
	return h.candidate.release()
}

// takeRelease is called with the registration owner's mutex held.
func (r *nativeRegistration) takeRelease() func() {
	if r.current || r.holds != 0 {
		return nil
	}
	release := r.release
	r.release = nil
	return release
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
	c := &nativeCandidate{owner: s, ready: make(map[*nativeActivation]bool), inactive: make(map[string]error), holds: 1, enabled: make(map[string]bool), attemptedFailures: make(map[string]error)}
	seenNames := make(map[string]bool)
	for _, spec := range specs {
		for _, binding := range spec.bindings {
			if binding.name != "" && !seenNames[binding.name] {
				seenNames[binding.name] = true
				c.heldNames = append(c.heldNames, binding.name)
			}
		}
	}
	s.holdCredentialNames(c.heldNames)
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
		c.enabled[spec.extension.Manifest.ID] = true
	}
	reused := make(map[*nativeActivation]bool)
	entries := make(map[*nativeActivation]extension.Extension)
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, c.release())
		}
		id := spec.extension.Manifest.ID
		s.nativeMu.Lock()
		failure := s.nativeFailures[id]
		s.nativeMu.Unlock()
		if failure != nil {
			c.inactive[id] = failure
			continue
		}
		if counts[id] > 1 {
			c.inactive[id] = fmt.Errorf("duplicate extension ID %q", id)
			continue
		}
		manifest := spec.extension.Manifest
		services := make(map[string]bool)
		for _, name := range manifest.Services {
			if spec.approvedServices == nil || slices.Contains(spec.approvedServices, name) {
				services[name] = true
			}
		}
		scope := spec.scope
		if scope.Kind == "" {
			scope = harnessScope
		}
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
				if a != nil && previous.ready[a] && !a.closed && a.lost == nil && a.id == id && a.version == manifest.Version &&
					bytes.Equal(a.section, spec.section) && maps.Equal(a.bindings, spec.bindings) && maps.Equal(a.services, services) {
					activation = a
					break
				}
			}
		}
		if activation == nil {
			manifest.SettingsSchema = bytes.Clone(manifest.SettingsSchema)
			manifest.Services = slices.Clone(manifest.Services)
			manifest.Secrets = slices.Clone(manifest.Secrets)
			activation = &nativeActivation{owner: s, id: id, version: manifest.Version, manifest: manifest, section: bytes.Clone(spec.section), bindings: maps.Clone(spec.bindings), services: services, scope: scope, warnings: &resourceSet{owner: s}, construct: c, installed: spec.installed}
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
		releases := activation.endObserversLocked()
		if err != nil {
			c.attemptedFailures[activation.id] = err
		}
		s.nativeMu.Unlock()
		for _, release := range releases {
			release()
		}
		activation.close = closeFn
		err = errors.Join(err, closeNativeActivation(activation))
		if ctx.Err() != nil {
			return nil, errors.Join(ctx.Err(), err, c.release())
		}
		c.inactive[activation.id] = err
	}
	return c, nil
}

func (c *nativeCandidate) preparePublishLocked() {
	if c.owner.nativeFailures == nil {
		c.owner.nativeFailures = make(map[string]error)
	}
	for id := range c.owner.nativeFailures {
		if !c.enabled[id] {
			delete(c.owner.nativeFailures, id)
		}
	}
	for id, err := range c.attemptedFailures {
		if err != nil {
			c.owner.nativeFailures[id] = err
		}
	}
	for _, activation := range c.activations {
		if activation != nil && activation.lost != nil {
			c.owner.nativeFailures[activation.id] = activation.lost
		}
	}
	for id, err := range c.owner.nativeFailures {
		if c.enabled[id] {
			c.inactive[id] = err
		}
	}
}

func (c *nativeCandidate) publishLocked() []Event {
	c.owner.nativeCurrent = c
	changed := false
	for _, activation := range c.activations {
		if activation != nil && c.ready[activation] && activation.lost == nil {
			changed = activation.warnings.commitWarnings() || changed
		}
	}
	if changed {
		return []Event{warningChangedEvent(c.owner.instance, runtimeEventScope(), c.owner.warnings.storeRevision())}
	}
	return nil
}

func closeNativeActivation(a *nativeActivation) error {
	if a.entry.cancel != nil {
		a.entry.end()
	}
	a.waitObservers()
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
	var releases []func()
	for i := len(c.activations) - 1; i >= 0; i-- {
		a := c.activations[i]
		if a == nil {
			continue
		}
		a.holds--
		if a.holds == 0 {
			a.closed = true
			releases = append(releases, a.endObserversLocked()...)
			closing = append(closing, a)
		}
	}
	s.nativeMu.Unlock()
	for _, release := range releases {
		release()
	}
	var err error
	for _, a := range closing {
		err = errors.Join(err, closeNativeActivation(a))
	}
	s.releaseCredentialNames(c.heldNames)
	if err != nil {
		s.cleanupMu.Lock()
		s.cleanupErr = errors.Join(s.cleanupErr, err)
		s.cleanupMu.Unlock()
	}
	return err
}

func (c *nativeCandidate) capture() *nativeAdmissionCapture {
	s := c.owner
	s.nativeMu.Lock()
	defer s.nativeMu.Unlock()
	return c.captureLocked()
}

func (c *nativeCandidate) captureLocked() *nativeAdmissionCapture {
	s := c.owner
	if c.holds == 0 || s.nativeCurrent != c {
		return nil
	}
	c.holds++
	capture := &nativeAdmissionCapture{nativeCaptureHold: nativeCaptureHold{candidate: c}}
	for _, a := range c.activations {
		if a != nil && c.ready[a] && a.lost == nil && s.nativeFailures[a.id] == nil {
			capture.activations = append(capture.activations, a)
		}
	}
	return capture
}

func (c *nativeAdmissionCapture) stage() (*nativeSnapshot, error) {
	owner := c.candidate.owner
	owner.nativeMu.Lock()
	defer owner.nativeMu.Unlock()
	if c.released {
		return nil, extension.ErrClosed
	}
	for _, a := range c.activations {
		if a.lost != nil {
			return nil, a.lost
		}
	}
	return c.candidate.snapshotLocked(c.activations), nil
}

func (c *nativeCandidate) snapshotLocked(activations []*nativeActivation) *nativeSnapshot {
	c.holds++
	out := &nativeSnapshot{nativeCaptureHold: nativeCaptureHold{candidate: c}, tools: make(map[string]*nativeRegistration), strategies: make(map[string]*nativeRegistration), services: make(map[string]*nativeRegistration), operations: make(map[nativeOperationKey]*nativeRegistration), hooks: make(map[string][]*nativeRegistration)}
	for _, a := range activations {
		for _, r := range a.entries {
			if r.kind == "observer" {
				// Observer delivery reads the activation's current registrations
				// at fact enqueue time; an admission snapshot never captures them.
				continue
			}
			r.holds++
			out.nativeCaptureHold.entries = append(out.nativeCaptureHold.entries, r)
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
	var release func()
	defer func() {
		a.owner.nativeMu.Unlock()
		if release != nil {
			release()
		}
	}()
	if err := a.unavailableLocked(); err != nil {
		return err
	}
	r, ok := handle.(*nativeRegistration)
	if !ok || r == nil || r.activation != a {
		return errors.New("invalid registration handle")
	}
	release = endRegistrationLocked(a, r)
	return nil
}

func (api nativeAPI) register(kind, name string, value any, replace extension.Registration) (extension.Registration, error) {
	return api.registerWith(kind, name, value, replace, nil)
}

// registerWith is the one registration body. attach, when non-nil, runs under
// the same owner hold that publishes the registration, so a dependent value
// such as an observer delivery cannot outlive a closing activation.
func (api nativeAPI) registerWith(kind, name string, value any, replace extension.Registration, attach func(*nativeRegistration)) (extension.Registration, error) {
	id, err := newInstanceID()
	if err != nil {
		return nil, err
	}
	a := api.activation
	s := a.owner
	s.nativeMu.Lock()
	var release func()
	defer func() {
		s.nativeMu.Unlock()
		if release != nil {
			release()
		}
	}()
	if err := a.unavailableLocked(); err != nil {
		return nil, err
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
	r := &nativeRegistration{id: id, activation: a, kind: kind, name: name, value: value, current: true}
	if old != nil {
		release = endRegistrationLocked(a, old)
	}
	a.entries = append(a.entries, r)
	if attach != nil {
		attach(r)
	}
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
	if _, meltaService := extension.HostService(service.Name); meltaService || service.Name == extension.ContractPrompt {
		return nil, fmt.Errorf("service %q already registered", service.Name)
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

// AttachObserver attaches one committed-fact observer to this activation. The
// observer receives no AuthorContext and no Call, so observation grants no
// command authority. Delivery is the Melta's bounded per-registration queue.
func (api nativeAPI) AttachObserver(observer extension.Observer, r extension.Registration) (extension.Registration, error) {
	if observer == nil {
		return nil, errors.New("missing observer")
	}
	a := api.activation
	return api.registerWith("observer", "", observer, r, func(entry *nativeRegistration) {
		if a.owner.obs != nil {
			entry.observer = a.owner.obs.attachObserver(a, observer, entry)
		}
	})
}

// CommandTargetScope maps one independent command target onto the Caller scope
// union its containment check uses.
func commandTargetScope(target extension.CommandTarget) CallerScope {
	switch target.Kind {
	case extension.ExposedTargetHarness:
		return CallerScope{Kind: CallerScopeHarness}
	case extension.ExposedTargetWorkspace:
		return CallerScope{Kind: CallerScopeWorkspace, Workspace: target.Workspace}
	case extension.ExposedTargetSession:
		return CallerScope{Kind: CallerScopeSession, SessionID: target.SessionID}
	}
	return CallerScope{}
}

// Command issues one independent command as this activation. A context Melta
// handed to a host callback of any extension is refused first, before
// publication, service, Session or scope checks and without taking the
// activation lock. Otherwise the command runs through the ordinary fixed
// producer with a fresh extension-origin Caller bounded to the target.
func (api nativeAPI) Command(ctx context.Context, target extension.CommandTarget, method string, args json.RawMessage) (extension.Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if callbackContext(ctx) {
		return extension.Result{}, fmt.Errorf("command issued from a host callback: %w", ErrDenied)
	}
	a := api.activation
	s := a.owner
	if s.composition == nil || s.composition.hosts == nil || s.composition.hosts.owner == nil {
		return extension.Result{}, extension.ErrClosed
	}
	targetScope := commandTargetScope(target)
	// A Session's Workspace is immutable: resolve it before the activation
	// guard so no Harness lock is taken under nativeMu.
	scope := a.scope
	var sessionWorkspace string
	var scopeErr error
	if target.Kind == extension.ExposedTargetSession && scope.Kind == CallerScopeWorkspace {
		sessionWorkspace, scopeErr = resolveSessionWorkspace(ctx, s.composition.hosts.harness, targetScope)
	}
	s.nativeMu.Lock()
	if current := s.nativeCurrent; current == nil || !current.ready[a] || a.closed || a.lost != nil {
		s.nativeMu.Unlock()
		return extension.Result{}, extension.ErrClosed
	}
	if !a.services[extension.ContractCommands] {
		s.nativeMu.Unlock()
		return extension.Result{}, ErrDenied
	}
	if scopeErr != nil {
		s.nativeMu.Unlock()
		return extension.Result{}, scopeErr
	}
	if err := authorizeResolvedScope(scope, targetScope, sessionWorkspace); err != nil {
		s.nativeMu.Unlock()
		return extension.Result{}, err
	}
	extensionID := a.id
	s.nativeMu.Unlock()

	arguments := append(json.RawMessage(nil), args...)
	if _, err := acceptHostMethod(extension.ContractCommands, method, arguments, false); err != nil {
		return extension.Result{}, err
	}
	handler, ok := s.composition.commandsHandler(method)
	if !ok {
		return extension.Result{}, fmt.Errorf("command method %q: %w", method, harness.ErrNotFound)
	}
	// The fixed bridge mints the fresh extension-origin Caller bounded to the
	// target scope exactly as the callback hostcall bridge does.
	callCtx := context.WithValue(ctx, grantContextKey{}, nativeGrant{commands: true, commandScope: targetScope})
	return handler(callCtx, hostCall{ExtensionID: extensionID, SessionID: targetScope.SessionID, Workspace: targetScope.Workspace}, arguments)
}

func (api nativeAPI) Call(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	a := api.activation
	s := a.owner
	s.nativeMu.Lock()
	c := a.construct
	unavailable := a.unavailableLocked()
	s.nativeMu.Unlock()
	if unavailable != nil {
		return extension.Result{}, unavailable
	}
	if c == nil {
		return extension.Result{}, extension.ErrClosed
	}
	callCtx, finish, err := a.entry.admit(ctx)
	if err != nil {
		return extension.Result{}, err
	}
	defer finish()
	scope := &nativeCallScope{candidate: c, activation: a, host: s.composition}
	if resulting, handled, err := scope.callHostService(callCtx, service, method, args); handled {
		return resulting, err
	}
	return c.callServiceScoped(callCtx, service, method, args, &nativeCallScope{candidate: c, activation: a})
}

type nativeCallScope struct {
	nativeCallLifetime
	candidate        *nativeCandidate
	activation       *nativeActivation
	pure             bool
	workspace        *nativeWorkspace
	workspaceBind    bool
	permissions      []harness.PermissionRequest
	session          harness.SessionIdentity
	requestID        string
	states           map[string]harness.SessionStateValue
	host             *composition
	lease            *artifactCallLease
	operationSecrets map[string]map[string]string
	commandScope     CallerScope
	receipt          *nativeReceipt
	start            *nativeSessionStart
}

type nativeSessionStart struct {
	mu          sync.Mutex
	consumed    bool
	extensionID string
	plan        extension.SessionStart
}

type nativeReceipt struct {
	mu        sync.Mutex
	committed bool
	result    *extension.Result
	partial   json.RawMessage
}

func (r *nativeReceipt) mark() {
	if r != nil {
		r.mu.Lock()
		r.committed = true
		r.mu.Unlock()
	}
}

func (r *nativeReceipt) command(result extension.Result, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		owned := ownResult(result)
		r.result = &owned
		r.partial = nil
		r.committed = true
		return
	}
	var partial *extension.CallError
	if errors.As(err, &partial) && len(partial.Partial) != 0 {
		r.result = nil
		r.partial = bytes.Clone(partial.Partial)
		r.committed = true
	}
}

func (r *nativeReceipt) observed() (*extension.Result, json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.result != nil {
		result := ownResult(*r.result)
		return &result, bytes.Clone(r.partial)
	}
	return nil, bytes.Clone(r.partial)
}

func (r *nativeReceipt) hasCommit() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.committed
}

func (scope *nativeCallScope) call(ctx context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
	callCtx, finish, err := scope.nativeCallLifetime.admit(ctx)
	if err != nil {
		return extension.Result{}, err
	}
	defer finish()
	if scope.activation != nil {
		if err := scope.activation.unavailable(); err != nil {
			return extension.Result{}, err
		}
	}
	if result, handled, err := scope.callHostService(callCtx, service, method, args); handled {
		return result, err
	}
	if service == extension.ContractSession {
		if scope.activation == nil || !scope.activation.services[service] || scope.session.SessionID == "" {
			return extension.Result{}, ErrDenied
		}
		if scope.host == nil || scope.host.hosts == nil || scope.host.hosts.background == nil || scope.host.hosts.background.h == nil {
			return extension.Result{}, extension.ErrClosed
		}
		if method == "start" && (scope.pure || scope.workspaceBind || scope.start == nil) {
			return extension.Result{}, ErrDenied
		}
		args, err = acceptHostMethod(service, method, args, scope.pure)
		if err != nil {
			return extension.Result{}, err
		}
		h := scope.host.hosts.background.h
		switch method {
		case "members":
			members, err := h.SessionMembers(callCtx, scope.session.SessionID)
			return commandResult(members, err)
		case "start":
			start := scope.start
			start.mu.Lock()
			if start.consumed {
				start.mu.Unlock()
				return extension.Result{}, fmt.Errorf("session start already consumed: %w", harness.ErrConflict)
			}
			start.consumed = true
			start.mu.Unlock()
			operationID, err := mintInstance()
			if err != nil {
				return extension.Result{}, err
			}
			var states map[string]harness.SessionStateValue
			if start.plan.InitialState != nil {
				states = map[string]harness.SessionStateValue{start.extensionID: bytes.Clone(start.plan.InitialState)}
			}
			launched, err := h.LaunchChildSession(callCtx, harness.LaunchChildRequest{
				ParentSessionID: scope.session.SessionID, ExtensionID: start.extensionID,
				OperationID: operationID, Inputs: start.plan.Inputs, ExtensionState: states,
				Content: start.plan.Input, Report: start.plan.Report, OutputLimit: start.plan.OutputLimit,
			})
			value, marshalErr := json.Marshal(struct {
				SessionID string `json:"session_id"`
			}{launched.ChildSessionID})
			if marshalErr != nil {
				return extension.Result{}, marshalErr
			}
			if err != nil {
				if launched.ChildSessionID != "" {
					return extension.Result{}, &extension.CallError{Err: err, Partial: value}
				}
				return extension.Result{}, err
			}
			return extension.Result{Value: value}, nil
		}
	}
	if service == extension.ContractState {
		if scope.activation == nil || !scope.activation.services[service] || scope.pure || scope.session.SessionID == "" {
			return extension.Result{}, ErrDenied
		}
		if scope.host == nil || scope.host.hosts == nil || scope.host.hosts.background == nil || scope.host.hosts.background.h == nil {
			return extension.Result{}, extension.ErrClosed
		}
		args, err = acceptHostMethod(service, method, args, false)
		if err != nil {
			return extension.Result{}, err
		}
		h := scope.host.hosts.background.h
		var input struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			return extension.Result{}, fmt.Errorf("state value: %v: %w", err, harness.ErrInvalid)
		}
		if _, err := h.SetSessionExtensionState(callCtx, scope.session.SessionID, scope.activation.id, harness.SessionStateValue(input.Value)); err != nil {
			return extension.Result{}, err
		}
		scope.receipt.mark()
		return extension.Result{Value: json.RawMessage("null")}, nil
	}
	if service == "melta.workspace" {
		if scope.activation == nil || !scope.activation.services[service] || scope.workspace == nil {
			return extension.Result{}, ErrDenied
		}
		args, err = acceptHostMethod(service, method, args, scope.pure && !scope.workspaceBind)
		if err != nil {
			return extension.Result{}, err
		}
		if method == "bind" {
			if !scope.workspaceBind {
				return extension.Result{}, ErrDenied
			}
			return scope.workspace.bind(args)
		}
		if scope.pure || scope.workspaceBind {
			return extension.Result{}, ErrDenied
		}
		return scope.workspace.execute(callCtx, method, args, scope.permissions)
	}
	if service == "melta.settings" {
		if scope.activation == nil || !scope.activation.services[service] || scope.pure || scope.commandScope.Kind != CallerScopeHarness {
			return extension.Result{}, ErrDenied
		}
		args, err = acceptHostMethod(service, method, args, false)
		if err != nil {
			return extension.Result{}, err
		}
		result, err := scope.updateSettings(callCtx, args)
		if err == nil {
			scope.receipt.mark()
		}
		return result, err
	}
	if service == extension.ContractCommands {
		if scope.activation == nil || !scope.activation.services[service] || scope.pure || scope.commandScope.Kind == "" {
			return extension.Result{}, ErrDenied
		}
		if scope.host == nil {
			return extension.Result{}, extension.ErrClosed
		}
		args, err = acceptHostMethod(service, method, args, false)
		if err != nil {
			return extension.Result{}, err
		}
		handler, ok := scope.host.commandsHandler(method)
		if !ok {
			return extension.Result{}, fmt.Errorf("command method %q: %w", method, harness.ErrNotFound)
		}
		callCtx = context.WithValue(callCtx, grantContextKey{}, nativeGrant{commands: true, commandScope: scope.commandScope})
		result, err := handler(callCtx, hostCall{ExtensionID: scope.activation.id, SessionID: scope.session.SessionID, Workspace: scope.session.Workspace}, args)
		scope.receipt.command(result, err)
		return result, err
	}
	return scope.candidate.callServiceScoped(callCtx, service, method, args, scope)
}

func (c *nativeCandidate) callServiceScoped(ctx context.Context, service, method string, args json.RawMessage, parent *nativeCallScope) (extension.Result, error) {
	s := c.owner
	s.nativeMu.Lock()
	var target *nativeActivation
	var binding nativeServiceMethod
	var registration *nativeRegistration
	var unavailable error
	for _, other := range c.activations {
		if other == nil || !c.ready[other] {
			continue
		}
		for _, r := range other.entries {
			if r.kind == "service" && r.name == service {
				target = other
				binding = r.value.(registeredService).methods[method]
				registration = r
			}
		}
	}
	if target != nil && (target.closed || target.lost != nil) {
		unavailable = target.lost
		if unavailable == nil {
			unavailable = extension.ErrClosed
		}
	} else if target != nil && binding.contract != nil {
		registration.holds++
	}
	s.nativeMu.Unlock()
	if target == nil {
		return extension.Result{}, &worker.Error{Class: worker.ClassUnavailable, Message: "service unavailable"}
	}
	if unavailable != nil {
		return extension.Result{}, unavailable
	}
	if binding.contract == nil {
		return extension.Result{}, fmt.Errorf("service method %q: %w", method, harness.ErrNotFound)
	}
	defer func() {
		s.nativeMu.Lock()
		registration.holds--
		release := registration.takeRelease()
		s.nativeMu.Unlock()
		if release != nil {
			release()
		}
	}()
	args, err := acceptNativeMethod(binding.contract, args, parent.pure)
	if err != nil {
		return extension.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return extension.Result{}, err
	}
	childCtx, cancel := context.WithCancel(ctx)
	scope := callbackScope(&nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: childCtx, cancel: cancel}, candidate: c, activation: target, pure: parent.pure || binding.contract.Mode == extension.ModePure, workspace: parent.workspace, workspaceBind: parent.workspaceBind, permissions: parent.permissions, session: parent.session, states: parent.states, host: parent.host, lease: parent.lease, operationSecrets: parent.operationSecrets, commandScope: parent.commandScope, receipt: parent.receipt, start: parent.start, requestID: parent.requestID})
	return executeNativeProvider(scope.ctx, extension.AuthorContext{ExtensionID: target.id, Settings: bytes.Clone(target.section), Session: scope.session, State: bytes.Clone(scope.states[target.id]), Call: scope.call, RequestID: scope.requestID}, binding.contract, func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
		defer scope.end()
		return binding.handler(ctx, call, args)
	}, "service", service, args)
}
