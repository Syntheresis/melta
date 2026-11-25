package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/catalog"
	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/pathutil"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
	"github.com/Syntheresis/melta/protocol"
)

// openInvocation turns one committed admission's execution-view selection and
// one Harness-issued Engine-call request into that invocation's opened
// callback contracts. It is the preparation's only associated continuation:
// there is no intermediate execution-plan type and no independently
// configured opener.
type openInvocation func(context.Context, harness.OperationAdmission, selection, harness.InvocationRequest) (harness.InvocationExecution, error)

func locationBinder(workspace string) func(string) (string, func() error, error) {
	return func(path string) (string, func() error, error) {
		resolved, err := pathutil.ResolveFilePathFrom(workspace, path)
		if err != nil {
			return "", nil, err
		}
		return resolved.CanonicalPath, func() error {
			current, err := pathutil.ResolveFilePathFrom(workspace, path)
			if err != nil {
				return err
			}
			if current.CanonicalPath != resolved.CanonicalPath {
				return fmt.Errorf("approved canonical location changed: %s to %s", resolved.CanonicalPath, current.CanonicalPath)
			}
			return nil
		}, nil
	}
}

// selection is one immutable preparation or execution input set: the composed
// candidate, the retained activation capture and Session state, the prompt
// provider, the tool catalog, and the captured Invocation.
type selection struct {
	candidate  extension.AdmissionCandidate
	native     *nativeAdmissionCapture
	states     map[string]harness.SessionStateValue
	prompt     *surfacePrompt
	catalog    []model.ToolDefinition
	defaultAll bool
	strategy   harness.StrategyCapture
	invocation Invocation

	// lease is the actual privately acquired execution artifact interval.
	// Every native callback running in this execution receives this same
	// borrowed pointer regardless of its implementation lifetime; nil outside
	// an admitted execution.
	lease *artifactCallLease

	// operationSecrets are the operation-lifetime declared credential values
	// captured at preparation and retained through the prepared opener. Every
	// tool and hook view of that execution carries them; no value reaches a
	// durable capture.
	operationSecrets map[string]map[string]string

	// credentials is the preparation's one captured credential observation
	// set, sampled with the same capture as the Invocation's snapshot: it
	// carries both the provider names the catalog references and every active
	// declaration's non-secret reference name. The concrete preparation
	// resolves its transport secrets from it, the operation fields derive
	// their values from it, and no value leaves the prepared opener or the
	// durable capture.
	credentials map[string]CredentialValue

	// warnings is the per-preparation private presentation collector: the
	// concrete preparation assigns the assembled prompt warnings into it and
	// the committed opener publishes them under the admitted Session identity
	// only after the opener succeeded, so a failed fork or admission
	// publishes nothing. It is never the Invocation or the durable capture.
	warnings *preparationWarnings
}

// preparationWarnings collects one preparation's presentation values.
type preparationWarnings struct {
	prompt []prompt.Warning
}

// publish replaces the owning Session's prompt group with the collected
// values through the passive publication adapter; a nil adapter drops the
// presentation. The committed opener calls it exactly once per successful
// execution, passing the publisher and the admitted Session identity at
// publication instead of storing a second pair.
func (w *preparationWarnings) publish(publisher *observationAdapter, sessionID string) {
	if w == nil {
		return
	}
	publisher.publishPrompt(sessionID, promptWarnings(sessionID, w.prompt))
}

// promptWarnings maps the assembled prompt diagnostics onto the store's
// typed session group.
func promptWarnings(sessionID string, warnings []prompt.Warning) []protocol.Warning {
	if len(warnings) == 0 {
		return nil
	}
	out := make([]protocol.Warning, 0, len(warnings))
	for _, warning := range warnings {
		id := sessionID
		out = append(out, protocol.Warning{Source: promptSource, Kind: warning.Kind, Message: warning.Message, SessionId: &id})
	}
	return out
}

// protocolWarnings maps one transport attempt's returned diagnostics onto the
// store's typed session group.
func protocolWarnings(sessionID string, warnings []model.ProtocolWarning) []protocol.Warning {
	if len(warnings) == 0 {
		return nil
	}
	out := make([]protocol.Warning, 0, len(warnings))
	for _, warning := range warnings {
		id := sessionID
		out = append(out, protocol.Warning{Source: protocolSource, Kind: warning.Kind, Message: warning.Message, SessionId: &id})
	}
	return out
}

// preparation supplies the existing Harness preparation contract for every
// admission producer: root, queued delivery and Fork all bind through the one
// closure returned by bind, with only resource lifetime and the selected
// invocation context added to what Harness already validates. The snapshot
// pointer loads once per call, so a reload overlapping preparation affects the
// next capture, not the in-flight one; the returned opener keeps the same
// immutable snapshot while borrowed bindings and the call context stay inside
// the preparation guard.
type preparation struct {
	config *configurationService

	// identity and locations are the host-supplied prompt and diagnostic
	// inputs: the application persona text and the explicit global rules and
	// wire-debug locations. Their zero values supply no identity, no global
	// rules and no wire diagnostics, never an implicit fallback.
	identity   string
	locations  Locations
	background *backgroundBridge
	passive    *observationAdapter

	// artifacts is the Melta's private per-Session artifact interval
	// registry. Every committed opener acquires its Session's token here
	// before any scope, plugin, model or tool work and releases it
	// last, after the scoped cleanup; nil leaves an isolated preparation
	// without artifact ownership.
	artifacts *artifactIntervals

	// subprocessEnv is the Melta's live subprocess-environment producer.
	// It is invoked at each process start, never at preparation; an error
	// fails the spawn before start.
	subprocessEnv func(context.Context) ([]string, error)
}

// newPreparation wires the binder to the published configuration, the
// selected locations, the host-supplied prompt identity, the background
// services bridge armed after harness.New returns, the Melta's passive
// publication adapter (nil in isolated preparation tests, dropping only
// passive presentation), the Melta's private artifact interval registry
// (nil without a Melta), and the Melta's live child-environment producer. One
// concrete preparation path serves every admission.
func newPreparation(config *configurationService, locations Locations, identity string, background *backgroundBridge, passive *observationAdapter, artifacts *artifactIntervals, subprocessEnv func(context.Context) ([]string, error)) *preparation {
	return &preparation{
		config:        config,
		identity:      identity,
		locations:     locations,
		background:    background,
		passive:       passive,
		artifacts:     artifacts,
		subprocessEnv: subprocessEnv,
	}
}

// bind returns the exact two-argument callback type required by
// harness.Dependencies.Prepare. Selected unknown or unavailable Agent types
// and models report harness.ErrInvalid at admission, never a failed
// configuration publication; preparation errors and cancellation abort
// admission with no partial consequence published.
func (p *preparation) bind() func(context.Context, harness.PreparationRequest) (harness.PreparedExecution, error) {
	return func(ctx context.Context, req harness.PreparationRequest) (prepared harness.PreparedExecution, resultErr error) {
		// Configuration and the credentials its catalog references are one
		// owner-coherent capture: the selected model, its transport secret,
		// and the revision all belong to the same ready pair.
		captured, err := p.config.capturePreparation(ctx)
		if err != nil {
			return harness.PreparedExecution{}, err
		}
		snapshot := captured.snapshot
		var releaseArtifacts func()
		if snapshot != nil {
			defer func() {
				if resultErr != nil {
					resultErr = errors.Join(resultErr, snapshot.resources.release())
					captured.releaseHeldNames()
				}
			}()
		}
		if snapshot == nil {
			return harness.PreparedExecution{}, fmt.Errorf("agent has no published configuration: %w", harness.ErrInvalid)
		}
		native := captured.native
		if native == nil {
			return harness.PreparedExecution{}, fmt.Errorf("extension registrations unavailable: %w", harness.ErrInvalid)
		}
		defer func() {
			if resultErr != nil {
				resultErr = errors.Join(resultErr, native.release())
			}
		}()
		stage, err := native.stage()
		if err != nil {
			return harness.PreparedExecution{}, err
		}
		defer stage.release()
		defaults, choices := p.config.defaults.Owned(), req.Session.Inputs.Owned()
		if choices.Instructions != nil {
			defaults.Instructions, defaults.Prompt = choices.Instructions, nil
		}
		if choices.Prompt != nil {
			defaults.Prompt, defaults.Instructions = choices.Prompt, nil
		}
		if choices.Tools != nil {
			defaults.Tools = choices.Tools
		}
		if choices.Strategy != nil {
			defaults.Strategy = choices.Strategy
		}
		invocation := Invocation{snapshot: snapshot}
		candidate := extension.AdmissionCandidate{Model: snapshot.baselineModel, Prompt: defaults.Prompt}
		if choices.Model != nil {
			candidate.Model = *choices.Model
		}
		if defaults.Instructions != nil {
			candidate.Instructions = *defaults.Instructions
		}
		candidate.Strategy.ID = p.config.strategy.ID
		if defaults.Strategy != nil && (defaults.Strategy.ID == p.config.strategy.ID || stage.strategies[defaults.Strategy.ID] != nil) {
			candidate.Strategy = *defaults.Strategy
		}
		catalog := make([]model.ToolDefinition, 0)
		known := make(map[string]bool)
		for _, item := range standardToolDeclarations() {
			catalog = append(catalog, item.definition)
			known[item.id] = true
		}
		for _, entry := range stage.entries {
			if entry.kind == "tool" {
				definition := entry.value.(nativeTool).definition
				catalog = append(catalog, definition)
				known[definition.Name] = true
			}
		}
		if defaults.Tools == nil {
			candidate.Tools = ownToolDefinitions(catalog)
		} else {
			candidate.Tools = make([]model.ToolDefinition, 0, len(defaults.Tools))
			for _, name := range defaults.Tools {
				for _, definition := range catalog {
					if definition.Name == name {
						candidate.Tools = append(candidate.Tools, definition)
						break
					}
				}
			}
		}
		initialNames := make([]string, len(candidate.Tools))
		for i, tool := range candidate.Tools {
			initialNames[i] = tool.Name
		}
		promptProvider := newSurfacePrompt(surfaceRequest{
			hostIdentity: p.identity, globalRulesDir: p.locations.GlobalRulesDir,
			workspace: req.Session.Identity.Workspace, sessionStart: req.Session.Identity.CreatedAt,
			promptBody: candidate.Instructions, shell: projectedShell(ctx, p.subprocessEnv),
		})
		var modelCatalog []model.ModelRef
		for _, ref := range snapshot.catalog.AllModels() {
			modelCatalog = append(modelCatalog, model.ModelRef{Provider: ref.Provider, Model: ref.Model})
		}
		admission := extension.AdmissionInput{
			Session: req.Session.Identity, Input: req.Content, Candidate: candidate,
			ModelCatalog: modelCatalog, ToolCatalog: catalog,
			Prompt: extension.PromptAccess{
				Default:  func() (string, error) { result, err := promptProvider.defaultPrompt(); return result.Prompt, err },
				Sections: promptProvider.sections, Rules: promptProvider.ruleSections, Environment: promptProvider.environment,
			},
		}
		operationSecrets := captured.operationSecrets()
		candidate, err = runRegisteredAdmission(ctx, stage, admission, req.Session.ExtensionState, snapshot.composition, operationSecrets, known, p.config.strategy.ID)
		if err != nil {
			return harness.PreparedExecution{}, err
		}
		strategy := harness.StrategyCapture{ID: candidate.Strategy.ID, Settings: bytes.Clone(candidate.Strategy.Settings)}
		if candidate.Strategy.ID == p.config.strategy.ID {
			strategy.RequiresContextWindow = p.config.strategy.RequiresContextWindow
		} else {
			strategy.RequiresContextWindow = stage.strategies[candidate.Strategy.ID].value.(harness.StrategyBinding).RequiresContextWindow
		}
		finalNames := make([]string, len(candidate.Tools))
		for i, tool := range candidate.Tools {
			finalNames[i] = tool.Name
		}
		sel := selection{candidate: candidate, native: native, states: req.Session.ExtensionState, prompt: promptProvider, catalog: catalog, defaultAll: defaults.Tools == nil && slices.Equal(initialNames, finalNames), strategy: strategy, invocation: invocation, credentials: captured.snapshot.credentials, operationSecrets: operationSecrets}
		entry, transport, err := p.bindModel(ctx, &sel)
		if err != nil {
			return harness.PreparedExecution{}, err
		}
		workspace := req.Session.Identity.Workspace
		runtimeScope, workspaces := snapshot.resources.runtime, snapshot.resources.workspaces
		workspaceInfo := ScopeInfo{Kind: ScopeWorkspace, DataDir: runtimeScope.info.DataDir, Workspace: workspace, Revision: snapshot.resources.revisionString()}
		workspaceScope, err := workspaces.get(ctx, workspaceInfo)
		if err != nil {
			return harness.PreparedExecution{}, err
		}

		callCtx, release, err := workspaceScope.enter(ctx)
		if err != nil {
			return harness.PreparedExecution{}, err
		}
		defer release() // the one preparation guard covers the concrete preparation and every pure hook

		// The per-preparation collector is allocated owned: only a successful
		// preparation — validation, hooks and cancellation checks all passed —
		// publishes its values under the admitted Session identity.
		sel.warnings = &preparationWarnings{}
		capture, opener, err := p.concretePrepare(callCtx, req, sel, entry, transport)
		if err != nil {
			return harness.PreparedExecution{}, err
		}
		// The collected presentation rides the prepared opener: only the
		// committed admission's opener publishes it, so a failed fork or
		// admission never creates warnings for an unpublished Session.
		warnings := sel.warnings
		prepared = harness.PreparedExecution{
			Capture: capture,
			Open: func(openCtx context.Context, admission harness.OperationAdmission) (harness.Execution, error) {
				return p.open(openCtx, admission, workspace, runtimeScope, workspaceScope, sel, snapshot, warnings, opener, &releaseArtifacts)
			},
		}
		p.passive.retainCapture(req.Session.Identity.SessionID, native)
		prepared.Release = func() error {
			p.passive.releaseCapture(req.Session.Identity.SessionID, native)
			err := errors.Join(native.release(), snapshot.resources.release())
			if releaseArtifacts != nil {
				releaseArtifacts()
			}
			captured.releaseHeldNames()
			return err
		}
		return prepared, nil
	}
}

// open turns the committed admission into the execution: it first acquires
// the Session's whole artifact interval — before any scope, plugin, model or
// tool work — then opens only the Operation scope and enters its
// guard. The returned Execution carries the invocation factory: every Engine
// call — the conversation run and each maintenance model request — opens its
// own Agent-tier scope over the same Operation, enters its guard, selects the
// Agent's capabilities over the four scopes, and hands the new selection with
// the actual committed admission and the Harness-issued request to this
// preparation's opener, so hooks' final prompt and tools can never be replaced
// by stale pre-hook values. The Operation guard covers opening and every
// nested invocation until the returned Close releases it and closes the
// Operation scope; each invocation's own Close releases its guard and closes
// its Agent scope before that; the prepared reservation releases the artifact
// token after the resource-set hold, and an opening failure releases it before returning. Only
// after the opening succeeds does the collected prompt presentation publish,
// so a failed fork or admission never creates warnings for an unpublished
// Session and a successful opener publishes before any progress.
func (p *preparation) open(ctx context.Context, admission harness.OperationAdmission, workspace string, runtimeScope, workspaceScope *scope, sel selection, snapshot *configuration, warnings *preparationWarnings, opener openInvocation, artifactRelease *func()) (harness.Execution, error) {
	releaseArtifacts, err := p.artifacts.acquireExecution(ctx, sessionArtifactDir(runtimeScope.info.DataDir, admission.SessionID))
	if err != nil {
		return harness.Execution{}, err
	}
	artifactLease := &artifactCallLease{
		intervals: p.artifacts,
		key:       sessionArtifactDir(runtimeScope.info.DataDir, admission.SessionID),
		borrowed:  true,
		release:   releaseArtifacts,
	}
	base := ScopeInfo{DataDir: runtimeScope.info.DataDir, Workspace: workspace, SessionID: admission.SessionID, OperationID: admission.OperationID, Revision: snapshot.resources.revisionString()}
	operationInfo := base
	operationInfo.Kind = ScopeOperation
	operation, err := snapshot.composition.openScope(ctx, operationInfo, []*scope{runtimeScope, workspaceScope})
	if err != nil {
		releaseArtifacts()
		return harness.Execution{}, err
	}
	_, releaseOperation, err := operation.enter(ctx)
	if err != nil {
		err = errors.Join(err, operation.close())
		releaseArtifacts() // the opened scope unwind finishes before the ownership releases
		return harness.Execution{}, err
	}
	agentInfo := base
	agentInfo.Kind = ScopeAgent
	openInvocation := func(ctx context.Context, request harness.InvocationRequest) (harness.InvocationExecution, error) {
		agentInfo.InvocationID = request.ID
		agentInfo.Purpose = string(request.Purpose)
		agentScope, err := snapshot.composition.openScope(ctx, agentInfo, []*scope{runtimeScope, workspaceScope, operation})
		if err != nil {
			return harness.InvocationExecution{}, err
		}
		unwind := func(cause error, release func()) (harness.InvocationExecution, error) {
			if release != nil {
				release()
			}
			return harness.InvocationExecution{}, errors.Join(cause, agentScope.close())
		}
		callCtx, releaseAgent, err := agentScope.enter(ctx)
		if err != nil {
			return unwind(err, nil)
		}
		// The invocation view carries the execution's artifact lease; the
		// registered tool and hook lookups come from the retained capture.
		invocationSelection := sel
		invocationSelection.lease = artifactLease
		invocation, err := opener(callCtx, admission, invocationSelection, request)
		if err != nil {
			return unwind(err, releaseAgent)
		}
		closeInvocation := invocation.Close
		invocation.Close = func() error {
			var closeErr error
			if closeInvocation != nil {
				closeErr = closeInvocation()
			}
			releaseAgent()
			return errors.Join(closeErr, agentScope.close())
		}
		return invocation, nil
	}
	// The committed opener owns the collected presentation: the successful
	// execution publishes its Session's prompt group before any model or tool
	// progress.
	warnings.publish(p.passive, admission.SessionID)
	*artifactRelease = releaseArtifacts
	lookup := func(_ context.Context, id string) (*harness.StrategyBinding, error) {
		if id == p.config.strategy.ID {
			return p.config.strategy, nil
		}
		stage, err := sel.native.stage()
		if err != nil {
			return nil, err
		}
		entry := stage.strategies[id]
		if entry == nil {
			_ = stage.release()
			return nil, fmt.Errorf("strategy %q unavailable: %w", id, extension.ErrClosed)
		}
		binding := entry.value.(harness.StrategyBinding)
		binding.Release = func() { _ = stage.release() }
		return &binding, nil
	}
	return harness.Execution{
		OpenInvocation:  openInvocation,
		ResolveStrategy: lookup,
		Close: func() error {
			releaseOperation()
			err := operation.close()
			return err
		},
	}, nil
}

func (p *preparation) bindModel(ctx context.Context, sel *selection) (*catalog.Model, preparedTransport, error) {
	snapshot := sel.invocation.snapshot
	chosen := sel.candidate.Model
	refs := []model.ModelRef{chosen}
	if !snapshot.baselineModel.IsZero() && snapshot.baselineModel != chosen {
		refs = append(refs, snapshot.baselineModel)
	}
	var lastErr error
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return nil, preparedTransport{}, err
		}
		if ref.IsZero() {
			lastErr = fmt.Errorf("no model configured: %w", harness.ErrInvalid)
			continue
		}
		provider, entry, err := snapshot.catalog.LookupOrIncomplete(catalog.ModelRef{Provider: ref.Provider, Model: ref.Model})
		if err != nil {
			lastErr = fmt.Errorf("agent model %s is not available in the catalog: %w: %w", ref.String(), err, harness.ErrInvalid)
			continue
		}
		if sel.strategy.RequiresContextWindow && entry.ContextWindow <= 0 {
			lastErr = fmt.Errorf("agent model %s is incomplete: %w: %w", ref.String(), catalog.ErrIncomplete, harness.ErrInvalid)
			continue
		}
		key := ""
		if env := provider.Transport.APIKeyEnv; env != "" {
			key = sel.credentials[env].Value
			if key == "" {
				lastErr = fmt.Errorf("%w: %s (for provider %q): %w", config.ErrMissingEnvVar, env, provider.ID, harness.ErrInvalid)
				continue
			}
		}
		transport, err := p.concreteTransport(provider, entry, ref, key)
		if err != nil {
			lastErr = err
			continue
		}
		sel.candidate.Model = ref
		return entry, transport, nil
	}
	return nil, preparedTransport{}, lastErr
}

// concretePrepare is the one production preparation. It consumes only the captured snapshot's inputs — the
// catalog entry, the once-resolved credential, and the composed prompt/tool
// surface — and returns the durable capture plus the invocation factory
// carrying the two constructed transports and the prepared workspace. Later
// configuration reloads and environment changes never reach a prepared
// execution, and no transport input, secret, or warning is stored beyond the
// durable capture fields.
func (p *preparation) concretePrepare(ctx context.Context, req harness.PreparationRequest, sel selection, entry *catalog.Model, transport preparedTransport) (harness.ExecutionCapture, openInvocation, error) {
	snapshot := sel.invocation.snapshot
	// The compact configuration is the top-level compaction section of the
	// same one-revision snapshot.
	compaction := snapshot.compactionValues
	// The effective compact model is one fallback chain ending at the
	// conversation model: the configured compaction model — when it names a
	// different model at all — must resolve in the catalog and build a
	// transport over a resolved credential; otherwise the conversation model
	// and its transport are kept. The recorded compact configuration is
	// always the final effective model's.
	compactRef := sel.candidate.Model
	compactTransport := transport
	compactCost := entry.Cost
	if !compaction.model.IsZero() && compaction.model != sel.candidate.Model {
		compactProvider, compactEntry, lookupErr := snapshot.catalog.Lookup(catalog.ModelRef{Provider: compaction.model.Provider, Model: compaction.model.Model})
		if lookupErr == nil {
			keyMissing := false
			compactKey := ""
			if env := compactProvider.Transport.APIKeyEnv; env != "" {
				compactKey = sel.credentials[env].Value
				keyMissing = compactKey == ""
			}
			if !keyMissing {
				built, buildErr := p.concreteTransport(compactProvider, compactEntry, compaction.model, compactKey)
				if buildErr == nil {
					compactRef = compaction.model
					compactTransport = built
					compactCost = compactEntry.Cost
				}
			}
		}
	}
	subprocessEnv := p.subprocessEnv
	if sel.prompt.input.Instructions != sel.candidate.Instructions {
		sel.prompt.input.Instructions = sel.candidate.Instructions
		sel.prompt.assembled = false
	}
	result, err := sel.prompt.resolve(sel.candidate.Prompt)
	if err != nil {
		return harness.ExecutionCapture{}, nil, err
	}
	catalog := ownToolDefinitions(sel.catalog)
	names := make([]string, len(sel.candidate.Tools))
	overridden := make(map[string]bool)
	for i, selected := range sel.candidate.Tools {
		names[i] = selected.Name
		for index := range catalog {
			if catalog[index].Name != selected.Name {
				continue
			}
			source := catalog[index]
			if !bytes.Equal(source.Parameters, selected.Parameters) || source.Description != selected.Description {
				overridden[selected.Name] = true
			}
			catalog[index] = selected
			break
		}
	}
	// The assembled prompt's diagnostics are this preparation's presentation
	// contribution, collected privately and published by the binder only
	// after every preparation step succeeded.
	if sel.warnings != nil {
		sel.warnings.prompt = result.Warnings
	}
	capture := harness.ExecutionCapture{
		ConfigurationRevision: sel.invocation.Revision(),
		Model:                 sel.candidate.Model,
		ContextWindow:         entry.ContextWindow,
		OutputReserve:         outputReserve(entry.MaxOutputTokens),
		SystemPrompt:          result.Prompt,
		Tools:                 ownToolDefinitions(catalog),
		InitialToolNames:      slices.Clone(names),
		Pricing:               capturedPricing(entry.Cost),
		Compact: harness.CompactCapture{
			Model:        compactRef,
			SystemPrompt: compaction.prompt,
			Pricing:      capturedPricing(compactCost),
		},
		Strategy: sel.strategy,
	}
	for _, activation := range sel.native.activations {
		capture.Implementations = append(capture.Implementations, harness.ImplementationCapture{
			ExtensionID: activation.id, Version: activation.version,
			Settings: bytes.Clone(activation.section), State: bytes.Clone(sel.states[activation.id]),
		})
	}
	workspace := req.Session.Identity.Workspace
	return capture, p.concreteOpener(transport, compactTransport, req.Session.Identity, workspace, catalog, names, overridden, subprocessEnv), nil
}

// projectedShell reads SHELL from the same child-environment projection the
// command path consumes. An absent producer, absent snapshot, absent value or
// empty value renders the existing "unknown".
func projectedShell(ctx context.Context, env func(context.Context) ([]string, error)) string {
	if env == nil {
		return "unknown"
	}
	snapshot, err := env(ctx)
	if err != nil || snapshot == nil {
		return "unknown"
	}
	for _, entry := range snapshot {
		name, value, ok := strings.Cut(entry, "=")
		if ok && name == "SHELL" {
			if value == "" {
				return "unknown"
			}
			return value
		}
	}
	return "unknown"
}

// outputReserve applies the retained reserve rule: the catalog entry's
// MaxOutputTokens when positive, else the fixed 131072 reserve.
func outputReserve(maxOutputTokens int) int {
	if maxOutputTokens > 0 {
		return maxOutputTokens
	}
	return 131072
}

// capturedPricing copies one catalog entry's per-million USD rates into
// owned capture pricing: an absent cost means unknown pricing, and a
// non-finite catalog rate becomes an absent rate, never a preparation
// failure.
func capturedPricing(cost *catalog.Cost) *harness.ModelPricing {
	if cost == nil {
		return nil
	}
	return &harness.ModelPricing{
		Input:     finiteRate(cost.Input),
		Output:    finiteRate(cost.Output),
		CacheRead: finiteRate(cost.CacheRead),
	}
}

func finiteRate(v *float64) *float64 {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	copied := *v
	return &copied
}

type preparedTransport struct {
	transport  *model.Transport
	parameters model.Extra
}

// concreteTransport constructs the fixed transport and its request-hook body defaults.
func (p *preparation) concreteTransport(provider *catalog.Provider, entry *catalog.Model, ref model.ModelRef, apiKey string) (preparedTransport, error) {
	resolved, err := p.resolveTransport(provider, entry, ref, apiKey)
	if err != nil {
		return preparedTransport{}, err
	}
	transport, err := model.NewTransport(resolved)
	if err != nil {
		return preparedTransport{}, fmt.Errorf("build model transport for %s: %w", ref.String(), err)
	}
	parameters := make(model.Extra)
	for _, layer := range []model.Extra{resolved.ProviderExtraBody, resolved.ModelExtraBody} {
		for key, value := range layer {
			if !strings.HasPrefix(key, "_melta_") {
				parameters[key] = model.CloneRaw(value)
			}
		}
	}
	return preparedTransport{transport: transport, parameters: parameters}, nil
}

// resolveTransport builds the transport's resolved input from the captured
// catalog provider/model entry and the resolved key, applying the retained
// conversion semantics exactly: the model entry's effective system role and
// streamed-usage flag as the catalog build already resolved them; sidecar
// extra-body layers re-encoded with raw numbers preserved; the same-provider
// source-family map from the provider's models; and the retained opt-in
// wire-debug directory. No transport option feeds this conversion; no timeout
// input or HTTP option exists.
func (p *preparation) resolveTransport(provider *catalog.Provider, entry *catalog.Model, ref model.ModelRef, apiKey string) (model.ResolvedTransport, error) {
	resolved := model.ResolvedTransport{
		Model:          ref,
		BaseURL:        provider.Transport.BaseURL,
		APIKey:         apiKey,
		Headers:        provider.Transport.Headers,
		WireSystemRole: string(entry.SystemRole), // the catalog build injects the effective role; Encode maps empty to "system" as backstop
		StreamedUsage:  entry.UsageInStream,
		WireDebugDir:   wireDebugDir(p.locations.WireDebugDir),
	}
	providerExtras, err := extraBody(provider.ExtraBody)
	if err != nil {
		return model.ResolvedTransport{}, fmt.Errorf("provider %s extra body: %w", provider.ID, err)
	}
	resolved.ProviderExtraBody = providerExtras
	modelExtras, err := extraBody(entry.ExtraBody)
	if err != nil {
		return model.ResolvedTransport{}, fmt.Errorf("model %s extra body: %w", entry.ID, err)
	}
	resolved.ModelExtraBody = modelExtras
	if meta := entry.ProtocolMetadata; meta != nil { // the built catalog's model metadata is the effective model-over-provider merge
		resolved.ProtocolFamily = meta.Family
		resolved.MustPreserve = meta.MustPreserve
		if len(meta.Drop) > 0 {
			resolved.Drop = make(map[string]bool, len(meta.Drop))
			for _, key := range meta.Drop {
				resolved.Drop[key] = true
			}
		}
	}
	resolved.SourceFamilies = make(map[model.ModelRef]string, len(provider.Models))
	for id, sibling := range provider.Models {
		if sibling == nil || sibling.ProtocolMetadata == nil || sibling.ProtocolMetadata.Family == "" {
			continue
		}
		resolved.SourceFamilies[model.ModelRef{Provider: provider.ID, Model: id}] = sibling.ProtocolMetadata.Family
	}
	return resolved, nil
}

// extraBody converts one catalog sidecar layer into the transport's extra
// layer by re-encoding each value verbatim: catalog numbers decode as
// json.Number, so raw number lexemes are preserved with no float coercion.
func extraBody(in map[string]any) (model.Extra, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(model.Extra, len(in))
	for key, value := range in {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("value of %q: %w", key, err)
		}
		out[key] = raw
	}
	return out, nil
}

// wireDebugDir applies the selected location: an empty location disables
// diagnostics; otherwise raw wire artifacts dump under it, created once per
// preparation; a failed creation disables diagnostics, never execution.
func wireDebugDir(location string) string {
	if location == "" {
		return ""
	}
	if err := os.MkdirAll(location, 0o700); err != nil {
		return ""
	}
	return location
}

// concreteOpener returns the production invocation factory: per Engine call
// it binds every advertised tool name from that invocation's view over all
// four scopes, binds the selected argument hooks, owns one ToolContext
// carrying the prepare-time Workspace, the calling Operation's admitted-input
// identity, the captured Invocation, the validated hard constraints and a
// never-armed background object for normalization, and answers with the
// invocation whose model callback makes exactly one physical attempt over the
// transport its request purpose selects — the conversation transport for a
// conversation request and the effective compact transport for a maintenance
// request, the same construction over the one shared transport when the
// compact model is the conversation model. Each tool dispatch copies that
// context with a fresh background object, prepares, freezes the plan's
// permissions and wraps its executor, so the plugin holds no background
// authority outside Execute. The ToolContext's environment callback is the
// preparation's fixed wrapper over the Melta's live producer: it filters the
// captured provider and declared credential names from every snapshot it
// returns, never rereads the selected execution policy, and keeps the nil
// producer and nil snapshot fail-closed shapes unchanged. The shared
// advertisement gate rejects unknown names before dispatch; retry
// classification and scope disposal belong to the shared machinery.
func (p *preparation) concreteOpener(transport, compactTransport preparedTransport, session harness.SessionIdentity, workspace string, catalog []model.ToolDefinition, visible []string, overridden map[string]bool, subprocessEnv func(context.Context) ([]string, error)) openInvocation {
	return func(_ context.Context, admission harness.OperationAdmission, sel selection, request harness.InvocationRequest) (harness.InvocationExecution, error) {
		sessionID := admission.AdmittedEntry.SessionID
		tc := ToolContext{
			Session:       session,
			Model:         sel.candidate.Model,
			Workspace:     workspace,
			OperationID:   admission.OperationID,
			InvocationID:  request.ID,
			Purpose:       request.Purpose,
			AdmittedEntry: admission.AdmittedEntry,
			Invocation:    sel.invocation,
			Background:    &boundBackground{bridge: p.background, sessionID: sessionID},
			SubprocessEnv: subprocessEnv,
		}
		attemptTransport := transport
		if request.Purpose == harness.InvocationPurposeMaintenance {
			attemptTransport = compactTransport
		}
		toolSettings, err := decodeToolSettings(sel.invocation.snapshot.tools)
		if err != nil {
			return harness.InvocationExecution{}, err
		}
		settings, err := json.Marshal(toolSettings)
		if err != nil {
			return harness.InvocationExecution{}, err
		}
		return harness.InvocationExecution{
			BindLocation:    locationBinder(workspace),
			ModelParameters: attemptTransport.parameters.Clone(),
			RequestStage: func() harness.RequestStage {
				current, err := sel.native.stage()
				if err != nil {
					return harness.RequestStage{Error: err}
				}
				var definitions []model.ToolDefinition
				present := make(map[string]bool)
				for _, admitted := range catalog {
					name := admitted.Name
					_, isStandard := sel.invocation.snapshot.composition.standardValues[name]
					entry := current.tools[name]
					if !isStandard && entry == nil {
						continue
					}
					if overridden[name] || isStandard {
						definitions = append(definitions, admitted)
					} else {
						definitions = append(definitions, entry.value.(nativeTool).definition)
					}
					present[name] = true
				}
				if sel.defaultAll {
					for _, entry := range current.entries {
						if entry.kind == "tool" && !present[entry.name] {
							definitions = append(definitions, entry.value.(nativeTool).definition)
							present[entry.name] = true
						}
					}
				}
				var names []string
				if !sel.defaultAll {
					names = make([]string, 0, len(visible))
					for _, name := range visible {
						if present[name] {
							names = append(names, name)
						}
					}
				}
				return harness.RequestStage{Catalog: definitions, InitialToolNames: names, Hooks: registeredRequestHooks(current, sel, session), Release: func() { _ = current.release() }}
			},
			Model: func(ctx context.Context, req model.Request, parameters model.Extra) (model.Stream, error) {
				stream, warnings, err := attemptTransport.transport.Stream(ctx, req, parameters)
				// The attempt's diagnostics are recorded under the admitted
				// Session identity even when the physical request failed after
				// encoding succeeded — a diagnostic is presentation only.
				p.passive.replaceProtocolWarnings(admission.SessionID, protocolWarnings(admission.SessionID, warnings))
				return stream, err
			},
			LookupTool: func(name string) (harness.ToolBinding, bool) {
				current, err := sel.native.stage()
				if err != nil {
					return harness.ToolBinding{Identity: harness.ToolImplementation{ID: name}, Normalize: func(context.Context, model.ToolCall) (json.RawMessage, error) { return nil, err }, Prepare: func(context.Context, model.ToolCall) harness.PreparedTool { return harness.PreparedTool{} }}, true
				}
				if tool := sel.invocation.snapshot.composition.standardValues[name]; tool != nil {
					_ = current.release()
					return harness.ToolBinding{
						Identity: harness.ToolImplementation{ID: name},
						Normalize: func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
							return tool.Normalize(ctx, tc, call)
						},
						Prepare: func(ctx context.Context, call model.ToolCall) harness.PreparedTool {
							callTC := tc
							background := &boundBackground{bridge: p.background, sessionID: sessionID}
							callTC.Background = background
							return background.bindPlan(tool.Prepare(ctx, callTC, call))
						},
					}, true
				}
				binding := current.bindTool(name)
				_ = current.release()
				if binding == nil {
					return harness.ToolBinding{}, false
				}
				tool := registeredExtensionTool{binding: binding, dataDir: p.config.dataDir, lease: sel.lease, states: sel.states, host: sel.invocation.snapshot.composition, operationSecrets: sel.operationSecrets}
				return harness.ToolBinding{
					Identity: harness.ToolImplementation{ID: binding.entry.id, ExtensionID: binding.entry.activation.id},
					Normalize: func(ctx context.Context, call model.ToolCall) (json.RawMessage, error) {
						return tool.Normalize(ctx, tc, call)
					},
					Prepare: func(ctx context.Context, call model.ToolCall) harness.PreparedTool {
						return tool.Prepare(ctx, tc, call)
					},
					Release: func() { _ = binding.release() },
				}, true
			},
			ArgumentStage: func() harness.ArgumentStage {
				current, err := sel.native.stage()
				if err != nil {
					return harness.ArgumentStage{Error: err}
				}
				return harness.ArgumentStage{Hooks: registeredArgumentHooks(current, sel, session), Release: func() { _ = current.release() }}
			},
			AuthorizationStage: func() harness.AuthorizationStage {
				current, err := sel.native.stage()
				if err != nil {
					return harness.AuthorizationStage{Error: err}
				}
				return harness.AuthorizationStage{Hooks: current.authorizationHooks(tc, sel.states, settings, sel.invocation.snapshot.composition, sel.lease, sel.operationSecrets), Release: func() { _ = current.release() }}
			},
			ResultStage: func() harness.ResultStage {
				current, err := sel.native.stage()
				if err != nil {
					return harness.ResultStage{Error: err}
				}
				return harness.ResultStage{Hooks: registeredResultHooks(current, sel, session), Release: func() { _ = current.release() }}
			},
		}, nil
	}
}

// ownToolDefinitions returns a deep copy of one tool-definition list: the
// slice, every definition, and its parameter bytes are independent of the
// Caller's storage.
func ownToolDefinitions(definitions []model.ToolDefinition) []model.ToolDefinition {
	if definitions == nil {
		return nil
	}
	owned := make([]model.ToolDefinition, len(definitions))
	for i, definition := range definitions {
		owned[i] = definition
		owned[i].Parameters = bytes.Clone(definition.Parameters)
	}
	return owned
}
