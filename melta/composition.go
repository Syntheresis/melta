package melta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/protocol"
)

// Error identities reported by scope lifetime.
var (
	// ErrComposition wraps invalid scope attribution and host misuse.
	ErrComposition = errors.New("melta: composition failure")

	// ErrClosed reports a closed or owner-canceled scope or owner: no guard
	// can enter and no binding resolves from an unavailable supplying scope.
	ErrClosed = errors.New("melta: closed")
)

// ScopeKind names one tier of the nested host scope hierarchy: the owner's
// resource lifetime tiers, independent of any extension registration.
type ScopeKind string

const (
	ScopeRuntime   ScopeKind = "runtime"
	ScopeWorkspace ScopeKind = "workspace"
	ScopeOperation ScopeKind = "operation"
	ScopeAgent     ScopeKind = "agent"
)

// ScopeInfo is the immutable identity of one opened scope. DataDir is the
// same normalized owner root in every ScopeInfo. Workspace scopes carry only
// lexical Workspace identity, never Session or Operation attribution; the
// runtime scope carries no narrower attribution at all; short scopes carry
// the complete Workspace/Session/Operation identity.
type ScopeInfo struct {
	Kind         ScopeKind
	DataDir      string
	Workspace    string
	SessionID    string
	OperationID  string
	InvocationID string
	Purpose      string
	Revision     string
}

// composition is the owner-wide host state: the constructed standard tool
// values, the neutral plugin-warning sink, and the fixed host named services.
// The data handler reads the owner Harness and Session artifact registry from
// the same shared value.
type composition struct {
	standardValues map[string]Tool
	reportWarning  func(pluginID, kind, message string)
	hosts          *hostServices
}

type hostServices struct {
	owner      *Melta
	background *backgroundBridge
	harness    *harness.Harness
	artifacts  *artifactIntervals
	dataDir    string
}

// newComposition builds the one owner-wide composition.
func newComposition() *composition {
	return &composition{
		standardValues: make(map[string]Tool),
		hosts:          &hostServices{},
	}
}

// validateScopeInfo enforces the per-kind attribution shape.
func validateScopeInfo(info ScopeInfo) error {
	if info.DataDir == "" {
		return fmt.Errorf("scope %s: missing data directory: %w", info.Kind, ErrComposition)
	}
	switch info.Kind {
	case ScopeRuntime:
		if info.Workspace != "" || info.SessionID != "" || info.OperationID != "" || info.InvocationID != "" {
			return fmt.Errorf("runtime scope carries narrower attribution %q/%q/%q/%q: %w", info.Workspace, info.SessionID, info.OperationID, info.InvocationID, ErrComposition)
		}
	case ScopeWorkspace:
		if info.Workspace == "" || info.SessionID != "" || info.OperationID != "" || info.InvocationID != "" {
			return fmt.Errorf("workspace scope requires only the Workspace identity, got %q/%q/%q/%q: %w", info.Workspace, info.SessionID, info.OperationID, info.InvocationID, ErrComposition)
		}
	case ScopeOperation:
		if info.Workspace == "" || info.SessionID == "" || info.OperationID == "" || info.InvocationID != "" {
			return fmt.Errorf("operation scope requires Workspace, Session, and Operation identity: %w", ErrComposition)
		}
	case ScopeAgent:
		if info.Workspace == "" || info.SessionID == "" || info.OperationID == "" || info.InvocationID == "" || info.Purpose == "" {
			return fmt.Errorf("invocation scope requires Workspace, Session, Operation, and invocation identity: %w", ErrComposition)
		}
	default:
		return fmt.Errorf("invalid scope kind %q: %w", info.Kind, ErrComposition)
	}
	return nil
}

// openScope opens one host lifetime scope: a cancellation and join guard for
// the work admitted under it, with its observation identity. Extension
// registrations are owned by the activation registrar, never by a scope.
func (c *composition) openScope(ctx context.Context, info ScopeInfo, ancestors []*scope) (*scope, error) {
	if err := validateScopeInfo(info); err != nil {
		return nil, err
	}
	scopeCtx, cancel := context.WithCancel(ctx)
	sc := &scope{info: info, ctx: scopeCtx, cancel: cancel}
	if len(ancestors) > 0 {
		sc.obs = ancestors[0].obs
	}
	if sc.obs != nil && info.Kind != ScopeWorkspace {
		sc.obs.publish(func() []Event {
			return []Event{scopeEvent(protocol.ScopeOpened, info)}
		})
	}
	return sc, nil
}

// scopeConstructionError keeps rollback failures distinct.
type scopeConstructionError struct {
	cause   error
	cleanup error
}

func (e *scopeConstructionError) Error() string { return errors.Join(e.cause, e.cleanup).Error() }

func (e *scopeConstructionError) Unwrap() []error {
	if e.cleanup == nil {
		return []error{e.cause}
	}
	return []error{e.cause, e.cleanup}
}

// nativeCloseDeadline is the fresh cooperative bound of one native close
// callback. It is a cancellation opportunity, not forced termination: trusted
// native code that ignores its context can still block cleanup.
var nativeCloseDeadline = 5 * time.Second

// declaredCredentialHandler resolves one declared credential field from the
// callback context's captured secret values. A field the extension did not
// declare is a misuse; every other absent, unresolved or undefined value
// fails uniformly as unavailable.
func declaredCredentialHandler(ctx context.Context, call hostCall, field string, secrets []extension.SecretRef) (extension.Result, error) {
	var declared *extension.SecretRef
	for i := range secrets {
		if secrets[i].Field == field {
			declared = &secrets[i]
			break
		}
	}
	if declared == nil {
		return extension.Result{}, fmt.Errorf("credential field %q is not declared: %w", field, ErrComposition)
	}
	values := secretValuesFrom(ctx)
	var value string
	var found bool
	if declared.Lifetime == extension.SecretActivation {
		value, found = values.activation[field]
	} else {
		perExtension := values.operation[call.ExtensionID]
		value, found = perExtension[field]
	}
	if !found || value == "" {
		return extension.Result{}, fmt.Errorf("credential %q is unavailable: %w", field, ErrClosed)
	}
	raw, err := json.Marshal(struct {
		Value string `json:"value"`
	}{Value: value})
	if err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: raw}, nil
}

// scope is one opened lifetime guard and its join state.
type scope struct {
	info   ScopeInfo
	ctx    context.Context
	cancel context.CancelFunc

	obs *observation

	closers []func() error

	mu      sync.Mutex
	closing bool
	closed  bool
	wg      sync.WaitGroup
}

// enter admits one call against the Caller's context.
func (s *scope) enter(ctx context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		err := ctx.Err()
		if err == nil {
			err = ErrClosed
		}
		return nil, nil, err
	}
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		err := ctx.Err()
		if err == nil {
			err = ErrClosed
		}
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	s.wg.Add(1)
	s.mu.Unlock()
	callCtx, cancelCall := context.WithCancel(s.ctx)
	stopCallerWatch := context.AfterFunc(ctx, cancelCall)
	var once sync.Once
	return callCtx, func() {
		once.Do(func() {
			stopCallerWatch()
			cancelCall()
			s.wg.Done()
		})
	}, nil
}

func (s *scope) close() error {
	commit := func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
	if s.obs != nil {
		s.obs.publish(func() []Event {
			commit()
			return []Event{scopeEvent(protocol.ScopeClosed, s.info)}
		})
	} else {
		commit()
	}
	return s.cleanup()
}

// cleanup is the common scope cleanup: mark closing before cancellation, join
// admitted work, then dispose.
func (s *scope) cleanup() error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
	var errs []error
	for i := len(s.closers) - 1; i >= 0; i-- {
		if err := s.closers[i](); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type workspaceAttempt struct {
	done chan struct{}
	sc   *scope
	err  error
}

type workspaceScopes struct {
	owner     context.Context
	c         *composition
	ancestors []*scope
	obs       *observation

	mu         sync.Mutex
	closed     bool
	live       map[string]*scope
	attempts   map[string]*workspaceAttempt
	building   sync.WaitGroup
	cleanupErr error
}

func newWorkspaceScopes(owner context.Context, c *composition, ancestors []*scope, obs *observation) *workspaceScopes {
	return &workspaceScopes{
		owner:     owner,
		c:         c,
		ancestors: ancestors,
		obs:       obs,
		live:      make(map[string]*scope),
		attempts:  make(map[string]*workspaceAttempt),
	}
}

func (w *workspaceScopes) get(ctx context.Context, info ScopeInfo) (*scope, error) {
	if info.Kind != ScopeWorkspace {
		return nil, fmt.Errorf("workspace scope requested with kind %q: %w", info.Kind, ErrComposition)
	}
	key := info.Workspace
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	if sc := w.live[key]; sc != nil {
		w.mu.Unlock()
		return sc, nil
	}
	attempt := w.attempts[key]
	if attempt == nil {
		attempt = &workspaceAttempt{done: make(chan struct{})}
		w.attempts[key] = attempt
		w.building.Add(1)
		go w.build(key, info, attempt)
	}
	w.mu.Unlock()
	select {
	case <-attempt.done:
		return attempt.sc, attempt.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (w *workspaceScopes) build(key string, info ScopeInfo, attempt *workspaceAttempt) {
	defer w.building.Done()
	sc, err := w.c.openScope(w.owner, info, w.ancestors)
	if err != nil {
		w.mu.Lock()
		delete(w.attempts, key)
		var construction *scopeConstructionError
		if errors.As(err, &construction) {
			w.cleanupErr = errors.Join(w.cleanupErr, construction.cleanup)
		}
		w.mu.Unlock()
		attempt.sc, attempt.err = sc, err
		close(attempt.done)
		return
	}
	var rejected error
	w.obs.publish(func() []Event {
		sc.obs = w.obs
		w.mu.Lock()
		delete(w.attempts, key)
		if w.closed || w.owner.Err() != nil {
			rejected = w.owner.Err()
			if rejected == nil {
				rejected = ErrClosed
			}
			w.mu.Unlock()
			return nil
		}
		w.live[key] = sc
		w.mu.Unlock()
		attempt.sc, attempt.err = sc, nil
		close(attempt.done)
		return []Event{scopeEvent(protocol.ScopeOpened, info)}
	})
	if rejected != nil {
		cleanup := sc.cleanup()
		w.mu.Lock()
		w.cleanupErr = errors.Join(w.cleanupErr, cleanup)
		w.mu.Unlock()
		attempt.err = &scopeConstructionError{cause: rejected, cleanup: cleanup}
		close(attempt.done)
	}
}

func (w *workspaceScopes) shutdown() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.building.Wait()
	w.mu.Lock()
	keys := make([]string, 0, len(w.live))
	for key := range w.live {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	live := make([]*scope, len(keys))
	for i, key := range keys {
		live[i] = w.live[key]
	}
	cleanupErr := w.cleanupErr
	w.mu.Unlock()
	errs := []error{cleanupErr}
	for _, sc := range live {
		errs = append(errs, sc.close())
	}
	return errors.Join(errs...)
}

// newInstanceID mints one host-issued opaque instance identity.
func newInstanceID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
