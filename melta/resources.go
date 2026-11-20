package melta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
)

// resourceSet owns one Melta scope and its Workspace registry. Publication
// holds one reference; preparations transfer their references to the Harness.
type resourceSet struct {
	owner       *configurationService
	revision    uint64
	cancel      context.CancelFunc
	runtime     *scope
	composition *composition
	workspaces  *workspaceScopes

	mu        sync.Mutex
	holds     int
	warnings  [][3]string
	committed bool

	// heldNames are the declared names this resource set keeps excluded for
	// its whole candidate/open/retirement lifetime. They are released only
	// after shutdown and runtime cleanup finish.
	heldNames []string
}

func (s *configurationService) prepareResources(Caller context.Context, snapshot *configuration) error {
	if s.composition == nil {
		return nil
	}
	previous := s.current()
	if previous != nil {
		equal := true
		for _, author := range s.authors {
			id := author.extension.Manifest.ID
			a, aok := previous.extensions[id]
			b, bok := snapshot.extensions[id]
			aok = aok && sectionEnabled(a)
			bok = bok && sectionEnabled(b)
			if !aok && !bok {
				continue
			}
			if aok != bok || !bytes.Equal(a, b) {
				equal = false
				break
			}
		}
		if equal {
			equal = maps.Equal(previous.declaredBindings, snapshot.declaredBindings)
		}
		if equal {
			snapshot.resources = previous.resources
			snapshot.composition = previous.resources.composition
			return nil
		}
	}
	ctx, cancel := context.WithCancel(s.owner)
	stopCaller := context.AfterFunc(Caller, cancel)
	r := &resourceSet{owner: s, revision: snapshot.generation, cancel: cancel, holds: 1, composition: s.composition}
	runtimeInfo := ScopeInfo{Kind: ScopeRuntime, DataDir: s.dataDir, Revision: fmt.Sprint(r.revision)}
	// The candidate's declared names are held before the runtime scope opens.
	// The hold survives failed-prefix cleanup and transfers to the successful set.
	r.heldNames = snapshot.declaredSecretNames()
	s.holdCredentialNames(r.heldNames)
	runtime, err := s.composition.openScope(ctx, runtimeInfo, nil)
	stopCaller()
	if err != nil {
		cancel()
		s.releaseCredentialNames(r.heldNames)
		if canceled := s.canceled(Caller); canceled != nil {
			return errors.Join(canceled, err)
		}
		return configurationFailure(err)
	}
	if err := s.canceled(Caller); err != nil {
		cancel()
		cleanupErr := runtime.cleanup()
		s.releaseCredentialNames(r.heldNames)
		return errors.Join(err, cleanupErr)
	}
	r.runtime = runtime
	r.workspaces = newWorkspaceScopes(ctx, s.composition, []*scope{runtime}, s.obs)
	snapshot.resources = r
	return nil
}

func (r *resourceSet) reportWarning(id, kind, message string) {
	r.mu.Lock()
	if !r.committed {
		r.warnings = append(r.warnings, [3]string{id, kind, message})
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	if r.owner.composition != nil && r.owner.composition.reportWarning != nil {
		r.owner.composition.reportWarning(id, kind, message)
	}
}

func (r *resourceSet) retain() {
	r.mu.Lock()
	r.holds++
	r.mu.Unlock()
}

func (r *resourceSet) release() error {
	r.mu.Lock()
	r.holds--
	last := r.holds == 0
	r.mu.Unlock()
	if !last {
		return nil
	}
	r.cancel() // builders must observe cancellation before shutdown joins them
	err := errors.Join(r.workspaces.shutdown(), r.runtime.close())
	r.owner.cleanupMu.Lock()
	r.owner.cleanupErr = errors.Join(r.owner.cleanupErr, err)
	r.owner.cleanupMu.Unlock()
	r.owner.releaseCredentialNames(r.heldNames)
	return err
}

func (r *resourceSet) commitWarnings() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := false
	for _, w := range r.warnings {
		changed = r.owner.warnings.appendPlugin(w[0], w[1], w[2]) || changed
	}
	r.warnings = nil
	r.committed = true
	return changed
}

func (r *resourceSet) revisionString() string {
	if r == nil {
		return ""
	}
	return fmt.Sprint(r.revision)
}
