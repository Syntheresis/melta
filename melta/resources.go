package melta

import (
	"bytes"
	"maps"
	"sync"
)

// resourceSet owns one configuration revision's held credential names.
// Publication holds one reference; preparations transfer their references to
// the Harness.
type resourceSet struct {
	owner       *configurationService
	composition *composition

	mu        sync.Mutex
	holds     int
	warnings  [][3]string
	committed bool

	// heldNames are the declared names this resource set keeps excluded for
	// its whole candidate/open/retirement lifetime. They are released only
	// after shutdown finishes.
	heldNames []string
}

func (s *configurationService) prepareResources(snapshot *configuration) {
	if s.composition == nil {
		return
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
			return
		}
	}
	r := &resourceSet{owner: s, holds: 1, composition: s.composition}
	// The candidate's declared names are held before anything else. The hold
	// survives failed-prefix cleanup and transfers to the successful set.
	r.heldNames = snapshot.declaredSecretNames()
	s.holdCredentialNames(r.heldNames)
	snapshot.resources = r
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

func (r *resourceSet) release() {
	r.mu.Lock()
	r.holds--
	last := r.holds == 0
	r.mu.Unlock()
	if !last {
		return
	}
	r.owner.releaseCredentialNames(r.heldNames)
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
