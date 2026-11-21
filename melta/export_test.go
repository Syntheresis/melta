package melta

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/catalog"
)

// ConfiguredInvocationForTest builds one configured Invocation whose
// captured snapshot carries the given top-level baseline sections and
// per-plugin sections. It is absent from production builds.
func ConfiguredInvocationForTest(tools, jobs, compaction string, sections map[string]string) (Invocation, error) {
	plugins := make(map[string]json.RawMessage, len(sections))
	for id, section := range sections {
		plugins[id] = json.RawMessage(section)
	}
	doc := capturedConfigDocument{
		Tools:      json.RawMessage(tools),
		Jobs:       json.RawMessage(jobs),
		Compaction: json.RawMessage(compaction),
		Extensions: plugins,
	}
	snapshot, err := newConfiguration(1, doc, catalog.BuildResult{}, nil)
	if err != nil {
		return Invocation{}, err
	}
	return Invocation{snapshot: snapshot}, nil
}

// EqualEventForTest exposes the field-wise Event comparison to tests.
func EqualEventForTest(a, b Event) bool {
	return equalEvent(a, b)
}

// publishForTest runs one prepared candidate through the locked publication
// steps the configuration commit uses, so tests observe the same publication
// the ready snapshot gets.
func (c *nativeCandidate) publishForTest() error {
	c.owner.nativeMu.Lock()
	defer c.owner.nativeMu.Unlock()
	if c.holds == 0 {
		return extension.ErrClosed
	}
	c.preparePublishLocked()
	if c.owner.obs != nil {
		c.owner.obs.publish(c.publishLocked)
	} else {
		c.publishLocked()
	}
	return nil
}

// snapshotForTest captures one candidate's ready registrations the way the
// cleanup path captures the current publication.
func (c *nativeCandidate) snapshotForTest() *nativeSnapshot {
	c.owner.nativeMu.Lock()
	defer c.owner.nativeMu.Unlock()
	if c.holds == 0 {
		return nil
	}
	var activations []*nativeActivation
	for _, a := range c.activations {
		if a != nil && c.ready[a] {
			activations = append(activations, a)
		}
	}
	return c.snapshotLocked(activations)
}

// createSession is the shared in-package fixture: it enters the owner's
// admitted-call gate and runs the one normalized creation core so tests
// needing the full Harness record observe the real gate.
func (r *Melta) createSession(ctx context.Context, workspace, agentType string) (harness.SessionRecord, error) {
	var record harness.SessionRecord
	if err := r.withHarness(ctx, func(ctx context.Context, h *harness.Harness) error {
		var err error
		state, marshalErr := json.Marshal(struct {
			Agent string `json:"agent"`
		}{agentType})
		if marshalErr != nil {
			return marshalErr
		}
		record, err = r.createSessionRecord(ctx, h, workspace, harness.SessionInputs{}, map[string]harness.SessionStateValue{"agents": state})
		return err
	}); err != nil {
		return harness.SessionRecord{}, err
	}
	return record, nil
}
