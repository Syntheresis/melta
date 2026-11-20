package melta

// This file owns the one post-commit local/external cleanup producer used by
// direct Session deletion and the lifecycle sweep. Canonical deletion is
// already committed; every local tree removal and every recorded external
// owner is attempted, individual failures are reported, and nothing is ever
// rolled back. External cleanup is an ordinary guarded Harness-scoped service
// call with no Session binding or grant.

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/protocol"
)

// cleanupUnavailableMessage is the fixed non-secret external failure detail:
// guest error text and domain payloads never enter the public result.
const cleanupUnavailableMessage = "extension data cleanup is unavailable"

// cleanupCallDeadline bounds one cooperative external cleanup callback. It is
// a cancellation opportunity, never forced termination.
const cleanupCallDeadline = 5 * time.Second

// deletedSession is one committed deletion's local identity and its
// once-returned external-data owners.
type deletedSession struct {
	sessionID string
	owners    []string
}

// cleanupDeletedSessions runs the shared post-commit producer: it always
// removes every committed Session tree, then takes one current
// configuration/resource reservation for the whole owner batch and invokes
// each owner's cleanup declaration. Every failure is reported separately;
// one failing owner never suppresses a sibling, local failure never suppresses
// external cleanup, and only the first result can report owners.
func (r *Melta) cleanupDeletedSessions(ctx context.Context, deletions []deletedSession) []protocol.CleanupFailure {
	failures := make([]protocol.CleanupFailure, 0)
	for _, deletion := range deletions {
		if err := r.removeSessionCode(deletion.sessionID); err != nil {
			failures = append(failures, protocol.CleanupFailure{Code: "storage", Message: err.Error()})
		}
	}
	hasOwners := false
	for _, deletion := range deletions {
		if len(deletion.owners) != 0 {
			hasOwners = true
			break
		}
	}
	if !hasOwners {
		return failures
	}
	captured, err := r.config.captureFor(ctx, true)
	snapshot := captured.snapshot
	if err != nil || snapshot == nil || snapshot.resources == nil {
		return appendOwnerFailures(failures, deletions)
	}
	defer func() {
		_ = snapshot.resources.release()
	}()
	r.config.nativeMu.Lock()
	var registered *nativeSnapshot
	if current := r.config.nativeCurrent; current != nil && current.holds > 0 {
		var activations []*nativeActivation
		for _, active := range current.activations {
			if active != nil && current.ready[active] {
				activations = append(activations, active)
			}
		}
		registered = current.snapshotLocked(activations)
	}
	r.config.nativeMu.Unlock()
	if registered != nil {
		defer registered.release()
	}
	for _, deletion := range deletions {
		for _, owner := range deletion.owners {
			var failure *protocol.CleanupFailure
			if registered != nil && registered.hasActivation(owner) {
				failure = registered.cleanupOwner(ctx, deletion.sessionID, owner)
			} else {
				failure = ownerUnavailable(owner)
			}
			if failure != nil {
				failures = append(failures, *failure)
			}
		}
	}
	return failures
}

func (s *nativeSnapshot) hasActivation(id string) bool {
	for _, activation := range s.candidate.activations {
		if activation != nil && activation.id == id {
			return true
		}
	}
	return false
}

func (s *nativeSnapshot) cleanupOwner(ctx context.Context, sessionID, owner string) *protocol.CleanupFailure {
	extensionID := owner
	failure := &protocol.CleanupFailure{ExtensionId: &extensionID, Code: "unavailable", Message: cleanupUnavailableMessage}
	found, failed := false, false
	for _, registration := range s.hooks["cleanup"] {
		if registration.activation.id != owner {
			continue
		}
		activation := registration.activation
		found = true
		if activation.unavailable() != nil {
			failed = true
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, cleanupCallDeadline)
		scope := &nativeCallScope{nativeCallLifetime: nativeCallLifetime{ctx: callCtx, cancel: cancel}, candidate: s.candidate, activation: activation}
		call := extension.AuthorContext{ExtensionID: owner, Settings: append(json.RawMessage(nil), activation.section...), Call: scope.call}
		err := registration.value.(extension.CleanupHook)(callCtx, call, harness.SessionIdentity{SessionID: sessionID})
		scope.end()
		if err != nil {
			failed = true
		}
	}
	if !found || failed {
		return failure
	}
	return nil
}

// appendOwnerFailures marks every recorded owner unavailable when the current
// resource batch cannot be observed at all.
func appendOwnerFailures(failures []protocol.CleanupFailure, deletions []deletedSession) []protocol.CleanupFailure {
	for _, deletion := range deletions {
		for _, owner := range deletion.owners {
			failures = append(failures, *ownerUnavailable(owner))
		}
	}
	return failures
}

// ownerUnavailable is the fixed unavailable outcome for one recorded external
// data owner: a disabled, retired or unregistered owner and a failed cleanup
// call are the same reported result.
func ownerUnavailable(owner string) *protocol.CleanupFailure {
	extensionID := owner
	return &protocol.CleanupFailure{ExtensionId: &extensionID, Code: "unavailable", Message: cleanupUnavailableMessage}
}

// cleanupFailureText renders one reported failure for the sweep's retained
// per-pass diagnostic.
func cleanupFailureText(failure protocol.CleanupFailure) string {
	if failure.ExtensionId != nil {
		return string(failure.Code) + ": " + failure.Message + " (extension " + *failure.ExtensionId + ")"
	}
	return string(failure.Code) + ": " + failure.Message
}
