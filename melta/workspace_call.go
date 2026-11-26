package melta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/tool"
	"github.com/Syntheresis/melta/protocol"
)

func scopedArguments(args json.RawMessage, value any) error {
	if err := protocol.DecodeDocument(args, value); err != nil {
		return fmt.Errorf("service arguments: %v: %w", err, harness.ErrInvalid)
	}
	return nil
}

func (scope *nativeCallScope) workspaceCall(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	var input struct {
		Path       string            `json:"path"`
		Bytes      *worker.JSONBytes `json:"bytes"`
		CreateOnly *bool             `json:"create_only"`
	}
	if err := scopedArguments(args, &input); err != nil {
		return extension.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return extension.Result{}, err
	}
	if method == "stat" {
		canonical, exists, err := tool.WorkspaceStat(scope.session.Workspace, input.Path)
		if err != nil {
			return extension.Result{}, err
		}
		value, err := json.Marshal(struct {
			CanonicalPath string `json:"canonical_path"`
			Exists        bool   `json:"exists"`
		}{canonical, exists})
		return extension.Result{Value: value}, err
	}
	if method == "read" {
		content, err := tool.WorkspaceRead(scope.session.Workspace, input.Path)
		if err != nil {
			return extension.Result{}, err
		}
		value, err := json.Marshal(struct {
			Bytes worker.JSONBytes `json:"bytes"`
		}{content})
		return extension.Result{Value: value}, err
	}
	if scope.lease == nil || scope.dataDir == "" || scope.admittedEntry.SessionID == "" || scope.admittedEntry.EntryID == "" {
		return extension.Result{}, extension.ErrClosed
	}
	scope.lease.mu.Lock()
	validLease := scope.lease.borrowed && !scope.lease.closed && scope.lease.release != nil && scope.lease.key == sessionArtifactDir(scope.dataDir, scope.admittedEntry.SessionID)
	scope.lease.mu.Unlock()
	if !validLease {
		return extension.Result{}, extension.ErrClosed
	}
	store, err := (&standardTools{dataDir: scope.dataDir}).openCodeGroup(ToolContext{AdmittedEntry: scope.admittedEntry})
	if err != nil {
		return extension.Result{}, err
	}
	var outcome tool.MutationOutcome
	if method == "write" {
		outcome, err = tool.WorkspaceWrite(scope.session.Workspace, input.Path, store, *input.Bytes, *input.CreateOnly)
	} else {
		outcome, err = tool.WorkspaceDelete(scope.session.Workspace, input.Path, store)
	}
	var value json.RawMessage
	switch outcome {
	case tool.MutationCompleted:
		value = json.RawMessage(`{"outcome":"completed"}`)
	case tool.MutationUncertain:
		value = json.RawMessage(`{"outcome":"uncertain"}`)
	}
	if err != nil {
		if len(value) != 0 {
			return extension.Result{}, &extension.CallError{Err: err, Partial: value}
		}
		return extension.Result{}, err
	}
	return extension.Result{Value: value}, nil
}
