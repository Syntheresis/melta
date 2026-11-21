package melta

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/tool"
	"github.com/Syntheresis/melta/protocol"
)

type nativeWorkspace struct {
	root            string
	dataDir         string
	entry           harness.EntryRef
	lease           *artifactCallLease
	restrictionBind func(string) (string, error)
	mu              sync.Mutex
	bindings        map[string]nativeWorkspaceTarget
}

type nativeWorkspaceTarget struct {
	bound     tool.WorkspaceBinding
	canonical string
}

func newNativeWorkspace(root, dataDir string, entry harness.EntryRef, lease *artifactCallLease) *nativeWorkspace {
	return &nativeWorkspace{root: root, dataDir: dataDir, entry: entry, lease: lease, bindings: make(map[string]nativeWorkspaceTarget)}
}

func scopedArguments(args json.RawMessage, value any) error {
	if err := protocol.DecodeDocument(args, value); err != nil {
		return fmt.Errorf("service arguments: %v: %w", err, harness.ErrInvalid)
	}
	return nil
}

func (w *nativeWorkspace) bind(args json.RawMessage) (extension.Result, error) {
	var input struct {
		Path string `json:"path"`
	}
	if err := scopedArguments(args, &input); err != nil {
		return extension.Result{}, err
	}
	bound, canonical, exists, err := tool.BindWorkspace(w.root, input.Path)
	if err != nil {
		return extension.Result{}, err
	}
	if w.restrictionBind != nil {
		witness, err := w.restrictionBind(input.Path)
		if err != nil {
			return extension.Result{}, err
		}
		if witness != canonical {
			return extension.Result{}, fmt.Errorf("workspace witness changed: %w", ErrDenied)
		}
	}
	handle, err := newInstanceID()
	if err != nil {
		return extension.Result{}, err
	}
	w.mu.Lock()
	w.bindings[handle] = nativeWorkspaceTarget{bound: bound, canonical: canonical}
	w.mu.Unlock()
	value, err := json.Marshal(struct {
		Binding       string `json:"binding"`
		CanonicalPath string `json:"canonical_path"`
		Exists        bool   `json:"exists"`
	}{handle, canonical, exists})
	return extension.Result{Value: value}, err
}

func (w *nativeWorkspace) execute(ctx context.Context, method string, args json.RawMessage, permissions []harness.PermissionRequest) (extension.Result, error) {
	var input struct {
		Binding    string            `json:"binding"`
		Bytes      *worker.JSONBytes `json:"bytes"`
		CreateOnly *bool             `json:"create_only"`
	}
	if err := scopedArguments(args, &input); err != nil {
		return extension.Result{}, err
	}
	w.mu.Lock()
	target, ok := w.bindings[input.Binding]
	w.mu.Unlock()
	if !ok {
		return extension.Result{}, ErrDenied
	}
	write := slices.Contains(permissions, harness.PermissionRequest{Permission: harness.PermissionFileWrite, Target: target.canonical})
	read := slices.Contains(permissions, harness.PermissionRequest{Permission: harness.PermissionFileRead, Target: target.canonical})
	if !write && (method != "read" || !read) {
		return extension.Result{}, ErrDenied
	}
	if w.lease == nil || w.dataDir == "" || w.entry.SessionID == "" || w.entry.EntryID == "" {
		return extension.Result{}, extension.ErrClosed
	}
	w.lease.mu.Lock()
	validLease := w.lease.borrowed && !w.lease.closed && w.lease.release != nil && w.lease.key == sessionArtifactDir(w.dataDir, w.entry.SessionID)
	w.lease.mu.Unlock()
	if !validLease {
		return extension.Result{}, extension.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return extension.Result{}, err
	}
	recheck := harness.LocationRecheckFromContext(ctx)
	if method == "read" {
		content, err := target.bound.Read(recheck)
		if err != nil {
			return extension.Result{}, err
		}
		value, err := json.Marshal(struct {
			Bytes worker.JSONBytes `json:"bytes"`
		}{content})
		return extension.Result{Value: value}, err
	}
	store, err := (&standardTools{dataDir: w.dataDir}).openCodeGroup(ToolContext{AdmittedEntry: w.entry})
	if err != nil {
		return extension.Result{}, err
	}
	var outcome tool.MutationOutcome
	if method == "write" {
		outcome, err = target.bound.Write(store, *input.Bytes, *input.CreateOnly, recheck)
	} else {
		outcome, err = target.bound.Delete(store, recheck)
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
