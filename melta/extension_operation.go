package melta

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/protocol"
)

// ListExtensions returns the registered tools and client operations.
func (r *Melta) ListExtensions(ctx context.Context, c Caller) ([]protocol.RegisteredExtension, error) {
	release, err := r.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := r.authorize(c, ctx, nil, harnessScope); err != nil {
		return nil, err
	}
	captured, err := r.config.capturePreparation(ctx)
	if err != nil {
		return nil, err
	}
	if captured.snapshot == nil || captured.native == nil {
		return nil, ErrClosed
	}
	defer func() {
		_ = captured.native.release()
		_ = captured.snapshot.resources.release()
		captured.releaseHeldNames()
	}()
	stage, err := captured.native.stage()
	if err != nil {
		return nil, err
	}
	defer stage.release()
	r.config.nativeMu.Lock()
	inactive := make(map[string]error, len(captured.snapshot.native.inactive))
	for id, reason := range captured.snapshot.native.inactive {
		inactive[id] = reason
	}
	r.config.nativeMu.Unlock()
	out := make([]protocol.RegisteredExtension, 0, len(r.config.authors)+len(inactive))
	seen := make(map[string]bool)
	for _, spec := range r.config.authors {
		manifest := spec.extension.Manifest
		section := captured.snapshot.extensions[manifest.ID]
		if !sectionEnabled(section) || seen[manifest.ID] {
			continue
		}
		seen[manifest.ID] = true
		schema := protocol.ExtensionConfigDocument(manifest.SettingsSchema)
		item := protocol.RegisteredExtension{
			ExtensionId: manifest.ID, Version: manifest.Version,
			ConfigurationSchema: &schema, Settings: string(section),
			Tools: []string{}, Operations: []protocol.RegisteredExtensionOperation{},
		}
		if failure := inactive[manifest.ID]; failure != nil {
			reason := failure.Error()
			item.InactiveReason = &reason
		}
		for _, entry := range stage.entries {
			if entry.activation.id != manifest.ID {
				continue
			}
			switch entry.kind {
			case "tool":
				item.Tools = append(item.Tools, entry.name)
			case "operation":
				op := entry.value.(struct {
					target extension.ExposedTargetKind
					method nativeServiceMethod
				})
				method := op.method.contract.Method
				projected := protocol.ExtensionMethod{Name: method.Name, Input: string(method.Input), Success: string(method.Success)}
				if len(method.Error) != 0 {
					value := string(method.Error)
					projected.Error = &value
				}
				if len(method.Partial) != 0 {
					value := string(method.Partial)
					projected.Partial = &value
				}
				item.Operations = append(item.Operations, protocol.RegisteredExtensionOperation{Method: projected, Target: protocol.RegisteredExtensionOperationTarget(op.target)})
			}
		}
		out = append(out, item)
	}
	var missing []string
	for id := range inactive {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	for _, id := range missing {
		reason := inactive[id].Error()
		out = append(out, protocol.RegisteredExtension{
			ExtensionId: id, Version: r.packages.selectedVersion(id),
			Settings: string(captured.snapshot.extensions[id]),
			Tools:    []string{}, Operations: []protocol.RegisteredExtensionOperation{}, InactiveReason: &reason,
		})
	}
	return out, nil
}

// InvokeExtension resolves one declared exposure, authorizes its exact target
// against the Caller grant, validates the raw arguments against the selected
// method schema, and calls the existing named-service provider once under a
// fresh scoped view. A validated declared domain failure is Result.Failure with
// a nil Go error; every predispatch or host failure is the ordinary public Go
// error class.
func (r *Melta) InvokeExtension(ctx context.Context, c Caller, request protocol.ExtensionInvocationRequest) (extension.Result, error) {
	if err := validateInvocationAddress(request); err != nil {
		return extension.Result{}, err
	}
	arguments := json.RawMessage("null")
	if request.Arguments != nil {
		arguments = append(json.RawMessage(nil), (*request.Arguments)...)
	}
	return r.invokeRegisteredOperation(ctx, c, request.ExtensionId, request.Operation, request.Target, arguments, request.RequestId)
}

func (r *Melta) invocationTarget(ctx context.Context, caller Caller, h *harness.Harness, input protocol.ExtensionInvocationTarget, declared extension.ExposedTargetKind) (CallerScope, harness.SessionIdentity, error) {
	target, err := resolveInvocationTarget(input, declared)
	if err != nil {
		return CallerScope{}, harness.SessionIdentity{}, err
	}
	if err := r.authorize(caller, ctx, h, target); err != nil {
		return CallerScope{}, harness.SessionIdentity{}, err
	}
	if target.Kind == CallerScopeSession {
		header, err := h.ReadSessionHeader(ctx, target.SessionID)
		if err != nil {
			return CallerScope{}, harness.SessionIdentity{}, err
		}
		return target, header.Identity, nil
	}
	return target, harness.SessionIdentity{Workspace: target.Workspace}, nil
}

// validateInvocationAddress enforces the nonempty addressing strings and the
// closed target union's exact member shape before any declaration lookup.
func validateInvocationAddress(request protocol.ExtensionInvocationRequest) error {
	for name, value := range map[string]string{
		"extension_id": request.ExtensionId,
		"operation":    request.Operation,
		"request_id":   request.RequestId,
	} {
		if value == "" {
			return fmt.Errorf("invocation member %q must be non-empty: %w", name, harness.ErrInvalid)
		}
	}
	return nil
}

// resolveInvocationTarget validates the closed target union, its exact member
// shape for the selected branch, and its match with the declaration's target
// kind.
func resolveInvocationTarget(request protocol.ExtensionInvocationTarget, declared extension.ExposedTargetKind) (CallerScope, error) {
	kind := protocol.ExtensionInvocationTargetKind(declared)
	if request.Kind != kind {
		return CallerScope{}, fmt.Errorf("invocation target kind %q does not match the declared %q: %w", request.Kind, declared, harness.ErrInvalid)
	}
	switch request.Kind {
	case protocol.ExtensionInvocationTargetKindHarness:
		if request.Workspace != nil || request.SessionId != nil {
			return CallerScope{}, fmt.Errorf("harness target carries no Workspace or Session: %w", harness.ErrInvalid)
		}
		return harnessScope, nil
	case protocol.ExtensionInvocationTargetKindWorkspace:
		if request.Workspace == nil || request.SessionId != nil {
			return CallerScope{}, fmt.Errorf("workspace target requires exactly one Workspace path: %w", harness.ErrInvalid)
		}
		workspace := *request.Workspace
		if !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace {
			return CallerScope{}, fmt.Errorf("workspace target requires one clean absolute path: %w", harness.ErrInvalid)
		}
		return CallerScope{Kind: CallerScopeWorkspace, Workspace: workspace}, nil
	case protocol.ExtensionInvocationTargetKindSession:
		if request.SessionId == nil || request.Workspace != nil {
			return CallerScope{}, fmt.Errorf("session target requires exactly one canonical Session ID: %w", harness.ErrInvalid)
		}
		if !canonicalSessionID(*request.SessionId) {
			return CallerScope{}, fmt.Errorf("session target requires one canonical Session ID: %w", harness.ErrInvalid)
		}
		return CallerScope{Kind: CallerScopeSession, SessionID: *request.SessionId}, nil
	default:
		return CallerScope{}, fmt.Errorf("invocation target kind %q is not one of harness, workspace or session: %w", request.Kind, harness.ErrInvalid)
	}
}
