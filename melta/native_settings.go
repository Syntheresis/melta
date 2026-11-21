package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

func (scope *nativeCallScope) updateSettings(ctx context.Context, args json.RawMessage) (extension.Result, error) {
	var input struct {
		Service   string          `json:"service"`
		Method    string          `json:"method"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := scopedArguments(args, &input); err != nil {
		return extension.Result{}, err
	}
	activation := scope.activation
	owner := activation.owner
	owner.nativeMu.Lock()
	var registration *nativeRegistration
	var method nativeServiceMethod
	for _, entry := range activation.entries {
		if entry.kind == "service" && entry.name == input.Service {
			registration = entry
			method = entry.value.(registeredService).methods[input.Method]
			break
		}
	}
	if registration != nil && method.contract != nil {
		registration.holds++
	}
	owner.nativeMu.Unlock()
	if registration == nil || method.contract == nil {
		return extension.Result{}, fmt.Errorf("settings transform is unavailable: %w", harness.ErrNotFound)
	}
	defer func() {
		owner.nativeMu.Lock()
		registration.holds--
		release := registration.takeRelease()
		owner.nativeMu.Unlock()
		if release != nil {
			release()
		}
	}()
	if method.contract.Mode != extension.ModePure {
		return extension.Result{}, fmt.Errorf("settings transform must be pure: %w", ErrDenied)
	}
	_, err := owner.mutate(ctx, configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
		owner.nativeMu.Lock()
		approved := false
		if current := owner.nativeCurrent; current != nil {
			for _, active := range current.activations {
				if active != nil && active.id == activation.id && current.ready[active] && active.lost == nil && maps.Equal(active.services, activation.services) {
					approved = true
					break
				}
			}
		}
		owner.nativeMu.Unlock()
		if !approved {
			return 0, fmt.Errorf("settings authority changed: %w", ErrDenied)
		}
		sections, err := extensionSections(roots, activation.id)
		if err != nil {
			return 0, err
		}
		section := sections[activation.id]
		if !sectionEnabled(section) || !sectionEnabled(activation.section) || !sameSecretNames(activation.manifest, activation.section, section) {
			return 0, fmt.Errorf("settings authority changed: %w", ErrDenied)
		}
		transformArgs, err := marshalRawObject(map[string]json.RawMessage{"section": section, "arguments": input.Arguments}, true)
		if err != nil {
			return 0, err
		}
		transformArgs, err = acceptNativeMethod(method.contract, transformArgs, true)
		if err != nil {
			return 0, err
		}
		childCtx, cancel := context.WithCancel(ctx)
		child := callbackScope(&nativeCallScope{
			nativeCallLifetime: nativeCallLifetime{ctx: childCtx, cancel: cancel},
			candidate:          scope.candidate, activation: activation, pure: true,
			workspace: scope.workspace, permissions: scope.permissions, session: scope.session, states: scope.states, requestID: scope.requestID,
		})
		result, err := func() (extension.Result, error) {
			defer child.end()
			return executeNativeProvider(child.ctx, extension.AuthorContext{
				ExtensionID: activation.id, Settings: bytes.Clone(activation.section),
				Session: scope.session, State: bytes.Clone(scope.states[activation.id]), Call: child.call, RequestID: scope.requestID,
			}, method.contract, method.handler, "service", input.Service, transformArgs)
		}()
		if err != nil {
			return 0, err
		}
		if result.Failure != nil {
			return 0, fmt.Errorf("%s: %s", result.Failure.Code, result.Failure.Message)
		}
		if !sectionEnabled(result.Value) || !sameSecretNames(activation.manifest, section, result.Value) {
			return 0, fmt.Errorf("settings transform changed authority: %w", ErrDenied)
		}
		if err := extension.ValidateManifestSection(activation.manifest, result.Value); err != nil {
			return 0, err
		}
		return writeExtensionSection(roots, sections, activation.id, result.Value)
	}})
	if err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: json.RawMessage("null")}, nil
}

func sameSecretNames(manifest extension.Manifest, left, right json.RawMessage) bool {
	for _, secret := range manifest.Secrets {
		leftName, leftOK := extension.SecretName(left, secret.Field)
		rightName, rightOK := extension.SecretName(right, secret.Field)
		if leftOK != rightOK || leftName != rightName {
			return false
		}
	}
	return true
}
