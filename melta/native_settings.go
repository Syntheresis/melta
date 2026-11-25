package melta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

func (scope *nativeCallScope) updateSettings(ctx context.Context, args json.RawMessage) (extension.Result, error) {
	var input struct {
		Section json.RawMessage `json:"section"`
	}
	if err := scopedArguments(args, &input); err != nil {
		return extension.Result{}, err
	}
	activation := scope.activation
	owner := activation.owner
	_, err := owner.mutate(ctx, configurationEdit{apply: func(roots rawRoots, _ configurationCapture) (editedFile, error) {
		sections, err := extensionSections(roots, activation.id)
		current := sections[activation.id]
		if err != nil || !bytes.Equal(current, activation.section) {
			return 0, fmt.Errorf("settings section changed: %w", harness.ErrConflict)
		}
		if !sectionEnabled(input.Section) || !sameSecretNames(activation.manifest, current, input.Section) {
			return 0, fmt.Errorf("settings authority changed: %w", ErrDenied)
		}
		if err := extension.ValidateManifestSection(activation.manifest, input.Section); err != nil {
			return 0, fmt.Errorf("%w: %w", harness.ErrInvalid, err)
		}
		return writeExtensionSection(roots, sections, activation.id, input.Section)
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
