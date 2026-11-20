package melta

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

func (a *nativeActivation) reportWarning(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	if !a.services["melta.warnings"] {
		return extension.Result{}, ErrDenied
	}
	if method != "report" {
		return extension.Result{}, fmt.Errorf("warning method %q: %w", method, harness.ErrNotFound)
	}
	var input struct {
		Kind    *string `json:"kind"`
		Message *string `json:"message"`
	}
	if err := scopedArguments(args, &input); err != nil {
		return extension.Result{}, err
	}
	if input.Kind == nil || input.Message == nil {
		return extension.Result{}, fmt.Errorf("warning kind and message are required: %w", harness.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return extension.Result{}, err
	}
	a.warnings.reportWarning(a.id, *input.Kind, *input.Message)
	return extension.Result{Value: json.RawMessage("null")}, nil
}
