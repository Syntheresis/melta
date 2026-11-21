package melta

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/extension"
)

func (a *nativeActivation) reportWarning(ctx context.Context, method string, args json.RawMessage) (extension.Result, error) {
	if !a.services["melta.warnings"] {
		return extension.Result{}, ErrDenied
	}
	var input struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}
	if err := scopedArguments(args, &input); err != nil {
		return extension.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return extension.Result{}, err
	}
	a.warnings.reportWarning(a.id, input.Kind, input.Message)
	return extension.Result{Value: json.RawMessage("null")}, nil
}
