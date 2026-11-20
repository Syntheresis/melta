package melta

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/extension"
)

// hostCall is the host-issued identity a Melta service handler serves: the
// calling extension and, for Session work, its Session and Workspace.
type hostCall struct {
	ExtensionID string
	SessionID   string
	Workspace   string
}

// hostHandler serves one method of a Melta service.
type hostHandler func(context.Context, hostCall, json.RawMessage) (extension.Result, error)
