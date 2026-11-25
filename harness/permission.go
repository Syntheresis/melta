package harness

import "encoding/json"

// PermissionFileRead, PermissionFileWrite, PermissionCommandRun,
// PermissionSessionOwn and PermissionProcessOwn are the
// published semantic permission names tools declare in their plans. They are
// exact strings; plugins may define new namespaced names.
const (
	PermissionFileRead   = "file.read"
	PermissionFileWrite  = "file.write"
	PermissionCommandRun = "command.run"
	PermissionSessionOwn = "session.own"
	PermissionProcessOwn = "process.own"
)

// PermissionRequest is one declared semantic permission/target pair of a
// prepared call. Target is one canonical string produced by the tool that
// understands it; target-independent operations use the fixed target "*".
type PermissionRequest struct {
	Permission string
	Target     string
}

// PreparedAction is one prepared call handed to an authorization hook.
// Arguments and permission pairs are independently owned.
type PreparedAction struct {
	ToolName           string
	Arguments          json.RawMessage
	CanonicalWorkspace string
	Permissions        []PermissionRequest
}
