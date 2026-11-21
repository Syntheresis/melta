package melta

// This file owns the fixed melta.data host service: the host-assigned
// Session and cache directories for one open extension callback, and the
// durable external-data claim. Session access runs under the enclosing
// callback tree's Session artifact lease; cache access belongs only to the
// extension component's own lifetime. Guest routing, paths and owner
// identities are never inputs.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/internal/safefs"
)

// extensionDataHash is the one path component derived from a validated
// extension identity; the identity itself is never a guest-controlled path.
func extensionDataHash(extensionID string) string {
	sum := sha256.Sum256([]byte(extensionID))
	return hex.EncodeToString(sum[:])
}

// sessionExtensionDir is the fixed owned Session data layout.
func sessionExtensionDir(dataDir, sessionID, extensionID string) string {
	return filepath.Join(dataDir, "code", sessionID, "extensions", extensionDataHash(extensionID))
}

// extensionCacheDir is the fixed reusable cache layout, outside every Session
// tree and code package.
func extensionCacheDir(dataDir, extensionID string) string {
	return filepath.Join(dataDir, "extensions", extensionDataHash(extensionID), "cache")
}

// dataHandler resolves one fixed melta.data method to its handler. The
// claim_external_data method is served through the calling activation's
// registered cleanup attachments, not from this table.
func (c *composition) dataHandler(method string) (hostHandler, bool) {
	switch method {
	case "session_namespace":
		return c.sessionNamespaceHandler, true
	case "cache_namespace":
		return c.cacheNamespaceHandler, true
	}
	return nil, false
}

// dataHost returns the shared host data owner, or the closed refusal when the
// owner is not armed.
func (c *composition) dataHost() (*hostServices, error) {
	if c.hosts == nil || c.hosts.dataDir == "" {
		return nil, fmt.Errorf("extension data host is not armed: %w", ErrClosed)
	}
	return c.hosts, nil
}

// sessionNamespaceHandler assigns the calling extension's owned directory for
// the host-issued Session identity. It rechecks the Session under the same
// callback-owned artifact interval and refuses a planted symlink instead of
// creating or returning an outside path.
func (c *composition) sessionNamespaceHandler(ctx context.Context, call hostCall, _ json.RawMessage) (extension.Result, error) {
	host, err := c.dataHost()
	if err != nil {
		return extension.Result{}, err
	}
	if host.harness == nil || call.SessionID == "" || call.ExtensionID == "" {
		return extension.Result{}, fmt.Errorf("session namespace requires a host-issued Session and extension identity: %w", ErrClosed)
	}
	lease := artifactLeaseFromContext(ctx)
	if lease == nil {
		return extension.Result{}, fmt.Errorf("session namespace has no call-owned artifact lease: %w", ErrClosed)
	}
	if err := lease.acquire(); err != nil {
		return extension.Result{}, err
	}
	if _, err := host.harness.ReadSessionHeader(ctx, call.SessionID); err != nil {
		return extension.Result{}, err
	}
	directory := sessionExtensionDir(host.dataDir, call.SessionID, call.ExtensionID)
	if err := safefs.EnsureDirectory(directory, 0o700); err != nil {
		return extension.Result{}, err
	}
	value, err := json.Marshal(struct {
		Directory string `json:"directory"`
	}{Directory: directory})
	if err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: value}, nil
}

// cacheNamespaceHandler assigns the calling extension's reusable cache
// directory. It reads no Session and takes no Session artifact interval.
func (c *composition) cacheNamespaceHandler(_ context.Context, call hostCall, _ json.RawMessage) (extension.Result, error) {
	host, err := c.dataHost()
	if err != nil {
		return extension.Result{}, err
	}
	if call.ExtensionID == "" {
		return extension.Result{}, fmt.Errorf("cache namespace requires a host-issued extension identity: %w", ErrClosed)
	}
	directory := extensionCacheDir(host.dataDir, call.ExtensionID)
	if err := safefs.EnsureDirectory(directory, 0o700); err != nil {
		return extension.Result{}, err
	}
	value, err := json.Marshal(struct {
		Directory string `json:"directory"`
	}{Directory: directory})
	if err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: value}, nil
}

// claimExternalData durably records the calling extension as an owner of
// external data for the host-issued Session before that data exists. The
// claim requires the extension's registered cleanup attachments and keeps the
// call-owned Session interval through the enclosing callback's external
// creation and terminal response.
func (c *composition) claimExternalData(ctx context.Context, call hostCall, cleanup bool) (extension.Result, error) {
	host, err := c.dataHost()
	if err != nil {
		return extension.Result{}, err
	}
	if host.harness == nil || call.SessionID == "" || call.ExtensionID == "" {
		return extension.Result{}, fmt.Errorf("external data claim requires a host-issued Session and extension identity: %w", ErrClosed)
	}
	if !cleanup {
		return extension.Result{}, fmt.Errorf("extension %q registers no cleanup: %w", call.ExtensionID, ErrClosed)
	}
	lease := artifactLeaseFromContext(ctx)
	if lease == nil {
		return extension.Result{}, fmt.Errorf("external data claim has no call-owned artifact lease: %w", ErrClosed)
	}
	if err := lease.acquire(); err != nil {
		return extension.Result{}, err
	}
	if err := host.harness.ClaimExternalData(ctx, call.SessionID, call.ExtensionID); err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: json.RawMessage("null")}, nil
}
