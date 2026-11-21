package clientops

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/client"
	"github.com/Syntheresis/melta/protocol"
)

// CreateDefaultSession uses the advertised Agents operation when available.
func CreateDefaultSession(ctx context.Context, c *client.Client, workspace string) (string, error) {
	found, err := HasExtensionOperation(ctx, c, "agents", "create", protocol.RegisteredExtensionOperationTargetWorkspace)
	if err != nil {
		return "", err
	}
	if found {
		raw, err := InvokeExtension(ctx, c, "agents", "create", protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace}, json.RawMessage(`{}`))
		if err != nil {
			return "", err
		}
		var receipt struct {
			SessionID string `json:"session_id"`
		}
		if json.Unmarshal(raw, &receipt) != nil || receipt.SessionID == "" {
			return "", fmt.Errorf("invalid extension session creation receipt")
		}
		return receipt.SessionID, nil
	}
	params, err := json.Marshal(map[string]any{"body": protocol.CreateSessionRequest{Workspace: workspace}})
	if err != nil {
		return "", err
	}
	raw, err := Call(ctx, c, "CreateSession", params)
	if err != nil {
		return "", err
	}
	var session protocol.Session
	if err := json.Unmarshal(raw, &session); err != nil {
		return "", err
	}
	if session.SessionId == "" {
		return "", fmt.Errorf("invalid session creation receipt")
	}
	return session.SessionId, nil
}

// HasExtensionOperation checks the current registered-operation catalog.
func HasExtensionOperation(ctx context.Context, c *client.Client, extensionID, operation string, target protocol.RegisteredExtensionOperationTarget) (bool, error) {
	raw, err := Call(ctx, c, "ListExtensions", nil)
	if err != nil {
		return false, err
	}
	var extensions []protocol.RegisteredExtension
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false, fmt.Errorf("invalid extension catalog")
	}
	if err := protocol.DecodeValue(raw, &extensions); err != nil {
		return false, err
	}
	seen := make(map[string]bool, len(extensions))
	for _, ext := range extensions {
		if ext.ExtensionId == "" || seen[ext.ExtensionId] {
			return false, fmt.Errorf("invalid extension catalog identity")
		}
		seen[ext.ExtensionId] = true
	}
	for _, ext := range extensions {
		if ext.ExtensionId == extensionID && ext.InactiveReason == nil {
			for _, item := range ext.Operations {
				if item.Method.Name == operation && item.Target == target {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// InvokeExtension calls one registered operation through the shared envelope.
func InvokeExtension(ctx context.Context, c *client.Client, extensionID, operation string, target protocol.ExtensionInvocationTarget, arguments json.RawMessage) (json.RawMessage, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	request := protocol.ExtensionInvocationRequest{
		ExtensionId: extensionID, Operation: operation, RequestId: hex.EncodeToString(id[:]),
		Target: target, Arguments: &arguments,
	}
	params, err := json.Marshal(map[string]any{"body": request})
	if err != nil {
		return nil, err
	}
	raw, err := Call(ctx, c, "InvokeExtension", params)
	if err != nil {
		return nil, err
	}
	var response struct {
		Kind   string          `json:"kind"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	if response.Kind != "success" || response.Result == nil {
		return nil, fmt.Errorf("invalid extension operation receipt")
	}
	return append(json.RawMessage(nil), response.Result...), nil
}
