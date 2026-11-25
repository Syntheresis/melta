package melta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
)

// Error identities reported by host composition.
var (
	// ErrComposition wraps invalid attribution and host misuse.
	ErrComposition = errors.New("melta: composition failure")

	// ErrClosed reports a closed or owner-canceled owner: no binding resolves
	// from an unavailable supplying owner.
	ErrClosed = errors.New("melta: closed")
)

// composition is the owner-wide host state: the constructed standard tool
// values, the neutral plugin-warning sink, and the fixed host named services.
// The data handler reads the owner Harness and Session artifact registry from
// the same shared value.
type composition struct {
	standardValues map[string]standardTool
	reportWarning  func(pluginID, kind, message string)
	hosts          *hostServices
}

type standardTool struct {
	tool     Tool
	validate func(json.RawMessage) error
}

type hostServices struct {
	owner      *Melta
	background *backgroundBridge
	harness    *harness.Harness
	artifacts  *artifactIntervals
	dataDir    string
}

// newComposition builds the one owner-wide composition.
func newComposition() *composition {
	return &composition{
		standardValues: make(map[string]standardTool),
		hosts:          &hostServices{},
	}
}

// nativeCloseDeadline is the fresh cooperative bound of one native close
// callback. It is a cancellation opportunity, not forced termination: trusted
// native code that ignores its context can still block cleanup.
var nativeCloseDeadline = 5 * time.Second

// declaredCredentialHandler resolves one declared credential field from the
// callback context's captured secret values. A field the extension did not
// declare is a misuse; every other absent, unresolved or undefined value
// fails uniformly as unavailable.
func declaredCredentialHandler(ctx context.Context, call hostCall, field string, secrets []extension.SecretRef) (extension.Result, error) {
	var declared *extension.SecretRef
	for i := range secrets {
		if secrets[i].Field == field {
			declared = &secrets[i]
			break
		}
	}
	if declared == nil {
		return extension.Result{}, fmt.Errorf("credential field %q is not declared: %w", field, ErrComposition)
	}
	values := secretValuesFrom(ctx)
	var value string
	var found bool
	if declared.Lifetime == extension.SecretActivation {
		value, found = values.activation[field]
	} else {
		perExtension := values.operation[call.ExtensionID]
		value, found = perExtension[field]
	}
	if !found || value == "" {
		return extension.Result{}, fmt.Errorf("credential %q is unavailable: %w", field, ErrClosed)
	}
	raw, err := json.Marshal(struct {
		Value string `json:"value"`
	}{Value: value})
	if err != nil {
		return extension.Result{}, err
	}
	return extension.Result{Value: raw}, nil
}

// newInstanceID mints one host-issued opaque instance identity.
func newInstanceID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
