// Package melta composes the inactive managed Melta around the public
// Harness. This file owns the private immutable configuration snapshot: one
// owned input set decoded from captured configuration bytes, consumed once
// and never reread, with no exported configuration, catalog, document-editing
// or warning API.
package melta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/internal/catalog"
	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/jobs"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// configuration is the immutable snapshot of one effective input set: the
// assembled catalog with its build warnings and each model's source label,
// the session policy, every enabled extension's owned raw configuration
// section, and the owned decoded provider user layer this same build
// consumed. Input documents are discarded once the snapshot is built.
type configuration struct {
	// model is the file choice; baselineModel is the one default model used
	// for admission and fallback.
	model            model.ModelRef
	baselineModel    model.ModelRef
	generation       uint64
	resources        *resourceSet
	native           *nativeCandidate
	composition      *composition
	catalog          *catalog.Catalog
	catalogWarnings  []catalog.Warning
	sessions         config.SessionConfig
	tools            json.RawMessage
	jobs             json.RawMessage
	compaction       json.RawMessage
	compactionValues compactionSettings
	extensions       map[string]json.RawMessage
	secrets          []declaredSecret
	declaredBindings map[declaredField]declaredBinding
	credentials      map[string]CredentialValue
	credentialNames  []string
	userProviders    map[string]any
}

// capturedConfigDocument is the private single-pass decode of one captured
// main configuration document: the catalog, sessions, the top-level baseline
// tools/jobs/compaction objects, and the plugin sections are consumed while
// unknown members stay unowned, numbers stay exact through the UseNumber
// decoder, and no legacy whole-document shape is enforced.
type capturedConfigDocument struct {
	Model      *string                    `json:"model"`
	Providers  map[string]any             `json:"providers"`
	Sessions   json.RawMessage            `json:"sessions"`
	Tools      json.RawMessage            `json:"tools"`
	Jobs       json.RawMessage            `json:"jobs"`
	Compaction json.RawMessage            `json:"compaction"`
	Extensions map[string]json.RawMessage `json:"extensions"`
}

// declaredSecret is one active extension SecretRef resolved to the non-secret
// credential name its section field holds. Only the reference metadata is
// captured here; values live in the private activation/Operation resources
// that resolve them.
type declaredSecret struct {
	extensionID string
	field       string
	lifetime    extension.SecretLifetime
	name        string
}

// declaredField identifies one active SecretRef field in its extension.
type declaredField struct {
	extensionID string
	field       string
}

// declaredBinding is one candidate's private capture of an active declared
// field: the resolved non-secret credential name and the value captured from
// the one candidate environment observation. It is never durable.
type declaredBinding struct {
	name  string
	value string
}

// declaredSecretNames returns the unique non-secret credential names every
// active declaration references. The live producer unions them with the
// current provider names; the preparation's one credential observation
// resolves the same names for its captured values and exclusions.
func (c *configuration) declaredSecretNames() []string {
	if c == nil || len(c.secrets) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(c.secrets))
	names := make([]string, 0, len(c.secrets))
	for _, secret := range c.secrets {
		if seen[secret.name] {
			continue
		}
		seen[secret.name] = true
		names = append(names, secret.name)
	}
	return names
}

// resolveAuthorSecrets inspects the SecretRef metadata of every active author
// extension against its captured section and records the non-secret credential
// names without executing extension code.
func resolveAuthorSecrets(authors []nativeActivationSpec, sections map[string]json.RawMessage) []declaredSecret {
	var out []declaredSecret
	for _, spec := range authors {
		manifest := spec.extension.Manifest
		section := sections[manifest.ID]
		if !sectionEnabled(section) {
			continue
		}
		for _, secret := range manifest.Secrets {
			if name, ok := extension.SecretName(section, secret.Field); ok {
				out = append(out, declaredSecret{extensionID: manifest.ID, field: secret.Field, lifetime: secret.Lifetime, name: name})
			}
		}
	}
	return out
}

// decodeCapturedConfig decodes one captured main configuration document in a
// single pass: the catalog, sessions, baseline and plugin sections are
// consumed, numbers stay exact through the UseNumber decoder, and no legacy
// whole-document shape is enforced.
func decodeCapturedConfig(configData []byte) (capturedConfigDocument, error) {
	var doc capturedConfigDocument
	decoder := json.NewDecoder(bytes.NewReader(configData))
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		return doc, fmt.Errorf("decode captured configuration: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return doc, fmt.Errorf("decode captured configuration: unexpected trailing JSON value")
		}
		return doc, fmt.Errorf("decode captured configuration: %w", err)
	}
	return doc, nil
}

// newConfiguration assembles the complete snapshot from one decoded captured
// document and the provider assembly already performed for the captured
// providers layer (a catalog entry never rereads the main configuration).
// Nothing here parses Agent definitions: the Agents activation owns its
// definition section and supplies the current roster through its registered
// operations and admission hook. Plugin-section validation belongs to the
// publisher, not here.
func newConfiguration(generation uint64, doc capturedConfigDocument, built catalog.BuildResult) (*configuration, error) {
	ref, err := config.ParseOptionalModel(doc.Model)
	if err != nil {
		return nil, fmt.Errorf("captured configuration: %w", err)
	}
	sessions, err := config.ParseSessions(doc.Sessions)
	if err != nil {
		return nil, fmt.Errorf("captured configuration sessions: %w", err)
	}
	if err := validateBaselineSection("tools", doc.Tools, validateToolsSettings); err != nil {
		return nil, fmt.Errorf("captured configuration tools: %w", err)
	}
	if err := validateBaselineSection("jobs", doc.Jobs, jobs.ValidateSettings); err != nil {
		return nil, fmt.Errorf("captured configuration jobs: %w", err)
	}
	compaction, err := parseCompactionSettings(doc.Compaction)
	if err != nil {
		return nil, fmt.Errorf("captured configuration compaction: %w", err)
	}
	return &configuration{
		model:            ref,
		generation:       generation,
		catalog:          built.Catalog,
		catalogWarnings:  built.Warnings,
		sessions:         sessions,
		tools:            doc.Tools,
		jobs:             doc.Jobs,
		compaction:       doc.Compaction,
		compactionValues: compaction,
		extensions:       doc.Extensions,
		userProviders:    doc.Providers,
	}, nil
}

// projectSettings projects the file model, session policy and owned sections
// from one revision. Absent baseline sections project as empty objects;
// extension sections remain opaque document strings with exact numeric lexemes.
func projectSettings(snapshot *configuration) protocol.Settings {
	out := protocol.Settings{
		Sessions: protocol.SessionsSettings{
			AutoArchive:            snapshot.sessions.AutoArchive,
			ArchiveAfterDays:       snapshot.sessions.ArchiveAfterDays,
			DeleteAfterArchiveDays: snapshot.sessions.DeleteAfterArchiveDays,
		},
		Tools:      projectSettingsObject(snapshot.tools),
		Jobs:       projectSettingsObject(snapshot.jobs),
		Compaction: projectSettingsObject(snapshot.compaction),
		Extensions: make(protocol.ExtensionsSettings, len(snapshot.extensions)),
	}
	if !snapshot.model.IsZero() {
		value := protocol.ModelRef(snapshot.model.String())
		out.Model = &value
	}
	for id, raw := range snapshot.extensions {
		out.Extensions[id] = string(raw)
	}
	return out
}

// projectSettingsObject returns one owned copy of a top-level baseline
// section; an absent section is the empty object its consuming service
// defaults from.
func projectSettingsObject(raw json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage("{}")
	}
	return append(json.RawMessage(nil), raw...)
}
