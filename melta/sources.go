package melta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/atomicfs"
	"github.com/Syntheresis/melta/internal/config"
)

// errConfigurationChanged reports a write that found its configuration
// document missing or holding bytes other than the ones its caller read.
var errConfigurationChanged = fmt.Errorf("configuration document changed since it was read: %w", harness.ErrConflict)

// ConfigurationSource supplies and replaces one complete configuration
// document. Read reports a missing document as an error wrapping
// fs.ErrNotExist. Write replaces the document with data and takes the bytes
// its caller read as previous: when it finds the document missing or holding
// other bytes, it fails with an error wrapping harness.ErrConflict. Any failed
// Write leaves the document as it was. A source whose document other programs
// can write without a lock it shares compares as late as it can; a change
// after its comparison is replaced.
type ConfigurationSource interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, previous, data []byte) error
}

// CredentialSource supplies credential observations and managed updates
// without mutating the process environment. Capture is the one observation
// each configuration build publishes; Set and Remove own only the selected
// managed location and refuse externally defined names; SubprocessEnv
// returns the environment a child process may receive with excluded names
// removed.
type CredentialSource interface {
	Capture(ctx context.Context, names []string) (map[string]CredentialValue, error)
	Set(ctx context.Context, name, value string) error
	Remove(ctx context.Context, name string) error
	SubprocessEnv(ctx context.Context, excluded []string) ([]string, error)
}

// CredentialValue is one source-coherent credential observation.
type CredentialValue = config.CredentialValue

// Locations are the independently optional Melta locations. An omitted
// location performs no lookup there.
type Locations struct {
	GlobalRulesDir    string
	DiscoveryCacheDir string
	WireDebugDir      string
}

// fileConfiguration is one explicit-path configuration source.
type fileConfiguration struct {
	path string
}

// NewFileConfigurationSource returns a configuration source over the explicit
// path. Reads never create the document. A write replaces it atomically only
// when it holds the bytes its caller read; the file is compared immediately
// before the rename, so a change that lands between the two is still replaced.
func NewFileConfigurationSource(path string) ConfigurationSource {
	return &fileConfiguration{path: path}
}

func (s *fileConfiguration) Read(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(s.path) // #nosec G703 -- the path is the host's explicitly selected configuration location.
}

func (s *fileConfiguration) Write(_ context.Context, previous, data []byte) error {
	err := atomicfs.WriteIfUnchanged(s.path, previous, data, 0o600)
	if errors.Is(err, atomicfs.ErrChanged) {
		return errConfigurationChanged
	}
	return err
}

// valueConfiguration is the locked value-backed configuration source: one
// supplied document in memory, replaced atomically under its own lock.
type valueConfiguration struct {
	mu   sync.Mutex
	data []byte
}

// NewValueConfigurationSource returns a configuration source backed by the
// supplied bytes. A write replaces the held bytes only while it still holds
// the bytes its caller read; readers receive an owned copy.
func NewValueConfigurationSource(data []byte) ConfigurationSource {
	return &valueConfiguration{data: append([]byte(nil), data...)}
}

func (s *valueConfiguration) Read(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...), nil
}

func (s *valueConfiguration) Write(_ context.Context, previous, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !bytes.Equal(s.data, previous) {
		return errConfigurationChanged
	}
	s.data = append([]byte(nil), data...)
	return nil
}

// environmentCredentials is one explicit dotenv file backed by the process
// environment for shell precedence and child-environment projection.
type environmentCredentials struct {
	env *config.ManagedEnv
}

// NewEnvironmentCredentialSource returns a credential source over the
// explicit dotenv path. Capture reads the file on every call; the process
// environment wins and stays unmanaged. Set and Remove update only the file,
// refusing externally defined names.
func NewEnvironmentCredentialSource(path string) CredentialSource {
	return &environmentCredentials{env: config.NewManagedEnv(path)}
}

func (s *environmentCredentials) Capture(ctx context.Context, names []string) (map[string]CredentialValue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.env.Capture(names)
}

func (s *environmentCredentials) Set(_ context.Context, name, value string) error {
	return s.env.TrySet(name, value)
}

func (s *environmentCredentials) Remove(_ context.Context, name string) error {
	return s.env.TryRemove(name)
}

func (s *environmentCredentials) SubprocessEnv(ctx context.Context, excluded []string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.env.SubprocessEnv(excluded)
}
