package melta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
)

func TestCleanupBatchRetainsOriginalRegisteredHandlers(t *testing.T) {
	r, _ := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	var handle extension.Registration
	var ids []string
	var borrowed func(context.Context, string, string, json.RawMessage) (extension.Result, error)
	e := extension.Extension{Manifest: registrarManifest("cleaner"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		var err error
		handle, err = api.RegisterCleanup(func(_ context.Context, call extension.AuthorContext, deleted harness.SessionIdentity) error {
			if call.Session.SessionID != "" {
				t.Error("cleanup acquired live session")
			}
			ids = append(ids, deleted.SessionID)
			borrowed = call.Call
			if len(ids) == 1 {
				return api.Remove(handle)
			}
			return nil
		}, nil)
		return nil, err
	}}
	candidate, err := r.config.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if err := candidate.publishForTest(); err != nil {
		t.Fatal(err)
	}
	failures := r.cleanupDeletedSessions(context.Background(), []deletedSession{
		{sessionID: "first", owners: []string{"cleaner"}},
		{sessionID: "second", owners: []string{"cleaner"}},
	})
	if len(failures) != 0 || !reflect.DeepEqual(ids, []string{"first", "second"}) {
		t.Fatalf("cleanup failures=%+v ids=%v", failures, ids)
	}
	if _, err := borrowed(context.Background(), "missing", "call", []byte(`{}`)); !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("cleanup callback remained live: %v", err)
	}
	failures = r.cleanupDeletedSessions(context.Background(), []deletedSession{{sessionID: "third", owners: []string{"cleaner"}}})
	if len(failures) != 1 || failures[0].Code != "unavailable" {
		t.Fatalf("removed cleanup: %+v", failures)
	}
}

func TestScopedServiceDataClaimRequiresRegisteredCleanup(t *testing.T) {
	r, env := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	session := projectionSession(t, r, filepath.Join(env.home, "cleanup-workspace"), "solo").Identity.SessionID
	var api extension.API
	var deleted string
	manifest := registrarManifest("cleaner")
	manifest.Services = []string{extension.ContractData}
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, a extension.API) (func(context.Context) error, error) {
		api = a
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "claim", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				return call.Call(ctx, extension.ContractData, "claim_external_data", []byte(`{}`))
			},
		}, nil)
		return nil, err
	}}
	candidate, err := r.config.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if err := candidate.publishForTest(); err != nil {
		t.Fatal(err)
	}
	invoke := func() error {
		_, err := r.invokeRegisteredOperation(context.Background(), testCaller(r), "cleaner", "claim", sessionTarget(session), []byte(`{}`), "")
		return err
	}
	if err := invoke(); err == nil {
		t.Fatal("external claim succeeded without cleanup")
	}
	if _, err := api.RegisterCleanup(func(_ context.Context, call extension.AuthorContext, identity harness.SessionIdentity) error {
		if call.Session.SessionID != "" {
			t.Error("cleanup acquired live session")
		}
		deleted = identity.SessionID
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := invoke(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ArchiveSession(context.Background(), testCaller(r), session); err != nil {
		t.Fatal(err)
	}
	result, err := r.DeleteSession(context.Background(), testCaller(r), session)
	if err != nil || len(result.CleanupFailures) != 0 || deleted != session {
		t.Fatalf("delete = %+v, %v; cleanup ID = %q", result, err, deleted)
	}
}

func TestCleanupBatchRunsAllAttachments(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			r, _ := openProjectionRuntime(t, storage.NewMemory())
			defer closeProjectionRuntime(r)
			var calls []int
			e := extension.Extension{Manifest: registrarManifest("cleaner"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
				for i := 0; i < 2; i++ {
					index := i
					_, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error {
						calls = append(calls, index)
						if index == 0 && fail {
							return errors.New("failed")
						}
						return nil
					}, nil)
					if err != nil {
						return nil, err
					}
				}
				return nil, nil
			}}
			candidate, err := r.config.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
			if err != nil {
				t.Fatal(err)
			}
			defer candidate.release()
			if err := candidate.publishForTest(); err != nil {
				t.Fatal(err)
			}
			failures := r.cleanupDeletedSessions(context.Background(), []deletedSession{{sessionID: "deleted", owners: []string{"cleaner"}}})
			if !reflect.DeepEqual(calls, []int{0, 1}) || len(failures) != map[bool]int{false: 0, true: 1}[fail] {
				t.Fatalf("calls=%v failures=%v", calls, failures)
			}
		})
	}
}
