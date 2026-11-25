package melta

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/protocol"
)

// publishNativeCandidate publishes one test-built native candidate and
// attaches it to the published configuration snapshot, the pair a production
// build commits together.
func publishNativeCandidate(t *testing.T, r *Melta, candidate *nativeCandidate) {
	t.Helper()
	if err := candidate.publishForTest(); err != nil {
		t.Fatal(err)
	}
	r.config.nativeMu.Lock()
	if snapshot := r.config.current(); snapshot != nil {
		snapshot.native = candidate
	}
	r.config.nativeMu.Unlock()
}

func TestScopedServiceReadVersusForkReceiptAfterDeletion(t *testing.T) {
	r, env := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	ctx := context.Background()
	workspace := filepath.Join(env.home, "operation-workspace")
	session := projectionSession(t, r, workspace, "solo").Identity.SessionID
	submitCodeOperation(t, r, session, "source", "hello")
	boundary := firstItemID(t, r, session)
	readEntered, forkEntered, fakeEntered, release := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	manifest := registrarManifest("operation")
	manifest.Services = []string{extension.ContractCommands}
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterService(extension.Service{Name: "relay", Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "fork", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				return call.Call(ctx, extension.ContractCommands, "fork", args)
			},
		}, {
			Method: extension.Method{Name: "fake", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: []byte(`{"session":{"session_id":"fake"}}`)}, nil
			},
		}}}, nil); err != nil {
			return nil, err
		}
		read := extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "read", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				close(readEntered)
				<-release
				return extension.Result{Value: []byte(`{"read":true}`)}, nil
			},
		}
		if _, err := api.RegisterOperation(read, nil); err != nil {
			return nil, err
		}
		fake := extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "fake", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				result, err := call.Call(ctx, "relay", "fake", args)
				close(fakeEntered)
				<-release
				return result, err
			},
		}
		if _, err := api.RegisterOperation(fake, nil); err != nil {
			return nil, err
		}
		fork := extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "fork", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				result, err := call.Call(ctx, "relay", "fork", args)
				close(forkEntered)
				<-release
				return result, err
			},
		}
		_, err := api.RegisterOperation(fork, nil)
		return nil, err
	}}
	candidate, err := r.config.prepareNativeCandidate(ctx, []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	publishNativeCandidate(t, r, candidate)
	readDone, forkDone, fakeDone := make(chan extensionOutcome, 1), make(chan extensionOutcome, 1), make(chan extensionOutcome, 1)
	go func() {
		result, err := r.invokeRegisteredOperation(ctx, testCaller(r), "operation", "read", sessionTarget(session), []byte(`{}`), "")
		readDone <- extensionOutcome{result: result, err: err}
	}()
	<-readEntered
	go func() {
		result, err := r.invokeRegisteredOperation(ctx, testCaller(r), "operation", "fake", sessionTarget(session), []byte(`{}`), "")
		fakeDone <- extensionOutcome{result: result, err: err}
	}()
	<-fakeEntered
	args, _ := json.Marshal(map[string]any{"operation_id": "fork-op", "boundary_item_id": boundary, "content": []map[string]string{{"kind": "text", "text": "fork"}}})
	go func() {
		result, err := r.invokeRegisteredOperation(ctx, testCaller(r), "operation", "fork", sessionTarget(session), args, "")
		forkDone <- extensionOutcome{result: result, err: err}
	}()
	<-forkEntered
	if _, err := r.ArchiveSession(ctx, testCaller(r), session); err != nil {
		t.Fatal(err)
	}
	if _, err := r.DeleteSession(ctx, testCaller(r), session); err != nil {
		t.Fatal(err)
	}
	close(release)
	read := <-readDone
	if read.err != nil || string(read.result.Value) != `{"read":true}` {
		t.Fatalf("read after deletion: %+v", read)
	}
	fake := <-fakeDone
	if fake.err != nil || string(fake.result.Value) != `{"session":{"session_id":"fake"}}` {
		t.Fatalf("handler value after deletion: %+v", fake)
	}
	fork := <-forkDone
	if fork.err != nil || len(fork.result.Value) == 0 || fork.result.Failure != nil {
		t.Fatalf("fork receipt after deletion: %+v", fork)
	}
	var receipt protocol.ForkResult
	if err := json.Unmarshal(fork.result.Value, &receipt); err != nil || receipt.Session.SessionId == "" || receipt.Operation.OperationId != "fork-op" {
		t.Fatalf("invalid fork receipt: %+v %v", receipt, err)
	}
	if _, err := r.invokeRegisteredOperation(ctx, testCaller(r), "operation", "read", sessionTarget(session), []byte(`{}`), "req-after-delete"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("operation started on a deleted Session = %v, want not-found", err)
	}
}

func TestScopedServiceBufferedSubmitReceiptSurvivesCancellation(t *testing.T) {
	r, env := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	ctx := context.Background()
	session := projectionSession(t, r, filepath.Join(env.home, "buffered-workspace"), "solo").Identity.SessionID
	gate := make(chan struct{})
	releaseModel := sync.OnceFunc(func() { close(gate) })
	defer releaseModel()
	env.server.setHold(gate)
	if _, err := r.SubmitSession(ctx, testCaller(r), session, commandSubmitRequest(t, "active", "active")); err != nil {
		t.Fatal(err)
	}
	awaitModelArrival(t, env)
	entered, resume := make(chan struct{}), make(chan struct{})
	successEntered, successResume := make(chan struct{}), make(chan struct{})
	manifest := registrarManifest("submitter")
	manifest.Services = []string{extension.ContractCommands}
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "submit", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				result, err := call.Call(ctx, extension.ContractCommands, "submit", args)
				close(entered)
				<-resume
				if err == nil && ctx.Err() != nil {
					return extension.Result{}, ctx.Err()
				}
				return result, err
			},
		}, nil)
		if err != nil {
			return nil, err
		}
		_, err = api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "submit-success", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				result, err := call.Call(ctx, extension.ContractCommands, "submit", args)
				close(successEntered)
				<-successResume
				return result, err
			},
		}, nil)
		if err != nil {
			return nil, err
		}
		_, err = api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetSession,
			Method: extension.Method{Name: "partial", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				if _, err := call.Call(ctx, extension.ContractCommands, "submit", args); err != nil {
					return extension.Result{}, err
				}
				return extension.Result{}, &extension.CallError{Err: harness.ErrStorage, Partial: []byte(`{"declared":true}`)}
			},
		}, nil)
		return nil, err
	}}
	candidate, err := r.config.prepareNativeCandidate(ctx, []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	publishNativeCandidate(t, r, candidate)
	args, err := json.Marshal(commandSubmitRequest(t, "buffered", "buffered"))
	if err != nil {
		t.Fatal(err)
	}
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan extensionOutcome, 1)
	go func() {
		result, err := r.invokeRegisteredOperation(callCtx, testCaller(r), "submitter", "submit", sessionTarget(session), args, "")
		done <- extensionOutcome{result: result, err: err}
	}()
	<-entered
	cancel()
	close(resume)
	got := <-done
	var receipt protocol.SubmitResult
	var canceled *extension.CallError
	if !errors.Is(got.err, context.Canceled) || errors.As(got.err, &canceled) {
		t.Fatalf("buffered receipt: %+v, want the handler's cancellation without a partial", got)
	}
	partialArgs, err := json.Marshal(commandSubmitRequest(t, "partial-buffered", "partial"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.invokeRegisteredOperation(ctx, testCaller(r), "submitter", "partial", sessionTarget(session), partialArgs, "")
	var partial *extension.CallError
	if !errors.As(err, &partial) || string(partial.Partial) != `{"declared":true}` {
		t.Fatalf("declared partial lost after committed submit: %v", err)
	}
	successArgs, err := json.Marshal(commandSubmitRequest(t, "success-buffered", "success"))
	if err != nil {
		t.Fatal(err)
	}
	successCtx, successCancel := context.WithCancel(ctx)
	successDone := make(chan extensionOutcome, 1)
	go func() {
		result, err := r.invokeRegisteredOperation(successCtx, testCaller(r), "submitter", "submit-success", sessionTarget(session), successArgs, "")
		successDone <- extensionOutcome{result: result, err: err}
	}()
	<-successEntered
	successCancel()
	close(successResume)
	success := <-successDone
	if success.err != nil || json.Unmarshal(success.result.Value, &receipt) != nil || receipt.Disposition != protocol.SubmitResultDispositionSteering {
		t.Fatalf("outer success after cancellation: %+v", success)
	}
	releaseModel()
	awaitOperation(t, r, session, "active", harness.OperationSuccess)
	awaitOperation(t, r, session, "buffered", harness.OperationSuccess)
	awaitOperation(t, r, session, "partial-buffered", harness.OperationSuccess)
	awaitOperation(t, r, session, "success-buffered", harness.OperationSuccess)
}

func TestScopedServiceOperationSelfReplacementRetainsHandler(t *testing.T) {
	r, _ := openProjectionRuntime(t, storage.NewMemory())
	defer closeProjectionRuntime(r)
	var handle extension.Registration
	makeOperation := func(value string) extension.Operation {
		return extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "replace", Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: []byte(`"` + value + `"`)}, nil
			},
		}
	}
	e := extension.Extension{Manifest: registrarManifest("self"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		first := makeOperation("old")
		first.Handler = func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
			var err error
			handle, err = api.RegisterOperation(makeOperation("new"), handle)
			return extension.Result{Value: []byte(`"old"`)}, err
		}
		var err error
		handle, err = api.RegisterOperation(first, nil)
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
	target := protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}
	for _, want := range []string{`"old"`, `"new"`} {
		result, err := r.invokeRegisteredOperation(context.Background(), testCaller(r), "self", "replace", target, []byte(`{}`), "")
		if err != nil || string(result.Value) != want {
			t.Fatalf("operation = %s, %v; want %s", result.Value, err, want)
		}
	}
}
