package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

// servePipe drives one author in-process over the author protocol.
type servePipe struct {
	t      *testing.T
	writer *worker.Writer
	reader *worker.Reader
	in     *io.PipeWriter
	out    *io.PipeReader
	outW   *io.PipeWriter
	done   chan error
	frames chan authorFrame
	closed chan struct{}
}

type authorFrame struct {
	frame worker.Frame
	err   error
}

func startServe(t *testing.T, author extension.Extension) *servePipe {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	p := &servePipe{t: t, writer: worker.NewWriter(inW), reader: worker.NewReader(outR), in: inW, out: outR, outW: outW, done: make(chan error, 1), frames: make(chan authorFrame, 64), closed: make(chan struct{})}
	go func() { p.done <- worker.Serve(context.Background(), author, inR, outW) }()
	go func() {
		for {
			frame, err := p.reader.ReadFrame()
			select {
			case p.frames <- authorFrame{frame: frame, err: err}:
			case <-p.closed:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(p.closed)
		_ = inW.Close()
		_ = outR.Close()
		_ = outW.Close()
	})
	return p
}

func (p *servePipe) send(frame worker.Frame) {
	p.t.Helper()
	if err := p.writer.WriteFrame(frame); err != nil {
		p.t.Fatalf("write frame: %v", err)
	}
}

func (p *servePipe) read() worker.Frame {
	p.t.Helper()
	select {
	case result := <-p.frames:
		if result.err != nil {
			p.t.Fatalf("read frame: %v", result.err)
		}
		return result.frame
	case <-time.After(5 * time.Second):
		p.t.Fatal("timed out waiting for a worker frame")
		return worker.Frame{}
	}
}

func (p *servePipe) hello(version string) worker.Frame {
	p.t.Helper()
	params, _ := json.Marshal(worker.HelloParams{APIVersion: version})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "hello", Method: worker.MethodHello, Params: params})
	return p.read()
}

func (p *servePipe) activate() []worker.AuthorRegisterParams {
	p.t.Helper()
	params, _ := json.Marshal(worker.AuthorActivateParams{Section: worker.JSONBytes(`{}`), Token: "entry"})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "activate", Method: worker.MethodActivate, Params: params})
	var registered []worker.AuthorRegisterParams
	for {
		frame := p.read()
		if frame.Method == "register" {
			var params worker.AuthorRegisterParams
			if err := json.Unmarshal(frame.Params, &params); err != nil {
				p.t.Fatalf("register params: %v", err)
			}
			registered = append(registered, params)
			p.send(worker.Frame{Kind: worker.KindResponse, ID: frame.ID, Result: json.RawMessage("null")})
			continue
		}
		if frame.ID == "activate" {
			if frame.Error != nil || string(frame.Result) != "null" {
				p.t.Fatalf("activation = %+v", frame)
			}
			return registered
		}
		p.t.Fatalf("unexpected activation frame: %+v", frame)
	}
}

// call sends one author call and returns its response, failing on a hostcall.
func (p *servePipe) call(id, handle, method string, arguments []byte, callContext worker.AuthorCallContext) worker.Frame {
	p.t.Helper()
	params, _ := json.Marshal(worker.AuthorCallParams{Handle: handle, Method: method, Context: callContext, Arguments: worker.JSONBytes(arguments)})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: id, Method: worker.MethodCall, Params: params})
	for {
		frame := p.read()
		if frame.Method == worker.MethodHostcall {
			p.t.Fatalf("unexpected hostcall while waiting for %q: %+v", id, frame)
		}
		if frame.ID == id {
			return frame
		}
		p.t.Fatalf("unexpected frame: %+v", frame)
	}
}

func (p *servePipe) closeInput() {
	_ = p.in.Close()
}

func (p *servePipe) wait() error {
	p.t.Helper()
	select {
	case err := <-p.done:
		return err
	case <-time.After(5 * time.Second):
		p.t.Fatal("serve did not stop")
		return nil
	}
}

func manifest() extension.Manifest {
	return extension.Manifest{ID: "author", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
}

func TestAuthorServeToolCheck(t *testing.T) {
	var seen model.ToolCall
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.AttachToolCheck(func(_ context.Context, _ extension.AuthorContext, call model.ToolCall) (*string, error) {
			seen = call
			reason := "restricted"
			return &reason, nil
		}, nil)
		return nil, err
	}})
	if reply := p.hello(extension.APIVersion); reply.Error != nil {
		t.Fatalf("hello = %+v", reply)
	}
	registered := p.activate()
	if len(registered) != 1 || registered[0].Kind != "check" || string(registered[0].Spec) != "{}" {
		t.Fatalf("registrations = %+v", registered)
	}
	input := model.ToolCall{ID: "call-1", Name: "echo", Arguments: json.RawMessage(`{"x":1}`)}
	raw := json.RawMessage(`{"id":"call-1","name":"echo","arguments":"eyJ4IjoxfQ=="}`)
	reply := p.call("check", registered[0].Handle, "check", raw, worker.AuthorCallContext{Token: "t"})
	if reply.Error != nil {
		t.Fatalf("check = %+v", reply)
	}
	reason, err := worker.DecodeAuthorRefusal(reply.Result)
	if err != nil || reason == nil || *reason != "restricted" || seen.ID != input.ID || seen.Name != input.Name || !bytes.Equal(seen.Arguments, input.Arguments) {
		t.Fatalf("reason = %v, call = %+v, err = %v", reason, seen, err)
	}
}

func callParams(t *testing.T, handle, method string) json.RawMessage {
	t.Helper()
	params, err := json.Marshal(worker.AuthorCallParams{Handle: handle, Method: method, Context: worker.AuthorCallContext{Token: "t"}, Arguments: worker.JSONBytes(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatal(err)
	}
	return params
}

// TestAuthorServeHandshakeFailures proves the first frame must be the one
// accepted hello: another method, an incompatible version and malformed hello
// params each end the serve loop.
func TestAuthorServeHandshakeFailures(t *testing.T) {
	t.Run("first frame is not hello", func(t *testing.T) {
		var called atomic.Bool
		p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
			called.Store(true)
			return nil, nil
		}})
		params, _ := json.Marshal(worker.AuthorActivateParams{Section: worker.JSONBytes(`{}`), Token: "entry"})
		p.send(worker.Frame{Kind: worker.KindRequest, ID: "x", Method: worker.MethodActivate, Params: params})
		if err := p.wait(); err == nil {
			t.Fatal("non-hello first frame was accepted")
		}
		if called.Load() {
			t.Fatal("non-hello first frame activated the extension")
		}
	})
	t.Run("incompatible version", func(t *testing.T) {
		p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
		reply := p.hello("5")
		if reply.Error == nil || reply.Error.Class != worker.ClassIncompatible {
			t.Fatalf("hello reply = %+v, want incompatible", reply)
		}
		if err := p.wait(); err == nil || !strings.Contains(err.Error(), "incompatible") {
			t.Fatalf("serve = %v, want the incompatible failure", err)
		}
	})
	t.Run("malformed hello params", func(t *testing.T) {
		p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
		p.send(worker.Frame{Kind: worker.KindRequest, ID: "hello", Method: worker.MethodHello, Params: json.RawMessage(`"x"`)})
		reply := p.read()
		if reply.Error == nil || reply.Error.Class != worker.ClassIncompatible || !strings.Contains(reply.Error.Message, "unmarshal") || strings.Contains(reply.Error.Message, "incompatible protocol or API version") {
			t.Fatalf("malformed hello reply = %+v, want the malformed-params incompatible error", reply)
		}
		if err := p.wait(); err == nil {
			t.Fatal("malformed hello did not end serve")
		}
	})
}

// TestAuthorServeBadRouting proves an unknown method and an unknown author
// handle each end the loop after activation.
func TestAuthorServeBadRouting(t *testing.T) {
	t.Run("unknown method", func(t *testing.T) {
		p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
		p.hello(extension.APIVersion)
		p.activate()
		p.send(worker.Frame{Kind: worker.KindRequest, ID: "bad", Method: "nope", Params: json.RawMessage(`{}`)})
		if err := p.wait(); err == nil {
			t.Fatal("unknown method did not end serve")
		}
	})
	t.Run("unknown handle", func(t *testing.T) {
		p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
		p.hello(extension.APIVersion)
		p.activate()
		p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: callParams(t, "missing", "cleanup")})
		if err := p.wait(); err == nil {
			t.Fatal("unknown handle did not end serve")
		}
	})
}

// TestAuthorServeCancellationAndLateCancel proves an in-flight callback
// observes cancellation and reports the cancelled class, a late cancel is a
// no-op, and the loop keeps serving.
func TestAuthorServeCancellationAndLateCancel(t *testing.T) {
	var calls atomic.Int64
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterCleanup(func(ctx context.Context, _ extension.AuthorContext, _ harness.SessionIdentity) error {
			if calls.Add(1) == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	if len(registered) != 1 {
		t.Fatalf("registrations = %d, want one", len(registered))
	}
	handle := registered[0].Handle
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: callParams(t, handle, "cleanup")})
	p.send(worker.Frame{Kind: worker.KindCancel, ID: "call"})
	reply := p.read()
	if reply.ID != "call" || reply.Error == nil || reply.Error.Class != worker.ClassCancelled {
		t.Fatalf("cancelled reply = %+v", reply)
	}
	p.send(worker.Frame{Kind: worker.KindCancel, ID: "call"})
	if reply := p.call("after", handle, "cleanup", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"}); reply.Error != nil {
		t.Fatalf("late cancel broke the next call: %+v", reply)
	}
}

// TestAuthorServeConcurrentCallbacks proves callbacks run concurrently: both
// enter before either completes.
func TestAuthorServeConcurrentCallbacks(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		for _, name := range []string{"first", "second"} {
			name := name
			if _, err := api.RegisterCleanup(func(_ context.Context, _ extension.AuthorContext, _ harness.SessionIdentity) error {
				entered <- name
				<-release
				return nil
			}, nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "one", Method: worker.MethodCall, Params: callParams(t, registered[0].Handle, "cleanup")})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "two", Method: worker.MethodCall, Params: callParams(t, registered[1].Handle, "cleanup")})
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case name := <-entered:
			seen[name] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("callbacks did not both enter: %v", seen)
		}
	}
	close(release)
	replies := map[string]bool{}
	for len(replies) < 2 {
		reply := p.read()
		if reply.Error != nil {
			t.Fatalf("reply = %+v", reply)
		}
		replies[reply.ID] = true
	}
}

// TestAuthorServeOversizedResultIsCallLocal proves an over-limit callback
// result is replaced by a small internal error while the loop serves later
// calls.
func TestAuthorServeOversizedResultIsCallLocal(t *testing.T) {
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "huge", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: json.RawMessage(`"` + strings.Repeat("x", worker.FrameLimit) + `"`)}, nil
			},
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	reply := p.call("huge-call", registered[0].Handle, "huge", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"})
	if reply.Error == nil || !strings.Contains(reply.Error.Message, "result exceeds the frame limit") {
		t.Fatalf("oversized result reply = %+v", reply)
	}
	again := p.call("after", registered[0].Handle, "huge", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"})
	if again.Error == nil || !strings.Contains(again.Error.Message, "result exceeds the frame limit") {
		t.Fatalf("loop stopped serving after an oversized result: %+v", again)
	}
}

// TestAuthorServeOpaqueArgumentsAndInterruption proves argument bytes cross
// unchanged and a declared interruption crosses without execution evidence.
func TestAuthorServeOpaqueArgumentsAndInterruption(t *testing.T) {
	opaque := []byte{0xff, 0x00, 0x7b}
	var seen []byte
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterTool(model.ToolDefinition{Name: "opaque", Description: "opaque", Parameters: json.RawMessage(`{"type":"object"}`)}, func(_ context.Context, call extension.ToolCall) (extension.ToolResult, error) {
			seen = append([]byte(nil), call.Arguments...)
			return extension.ToolResult{Content: "ok"}, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "effect", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{}, &harness.EffectError{Interrupted: true, Err: errors.New("hook boom")}
			},
		}, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	if len(registered) != 2 {
		t.Fatalf("registrations = %d, want tool and operation", len(registered))
	}
	toolCall := json.RawMessage(`{"id":"call","name":"opaque","arguments":"/wB7"}`)
	reply := p.call("run", registered[0].Handle, "run", toolCall, worker.AuthorCallContext{Token: "t"})
	if reply.Error != nil || !bytes.Equal(seen, opaque) {
		t.Fatalf("opaque run = %+v, seen %v", reply, seen)
	}
	outcome, err := worker.DecodeExecuteResult(reply.Result)
	if err != nil || outcome.Result.Status != model.ResultSuccess || outcome.Result.Content != "ok" {
		t.Fatalf("outcome = %s, %v", reply.Result, err)
	}
	reply = p.call("effect", registered[1].Handle, "effect", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"})
	if reply.Error == nil || !reply.Error.Interrupted {
		t.Fatalf("effect error reply = %+v", reply)
	}
	encoded, merr := json.Marshal(reply.Error)
	if merr != nil {
		t.Fatalf("marshal error frame: %v", merr)
	}
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &frame); err != nil {
		t.Fatalf("unmarshal error frame: %v", err)
	}
	if _, present := frame["evidence"]; present {
		t.Fatalf("error frame carries evidence: %s", encoded)
	}
}

func TestAuthorServeRejectsRetiredToolRoutes(t *testing.T) {
	for _, method := range []string{"normalize", "prepare", "execute", "authorization"} {
		t.Run(method, func(t *testing.T) {
			p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
				if method == "authorization" {
					_, err := api.AttachToolCheck(func(context.Context, extension.AuthorContext, model.ToolCall) (*string, error) { return nil, nil }, nil)
					return nil, err
				}
				_, err := api.RegisterTool(model.ToolDefinition{Name: "echo", Parameters: json.RawMessage(`{}`)}, func(context.Context, extension.ToolCall) (extension.ToolResult, error) {
					return extension.ToolResult{}, nil
				}, nil)
				return nil, err
			}})
			p.hello(extension.APIVersion)
			registered := p.activate()
			p.send(worker.Frame{Kind: worker.KindRequest, ID: "old", Method: worker.MethodCall, Params: callParams(t, registered[0].Handle, method)})
			if err := p.wait(); err == nil {
				t.Fatal("retired method accepted")
			}
		})
	}
}

// TestAuthorHostcallEmptyArgumentsAndPartial proves an empty hostcall argument
// document is carried and a classed error partial reaches the author as the
// typed CallError.
func TestAuthorHostcallEmptyArgumentsAndPartial(t *testing.T) {
	var partialErr error
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterCleanup(func(ctx context.Context, call extension.AuthorContext, _ harness.SessionIdentity) error {
			if _, err := call.Call(ctx, "svc", "empty", nil); err != nil {
				return err
			}
			_, partialErr = call.Call(ctx, "svc", "partial", json.RawMessage(`{}`))
			return nil
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: callParams(t, registered[0].Handle, "cleanup")})
	hostcall := p.read()
	if hostcall.Method != worker.MethodHostcall {
		t.Fatalf("first callback frame = %+v, want a hostcall", hostcall)
	}
	var first worker.AuthorHostcallParams
	if err := json.Unmarshal(hostcall.Params, &first); err != nil || first.Service != "svc" || first.Method != "empty" || len(first.Arguments) != 0 {
		t.Fatalf("empty hostcall = %q, %+v", hostcall.Params, err)
	}
	var wireMembers map[string]json.RawMessage
	if err := json.Unmarshal(hostcall.Params, &wireMembers); err != nil || string(wireMembers["arguments"]) != `""` {
		t.Fatalf("empty hostcall arguments wire = %s, %v; want the empty base64 string", wireMembers["arguments"], err)
	}
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Result: json.RawMessage(`{"value":"bnVsbA=="}`)})
	second := p.read()
	if second.Method != worker.MethodHostcall {
		t.Fatalf("second callback frame = %+v, want a hostcall", second)
	}
	p.send(worker.Frame{Kind: worker.KindResponse, ID: second.ID, Error: &worker.Error{Class: worker.ClassNotFound, Message: "gone", Partial: worker.JSONBytes(`{"kept":1}`)}})
	reply := p.read()
	if reply.Error != nil {
		t.Fatalf("cleanup reply = %+v", reply)
	}
	var callErr *extension.CallError
	if !errors.As(partialErr, &callErr) || string(callErr.Partial) != `{"kept":1}` || !errors.Is(partialErr, harness.ErrNotFound) {
		t.Fatalf("hostcall partial = %#v, want the typed CallError", partialErr)
	}
}

// TestAuthorHostcallCancellationSendsCancelAndWaits proves a cancelled
// callback hostcall sends its cancel but still waits for and returns the
// host's success result.
func TestAuthorHostcallCancellationSendsCancelAndWaits(t *testing.T) {
	cancelReady := make(chan context.CancelFunc, 1)
	value := json.RawMessage(`{"kept":"yes"}`)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "slow", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				child, cancel := context.WithCancel(ctx)
				cancelReady <- cancel
				return call.Call(child, "svc", "slow", nil)
			},
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: mustCallParams(t, registered[0].Handle, "slow")})
	hostcall := p.read()
	(<-cancelReady)()
	sent := p.read()
	if sent.Kind != worker.KindCancel || sent.ID != hostcall.ID {
		t.Fatalf("worker cancel = %+v, want the hostcall cancellation", sent)
	}
	result, err := worker.EncodeServiceResult(extension.Result{Value: value})
	if err != nil {
		t.Fatal(err)
	}
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Result: result})
	reply := p.read()
	if reply.ID != "call" || reply.Error != nil {
		t.Fatalf("callback reply = %+v, want the success result", reply)
	}
	decoded, err := worker.DecodeServiceResult(reply.Result)
	if err != nil || !bytes.Equal(decoded.Value, value) {
		t.Fatalf("callback result = %+v, %v; want the host success value", decoded, err)
	}
}

// TestAuthorProtocolFailureEndsServe proves a malformed hostcall response ends
// the serve loop.
func TestAuthorProtocolFailureEndsServe(t *testing.T) {
	observed := make(chan error, 1)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterCleanup(func(ctx context.Context, call extension.AuthorContext, _ harness.SessionIdentity) error {
			_, err := call.Call(ctx, "svc", "method", nil)
			observed <- err
			return err
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: callParams(t, registered[0].Handle, "cleanup")})
	hostcall := p.read()
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Result: json.RawMessage("null"), Error: &worker.Error{Class: worker.ClassInternal, Message: "both"}})
	if err := p.wait(); err == nil {
		t.Fatal("malformed hostcall response did not end serve")
	}
	select {
	case err := <-observed:
		if err == nil {
			t.Fatal("pending callback observed no terminal error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending callback did not settle")
	}
}

// TestAuthorServeEOFCancelsWork proves closing the host input cancels an
// in-flight callback and stops the loop cleanly.
func TestAuthorServeEOFCancelsWork(t *testing.T) {
	cancelled := make(chan struct{}, 1)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterCleanup(func(ctx context.Context, _ extension.AuthorContext, _ harness.SessionIdentity) error {
			<-ctx.Done()
			cancelled <- struct{}{}
			return ctx.Err()
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: callParams(t, registered[0].Handle, "cleanup")})
	time.Sleep(50 * time.Millisecond)
	p.closeInput()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("EOF did not cancel the in-flight callback")
	}
	if err := p.wait(); err != nil {
		t.Fatalf("EOF serve = %v, want a clean stop", err)
	}
}

// TestAuthorServeDroppedProgressFlood proves progress frames are dropped
// while a callback is outstanding and the callback still completes.
func TestAuthorServeDroppedProgressFlood(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error {
			close(entered)
			<-release
			return nil
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: callParams(t, registered[0].Handle, "cleanup")})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("outstanding callback never entered")
	}
	for i := 0; i < 128; i++ {
		p.send(worker.Frame{Kind: worker.KindProgress, ID: "call", Value: json.RawMessage(`1`)})
	}
	close(release)
	reply := p.read()
	if reply.ID != "call" || reply.Error != nil {
		t.Fatalf("outstanding call after a progress flood = %+v", reply)
	}
}

// TestAuthorRetainedCallContextClosed proves a call context captured from a
// callback is closed once that callback returns.
func TestAuthorRetainedCallContextClosed(t *testing.T) {
	captured := make(chan func(context.Context, string, string, json.RawMessage) (extension.Result, error), 1)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterCleanup(func(_ context.Context, call extension.AuthorContext, _ harness.SessionIdentity) error {
			captured <- call.Call
			return nil
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	if reply := p.call("call", registered[0].Handle, "cleanup", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"}); reply.Error != nil {
		t.Fatalf("cleanup reply = %+v", reply)
	}
	retained := <-captured
	if _, err := retained(context.Background(), "dep", "ping", nil); !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("retained call context = %v, want extension.ErrClosed", err)
	}
}

// TestAuthorHostcallRoutingTokenAndBytes proves the callback token and exact
// argument/result bytes cross the hostcall route unchanged.
func TestAuthorHostcallRoutingTokenAndBytes(t *testing.T) {
	arguments := []byte("{\"text\":\"line one\\nline two\"}")
	value := json.RawMessage("{\n  \"n\": 9007199254740993\n}")
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "echo", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, arguments json.RawMessage) (extension.Result, error) {
				return call.Call(ctx, "dep", "ping", arguments)
			},
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	params, _ := json.Marshal(worker.AuthorCallParams{Handle: registered[0].Handle, Method: "echo", Context: worker.AuthorCallContext{Token: "call-token"}, Arguments: worker.JSONBytes(arguments)})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: params})
	hostcall := p.read()
	if hostcall.Method != worker.MethodHostcall {
		t.Fatalf("callback frame = %+v, want a hostcall", hostcall)
	}
	var routed worker.AuthorHostcallParams
	if err := json.Unmarshal(hostcall.Params, &routed); err != nil || routed.Token != "call-token" || routed.Service != "dep" || routed.Method != "ping" || !bytes.Equal(routed.Arguments, arguments) {
		t.Fatalf("hostcall route = %+v, %v; want token, service, method and exact arguments", routed, err)
	}
	result, err := worker.EncodeServiceResult(extension.Result{Value: value})
	if err != nil {
		t.Fatal(err)
	}
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Result: result})
	reply := p.read()
	if reply.Error != nil {
		t.Fatalf("callback reply = %+v", reply)
	}
	decoded, err := worker.DecodeServiceResult(reply.Result)
	if err != nil || !bytes.Equal(decoded.Value, value) {
		t.Fatalf("callback result = %+v, %v; want the exact value bytes", decoded, err)
	}
}

// TestAuthorMalformedErrorPartialEndsServe proves an invalid partial payload
// on a hostcall response is a protocol failure.
func TestAuthorMalformedErrorPartialEndsServe(t *testing.T) {
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "call", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				return call.Call(ctx, "dep", "ping", nil)
			},
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	params, _ := json.Marshal(worker.AuthorCallParams{Handle: registered[0].Handle, Method: "call", Context: worker.AuthorCallContext{Token: "t"}, Arguments: worker.JSONBytes(json.RawMessage(`{}`))})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: params})
	hostcall := p.read()
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Error: &worker.Error{Class: worker.ClassInvalid, Message: "bad", Partial: worker.JSONBytes(`{`)}})
	if err := p.wait(); err == nil {
		t.Fatal("malformed error partial did not end serve")
	}
}

// TestAuthorErrorClassMappingBothDirections proves the closed class unwraps
// to its sentinel and callback/hostcall errors carry the class back out.
func TestAuthorErrorClassMappingBothDirections(t *testing.T) {
	for _, tc := range []struct {
		class worker.Class
		want  error
	}{
		{worker.ClassClosed, extension.ErrClosed},
		{worker.ClassDenied, extension.ErrDenied},
		{worker.ClassCancelled, context.Canceled},
		{worker.ClassInvalid, harness.ErrInvalid},
		{worker.ClassNotFound, harness.ErrNotFound},
		{worker.ClassConflict, harness.ErrConflict},
		{worker.ClassCorrupt, harness.ErrCorrupt},
		{worker.ClassIncompatible, harness.ErrIncompatible},
		{worker.ClassStorage, harness.ErrStorage},
	} {
		if !errors.Is(&worker.Error{Class: tc.class}, tc.want) {
			t.Fatalf("class %q does not unwrap to %v", tc.class, tc.want)
		}
	}
	if failure := worker.Classify(fmt.Errorf("wrapped: %w", extension.ErrDenied)); failure.Class != worker.ClassDenied {
		t.Fatalf("wrapped denied error classified as %q, want %q", failure.Class, worker.ClassDenied)
	}
	nested := make(chan bool, 1)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "direct", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{}, extension.ErrClosed
			},
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "nested", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				_, err := call.Call(ctx, "dep", "ping", nil)
				nested <- errors.Is(err, extension.ErrClosed)
				return extension.Result{}, err
			},
		}, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	if reply := p.call("direct", registered[0].Handle, "direct", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"}); reply.Error == nil || reply.Error.Class != worker.ClassClosed {
		t.Fatalf("direct closed reply = %+v", reply)
	}
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "nested", Method: worker.MethodCall, Params: mustCallParams(t, registered[1].Handle, "nested")})
	hostcall := p.read()
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Error: &worker.Error{Class: worker.ClassClosed, Message: "gone"}})
	reply := p.read()
	if reply.ID != "nested" || reply.Error == nil || reply.Error.Class != worker.ClassClosed || !<-nested {
		t.Fatalf("nested closed reply = %+v", reply)
	}
	if !errors.Is(&worker.Error{Class: worker.ClassCancelled}, context.Canceled) {
		t.Fatal("cancelled class does not unwrap to context.Canceled")
	}
}

func mustCallParams(t *testing.T, handle, method string) json.RawMessage {
	t.Helper()
	params, err := json.Marshal(worker.AuthorCallParams{Handle: handle, Method: method, Context: worker.AuthorCallContext{Token: "t"}, Arguments: worker.JSONBytes(json.RawMessage(`{}`))})
	if err != nil {
		t.Fatal(err)
	}
	return params
}

// TestAuthorCancelledClassUnwrapsContext proves a cancelled hostcall response
// is observable as context.Canceled inside the callback and carried back out.
func TestAuthorCancelledClassUnwrapsContext(t *testing.T) {
	observed := make(chan bool, 1)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "nested", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				_, err := call.Call(ctx, "dep", "ping", nil)
				observed <- errors.Is(err, context.Canceled)
				return extension.Result{}, err
			},
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "nested", Method: worker.MethodCall, Params: mustCallParams(t, registered[0].Handle, "nested")})
	hostcall := p.read()
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Error: &worker.Error{Class: worker.ClassCancelled, Message: "cancelled"}})
	reply := p.read()
	cancelled := <-observed
	if reply.Error == nil || reply.Error.Class != worker.ClassCancelled || !cancelled {
		t.Fatalf("cancelled reply = %+v, observed %v", reply, cancelled)
	}
}

// TestAuthorOversizedErrorMessageIsCallLocal proves an over-limit error message
// is replaced by the small internal error while the loop keeps serving.
func TestAuthorOversizedErrorMessageIsCallLocal(t *testing.T) {
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "blow", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{}, errors.New(strings.Repeat("x", worker.FrameLimit))
			},
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error { return nil }, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	reply := p.call("blow", registered[0].Handle, "blow", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"})
	if reply.Error == nil || reply.Error.Message != "error response exceeds the frame limit" {
		t.Fatalf("oversized error reply = %+v", reply)
	}
	if after := p.call("after", registered[1].Handle, "cleanup", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"}); after.Error != nil {
		t.Fatalf("loop stopped serving: %+v", after)
	}
}

// TestAuthorProtocolRoutingFailures proves an unknown response ID and an
// unknown frame kind each end the serve loop.
func TestAuthorProtocolRoutingFailures(t *testing.T) {
	t.Run("unknown response id", func(t *testing.T) {
		p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
		p.hello(extension.APIVersion)
		p.activate()
		p.send(worker.Frame{Kind: worker.KindResponse, ID: "ghost", Result: json.RawMessage(`{"value":"bnVsbA=="}`)})
		if err := p.wait(); err == nil {
			t.Fatal("unknown response id did not end serve")
		}
	})
	t.Run("unknown frame kind", func(t *testing.T) {
		p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
		p.hello(extension.APIVersion)
		p.activate()
		p.send(worker.Frame{Kind: "bogus"})
		if err := p.wait(); err == nil {
			t.Fatal("unknown frame kind did not end serve")
		}
	})
}

// TestAuthorJSONBytesCanonicalBase64 proves the opaque value accepts only the
// canonical padded standard base64 spelling.
func TestAuthorJSONBytesCanonicalBase64(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		ok   bool
		want []byte
	}{
		{`""`, true, []byte{}},
		{`"AAE="`, true, []byte{0, 1}},
		{`"AAE"`, false, nil},
		{`"AAE=="`, false, nil},
		{`"A A E ="`, false, nil},
		{`"_-8="`, false, nil},
		{`null`, false, nil},
		{`7`, false, nil},
	} {
		var value worker.JSONBytes
		err := json.Unmarshal([]byte(tc.raw), &value)
		if (err == nil) != tc.ok || tc.ok && !bytes.Equal(value, tc.want) {
			t.Fatalf("JSONBytes %s = %v, %v; want ok=%v", tc.raw, value, err, tc.ok)
		}
	}
}

// TestAuthorActivationCallContextClosed proves a Call retained from Activate
// is closed after the activation reply and writes no hostcall frame.
func TestAuthorActivationCallContextClosed(t *testing.T) {
	apis := make(chan extension.API, 1)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error { return nil }, nil); err != nil {
			return nil, err
		}
		apis <- api
		return nil, nil
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	api := <-apis
	if _, err := api.Call(context.Background(), "svc", "method", nil); !errors.Is(err, extension.ErrClosed) {
		t.Fatalf("retained activation Call = %v, want extension.ErrClosed", err)
	}
	if reply := p.call("after", registered[0].Handle, "cleanup", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"}); reply.Error != nil {
		t.Fatalf("frame after retained activation Call = %+v, want no hostcall frame", reply)
	}
}

// TestAuthorActivateFailureRepliesClassed proves an Activate error becomes the
// request's classed error response.
func TestAuthorActivateFailureRepliesClassed(t *testing.T) {
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return nil, errors.New("boom")
	}})
	p.hello(extension.APIVersion)
	params, _ := json.Marshal(worker.AuthorActivateParams{Section: worker.JSONBytes(`{}`), Token: "entry"})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "activate", Method: worker.MethodActivate, Params: params})
	reply := p.read()
	if reply.Error == nil || reply.Error.Class != worker.ClassInternal || reply.Error.Message != "boom" {
		t.Fatalf("activation reply = %+v, want internal/boom", reply)
	}
}

// TestAuthorActivationCloseRunsAtShutdown proves the close function returned
// by Activate runs once the serve loop stops.
func TestAuthorActivationCloseRunsAtShutdown(t *testing.T) {
	var closed atomic.Bool
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return func(context.Context) error { closed.Store(true); return nil }, nil
	}})
	p.hello(extension.APIVersion)
	p.activate()
	p.closeInput()
	if err := p.wait(); err != nil {
		t.Fatalf("serve = %v", err)
	}
	if !closed.Load() {
		t.Fatal("activation close function did not run")
	}
}

// TestAuthorActivationHostcallUsesActivationToken proves a hostcall issued
// during Activate carries the activation token and empty arguments on the wire.
func TestAuthorActivationHostcallUsesActivationToken(t *testing.T) {
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.Call(ctx, "svc", "method", nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	params, _ := json.Marshal(worker.AuthorActivateParams{Section: worker.JSONBytes(`{}`), Token: "entry"})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "activate", Method: worker.MethodActivate, Params: params})
	hostcall := p.read()
	if hostcall.Method != worker.MethodHostcall {
		t.Fatalf("activation frame = %+v, want a hostcall", hostcall)
	}
	var routed worker.AuthorHostcallParams
	if err := json.Unmarshal(hostcall.Params, &routed); err != nil || routed.Token != "entry" || routed.Service != "svc" || routed.Method != "method" || len(routed.Arguments) != 0 {
		t.Fatalf("activation hostcall = %+v, %v; want the activation token and empty arguments", routed, err)
	}
	result, err := worker.EncodeServiceResult(extension.Result{Value: json.RawMessage("null")})
	if err != nil {
		t.Fatal(err)
	}
	p.send(worker.Frame{Kind: worker.KindResponse, ID: hostcall.ID, Result: result})
	reply := p.read()
	if reply.Error != nil {
		t.Fatalf("activation reply = %+v", reply)
	}
}

// TestAuthorMalformedActivationSectionEndsServe proves a non-canonical base64
// section is a protocol failure.
func TestAuthorMalformedActivationSectionEndsServe(t *testing.T) {
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
	p.hello(extension.APIVersion)
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "activate", Method: worker.MethodActivate, Params: json.RawMessage(`{"section":"AAE","token":"entry"}`)})
	if err := p.wait(); err == nil {
		t.Fatal("malformed activation section did not end serve")
	}
}

// panicReader fails the test if Serve reads any frame.
type panicReader struct{ t *testing.T }

func (r panicReader) Read([]byte) (int, error) {
	r.t.Fatal("Serve read a frame before validating its manifest")
	return 0, io.EOF
}

// TestAuthorServeRejectsInvalidManifestBeforeReading proves manifest
// validation precedes the first read.
func TestAuthorServeRejectsInvalidManifestBeforeReading(t *testing.T) {
	err := worker.Serve(context.Background(), extension.Extension{Manifest: extension.Manifest{}}, panicReader{t: t}, io.Discard)
	if err == nil {
		t.Fatal("invalid manifest was accepted")
	}
}

// TestAuthorOversizedHelloResultEndsServe proves a hello result over the frame
// limit ends the loop with an error.
func TestAuthorOversizedHelloResultEndsServe(t *testing.T) {
	manifest := manifest()
	manifest.SettingsSchema = json.RawMessage(`{"type":"string","description":"` + strings.Repeat("x", worker.FrameLimit) + `"}`)
	p := startServe(t, extension.Extension{Manifest: manifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }})
	params, _ := json.Marshal(worker.HelloParams{APIVersion: extension.APIVersion})
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "hello", Method: worker.MethodHello, Params: params})
	if err := p.wait(); err == nil {
		t.Fatal("oversized hello result did not end serve")
	}
}

// startHostcallProbe activates one operation that makes one hostcall and
// returns the pipe with that outstanding hostcall frame.
func startHostcallProbe(t *testing.T) (*servePipe, worker.Frame) {
	t.Helper()
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "call", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				return call.Call(ctx, "dep", "ping", nil)
			},
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindRequest, ID: "call", Method: worker.MethodCall, Params: mustCallParams(t, registered[0].Handle, "call")})
	return p, p.read()
}

// TestAuthorHostcallResponseValidationIsFatal proves malformed hostcall
// responses end the serve loop.
func TestAuthorHostcallResponseValidationIsFatal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response func(id string) worker.Frame
	}{
		{"neither arm", func(id string) worker.Frame { return worker.Frame{Kind: worker.KindResponse, ID: id} }},
		{"unknown error class", func(id string) worker.Frame {
			return worker.Frame{Kind: worker.KindResponse, ID: id, Error: &worker.Error{Class: "bogus", Message: "x"}}
		}},
		{"invalid service result", func(id string) worker.Frame {
			return worker.Frame{Kind: worker.KindResponse, ID: id, Result: json.RawMessage(`{}`)}
		}},
		{"invalid success value", func(id string) worker.Frame {
			return worker.Frame{Kind: worker.KindResponse, ID: id, Result: json.RawMessage(`{"value":"eA=="}`)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, hostcall := startHostcallProbe(t)
			p.send(tc.response(hostcall.ID))
			if err := p.wait(); err == nil {
				t.Fatal("malformed hostcall response did not end serve")
			}
		})
	}
}

// TestAuthorUnknownCancelIsNoOp proves a cancel for a never-seen request id is
// ignored and the loop keeps serving.
func TestAuthorUnknownCancelIsNoOp(t *testing.T) {
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error { return nil }, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	p.send(worker.Frame{Kind: worker.KindCancel, ID: "ghost"})
	if reply := p.call("after", registered[0].Handle, "cleanup", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"}); reply.Error != nil {
		t.Fatalf("call after an unknown cancel = %+v", reply)
	}
}

// TestAuthorResultJustUnderFrameLimit proves a large result below the frame
// limit is delivered normally.
func TestAuthorResultJustUnderFrameLimit(t *testing.T) {
	value := json.RawMessage(`"` + strings.Repeat("x", 2<<20) + `"`)
	p := startServe(t, extension.Extension{Manifest: manifest(), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterOperation(extension.Operation{
			Target: extension.ExposedTargetHarness,
			Method: extension.Method{Name: "large", Input: json.RawMessage(`{}`), Success: json.RawMessage(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: value}, nil
			},
		}, nil)
		return nil, err
	}})
	p.hello(extension.APIVersion)
	registered := p.activate()
	reply := p.call("large", registered[0].Handle, "large", json.RawMessage(`{}`), worker.AuthorCallContext{Token: "t"})
	if reply.Error != nil {
		t.Fatalf("large result reply = %+v", reply.Error)
	}
	decoded, err := worker.DecodeServiceResult(reply.Result)
	if err != nil || !bytes.Equal(decoded.Value, value) {
		t.Fatalf("large result = %d bytes, %v; want the exact value", len(decoded.Value), err)
	}
}

// TestAuthorFrameBoundsAndDroppedProgress proves the frame reader/writer bound
// and the retained progress and null-member rules.
func TestAuthorFrameBoundsAndDroppedProgress(t *testing.T) {
	makeFrame := func(size int) []byte {
		const prefix = `{"kind":"progress","value":"`
		const suffix = `"}`
		frame := make([]byte, 0, size)
		frame = append(frame, prefix...)
		frame = append(frame, bytes.Repeat([]byte("a"), size-len(prefix)-len(suffix))...)
		frame = append(frame, suffix...)
		return frame
	}
	exact := makeFrame(worker.FrameLimit)
	if len(exact) != worker.FrameLimit {
		t.Fatalf("fixture length = %d", len(exact))
	}
	if _, err := worker.NewReader(bytes.NewReader(append(exact, '\n'))).ReadFrame(); err != nil {
		t.Fatalf("exactly FrameLimit frame = %v", err)
	}
	oversized := makeFrame(worker.FrameLimit + 1)
	if _, err := worker.NewReader(bytes.NewReader(append(oversized, '\n'))).ReadFrame(); !errors.Is(err, worker.ErrFrameTooLarge) {
		t.Fatalf("oversized frame = %v, want ErrFrameTooLarge", err)
	}
	var sink bytes.Buffer
	writer := worker.NewWriter(&sink)
	if err := writer.WriteFrame(worker.Frame{Kind: worker.KindProgress, Value: json.RawMessage(`"` + strings.Repeat("x", worker.FrameLimit) + `"`)}); !errors.Is(err, worker.ErrFrameTooLarge) {
		t.Fatalf("oversized write = %v, want ErrFrameTooLarge", err)
	}
	if sink.Len() != 0 {
		t.Fatalf("oversized write left %d bytes in the output, want none", sink.Len())
	}
	if frame, err := worker.NewReader(strings.NewReader("{\"kind\":\"response\",\"id\":\"x\",\"result\":null}\n")).ReadFrame(); err != nil || string(frame.Result) != "null" {
		t.Fatalf("response result null = %+v, %v; want it accepted", frame, err)
	}
	if _, err := worker.NewReader(strings.NewReader("{\"kind\":\"response\",\"id\":\"x\",\"result\":null,\"error\":null}\n")).ReadFrame(); err == nil {
		t.Fatal("null error member accepted")
	}
	if _, err := worker.NewReader(strings.NewReader("{\"kind\":\"response\",\"id\":\"x\",\"error\":{\"class\":\"invalid\",\"message\":\"x\",\"partial\":null}}\n")).ReadFrame(); err == nil {
		t.Fatal("nested null error partial accepted")
	}
	if _, err := worker.NewReader(strings.NewReader("{\"kind\":\"request\",\"id\":\"x\",\"method\":\"m\",\"params\":{\"n\":null}}\n")).ReadFrame(); err == nil {
		t.Fatal("nested null params accepted")
	}
	if frame, err := worker.NewReader(strings.NewReader("{\"kind\":\"progress\",\"value\":{\"a\":null}}\n")).ReadFrame(); err != nil || frame.Kind != worker.KindProgress {
		t.Fatalf("progress frame = %+v, %v; want it dropped", frame, err)
	}
	const requestPrefix = `{"kind":"request","id":"x","method":"m","params":"`
	const requestSuffix = `"}`
	request := make([]byte, 0, worker.FrameLimit)
	request = append(request, requestPrefix...)
	request = append(request, bytes.Repeat([]byte("a"), worker.FrameLimit-len(requestPrefix)-len(requestSuffix))...)
	request = append(request, requestSuffix...)
	if len(request) != worker.FrameLimit {
		t.Fatalf("request fixture length = %d", len(request))
	}
	if frame, err := worker.NewReader(bytes.NewReader(append(request, '\n'))).ReadFrame(); err != nil || frame.Kind != worker.KindRequest {
		t.Fatalf("exact request frame = %+v, %v", frame, err)
	}
	overRequest := append(append([]byte(nil), requestPrefix...), append(bytes.Repeat([]byte("a"), worker.FrameLimit-len(requestPrefix)-len(requestSuffix)+1), []byte(requestSuffix)...)...)
	if _, err := worker.NewReader(bytes.NewReader(append(overRequest, '\n'))).ReadFrame(); !errors.Is(err, worker.ErrFrameTooLarge) {
		t.Fatalf("one-byte-over request = %v, want ErrFrameTooLarge", err)
	}
}

// TestAuthorServiceResultCodecArms proves the result codec carries exactly one
// arm and rejects a null document.
func TestAuthorServiceResultCodecArms(t *testing.T) {
	encoded, err := worker.EncodeServiceResult(extension.Result{Value: json.RawMessage("null")})
	if err != nil || string(encoded) != `{"value":"bnVsbA=="}` {
		t.Fatalf("success encode = %s, %v", encoded, err)
	}
	decoded, err := worker.DecodeServiceResult(encoded)
	if err != nil || string(decoded.Value) != "null" || decoded.Failure != nil {
		t.Fatalf("success decode = %+v, %v", decoded, err)
	}
	failure, err := worker.EncodeServiceResult(extension.Result{Failure: &extension.Failure{Code: "x", Message: "y"}})
	if err != nil || !bytes.Contains(failure, []byte(`"failure"`)) || bytes.Contains(failure, []byte(`"value"`)) {
		t.Fatalf("failure encode = %s, %v", failure, err)
	}
	if _, err := worker.EncodeServiceResult(extension.Result{}); err == nil {
		t.Fatal("empty result encoded")
	}
	if _, err := worker.EncodeServiceResult(extension.Result{Value: json.RawMessage("null"), Failure: &extension.Failure{Code: "x", Message: "y"}}); err == nil {
		t.Fatal("both result arms encoded")
	}
	if _, err := worker.DecodeServiceResult(json.RawMessage(`null`)); err == nil {
		t.Fatal("null service result decoded")
	}
}
