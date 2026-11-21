package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
)

// TestAuthorObserverAndIndependentCommand proves the worker author protocol's
// observer registration and distinct command request: the observer receives a
// delivered fact with a null result, and Command sends the fixed target,
// method and argument bytes and returns the host's typed result.
func TestAuthorObserverAndIndependentCommand(t *testing.T) {
	manifest := extension.Manifest{ID: "author", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`), Services: []string{extension.ContractCommands}}
	facts := make(chan extension.Fact, 1)
	commandResult := make(chan extension.Result, 1)
	commandErr := make(chan error, 1)
	in, host := io.Pipe()
	guest, out := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- worker.Serve(context.Background(), extension.Extension{
			Manifest: manifest,
			Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
				if _, err := api.AttachObserver(func(_ context.Context, fact extension.Fact) error {
					facts <- fact
					return nil
				}, nil); err != nil {
					return nil, err
				}
				go func() {
					result, err := api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: "session-1"}, "submit", json.RawMessage(`{"operation_id":"op-1"}`))
					commandResult <- result
					commandErr <- err
				}()
				return nil, nil
			},
		}, in, out)
	}()
	defer host.Close()
	defer guest.Close()
	defer out.Close()
	w, r := worker.NewWriter(host), worker.NewReader(guest)
	send := func(id, method string, value any) {
		t.Helper()
		params, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteFrame(worker.Frame{Kind: worker.KindRequest, ID: id, Method: method, Params: params}); err != nil {
			t.Fatal(err)
		}
	}
	send("hello", worker.MethodHello, worker.HelloParams{APIVersion: extension.APIVersion})
	if _, err := r.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	send("activate", worker.MethodActivate, worker.AuthorActivateParams{Section: worker.JSONBytes(`{}`), Token: "entry"})

	observerHandle, commandFrame := "", worker.Frame{}
	activated := false
	for observerHandle == "" || commandFrame.ID == "" || !activated {
		frame, err := r.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		switch frame.Method {
		case "register":
			var params worker.AuthorRegisterParams
			if err := json.Unmarshal(frame.Params, &params); err != nil || params.Kind != "observer" {
				t.Fatalf("registration = %#v, %v", frame, err)
			}
			observerHandle = params.Handle
			respondNull(t, w, frame.ID)
		case worker.MethodCommand:
			commandFrame = frame
		case "":
			if frame.ID != "activate" || frame.Error != nil {
				t.Fatalf("unexpected response %#v", frame)
			}
			activated = true
		default:
			t.Fatalf("unexpected frame %#v", frame)
		}
	}
	var command worker.AuthorCommandParams
	if err := json.Unmarshal(commandFrame.Params, &command); err != nil {
		t.Fatal(err)
	}
	if command.Target.Kind != extension.ExposedTargetSession || command.Target.SessionID != "session-1" || command.Method != "submit" || string(command.Arguments) != `{"operation_id":"op-1"}` {
		t.Fatalf("command params = %+v", command)
	}
	encoded, err := worker.EncodeServiceResult(extension.Result{Value: json.RawMessage(`{"operation_id":"op-1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteFrame(worker.Frame{Kind: worker.KindResponse, ID: commandFrame.ID, Result: encoded}); err != nil {
		t.Fatal(err)
	}
	if result := <-commandResult; string(result.Value) != `{"operation_id":"op-1"}` {
		t.Fatalf("command result = %+v", result)
	}
	if err := <-commandErr; err != nil {
		t.Fatalf("command error = %v", err)
	}

	fact := extension.Fact{Kind: extension.FactOperationSettled, SessionID: "session-1", OperationID: "op-1", Status: string(harness.OperationSuccess)}
	args, err := json.Marshal(fact)
	if err != nil {
		t.Fatal(err)
	}
	send("observe", worker.MethodCall, worker.AuthorCallParams{Handle: observerHandle, Method: "observe", Context: worker.AuthorCallContext{Token: "callback"}, Arguments: worker.JSONBytes(args)})
	reply, err := r.ReadFrame()
	if err != nil || reply.ID != "observe" || reply.Error != nil || string(reply.Result) != "null" {
		t.Fatalf("observe reply = %#v, %v", reply, err)
	}
	select {
	case delivered := <-facts:
		if delivered != fact {
			t.Fatalf("delivered fact = %+v, want %+v", delivered, fact)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("observer received no fact")
	}
	host.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// respondNull answers one host request with the protocol-level null result.
func respondNull(t *testing.T, w *worker.Writer, id string) {
	t.Helper()
	if err := w.WriteFrame(worker.Frame{Kind: worker.KindResponse, ID: id, Result: json.RawMessage("null")}); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorHandshake(t *testing.T) {
	manifest := extension.Manifest{ID: "author", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object","default":null}`)}
	for _, version := range []string{"5", extension.APIVersion} {
		t.Run(version, func(t *testing.T) {
			var called atomic.Bool
			in, host := io.Pipe()
			guest, out := io.Pipe()
			done := make(chan error, 1)
			go func() {
				done <- worker.Serve(context.Background(), extension.Extension{
					Manifest: manifest,
					Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
						called.Store(true)
						if !bytes.Equal(api.Settings(), []byte(`{"value":1}`)) {
							t.Errorf("settings = %s", api.Settings())
						}
						return nil, nil
					},
				}, in, out)
			}()
			defer host.Close()
			defer guest.Close()
			defer out.Close()
			w, r := worker.NewWriter(host), worker.NewReader(guest)
			params, _ := json.Marshal(worker.HelloParams{APIVersion: version})
			if err := w.WriteFrame(worker.Frame{Kind: worker.KindRequest, ID: "hello", Method: worker.MethodHello, Params: params}); err != nil {
				t.Fatal(err)
			}
			reply, err := r.ReadFrame()
			if err != nil {
				t.Fatal(err)
			}
			if version != extension.APIVersion {
				if reply.Error == nil || reply.Error.Class != worker.ClassIncompatible || called.Load() {
					t.Fatalf("mismatched hello: %#v, called %v", reply, called.Load())
				}
				<-done
				return
			}
			var hello worker.HelloResult
			if reply.Error != nil || json.Unmarshal(reply.Result, &hello) != nil || !reflect.DeepEqual(hello.Manifest.Value(), manifest) {
				t.Fatalf("hello result: %#v (%s)", reply, reply.Result)
			}
			params, _ = json.Marshal(worker.AuthorActivateParams{Section: worker.JSONBytes(`{"value":1}`), Token: "entry"})
			if err := w.WriteFrame(worker.Frame{Kind: worker.KindRequest, ID: "activate", Method: worker.MethodActivate, Params: params}); err != nil {
				t.Fatal(err)
			}
			reply, err = r.ReadFrame()
			if err != nil || reply.Error != nil || string(reply.Result) != "null" || !called.Load() {
				t.Fatalf("activate result: %#v, %v, called %v", reply, err, called.Load())
			}
			host.Close()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAuthorRegistrationDuringActivation(t *testing.T) {
	manifest := extension.Manifest{ID: "author", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}
	var called atomic.Bool
	in, host := io.Pipe()
	guest, out := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- worker.Serve(context.Background(), extension.Extension{
			Manifest: manifest,
			Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
				_, err := api.RegisterCleanup(func(_ context.Context, call extension.AuthorContext, session harness.SessionIdentity) error {
					called.Store(call.ExtensionID == "author" && call.RequestID == "client-request" && session.SessionID == "s")
					return nil
				}, nil)
				return nil, err
			},
		}, in, out)
	}()
	defer host.Close()
	defer guest.Close()
	defer out.Close()
	w, r := worker.NewWriter(host), worker.NewReader(guest)
	send := func(id, method string, value any) {
		t.Helper()
		params, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteFrame(worker.Frame{Kind: worker.KindRequest, ID: id, Method: method, Params: params}); err != nil {
			t.Fatal(err)
		}
	}
	send("hello", worker.MethodHello, worker.HelloParams{APIVersion: extension.APIVersion})
	if _, err := r.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	send("activate", worker.MethodActivate, worker.AuthorActivateParams{Section: worker.JSONBytes(`{}`), Token: "entry"})
	registration, err := r.ReadFrame()
	if err != nil || registration.Method != "register" {
		t.Fatalf("registration: %#v, %v", registration, err)
	}
	if err := w.WriteFrame(worker.Frame{Kind: worker.KindResponse, ID: registration.ID, Result: json.RawMessage("null")}); err != nil {
		t.Fatal(err)
	}
	reply, err := r.ReadFrame()
	if err != nil || reply.ID != "activate" || reply.Error != nil || string(reply.Result) != "null" {
		t.Fatalf("activation: %#v, %v", reply, err)
	}
	var registered worker.AuthorRegisterParams
	if err := json.Unmarshal(registration.Params, &registered); err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal(harness.SessionIdentity{SessionID: "s"})
	send("call", worker.MethodCall, worker.AuthorCallParams{Handle: registered.Handle, Method: "cleanup", Context: worker.AuthorCallContext{Token: "callback", RequestID: "client-request", ExtensionID: "author", Settings: worker.JSONBytes(`{}`)}, Arguments: args})
	reply, err = r.ReadFrame()
	if err != nil || reply.ID != "call" || reply.Error != nil || string(reply.Result) != "null" || !called.Load() {
		t.Fatalf("callback: %#v, %v, called %v", reply, err, called.Load())
	}
	send("release", "release", struct {
		Handle string `json:"handle"`
	}{registered.Handle})
	reply, err = r.ReadFrame()
	if err != nil || reply.ID != "release" || reply.Error != nil || string(reply.Result) != "null" {
		t.Fatalf("release: %#v, %v", reply, err)
	}
	send("released-call", worker.MethodCall, worker.AuthorCallParams{Handle: registered.Handle, Method: "cleanup", Context: worker.AuthorCallContext{Token: "callback", ExtensionID: "author", Settings: worker.JSONBytes(`{}`)}, Arguments: args})
	if err := <-done; err == nil {
		t.Fatal("released handler remained callable")
	}
}
