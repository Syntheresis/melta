package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
)

func TestAuthorHandshake(t *testing.T) {
	manifest := extension.Manifest{ID: "author", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object","default":null}`)}
	for _, version := range []string{"4", "5"} {
		t.Run(version, func(t *testing.T) {
			var called atomic.Bool
			in, host := io.Pipe()
			guest, out := io.Pipe()
			done := make(chan error, 1)
			go func() {
				done <- worker.ServeAuthor(context.Background(), extension.Extension{
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
			if version == "4" {
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
		done <- worker.ServeAuthor(context.Background(), extension.Extension{
			Manifest: manifest,
			Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
				_, err := api.RegisterCleanup(func(_ context.Context, call extension.AuthorContext, session harness.SessionIdentity) error {
					called.Store(call.ExtensionID == "author" && session.SessionID == "s")
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
	send("call", worker.MethodCall, worker.AuthorCallParams{Handle: registered.Handle, Method: "cleanup", Context: worker.AuthorCallContext{Token: "callback", ExtensionID: "author", Settings: worker.JSONBytes(`{}`)}, Arguments: args})
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
