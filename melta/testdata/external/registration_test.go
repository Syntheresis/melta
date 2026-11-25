package external_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/extensions/agents"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/melta"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

func TestInstallUsesAuthorManifest(t *testing.T) {
	ctx := context.Background()
	opts := melta.Options{
		DataDir: t.TempDir(), Configuration: melta.NewValueConfigurationSource([]byte(`{"extensions":{"author":{"enabled":true}}}`)),
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{}},
	}
	owner := openOwner(t, opts)
	defer owner.Close(ctx)
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "worker"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest, err := worker.EncodeManifest(extension.Manifest{ID: "author", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	installed, err := owner.InstallExtensionPackage(ctx, caller(t, owner), source)
	if err != nil || installed.Id != "author" || installed.SelectedVersion != "1" {
		t.Fatalf("author install = %+v, %v", installed, err)
	}
	legacy := []byte(`{"contract_version":"4","entry_point":"worker","declaration":{"id":"legacy","version":"1"}}`)
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.InstallExtensionPackage(ctx, caller(t, owner), source); err == nil {
		t.Fatal("legacy package accepted")
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("author"))
	extensionDir := filepath.Join(opts.DataDir, "extensions", hex.EncodeToString(hash[:]))
	selectedRaw, err := os.ReadFile(filepath.Join(extensionDir, "selected"))
	if err != nil {
		t.Fatal(err)
	}
	var selected struct {
		Copy string `json:"copy"`
	}
	if err := json.Unmarshal(selectedRaw, &selected); err != nil || selected.Copy == "" {
		t.Fatalf("selected package = %s, %v", selectedRaw, err)
	}
	if err := os.Remove(filepath.Join(extensionDir, "packages", selected.Copy, "worker")); err != nil {
		t.Fatal(err)
	}
	reopened := openOwner(t, opts)
	extensions, err := reopened.ListExtensions(ctx, caller(t, reopened))
	if err != nil || len(extensions) != 1 || extensions[0].ExtensionId != "author" || extensions[0].InactiveReason == nil {
		t.Fatalf("failed installed activation = %+v, %v", extensions, err)
	}
	if _, err := reopened.Health(ctx, caller(t, reopened)); err != nil {
		t.Fatalf("baseline unavailable after worker failure: %v", err)
	}
	if err := reopened.Close(ctx); err != nil {
		t.Fatal(err)
	}
	opts.Configuration = melta.NewValueConfigurationSource([]byte(`{"extensions":{"author":{"enabled":false}}}`))
	disabled := openOwner(t, opts)
	defer disabled.Close(ctx)
	extensions, err = disabled.ListExtensions(ctx, caller(t, disabled))
	if err != nil || len(extensions) != 0 {
		t.Fatalf("disabled missing worker = %+v, %v", extensions, err)
	}
}

func TestAuthorInstallRejectsUncontainedAndUnsafeEntries(t *testing.T) {
	for _, row := range []struct {
		name, entry string
		symlink     bool
		nonexec     bool
	}{
		{"parent", "../outside", false, false},
		{"absolute", "/tmp/outside", false, false},
		{"symlink", "worker", true, false},
		{"non-executable", "worker", false, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			ctx := context.Background()
			dataDir, source := t.TempDir(), t.TempDir()
			owner := openOwner(t, melta.Options{
				DataDir: dataDir, Configuration: melta.NewValueConfigurationSource([]byte(`{}`)),
				Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{}},
			})
			defer owner.Close(ctx)
			manifest, err := worker.EncodeManifest(extension.Manifest{ID: "unsafe", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}, row.entry)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifest, 0o600); err != nil {
				t.Fatal(err)
			}
			var outside string
			if row.symlink {
				outside = filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(outside, []byte("unchanged"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(source, row.entry)); err != nil {
					t.Fatal(err)
				}
			} else if row.nonexec {
				if err := os.WriteFile(filepath.Join(source, row.entry), []byte("not executable"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := owner.InstallExtensionPackage(ctx, caller(t, owner), source); !errors.Is(err, harness.ErrInvalid) {
				t.Fatalf("unsafe install = %v, want invalid", err)
			}
			if outside != "" {
				if data, err := os.ReadFile(outside); err != nil || string(data) != "unchanged" {
					t.Fatalf("outside sentinel = %q, %v", data, err)
				}
			}
			hash := sha256.Sum256([]byte("unsafe"))
			selected := filepath.Join(dataDir, "extensions", hex.EncodeToString(hash[:]), "selected")
			if _, err := os.Stat(selected); !os.IsNotExist(err) {
				t.Fatalf("unsafe install created selection: %v", err)
			}
		})
	}
}

func TestNativeOperationRequestIdentity(t *testing.T) {
	ctx := context.Background()
	service := extension.Extension{
		Manifest: extension.Manifest{ID: "identity-provider", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			method := extension.ServiceMethod("echo", `{}`, `{}`, "", "")
			_, err := api.RegisterService(extension.Service{Name: "identity/echo", Methods: []extension.RegisteredMethod{{Method: method, Handler: func(_ context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				if call.RequestID != "native-request" || call.ExtensionID != "identity-provider" || !strings.Contains(string(call.Settings), `"provider"`) {
					t.Errorf("nested identity = %q, %q, %s", call.RequestID, call.ExtensionID, call.Settings)
				}
				return extension.Result{Value: args}, nil
			}}}}, nil)
			return nil, err
		},
	}
	guest := extension.Extension{
		Manifest: extension.Manifest{ID: "identity", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			method := extension.ServiceMethod("echo", `{}`, `{}`, "", "")
			_, err := api.RegisterOperation(extension.Operation{Target: extension.ExposedTargetHarness, Method: method, Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				if call.RequestID != "native-request" || !strings.Contains(string(call.Settings), `"caller"`) {
					t.Errorf("operation request = %q, %s", call.RequestID, call.Settings)
				}
				return call.Call(ctx, "identity/echo", "echo", args)
			}}, nil)
			return nil, err
		},
	}
	owner := openOwner(t, melta.Options{DataDir: t.TempDir(), Configuration: melta.NewValueConfigurationSource([]byte(`{"extensions":{"identity":{"enabled":true,"role":"caller"},"identity-provider":{"enabled":true,"role":"provider"}}}`)), Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{}}, Extensions: []extension.Extension{guest, service}})
	defer owner.Close(ctx)
	args := json.RawMessage(`{"value":1}`)
	result, err := owner.InvokeExtension(ctx, caller(t, owner), protocol.ExtensionInvocationRequest{ExtensionId: "identity", Operation: "echo", RequestId: "native-request", Target: protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}, Arguments: &args})
	if err != nil || string(result.Value) != string(args) {
		t.Fatalf("native identity operation = %s, %v", result.Value, err)
	}
}

func TestInstalledAuthorWorkerOperations(t *testing.T) {
	source := t.TempDir()
	code := `package main
import (
 "context"
 "encoding/json"
 "os"
 "github.com/Syntheresis/melta/extension"
 "github.com/Syntheresis/melta/extension/worker"
)
func main() {
 e := extension.Extension{
  Manifest: extension.Manifest{ID:"installed",Version:"1",APIVersion:extension.APIVersion,SettingsSchema:json.RawMessage("{\"type\":\"object\"}")},
  Activate: func(_ context.Context, api extension.API)(func(context.Context) error,error) {
   _,err:=api.RegisterService(extension.Service{Name:"fixture/identity",Methods:[]extension.RegisteredMethod{{Method:extension.ServiceMethod("echo","{}","{}","",""),Handler:func(_ context.Context,c extension.AuthorContext,args json.RawMessage)(extension.Result,error){if c.RequestID!="worker-raw"{panic("wrong nested request identity")};return extension.Result{Value:args},nil}}}},nil)
   if err!=nil{return nil,err}
   ops:=[]extension.Operation{
    {Target:extension.ExposedTargetHarness,Method:extension.ServiceMethod("raw","{}","{}","",""),Handler:func(ctx context.Context,c extension.AuthorContext,args json.RawMessage)(extension.Result,error){if c.RequestID!="worker-raw"{panic("wrong request identity")};return c.Call(ctx,"fixture/identity","echo",args)}},
    {Target:extension.ExposedTargetHarness,Method:extension.ServiceMethod("fail","{}","{}","{}","{}"),Handler:func(_ context.Context,_ extension.AuthorContext,_ json.RawMessage)(extension.Result,error){return extension.Result{Failure:&extension.Failure{Code:"fixture",Message:"refused",Partial:json.RawMessage("[1,null]")}},nil}},
   }
   for _,op:=range ops {if _,err:=api.RegisterOperation(op,nil);err!=nil{return nil,err}}
   return nil,nil
  },
 }
 if err:=worker.Serve(context.Background(),e,os.Stdin,os.Stdout);err!=nil{os.Exit(1)}
}`
	mainPath := filepath.Join(source, "main.go")
	if err := os.WriteFile(mainPath, []byte(code), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "build", "-o", filepath.Join(source, "worker"), mainPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v: %s", err, output)
	}
	manifest, err := worker.EncodeManifest(extension.Manifest{ID: "installed", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	configSource := melta.NewValueConfigurationSource([]byte(`{"extensions":{"installed":{"enabled":true}}}`))
	opts := melta.Options{
		DataDir: t.TempDir(), Configuration: configSource,
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{}},
	}
	owner := openOwner(t, opts)
	if _, err := owner.InstallExtensionPackage(ctx, caller(t, owner), source); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	owner = openOwner(t, opts)
	defer owner.Close(ctx)
	bound := caller(t, owner)
	extensions, err := owner.ListExtensions(ctx, bound)
	if err != nil || len(extensions) != 1 || extensions[0].InactiveReason != nil || len(extensions[0].Operations) != 2 {
		t.Fatalf("worker discovery = %+v, %v", extensions, err)
	}
	value := json.RawMessage(" {\"number\":9007199254740993} ")
	raw, err := owner.InvokeExtension(ctx, bound, protocol.ExtensionInvocationRequest{
		ExtensionId: "installed", Operation: "raw", RequestId: "worker-raw",
		Target: protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}, Arguments: &value,
	})
	if err != nil || string(raw.Value) != string(value) {
		t.Fatalf("raw worker result = %q, %v", raw.Value, err)
	}
	empty := json.RawMessage(`{}`)
	failure, err := owner.InvokeExtension(ctx, bound, protocol.ExtensionInvocationRequest{
		ExtensionId: "installed", Operation: "fail", RequestId: "worker-fail",
		Target: protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindHarness}, Arguments: &empty,
	})
	if err != nil || failure.Failure == nil || failure.Failure.Code != "fixture" || string(failure.Failure.Partial) != `[1,null]` {
		t.Fatalf("worker failure = %+v, %v", failure, err)
	}
	before, err := configSource.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v2 := strings.Replace(code, `Version:"1"`, `Version:"2"`, 1)
	if v2 == code {
		t.Fatal("worker version was not replaced")
	}
	if err := os.WriteFile(mainPath, []byte(v2), 0o600); err != nil {
		t.Fatal(err)
	}
	command = exec.Command("go", "build", "-o", filepath.Join(source, "worker"), mainPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build successor worker: %v: %s", err, output)
	}
	manifest, err = worker.EncodeManifest(extension.Manifest{ID: "installed", Version: "2", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)}, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "manifest.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	updated, err := owner.InstallExtensionPackage(ctx, bound, source)
	if err != nil || updated.LoadedVersion != "1" || updated.SelectedVersion != "2" {
		t.Fatalf("successor selection = %+v, %v", updated, err)
	}
	extensions, err = owner.ListExtensions(ctx, bound)
	if err != nil || len(extensions) != 1 || extensions[0].Version != "1" {
		t.Fatalf("live worker changed before restart: %+v, %v", extensions, err)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := openOwner(t, opts)
	defer restarted.Close(ctx)
	extensions, err = restarted.ListExtensions(ctx, caller(t, restarted))
	if err != nil || len(extensions) != 1 || extensions[0].Version != "2" || extensions[0].InactiveReason != nil {
		t.Fatalf("restarted worker = %+v, %v", extensions, err)
	}
	after, err := configSource.Read(ctx)
	if err != nil || string(after) != string(before) {
		t.Fatalf("installation changed configuration: %s, %v", after, err)
	}
}

type capturedCredentialTool struct {
	count   atomic.Int32
	entered chan struct{}
	release chan struct{}
	values  chan [2]string
}

func (t *capturedCredentialTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *capturedCredentialTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Permissions: []harness.PermissionRequest{{Permission: harness.PermissionSleep, Target: "*"}}, Data: []byte(call.ID)}
}

func (t *capturedCredentialTool) Execute(ctx context.Context, call extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	if t.count.Add(1) == 1 {
		close(t.entered)
		<-t.release
	}
	var values [2]string
	for i, field := range []string{"/activation", "/operation"} {
		result, err := call.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"`+field+`"}`))
		if err != nil {
			return harness.ToolOutcome{Result: model.ToolResult{CallID: string(plan.Data), Status: model.ResultError, Content: err.Error()}}
		}
		var body struct {
			Value string `json:"value"`
		}
		_ = json.Unmarshal(result.Value, &body)
		values[i] = body.Value
	}
	t.values <- values
	return harness.ToolOutcome{Result: model.ToolResult{CallID: string(plan.Data), Status: model.ResultSuccess, Content: "done"}}
}

func TestRetainedOperationKeepsCredentialCapture(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		if wireLastRole(string(body)) == "tool" {
			writeTextTurn(w, "done")
		} else {
			writeToolTurn(w, "credential-call", "credential_probe", `{}`)
		}
	}))
	defer server.Close()
	var config map[string]json.RawMessage
	if err := json.Unmarshal(configurationDocument(server.URL), &config); err != nil {
		t.Fatal(err)
	}
	config["extensions"] = json.RawMessage(`{"credential":{"enabled":true,"activation":"EXT_SECRET","operation":"EXT_SECRET"}}`)
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	probe := &capturedCredentialTool{entered: make(chan struct{}), release: make(chan struct{}), values: make(chan [2]string, 2)}
	guest := extension.Extension{
		Manifest: extension.Manifest{
			ID: "credential", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`),
			Services: []string{extension.ContractCredentials},
			Secrets: []extension.SecretRef{
				{Field: "/activation", Lifetime: extension.SecretActivation},
				{Field: "/operation", Lifetime: extension.SecretOperation},
			},
		},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterTool(model.ToolDefinition{Name: "credential_probe", Parameters: json.RawMessage(`{"type":"object"}`)}, probe, nil)
			return nil, err
		},
	}
	credentials := &memoryCredentials{values: map[string]melta.CredentialValue{
		externalKeyEnv: {Value: externalKey, Defined: true},
		"EXT_SECRET":   {Value: "first-secret", Defined: true},
	}}
	ref := model.ModelRef{Provider: "prov", Model: "m"}
	ctx := context.Background()
	owner := openOwner(t, melta.Options{
		DataDir: t.TempDir(), Configuration: melta.NewValueConfigurationSource(raw),
		Credentials: credentials, Defaults: harness.SessionInputs{Model: &ref}, Extensions: []extension.Extension{guest},
	})
	defer owner.Close(ctx)
	bound := caller(t, owner)
	first, err := owner.CreateSession(ctx, bound, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.SubmitSession(ctx, bound, first.SessionId, protocol.SubmitRequest{OperationId: "first", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textPart(t, "first")}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first credential call did not park")
	}
	if err := credentials.Set(ctx, "EXT_SECRET", "changed-secret"); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Reload(ctx, bound); err != nil {
		t.Fatal(err)
	}
	second, err := owner.CreateSession(ctx, bound, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	submit(t, owner, bound, second.SessionId, "second", "second")
	if got := <-probe.values; got != [2]string{"changed-secret", "changed-secret"} {
		t.Fatalf("new operation credentials = %q", got)
	}
	close(probe.release)
	awaitOperation(t, owner, bound, first.SessionId, "first", protocol.OperationStatusSuccess)
	if got := <-probe.values; got != [2]string{"first-secret", "first-secret"} {
		t.Fatalf("retained operation credentials = %q", got)
	}
}

type replacementTool struct {
	entered chan struct{}
	release chan struct{}
	output  string
}

func (t replacementTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	if t.entered != nil {
		close(t.entered)
		<-t.release
	}
	return call.Arguments, nil
}

func (t replacementTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{
		Permissions: []harness.PermissionRequest{{Permission: harness.PermissionSleep, Target: "*"}},
		Data:        []byte(call.ID),
	}
}

func (t replacementTool) Execute(_ context.Context, _ extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Result: model.ToolResult{CallID: string(plan.Data), Status: model.ResultSuccess, Content: t.output}}
}

func TestToolReplacementKeepsPreparedRegistration(t *testing.T) {
	var bodiesMu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodiesMu.Lock()
		bodies = append(bodies, string(body))
		bodiesMu.Unlock()
		if wireLastRole(string(body)) == "tool" {
			writeTextTurn(w, "done")
		} else {
			writeToolTurn(w, "probe-call", "probe", `{}`)
		}
	}))
	defer server.Close()
	var config map[string]json.RawMessage
	if err := json.Unmarshal(configurationDocument(server.URL), &config); err != nil {
		t.Fatal(err)
	}
	config["extensions"] = json.RawMessage(`{"fixture":{"enabled":true}}`)
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var api extension.API
	var first extension.Registration
	definition := model.ToolDefinition{Name: "probe", Description: "old description", Parameters: json.RawMessage(`{"type":"object","properties":{"old":{"type":"string"}}}`)}
	replacement := model.ToolDefinition{Name: "probe", Description: "new description", Parameters: json.RawMessage(`{"type":"object","properties":{"current":{"type":"boolean"}}}`)}
	guest := extension.Extension{
		Manifest: extension.Manifest{ID: "fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(_ context.Context, registered extension.API) (func(context.Context) error, error) {
			api = registered
			var err error
			first, err = api.RegisterTool(definition, replacementTool{entered: entered, release: release, output: "old"}, nil)
			return nil, err
		},
	}
	ctx := context.Background()
	dataDir := t.TempDir()
	ref := model.ModelRef{Provider: "prov", Model: "m"}
	owner := openOwner(t, melta.Options{
		DataDir: dataDir, Configuration: melta.NewValueConfigurationSource(raw),
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{externalKeyEnv: {Value: externalKey, Defined: true}}},
		Defaults:    harness.SessionInputs{Model: &ref}, Extensions: []extension.Extension{guest},
	})
	bound := caller(t, owner)
	session, err := owner.CreateSession(ctx, bound, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.SubmitSession(ctx, bound, session.SessionId, protocol.SubmitRequest{
		OperationId: "first", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textPart(t, "first")},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first tool was not bound")
	}
	if _, err := api.RegisterTool(replacement, replacementTool{output: "new"}, first); err != nil {
		t.Fatal(err)
	}
	close(release)
	awaitOperation(t, owner, bound, session.SessionId, "first", protocol.OperationStatusSuccess)
	submit(t, owner, bound, session.SessionId, "second", "second")
	bodiesMu.Lock()
	requests := append([]string(nil), bodies...)
	bodiesMu.Unlock()
	if len(requests) < 2 {
		t.Fatalf("model requests = %d, want the first and second Operation's advertisement", len(requests))
	}
	firstTools := advertisedProbe(t, requests[0])
	postToolTools := advertisedProbe(t, requests[1])
	currentTools := advertisedProbe(t, requests[len(requests)-1])
	if firstTools.Description != "old description" || !strings.Contains(string(firstTools.Parameters), `"old"`) {
		t.Fatalf("first advertisement = %+v, want the captured definition", firstTools)
	}
	if postToolTools.Description != "new description" || !strings.Contains(string(postToolTools.Parameters), `"current"`) {
		t.Fatalf("post-tool advertisement = %+v, want the replacement's definition", postToolTools)
	}
	if currentTools.Description != "new description" || !strings.Contains(string(currentTools.Parameters), `"current"`) {
		t.Fatalf("next advertisement = %+v, want the replacement's definition", currentTools)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dataDir, "melta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ids []string
	for i, operation := range []string{"first", "second"} {
		var payload []byte
		if err := db.QueryRow(`SELECT payload FROM entries WHERE operation_id = ? AND kind = 'tool_result'`, operation).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var record struct {
			Content        string                      `json:"content"`
			Implementation *harness.ToolImplementation `json:"implementation"`
		}
		if err := json.Unmarshal(payload, &record); err != nil {
			t.Fatal(err)
		}
		if record.Content != []string{"old", "new"}[i] || record.Implementation == nil || record.Implementation.ExtensionID != "fixture" || record.Implementation.ID == "" {
			t.Fatalf("%s result = %+v", operation, record)
		}
		ids = append(ids, record.Implementation.ID)
	}
	if ids[0] == ids[1] {
		t.Fatalf("replacement reused registration identity %q", ids[0])
	}
}

// advertisedProbe returns the probe definition carried by one recorded model
// request, failing the test when the tool is absent.
func advertisedProbe(t *testing.T, body string) struct {
	Description string
	Parameters  json.RawMessage
} {
	t.Helper()
	var doc struct {
		Tools []struct {
			Function struct {
				Name        string          `json:"name"`
				Description string          `json:"description"`
				Parameters  json.RawMessage `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("advertised tools: %v", err)
	}
	for _, tool := range doc.Tools {
		if tool.Function.Name == "probe" {
			return struct {
				Description string
				Parameters  json.RawMessage
			}{Description: tool.Function.Description, Parameters: tool.Function.Parameters}
		}
	}
	t.Fatalf("model request does not advertise probe: %s", body)
	return struct {
		Description string
		Parameters  json.RawMessage
	}{}
}

// TestAdmissionToolOverrideSurvivesReplacement proves the nearest forbidden
// sibling: a definition an admission hook overrode keeps that override for the
// rest of the Operation even after the tool registration is replaced.
func TestAdmissionToolOverrideSurvivesReplacement(t *testing.T) {
	var bodiesMu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodiesMu.Lock()
		bodies = append(bodies, string(body))
		bodiesMu.Unlock()
		if wireLastRole(string(body)) == "tool" {
			writeTextTurn(w, "done")
		} else {
			writeToolTurn(w, "probe-call", "probe", `{}`)
		}
	}))
	defer server.Close()
	var config map[string]json.RawMessage
	if err := json.Unmarshal(configurationDocument(server.URL), &config); err != nil {
		t.Fatal(err)
	}
	config["extensions"] = json.RawMessage(`{"fixture":{"enabled":true}}`)
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var api extension.API
	var first extension.Registration
	definition := model.ToolDefinition{Name: "probe", Description: "registered description", Parameters: json.RawMessage(`{"type":"object"}`)}
	replacement := model.ToolDefinition{Name: "probe", Description: "replacement description", Parameters: json.RawMessage(`{"type":"object","properties":{"replacement":{"type":"string"}}}`)}
	guest := extension.Extension{
		Manifest: extension.Manifest{ID: "fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(_ context.Context, registered extension.API) (func(context.Context) error, error) {
			api = registered
			if _, err := api.AttachAdmission(func(_ context.Context, _ extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
				candidate := input.Candidate
				for i := range candidate.Tools {
					if candidate.Tools[i].Name == "probe" {
						candidate.Tools[i].Description = "admission override"
						candidate.Tools[i].Parameters = json.RawMessage(`{"type":"object","properties":{"override":{"type":"boolean"}}}`)
					}
				}
				return extension.AdmissionResult{Candidate: &candidate}, nil
			}, nil); err != nil {
				return nil, err
			}
			var err error
			first, err = api.RegisterTool(definition, replacementTool{entered: entered, release: release, output: "old"}, nil)
			return nil, err
		},
	}
	ctx := context.Background()
	dataDir := t.TempDir()
	ref := model.ModelRef{Provider: "prov", Model: "m"}
	owner := openOwner(t, melta.Options{
		DataDir: dataDir, Configuration: melta.NewValueConfigurationSource(raw),
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{externalKeyEnv: {Value: externalKey, Defined: true}}},
		Defaults:    harness.SessionInputs{Model: &ref}, Extensions: []extension.Extension{guest},
	})
	bound := caller(t, owner)
	session, err := owner.CreateSession(ctx, bound, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.SubmitSession(ctx, bound, session.SessionId, protocol.SubmitRequest{
		OperationId: "first", Mode: protocol.SubmitRequestModeRegular, Content: []protocol.ContentPart{textPart(t, "first")},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("tool was not bound")
	}
	if _, err := api.RegisterTool(replacement, replacementTool{output: "new"}, first); err != nil {
		t.Fatal(err)
	}
	close(release)
	awaitOperation(t, owner, bound, session.SessionId, "first", protocol.OperationStatusSuccess)
	bodiesMu.Lock()
	requests := append([]string(nil), bodies...)
	bodiesMu.Unlock()
	if len(requests) < 2 {
		t.Fatalf("model requests = %d, want the first turn and the post-tool turn", len(requests))
	}
	firstTools := advertisedProbe(t, requests[0])
	nextTools := advertisedProbe(t, requests[1])
	if firstTools.Description != "admission override" {
		t.Fatalf("first advertisement = %+v, want the admission override", firstTools)
	}
	if nextTools.Description != "admission override" || !strings.Contains(string(nextTools.Parameters), `"override"`) {
		t.Fatalf("post-tool advertisement = %+v, want the retained admission override", nextTools)
	}
	if strings.Contains(string(nextTools.Parameters), `"replacement"`) {
		t.Fatalf("post-tool advertisement carried the replaced registration: %+v", nextTools)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRequestHookReplacementKeepsRecordedRegistration(t *testing.T) {
	ctx := context.Background()
	server := newScriptedModel(t)
	dataDir := t.TempDir()
	workspace := t.TempDir()
	var config map[string]json.RawMessage
	if err := json.Unmarshal(configurationDocument(server.URL), &config); err != nil {
		t.Fatal(err)
	}
	config["extensions"] = json.RawMessage(`{"fixture":{"enabled":true}}`)
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var api extension.API
	var first extension.Registration
	guest := extension.Extension{
		Manifest: extension.Manifest{ID: "fixture", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(_ context.Context, registered extension.API) (func(context.Context) error, error) {
			api = registered
			var err error
			first, err = api.AttachRequest(extension.HookModePure, func(context.Context, extension.AuthorContext, extension.AuthorRequestInput) (harness.RequestConsequence, error) {
				close(entered)
				<-release
				return harness.RequestConsequence{}, nil
			}, nil)
			return nil, err
		},
	}
	ref := model.ModelRef{Provider: "prov", Model: "m"}
	owner := openOwner(t, melta.Options{
		DataDir: dataDir, Configuration: melta.NewValueConfigurationSource(raw),
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{externalKeyEnv: {Value: externalKey, Defined: true}}},
		Defaults:    harness.SessionInputs{Model: &ref}, Extensions: []extension.Extension{guest},
	})
	bound := caller(t, owner)
	session, err := owner.CreateSession(ctx, bound, workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.SubmitSession(ctx, bound, session.SessionId, protocol.SubmitRequest{
		OperationId: "first", Mode: protocol.SubmitRequestModeRegular,
		Content: []protocol.ContentPart{textPart(t, "first")},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first hook did not run")
	}
	secondCalled := make(chan struct{}, 1)
	_, err = api.AttachRequest(extension.HookModePure, func(context.Context, extension.AuthorContext, extension.AuthorRequestInput) (harness.RequestConsequence, error) {
		secondCalled <- struct{}{}
		return harness.RequestConsequence{}, nil
	}, first)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	awaitOperation(t, owner, bound, session.SessionId, "first", protocol.OperationStatusSuccess)
	submit(t, owner, bound, session.SessionId, "second", "second")
	select {
	case <-secondCalled:
	default:
		t.Fatal("replacement hook did not run")
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dataDir, "melta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ids []string
	for _, operation := range []string{"first", "second"} {
		var payload []byte
		if err := db.QueryRow(`SELECT payload FROM entries WHERE operation_id = ? AND kind = 'assistant'`, operation).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var record struct {
			Evidence struct {
				HookIDs []harness.HookIdentity `json:"hook_ids"`
			} `json:"evidence"`
		}
		if err := json.Unmarshal(payload, &record); err != nil {
			t.Fatal(err)
		}
		if len(record.Evidence.HookIDs) != 1 || record.Evidence.HookIDs[0].ExtensionID != "fixture" || record.Evidence.HookIDs[0].HookID == "" {
			t.Fatalf("%s evidence = %+v", operation, record.Evidence)
		}
		ids = append(ids, record.Evidence.HookIDs[0].HookID)
	}
	if ids[0] == ids[1] {
		t.Fatalf("replacement reused registration identity %q", ids[0])
	}
}

// TestReusedActivationKeepsPerSessionCandidates proves one activation reused
// across Sessions keeps each Session's own model, prompt and empty tool list in
// its own candidate and stored admission, with a stable hook identity.
func TestReusedActivationKeepsPerSessionCandidates(t *testing.T) {
	ctx := context.Background()
	server := newScriptedModel(t)
	var config map[string]any
	if err := json.Unmarshal(configurationDocument(server.URL), &config); err != nil {
		t.Fatal(err)
	}
	providers := config["providers"].(map[string]any)
	models := providers["prov"].(map[string]any)["models"].(map[string]any)
	models["b"] = map[string]any{"name": "B", "context_window": 65536}
	config["extensions"] = map[string]any{"choices": map[string]any{"enabled": true}}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var activations atomic.Int32
	observed := make(chan extension.AdmissionInput, 2)
	guest := extension.Extension{
		Manifest: extension.Manifest{ID: "choices", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: json.RawMessage(`{"type":"object"}`)},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			activations.Add(1)
			_, err := api.AttachAdmission(func(_ context.Context, _ extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
				observed <- input
				return extension.AdmissionResult{}, nil
			}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachRequest(extension.HookModePure, func(context.Context, extension.AuthorContext, extension.AuthorRequestInput) (harness.RequestConsequence, error) {
				return harness.RequestConsequence{}, nil
			}, nil)
			return nil, err
		},
	}
	dataDir := t.TempDir()
	owner := openOwner(t, melta.Options{
		DataDir: dataDir, Configuration: melta.NewValueConfigurationSource(raw),
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{externalKeyEnv: {Value: externalKey, Defined: true}}},
		Extensions:  []extension.Extension{guest},
	})
	defer owner.Close(ctx)
	bound := caller(t, owner)
	workspace := t.TempDir()
	for i, name := range []string{"m", "b"} {
		ref := protocol.ModelRef("prov/" + name)
		prompt := "prompt-" + name
		tools := []string{}
		session, err := owner.CreateSession(ctx, bound, workspace, &protocol.SessionInputs{Model: &ref, Prompt: &prompt, Tools: &tools})
		if err != nil {
			t.Fatal(err)
		}
		submit(t, owner, bound, session.SessionId, "choices-"+name, "hello")
		input := <-observed
		if input.Candidate.Model.Model != name || input.Candidate.Prompt == nil || *input.Candidate.Prompt != prompt || len(input.Candidate.Tools) != 0 {
			t.Fatalf("session %d candidate = %+v, want model %q, prompt %q and an empty tool list", i, input.Candidate, name, prompt)
		}
	}
	if activations.Load() != 1 {
		t.Fatalf("activations = %d, want one", activations.Load())
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dataDir, "melta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var hookID string
	for _, name := range []string{"m", "b"} {
		var payload []byte
		if err := db.QueryRow(`SELECT payload FROM registers WHERE operation_id = ? AND kind = 'operation'`, "choices-"+name).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var operation struct {
			Admission struct {
				Inputs    harness.SessionInputs `json:"inputs"`
				Execution struct {
					Model struct {
						Provider string `json:"provider"`
						Model    string `json:"model"`
					} `json:"model"`
					InitialToolNames []string `json:"initial_tool_names"`
				} `json:"execution"`
			} `json:"admission"`
		}
		if err := json.Unmarshal(payload, &operation); err != nil {
			t.Fatal(err)
		}
		if operation.Admission.Inputs.Model == nil || operation.Admission.Inputs.Model.Model != name ||
			operation.Admission.Inputs.Prompt == nil || *operation.Admission.Inputs.Prompt != "prompt-"+name ||
			operation.Admission.Execution.Model.Model != name || len(operation.Admission.Execution.InitialToolNames) != 0 {
			t.Fatalf("stored admission = %+v", operation.Admission)
		}
		if err := db.QueryRow(`SELECT payload FROM entries WHERE operation_id = ? AND kind = 'assistant'`, "choices-"+name).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var record struct {
			Evidence struct {
				HookIDs []harness.HookIdentity `json:"hook_ids"`
			} `json:"evidence"`
		}
		if err := json.Unmarshal(payload, &record); err != nil || len(record.Evidence.HookIDs) != 1 || record.Evidence.HookIDs[0].ExtensionID != "choices" || record.Evidence.HookIDs[0].HookID == "" {
			t.Fatalf("hook evidence = %+v, %v", record.Evidence, err)
		}
		if hookID != "" && hookID != record.Evidence.HookIDs[0].HookID {
			t.Fatalf("reused activation changed hook identity: %q to %q", hookID, record.Evidence.HookIDs[0].HookID)
		}
		hookID = record.Evidence.HookIDs[0].HookID
	}
}

// TestSelectedFullDefinitionReplacesCompletePrompt proves a selected full-mode
// Agents definition replaces a Session's complete prompt with the default
// assembly plus the definition's instructions.
func TestSelectedFullDefinitionReplacesCompletePrompt(t *testing.T) {
	ctx := context.Background()
	server := newScriptedModel(t)
	var config map[string]any
	if err := json.Unmarshal(configurationDocument(server.URL), &config); err != nil {
		t.Fatal(err)
	}
	config["extensions"] = map[string]any{"agents": map[string]any{
		"enabled": true,
		"definitions": map[string]any{"author": map[string]any{
			"system_prompt": "full",
			"prompt":        "Follow the author instructions.",
		}},
	}}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	owner := openOwner(t, melta.Options{
		DataDir: t.TempDir(), Configuration: melta.NewValueConfigurationSource(raw),
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{externalKeyEnv: {Value: externalKey, Defined: true}}},
		Identity:    "external full-mode identity",
		Extensions:  []extension.Extension{agents.Author()},
	})
	defer owner.Close(ctx)
	bound := caller(t, owner)
	ref := protocol.ModelRef("prov/m")
	complete := "session complete prompt"
	tools := []string{}
	session, err := owner.CreateSession(ctx, bound, t.TempDir(), &protocol.SessionInputs{Model: &ref, Prompt: &complete, Tools: &tools})
	if err != nil {
		t.Fatal(err)
	}
	arguments, err := json.Marshal(map[string]string{"agent": "author"})
	if err != nil {
		t.Fatal(err)
	}
	rawArguments := json.RawMessage(arguments)
	_, err = owner.InvokeExtension(ctx, bound, protocol.ExtensionInvocationRequest{
		ExtensionId: "agents", Operation: "select", RequestId: "full-prompt-select",
		Target:    protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindSession, SessionId: &session.SessionId},
		Arguments: &rawArguments,
	})
	if err != nil {
		t.Fatal(err)
	}
	submit(t, owner, bound, session.SessionId, "op-full-prompt", "hello")
	bodies, _ := server.requests()
	if len(bodies) == 0 {
		t.Fatal("the model received no request")
	}
	system := wireSystemText(bodies[0])
	if !strings.Contains(system, "external full-mode identity") || !strings.Contains(system, "Follow the author instructions.") || !strings.Contains(system, "## Your Role and Instructions") {
		t.Fatalf("system prompt = %q, want the default assembly plus the definition instructions", system)
	}
	if strings.Contains(system, "session complete prompt") {
		t.Fatalf("system prompt = %q, want the Session's complete prompt replaced", system)
	}
}

// wireSystemText returns the system message text of one chat body.
func wireSystemText(body string) string {
	var doc struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal([]byte(body), &doc) != nil {
		return ""
	}
	for _, message := range doc.Messages {
		if message.Role != "system" {
			continue
		}
		var text string
		if json.Unmarshal(message.Content, &text) == nil {
			return text
		}
		var parts []struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(message.Content, &parts) == nil {
			for _, part := range parts {
				text += part.Text
			}
			return text
		}
	}
	return ""
}

func TestOrdinaryAndAgentsCreateUseDifferentModels(t *testing.T) {
	ctx := context.Background()
	server := newScriptedModel(t)
	var config map[string]any
	if err := json.Unmarshal(configurationDocument(server.URL), &config); err != nil {
		t.Fatal(err)
	}
	providers := config["providers"].(map[string]any)
	provider := providers["prov"].(map[string]any)
	models := provider["models"].(map[string]any)
	models["b"] = map[string]any{"name": "B", "context_window": 65536}
	config["extensions"] = map[string]any{"agents": map[string]any{
		"enabled": true, "definitions": map[string]any{"primary": map[string]any{"model": "prov/b"}},
	}}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.ModelRef{Provider: "prov", Model: "m"}
	owner := openOwner(t, melta.Options{
		DataDir: t.TempDir(), Configuration: melta.NewValueConfigurationSource(raw),
		Credentials: &memoryCredentials{values: map[string]melta.CredentialValue{externalKeyEnv: {Value: externalKey, Defined: true}}},
		Defaults:    harness.SessionInputs{Model: &ref}, Extensions: []extension.Extension{agents.Author()},
	})
	defer owner.Close(ctx)
	bound := caller(t, owner)
	workspace := t.TempDir()
	extensions, err := owner.ListExtensions(ctx, bound)
	if err != nil || len(extensions) != 1 || extensions[0].ExtensionId != "agents" {
		t.Fatalf("extension discovery = %+v, %v", extensions, err)
	}
	foundCreate := false
	for _, operation := range extensions[0].Operations {
		foundCreate = foundCreate || (operation.Method.Name == "create" && operation.Target == protocol.RegisteredExtensionOperationTargetWorkspace)
	}
	if !foundCreate {
		t.Fatalf("agents create missing from discovery: %+v", extensions[0])
	}
	ordinary, err := owner.CreateSession(ctx, bound, workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	submit(t, owner, bound, ordinary.SessionId, "ordinary", "ordinary")
	args := json.RawMessage(`{}`)
	result, err := owner.InvokeExtension(ctx, bound, protocol.ExtensionInvocationRequest{
		ExtensionId: "agents", Operation: "create", RequestId: "create-agent",
		Target:    protocol.ExtensionInvocationTarget{Kind: protocol.ExtensionInvocationTargetKindWorkspace, Workspace: &workspace},
		Arguments: &args,
	})
	if err != nil || result.Failure != nil {
		t.Fatalf("agents create = %+v, %v", result, err)
	}
	var created struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(result.Value, &created); err != nil || created.SessionID == "" {
		t.Fatalf("agents create result = %s, %v", result.Value, err)
	}
	submit(t, owner, bound, created.SessionID, "selected", "selected")
	bodies, _ := server.requests()
	if len(bodies) != 2 {
		t.Fatalf("requests = %d", len(bodies))
	}
	for i, want := range []string{"m", "b"} {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal([]byte(bodies[i]), &body); err != nil || body.Model != want {
			t.Fatalf("request %d model = %q, want %q (%v)", i, body.Model, want, err)
		}
	}
}

func TestActivationCredentialIsScoped(t *testing.T) {
	var activationValue string
	var activationErr, operationErr, undeclaredErr error
	guest := extension.Extension{
		Manifest: extension.Manifest{
			ID: "credential", Version: "1", APIVersion: extension.APIVersion,
			SettingsSchema: json.RawMessage(`{"type":"object"}`), Services: []string{extension.ContractCredentials},
			Secrets: []extension.SecretRef{
				{Field: "/activation", Lifetime: extension.SecretActivation},
				{Field: "/operation", Lifetime: extension.SecretOperation},
			},
		},
		Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
			result, err := api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/activation"}`))
			activationErr = err
			if err == nil {
				var body struct {
					Value string `json:"value"`
				}
				_ = json.Unmarshal(result.Value, &body)
				activationValue = body.Value
			}
			_, operationErr = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/operation"}`))
			_, undeclaredErr = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/undeclared"}`))
			return nil, nil
		},
	}
	credentials := &memoryCredentials{values: map[string]melta.CredentialValue{
		"ACTIVATION": {Value: "activation-secret", Defined: true},
		"OPERATION":  {Value: "operation-secret", Defined: true},
	}}
	opts := melta.Options{
		DataDir:       t.TempDir(),
		Configuration: melta.NewValueConfigurationSource([]byte(`{"extensions":{"credential":{"enabled":true,"activation":"ACTIVATION","operation":"OPERATION"}}}`)),
		Credentials:   credentials, Extensions: []extension.Extension{guest},
	}
	owner := openOwner(t, opts)
	defer owner.Close(context.Background())
	if activationErr != nil || activationValue != "activation-secret" || operationErr == nil || undeclaredErr == nil {
		t.Fatalf("activation=%q err=%v operation=%v undeclared=%v", activationValue, activationErr, operationErr, undeclaredErr)
	}
}
