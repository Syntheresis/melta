package melta

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

type registrarTestTool struct{ name string }

type registrarContext struct {
	context.Context
	check func()
}

func (c registrarContext) Err() error {
	c.check()
	return c.Context.Err()
}

func registrarManifest(id string) extension.Manifest {
	return extension.Manifest{ID: id, Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)}
}

func TestRegistrarServiceAuthorityChangeReplacesActivation(t *testing.T) {
	owner := &configurationService{}
	manifest := registrarManifest("authority")
	manifest.Services = []string{"melta.first"}
	e := extension.Extension{Manifest: manifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return nil, nil
	}}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if err := first.publish(); err != nil {
		t.Fatal(err)
	}
	e.Manifest.Services = []string{"melta.second"}
	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if first.activations[0] == second.activations[0] {
		t.Fatal("activation reused after service authority changed")
	}
}

func TestRegistrarServiceAuthorityIntersection(t *testing.T) {
	manifest := registrarManifest("authority")
	manifest.Services = []string{"melta.one", "melta.two"}
	e := extension.Extension{Manifest: manifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return nil, nil
	}}
	for _, test := range []struct {
		name     string
		approved []string
		want     map[string]bool
	}{
		{"unrestricted", nil, map[string]bool{"melta.one": true, "melta.two": true}},
		{"none", []string{}, map[string]bool{}},
		{"intersection", []string{"melta.one", "melta.other"}, map[string]bool{"melta.one": true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := &configurationService{}
			candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
				extension: e, section: []byte(`{}`), approvedServices: test.approved,
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer candidate.release()
			if got := candidate.activations[0].services; !reflect.DeepEqual(got, test.want) {
				t.Fatalf("services = %v, want %v", got, test.want)
			}
			if len(test.approved) > 0 {
				test.approved[0] = "changed"
				if !candidate.activations[0].services["melta.one"] {
					t.Fatal("activation authority changed with host input")
				}
			}
		})
	}
}

func TestRegistrarServiceAuthorityNarrowingReplacesActivation(t *testing.T) {
	owner := &configurationService{}
	manifest := registrarManifest("authority")
	manifest.Services = []string{"melta.one"}
	e := extension.Extension{Manifest: manifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return nil, nil
	}}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if err := first.publish(); err != nil {
		t.Fatal(err)
	}
	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`), approvedServices: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if first.activations[0] == second.activations[0] {
		t.Fatal("activation reused after host narrowed service authority")
	}
}

func TestScopedServicePreservesIssuingContextValues(t *testing.T) {
	type key struct{}
	origin, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "origin"))
	scope := &nativeCallLifetime{ctx: origin, cancel: cancel}
	defer scope.end()
	supplied := context.WithValue(context.Background(), key{}, "forged")
	merged, finish, err := scope.admit(supplied)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if got := merged.Value(key{}); got != "origin" {
		t.Fatalf("callback context value = %v, want issuing context", got)
	}
}

func TestScopedServiceNestedProviderIdentityAndState(t *testing.T) {
	owner := &configurationService{}
	method := func(name string) extension.Method {
		return extension.Method{Name: name, Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)}
	}
	provider := extension.Extension{Manifest: registrarManifest("provider"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "inner/opaque", Methods: []extension.RegisteredMethod{{
			Method: method("call/opaque"),
			Handler: func(_ context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				if call.ExtensionID != "provider" || string(call.Settings) != `{"owner":"provider"}` || string(call.State) != `{"state":"provider"}` || call.Session.SessionID != "session" {
					return extension.Result{}, errors.New("wrong serving context")
				}
				return extension.Result{Value: []byte(`null`)}, nil
			},
		}}}, nil)
		return nil, err
	}}
	outerManifest := registrarManifest("outer")
	outer := extension.Extension{Manifest: outerManifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "outer/opaque", Methods: []extension.RegisteredMethod{{
			Method: method("call/opaque"),
			Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
				if call.ExtensionID != "outer" || string(call.State) != `{"state":"outer"}` {
					return extension.Result{}, errors.New("wrong outer context")
				}
				return call.Call(ctx, "inner/opaque", "call/opaque", args)
			},
		}}}, nil)
		return nil, err
	}}
	callerManifest := registrarManifest("caller")
	caller := extension.Extension{Manifest: callerManifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: provider, section: []byte(`{"owner":"provider"}`)},
		{extension: outer, section: []byte(`{}`)},
		{extension: caller, section: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	ctx, cancel := context.WithCancel(context.Background())
	scope := &nativeCallScope{
		nativeCallLifetime: nativeCallLifetime{ctx: ctx, cancel: cancel},
		candidate:          candidate, activation: candidate.activations[2], pure: true,
		session: harness.SessionIdentity{SessionID: "session"},
		states:  map[string]harness.SessionStateValue{"outer": []byte(`{"state":"outer"}`), "provider": []byte(`{"state":"provider"}`)},
	}
	defer scope.end()
	result, err := scope.call(ctx, "outer/opaque", "call/opaque", []byte(`{}`))
	if err != nil || string(result.Value) != "null" {
		t.Fatalf("nested provider: %s %v", result.Value, err)
	}
}

func TestRegistrarReplaceRenamesAndPreservesConflict(t *testing.T) {
	owner := &configurationService{}
	var api extension.API
	var old extension.Registration
	e := extension.Extension{Manifest: extension.Manifest{ID: "a", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(_ context.Context, a extension.API) (func(context.Context) error, error) {
			api = a
			var err error
			old, err = a.RegisterTool(model.ToolDefinition{Name: "old", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
			if err == nil {
				_, err = a.RegisterTool(model.ToolDefinition{Name: "occupied", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
			}
			return nil, err
		}}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if _, err := api.RegisterTool(model.ToolDefinition{Name: "occupied", Parameters: []byte(`{}`)}, registrarTestTool{}, old); err == nil {
		t.Fatal("conflicting replacement accepted")
	}
	current := c.snapshot()
	if current.tools["old"] == nil {
		t.Fatal("failed replacement removed old registration")
	}
	current.release()
	replacement, err := api.RegisterTool(model.ToolDefinition{Name: "new", Parameters: []byte(`{}`)}, registrarTestTool{}, old)
	if err != nil {
		t.Fatal(err)
	}
	current = c.snapshot()
	if current.tools["old"] != nil || current.tools["new"] == nil {
		t.Fatal("rename did not replace")
	}
	current.release()
	cleanup, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error { return nil }, replacement)
	if err != nil {
		t.Fatal(err)
	}
	current = c.snapshot()
	defer current.release()
	if current.tools["new"] != nil || len(current.hooks["cleanup"]) != 1 {
		t.Fatal("cross-kind replacement failed")
	}
	if err := api.Remove(replacement); err != nil {
		t.Fatal(err)
	}
	if err := api.Remove(cleanup); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrarRegistrationReleaseAfterStageAndPlan(t *testing.T) {
	owner := &configurationService{}
	var api extension.API
	var original extension.Registration
	e := extension.Extension{Manifest: registrarManifest("release"), Activate: func(_ context.Context, a extension.API) (func(context.Context) error, error) {
		api = a
		var err error
		original, err = a.RegisterTool(model.ToolDefinition{Name: "tool", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
		return nil, err
	}}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	released := 0
	original.(*nativeRegistration).release = func() { released++ }
	stage := candidate.snapshot()
	binding := stage.bindTool("tool")
	if _, err := api.RegisterTool(model.ToolDefinition{Name: "tool", Parameters: []byte(`{}`)}, registrarTestTool{}, original); err != nil {
		t.Fatal(err)
	}
	if released != 0 {
		t.Fatal("registration released while stage and plan hold it")
	}
	stage.release()
	if released != 0 {
		t.Fatal("registration released while plan holds it")
	}
	binding.release()
	if released != 1 {
		t.Fatalf("release count = %d", released)
	}
}

func TestRegistrarCandidateCredentialHolds(t *testing.T) {
	owner := &configurationService{}
	e := extension.Extension{Manifest: registrarManifest("credential"), Activate: func(_ context.Context, _ extension.API) (func(context.Context) error, error) {
		if !slices.Contains(owner.heldCredentialNames(), "CANDIDATE_SECRET") {
			t.Error("candidate secret was not held before activation")
		}
		return nil, nil
	}}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
		extension: e, section: []byte(`{}`), bindings: map[string]declaredBinding{"/secret": {name: "CANDIDATE_SECRET"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	stage := candidate.snapshot()
	candidate.release()
	if !slices.Contains(owner.heldCredentialNames(), "CANDIDATE_SECRET") {
		t.Fatal("captured stage lost secret exclusion")
	}
	stage.release()
	if slices.Contains(owner.heldCredentialNames(), "CANDIDATE_SECRET") {
		t.Fatal("secret exclusion leaked after final release")
	}
}

func TestRegistrarOperationTupleAndCleanupAttachments(t *testing.T) {
	owner := &configurationService{}
	handler := func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
		return extension.Result{Value: []byte(`null`)}, nil
	}
	spec := func(id string, names ...string) nativeActivationSpec {
		return nativeActivationSpec{section: []byte(`{}`), extension: extension.Extension{
			Manifest: registrarManifest(id),
			Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
				for _, name := range names {
					op := extension.Operation{Target: extension.ExposedTargetSession, Method: extension.Method{Name: name, Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)}, Handler: handler}
					if _, err := api.RegisterOperation(op, nil); err != nil {
						return nil, err
					}
					if _, err := api.RegisterOperation(op, nil); err == nil {
						return nil, errors.New("duplicate operation accepted")
					}
				}
				for range 2 {
					if _, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error { return nil }, nil); err != nil {
						return nil, err
					}
				}
				return nil, nil
			},
		}}
	}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{spec("a", "shared", "b\x00c"), spec("a\x00b", "shared", "c")})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if len(c.inactive) != 0 {
		t.Fatal(c.inactive)
	}
	stage := c.snapshot()
	defer stage.release()
	for _, key := range []nativeOperationKey{{"a", "shared"}, {"a\x00b", "shared"}, {"a", "b\x00c"}, {"a\x00b", "c"}} {
		if stage.operations[key] == nil {
			t.Fatalf("missing operation %#v", key)
		}
	}
	if len(stage.operations) != 4 || len(stage.hooks["cleanup"]) != 4 {
		t.Fatal("operations or cleanups collapsed")
	}
}

func TestRegistrarInvalidEntriesLeaveSiblingActive(t *testing.T) {
	owner := &configurationService{}
	badAPI := registrarManifest("bad-api")
	badAPI.APIVersion = "unsupported"
	host := registrarManifest(extension.HostProvider)
	badSection := registrarManifest("bad-section")
	badSection.SettingsSchema = []byte(`{"type":"object"}`)
	starts := 0
	active := func(context.Context, extension.API) (func(context.Context) error, error) { starts++; return nil, nil }
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: extension.Extension{Manifest: badAPI, Activate: active}, section: []byte(`{}`)},
		{extension: extension.Extension{Manifest: host, Activate: active}, section: []byte(`{}`)},
		{extension: extension.Extension{Manifest: badSection, Activate: active}, section: []byte(`[]`)},
		{extension: extension.Extension{Manifest: registrarManifest("missing")}, section: []byte(`{}`)},
		{extension: extension.Extension{Manifest: registrarManifest("good"), Activate: active}, section: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	for _, id := range []string{"bad-api", extension.HostProvider, "bad-section", "missing"} {
		if c.inactive[id] == nil {
			t.Fatalf("%q was not inactive", id)
		}
	}
	if c.inactive["good"] != nil || starts != 1 {
		t.Fatalf("sibling inactive: %v, starts %d", c.inactive, starts)
	}
}

func TestRegistrarActivationOrder(t *testing.T) {
	owner := &configurationService{}
	var order []string
	spec := func(id string, installed bool) nativeActivationSpec {
		return nativeActivationSpec{installed: installed, section: []byte(`{}`), extension: extension.Extension{
			Manifest: extension.Manifest{ID: id, Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
			Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
				order = append(order, id)
				return nil, nil
			},
		}}
	}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		spec("z", true), spec("host-z", false), spec("a", true), spec("host-a", false),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if !reflect.DeepEqual(order, []string{"host-z", "host-a", "a", "z"}) {
		t.Fatal(order)
	}
}

func TestRegistrarAmbiguousIdentityInactive(t *testing.T) {
	owner := &configurationService{}
	starts := 0
	e := extension.Extension{Manifest: extension.Manifest{ID: "same", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(context.Context, extension.API) (func(context.Context) error, error) { starts++; return nil, nil }}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}, {extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if c.inactive["same"] == nil || starts != 0 {
		t.Fatalf("ambiguous ID: %v, starts=%d", c.inactive, starts)
	}
}

func (t registrarTestTool) Normalize(extension.AuthorContext, model.ToolCall) (json.RawMessage, error) {
	return json.RawMessage(t.name), nil
}

func TestRegistrarReuseReconcilesHeldClaims(t *testing.T) {
	owner := &configurationService{}
	manifest := registrarManifest
	starts := map[string]int{}
	var aAPI extension.API
	a := extension.Extension{Manifest: manifest("a"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		starts["a"]++
		aAPI = api
		return nil, nil
	}}
	b := extension.Extension{Manifest: manifest("b"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		starts["b"]++
		_, err := api.RegisterTool(model.ToolDefinition{Name: "held", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
		return nil, err
	}}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: a, section: []byte(`{"v":1}`)}, {extension: b, section: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if err := first.publish(); err != nil {
		t.Fatal(err)
	}
	a.Activate = func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		starts["a"]++
		_, err := api.RegisterTool(model.ToolDefinition{Name: "held", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
		return nil, err
	}
	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: a, section: []byte(`{"v":2}`)}, {extension: b, section: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if second.inactive["a"] == nil || second.inactive["b"] != nil {
		t.Fatalf("claims not reconciled: %v", second.inactive)
	}
	if !reflect.DeepEqual(starts, map[string]int{"a": 2, "b": 1}) {
		t.Fatalf("unrelated activation restarted: %v", starts)
	}
	if _, err := aAPI.RegisterTool(model.ToolDefinition{Name: "held", Parameters: []byte(`{}`)}, registrarTestTool{}, nil); err == nil {
		t.Fatal("shared configuration collision accepted")
	}
}

func TestRegistrarCredentialBindingReuseIsPerExtension(t *testing.T) {
	owner := &configurationService{}
	starts := map[string]int{}
	makeSpec := func(id, value string) nativeActivationSpec {
		return nativeActivationSpec{
			section: []byte(`{}`), bindings: map[string]declaredBinding{"/key": {name: "key", value: value}},
			extension: extension.Extension{Manifest: extension.Manifest{ID: id, Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
				Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
					starts[id]++
					return nil, nil
				}},
		}
	}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{makeSpec("a", "1"), makeSpec("b", "1")})
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if err := first.publish(); err != nil {
		t.Fatal(err)
	}
	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{makeSpec("a", "1"), makeSpec("b", "2")})
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if !reflect.DeepEqual(starts, map[string]int{"a": 1, "b": 2}) {
		t.Fatal(starts)
	}
}

func TestRegistrarSharedActivationChecksEveryHeldConfiguration(t *testing.T) {
	owner := &configurationService{}
	var api extension.API
	manifest := registrarManifest
	a := extension.Extension{Manifest: manifest("a"), Activate: func(_ context.Context, value extension.API) (func(context.Context) error, error) {
		api = value
		return nil, nil
	}}
	b := extension.Extension{Manifest: manifest("b"), Activate: func(_ context.Context, value extension.API) (func(context.Context) error, error) {
		name := "old"
		if string(value.Settings()) == `{"v":2}` {
			name = "new"
		}
		_, err := value.RegisterTool(model.ToolDefinition{Name: name, Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
		return nil, err
	}}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: a, section: []byte(`{}`)}, {extension: b, section: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if err := first.publish(); err != nil {
		t.Fatal(err)
	}
	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: a, section: []byte(`{}`)}, {extension: b, section: []byte(`{"v":2}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if len(second.inactive) != 0 {
		t.Fatal(second.inactive)
	}
	for _, name := range []string{"old", "new"} {
		if _, err := api.RegisterTool(model.ToolDefinition{Name: name, Parameters: []byte(`{}`)}, registrarTestTool{}, nil); err == nil {
			t.Fatalf("shared activation displaced %q", name)
		}
	}
}

func TestRegistrarReusedConflictsRemainPrivate(t *testing.T) {
	owner := &configurationService{}
	native := func(id string) nativeActivationSpec {
		return nativeActivationSpec{extension: extension.Extension{
			Manifest: extension.Manifest{ID: id, Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
			Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
				_, err := api.RegisterTool(model.ToolDefinition{Name: "same", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
				return nil, err
			},
		}, section: []byte(`{}`)}
	}
	a, b := native("a"), native("b")
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{a})
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{b})
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if err := second.publish(); err != nil {
		t.Fatal(err)
	}
	combined, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{a, b})
	if err != nil {
		t.Fatal(err)
	}
	defer combined.release()
	if combined.inactive["a"] == nil {
		t.Fatalf("conflicting held activation published: %v", combined.inactive)
	}
	stage := combined.snapshot()
	defer stage.release()
	if got := stage.tools["same"].activation.id; got != "b" {
		t.Fatalf("holder displaced by %q", got)
	}
}

func TestRegistrarSuccessiveActivationsCanShareNames(t *testing.T) {
	owner := &configurationService{}
	starts := 0
	e := extension.Extension{
		Manifest: extension.Manifest{ID: "entry", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			starts++
			_, err := api.RegisterTool(model.ToolDefinition{Name: "shared", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
			return nil, err
		},
	}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{"v":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{"v":2}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if len(first.inactive) != 0 || len(second.inactive) != 0 || starts != 2 {
		t.Fatalf("%v %v starts %d", first.inactive, second.inactive, starts)
	}
	a, b := first.snapshot(), second.snapshot()
	defer a.release()
	defer b.release()
	if a.tools["shared"].activation == b.tools["shared"].activation {
		t.Fatal("replacement activation was reused")
	}
}

func TestRegistrarReenableDoesNotReuseRetiredActivation(t *testing.T) {
	owner := &configurationService{}
	starts := 0
	e := extension.Extension{Manifest: extension.Manifest{ID: "entry", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(context.Context, extension.API) (func(context.Context) error, error) { starts++; return nil, nil }}
	spec := []nativeActivationSpec{{extension: e, section: []byte(`{}`)}}
	first, err := owner.prepareNativeCandidate(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if err := first.publish(); err != nil {
		t.Fatal(err)
	}
	disabled, err := owner.prepareNativeCandidate(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.release()
	if err := disabled.publish(); err != nil {
		t.Fatal(err)
	}
	again, err := owner.prepareNativeCandidate(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	defer again.release()
	if starts != 2 {
		t.Fatalf("retired activation was reused: starts=%d", starts)
	}
}

func TestRegistrarEntryServiceVisibility(t *testing.T) {
	owner := &configurationService{}
	manifest := registrarManifest
	getCalls := 0
	service := func(name string) extension.Service {
		return extension.Service{Name: name, Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "get", Mode: extension.ModePure, Input: []byte(`{"type":"object"}`), Success: []byte(`{"type":"string"}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				getCalls++
				return extension.Result{Value: []byte(`"ok"`)}, nil
			},
		}, {
			Method: extension.Method{Name: "bad_result", Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{"type":"string"}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{Value: []byte(`42`)}, nil
			},
		}, {
			Method: extension.Method{Name: "bad_partial", Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
				return extension.Result{}, &extension.CallError{Err: harness.ErrStorage, Partial: []byte(`{`)}
			},
		}}}
	}
	first := extension.Extension{Manifest: manifest("first"), Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterService(service("own"), nil); err != nil {
			return nil, err
		}
		if _, err := api.Call(ctx, "own", "get", []byte(`{}`)); err == nil {
			t.Fatal("unpublished service visible")
		}
		return nil, nil
	}}
	failed := extension.Extension{Manifest: manifest("failed"), Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		value, err := api.Call(ctx, "own", "get", []byte(`{}`))
		if err != nil || string(value.Value) != `"ok"` {
			t.Fatalf("ready service: %s, %v", value.Value, err)
		}
		_, err = api.Call(ctx, "own", "missing", []byte(`{}`))
		if !errors.Is(err, harness.ErrNotFound) {
			t.Fatalf("missing method: %v", err)
		}
		_, err = api.Call(ctx, "missing", "get", []byte(`{}`))
		var failure *worker.Error
		if !errors.As(err, &failure) || failure.Class != worker.ClassUnavailable {
			t.Fatalf("missing service: %v", err)
		}
		if _, err := api.RegisterService(service("private"), nil); err != nil {
			return nil, err
		}
		return nil, errors.New("failed")
	}}
	last := extension.Extension{Manifest: manifest("last"), Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.Call(ctx, "private", "get", []byte(`{}`)); err == nil {
			t.Fatal("failed entry visible")
		}
		before := getCalls
		if _, err := api.Call(ctx, "own", "get", []byte(`[]`)); !errors.Is(err, extension.ErrInvalid) || ErrorCode(err) != protocol.ErrorCodeInvalid || getCalls != before {
			t.Fatalf("invalid arguments: err=%v handler calls=%d", err, getCalls-before)
		}
		for _, method := range []string{"bad_result", "bad_partial"} {
			_, err := api.Call(ctx, "own", method, []byte(`{}`))
			if !errors.Is(err, extension.ErrInvalid) {
				t.Fatalf("%s: want invalid error, got %v", method, err)
			}
			var callErr *extension.CallError
			if method == "bad_partial" && errors.As(err, &callErr) {
				t.Fatalf("malformed partial exposed: %v", err)
			}
		}
		checked, locked := false, false
		supplied := registrarContext{Context: ctx, check: func() {
			checked = true
			if !owner.nativeMu.TryLock() {
				locked = true
			} else {
				owner.nativeMu.Unlock()
			}
		}}
		if _, err := api.Call(supplied, "own", "get", []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if !checked || locked {
			t.Fatalf("supplied context checked with owner lock: checked=%v locked=%v", checked, locked)
		}
		return nil, nil
	}}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: first, section: []byte(`{}`)}, {extension: failed, section: []byte(`{}`)}, {extension: last, section: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if c.inactive["failed"] == nil || c.inactive["last"] != nil {
		t.Fatal(c.inactive)
	}
}

func TestRegistrarEntryCallExpiresAndJoins(t *testing.T) {
	owner := &configurationService{}
	entered, settled := make(chan struct{}), make(chan struct{})
	manifest := registrarManifest
	provider := extension.Extension{Manifest: manifest("provider"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "svc", Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "call", Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				close(entered)
				<-ctx.Done()
				close(settled)
				return extension.Result{}, ctx.Err()
			},
		}}}, nil)
		return nil, err
	}}
	caller := extension.Extension{Manifest: manifest("caller"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		go func() { _, _ = api.Call(context.Background(), "svc", "call", []byte(`{}`)) }()
		<-entered
		return nil, nil
	}}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: provider, section: []byte(`{}`)}, {extension: caller, section: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	select {
	case <-settled:
	default:
		t.Fatal("entry returned before its service call settled")
	}
}

func TestRegistrarCanceledEntryJoinsBeforeClosingAndReportsErrors(t *testing.T) {
	owner := &configurationService{}
	entered, canceled, proceed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	callerClose, providerClose := errors.New("caller close"), errors.New("provider close")
	provider := extension.Extension{Manifest: registrarManifest("provider"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "svc", Methods: []extension.RegisteredMethod{{
			Method: extension.Method{Name: "call", Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)},
			Handler: func(ctx context.Context, _ extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-proceed
				return extension.Result{}, ctx.Err()
			},
		}}}, nil)
		return func(context.Context) error { return providerClose }, err
	}}
	caller := extension.Extension{Manifest: registrarManifest("caller"), Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		go func() { _, _ = api.Call(context.Background(), "svc", "call", []byte(`{}`)) }()
		<-entered
		<-ctx.Done()
		return func(context.Context) error { return callerClose }, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := owner.prepareNativeCandidate(ctx, []nativeActivationSpec{{extension: provider, section: []byte(`{}`)}, {extension: caller, section: []byte(`{}`)}})
		done <- err
	}()
	<-entered
	owner.nativeMu.Lock()
	for candidate := range owner.nativeCandidates {
		if candidate.holds != 1 {
			t.Errorf("entry borrowed candidate hold: %d", candidate.holds)
		}
	}
	owner.nativeMu.Unlock()
	cancel()
	<-canceled
	select {
	case err := <-done:
		t.Fatalf("returned before callback settled: %v", err)
	default:
	}
	close(proceed)
	err := <-done
	if !errors.Is(err, context.Canceled) || !errors.Is(err, callerClose) || !errors.Is(err, providerClose) {
		t.Fatalf("incomplete cancellation/close errors: %v", err)
	}
}

func TestRegistrarNestedServiceScope(t *testing.T) {
	owner := &configurationService{}
	var borrowed func(context.Context, string, string, json.RawMessage) (extension.Result, error)
	method := func(name string, mode extension.Mode) extension.Method {
		return extension.Method{Name: name, Mode: mode, Input: []byte(`{}`), Success: []byte(`{}`)}
	}
	e := extension.Extension{
		Manifest: extension.Manifest{ID: "provider", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterService(extension.Service{Name: "service", Methods: []extension.RegisteredMethod{
				{Method: method("nested", extension.ModePure), Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
					return extension.Result{Value: []byte(`null`)}, nil
				}},
				{Method: method("effect", extension.ModeServiceIO), Handler: func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
					return extension.Result{Value: []byte(`null`)}, nil
				}},
				{Method: method("outer", extension.ModePure), Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
					if call.Call == nil {
						return extension.Result{}, errors.New("missing scoped call")
					}
					borrowed = call.Call
					if _, err := call.Call(ctx, "service", "nested", []byte(`{}`)); err != nil {
						return extension.Result{}, err
					}
					if _, err := call.Call(ctx, "service", "effect", []byte(`{}`)); !errors.Is(err, ErrDenied) {
						return extension.Result{}, errors.New("pure call invoked effectful method")
					}
					return extension.Result{Value: []byte(`null`)}, nil
				}},
			}}, nil)
			return nil, err
		},
	}
	caller := extension.Extension{Manifest: extension.Manifest{ID: "caller", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)}, Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.Call(ctx, "service", "outer", []byte(`{}`)); err != nil {
			return nil, err
		}
		if _, err := borrowed(ctx, "service", "nested", []byte(`{}`)); !errors.Is(err, extension.ErrClosed) {
			return nil, errors.New("borrowed callback remained usable")
		}
		return nil, nil
	}}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}, {extension: caller, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if len(c.inactive) != 0 {
		t.Fatal(c.inactive)
	}
}

func TestRegistrarReentrantReplacementDoesNotWait(t *testing.T) {
	owner := &configurationService{}
	entered, proceed, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var api extension.API
	var handle extension.Registration
	e := extension.Extension{
		Manifest: extension.Manifest{ID: "entry", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(_ context.Context, a extension.API) (func(context.Context) error, error) {
			api = a
			var err error
			handle, err = a.AttachArguments(extension.HookModePure, func(context.Context, extension.AuthorContext, model.ToolCall) (extension.ArgumentsResult, error) {
				close(entered)
				<-proceed
				_, err := api.AttachArguments(extension.HookModePure, func(context.Context, extension.AuthorContext, model.ToolCall) (extension.ArgumentsResult, error) {
					return extension.ArgumentsResult{}, nil
				}, handle)
				return extension.ArgumentsResult{}, err
			}, nil)
			return nil, err
		},
	}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	stage := c.snapshot()
	defer stage.release()
	h := stage.hooks["arguments"][0].value.(nativeHook).handler.(extension.ArgumentsHook)
	go func() { _, err := h(context.Background(), extension.AuthorContext{}, model.ToolCall{}); done <- err }()
	<-entered
	// A different attachment must finish while the original callback is parked.
	if _, err := api.AttachResult(func(context.Context, extension.AuthorContext, harness.ToolOutputInput) (string, error) {
		return "", nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	close(proceed)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRegistrarCancellationReleasesOnlyCandidateHolds(t *testing.T) {
	owner := &configurationService{}
	entered, proceed := make(chan struct{}), make(chan struct{})
	closedA, closedB := 0, 0
	manifest := registrarManifest
	a := extension.Extension{Manifest: manifest("a"), Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return func(context.Context) error { closedA++; return nil }, nil
	}}
	b := extension.Extension{Manifest: manifest("b"), Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		close(entered)
		<-proceed
		return func(context.Context) error { closedB++; return nil }, nil
	}}
	first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: a, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.publish(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := owner.prepareNativeCandidate(ctx, []nativeActivationSpec{{extension: a, section: []byte(`{}`)}, {extension: b, section: []byte(`{}`)}})
		done <- err
	}()
	<-entered
	cancel()
	close(proceed)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if closedA != 0 || closedB != 1 {
		t.Fatalf("closed A=%d B=%d", closedA, closedB)
	}
	first.release()
	if closedA != 1 {
		t.Fatalf("A closed %d times", closedA)
	}
}

func TestRegistrarCloseMayReenter(t *testing.T) {
	owner := &configurationService{}
	closed := make(chan error, 1)
	e := extension.Extension{Manifest: extension.Manifest{ID: "entry", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			return func(context.Context) error {
				_, err := api.RegisterTool(model.ToolDefinition{Name: "late", Parameters: []byte(`{}`)}, registrarTestTool{}, nil)
				closed <- err
				return nil
			}, nil
		}}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	c.release()
	if err := <-closed; !errors.Is(err, extension.ErrClosed) {
		t.Fatal(err)
	}
	if err := c.publish(); !errors.Is(err, extension.ErrClosed) {
		t.Fatal(err)
	}
	if c.snapshot() != nil {
		t.Fatal("released candidate produced a snapshot")
	}
}

func TestRegistrarParkedStageReplacementAndRelease(t *testing.T) {
	owner := &configurationService{}
	var api extension.API
	var old extension.Registration
	var closed []string
	hook := func(label string) extension.ArgumentsHook {
		return func(context.Context, extension.AuthorContext, model.ToolCall) (extension.ArgumentsResult, error) {
			return extension.ArgumentsResult{Arguments: []byte(label)}, nil
		}
	}
	manifest := registrarManifest
	first := extension.Extension{Manifest: manifest("first"), Activate: func(_ context.Context, a extension.API) (func(context.Context) error, error) {
		api = a
		var err error
		old, err = api.AttachArguments(extension.HookModePure, hook("old"), nil)
		if err != nil {
			return nil, err
		}
		_, err = api.AttachArguments(extension.HookModePure, hook("second"), nil)
		return func(context.Context) error { closed = append(closed, "first"); return nil }, err
	}}
	second := extension.Extension{Manifest: manifest("second"), Activate: func(_ context.Context, a extension.API) (func(context.Context) error, error) {
		_, err := a.AttachArguments(extension.HookModePure, hook("other"), nil)
		return func(context.Context) error { closed = append(closed, "second"); return nil }, err
	}}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: first, section: []byte(`{}`)}, {extension: second, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	parked := candidate.snapshot()
	replacement, err := api.AttachArguments(extension.HookModePure, hook("new"), old)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.Remove(old); err != nil {
		t.Fatal(err)
	}
	if _, err := api.AttachArguments(extension.HookModePure, hook("bad"), old); err == nil {
		t.Fatal("stale replacement accepted")
	}
	current := candidate.snapshot()
	labels := func(s *nativeSnapshot) []string {
		var out []string
		for _, entry := range s.hooks["arguments"] {
			h := entry.value.(nativeHook).handler.(extension.ArgumentsHook)
			result, err := h(context.Background(), extension.AuthorContext{}, model.ToolCall{})
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, string(result.Arguments))
		}
		return out
	}
	if got := labels(parked); !reflect.DeepEqual(got, []string{"old", "second", "other"}) {
		t.Fatal(got)
	}
	if got := labels(current); !reflect.DeepEqual(got, []string{"second", "new", "other"}) {
		t.Fatal(got)
	}
	if err := api.Remove(replacement); err != nil {
		t.Fatal(err)
	}
	candidate.release()
	current.release()
	if len(closed) != 0 {
		t.Fatalf("closed while stage retained: %v", closed)
	}
	parked.release()
	if !reflect.DeepEqual(closed, []string{"second", "first"}) {
		t.Fatalf("close order: %v", closed)
	}
}
func (t registrarTestTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Data: []byte(t.name)}
}
func (t registrarTestTool) Execute(_ context.Context, _ extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	return harness.ToolOutcome{Result: model.ToolResult{Content: t.name + ":" + string(plan.Data)}}
}

func TestRegistrationAllContributions(t *testing.T) {
	owner := &configurationService{}
	handler := func(context.Context, extension.AuthorContext, json.RawMessage) (extension.Result, error) {
		return extension.Result{Value: []byte(`null`)}, nil
	}
	method := extension.Method{Name: "run", Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)}
	e := extension.Extension{
		Manifest: extension.Manifest{ID: "entry", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			if _, err := api.RegisterTool(model.ToolDefinition{Name: "custom", Parameters: []byte(`{}`)}, registrarTestTool{}, nil); err != nil {
				return nil, err
			}
			if _, err := api.RegisterStrategy(*harness.IncludedStrategyBinding(), nil); err != nil {
				return nil, err
			}
			if _, err := api.RegisterService(extension.Service{Name: "svc", Methods: []extension.RegisteredMethod{{Method: method, Handler: handler}}}, nil); err != nil {
				return nil, err
			}
			if _, err := api.RegisterOperation(extension.Operation{Target: extension.ExposedTargetSession, Method: method, Handler: handler}, nil); err != nil {
				return nil, err
			}
			if _, err := api.AttachAdmission(func(context.Context, extension.AuthorContext, extension.AdmissionInput) (extension.AdmissionResult, error) {
				return extension.AdmissionResult{}, nil
			}, nil); err != nil {
				return nil, err
			}
			if _, err := api.AttachRequest(extension.HookModeEffectful, func(context.Context, extension.AuthorContext, harness.RequestHookInput) (harness.RequestConsequence, error) {
				return harness.RequestConsequence{}, nil
			}, nil); err != nil {
				return nil, err
			}
			if _, err := api.AttachArguments(extension.HookModePure, func(context.Context, extension.AuthorContext, model.ToolCall) (extension.ArgumentsResult, error) {
				return extension.ArgumentsResult{}, nil
			}, nil); err != nil {
				return nil, err
			}
			if _, err := api.AttachAuthorization(func(context.Context, extension.AuthorContext, extension.AuthorizationInput) (*string, error) {
				return nil, nil
			}, nil); err != nil {
				return nil, err
			}
			if _, err := api.AttachResult(func(context.Context, extension.AuthorContext, harness.ToolOutputInput) (string, error) {
				return "", nil
			}, nil); err != nil {
				return nil, err
			}
			if _, err := api.RegisterCleanup(func(context.Context, extension.AuthorContext, harness.SessionIdentity) error { return nil }, nil); err != nil {
				return nil, err
			}
			return nil, nil
		},
	}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if len(c.inactive) != 0 {
		t.Fatal(c.inactive)
	}
	stage := c.snapshot()
	defer stage.release()
	if len(stage.tools) != 1 || len(stage.strategies) != 1 || len(stage.services) != 1 || len(stage.operations) != 1 {
		t.Fatal(stage)
	}
	for _, kind := range []string{"admission", "request", "arguments", "authorization", "result", "cleanup"} {
		if len(stage.hooks[kind]) != 1 {
			t.Fatalf("%s: %v", kind, stage.hooks[kind])
		}
	}
}

func TestRegistrationRejectsInvalidAndForeignHandles(t *testing.T) {
	owner := &configurationService{}
	var first extension.Registration
	manifest := registrarManifest
	def := model.ToolDefinition{Name: "custom", Parameters: []byte(`{}`)}
	a := extension.Extension{Manifest: manifest("a"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		var err error
		first, err = api.RegisterTool(def, registrarTestTool{}, nil)
		if err != nil {
			return nil, err
		}
		if _, err := api.RegisterTool(def, registrarTestTool{}, nil); err == nil {
			t.Fatal("duplicate accepted")
		}
		if _, err := api.RegisterTool(model.ToolDefinition{Name: "read_file", Parameters: []byte(`{}`)}, registrarTestTool{}, nil); err == nil {
			t.Fatal("standard tool replaced")
		}
		if _, err := api.RegisterTool(model.ToolDefinition{Name: "bad", Parameters: []byte(`[]`)}, registrarTestTool{}, nil); err == nil {
			t.Fatal("invalid tool accepted")
		}
		if _, err := api.AttachRequest(extension.HookMode("invalid"), func(context.Context, extension.AuthorContext, harness.RequestHookInput) (harness.RequestConsequence, error) {
			return harness.RequestConsequence{}, nil
		}, nil); err == nil {
			t.Fatal("invalid hook mode accepted")
		}
		return nil, nil
	}}
	b := extension.Extension{Manifest: manifest("b"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterTool(def, registrarTestTool{}, first); err == nil {
			t.Fatal("foreign replacement accepted")
		}
		if err := api.Remove(first); err == nil {
			t.Fatal("foreign removal accepted")
		}
		return nil, nil
	}}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: a, section: []byte(`{}`)}, {extension: b, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.release()
	if len(c.inactive) != 0 {
		t.Fatal(c.inactive)
	}
	stage := c.snapshot()
	defer stage.release()
	if len(stage.tools) != 1 || stage.tools["custom"].activation.id != "a" {
		t.Fatal("holder displaced")
	}
}

func TestRegistrarPreparedToolRetainsIdentity(t *testing.T) {
	owner := &configurationService{}
	var api extension.API
	var original extension.Registration
	var closed bool
	definition := model.ToolDefinition{Name: "tool", Parameters: []byte(`{}`)}
	e := extension.Extension{
		Manifest: extension.Manifest{ID: "entry", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{}`)},
		Activate: func(_ context.Context, a extension.API) (func(context.Context) error, error) {
			api = a
			var err error
			original, err = a.RegisterTool(definition, registrarTestTool{name: `"old"`}, nil)
			return func(context.Context) error { closed = true; return nil }, err
		},
	}
	c, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	stage := c.snapshot()
	binding := stage.bindTool("tool")
	if binding == nil {
		t.Fatal("missing binding")
	}
	prepared := binding.entry.value.(nativeTool).tool.Prepare(context.Background(), extension.AuthorContext{}, model.ToolCall{})
	if _, err := api.RegisterTool(definition, registrarTestTool{name: `"new"`}, original); err != nil {
		t.Fatal(err)
	}
	stage.release()
	if stage.bindTool("tool") != nil {
		t.Fatal("released stage admitted a binding")
	}
	stage.release()
	c.release()
	if closed {
		t.Fatal("closed while prepared binding held")
	}
	result := binding.entry.value.(nativeTool).tool.Execute(context.Background(), extension.AuthorContext{}, prepared)
	if result.Result.Content != `"old":"old"` {
		t.Fatalf("bound implementation: %s", result.Result.Content)
	}
	binding.release()
	binding.release()
	if !closed {
		t.Fatal("activation not closed on final binding release")
	}
}

func TestRegistrarCandidateIsolation(t *testing.T) {
	fail := errors.New("entry failed")
	definition := model.ToolDefinition{Name: "example", Parameters: []byte(`{}`)}
	manifest := registrarManifest
	owner := &configurationService{}
	first := extension.Extension{Manifest: manifest("first"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterTool(definition, registrarTestTool{}, nil); err != nil {
			return nil, err
		}
		return nil, fail
	}}
	second := extension.Extension{Manifest: manifest("second"), Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(definition, registrarTestTool{}, nil)
		return nil, err
	}}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{
		{extension: first, section: []byte(`{}`)},
		{extension: second, section: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.release()
	if !errors.Is(candidate.inactive["first"], fail) || candidate.inactive["second"] != nil {
		t.Fatalf("inactive: %v", candidate.inactive)
	}
	stage := candidate.snapshot()
	defer stage.release()
	if got := stage.tools["example"]; got == nil {
		t.Fatal("successful entry was not published")
	}
}
