package melta

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/catalog"
	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

// credentialAuthor builds one author extension with the credential fixture
// manifest: the credentials service requested and the given secret refs.
func credentialAuthor(t *testing.T, id string, secrets []extension.SecretRef, activate func(context.Context, extension.API) (func(context.Context) error, error)) extension.Extension {
	t.Helper()
	manifest := registrarManifest(id)
	manifest.SettingsSchema = json.RawMessage(`{"type":"object","properties":{"enabled":{"type":"boolean"}},"required":["enabled"]}`)
	manifest.Services = []string{extension.ContractCredentials}
	manifest.Secrets = secrets
	return extension.Extension{Manifest: manifest, Activate: activate}
}

func credentialValue(t *testing.T, result extension.Result, err error) string {
	t.Helper()
	testassert.NoError(t, err)
	var value struct {
		Value string `json:"value"`
	}
	testassert.NoError(t, json.Unmarshal(result.Value, &value))
	return value.Value
}

// TestCredentialsActivationReadsResolveAndRefuseUniformly proves the
// credential host contract at activation: a declared activation field
// resolves from the candidate's captured binding and stays available to the
// activation's later calls; an operation field and a declared field with no
// captured binding refuse with the uniform unavailable message; an undeclared
// field refuses with the declaration message.
func TestCredentialsActivationReadsResolveAndRefuseUniformly(t *testing.T) {
	const (
		activationName = "MELTA_CRED_ACTIVATION"
		operationName  = "MELTA_CRED_OPERATION"
	)
	t.Setenv(activationName, "activation-value")
	t.Setenv(operationName, "operation-value")

	var openActivation, openOperationErr, openMissingErr, openUndeclaredErr string
	author := credentialAuthor(t, "cred", []extension.SecretRef{
		{Field: "/activation", Lifetime: extension.SecretActivation},
		{Field: "/operation", Lifetime: extension.SecretOperation},
		{Field: "/missing", Lifetime: extension.SecretOperation},
	}, func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		result, err := api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/activation"}`))
		openActivation = credentialValue(t, result, err)
		_, err = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/operation"}`))
		testassert.Require(t, err != nil, "an operation credential resolved during activation")
		openOperationErr = err.Error()
		_, err = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/missing"}`))
		testassert.Require(t, err != nil, "an unresolved operation field resolved")
		openMissingErr = err.Error()
		_, err = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/undeclared"}`))
		testassert.Require(t, err != nil, "an undeclared credential field resolved")
		openUndeclaredErr = err.Error()
		// The activation capture stays available to every later call of the
		// activation that opened it.
		result, err = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/activation"}`))
		testassert.Equal(t, credentialValue(t, result, err), "activation-value")
		return nil, nil
	})
	h := newServiceHarness(t)
	writeServiceFile(t, h.configPath, credentialServiceConfig(`{"cred":{"enabled":true,"activation":"`+activationName+`","operation":"`+operationName+`"}}`))
	svc := h.preparedService(context.Background(), author)
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	defer svc.current().resources.release()

	testassert.Equal(t, openActivation, "activation-value")
	testassert.Require(t, strings.Contains(openOperationErr, `credential "/operation" is unavailable`) && !strings.Contains(openOperationErr, "outside an Operation"), "operation read during activation = %v, want the uniform unavailable refusal", openOperationErr)
	testassert.Require(t, strings.Contains(openMissingErr, `credential "/missing" is unavailable`) && !strings.Contains(openMissingErr, "outside an Operation"), "missing operation field read = %v, want the uniform unavailable refusal", openMissingErr)
	testassert.Require(t, strings.Contains(openUndeclaredErr, "is not declared"), "undeclared field read = %v, want the declaration refusal", openUndeclaredErr)
}

// TestOperationCredentialsUsePreparedOpeningCapture proves the operation
// credential value sampled at the preparation boundary is the one retained
// through the prepared opener, even when the environment changes while the
// admitted opener is parked before Operation construction.
func TestOperationCredentialsUsePreparedOpeningCapture(t *testing.T) {
	const name = "MELTA_OP_PREPARED"
	t.Setenv(name, "prepared-value")
	h := newServiceHarness(t)
	probe := &credentialGateProbe{}
	writeServiceFile(t, h.configPath, credentialServiceConfig(`{"credprobe":{"enabled":true,"activation":"`+name+`","operation":"`+name+`"}}`))
	svc := h.preparedService(context.Background(), probe.author(t))
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	defer svc.current().resources.release()

	artifacts := newArtifactIntervals()
	p := newPreparation(svc, h.locations(), "", nil, newObservationAdapter(newObservation(), newWarningStore()), artifacts, nil)
	sessionID := testSessionID
	prepared, err := p.bind()(context.Background(), harness.PreparationRequest{
		Session: harness.PreparationSession{
			Identity: harness.SessionIdentity{SessionID: sessionID, Workspace: h.dataDir},
			Inputs:   harness.SessionInputs{Model: &model.ModelRef{Provider: "prov", Model: "m"}, Prompt: new(string), Tools: []string{"cred_probe"}},
		},
		RequestKind: harness.RequestKindMessage,
	})
	testassert.NoError(t, err)
	defer prepared.Release()
	var provenanceSettings string
	for _, implementation := range prepared.Capture.Implementations {
		if implementation.ExtensionID == "credprobe" {
			provenanceSettings = string(implementation.Settings)
		}
	}
	testassert.Require(t, strings.Contains(provenanceSettings, name), "provenance settings = %q, want the declared reference name", provenanceSettings)
	testassert.Require(t, !strings.Contains(provenanceSettings, "prepared-value"), "provenance settings = %q, want no captured credential value", provenanceSettings)
	key := sessionArtifactDir(svc.dataDir, sessionID)
	releaseHolder, err := artifacts.acquireExecution(context.Background(), key)
	testassert.NoError(t, err)
	var execution harness.Execution
	var openErr error
	openDone := make(chan struct{})
	go func() {
		execution, openErr = prepared.Open(context.Background(), harness.OperationAdmission{SessionID: sessionID, OperationID: "op-prepared", RequestKind: harness.RequestKindMessage, AdmittedAt: time.Now()})
		close(openDone)
	}()
	awaitRegistryState(t, artifacts, key, 2)
	t.Setenv(name, "changed-value")
	releaseHolder()
	<-openDone
	testassert.NoError(t, openErr)
	defer func() { testassert.NoError(t, execution.Close()) }()

	inv, err := execution.OpenInvocation(context.Background(), harness.InvocationRequest{ID: "inv-prepared", Purpose: harness.InvocationPurposeConversation, Model: prepared.Capture.Model})
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, inv.Close()) }()
	outcome := fixtureBinding(inv, "cred_probe").Run(context.Background(), model.ToolCall{ID: "call-prepared", Name: "cred_probe", Arguments: json.RawMessage(`{"key":"probe"}`)})
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "tool outcome = %+v, want success", outcome.Result)
	probe.mu.Lock()
	defer probe.mu.Unlock()
	testassert.Equal(t, probe.values["probe"], "prepared-value|prepared-value")
}

// TestOperationCredentialNamesShareOnePreparationCapture proves two active
// extensions resolving the same credential name derive their Operation fields
// from the same preparation capture.
func TestOperationCredentialNamesShareOnePreparationCapture(t *testing.T) {
	const name = "MELTA_OP_SHARED"
	t.Setenv(name, "shared-value")
	h := newServiceHarness(t)
	first := &credentialGateProbe{}
	second := &credentialGateProbe{}
	writeServiceFile(t, h.configPath, credentialServiceConfig(`{"creda":{"enabled":true,"activation":"`+name+`","operation":"`+name+`"},"credb":{"enabled":true,"activation":"`+name+`","operation":"`+name+`"}}`))
	svc := h.preparedService(context.Background(), first.authorFor(t, "creda", "cred_probe_a"), second.authorFor(t, "credb", "cred_probe_b"))
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	defer svc.current().resources.release()

	p := newPreparation(svc, h.locations(), "", nil, newObservationAdapter(newObservation(), newWarningStore()), newArtifactIntervals(), nil)
	sessionID := testSessionID
	prepared, err := p.bind()(context.Background(), harness.PreparationRequest{
		Session: harness.PreparationSession{
			Identity: harness.SessionIdentity{SessionID: sessionID, Workspace: h.dataDir},
			Inputs:   harness.SessionInputs{Model: &model.ModelRef{Provider: "prov", Model: "m"}, Prompt: new(string), Tools: []string{"cred_probe_a", "cred_probe_b"}},
		},
		RequestKind: harness.RequestKindMessage,
	})
	testassert.NoError(t, err)
	defer prepared.Release()

	t.Setenv(name, "changed-value")
	execution, err := prepared.Open(context.Background(), harness.OperationAdmission{SessionID: sessionID, OperationID: "op-shared", RequestKind: harness.RequestKindMessage, AdmittedAt: time.Now()})
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, execution.Close()) }()
	inv, err := execution.OpenInvocation(context.Background(), harness.InvocationRequest{ID: "inv-shared", Purpose: harness.InvocationPurposeConversation, Model: prepared.Capture.Model})
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, inv.Close()) }()
	fixtureBinding(inv, "cred_probe_a").Run(context.Background(), model.ToolCall{ID: "call-a", Name: "cred_probe_a", Arguments: json.RawMessage(`{"key":"a"}`)})
	fixtureBinding(inv, "cred_probe_b").Run(context.Background(), model.ToolCall{ID: "call-b", Name: "cred_probe_b", Arguments: json.RawMessage(`{"key":"b"}`)})
	first.mu.Lock()
	second.mu.Lock()
	defer first.mu.Unlock()
	defer second.mu.Unlock()
	testassert.Equal(t, first.values["a"], "shared-value|shared-value")
	testassert.Equal(t, second.values["b"], "shared-value|shared-value")
}

// TestDeclaredCredentialRotationReplacesResources proves a candidate whose
// active sections are unchanged but whose declared binding value rotated
// re-activates with the new captured value and prepares a replacement
// resource set, while an identical candidate reuses the current activation
// and resource set.
func TestDeclaredCredentialRotationReplacesResources(t *testing.T) {
	const name = "MELTA_RES_ROTATE"
	t.Setenv(name, "one")
	h := newServiceHarness(t)
	var opens []string
	author := credentialAuthor(t, "credres", []extension.SecretRef{{Field: "/activation", Lifetime: extension.SecretActivation}}, func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		result, err := api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/activation"}`))
		opens = append(opens, credentialValue(t, result, err))
		return nil, nil
	})
	writeServiceFile(t, h.configPath, credentialServiceConfig(`{"credres":{"enabled":true,"activation":"`+name+`"}}`))
	svc := h.preparedService(context.Background(), author)
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	first := svc.current().resources
	testassert.Equal(t, strings.Join(opens, ","), "one")

	t.Setenv(name, "two")
	_, err = svc.publish(context.Background())
	testassert.NoError(t, err)
	second := svc.current().resources
	testassert.Require(t, second != first, "a rotated declared binding reused the old resource set")
	testassert.Equal(t, strings.Join(opens, ","), "one,two")

	_, err = svc.publish(context.Background())
	testassert.NoError(t, err)
	third := svc.current().resources
	testassert.Require(t, third == second, "an identical candidate replaced the resource set")
	testassert.Equal(t, strings.Join(opens, ","), "one,two")
	defer svc.current().resources.release()
}

// TestExternalSharedCredentialRotationKeepsProviderObservation proves an
// external rotation of a provider/extension shared name after startup, with
// no reload, keeps the provider's ordinary fresh observation and the
// preparation's Operation value from that same observation, while activation
// retains its candidate binding.
func TestExternalSharedCredentialRotationKeepsProviderObservation(t *testing.T) {
	const name = "MELTA_SHARED_PROVIDER"
	t.Setenv(name, "shared-candidate")
	h := newServiceHarness(t)
	author := credentialAuthor(t, "credshared", []extension.SecretRef{
		{Field: "/activation", Lifetime: extension.SecretActivation},
		{Field: "/operation", Lifetime: extension.SecretOperation},
	}, func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil })
	writeServiceFile(t, h.configPath, `{"providers":{"prov":`+nominalProvMember("http://prov.test/v1", name, `{"m":{"name":"M","context_window":4096}}`)+`},"extensions":{"credshared":{"enabled":true,"activation":"`+name+`","operation":"`+name+`"}}}`)
	svc := h.preparedService(context.Background(), author)
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	defer svc.current().resources.release()
	t.Setenv(name, "shared-later")
	_, err = svc.publish(context.Background())
	testassert.NoError(t, err)

	captured, err := svc.capture(context.Background())
	testassert.NoError(t, err)
	testassert.Equal(t, captured.snapshot.credentials[name].Value, "shared-later")
	binding := captured.snapshot.declaredBindings[declaredField{extensionID: "credshared", field: "/operation"}]
	testassert.Equal(t, binding.value, "shared-later")
	activationBinding := captured.snapshot.declaredBindings[declaredField{extensionID: "credshared", field: "/activation"}]
	testassert.Equal(t, activationBinding.value, "shared-later")
	operations := captured.operationSecrets()
	testassert.Equal(t, operations["credshared"]["/operation"], "shared-later")
	settings := projectSettings(captured.snapshot)
	projected := settings.Extensions["credshared"]
	testassert.Require(t, strings.Contains(projected, name), "settings extension document = %q, want the reference name", projected)
	testassert.Require(t, !strings.Contains(projected, "shared-candidate") && !strings.Contains(projected, "shared-later"), "settings extension document = %q, want no credential value", projected)
}

// TestManagedKeyEditRefreshesSharedDeclaredBinding proves a connection edit
// that writes a managed key shared with a declared reference publishes the
// new binding and replaces the resource set instead of reusing stale values.
func TestManagedKeyEditRefreshesSharedDeclaredBinding(t *testing.T) {
	const name = "MELTA_SHARED_MANAGED"
	h := newServiceHarness(t)
	t.Cleanup(func() { _ = os.Unsetenv(name) })
	managed := config.NewManagedEnv(filepath.Join(h.home, ".melta", ".env"))
	testassert.NoError(t, managed.TrySet(name, "old-value"))
	author := credentialAuthor(t, "credmanaged", []extension.SecretRef{
		{Field: "/activation", Lifetime: extension.SecretActivation},
		{Field: "/operation", Lifetime: extension.SecretOperation},
	}, func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil })
	writeServiceFile(t, h.configPath, `{"providers":{"prov":`+nominalProvMember("http://prov.test/v1", name, `{"m":{"name":"M","context_window":4096}}`)+`},"extensions":{"credmanaged":{"enabled":true,"activation":"`+name+`","operation":"`+name+`"}}}`)
	svc := h.preparedService(context.Background(), author)
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	first := svc.current().resources
	provider := svc.current().catalog.Providers["prov"]
	testassert.Require(t, provider != nil, "provider fixture is missing")

	mutation, err := svc.mutate(context.Background(), svc.editProviderConnect(&connectionEffects{
		providerID: "prov", transport: provider.Transport, keyAction: keyActionSet, keyEnv: name, keyValue: "new-value",
	}))
	testassert.NoError(t, err)
	second := svc.current().resources
	testassert.Require(t, second != first, "a managed-key edit reused the stale resource set")
	binding := mutation.snapshot.declaredBindings[declaredField{extensionID: "credmanaged", field: "/operation"}]
	testassert.Equal(t, binding.value, "new-value")
	testassert.Equal(t, captureValueForTest(t, managed, name), "new-value")
	captured, err := svc.capture(context.Background())
	testassert.NoError(t, err)
	testassert.Equal(t, captured.snapshot.credentials[name].Value, "new-value")
	defer svc.current().resources.release()
}

// TestCredentialNestedProviderUsesItsOwnDeclaredField proves a nested
// registered service call runs as the serving extension: it resolves its own
// declared credential field, never the caller's.
func TestCredentialNestedProviderUsesItsOwnDeclaredField(t *testing.T) {
	const callerName = "MELTA_NESTED_CALLER"
	const providerName = "MELTA_NESTED_PROVIDER"
	t.Setenv(callerName, "caller-value")
	t.Setenv(providerName, "provider-value")
	h := newServiceHarness(t)
	providerAuthor := credentialAuthor(t, "credprovider", []extension.SecretRef{{Field: "/provider", Lifetime: extension.SecretActivation}}, func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterService(extension.Service{Name: "cred_provider", Methods: []extension.RegisteredMethod{{
			Method: extension.ServiceMethod("call", `{}`, `{"type":"object"}`, "", ""),
			Handler: func(ctx context.Context, call extension.AuthorContext, _ json.RawMessage) (extension.Result, error) {
				return call.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/provider"}`))
			},
		}}}, nil)
		return nil, err
	})
	callerAuthor := credentialAuthor(t, "credcaller", []extension.SecretRef{{Field: "/caller", Lifetime: extension.SecretActivation}}, func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		_, err := api.RegisterTool(model.ToolDefinition{Name: "cred_caller", Description: "reads its own credential and nests a provider call", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}, credentialNestedTool{}, nil)
		return nil, err
	})
	writeServiceFile(t, h.configPath, credentialServiceConfig(`{"credprovider":{"enabled":true,"provider":"`+providerName+`"},"credcaller":{"enabled":true,"caller":"`+callerName+`"}}`))
	svc := h.preparedService(context.Background(), providerAuthor, callerAuthor)
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	defer svc.current().resources.release()

	p := newPreparation(svc, h.locations(), "", nil, newObservationAdapter(newObservation(), newWarningStore()), nil, nil)
	prepared, err := p.bind()(context.Background(), harness.PreparationRequest{
		Session: harness.PreparationSession{
			Identity: harness.SessionIdentity{SessionID: testSessionID, Workspace: h.dataDir},
			Inputs:   harness.SessionInputs{Model: &model.ModelRef{Provider: "prov", Model: "m"}, Prompt: new(string), Tools: []string{"cred_caller"}},
		},
		RequestKind: harness.RequestKindMessage,
	})
	testassert.NoError(t, err)
	defer prepared.Release()
	execution, err := prepared.Open(context.Background(), harness.OperationAdmission{SessionID: testSessionID, OperationID: "op-nested", RequestKind: harness.RequestKindMessage, AdmittedAt: time.Now()})
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, execution.Close()) }()
	inv, err := execution.OpenInvocation(context.Background(), harness.InvocationRequest{ID: "inv-nested", Purpose: harness.InvocationPurposeConversation, Model: prepared.Capture.Model})
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, inv.Close()) }()
	outcome := fixtureBinding(inv, "cred_caller").Run(context.Background(), model.ToolCall{ID: "call-nested", Name: "cred_caller", Arguments: json.RawMessage(`{}`)})
	testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "nested outcome = %+v, want success", outcome.Result)
	testassert.Equal(t, outcome.Result.Content, "caller=caller-value|provider=provider-value")
}

// credentialNestedTool is one caller tool whose nested provider call reads
// the provider's own declared credential field.
type credentialNestedTool struct{}

func (credentialNestedTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}
func (credentialNestedTool) Prepare(context.Context, extension.AuthorContext, model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{}
}
func (credentialNestedTool) Execute(ctx context.Context, tc extension.AuthorContext, _ extension.ToolPlan) harness.ToolOutcome {
	own, err := tc.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/caller"}`))
	if err != nil {
		return credentialOutcome("call", "caller error: "+err.Error())
	}
	nested, err := tc.Call(ctx, "cred_provider", "call", json.RawMessage(`{}`))
	if err != nil {
		return credentialOutcome("call", "provider error: "+err.Error())
	}
	return credentialOutcome("call", "caller="+credentialString(own)+"|provider="+credentialString(nested))
}

// TestCredentialHookUsesOperationCapture proves every callback kind
// receives the retained Operation capture through the same handoff: the
// admission hook changes the declared environment variable to a third value
// before its own read, and every hook still observes the later value captured
// for the Operation.
func TestCredentialHookUsesOperationCapture(t *testing.T) {
	const name = "MELTA_EFFECTFUL_HOOK"
	t.Setenv(name, "hook-value")
	h := newServiceHarness(t)
	var mu sync.Mutex
	values := map[string]string{}
	var undeclared error
	read := func(kind string, call extension.AuthorContext, hookCtx context.Context) {
		result, err := call.Call(hookCtx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/operation"}`))
		if err != nil {
			t.Errorf("%s hook credential read: %v", kind, err)
			return
		}
		mu.Lock()
		values[kind] = credentialString(result)
		mu.Unlock()
	}
	author := credentialAuthor(t, "credhook", []extension.SecretRef{{Field: "/operation", Lifetime: extension.SecretOperation}}, func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
		if _, err := api.RegisterTool(testDescribeTool("cred_tool"), testRegisteredTool{}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachAdmission(func(ctx context.Context, call extension.AuthorContext, _ extension.AdmissionInput) (extension.AdmissionResult, error) {
			if err := os.Setenv(name, "third-value"); err != nil {
				return extension.AdmissionResult{}, err
			}
			read("admission", call, ctx)
			return extension.AdmissionResult{}, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachRequest(func(ctx context.Context, call extension.AuthorContext, _ extension.AuthorRequestInput) (harness.RequestConsequence, error) {
			read("request", call, ctx)
			return harness.RequestConsequence{}, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachArguments(func(ctx context.Context, call extension.AuthorContext, toolCall model.ToolCall) (extension.ArgumentsResult, error) {
			read("arguments", call, ctx)
			return extension.ArgumentsResult{Arguments: toolCall.Arguments}, nil
		}, nil); err != nil {
			return nil, err
		}
		if _, err := api.AttachToolCheck(func(ctx context.Context, call extension.AuthorContext, _ model.ToolCall) (*string, error) {
			read("check", call, ctx)
			return nil, nil
		}, nil); err != nil {
			return nil, err
		}
		_, err := api.AttachResult(func(ctx context.Context, call extension.AuthorContext, input harness.ToolOutputInput) (string, error) {
			read("result", call, ctx)
			_, undeclared = call.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/other"}`))
			return input.Content, nil
		}, nil)
		return nil, err
	})
	writeServiceFile(t, h.configPath, credentialServiceConfig(`{"credhook":{"enabled":true,"operation":"`+name+`"}}`))
	svc := h.preparedService(context.Background(), author)
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	defer svc.current().resources.release()
	t.Setenv(name, "later-value")
	{
		_, err := svc.publish(context.Background())
		testassert.NoError(t, err)
	}

	p := newPreparation(svc, h.locations(), "", nil, newObservationAdapter(newObservation(), newWarningStore()), nil, nil)
	prepared, err := p.bind()(context.Background(), harness.PreparationRequest{
		Session: harness.PreparationSession{
			Identity: harness.SessionIdentity{SessionID: testSessionID, Workspace: h.dataDir},
			Inputs:   harness.SessionInputs{Model: &model.ModelRef{Provider: "prov", Model: "m"}, Prompt: new(string), Tools: []string{"cred_tool"}},
		},
		RequestKind: harness.RequestKindMessage,
	})
	testassert.NoError(t, err)
	defer prepared.Release()
	testassert.Equal(t, os.Getenv(name), "third-value")
	execution, err := prepared.Open(context.Background(), harness.OperationAdmission{SessionID: testSessionID, OperationID: "op-hook", RequestKind: harness.RequestKindMessage, AdmittedAt: time.Now()})
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, execution.Close()) }()
	inv, err := execution.OpenInvocation(context.Background(), harness.InvocationRequest{ID: "inv-hook", Purpose: harness.InvocationPurposeConversation, Model: prepared.Capture.Model})
	testassert.NoError(t, err)
	defer func() { testassert.NoError(t, inv.Close()) }()

	request := inv.RequestStage()
	if request.Release != nil {
		defer request.Release()
	}
	testassert.NoError(t, request.Error)
	testassert.Require(t, len(request.Hooks) == 1, "request hooks = %d, want one", len(request.Hooks))
	_, err = request.Hooks[0].Run(context.Background(), harness.RequestHookInput{Purpose: harness.InvocationPurposeConversation, Model: prepared.Capture.Model})
	testassert.NoError(t, err)

	arguments := inv.ArgumentStage()
	if arguments.Release != nil {
		defer arguments.Release()
	}
	testassert.NoError(t, arguments.Error)
	testassert.Require(t, len(arguments.Hooks) == 1, "tool hooks = %d, want one", len(arguments.Hooks))
	_, err = arguments.Hooks[0].Run(context.Background(), model.ToolCall{ID: "call-hook", Name: "cred_tool", Arguments: json.RawMessage(`{}`)})
	testassert.NoError(t, err)

	check := inv.CheckStage()
	if check.Release != nil {
		defer check.Release()
	}
	testassert.NoError(t, check.Error)
	testassert.Require(t, len(check.Hooks) == 1, "check hooks = %d, want one", len(check.Hooks))
	_, err = check.Hooks[0].Run(context.Background(), model.ToolCall{})
	testassert.NoError(t, err)

	result := inv.ResultStage()
	if result.Release != nil {
		defer result.Release()
	}
	testassert.NoError(t, result.Error)
	testassert.Require(t, len(result.Hooks) == 1, "result hooks = %d, want one", len(result.Hooks))
	_, err = result.Hooks[0].Run(context.Background(), harness.ToolOutputInput{Content: "kept"})
	testassert.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	for _, kind := range []string{"admission", "request", "arguments", "check", "result"} {
		testassert.Equal(t, values[kind], "later-value")
	}
	testassert.Require(t, undeclared != nil, "an undeclared credential field resolved")
}

// TestCustomProviderCreateRefreshesSharedDeclaredBinding proves a custom
// provider create with a supplied key shared by an active extension carries
// that pending key into the candidate binding, publishes the managed key, and
// replaces the resource set for both an absent and a previously managed key.
func TestCustomProviderCreateRefreshesSharedDeclaredBinding(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		envName     string
		seedManaged bool
	}{
		{name: "previously absent", envName: "MELTA_CREATE_ABSENT"},
		{name: "previously managed", envName: "MELTA_CREATE_MANAGED", seedManaged: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newServiceHarness(t)
			t.Cleanup(func() { _ = os.Unsetenv(testCase.envName) })
			managed := config.NewManagedEnv(filepath.Join(h.home, ".melta", ".env"))
			if testCase.seedManaged {
				testassert.NoError(t, managed.TrySet(testCase.envName, "old-managed"))
			}
			var opens atomic.Int64
			author := credentialAuthor(t, "credcreate", []extension.SecretRef{
				{Field: "/activation", Lifetime: extension.SecretActivation},
				{Field: "/operation", Lifetime: extension.SecretOperation},
			}, func(context.Context, extension.API) (func(context.Context) error, error) {
				opens.Add(1)
				return nil, nil
			})
			writeServiceFile(t, h.configPath, credentialServiceConfig(`{"credcreate":{"enabled":true,"activation":"`+testCase.envName+`","operation":"`+testCase.envName+`"}}`))
			svc := h.preparedService(context.Background(), author)
			_, err := svc.publish(context.Background())
			testassert.NoError(t, err)
			firstResources := svc.current().resources
			testassert.Equal(t, opens.Load(), int64(1))

			const keyValue = "supplied-create-key"
			baseURL := "https://created.test/v1"
			window := 4096
			key := keyValue
			mutation, err := svc.mutate(context.Background(), svc.editProviderCreate(context.Background(), "newp", protocol.ProviderEdit{BaseUrl: &baseURL, ApiKeyEnv: &testCase.envName}, map[string]protocol.ModelEdit{"m": {ContextWindow: &window}}, &key))
			testassert.NoError(t, err)
			testassert.Require(t, svc.current().resources != firstResources, "custom create reused the stale resource set")
			testassert.Equal(t, opens.Load(), int64(2))
			binding := mutation.snapshot.declaredBindings[declaredField{extensionID: "credcreate", field: "/operation"}]
			testassert.Equal(t, binding.value, keyValue)
			provider := mutation.snapshot.catalog.Providers["newp"]
			testassert.Require(t, provider != nil && provider.Models["m"] != nil && provider.Models["m"].ContextWindow == window, "created provider = %+v, want the ready model", provider)
			captured, err := svc.capture(context.Background())
			testassert.NoError(t, err)
			observed := captured.snapshot.credentials[testCase.envName]
			testassert.Require(t, observed.Defined && observed.Managed && observed.Value == keyValue, "provider credential = %+v, want the supplied managed key", observed)
			testassert.Require(t, catalog.ProviderConnected(provider, capturedEnvIsSet(captured.snapshot.credentials)), "created provider = %+v, want the ready authenticated provider", provider)
			testassert.Equal(t, captured.operationSecrets()["credcreate"]["/operation"], keyValue)
			testassert.Equal(t, captureValueForTest(t, managed, testCase.envName), keyValue)
			defer svc.current().resources.release()
		})
	}
}

// TestDefinedEmptyExternalSharedKeyRefusesSuppliedConnect proves a shared
// declared binding does not reclassify a defined-empty external provider key:
// the external observation survives and a supplied-key connect refuses before
// any config, cache or managed-source effect.
func TestDefinedEmptyExternalSharedKeyRefusesSuppliedConnect(t *testing.T) {
	const name = "MELTA_DEFINED_EMPTY_SHARED"
	e := newProductionEnv(t, storage.NewMemory())
	t.Setenv(name, "")
	writeServiceFile(t, e.configPath, `{"providers":{"prov":`+nominalProvMember(e.server.URL, name, `{"m":{"name":"M","context_window":4096}}`)+`},"extensions":{"credexternal":{"enabled":true,"value":"`+name+`"}}}`)
	manifest := registrarManifest("credexternal")
	manifest.SettingsSchema = json.RawMessage(`{"type":"object","properties":{"enabled":{"type":"boolean"},"value":{"type":"string"}},"required":["enabled"]}`)
	manifest.Secrets = []extension.SecretRef{{Field: "/value", Lifetime: extension.SecretActivation}}
	native := extension.Extension{Manifest: manifest, Activate: func(context.Context, extension.API) (func(context.Context) error, error) {
		return nil, nil
	}}
	r, err := e.openPlugins(context.Background(), native)
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	before, err := os.ReadFile(e.configPath)
	testassert.NoError(t, err)
	generation := r.config.current().generation
	captured, err := r.config.capture(context.Background())
	testassert.NoError(t, err)
	observed := captured.snapshot.credentials[name]
	testassert.Require(t, observed.Defined && !observed.Managed && observed.Value == "", "shared external observation = %+v, want the unchanged defined-empty external record", observed)
	testassert.Equal(t, classifyKeySource(name, captured.snapshot.credentials), config.KeySourceExternal)
	testassert.Require(t, !capturedEnvIsSet(captured.snapshot.credentials)(name), "defined-empty external shared key reported as set")

	key := "supplied-key-must-refuse"
	if _, err := r.ConnectProvider(context.Background(), testCaller(r), "prov", &key); err == nil || !strings.Contains(err.Error(), "set externally") {
		t.Fatalf("connectProvider with a defined-empty external shared key = %v, want the external refusal", err)
	}
	after, err := os.ReadFile(e.configPath)
	testassert.NoError(t, err)
	testassert.Require(t, string(after) == string(before), "refused connect rewrote the config document")
	testassert.Require(t, !testManaged(r, name), "refused connect managed the external shared key")
	testassert.Require(t, r.config.current().generation == generation, "refused connect consumed a configuration generation")
}

// TestPreparationUsesCurrentCredentialObservationAfterStartup proves a
// preparation started after an external shared-name rotation uses the current
// provider/Operation observation, while an already prepared Operation keeps
// its earlier provider, Operation and activation values frozen.
func TestPreparationUsesCurrentCredentialObservationAfterStartup(t *testing.T) {
	const name = "MELTA_CURRENT_OBSERVATION"
	t.Setenv(name, "shared-candidate")
	h := newServiceHarness(t)
	server := newProductionModelServer(t, "", "")
	probe := &credentialGateProbe{}
	writeServiceFile(t, h.configPath, `{"providers":{"prov":`+nominalProvMember(server.URL, name, `{"m":{"name":"M","context_window":4096}}`)+`},"extensions":{"credprobe":{"enabled":true,"activation":"`+name+`","operation":"`+name+`"}}}`)
	svc := h.preparedService(context.Background(), probe.author(t))
	_, err := svc.publish(context.Background())
	testassert.NoError(t, err)
	defer svc.current().resources.release()

	p := newPreparation(svc, h.locations(), "", nil, newObservationAdapter(newObservation(), newWarningStore()), newArtifactIntervals(), nil)
	request := func(session string) harness.PreparationRequest {
		return harness.PreparationRequest{
			Session: harness.PreparationSession{
				Identity: harness.SessionIdentity{SessionID: session, Workspace: h.dataDir},
				Inputs:   harness.SessionInputs{Model: &model.ModelRef{Provider: "prov", Model: "m"}, Prompt: new(string), Tools: []string{"cred_probe"}},
			},
			RequestKind: harness.RequestKindMessage,
		}
	}
	oldPrepared, err := p.bind()(context.Background(), request("session-old"))
	testassert.NoError(t, err)
	defer oldPrepared.Release()

	t.Setenv(name, "shared-later")
	_, err = svc.publish(context.Background())
	testassert.NoError(t, err)
	newPrepared, err := p.bind()(context.Background(), request("session-new"))
	testassert.NoError(t, err)
	defer newPrepared.Release()

	run := func(prepared harness.PreparedExecution, session, operation, invocation, key string) {
		t.Helper()
		execution, err := prepared.Open(context.Background(), harness.OperationAdmission{SessionID: session, OperationID: operation, RequestKind: harness.RequestKindMessage, AdmittedAt: time.Now()})
		testassert.NoError(t, err)
		defer func() { testassert.NoError(t, execution.Close()) }()
		inv, err := execution.OpenInvocation(context.Background(), harness.InvocationRequest{ID: invocation, Purpose: harness.InvocationPurposeConversation, Model: prepared.Capture.Model})
		testassert.NoError(t, err)
		defer func() { testassert.NoError(t, inv.Close()) }()
		before := server.requests()
		stream, err := inv.Model(context.Background(), model.Request{}, nil)
		testassert.NoError(t, err)
		if stream != nil {
			_ = stream.Close()
		}
		testassert.Require(t, server.requests() == before+1 && server.authAt(before) == "Bearer "+key, "provider authorization = %q, want %q", server.authAt(before), "Bearer "+key)
		outcome := fixtureBinding(inv, "cred_probe").Run(context.Background(), model.ToolCall{ID: "call-" + key, Name: "cred_probe", Arguments: json.RawMessage(`{"key":"` + key + `"}`)})
		testassert.Require(t, outcome.Result.Status == model.ResultSuccess, "tool outcome = %+v, want success", outcome.Result)
	}
	run(newPrepared, "session-new", "op-new", "inv-new", "shared-later")
	run(oldPrepared, "session-old", "op-old", "inv-old", "shared-candidate")
	probe.mu.Lock()
	defer probe.mu.Unlock()
	testassert.Equal(t, probe.values["shared-later"], "shared-later|shared-later")
	testassert.Equal(t, probe.values["shared-candidate"], "shared-candidate|shared-candidate")
}

func credentialServiceConfig(extensions string) string {
	return `{"providers":{"prov":` + nominalProvMember("http://prov.test/v1", "", `{"m":{"name":"M","context_window":4096}}`) + `},"extensions":` + extensions + `}`
}

// credentialProbe is the runtime acceptance extension: one Harness-scoped
// tool and preparation hook over the declared credentials host binding.
type credentialProbe struct {
	mu sync.Mutex

	openActivation   string
	openOperationErr string
	openUndeclared   string
	hookShell        string
	hookCalls        int

	toolValues []string
}

func (p *credentialProbe) author(t *testing.T) extension.Extension {
	t.Helper()
	return extension.Extension{
		Manifest: extension.Manifest{
			ID:             "credprobe",
			Version:        "1",
			APIVersion:     extension.APIVersion,
			SettingsSchema: json.RawMessage(`{"type":"object","properties":{"enabled":{"type":"boolean"},"activation":{"type":"string"},"operation":{"type":"string"}},"required":["enabled"]}`),
			Services:       []string{extension.ContractCredentials},
			Secrets: []extension.SecretRef{
				{Field: "/activation", Lifetime: extension.SecretActivation},
				{Field: "/operation", Lifetime: extension.SecretOperation},
			},
		},
		Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
			p.mu.Lock()
			defer p.mu.Unlock()
			result, err := api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/activation"}`))
			p.openActivation = credentialValue(t, result, err)
			_, err = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/operation"}`))
			if err != nil {
				p.openOperationErr = err.Error()
			}
			_, err = api.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/missing"}`))
			if err != nil {
				p.openUndeclared = err.Error()
			}
			_, err = api.RegisterTool(model.ToolDefinition{Name: "cred_probe", Description: "reads declared credentials", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}, &credentialTool{probe: p}, nil)
			if err != nil {
				return nil, err
			}
			_, err = api.AttachAdmission(p.prepare, nil)
			return nil, err
		},
	}
}

func openCredentialProbe(t *testing.T, e *productionEnv, author extension.Extension) (*Melta, error) {
	t.Helper()
	useFixtureStore(t, e.store)
	return Open(context.Background(), Options{
		DataDir: e.dataDir, Configuration: NewFileConfigurationSource(e.configPath),
		Credentials: NewEnvironmentCredentialSource(filepath.Join(e.home, ".melta", ".env")),
		Locations:   e.locations(), Identity: e.identity, Extensions: []extension.Extension{author},
	})
}

func (p *credentialProbe) prepare(_ context.Context, _ extension.AuthorContext, input extension.AdmissionInput) (extension.AdmissionResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, line := range strings.Split(input.Prompt.Environment(), "\n") {
		if value, ok := strings.CutPrefix(line, "Shell: "); ok {
			p.hookShell = value
		}
	}
	p.hookCalls++
	return extension.AdmissionResult{}, nil
}

type credentialTool struct{ probe *credentialProbe }

func (t *credentialTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *credentialTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	return extension.ToolPlan{Data: []byte(call.ID)}
}

func (t *credentialTool) Execute(ctx context.Context, tc extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	activation, err := tc.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/activation"}`))
	if err != nil {
		return credentialOutcome(string(plan.Data), "activation error: "+err.Error())
	}
	operation, err := tc.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/operation"}`))
	if err != nil {
		return credentialOutcome(string(plan.Data), "operation error: "+err.Error())
	}
	t.probe.mu.Lock()
	combined := "activation=" + credentialString(activation) + "|operation=" + credentialString(operation)
	t.probe.toolValues = append(t.probe.toolValues, combined)
	t.probe.mu.Unlock()
	return credentialOutcome(string(plan.Data), combined)
}

func credentialString(result extension.Result) string {
	var value struct {
		Value string `json:"value"`
	}
	_ = json.Unmarshal(result.Value, &value)
	return value.Value
}

func credentialOutcome(callID, content string) harness.ToolOutcome {
	return harness.ToolOutcome{Evidence: harness.ExecutionObserved, Result: model.ToolResult{CallID: callID, Status: model.ResultSuccess, Content: content}}
}

func credentialConfigDocument(endpoint, activation, operation string) string {
	return `{"providers":{"prov":{"transport":{"base_url":"` + endpoint + `","api_key_env":""},"discovery":false,"models":{"m":{"name":"M","context_window":262144}}}},"extensions":{"credprobe":{"enabled":true,"activation":` + fmt.Sprintf("%q", activation) + `,"operation":` + fmt.Sprintf("%q", operation) + `}}}`
}

func TestProjectedShellUsesTheOwnerEnvironmentProjection(t *testing.T) {
	t.Setenv("SHELL", "/bin/projected")
	t.Setenv("EXTENSION_SECRET", "secret")
	t.Setenv("KEEP", "value")
	h := newServiceHarness(t)
	svc := h.service(context.Background())

	svc.published.Store(&configuration{catalog: &catalog.Catalog{}})
	if got := projectedShell(context.Background(), svc.subprocessEnvironment); got != "/bin/projected" {
		t.Fatalf("projectedShell(owner projection) = %q, want the projected shell", got)
	}
	svc.published.Store(&configuration{catalog: &catalog.Catalog{Providers: map[string]*catalog.Provider{
		"p": {Transport: catalog.Transport{APIKeyEnv: "SHELL"}},
	}}})
	if got := projectedShell(context.Background(), svc.subprocessEnvironment); got != "unknown" {
		t.Fatalf("projectedShell(provider name) = %q, want unknown", got)
	}
	svc.published.Store(&configuration{catalog: &catalog.Catalog{}, secrets: []declaredSecret{{name: "EXTENSION_SECRET"}}})
	projected := subprocessEnvForTest(t, svc)
	testassert.Require(t, !slices.Contains(projected, "EXTENSION_SECRET=secret") && slices.Contains(projected, "SHELL=/bin/projected") && slices.Contains(projected, "KEEP=value"), "owner projection = %v, want the declared name removed and siblings kept", projected)
	svc.published.Store(&configuration{catalog: &catalog.Catalog{}})
	svc.holdCredentialNames([]string{"SHELL"})
	if got := projectedShell(context.Background(), svc.subprocessEnvironment); got != "unknown" {
		t.Fatalf("projectedShell(held name) = %q, want unknown", got)
	}
	svc.releaseCredentialNames([]string{"SHELL"})
	if got := projectedShell(context.Background(), svc.subprocessEnvironment); got != "/bin/projected" {
		t.Fatalf("projectedShell(released held name) = %q, want the projected shell", got)
	}
	t.Setenv("SHELL", "")
	if got := projectedShell(context.Background(), svc.subprocessEnvironment); got != "unknown" {
		t.Fatalf("projectedShell(empty shell) = %q, want unknown", got)
	}
}

func TestResolveDeclaredSecretsOnlyReadsActiveSections(t *testing.T) {
	author := extension.Extension{Manifest: extension.Manifest{
		ID:             "ext",
		Version:        "1",
		APIVersion:     extension.APIVersion,
		SettingsSchema: json.RawMessage(`{"type":"object","properties":{"enabled":{"type":"boolean"}},"required":["enabled"]}`),
		Secrets: []extension.SecretRef{
			{Field: "/activation", Lifetime: extension.SecretActivation},
			{Field: "/operation", Lifetime: extension.SecretOperation},
			{Field: "/absent", Lifetime: extension.SecretOperation},
		},
	}, Activate: func(context.Context, extension.API) (func(context.Context) error, error) { return nil, nil }}
	specs := []nativeActivationSpec{{extension: author}}
	resolved := resolveAuthorSecrets(specs, map[string]json.RawMessage{
		"ext": json.RawMessage(`{"enabled":true,"activation":"ACTIVATION_NAME","operation":"OPERATION_NAME"}`),
	})
	testassert.Require(t, len(resolved) == 2, "resolved secrets = %+v, want the two present fields", resolved)
	testassert.Equal(t, resolved[0].name, "ACTIVATION_NAME")
	testassert.Equal(t, resolved[1].name, "OPERATION_NAME")
	testassert.Require(t, len((&configuration{secrets: resolved}).declaredSecretNames()) == 2, "declared names did not dedupe the resolved references")
	if got := resolveAuthorSecrets(specs, map[string]json.RawMessage{
		"ext": json.RawMessage(`{"enabled":false,"activation":"ACTIVATION_NAME","operation":"OPERATION_NAME"}`),
	}); len(got) != 0 {
		t.Fatalf("inactive section resolved %+v, want nothing", got)
	}
	if got := resolveAuthorSecrets(specs, nil); len(got) != 0 {
		t.Fatalf("absent section resolved %+v, want nothing", got)
	}
}

func TestDeclaredCredentialsPromptAndChildEnvironment(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		activationName string
		shell          string
		wantPrompt     string
		wantChild      string
	}{
		{
			name:           "declared shell collision",
			activationName: "SHELL",
			shell:          "/bin/collision-shell",
			wantPrompt:     "unknown",
			wantChild:      "shell=|secret=|other=extension-only-secret",
		},
		{
			name:           "ordinary shell sibling",
			activationName: "MELTA_EXT_ONLY",
			shell:          "/bin/ordinary-shell",
			wantPrompt:     "/bin/ordinary-shell",
			wantChild:      "shell=/bin/ordinary-shell|secret=|other=extension-only-secret",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			e := newProductionEnv(t, storage.NewMemory())
			const (
				operationName = "MELTA_EXT_OPERATION"
				secretValue   = "extension-only-secret"
			)
			writeServiceFile(t, e.configPath, credentialConfigDocument(e.server.URL, testCase.activationName, operationName))
			t.Setenv("SHELL", testCase.shell)
			t.Setenv(testCase.activationName, secretValue)
			t.Setenv(operationName, "operation-secret")
			// The fixture's ordinary sibling carries the same bytes as the
			// secret value: exclusion is by declared name, never by scanning
			// arbitrary payload bytes.
			t.Setenv("MELTA_UNRELATED_PAYLOAD", secretValue)

			probe := &credentialProbe{}
			var steps atomic.Int64
			e.server.setScript(func(context.Context, string) []string {
				switch steps.Add(1) {
				case 1:
					return toolCallTurnEvents("cred-call", "cred_probe", `{}`)
				case 2:
					command := `printf 'shell=%s|secret=%s|other=%s' "$SHELL" "$` + testCase.activationName + `" "$MELTA_UNRELATED_PAYLOAD"`
					args, err := json.Marshal(map[string]any{"command": command})
					testassert.NoError(t, err)
					return toolCallTurnEvents("cmd-call", "run_command", string(args))
				default:
					return textTurnEvents("done")
				}
			})

			r, err := openCredentialProbe(t, e, probe.author(t))
			testassert.NoError(t, err)
			t.Cleanup(func() { _ = r.Close(context.Background()) })
			workspace := e.workspace("credentials-workspace")
			testassert.NoError(t, os.MkdirAll(workspace, 0o700))
			var session harness.SessionRecord
			err = r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
				var createErr error
				session, createErr = r.createSessionRecord(ctx, h, workspace, harness.SessionInputs{
					Model: &model.ModelRef{Provider: "prov", Model: "m"}, Tools: []string{"cred_probe", "run_command"},
				}, nil)
				return createErr
			})
			testassert.NoError(t, err)
			submitThroughRuntime(t, r, session.Identity.SessionID, "op-cred", "hello")
			awaitOperation(t, r, session.Identity.SessionID, "op-cred", harness.OperationSuccess)

			probe.mu.Lock()
			defer probe.mu.Unlock()
			testassert.Equal(t, probe.openActivation, secretValue)
			testassert.Require(t, strings.Contains(probe.openOperationErr, "is unavailable"), "operation read during Open = %v, want the unavailable refusal", probe.openOperationErr)
			testassert.Require(t, strings.Contains(probe.openUndeclared, "is not declared"), "undeclared field read during Open = %v, want the declaration refusal", probe.openUndeclared)
			testassert.Require(t, probe.hookCalls == 1 && probe.hookShell == testCase.wantPrompt, "preparation hook = (%d, %q), want one call seeing %q", probe.hookCalls, probe.hookShell, testCase.wantPrompt)
			testassert.Require(t, len(probe.toolValues) == 1, "tool credential reads = %v, want one", probe.toolValues)
			testassert.Equal(t, probe.toolValues[0], "activation="+secretValue+"|operation=operation-secret")

			testassert.Require(t, e.server.requests() == 3, "model requests = %d, want tool, command and final turns", e.server.requests())
			for i := 0; i < e.server.requests(); i++ {
				body := e.server.bodyAt(i)
				testassert.Require(t, strings.Contains(body, "Shell: "+testCase.wantPrompt), "request %d prompt = %q, want the projected shell %q", i, body, testCase.wantPrompt)
			}
			testassert.Require(t, strings.Contains(e.server.bodyAt(1), "activation="+secretValue+"|operation=operation-secret"), "credential tool result = %q, want the activation and operation values", e.server.bodyAt(1))
			testassert.Require(t, strings.Contains(e.server.bodyAt(2), testCase.wantChild), "command result = %q, want %q", e.server.bodyAt(2), testCase.wantChild)
		})
	}
}

// TestDeclaredCredentialCapturesStayPerOperation proves two concurrent
// Operations keep the credential values captured at their own admission, and
// a reload that renames the declared field does not rewrite the old capture
// or lift its child-environment exclusion.
func TestDeclaredCredentialCapturesStayPerOperation(t *testing.T) {
	e := newProductionEnv(t, storage.NewMemory())
	const first = "MELTA_EXT_FIRST"
	const second = "MELTA_EXT_SECOND"
	writeServiceFile(t, e.configPath, credentialConfigDocument(e.server.URL, first, first))
	t.Setenv("SHELL", "/bin/capture-shell")
	t.Setenv(first, "first-secret")
	t.Setenv(second, "second-secret")

	release := make(chan struct{})
	probe := &credentialGateProbe{release: release}
	var steps atomic.Int64
	e.server.setScript(func(context.Context, string) []string {
		switch steps.Add(1) {
		case 1:
			return toolCallTurnEvents("cred-call", "cred_probe", `{"key":"first","park":true}`)
		case 2:
			return toolCallTurnEvents("cred-call", "cred_probe", `{"key":"changed","park":false}`)
		case 4:
			return toolCallTurnEvents("cred-call", "cred_probe", `{"key":"third","park":false}`)
		case 6:
			return toolCallTurnEvents("cmd-call", "run_command", `{"command":"printf 'first=%s|second=%s' \"$MELTA_EXT_FIRST\" \"$MELTA_EXT_SECOND\""}`)
		default:
			return textTurnEvents("done")
		}
	})
	r, err := openCredentialProbe(t, e, probe.author(t))
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	workspace := e.workspace("capture-workspace")
	testassert.NoError(t, os.MkdirAll(workspace, 0o700))

	create := func() harness.SessionRecord {
		t.Helper()
		var session harness.SessionRecord
		err := r.withHarness(context.Background(), func(ctx context.Context, h *harness.Harness) error {
			var createErr error
			session, createErr = r.createSessionRecord(ctx, h, workspace, harness.SessionInputs{
				Model: &model.ModelRef{Provider: "prov", Model: "m"}, Prompt: new(string), Tools: []string{"cred_probe", "run_command"},
			}, nil)
			return createErr
		})
		testassert.NoError(t, err)
		return session
	}
	firstSession := create()
	submitThroughRuntime(t, r, firstSession.Identity.SessionID, "op-first", "hello")
	firstResources := r.config.current().resources
	deadline := time.Now().Add(10 * time.Second)
	for {
		probe.mu.Lock()
		parked := probe.parked
		probe.mu.Unlock()
		if parked {
			break
		}
		testassert.Require(t, time.Now().Before(deadline), "the first credential read never parked")
		time.Sleep(5 * time.Millisecond)
	}

	// A changed value under the same name is observed at the next build.
	t.Setenv(first, "changed-secret")
	_, err = r.Reload(context.Background(), testCaller(r))
	testassert.NoError(t, err)
	secondSession := create()
	submitThroughRuntime(t, r, secondSession.Identity.SessionID, "op-second", "hello")
	awaitOperation(t, r, secondSession.Identity.SessionID, "op-second", harness.OperationSuccess)

	// Rename the declared field and retain the old variable: a reload's new
	// resource set captures the new name while the parked Operation keeps the
	// old name and value.
	writeServiceFile(t, e.configPath, credentialConfigDocument(e.server.URL, second, second))
	revision, err := r.Reload(context.Background(), testCaller(r))
	testassert.Require(t, err == nil && revision.Generation == "3", "Reload = (%q, %v), want 3", revision, err)
	testassert.Require(t, r.config.current().resources != firstResources, "the rotated declared binding reused the pre-reload resource set")
	thirdSession := create()
	submitThroughRuntime(t, r, thirdSession.Identity.SessionID, "op-third", "hello")
	awaitOperation(t, r, thirdSession.Identity.SessionID, "op-third", harness.OperationSuccess)
	close(release)
	awaitOperation(t, r, firstSession.Identity.SessionID, "op-first", harness.OperationSuccess)

	probe.mu.Lock()
	defer probe.mu.Unlock()
	// The first Operation's activation and Operation captures predate both the
	// value change and the rename. The second Operation's revision captures the
	// changed value; the third captures the reloaded name.
	testassert.Equal(t, probe.values["first"], "first-secret|first-secret")
	testassert.Equal(t, probe.values["changed"], "changed-secret|changed-secret")
	testassert.Equal(t, probe.values["third"], "second-secret|second-secret")
	// The first Operation's prepared exclusion still names the old variable
	// after the reload renamed it, and the live revision adds the new one.
	testassert.Require(t, e.server.requests() == 7, "model requests = %d, want three probes and four final turns", e.server.requests())
	testassert.Require(t, strings.Contains(e.server.bodyAt(6), "first=|second="), "renamed credential command = %q, want both declared names excluded", e.server.bodyAt(6))
}

// credentialGateProbe is one Harness-scoped tool that captures the declared
// credentials and can park before reading them.
type credentialGateProbe struct {
	mu      sync.Mutex
	release chan struct{}

	parked bool
	values map[string]string
}

func (p *credentialGateProbe) author(t *testing.T) extension.Extension {
	return p.authorFor(t, "credprobe", "cred_probe")
}

func (p *credentialGateProbe) authorFor(t *testing.T, id, toolID string) extension.Extension {
	t.Helper()
	return extension.Extension{
		Manifest: extension.Manifest{
			ID: id, Version: "1", APIVersion: extension.APIVersion,
			SettingsSchema: json.RawMessage(`{"type":"object","properties":{"enabled":{"type":"boolean"},"activation":{"type":"string"},"operation":{"type":"string"}},"required":["enabled"]}`),
			Services:       []string{extension.ContractCredentials},
			Secrets: []extension.SecretRef{
				{Field: "/activation", Lifetime: extension.SecretActivation},
				{Field: "/operation", Lifetime: extension.SecretOperation},
			},
		},
		Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
			_, err := api.RegisterTool(model.ToolDefinition{Name: toolID, Description: "reads the captured credentials", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}, &credentialGateTool{probe: p}, nil)
			return nil, err
		},
	}
}

type credentialGateTool struct{ probe *credentialGateProbe }

func (t *credentialGateTool) Normalize(_ extension.AuthorContext, call model.ToolCall) (json.RawMessage, error) {
	return call.Arguments, nil
}

func (t *credentialGateTool) Prepare(_ context.Context, _ extension.AuthorContext, call model.ToolCall) extension.ToolPlan {
	data, _ := json.Marshal(struct {
		CallID    string          `json:"call_id"`
		Arguments json.RawMessage `json:"arguments"`
	}{CallID: call.ID, Arguments: call.Arguments})
	return extension.ToolPlan{Data: data}
}

func (t *credentialGateTool) Execute(ctx context.Context, tc extension.AuthorContext, plan extension.ToolPlan) harness.ToolOutcome {
	var prepared struct {
		CallID    string          `json:"call_id"`
		Arguments json.RawMessage `json:"arguments"`
	}
	_ = json.Unmarshal(plan.Data, &prepared)
	var request struct {
		Key  string `json:"key"`
		Park bool   `json:"park"`
	}
	_ = json.Unmarshal(prepared.Arguments, &request)
	if request.Park {
		t.probe.mu.Lock()
		t.probe.parked = true
		t.probe.mu.Unlock()
		<-t.probe.release
	}
	activation, err := tc.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/activation"}`))
	if err != nil {
		return credentialOutcome(prepared.CallID, "activation error: "+err.Error())
	}
	operation, err := tc.Call(ctx, extension.ContractCredentials, "get", json.RawMessage(`{"field":"/operation"}`))
	if err != nil {
		return credentialOutcome(prepared.CallID, "operation error: "+err.Error())
	}
	t.probe.mu.Lock()
	if t.probe.values == nil {
		t.probe.values = make(map[string]string)
	}
	t.probe.values[request.Key] = credentialString(activation) + "|" + credentialString(operation)
	t.probe.mu.Unlock()
	return credentialOutcome(prepared.CallID, "probed")
}

// captureValueForTest returns one managed file value.
func captureValueForTest(t *testing.T, managed *config.ManagedEnv, name string) string {
	t.Helper()
	captured, err := managed.Capture([]string{name})
	testassert.NoError(t, err)
	return captured[name].Value
}
