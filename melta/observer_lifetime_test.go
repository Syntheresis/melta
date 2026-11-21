package melta

// Observer lifetime contract: one end path under nativeMu, one acceptance rule,
// callback-scope delivery and close-side in-flight waiting.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/internal/storage"
	"github.com/Syntheresis/melta/internal/testassert"
)

type observerLifetimeFixture struct {
	owner      *configurationService
	candidate  *nativeCandidate
	activation *nativeActivation
	api        extension.API
	handle     extension.Registration
	entry      *nativeRegistration
	delivery   *observerDelivery
}

func newObserverLifetimeFixture(t *testing.T, observer extension.Observer, closeFn func(context.Context) error) *observerLifetimeFixture {
	t.Helper()
	owner := &configurationService{obs: newObservation()}
	manifest := registrarManifest("observer-lifetime")
	manifest.Services = []string{extension.ContractCommands}
	fixture := &observerLifetimeFixture{owner: owner}
	e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, api extension.API) (func(context.Context) error, error) {
		fixture.api = api
		var err error
		fixture.handle, err = api.AttachObserver(observer, nil)
		return closeFn, err
	}}
	candidate, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{extension: e, section: []byte(`{}`)}})
	testassert.NoError(t, err)
	fixture.candidate = candidate
	testassert.Require(t, len(candidate.activations) == 1 && candidate.activations[0] != nil, "activation was not prepared")
	fixture.activation = candidate.activations[0]
	fixture.entry = fixture.handle.(*nativeRegistration)
	fixture.delivery = fixture.entry.observer
	testassert.Require(t, fixture.delivery != nil, "observer delivery was not attached")
	return fixture
}

// TestObserverRemoveCancelsInFlightAndDropsQueued proves Remove cancels the
// in-flight observer call, drops queued facts and leaves the handle stale.
func TestObserverRemoveCancelsInFlightAndDropsQueued(t *testing.T) {
	entered := make(chan struct{})
	returned := make(chan struct{})
	var calls atomic.Int32
	fixture := newObserverLifetimeFixture(t, func(ctx context.Context, fact extension.Fact) error {
		calls.Add(1)
		if fact.OperationID == "in-flight" {
			close(entered)
			<-ctx.Done()
			close(returned)
			return ctx.Err()
		}
		return nil
	}, nil)
	fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationAdmitted, SessionID: "s", OperationID: "in-flight"})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the observer call never started")
	}
	fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationSettled, SessionID: "s", OperationID: "queued"})
	fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationSettled, SessionID: "s", OperationID: "queued-2"})

	testassert.NoError(t, fixture.api.Remove(fixture.handle))
	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("Remove did not cancel the in-flight observer call")
	}
	select {
	case <-fixture.delivery.runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the delivery goroutine did not end after Remove")
	}
	testassert.Equal(t, calls.Load(), int32(1))
	// The handle now behaves as a removed (stale) handle.
	testassert.NoError(t, fixture.api.Remove(fixture.handle))
	testassert.Require(t, !fixture.entry.current, "the removed registration stayed current")
	testassert.Equal(t, len(fixture.activation.entries), 0)
	fixture.candidate.release()
}

// TestObserverReplacementEndsOldAndNewReceives proves replacement ends the old
// observer exactly like Remove and the new registration receives later facts.
func TestObserverReplacementEndsOldAndNewReceives(t *testing.T) {
	oldEntered := make(chan struct{})
	oldReturned := make(chan struct{})
	oldCalls := atomic.Int32{}
	fixture := newObserverLifetimeFixture(t, func(ctx context.Context, fact extension.Fact) error {
		oldCalls.Add(1)
		close(oldEntered)
		<-ctx.Done()
		close(oldReturned)
		return ctx.Err()
	}, nil)
	oldEntry := fixture.entry
	oldDelivery := fixture.delivery
	fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationAdmitted, SessionID: "s", OperationID: "old"})
	select {
	case <-oldEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the old observer call never started")
	}
	later := make(chan extension.Fact, 1)
	replacement, err := fixture.api.AttachObserver(func(_ context.Context, fact extension.Fact) error {
		select {
		case later <- fact:
		default:
		}
		return nil
	}, fixture.handle)
	testassert.NoError(t, err)
	select {
	case <-oldReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("replacement did not cancel the old observer call")
	}
	<-oldDelivery.runDone
	testassert.Require(t, !oldEntry.current, "the replaced registration stayed current")
	newEntry := replacement.(*nativeRegistration)
	testassert.Require(t, newEntry.current && newEntry.observer != nil, "the replacement registration is not live")
	newEntry.observer.enqueue(extension.Fact{Kind: extension.FactOperationSettled, SessionID: "s", OperationID: "later"})
	select {
	case fact := <-later:
		testassert.Require(t, fact.OperationID == "later", "new observer fact = %+v", fact)
	case <-time.After(10 * time.Second):
		t.Fatal("the replacement observer received no later fact")
	}
	testassert.Equal(t, oldCalls.Load(), int32(1))
	fixture.candidate.release()
}

// TestObserverOverflowErrorAndPanicEndRegistration proves every self-end path
// removes the registration and later facts are not offered to it.
func TestObserverOverflowErrorAndPanicEndRegistration(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		block := make(chan struct{})
		var calls atomic.Int32
		fixture := newObserverLifetimeFixture(t, func(context.Context, extension.Fact) error {
			calls.Add(1)
			<-block
			return nil
		}, nil)
		for i := range observerQueueCapacity + 2 {
			fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationAdmitted, SessionID: "s", OperationID: "op"})
			_ = i
		}
		select {
		case <-fixture.delivery.done:
		case <-time.After(10 * time.Second):
			t.Fatal("the overflowing registration stayed attached")
		}
		testassert.Require(t, !fixture.entry.current, "the overflowing registration stayed current")
		testassert.Equal(t, len(fixture.activation.entries), 0)
		before := calls.Load()
		fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationSettled, SessionID: "s", OperationID: "late"})
		testassert.NoError(t, fixture.api.Remove(fixture.handle))
		close(block)
		time.Sleep(10 * time.Millisecond)
		testassert.Equal(t, calls.Load(), before)
		fixture.candidate.release()
	})
	for _, test := range []struct {
		name     string
		observer func(context.Context, extension.Fact) error
	}{
		{name: "error", observer: func(context.Context, extension.Fact) error { return errors.New("observer failure") }},
		{name: "panic", observer: func(context.Context, extension.Fact) error { panic("observer panic") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newObserverLifetimeFixture(t, test.observer, nil)
			fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationAdmitted, SessionID: "s", OperationID: "op"})
			select {
			case <-fixture.delivery.runDone:
			case <-time.After(10 * time.Second):
				t.Fatal("the failing observer stayed attached")
			}
			testassert.Require(t, !fixture.entry.current, "the failing registration stayed current")
			testassert.Equal(t, len(fixture.activation.entries), 0)
			testassert.NoError(t, fixture.api.Remove(fixture.handle))
			fixture.candidate.release()
		})
	}
}

// TestObserverRegistrationRefusedWhenUnavailable proves every registration
// kind and Remove use the activation's unavailability error on lost and closed
// activations.
func TestObserverRegistrationRefusedWhenUnavailable(t *testing.T) {
	t.Run("lost", func(t *testing.T) {
		fixture := newObserverLifetimeFixture(t, func(context.Context, extension.Fact) error { return nil }, nil)
		loss := errors.New("activation lost")
		fixture.activation.markLost(loss)
		_, err := fixture.api.AttachObserver(func(context.Context, extension.Fact) error { return nil }, nil)
		testassert.Require(t, errors.Is(err, loss), "observer registration on a lost activation = %v, want the loss", err)
		_, err = fixture.api.RegisterTool(testDescribeTool("lost-tool"), testRegisteredTool{}, nil)
		testassert.Require(t, errors.Is(err, loss), "tool registration on a lost activation = %v, want the loss", err)
		err = fixture.api.Remove(fixture.handle)
		testassert.Require(t, errors.Is(err, loss), "Remove on a lost activation = %v, want the loss", err)
		fixture.candidate.release()
	})
	t.Run("closed", func(t *testing.T) {
		fixture := newObserverLifetimeFixture(t, func(context.Context, extension.Fact) error { return nil }, nil)
		fixture.candidate.release()
		_, err := fixture.api.AttachObserver(func(context.Context, extension.Fact) error { return nil }, nil)
		testassert.Require(t, errors.Is(err, extension.ErrClosed), "observer registration on a closed activation = %v, want closed", err)
		_, err = fixture.api.RegisterTool(testDescribeTool("closed-tool"), testRegisteredTool{}, nil)
		testassert.Require(t, errors.Is(err, extension.ErrClosed), "tool registration on a closed activation = %v, want closed", err)
		err = fixture.api.Remove(fixture.handle)
		testassert.Require(t, errors.Is(err, extension.ErrClosed), "Remove on a closed activation = %v, want closed", err)
	})
}

// TestObserverActivationCloseWaitsForInFlightCall proves native activation
// close cancels the observer call and runs the extension close afterwards.
func TestObserverActivationCloseWaitsForInFlightCall(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	closeRan := make(chan struct{})
	fixture := newObserverLifetimeFixture(t, func(ctx context.Context, _ extension.Fact) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		record("observer-return")
		return ctx.Err()
	}, func(context.Context) error {
		record("close")
		close(closeRan)
		return nil
	})
	fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationAdmitted, SessionID: "s", OperationID: "op"})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the observer call never started")
	}
	released := make(chan struct{})
	go func() {
		fixture.candidate.release()
		close(released)
	}()
	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("activation close did not cancel the in-flight observer call")
	}
	select {
	case <-closeRan:
		t.Fatal("the extension close ran while the observer call was still blocked")
	case <-time.After(100 * time.Millisecond):
	}
	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	testassert.Require(t, len(got) == 0, "extension close events before observer return = %v, want none", got)
	close(release)
	select {
	case <-released:
	case <-time.After(10 * time.Second):
		t.Fatal("activation close did not finish after the observer returned")
	}
	mu.Lock()
	got = append([]string(nil), events...)
	mu.Unlock()
	testassert.Require(t, len(got) == 2 && got[0] == "observer-return" && got[1] == "close", "close order = %v, want the observer call before the extension close", got)
}

// TestObserverCancelledDeliveryReleasesCallbackCount proves the activation's
// callback count returns to zero after a cancelled delivery.
func TestObserverCancelledDeliveryReleasesCallbackCount(t *testing.T) {
	entered := make(chan struct{})
	fixture := newObserverLifetimeFixture(t, func(ctx context.Context, _ extension.Fact) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	fixture.owner.composition = newComposition()
	fixture.owner.composition.hosts.owner = &Melta{}
	testassert.NoError(t, fixture.candidate.publishForTest())
	fixture.delivery.enqueue(extension.Fact{Kind: extension.FactOperationAdmitted, SessionID: "s", OperationID: "op"})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the observer call never started")
	}
	_, err := fixture.api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetHarness}, "interrupt", []byte(`{}`))
	testassert.Require(t, errors.Is(err, harness.ErrConflict), "command while the observer call is in flight = %v, want conflict", err)
	testassert.NoError(t, fixture.api.Remove(fixture.handle))
	<-fixture.delivery.runDone
	fixture.activation.owner.nativeMu.Lock()
	callbacks := fixture.activation.callbacks
	fixture.activation.owner.nativeMu.Unlock()
	testassert.Equal(t, callbacks, 0)
	fixture.candidate.release()
}

// TestCommandUnavailablePrecedesSessionReadError proves the check order: a
// retired activation answers closed, and an unapproved command service answers
// denied, before either pre-guard Session read error is surfaced.
func TestCommandUnavailablePrecedesSessionReadError(t *testing.T) {
	workspace := "/tmp/command-order"
	t.Run("retired activation", func(t *testing.T) {
		owner := &configurationService{composition: newComposition()}
		owner.composition.hosts.owner = &Melta{}
		manifest := registrarManifest("command-order")
		manifest.Services = []string{extension.ContractCommands}
		var api extension.API
		e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, got extension.API) (func(context.Context) error, error) {
			if api == nil {
				api = got
			}
			return nil, nil
		}}
		first, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
			extension: e, section: []byte(`{"v":1}`),
			scope: CallerScope{Kind: CallerScopeWorkspace, Workspace: workspace},
		}})
		testassert.NoError(t, err)
		defer first.release()
		testassert.NoError(t, first.publishForTest())
		second, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
			extension: e, section: []byte(`{"v":2}`),
			scope: CallerScope{Kind: CallerScopeWorkspace, Workspace: workspace},
		}})
		testassert.NoError(t, err)
		defer second.release()
		testassert.NoError(t, second.publishForTest())

		_, err = api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, "interrupt", []byte(`{}`))
		testassert.Require(t, errors.Is(err, extension.ErrClosed), "retired command = %v, want closed before the Session read error", err)
	})

	t.Run("unapproved service", func(t *testing.T) {
		owner := &configurationService{composition: newComposition()}
		owner.composition.hosts.owner = &Melta{}
		hctx, cancel := context.WithCancel(context.Background())
		h, err := harness.New(hctx, harness.Dependencies{Storage: storage.NewMemory(), Prepare: func(context.Context, harness.PreparationRequest) (harness.PreparedExecution, error) {
			return harness.PreparedExecution{}, errors.New("no preparation")
		}})
		testassert.NoError(t, err)
		owner.composition.hosts.harness = h
		defer func() {
			cancel()
			testassert.NoError(t, h.Wait(context.Background()))
		}()
		manifest := registrarManifest("command-order-denied")
		manifest.Services = []string{extension.ContractCommands}
		var api extension.API
		e := extension.Extension{Manifest: manifest, Activate: func(_ context.Context, got extension.API) (func(context.Context) error, error) {
			api = got
			return nil, nil
		}}
		denied, err := owner.prepareNativeCandidate(context.Background(), []nativeActivationSpec{{
			extension: e, section: []byte(`{}`), approvedServices: []string{},
			scope: CallerScope{Kind: CallerScopeWorkspace, Workspace: workspace},
		}})
		testassert.NoError(t, err)
		defer denied.release()
		testassert.NoError(t, denied.publishForTest())
		_, err = api.Command(context.Background(), extension.CommandTarget{Kind: extension.ExposedTargetSession, SessionID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, "interrupt", []byte(`{}`))
		testassert.Require(t, errors.Is(err, ErrDenied) && !errors.Is(err, harness.ErrNotFound), "unapproved command on an unknown Session = %v, want denied before not-found", err)
	})
}
