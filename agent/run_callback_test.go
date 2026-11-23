package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/Syntheresis/melta/internal/testassert"
	"github.com/Syntheresis/melta/model"
)

// TestRunCallbackZeroCallsWithOutputRejected pins that a settlement carrying an output requires a callback invocation that produced one: an effect that never accepts work through the callback cannot settle an output at all, and a call-bearing output must never reach dispatch.
func TestRunCallbackZeroCallsWithOutputRejected(t *testing.T) {
	f := &runFakes{noCallbackFill: true}
	callsOut := mkCallsTerminalOutput()
	f.modelSet = func(int, model.Request, AssemblyCallback) (ModelSettlement, error) {
		return ModelSettlement{Disposition: DispoReady, Output: callsOut}, nil
	}

	res, err := Run(context.Background(), f.invocation())
	requireBoundaryViolation(t, err, "model")
	requireZeroTerminal(t, res)
	testassert.Require(t, f.modelCalls == 1 && f.toolCalls == 0, "model=%d tool=%d, want the run to stop before dispatch", f.modelCalls, f.toolCalls)
}

// TestRunCallbackOneCallWithNoOutputSettlementRejected pins the other half of the output rule: an outputless settlement requires that the most recent callback invocation produced no output, so accepting model work and then discarding its output is a protocol violation too.
func TestRunCallbackOneCallWithNoOutputSettlementRejected(t *testing.T) {
	f := &runFakes{noCallbackFill: true}
	stream := newFakeStream(deltaStep(choiceDelta(txtPos(0, "x"))))
	f.modelSet = func(_ int, _ model.Request, cb AssemblyCallback) (ModelSettlement, error) {
		if _, err := cb(testRef, stream); err != nil {
			return ModelSettlement{}, err
		}
		return ModelSettlement{Disposition: DispoFailure, Detail: "gave up"}, nil
	}

	res, err := Run(context.Background(), f.invocation())
	requireBoundaryViolation(t, err, "model")
	requireZeroTerminal(t, res)
}

// TestRunCallbackInvocationAfterOutputRejectedWithoutSecondAssembly pins that a callback invocation after one that returned an output is a model boundary-protocol violation: the attempt fails without entering another assembly path (its stream is never read or closed), and an effect that ignores the reported error still cannot settle with the first output.
func TestRunCallbackInvocationAfterOutputRejectedWithoutSecondAssembly(t *testing.T) {
	f := &runFakes{noCallbackFill: true}
	firstStream := newFakeStream(deltaStep(choiceDelta(txtPos(0, "first"))))
	secondStream := newFakeStream(deltaStep(choiceDelta(txtPos(0, "second"))))
	var first model.Output
	f.modelSet = func(_ int, _ model.Request, cb AssemblyCallback) (ModelSettlement, error) {
		out, err := cb(testRef, firstStream)
		if err != nil {
			return ModelSettlement{}, err
		}
		first = out
		{
			_, err2 := cb(testRef, secondStream)
			testassert.Require(t, errors.Is(err2, ErrBoundaryProtocol), "second callback attempt returned %v, want a typed boundary-protocol error", err2)
		}
		return ModelSettlement{Disposition: DispoReady, Output: &first}, nil
	}

	res, err := Run(context.Background(), f.invocation())
	requireBoundaryViolation(t, err, "model")
	requireZeroTerminal(t, res)
	testassert.Require(t, secondStream.recvCount == 0 && secondStream.closeCount == 0, "second callback entered an assembly path: recv=%d close=%d", secondStream.recvCount, secondStream.closeCount)
	testassert.Equal(t, firstStream.closeCount, 1)
}

// TestRunCallbackInvocationAfterStreamErrorSettlesLastOutput pins the retry shape of the callback contract: an invocation that failed with a *StreamError may be followed by another one, and the settlement carries the most recent invocation's output without comparing content.
func TestRunCallbackInvocationAfterStreamErrorSettlesLastOutput(t *testing.T) {
	f := &runFakes{noCallbackFill: true}
	broken := newFakeStream(errStep(errors.New("read failure")))
	good := newFakeStream(deltaStep(choiceDelta(txtPos(0, "second"))))
	f.modelSet = func(_ int, _ model.Request, cb AssemblyCallback) (ModelSettlement, error) {
		_, err := cb(testRef, broken)
		var streamErr *StreamError
		testassert.Require(t, errors.As(err, &streamErr), "first callback returned %v, want a *StreamError", err)
		out, err := cb(testRef, good)
		if err != nil {
			return ModelSettlement{}, err
		}
		return ModelSettlement{Disposition: DispoReady, Output: &out}, nil
	}

	res, err := Run(context.Background(), f.invocation())
	testassert.NoError(t, err)
	testassert.Require(t, res.Status == TerminalSuccess && res.LastOutput != nil && res.LastOutput.Message.TextContent() == "second", "terminal = %#v, want success carrying the second invocation's output", res)
	testassert.Equal(t, broken.closeCount, 1)
	testassert.Equal(t, good.closeCount, 1)
}

// TestRunCallbackForeignSourceRejectedEvenWithValidSettlement pins the callback-source rule: a source unequal to the invocation's expected model identity fails the callback before assembly (the stream is never touched), and an effect that ignores that error still cannot rescue the run with an otherwise-valid expected-source settlement.
func TestRunCallbackForeignSourceRejectedEvenWithValidSettlement(t *testing.T) {
	f := &runFakes{noCallbackFill: true}
	foreign := model.ModelRef{Provider: "acme", Model: "other"}
	stream := newFakeStream(deltaStep(choiceDelta(txtPos(0, "x"))))
	f.modelSet = func(_ int, _ model.Request, cb AssemblyCallback) (ModelSettlement, error) {
		{
			_, err := cb(foreign, stream)
			testassert.Require(t, errors.Is(err, ErrBoundaryProtocol), "callback returned %v for a foreign source, want a typed boundary-protocol error", err)
		}
		return ModelSettlement{Disposition: DispoReady, Output: mkOutput(model.OutputCompleted, "", nil)}, nil
	}

	res, err := Run(context.Background(), f.invocation())
	requireBoundaryViolation(t, err, "model")
	requireZeroTerminal(t, res)
	testassert.Require(t, stream.recvCount == 0 && stream.closeCount == 0, "foreign-source call reached assembly: recv=%d close=%d", stream.recvCount, stream.closeCount)
}

// TestRunCallbackInvalidCallErrorIgnored pins the remembered-error rule: an assembler invalid-call failure (here a nil stream) is recorded even when the callback was otherwise correctly invoked once, so an effect cannot drop the error on the floor and settle an otherwise-valid output.
func TestRunCallbackInvalidCallErrorIgnored(t *testing.T) {
	f := &runFakes{noCallbackFill: true}
	f.modelSet = func(_ int, _ model.Request, cb AssemblyCallback) (ModelSettlement, error) {
		{
			_, err := cb(testRef, nil)
			testassert.Require(t, err != nil, "%v", "callback accepted a nil stream")
		}
		return ModelSettlement{Disposition: DispoReady, Output: mkOutput(model.OutputCompleted, "", nil)}, nil
	}

	res, err := Run(context.Background(), f.invocation())
	requireBoundaryViolation(t, err, "agent") // the remembered invalid-call failure is Assemble's own typed agent-boundary violation and surfaces verbatim.
	requireZeroTerminal(t, res)
	testassert.Require(t, f.modelCalls == 1 && f.toolCalls == 0, "model=%d tool=%d, want the run to stop at the ignored callback error", f.modelCalls, f.toolCalls)
}
