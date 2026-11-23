package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/Syntheresis/melta/model"
)

// Agent-observed run-context cancellation and interrupted tool results share this detail.
const detailInterrupted = "agent interrupted"

// Run performs one caller-driven Agent invocation over its three supplied
// boundaries. It validates and owns each ready request, preserves its tool
// advertisement, enforces model assembly discipline, and dispatches each
// resulting tool batch sequentially. Callback errors return unchanged with no
// terminal result; interruption is explicit or observed at a cancellation
// checkpoint without discarding an already-settled authoritative value. Run
// retains no state and owns no retry, persistence, events, adaptation or history.
func Run(ctx context.Context, inv Invocation) (TerminalResult, error) {
	if ctx == nil { // rejected outright: a nil run context is never normalized to any other context.
		return TerminalResult{}, newBoundaryViolation("agent", "run requires a non-nil run context")
	}
	if !nonzeroSource(inv.ExpectedModel) {
		return TerminalResult{}, newBoundaryViolation("agent", "run requires a nonzero expected model identity")
	}
	if inv.Request == nil || inv.ModelEffect == nil || inv.ToolEffect == nil {
		return TerminalResult{}, newBoundaryViolation("agent", "run requires non-nil request, model and tool effect functions")
	}

	var lastSettled *model.Output // the most recent authoritative model settlement; only a completed one may be carried by a later interruption.

	for {
		if ctx.Err() != nil {
			return interruptionTerminal(detailInterrupted, lastSettled, nil)
		}

		req, err := inv.Request(ctx)
		if err != nil {
			return TerminalResult{}, err
		}
		if (req.Request != nil) == req.Interrupted {
			return TerminalResult{}, newBoundaryViolation("agent", "request source requires exactly one outcome")
		}
		if req.Interrupted {
			return interruptionTerminal(detailInterrupted, lastSettled, nil)
		}
		owned, err := model.NewRequest(*req.Request) // one validation point per round covering the supplied messages and tools; the owned deep copy is independent of everything the source retains.
		if err != nil {
			return TerminalResult{}, fmt.Errorf("%w: %v", newBoundaryViolation("agent", "request is invalid"), err)
		}
		names := make([]string, len(owned.Tools)) // the round's advertised names, copied out of the owned request before the model effect may mutate it.
		for i, def := range owned.Tools {
			names[i] = def.Name
		}

		if ctx.Err() != nil {
			return interruptionTerminal(detailInterrupted, lastSettled, nil)
		}
		cb := &callbackProtocol{ctx: ctx, expected: inv.ExpectedModel} // one observation scope per logical model effect; its callback closes over the run context so no caller can substitute another cancellation channel.
		set, err := inv.ModelEffect(ctx, owned, cb.assemble)
		if err != nil {
			return TerminalResult{}, err // a non-nil caller error is never reclassified by concurrent cancellation.
		}
		if err := cb.settlementFailure(set.Output); err != nil { // the callback discipline behind the settlement gates it before anything else: an ignored protocol or invalid-call failure cannot settle.
			return TerminalResult{}, err
		}
		if _, err := validateSettlement(set, inv.ExpectedModel, false); err != nil { // the read-only settlement path: the same closed table and expected-identity rules without an ownership copy, so Run keeps its original settlement and output pointer.
			return TerminalResult{}, err
		}

		switch set.Disposition {
		case DispoFailure:
			return finishTerminal(TerminalResult{Status: TerminalFailure, LastOutput: set.Output, Detail: set.Detail})
		case DispoInterruption: // the matching terminal preserves the settlement's own detail; a retained completed response is final, so all of its — so far undispatched — calls come back unstarted in message order.
			var unstarted []model.ToolCall
			if set.Output != nil && set.Output.Status == model.OutputCompleted {
				unstarted = set.Output.Message.ToolCalls
			}
			return interruptionTerminal(set.Detail, set.Output, unstarted)
		}

		lastSettled = set.Output
		calls := set.Output.Message.ToolCalls // validation guarantees a ready disposition carries a completed output, and completed guarantees its message.
		if len(calls) == 0 {
			if ctx.Err() != nil { // the completed settlement is not discarded: the interruption carries exactly the output a success would have.
				return interruptionTerminal(detailInterrupted, set.Output, nil)
			}
			return finishTerminal(TerminalResult{Status: TerminalSuccess, LastOutput: set.Output})
		}

		for i, call := range calls { // sequential dispatch in assembled order.
			if ctx.Err() != nil { // no later effect starts: this call and everything after it remain unstarted.
				return interruptionTerminal(detailInterrupted, set.Output, calls[i:])
			}
			call.Arguments = model.CloneRaw(call.Arguments) // per dispatched call the tool boundary receives owned argument bytes, extras, and advertised names, all cloned from the retained settled output and the round's advertisement: one call's mutations cannot alter what a later sibling call or the retained output sees.
			call.Extra = call.Extra.Clone()
			res, err := inv.ToolEffect(ctx, call, append([]string(nil), names...))
			if err != nil {
				return TerminalResult{}, err
			}
			if err := validateToolResult(res, call.ID); err != nil {
				return TerminalResult{}, err
			}
			if res.Status == model.ResultInterrupted { // stops the batch immediately; only calls later than this settled one are unstarted.
				return interruptionTerminal(detailInterrupted, set.Output, calls[i+1:])
			}
			// success, error and denied settle only their own call and never stop the batch.
		}
	}
}

// isCancellation reports whether err is a context-cancellation failure, including wrapped values.
func isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// callbackProtocol observes one logical model effect's use of the assembly callback it was handed: the expected identity is enforced before anything assembles, a fresh invocation after one that returned a *StreamError is another attempt while an invocation after one that produced an output is a protocol violation, and any boundary or invalid-call failure is remembered so the effect cannot ignore it and settle anyway.
type callbackProtocol struct {
	ctx      context.Context
	expected model.ModelRef
	produced bool  // the most recent invocation returned an output.
	failure  error // sticky first protocol violation or assembler invalid-call error.
}

// assemble is the AssemblyCallback handed to one model effect; stream ownership transfers into the assembler exactly like the callback contract says, but only for a correctly identified attempt that follows no produced output.
func (p *callbackProtocol) assemble(source model.ModelRef, stream model.Stream) (model.Output, error) {
	var fail error
	switch {
	case p.produced: // an attempt that produced an output is final: nothing follows it.
		fail = newBoundaryViolation("model", "assembly callback invoked after a previous invocation produced an output")
	case source != p.expected: // the callback carries no authority to substitute another model identity.
		fail = newBoundaryViolation("model", fmt.Sprintf("assembly callback source %q does not equal invocation expected model identity %q", source.String(), p.expected.String()))
	}
	if fail != nil {
		p.remember(fail)
		return model.Output{}, fail
	}
	out, err := Assemble(p.ctx, source, stream)
	if err != nil {
		var streamErr *StreamError
		if !errors.As(err, &streamErr) { // a stream failure is another attempt's business; every other failure is the effect's own protocol breach to answer for.
			p.remember(err)
		}
		return out, err
	}
	p.produced = true
	return out, nil
}

func (p *callbackProtocol) remember(err error) {
	if p.failure == nil {
		p.failure = err
	}
}

// settlementFailure gates an authoritative settlement on the callback discipline behind it: any remembered failure ends the run, and an output is carried exactly when the most recent invocation returned one — every settled output came from one accepted callback, and no output means the latest attempt failed. Callback and settlement outputs are deliberately not compared for content: substituting different otherwise-valid content is a broken trusted caller, not another protocol path.
func (p *callbackProtocol) settlementFailure(out *model.Output) error {
	if p.failure != nil {
		return p.failure
	}
	if out != nil && !p.produced {
		return newBoundaryViolation("model", "settlement carries an output no assembly callback invocation produced")
	}
	if out == nil && p.produced {
		return newBoundaryViolation("model", "settlement carries no output but the most recent assembly callback invocation produced one")
	}
	return nil
}

// interruptionTerminal builds one interruption result; unstarted calls are an owned copy of the retained output's own ordered suffix, so the returned terminal never aliases LastOutput's message through its call slice.
func interruptionTerminal(detail string, out *model.Output, unstarted []model.ToolCall) (TerminalResult, error) {
	res := TerminalResult{Status: TerminalInterruption, LastOutput: out, Detail: detail}
	if len(unstarted) > 0 {
		res.UnstartedCalls = append(make([]model.ToolCall, 0, len(unstarted)), unstarted...)
	}
	return finishTerminal(res)
}

// finishTerminal is the single producer of every terminal Run returns: each constructed result passes the shared terminal validator first. Every branch above is total on the validated settlements it consumes, so a rejection here is an internal inconsistency, reported the same way the assembler reports its own invariant violations.
func finishTerminal(res TerminalResult) (TerminalResult, error) {
	if err := validateTerminalResult(res); err != nil {
		panic(fmt.Sprintf("internal agent invariant violated: constructed terminal result failed validation: %v", err))
	}
	return res, nil
}
