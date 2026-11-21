package agent

import (
	"context"
	"fmt"
	"sort"

	"github.com/Syntheresis/melta/model"
)

// firstIncompleteToolSlot returns the lowest normalized position whose observed slot does not end structurally complete (non-empty id and name), or -1 when every observed slot is complete. It reads state only and is used by the final-state incomplete-slot pin.
func (st *assemblyState) firstIncompleteToolSlot() int {
	if len(st.toolDeltas) == 0 {
		return -1
	}

	order := make([]int, 0, len(st.toolDeltas)) // sorted slots keep the reported position deterministic across runs even though the underlying state is a map.
	for pos := range st.toolDeltas {
		order = append(order, pos)
	}
	sort.Ints(order)

	for _, pos := range order {
		if entry := st.toolDeltas[pos]; entry.id == "" || entry.name == "" { // every slot materialized during correlation is an observation of a tool call; at its current state it must be complete in identity and name.
			return pos
		}
	}

	return -1
}

func (st *assemblyState) finalize(ctx context.Context, readErr error) (model.Output, error) {
	if readErr != nil && st.finishReason == "" { // the response never ended: interruption under a done context, a stream failure under a live one.
		if ctx.Err() != nil {
			return st.interruptedOutput(ctx), nil // interruption discards every tool-call block, complete or not.
		}
		return model.Output{}, &StreamError{Err: readErr, Usage: st.usage} // no output: the attempt failed before the provider ended the response.
	}

	if r := st.role; r != "" && model.Role(r) != model.RoleAssistant {
		st.noteConflict("response role %q is not assistant", r)
	}

	st.pinIncompleteOpaque()

	st.pinIncompleteToolCall() // every observed normalized tool-call slot must end structurally complete — an unfinished one, with data in it or without, errors the whole response instead of being silently dropped when another valid call exists.

	if st.conflictDetail != "" {
		return st.erroredOutput(st.conflictDetail), nil
	}

	return st.cleanTermination(), nil
}

// pinIncompleteOpaque pins a final-state conflict for any content position whose opaque kind never received its structural wire type. Such a part can neither complete an output nor be retained, so the response errors on it no matter how cleanly the stream otherwise terminated.
func (st *assemblyState) pinIncompleteOpaque() {
	for _, pos := range st.contentOrder { // arrival order suffices: noteConflict keeps whichever position reports first and nothing downstream depends on which one that is.
		if acc := st.content[pos]; acc.kind == model.PartOpaque && acc.opaque == "" {
			st.noteConflict("content at position %d is an incomplete opaque part: no wire type arrived", pos)
			return // set-once keeps the first pinned conflict; a second pin would be dead weight.
		}
	}
}

// pinIncompleteToolCall pins a final-state conflict for every observed normalized tool-call slot that does not end structurally complete — whether it accumulated only part of its identity or nothing at all: an unfinished block must error the response rather than vanish silently when another valid call happens to exist in the same payload.
func (st *assemblyState) pinIncompleteToolCall() {
	if pos := st.firstIncompleteToolSlot(); pos >= 0 { // the non-mutating completeness predicate, here turned into the shared set-once conflict detail.
		st.noteConflict("tool call at position %d is incomplete: missing id or name", pos)
	}
}

// finishStateVerdict applies the one shared finish/payload consistency matrix over a single observed semantic snapshot, reporting whether completion holds for it and — when not — exactly the wording an errored output carries on that same shape: cleanTermination consumes this helper for every termination path.
func (st *assemblyState) finishStateVerdict() (bool, string) {
	calls := st.completeCallCount()

	switch st.finishReason { // the closed reason vocabulary decides which half of the matrix applies to this snapshot before any payload check runs below it in wire order respectively left-to-right as they appear here now.
	case "": // an absent explicit reason completes whenever any eligible payload or valid calls exist at all — the truly empty response is its only errored shape on this branch above these lines verbatim (the fallback row of the same shared matrix rather than a separate special case anywhere downstream along this trajectory forward now).
		if calls > 0 || st.hasPayload() {
			return true, ""
		}

		return false, "empty response: no eligible content, refusal or tool calls"

	case "stop": // one payload-bearing message with zero valid calls is the only stop shape this matrix accepts on this path respectively per contract documented inline within its own single-line comment further up over there.
		if calls == 0 && st.hasPayload() {
			return true, ""
		}

		if calls > 0 {
			return false, "finish reason stop with present tool calls is a payload mismatch"
		}

		return false, "finish reason stop requires payload through content, refusal or finalized message extras"

	case "tool_calls": // at least one valid call must justify this finish reason on its own before any other state may rescue it respectively per contract above these lines verbatim without duplication elsewhere in wire order now.
		if calls > 0 {
			return true, ""
		}

		return false, "finish reason tool_calls requires one or more valid tool calls"

	default: // every other explicit value is unsupported under this closed finish-reason set rather than silently mapped onto some near-neighbor branch anywhere downstream along this trajectory forward now.
		return false, fmt.Sprintf("unsupported finish reason %q", st.finishReason)
	}
}

func (st *assemblyState) cleanTermination() model.Output { // the single consumer of the shared matrix: completion when its verdict holds, otherwise the errored output carrying that verdict's own wording.
	ok, detail := st.finishStateVerdict()

	if ok {
		return st.completedOutput()
	}

	return st.erroredOutput(detail)
}

func (st *assemblyState) completedOutput() model.Output {
	msg := st.assembleMessage(true)
	out, err := model.NewOutput(model.Output{Status: model.OutputCompleted, Source: st.source, Message: &msg, Usage: st.usage})
	if err != nil {
		panic(fmt.Sprintf("internal assembler invariant violated: completed output failed model-output validation: %v", err))
	}

	return out
}

func (st *assemblyState) erroredOutput(detail string) model.Output {
	out, err := model.NewOutput(model.Output{Status: model.OutputErrored, Source: st.source, Usage: st.usage, Detail: detail})
	if err != nil {
		panic(fmt.Sprintf("internal assembler invariant violated: errored output failed model-output validation: %v", err))
	}
	return out
}

// interruptedOutput classifies a read failure observed before a finish reason arrived while the run context was already cancelled: the output keeps its usage, and a message only when text content or a refusal had started, without tool calls.
func (st *assemblyState) interruptedOutput(ctx context.Context) model.Output {
	msg := st.assembleMessage(false)
	var msgPtr *model.Message
	if msg.TextContent() != "" || msg.Refusal != "" { // reasoning, extras or opaque parts alone are not a kept partial.
		m := msg
		msgPtr = &m
	}

	out, err := model.NewOutput(model.Output{Status: model.OutputInterrupted, Source: st.source, Message: msgPtr, Usage: st.usage, Detail: fmt.Sprintf("stream interrupted: %v", ctx.Err())})
	if err != nil {
		panic(fmt.Sprintf("internal assembler invariant violated: interrupted output failed model-output validation: %v", err))
	}

	return out
}
