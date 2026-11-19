package tool

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Syntheresis/melta/internal/safefs"
)

// executeParsedPatch is the shared apply engine for an already-parsed patch
// and its prepared targets. Before applying the patch,
// the bound canonical Workspace root, the bound canonical write-dir
// boundary and every bound target are revalidated before any content
// access, every op is located
// against current files before any mutation, and per-op current-content
// checks rerun inside the apply loop. A later mutation or bookkeeping failure
// returns *ExitError with the completed-files A/M/D summary; uncertain
// mutations retain their preimage without claiming a produced identity.
//
// The engine captures per-op pre/post/hunk data during apply and returns it
// as the second value. Callers attach those previews to display metadata so
// renderers do not need post-write disk reads. Read-after-apply is not
// viable for updates, deletes, or moves.
func executeParsedPatch(store SnapshotStore, p *patch, targets []boundCall) (string, []AppliedFilePreview, error) {
	// Revalidate the bound root, the canonical write-dir boundary and every
	// target before content access; the per-op checks rerun where the apply
	// safety requires.
	for _, target := range targets {
		if err := target.revalidate(); err != nil {
			return "", nil, err
		}
	}

	// Phase 1: validate (read-only).
	plans, err := buildApplyPlans(p, targets)
	if err != nil {
		return "", nil, err
	}

	// Phase 2: apply (mutations). Each op tracks its own committed state
	// so a later op's failure can report the earlier committed files. The
	// already-approved Workspace root, write-dir and canonical target
	// witnesses ride to each op's snapshot-to-I/O boundary.
	previews := make([]AppliedFilePreview, 0, len(plans))
	committed := make([]appliedOp, 0, len(plans))
	for i := range plans {
		pl := &plans[i]
		preview, err := applyOne(pl, store, &committed)
		if err != nil {
			return "", nil, err
		}
		previews = append(previews, preview...)
	}
	return buildSuccessSummary(committed), previews, nil
}

type appliedOp struct {
	kind string // "A", "M", "D"
	path string
}

// AppliedFilePreview is the per-file data the engine captures so display
// metadata can be built without post-write disk reads. Pre is the pre-mutation
// content (nil for Add); Post is the post-mutation content (nil for Delete);
// Hunks is the list of applied hunks. A Move produces two entries: destination
// M and source D.
type AppliedFilePreview struct {
	Op    string
	Path  string
	Pre   []string
	Post  []string
	Hunks []AppliedHunkPreview
}

// AppliedHunkPreview carries the data needed to reconstruct a hunk's
// diff rows in DisplayMetadata without re-reading the file. StartLine
// is the 1-based line in Pre where the hunk's old pattern was
// matched; Old is the matched pattern (context + remove lines, in
// order); New is the hunk's new content (context + add lines, in
// order). Diff rows are derived from these by the same row-derivation
// the existing editpreview package uses.
type AppliedHunkPreview struct {
	StartLine int
	Old       []string
	New       []string
	// Lines is the classified hunk lines (kind + text, in original
	// order). DisplayMetadata uses these to build diff rows without
	// re-reading the file: the post-mutation state has overwritten
	// the matched pattern, so a disk read is not viable (Update
	// overwrites, Delete removes, Move unlinks). The Old / New
	// fields are kept for any future consumer that wants the
	// pre-flattened split.
	Lines []hunkLine
}

type applyPlan struct {
	op          fileOp
	newContent  string               // for Add / Update / Move
	preLines    []string             // pre-mutation content (for Update/Delete/Move source)
	preContent  []byte               // exact content validated before mutation
	hunks       []AppliedHunkPreview // applied hunks (for Update/Move destination)
	source      boundCall
	destination boundCall
}

func buildApplyPlans(p *patch, targets []boundCall) ([]applyPlan, error) {
	plans := make([]applyPlan, 0, len(p.ops))
	targetIdx := 0
	for _, op := range p.ops {
		target := targets[targetIdx]
		targetIdx++
		pl := applyPlan{op: op, source: target}

		switch op.kind {
		case opAdd:
			existed, shapeErr := ensureRegularExistingTarget(pl.source.canonical)
			if shapeErr != nil {
				return nil, fmt.Errorf("apply_patch: %s: %w", op.path, shapeErr)
			}
			if existed {
				return nil, fmt.Errorf("apply_patch: %s already exists (Add File requires the path to not exist)", op.path)
			}
			pl.newContent = buildAddContent(op)
		case opUpdate:
			existed, shapeErr := ensureRegularExistingTarget(pl.source.canonical)
			if shapeErr != nil {
				return nil, fmt.Errorf("apply_patch: %s: %w", op.path, shapeErr)
			}
			if !existed {
				return nil, fmt.Errorf("apply_patch: %s does not exist (Update requires the path to exist)", op.path)
			}
			preContent, preLines, content, hunks, err := computeUpdatedContent(pl.source.canonical, op)
			if err != nil {
				return nil, err
			}
			pl.preContent = preContent
			pl.preLines = preLines
			pl.newContent = content
			pl.hunks = hunks
		case opDelete:
			existed, shapeErr := ensureRegularExistingTarget(pl.source.canonical)
			if shapeErr != nil {
				return nil, fmt.Errorf("apply_patch: %s: %w", op.path, shapeErr)
			}
			if !existed {
				return nil, fmt.Errorf("apply_patch: %s does not exist (Delete requires the path to exist)", op.path)
			}
			// Capture pre-mutation content for the preview stash so the
			// GUI / CLI can show the file's pre-delete state.
			data, readErr := readFileBytes(pl.source.canonical)
			if readErr != nil {
				return nil, fmt.Errorf("apply_patch: read %s: %w", op.path, readErr)
			}
			pl.preContent = data
			pl.preLines = splitLinesRaw(data)
		}

		if op.movePath != "" {
			moveTarget := targets[targetIdx]
			targetIdx++
			moveCanonical := moveTarget.canonical
			existed, shapeErr := ensureRegularExistingTarget(moveCanonical)
			if shapeErr != nil {
				return nil, fmt.Errorf("apply_patch: %s (move destination): %w", op.movePath, shapeErr)
			}
			if existed {
				return nil, fmt.Errorf("apply_patch: %s (move destination) already exists (Move over an existing file is not allowed)", op.movePath)
			}
			pl.destination = moveTarget
			if len(op.hunks) == 0 {
				pl.newContent = string(pl.preContent)
			}
		}

		plans = append(plans, pl)
	}
	return plans, nil
}

// buildAddContent joins an Add File's hunk lines back into a single string.
// Each hunk carries a single + line (the parser's Add shape), and a file's
// trailing newline is preserved only if the patch's last line had one.
func buildAddContent(op fileOp) string {
	parts := make([]string, 0, len(op.hunks))
	for _, h := range op.hunks {
		for _, l := range h.lines {
			parts = append(parts, l.Text)
		}
	}
	// If the original input ended with a newline (it usually does), the parser
	// preserves that as a final empty line in the model's content. We do not
	// re-inject it here; the model controls the trailing-newline behavior of
	// Add by including or omitting a bare "+" line at the end.
	return strings.Join(parts, "\n")
}

// computeUpdatedContent reads canonicalPath, applies every hunk in order via
// the Codex fuzzy ladder, and returns the resulting file content as a
// single string (lines joined by \n, with no trailing newline). It also
// returns the pre-mutation content and the applied-hunk descriptors
// (StartLine 1-based, Old pattern, New content) so diff rows can be
// reconstructed without re-reading the file. Hunk location failures
// (Failed to find context / Failed to find expected lines) bubble up
// unchanged so the model sees Codex's exact error string.
func computeUpdatedContent(canonicalPath string, op fileOp) (preContent []byte, preLines []string, content string, hunks []AppliedHunkPreview, err error) {
	data, err := readFileBytes(canonicalPath)
	if err != nil {
		return nil, nil, "", nil, fmt.Errorf("apply_patch: read %s: %w", op.path, err)
	}
	preLines = splitLinesRaw(data)
	lines := preLines
	cursor := 0
	for _, h := range op.hunks {
		if !hunkHasMatchLines(h) {
			return nil, nil, "", nil, fmt.Errorf("apply_patch: Update File %q has a hunk with no context or removed lines", op.path)
		}
		next, end, hunkPrev, lerr := applyHunk(lines, h, op.path, cursor)
		if lerr != nil {
			return nil, nil, "", nil, lerr
		}
		lines = next
		cursor = end
		hunks = append(hunks, hunkPrev)
	}
	return data, preLines, strings.Join(lines, "\n"), hunks, nil
}

func hunkHasMatchLines(h hunk) bool {
	for _, line := range h.lines {
		if line.Kind == lineContext || line.Kind == lineRemove {
			return true
		}
	}
	return false
}

// applyHunk locates h's pattern (context + remove lines) starting at cursor
// and replaces the matched block with the new content. Fuzzy matching is used
// only to locate the hunk: context rows in the replacement are preserved from
// the actual matched file content, while add rows come from the patch.
// Returns the new lines slice, the new cursor (positioned just past the new
// content), and the AppliedHunkPreview so the diff rows can be reconstructed
// without re-reading the file. Anchor handling is inside locate: the anchor
// is located first if present, advancing the cursor; the pattern is then
// located at the advanced cursor.
func applyHunk(fileLines []string, h hunk, path string, cursor int) ([]string, int, AppliedHunkPreview, error) {
	match, err := locateHunk(fileLines, h, path, cursor)
	if err != nil {
		return nil, 0, AppliedHunkPreview{}, err
	}
	start := match.start
	patternLines := hunkPatternLines(h)
	matchEnd := start + match.realMatchedLen
	if matchEnd > len(fileLines) {
		return nil, 0, AppliedHunkPreview{}, fmt.Errorf("Failed to find expected lines in %s:\n%s", path, strings.Join(patternLines, "\n"))
	}
	matchedLines := fileLines[start:matchEnd]
	oldLines, newLines, lines, err := hunkTransformPreservingMatchedContext(h, matchedLines, match.syntheticTrailingEmpty, path, patternLines)
	if err != nil {
		return nil, 0, AppliedHunkPreview{}, err
	}
	out := make([]string, 0, len(fileLines)+len(newLines)-len(oldLines))
	out = append(out, fileLines[:start]...)
	out = append(out, newLines...)
	out = append(out, fileLines[matchEnd:]...)
	return out, start + len(newLines), AppliedHunkPreview{StartLine: start + 1, Old: oldLines, New: newLines, Lines: lines}, nil
}

func hunkPatternLines(h hunk) []string {
	out := make([]string, 0, len(h.lines))
	for _, hl := range h.lines {
		if hl.Kind == lineContext || hl.Kind == lineRemove {
			out = append(out, hl.Text)
		}
	}
	return out
}

func hunkTransformPreservingMatchedContext(h hunk, matchedLines []string, syntheticTrailingEmpty bool, path string, patternLines []string) (oldLines, newLines []string, lines []hunkLine, err error) {
	lines = make([]hunkLine, 0, len(h.lines))
	matchIdx := 0
	for _, hl := range h.lines {
		switch hl.Kind {
		case lineContext:
			if matchIdx >= len(matchedLines) && syntheticTrailingEmpty && hl.Text == "" {
				continue
			}
			text := hl.Text
			if matchIdx < len(matchedLines) {
				text = matchedLines[matchIdx]
			}
			oldLines = append(oldLines, text)
			newLines = append(newLines, text)
			lines = append(lines, hunkLine{Kind: lineContext, Text: text})
			matchIdx++
		case lineRemove:
			if matchIdx >= len(matchedLines) {
				return nil, nil, nil, fmt.Errorf("Failed to find expected lines in %s:\n%s", path, strings.Join(patternLines, "\n"))
			}
			text := hl.Text
			if matchIdx < len(matchedLines) {
				text = matchedLines[matchIdx]
			}
			oldLines = append(oldLines, text)
			lines = append(lines, hunkLine{Kind: lineRemove, Text: text})
			matchIdx++
		case lineAdd:
			newLines = append(newLines, hl.Text)
			lines = append(lines, hl)
		}
	}
	return oldLines, newLines, lines, nil
}

// applyOne applies a single op's mutation and returns the per-file
// preview data captured during the apply. Failures include the completed-files
// A/M/D list in *ExitError.
func applyOne(pl *applyPlan, store SnapshotStore, committed *[]appliedOp) ([]AppliedFilePreview, error) {
	if err := revalidateApplyPlan(pl); err != nil {
		return nil, &ExitError{Output: buildPartialSummary(*committed) + "\n\n" + err.Error()}
	}

	// Move: write the destination first (so it lands), then snapshot + delete the source.
	if pl.op.movePath != "" {
		outcome, err := writeBound(pl.destination, store, []byte(pl.newContent), true)
		if outcome == mutationCompleted {
			*committed = append(*committed, appliedOp{kind: "M", path: pl.op.movePath})
		}
		if err != nil {
			return nil, &ExitError{Output: buildPartialSummary(*committed) + "\n\n" + err.Error()}
		}
		if err := revalidateApplySourceContent(pl); err != nil {
			return nil, &ExitError{Output: buildPartialSummary(*committed) + "\n\n" + err.Error()}
		}
	}

	// Now the original-path mutation (Add / Update / Delete), tracked separately for Move.
	origErr := applyOriginalMutation(pl, store, committed)
	if origErr != nil {
		return nil, &ExitError{Output: buildPartialSummary(*committed) + "\n\n" + origErr.Error()}
	}
	return buildOpPreviews(pl), nil
}

func revalidateApplyPlan(pl *applyPlan) error {
	if err := pl.source.revalidate(); err != nil {
		return err
	}
	if pl.op.movePath != "" {
		if err := pl.destination.revalidate(); err != nil {
			return err
		}
	}
	if pl.op.kind == opUpdate || pl.op.kind == opDelete || pl.op.movePath != "" {
		data, err := readFileBytes(pl.source.canonical)
		if err != nil {
			return fmt.Errorf("apply_patch: read %s before mutation: %w", pl.op.path, err)
		}
		if !bytes.Equal(data, pl.preContent) {
			return fmt.Errorf("apply_patch: %s changed after validation", pl.op.path)
		}
	}
	return nil
}

func revalidateApplySourceContent(pl *applyPlan) error {
	data, err := readFileBytes(pl.source.canonical)
	if err != nil {
		return fmt.Errorf("apply_patch: read %s before mutation: %w", pl.op.path, err)
	}
	if !bytes.Equal(data, pl.preContent) {
		return fmt.Errorf("apply_patch: %s changed after validation", pl.op.path)
	}
	return nil
}

// buildOpPreviews assembles the per-op preview data captured during
// buildApplyPlans / apply for DisplayMetadata to read. Move produces
// two entries (destination M with the new content and the applied
// hunks, source D with the original content and no hunks); other ops
// produce one entry each.
func buildOpPreviews(pl *applyPlan) []AppliedFilePreview {
	switch {
	case pl.op.movePath != "":
		postLines := splitLinesRaw([]byte(pl.newContent))
		return []AppliedFilePreview{
			{Op: "M", Path: pl.op.movePath, Post: postLines, Hunks: pl.hunks},
			{Op: "D", Path: pl.op.path, Pre: pl.preLines},
		}
	case pl.op.kind == opAdd:
		postLines := splitLinesRaw([]byte(pl.newContent))
		return []AppliedFilePreview{{Op: "A", Path: pl.op.path, Post: postLines}}
	case pl.op.kind == opUpdate:
		postLines := splitLinesRaw([]byte(pl.newContent))
		return []AppliedFilePreview{{Op: "M", Path: pl.op.path, Pre: pl.preLines, Post: postLines, Hunks: pl.hunks}}
	case pl.op.kind == opDelete:
		return []AppliedFilePreview{{Op: "D", Path: pl.op.path, Pre: pl.preLines}}
	}
	return nil
}

func applyOriginalMutation(pl *applyPlan, store SnapshotStore, committed *[]appliedOp) error {
	var outcome mutationOutcome
	var err error
	kind := ""
	if pl.op.movePath != "" {
		kind = "D"
		outcome, err = deleteBound(pl.source, store)
	} else {
		switch pl.op.kind {
		case opAdd:
			kind = "A"
			outcome, err = writeBound(pl.source, store, []byte(pl.newContent), true)
		case opUpdate:
			kind = "M"
			outcome, err = writeBound(pl.source, store, []byte(pl.newContent), false)
		case opDelete:
			kind = "D"
			outcome, err = deleteBound(pl.source, store)
		}
	}
	if outcome == mutationCompleted {
		*committed = append(*committed, appliedOp{kind: kind, path: pl.op.path})
	}
	return err
}

func deleteBound(bound boundCall, store SnapshotStore) (mutationOutcome, error) {
	return mutateBound(bound, store, nil, func() (mutationOutcome, []byte, bool, error) {
		if err := safefs.RemoveLeaf(bound.canonical); err != nil {
			return mutationUntouched, nil, false, fmt.Errorf("remove: %w", err)
		}
		return mutationCompleted, nil, true, nil
	})
}

func buildSuccessSummary(committed []appliedOp) string {
	if len(committed) == 0 {
		return "Success. Updated the following files:"
	}
	var b strings.Builder
	b.WriteString("Success. Updated the following files:\n")
	for _, op := range committed {
		b.WriteString(op.kind)
		b.WriteByte(' ')
		b.WriteString(op.path)
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

func buildPartialSummary(committed []appliedOp) string {
	if len(committed) == 0 {
		return "Success. Updated the following files:"
	}
	return buildSuccessSummary(committed)
}

// readFileBytes reads the entire file at canonicalPath with O_NOFOLLOW and
// returns the raw bytes. Used by the Update/Move path to
// read the file's current content; raw bytes (including \r) ride through
// unchanged.
func readFileBytes(canonicalPath string) ([]byte, error) {
	f, err := safefs.OpenExisting(canonicalPath, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// splitLinesRaw splits data on \n and returns the lines. A trailing \n
// produces a final empty string in the slice, matching the V4A convention
// that a file's blank last line is a context line of text "". Raw bytes
// (including \r) ride through inside each line.
func splitLinesRaw(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	return strings.Split(string(data), "\n")
}
