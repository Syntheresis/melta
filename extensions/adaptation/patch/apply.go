package patch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/Syntheresis/melta/extension"
)

type outcome uint8

const (
	outcomeUntouched outcome = iota
	outcomeCompleted
	outcomeUncertain
)

// parseAndStatTargets parses the patch, stats every target and rejects duplicate paths.
func parseAndStatTargets(ctx context.Context, call extension.AuthorContext, input string) ([]fileOp, error) {
	parsed, err := parsePatch(input)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(parsed.ops)*2)
	for i := range parsed.ops {
		op := &parsed.ops[i]
		if op.sourceExists, err = statTarget(ctx, call, op.path, seen); err != nil {
			return nil, err
		}
		if op.movePath != "" {
			if op.destinationExists, err = statTarget(ctx, call, op.movePath, seen); err != nil {
				return nil, err
			}
		}
	}
	return parsed.ops, nil
}

// statTarget reports whether one patch target exists and refuses it when an
// earlier entry of the same patch resolved to the same file.
func statTarget(ctx context.Context, call extension.AuthorContext, path string, seen map[string]bool) (bool, error) {
	result, err := call.StatFile(ctx, path)
	if err != nil {
		return false, err
	}
	key := filepath.Clean(result.Path)
	if seen[key] {
		return false, fmt.Errorf("apply_patch: multiple patch entries resolve to the same file: %s", result.Path)
	}
	seen[key] = true
	return result.Exists, nil
}

func writeTarget(ctx context.Context, call extension.AuthorContext, path string, content []byte, createOnly bool) (outcome, error) {
	var err error
	if createOnly {
		err = call.CreateFile(ctx, path, content)
	} else {
		err = call.WriteFile(ctx, path, content)
	}
	return mutationOutcome(err)
}

func deleteTarget(ctx context.Context, call extension.AuthorContext, path string) (outcome, error) {
	return mutationOutcome(call.DeleteFile(ctx, path))
}

// mutationOutcome reads the Host's reported completion independently of its
// error: an error carrying a partial outcome still reports what the producer
// knows.
func mutationOutcome(err error) (outcome, error) {
	if err != nil {
		var callErr *extension.CallError
		if errors.As(err, &callErr) {
			return decodeOutcome(callErr.Partial), err
		}
		return outcomeUntouched, err
	}
	return outcomeCompleted, nil
}

func decodeOutcome(value json.RawMessage) outcome {
	var reported struct {
		Outcome string `json:"outcome"`
	}
	if len(value) == 0 || json.Unmarshal(value, &reported) != nil {
		return outcomeUntouched
	}
	switch reported.Outcome {
	case "completed":
		return outcomeCompleted
	case "uncertain":
		return outcomeUncertain
	default:
		return outcomeUntouched
	}
}

type appliedOp struct {
	kind      string
	path      string
	uncertain bool
}

type applyPlan struct {
	op         fileOp
	newContent string
	preLines   []string
	preContent []byte
	hunks      []appliedHunkPreview
}

// buildApplyPlans validates existence and every hunk before the first mutation.
func buildApplyPlans(ctx context.Context, call extension.AuthorContext, ops []fileOp) ([]applyPlan, error) {
	plans := make([]applyPlan, 0, len(ops))
	for _, op := range ops {
		pl := applyPlan{op: op}
		switch op.kind {
		case opAdd:
			if op.sourceExists {
				return nil, fmt.Errorf("apply_patch: %s already exists (Add File requires the path to not exist)", op.path)
			}
			pl.newContent = buildAddContent(op)
		case opUpdate:
			data, err := readExisting(ctx, call, op.path, op.sourceExists, "Update")
			if err != nil {
				return nil, err
			}
			pl.preContent = data
			pl.preLines = splitLinesRaw(data)
			pl.newContent, pl.hunks, err = computeUpdatedContent(data, op)
			if err != nil {
				return nil, err
			}
		case opDelete:
			data, err := readExisting(ctx, call, op.path, op.sourceExists, "Delete")
			if err != nil {
				return nil, err
			}
			pl.preContent = data
			pl.preLines = splitLinesRaw(data)
		}
		if op.movePath != "" {
			if op.destinationExists {
				return nil, fmt.Errorf("apply_patch: %s (move destination) already exists (Move over an existing file is not allowed)", op.movePath)
			}
			if len(op.hunks) == 0 {
				pl.newContent = string(pl.preContent)
			}
		}
		plans = append(plans, pl)
	}
	return plans, nil
}

// readExisting reads one source that the patch requires to exist.
func readExisting(ctx context.Context, call extension.AuthorContext, path string, exists bool, requirement string) ([]byte, error) {
	if !exists {
		return nil, fmt.Errorf("apply_patch: %s does not exist (%s requires the path to exist)", path, requirement)
	}
	data, err := call.ReadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("apply_patch: read %s: %w", path, err)
	}
	return data, nil
}

// buildAddContent joins an Add File's hunk lines back into a single string.
func buildAddContent(op fileOp) string {
	parts := make([]string, 0, len(op.hunks))
	for _, h := range op.hunks {
		for _, l := range h.Lines {
			parts = append(parts, l.Text)
		}
	}
	return strings.Join(parts, "\n")
}

func computeUpdatedContent(data []byte, op fileOp) (content string, hunks []appliedHunkPreview, err error) {
	preLines := splitLinesRaw(data)
	lines := preLines
	cursor := 0
	for _, h := range op.hunks {
		if !hunkHasMatchLines(h) {
			return "", nil, fmt.Errorf("apply_patch: Update File %q has a hunk with no context or removed lines", op.path)
		}
		next, end, hunkPrev, lerr := applyHunk(lines, h, op.path, cursor)
		if lerr != nil {
			return "", nil, lerr
		}
		lines = next
		cursor = end
		hunks = append(hunks, hunkPrev)
	}
	return strings.Join(lines, "\n"), hunks, nil
}

func hunkHasMatchLines(h hunk) bool {
	for _, line := range h.Lines {
		if line.Kind == lineContext || line.Kind == lineRemove {
			return true
		}
	}
	return false
}

func applyHunk(fileLines []string, h hunk, path string, cursor int) ([]string, int, appliedHunkPreview, error) {
	match, err := locateHunk(fileLines, h, path, cursor)
	if err != nil {
		return nil, 0, appliedHunkPreview{}, err
	}
	start := match.start
	patternLines := hunkPatternLines(h)
	matchEnd := start + match.realMatchedLen
	if matchEnd > len(fileLines) {
		return nil, 0, appliedHunkPreview{}, fmt.Errorf("Failed to find expected lines in %s:\n%s", path, strings.Join(patternLines, "\n"))
	}
	matchedLines := fileLines[start:matchEnd]
	oldLines, newLines, lines, err := hunkTransformPreservingMatchedContext(h, matchedLines, match.syntheticTrailingEmpty, path, patternLines)
	if err != nil {
		return nil, 0, appliedHunkPreview{}, err
	}
	out := make([]string, 0, len(fileLines)+len(newLines)-len(oldLines))
	out = append(out, fileLines[:start]...)
	out = append(out, newLines...)
	out = append(out, fileLines[matchEnd:]...)
	return out, start + len(newLines), appliedHunkPreview{StartLine: start + 1, Old: oldLines, New: newLines, Lines: lines}, nil
}

func hunkPatternLines(h hunk) []string {
	out := make([]string, 0, len(h.Lines))
	for _, hl := range h.Lines {
		if hl.Kind == lineContext || hl.Kind == lineRemove {
			out = append(out, hl.Text)
		}
	}
	return out
}

func hunkTransformPreservingMatchedContext(h hunk, matchedLines []string, syntheticTrailingEmpty bool, path string, patternLines []string) (oldLines, newLines []string, lines []hunkLine, err error) {
	lines = make([]hunkLine, 0, len(h.Lines))
	matchIdx := 0
	for _, hl := range h.Lines {
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

// executePlan applies each operation in plan order. Update, delete and move
// sources are reread immediately before their mutation; a move rereads its
// source before the create-only destination write and once more before
// deleting the source. The returned summary and previews are built from each
// target's actual reported outcome.
func executePlan(ctx context.Context, call extension.AuthorContext, plan []fileOp) (string, []appliedFilePreview, error) {
	plans, err := buildApplyPlans(ctx, call, plan)
	if err != nil {
		return "", nil, err
	}
	previews := make([]appliedFilePreview, 0, len(plans))
	committed := make([]appliedOp, 0, len(plans))
	for i := range plans {
		preview, err := applyOne(ctx, call, &plans[i], &committed)
		if err != nil {
			if len(committed) == 0 {
				return "", nil, err
			}
			return buildSuccessSummary(committed), nil, err
		}
		previews = append(previews, preview...)
	}
	return buildSuccessSummary(committed), previews, nil
}

func applyOne(ctx context.Context, call extension.AuthorContext, pl *applyPlan, committed *[]appliedOp) ([]appliedFilePreview, error) {
	if pl.op.movePath != "" {
		if err := revalidateSourceContent(ctx, call, pl); err != nil {
			return nil, err
		}
		writeOutcome, err := writeTarget(ctx, call, pl.op.movePath, []byte(pl.newContent), true)
		if writeOutcome == outcomeCompleted || writeOutcome == outcomeUncertain {
			*committed = append(*committed, appliedOp{kind: "M", path: pl.op.movePath, uncertain: writeOutcome == outcomeUncertain})
		}
		if err != nil {
			return nil, err
		}
		if err := revalidateSourceContent(ctx, call, pl); err != nil {
			return nil, err
		}
	} else if pl.op.kind == opUpdate || pl.op.kind == opDelete {
		if err := revalidateSourceContent(ctx, call, pl); err != nil {
			return nil, err
		}
	}
	committedOps, err := applyOriginalMutation(ctx, call, pl, *committed)
	*committed = committedOps
	if err != nil {
		return nil, err
	}
	return buildOpPreviews(pl), nil
}

func revalidateSourceContent(ctx context.Context, call extension.AuthorContext, pl *applyPlan) error {
	data, err := call.ReadFile(ctx, pl.op.path)
	if err != nil {
		return fmt.Errorf("apply_patch: read %s before mutation: %w", pl.op.path, err)
	}
	if !bytes.Equal(data, pl.preContent) {
		return fmt.Errorf("apply_patch: %s changed after validation", pl.op.path)
	}
	return nil
}

func applyOriginalMutation(ctx context.Context, call extension.AuthorContext, pl *applyPlan, committed []appliedOp) ([]appliedOp, error) {
	var result outcome
	var err error
	kind := ""
	switch {
	case pl.op.movePath != "":
		kind = "D"
		result, err = deleteTarget(ctx, call, pl.op.path)
	case pl.op.kind == opAdd:
		kind = "A"
		result, err = writeTarget(ctx, call, pl.op.path, []byte(pl.newContent), true)
	case pl.op.kind == opUpdate:
		kind = "M"
		result, err = writeTarget(ctx, call, pl.op.path, []byte(pl.newContent), false)
	case pl.op.kind == opDelete:
		kind = "D"
		result, err = deleteTarget(ctx, call, pl.op.path)
	}
	if result == outcomeCompleted || result == outcomeUncertain {
		committed = append(committed, appliedOp{kind: kind, path: pl.op.path, uncertain: result == outcomeUncertain})
	}
	return committed, err
}

func buildOpPreviews(pl *applyPlan) []appliedFilePreview {
	switch {
	case pl.op.movePath != "":
		postLines := splitLinesRaw([]byte(pl.newContent))
		return []appliedFilePreview{
			{Op: "M", Path: pl.op.movePath, Post: postLines, Hunks: pl.hunks},
			{Op: "D", Path: pl.op.path, Pre: pl.preLines},
		}
	case pl.op.kind == opAdd:
		postLines := splitLinesRaw([]byte(pl.newContent))
		return []appliedFilePreview{{Op: "A", Path: pl.op.path, Post: postLines}}
	case pl.op.kind == opUpdate:
		postLines := splitLinesRaw([]byte(pl.newContent))
		return []appliedFilePreview{{Op: "M", Path: pl.op.path, Pre: pl.preLines, Post: postLines, Hunks: pl.hunks}}
	case pl.op.kind == opDelete:
		return []appliedFilePreview{{Op: "D", Path: pl.op.path, Pre: pl.preLines}}
	}
	return nil
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
		if op.uncertain {
			b.WriteString(" (uncertain)")
		}
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

// splitLinesRaw splits data on \n and returns the lines. A trailing \n
// produces a final empty string, matching the V4A convention that a file's
// blank last line is a context line of text "". Raw bytes (including \r) ride
// through inside each line.
func splitLinesRaw(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	return strings.Split(string(data), "\n")
}
