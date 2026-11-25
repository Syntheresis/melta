package patch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/Syntheresis/melta/extension"
)

const workspaceService = "melta.workspace"

// callFunc is the extension host-call shape one tool receives in its
// AuthorContext. The engine reaches files only through the Host's Workspace
// operations.
type callFunc func(context.Context, string, string, json.RawMessage) (extension.Result, error)

type outcome uint8

const (
	outcomeUntouched outcome = iota
	outcomeCompleted
	outcomeUncertain
)

// boundTarget is one path bound during preparation: the Host-issued binding
// handle and its canonical witness.
type boundTarget struct {
	Binding   string `json:"binding"`
	Canonical string `json:"canonical"`
	Path      string `json:"path"`
}

// planData is the prepared plan carried from Prepare to Execute: the call
// identity and the parsed operations with their bound targets.
type planData struct {
	CallID string
	Ops    []fileOp
}

// wireOp is the plan's JSON shape; fileOp keeps the parser's private fields.
type wireOp struct {
	Kind        opKind       `json:"kind"`
	Path        string       `json:"path"`
	MovePath    string       `json:"move_path,omitempty"`
	Hunks       []hunk       `json:"hunks,omitempty"`
	Source      boundTarget  `json:"source"`
	Destination *boundTarget `json:"destination,omitempty"`
}

func (p planData) MarshalJSON() ([]byte, error) {
	ops := make([]wireOp, 0, len(p.Ops))
	for _, op := range p.Ops {
		ops = append(ops, wireOp{Kind: op.kind, Path: op.path, MovePath: op.movePath, Hunks: op.hunks, Source: op.source, Destination: op.destination})
	}
	return json.Marshal(struct {
		CallID string   `json:"call_id"`
		Ops    []wireOp `json:"ops"`
	}{p.CallID, ops})
}

func (p *planData) UnmarshalJSON(data []byte) error {
	var wire struct {
		CallID string   `json:"call_id"`
		Ops    []wireOp `json:"ops"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	p.CallID = wire.CallID
	p.Ops = make([]fileOp, 0, len(wire.Ops))
	for _, op := range wire.Ops {
		p.Ops = append(p.Ops, fileOp{kind: op.Kind, path: op.Path, movePath: op.MovePath, hunks: op.Hunks, source: op.Source, destination: op.Destination})
	}
	return nil
}

// preparePlan parses the patch, binds every source and destination through
// the Workspace service and rejects two targets with the same canonical path.
func preparePlan(ctx context.Context, call callFunc, input string) (planData, error) {
	parsed, err := parsePatch(input)
	if err != nil {
		return planData{}, err
	}
	plan := planData{Ops: make([]fileOp, 0, len(parsed.ops))}
	targets := make([]boundTarget, 0, len(parsed.ops)*2)
	for _, parsedOp := range parsed.ops {
		op := parsedOp
		op.source, err = bindTarget(ctx, call, op.path)
		if err != nil {
			return planData{}, err
		}
		targets = append(targets, op.source)
		if op.movePath != "" {
			destination, err := bindTarget(ctx, call, op.movePath)
			if err != nil {
				return planData{}, err
			}
			op.destination = &destination
			targets = append(targets, destination)
		}
		plan.Ops = append(plan.Ops, op)
	}
	seen := make(map[string]bool, len(targets))
	for _, target := range targets {
		key := filepath.Clean(target.Canonical)
		if seen[key] {
			return planData{}, fmt.Errorf("apply_patch: multiple patch entries resolve to the same file: %s", target.Canonical)
		}
		seen[key] = true
	}
	return plan, nil
}

func bindTarget(ctx context.Context, call callFunc, path string) (boundTarget, error) {
	args, err := json.Marshal(map[string]string{"path": path})
	if err != nil {
		return boundTarget{}, err
	}
	result, err := call(ctx, workspaceService, "bind", args)
	if err != nil {
		return boundTarget{}, err
	}
	var bound struct {
		Binding       string `json:"binding"`
		CanonicalPath string `json:"canonical_path"`
	}
	if err := json.Unmarshal(result.Value, &bound); err != nil {
		return boundTarget{}, fmt.Errorf("apply_patch: bind %s: %w", path, err)
	}
	if bound.Binding == "" || bound.CanonicalPath == "" {
		return boundTarget{}, fmt.Errorf("apply_patch: bind %s: empty binding", path)
	}
	return boundTarget{Binding: bound.Binding, Canonical: bound.CanonicalPath, Path: path}, nil
}

func readTarget(ctx context.Context, call callFunc, target boundTarget) ([]byte, error) {
	args, err := json.Marshal(map[string]string{"binding": target.Binding})
	if err != nil {
		return nil, err
	}
	result, err := call(ctx, workspaceService, "read", args)
	if err != nil {
		return nil, err
	}
	var read struct {
		Bytes []byte `json:"bytes"`
	}
	if err := json.Unmarshal(result.Value, &read); err != nil {
		return nil, fmt.Errorf("apply_patch: read %s: %w", target.Path, err)
	}
	return read.Bytes, nil
}

func writeTarget(ctx context.Context, call callFunc, target boundTarget, content []byte, createOnly bool) (outcome, error) {
	args, err := json.Marshal(struct {
		Binding    string `json:"binding"`
		Bytes      []byte `json:"bytes"`
		CreateOnly bool   `json:"create_only"`
	}{target.Binding, content, createOnly})
	if err != nil {
		return outcomeUntouched, err
	}
	result, err := call(ctx, workspaceService, "write", args)
	return mutationOutcome(result, err)
}

func deleteTarget(ctx context.Context, call callFunc, target boundTarget) (outcome, error) {
	args, err := json.Marshal(map[string]string{"binding": target.Binding})
	if err != nil {
		return outcomeUntouched, err
	}
	result, err := call(ctx, workspaceService, "delete", args)
	return mutationOutcome(result, err)
}

// mutationOutcome reads the Host's reported completion independently of its
// error: an error carrying a partial outcome still reports what the producer
// knows.
func mutationOutcome(result extension.Result, err error) (outcome, error) {
	if err != nil {
		var callErr *extension.CallError
		if errors.As(err, &callErr) {
			return decodeOutcome(callErr.Partial), err
		}
		return outcomeUntouched, err
	}
	return decodeOutcome(result.Value), nil
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

// buildApplyPlans establishes every target's current existence through
// Workspace reads and validates every hunk before the first mutation.
func buildApplyPlans(ctx context.Context, call callFunc, plan planData) ([]applyPlan, error) {
	plans := make([]applyPlan, 0, len(plan.Ops))
	for _, op := range plan.Ops {
		pl := applyPlan{op: op}
		switch op.kind {
		case opAdd:
			absent, err := targetAbsent(ctx, call, op.source)
			if err != nil {
				return nil, fmt.Errorf("apply_patch: read %s: %w", op.path, err)
			}
			if !absent {
				return nil, fmt.Errorf("apply_patch: %s already exists (Add File requires the path to not exist)", op.path)
			}
			pl.newContent = buildAddContent(op)
		case opUpdate:
			data, err := readExisting(ctx, call, op.source, "Update")
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
			data, err := readExisting(ctx, call, op.source, "Delete")
			if err != nil {
				return nil, err
			}
			pl.preContent = data
			pl.preLines = splitLinesRaw(data)
		}
		if op.movePath != "" {
			absent, err := targetAbsent(ctx, call, *op.destination)
			if err != nil {
				return nil, fmt.Errorf("apply_patch: read %s before apply: %w", op.movePath, err)
			}
			if !absent {
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

// targetAbsent reports whether the bound target's current read reports not
// found. Any other outcome is an error, including a successful read.
func targetAbsent(ctx context.Context, call callFunc, target boundTarget) (bool, error) {
	_, err := readTarget(ctx, call, target)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	return false, err
}

// readExisting reads one source that the patch requires to exist.
func readExisting(ctx context.Context, call callFunc, target boundTarget, requirement string) ([]byte, error) {
	data, err := readTarget(ctx, call, target)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("apply_patch: %s does not exist (%s requires the path to exist)", target.Path, requirement)
		}
		return nil, fmt.Errorf("apply_patch: read %s: %w", target.Path, err)
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
func executePlan(ctx context.Context, call callFunc, plan planData) (string, []appliedFilePreview, error) {
	plans, err := buildApplyPlans(ctx, call, plan)
	if err != nil {
		return "", nil, err
	}
	previews := make([]appliedFilePreview, 0, len(plans))
	committed := make([]appliedOp, 0, len(plans))
	for i := range plans {
		preview, err := applyOne(ctx, call, &plans[i], &committed)
		if err != nil {
			return buildSuccessSummary(committed), nil, err
		}
		previews = append(previews, preview...)
	}
	return buildSuccessSummary(committed), previews, nil
}

func applyOne(ctx context.Context, call callFunc, pl *applyPlan, committed *[]appliedOp) ([]appliedFilePreview, error) {
	if pl.op.movePath != "" {
		if err := revalidateSourceContent(ctx, call, pl); err != nil {
			return nil, err
		}
		writeOutcome, err := writeTarget(ctx, call, *pl.op.destination, []byte(pl.newContent), true)
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

func revalidateSourceContent(ctx context.Context, call callFunc, pl *applyPlan) error {
	data, err := readTarget(ctx, call, pl.op.source)
	if err != nil {
		return fmt.Errorf("apply_patch: read %s before mutation: %w", pl.op.path, err)
	}
	if !bytes.Equal(data, pl.preContent) {
		return fmt.Errorf("apply_patch: %s changed after validation", pl.op.path)
	}
	return nil
}

func applyOriginalMutation(ctx context.Context, call callFunc, pl *applyPlan, committed []appliedOp) ([]appliedOp, error) {
	var result outcome
	var err error
	kind := ""
	switch {
	case pl.op.movePath != "":
		kind = "D"
		result, err = deleteTarget(ctx, call, pl.op.source)
	case pl.op.kind == opAdd:
		kind = "A"
		result, err = writeTarget(ctx, call, pl.op.source, []byte(pl.newContent), true)
	case pl.op.kind == opUpdate:
		kind = "M"
		result, err = writeTarget(ctx, call, pl.op.source, []byte(pl.newContent), false)
	case pl.op.kind == opDelete:
		kind = "D"
		result, err = deleteTarget(ctx, call, pl.op.source)
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
