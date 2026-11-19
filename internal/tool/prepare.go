package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/pathutil"
	"github.com/Syntheresis/melta/internal/safefs"
)

// PreparedTarget is one canonical target bound during preparation. The
// permission-pair conversion consumes the canonical path; the permission
// name follows from the preparing tool (file.read for reads including the
// authorized suggestion-directory binding, file.write for mutations).
type PreparedTarget struct {
	CanonicalPath string
}

// PreparedCall is the shared preparation output for one read/write/edit
// call: every declared canonical target in declaration order, and one
// execution closure. Preparation performs only
// path/stat/canonicalization and pure parsing; content reads, precondition
// checks, snapshot capture and mutation run inside Execute. The closure
// revalidates the bound canonical Workspace root, write-dir and every
// target before content access and never silently substitutes a changed
// canonical path.
type PreparedCall struct {
	Targets []PreparedTarget
	Execute func(context.Context) (string, error)
}

// PreparedPatchResult is the structured execution result of one prepared
// apply_patch call: the model-visible summary plus the per-file preview
// data the engine captured, in the exported engine shape, so the target
// plugin can convert previews to its metadata.
type PreparedPatchResult struct {
	Result   string
	Previews []AppliedFilePreview
}

// PreparedPatchCall is the preparation output for one apply_patch call.
// The execution closure consumes the patch parsed during preparation — no
// redecode or reparse — and revalidates the bound root and every target
// before content access.
type PreparedPatchCall struct {
	Targets []PreparedTarget
	Execute func(context.Context) (PreparedPatchResult, error)
}

// readTarget is the private per-target binding read preparation tracks;
// the display path stays private to the suggestion path.
type readTarget struct {
	displayPath   string
	canonicalPath string
}

// consumedIntAtUse parses one canonical-integer json.Number argument to int
// at its point of use. Normalization owns every default, clamp and
// validation; this consumption accepts only the normalized input, and any
// other value reads as zero — the same fallback the previous typed
// assertion applied to un-normalized input. Atoi is ParseInt with the
// platform-int bit size, so every error class — syntax and platform-int
// overflow alike, the latter carrying a clamped extremum that must be
// discarded — reads as zero.
func consumedIntAtUse(args map[string]any, key string) int {
	n, _ := args[key].(json.Number)
	v, err := strconv.Atoi(string(n))
	if err != nil {
		return 0
	}
	return v
}

// approvedBinding carries the lexical write-dir and canonical root/write-dir
// witnesses. An empty canonical witness is bound from its lexical input.
type approvedBinding struct {
	writeDir          string
	rootCanonical     string
	writeDirCanonical string
}

// bindWriteDir resolves the lexical write-dir witness to its canonical
// boundary; an empty write-dir stays unconstrained.
func bindWriteDir(root, toolName, writeDir string) (string, error) {
	if writeDir == "" {
		return "", nil
	}
	resolved, err := pathutil.ResolveFilePathFrom(root, writeDir)
	if err != nil {
		return "", fmt.Errorf("%s: resolve write_dir %q: %w", toolName, writeDir, err)
	}
	return resolved.CanonicalPath, nil
}

func bindApproved(root, toolName string, approved approvedBinding) (approvedBinding, error) {
	var err error
	if approved.rootCanonical == "" {
		approved.rootCanonical, err = bindWorkspaceRoot(root)
		if err != nil {
			return approvedBinding{}, fmt.Errorf("%s: resolve path: %w", toolName, err)
		}
	}
	if approved.writeDirCanonical == "" {
		approved.writeDirCanonical, err = bindWriteDir(root, toolName, approved.writeDir)
		if err != nil {
			return approvedBinding{}, err
		}
	}
	return approved, nil
}

// bindCanonicalTarget resolves the target and checks it against the current
// write-dir while retaining the supplied witnesses for revalidation.
func bindCanonicalTarget(root, toolName, path string, approved approvedBinding) (boundCall, error) {
	resolved, err := pathutil.ResolveFilePathFrom(root, path)
	if err != nil {
		return boundCall{}, fmt.Errorf("%s: resolve path: %w", toolName, err)
	}
	if err := checkWriteDirTarget(root, toolName, resolved.CanonicalPath, CapabilityOptions{WriteDir: approved.writeDir}); err != nil {
		return boundCall{}, err
	}
	return boundCall{root: root, toolName: toolName, path: path, rootCanonical: approved.rootCanonical,
		displayAbs: resolved.AbsPath, canonical: resolved.CanonicalPath, writeDir: approved.writeDir,
		writeDirCanonical: approved.writeDirCanonical, leafExists: resolved.LeafExists}, nil
}

// bindWorkspaceRoot resolves the lexical Workspace root to its canonical
// form for binding; preparation binds the root together with the targets.
func bindWorkspaceRoot(root string) (string, error) {
	resolved, err := pathutil.ResolveFilePathFrom("", root)
	if err != nil {
		return "", err
	}
	return resolved.CanonicalPath, nil
}

// revalidateWorkspaceRoot refuses any change of the bound canonical
// Workspace root; a changed root binding fails without substituting a
// replacement.
func revalidateWorkspaceRoot(root, boundCanonical string) error {
	resolved, err := pathutil.ResolveFilePathFrom("", root)
	if err != nil {
		return err
	}
	if resolved.CanonicalPath != boundCanonical {
		return fmt.Errorf("approved canonical workspace root changed from %s to %s", boundCanonical, resolved.CanonicalPath)
	}
	return nil
}

func canonicalChangedError(boundCanonical, canonical string) error {
	return fmt.Errorf("approved canonical path changed from %s to %s", boundCanonical, canonical)
}

// boundCall carries the canonical bindings one prepared call revalidates
// before content access and again after snapshot creation.
type boundCall struct {
	root              string
	toolName          string
	path              string
	rootCanonical     string
	canonical         string
	writeDir          string // lexical write-dir witness (empty when unconstrained)
	writeDirCanonical string // canonical write-dir boundary bound at preparation
	displayAbs        string
	leafExists        bool
}

func (b boundCall) revalidate() error {
	if err := revalidateWorkspaceRoot(b.root, b.rootCanonical); err != nil {
		return err
	}
	resolved, err := pathutil.ResolveFilePathFrom(b.root, b.path)
	if err != nil {
		return err
	}
	if resolved.CanonicalPath != b.canonical {
		return canonicalChangedError(b.canonical, resolved.CanonicalPath)
	}
	return b.revalidateWriteDir()
}

// revalidateWriteDirWitness re-resolves the lexical write-dir witness and
// refuses any change of its bound canonical boundary — so a repointed
// write-dir symlink fails even when the target would sit inside both
// directories. An empty witness stays unconstrained.
func revalidateWriteDirWitness(root, writeDir, writeDirCanonical string) error {
	if writeDir == "" {
		return nil
	}
	resolved, err := pathutil.ResolveFilePathFrom(root, writeDir)
	if err != nil {
		return err
	}
	if resolved.CanonicalPath != writeDirCanonical {
		return fmt.Errorf("approved canonical write_dir changed from %s to %s", writeDirCanonical, resolved.CanonicalPath)
	}
	return nil
}

// revalidateWriteDir re-resolves the lexical write-dir witness and refuses
// any change of its bound canonical boundary, then re-checks the target
// against the bound boundary.
func (b boundCall) revalidateWriteDir() error {
	if err := revalidateWriteDirWitness(b.root, b.writeDir, b.writeDirCanonical); err != nil {
		return err
	}
	if b.writeDir != "" && !pathInsideDir(b.canonical, b.writeDirCanonical) {
		return fmt.Errorf("%s: path %s is outside write_dir %s", b.toolName, b.canonical, b.writeDirCanonical)
	}
	return nil
}

// prepareRead binds read_file's canonical targets and builds its execution
// closure. Preparation resolves the canonical path and observes whether the
// leaf exists; when it does not, the parent directory is bound as a second
// declared target for the authorized suggestion listing. No content read
// happens here.
func prepareRead(root string, cfg config.ToolsConfig, args map[string]any, approved approvedBinding) (*PreparedCall, error) {
	path, _ := args["path"].(string)
	approved, err := bindApproved(root, "read_file", approved)
	if err != nil {
		return nil, err
	}
	bound, err := bindCanonicalTarget(root, "read_file", path, approved)
	if err != nil {
		return nil, err
	}
	targets := []PreparedTarget{{CanonicalPath: bound.canonical}}
	parent := readTarget{}
	if !bound.leafExists {
		parent = readTarget{
			displayPath:   filepath.Dir(bound.displayAbs),
			canonicalPath: filepath.Dir(bound.canonical),
		}
		targets = append(targets, PreparedTarget{CanonicalPath: parent.canonicalPath})
	}

	execute := func(_ context.Context) (string, error) {
		if err := bound.revalidate(); err != nil {
			return "", fmt.Errorf("read_file: resolve path: %w", err)
		}
		if _, err := ensureRegularExistingTarget(bound.canonical); err != nil {
			return "", fmt.Errorf("read_file: %w", err)
		}
		f, err := safefs.OpenExisting(bound.canonical, os.O_RDONLY)
		if err != nil {
			if os.IsNotExist(err) {
				// Suggestions run only when preparation observed the leaf
				// missing and bound the parent directory; a leaf that
				// existed during preparation and disappeared before
				// opening returns plain not-found with no undeclared
				// fallback I/O (parent is unbound here).
				return suggestFromBoundDirectory(parent, bound.displayAbs), nil
			}
			return "", fmt.Errorf("read_file: %w", err)
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			return "", fmt.Errorf("read_file: stat: %w", err)
		}
		if err := ensureRegularFileInfo(bound.canonical, info); err != nil {
			return "", fmt.Errorf("read_file: %w", err)
		}

		data, err := io.ReadAll(f)
		if err != nil {
			return "", fmt.Errorf("read_file: %w", err)
		}

		// The canonical-integer json.Number arguments parse at their point
		// of use: preparation bound targets only and performs no
		// validation or defaulting here.
		offset := consumedIntAtUse(args, "offset")
		limit := consumedIntAtUse(args, "limit")

		// Binary detection.
		if isBinary(data) {
			return "", fmt.Errorf("read_file: %s appears to be a binary file", path)
		}

		result, totalLines := formatReadOutput(data, offset, limit, cfg.MaxOutputBytes, cfg.ReadLineMaxChars)

		// Footer for truncated files.
		if offset > 1 || limit < totalLines {
			lastLine := totalLines
			if remaining := totalLines - offset + 1; limit < remaining {
				lastLine = offset + limit - 1
			}
			if result != "" {
				result += "\n"
			}
			if offset == 1 && limit < totalLines {
				result += fmt.Sprintf("(Showing lines 1-%d of %d. Use offset=%d to continue.)", lastLine, totalLines, lastLine+1)
			} else if offset > 1 {
				result += fmt.Sprintf("(Showing lines %d-%d of %d.)", offset, lastLine, totalLines)
			}
		}

		return result, nil
	}

	return &PreparedCall{Targets: targets, Execute: execute}, nil
}

// PrepareReadCall is the exported preparation entry for target callers.
// The caller supplies already-normalized arguments (NormalizeReadArgs) and
// the canonical Workspace root witness bound at authorization, which
// preparation binds instead of re-resolving the lexical root; the lexical
// root remains the re-resolution input. Target reads always return the
// bounded requested content. Preparation binds the canonical file target
// plus the parent directory when the leaf is missing.
func PrepareReadCall(root, rootCanonical string, cfg config.ToolsConfig, args map[string]any) (*PreparedCall, error) {
	return prepareRead(root, cfg, args, approvedBinding{rootCanonical: rootCanonical})
}

// suggestFromBoundDirectory lists the bound parent directory through its
// no-follow descriptor — never the display path — and renders the scored
// filename suggestions. Any listing failure degrades to the plain
// not-found message.
func suggestFromBoundDirectory(parent readTarget, displayPath string) string {
	plain := fmt.Sprintf("read_file: file not found: %s", displayPath)
	if parent.canonicalPath == "" {
		return plain
	}
	dir, err := safefs.OpenDirectory(parent.canonicalPath)
	if err != nil {
		return plain
	}
	defer dir.Close()
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return plain
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	suggestions := scoreFileSuggestions(entries, filepath.Base(displayPath))
	if len(suggestions) == 0 {
		return plain
	}
	return formatFileSuggestions(displayPath, filepath.Base(parent.displayPath), suggestions)
}

// prepareWrite binds write_file's canonical target and builds its execution
// closure. Preparation performs path/stat/canonicalization and the write-dir
// check only; the precondition/preimage reads, required parent creation and
// the write itself run inside the closure as part of the single declared
// file.write target.
func prepareWrite(root string, store SnapshotStore, args map[string]any, approved approvedBinding) (*PreparedCall, error) {
	path, _ := args["path"].(string)
	content, _ := args["content"].(string)
	approved, err := bindApproved(root, "write_file", approved)
	if err != nil {
		return nil, err
	}
	bound, err := bindCanonicalTarget(root, "write_file", path, approved)
	if err != nil {
		return nil, err
	}
	execute := func(_ context.Context) (string, error) {
		_, err := writeBound(bound, store, []byte(content), false)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Wrote %s.", path), nil
	}

	return &PreparedCall{
		Targets: []PreparedTarget{{CanonicalPath: bound.canonical}},
		Execute: execute,
	}, nil
}

// PrepareWriteCall is the exported preparation entry for target callers.
// The caller supplies already-normalized arguments (NormalizeWriteArgs),
// the hard write-dir constraint, and the canonical Workspace root and
// write-dir witnesses bound at authorization, which preparation binds
// instead of re-resolving them; the lexical root and write-dir remain the
// re-resolution inputs.
func PrepareWriteCall(root, rootCanonical, writeDirCanonical string, opts CapabilityOptions, store SnapshotStore, args map[string]any) (*PreparedCall, error) {
	return prepareWrite(root, store, args, approvedBinding{rootCanonical: rootCanonical, writeDir: opts.WriteDir, writeDirCanonical: writeDirCanonical})
}

// prepareEdit binds edit_file's canonical target and builds its execution
// closure. Preparation performs path/stat/canonicalization and the write-dir
// check only; the current-content read, old_string validation and the
// mutation run inside the closure as part of the single declared file.write
// target.
func prepareEdit(root string, store SnapshotStore, args map[string]any, approved approvedBinding) (*PreparedCall, error) {
	path, _ := args["path"].(string)
	oldString, _ := args["old_string"].(string)
	newString, _ := args["new_string"].(string)
	replaceAll, _ := args["replace_all"].(bool)
	approved, err := bindApproved(root, "edit_file", approved)
	if err != nil {
		return nil, err
	}
	bound, err := bindCanonicalTarget(root, "edit_file", path, approved)
	if err != nil {
		return nil, err
	}
	execute := func(_ context.Context) (string, error) {
		var result *editResult
		_, err := mutateBound(bound, store, func() error {
			return preflightEditSnapshotTarget(bound.canonical)
		}, func() (mutationOutcome, []byte, bool, error) {
			res, produced, outcome, err := editMutation(bound.canonical, path, oldString, newString, replaceAll)
			result = res
			return outcome, produced, false, err
		})
		if err != nil {
			return "", err
		}
		return result.Result, nil
	}

	return &PreparedCall{
		Targets: []PreparedTarget{{CanonicalPath: bound.canonical}},
		Execute: execute,
	}, nil
}

// editMutation validates the edit against current file content and applies
// it, returning the mutation outcome independently from any error.
func editMutation(canonical string, displayPath, oldString, newString string, replaceAll bool) (res *editResult, produced []byte, outcome mutationOutcome, err error) {
	if oldString == "" {
		return nil, nil, mutationUntouched, fmt.Errorf("edit_file: old_string must not be empty")
	}
	if oldString == newString {
		return nil, nil, mutationUntouched, fmt.Errorf("edit_file: old_string and new_string are identical")
	}

	if _, err := ensureRegularExistingTarget(canonical); err != nil {
		return nil, nil, mutationUntouched, fmt.Errorf("edit_file: %w", err)
	}

	f, err := openExistingMutationFile(canonical, os.O_RDWR)
	if err != nil {
		return nil, nil, mutationUntouched, fmt.Errorf("edit_file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, mutationUntouched, fmt.Errorf("edit_file: stat: %w", err)
	}
	if err := ensureRegularFileInfo(canonical, info); err != nil {
		return nil, nil, mutationUntouched, fmt.Errorf("edit_file: %w", err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, nil, mutationUntouched, fmt.Errorf("edit_file: %w", err)
	}
	content := string(data)

	edit, err := ApplyEdit(content, oldString, newString, replaceAll, displayPath)
	if err != nil {
		return nil, nil, mutationUntouched, err
	}
	produced = []byte(edit.UpdatedContent)

	complete, werr := writeReplacement(f, produced)
	if !complete {
		return nil, produced, mutationUncertain, fmt.Errorf("edit_file: %w", werr)
	}
	if werr != nil {
		return nil, produced, mutationCompleted, fmt.Errorf("edit_file: %w", werr)
	}

	return &editResult{
		Result:         edit.Summary,
		UpdatedContent: edit.UpdatedContent,
		LineRanges:     edit.LineRanges,
		Count:          edit.Count,
	}, produced, mutationCompleted, nil
}

// PrepareEditCall is the exported preparation entry for target callers.
// The caller supplies already-normalized arguments (NormalizeEditArgs),
// the hard write-dir constraint, and the canonical Workspace root and
// write-dir witnesses bound at authorization, which preparation binds
// instead of re-resolving them; the lexical root and write-dir remain the
// re-resolution inputs.
func PrepareEditCall(root, rootCanonical, writeDirCanonical string, opts CapabilityOptions, store SnapshotStore, args map[string]any) (*PreparedCall, error) {
	return prepareEdit(root, store, args, approvedBinding{rootCanonical: rootCanonical, writeDir: opts.WriteDir, writeDirCanonical: writeDirCanonical})
}

// preparePatchFromArgs parses the V4A patch exactly once and resolves every
// source/destination target under the write-dir constraint. Preparation
// performs only path resolution, collision and write-dir checks; no content
// read happens here, so a patch source/destination outside write_dir is
// rejected at preparation, before authorization.
func preparePatchFromArgs(root string, args map[string]any, approved approvedBinding) (*patch, []boundCall, error) {
	input, _ := args["input"].(string)
	p, err := parsePatch(input)
	if err != nil {
		return nil, nil, err
	}
	targets, err := resolveApplyPatchTargetsFromParsed(root, p, approved)
	if err != nil {
		return nil, nil, err
	}
	return p, targets, nil
}

// PreparePatchCall is the exported preparation entry for target callers.
// The caller supplies already-normalized arguments (NormalizePatchArgs),
// the hard write-dir constraint, and the canonical Workspace root and
// write-dir witnesses bound at authorization, which replace the internal
// resolutions: the execution closure revalidates against these witnesses,
// re-resolving the lexical root and write-dir. Preparation applies the
// write-dir constraint so every patch source/destination outside write_dir
// is rejected before authorization. The returned call's execution closure
// consumes the patch parsed here — no redecode or reparse — revalidates the
// bound root, the canonical write-dir boundary and every target before
// content access, and returns the structured result carrying the
// model-visible summary and the captured preview data.
func PreparePatchCall(root, rootCanonical, writeDirCanonical string, opts CapabilityOptions, store SnapshotStore, args map[string]any) (*PreparedPatchCall, error) {
	approved, err := bindApproved(root, "apply_patch", approvedBinding{rootCanonical: rootCanonical, writeDir: opts.WriteDir, writeDirCanonical: writeDirCanonical})
	if err != nil {
		return nil, err
	}
	p, targets, err := preparePatchFromArgs(root, args, approved)
	if err != nil {
		return nil, err
	}
	displayTargets := make([]PreparedTarget, 0, len(targets))
	for _, t := range targets {
		displayTargets = append(displayTargets, PreparedTarget{CanonicalPath: t.canonical})
	}
	execute := func(_ context.Context) (PreparedPatchResult, error) {
		result, previews, err := executeParsedPatch(store, p, targets)
		return PreparedPatchResult{Result: result, Previews: previews}, err
	}
	return &PreparedPatchCall{Targets: displayTargets, Execute: execute}, nil
}
