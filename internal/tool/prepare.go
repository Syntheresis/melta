package tool

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/Syntheresis/melta/internal/config"
	"github.com/Syntheresis/melta/internal/pathutil"
	"github.com/Syntheresis/melta/internal/safefs"
)

func bindCanonicalTarget(root, toolName, path string) (boundCall, error) {
	resolved, err := pathutil.ResolveFilePathFrom(root, path)
	if err != nil {
		return boundCall{}, fmt.Errorf("%s: resolve path: %w", toolName, err)
	}
	return boundCall{root: root, toolName: toolName, path: path, displayAbs: resolved.AbsPath, canonical: resolved.CanonicalPath, leafExists: resolved.LeafExists}, nil
}

func canonicalChangedError(boundCanonical, canonical string) error {
	return fmt.Errorf("approved canonical path changed from %s to %s", boundCanonical, canonical)
}

// boundCall retains a target's lexical and canonical paths for revalidation.
type boundCall struct {
	root       string
	toolName   string
	path       string
	canonical  string
	displayAbs string
	leafExists bool
	recheck    func() error
}

func (b boundCall) revalidate() error {
	if b.recheck != nil {
		if err := b.recheck(); err != nil {
			return err
		}
	}
	resolved, err := pathutil.ResolveFilePathFrom(b.root, b.path)
	if err != nil {
		return err
	}
	if resolved.CanonicalPath != b.canonical {
		return canonicalChangedError(b.canonical, resolved.CanonicalPath)
	}
	return nil
}

// ReadFile resolves and reads a target, returning suggestions for a missing file.
func ReadFile(root string, cfg config.ToolsConfig, path string, offset, limit int) (string, error) {
	bound, err := bindCanonicalTarget(root, "read_file", path)
	if err != nil {
		return "", err
	}
	if _, err := ensureRegularExistingTarget(bound.canonical); err != nil {
		return "", fmt.Errorf("read_file: %w", err)
	}
	f, err := safefs.OpenExisting(bound.canonical, os.O_RDONLY)
	if err != nil {
		if os.IsNotExist(err) {
			return suggestFromBoundDirectory(bound.canonical, bound.displayAbs), nil
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

// suggestFromBoundDirectory lists the bound parent directory through its
// no-follow descriptor — never the display path — and renders the scored
// filename suggestions. Any listing failure degrades to the plain
// not-found message.
func suggestFromBoundDirectory(canonicalPath, displayPath string) string {
	plain := fmt.Sprintf("read_file: file not found: %s", displayPath)
	dir, err := safefs.OpenDirectory(filepath.Dir(canonicalPath))
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
	return formatFileSuggestions(displayPath, filepath.Base(filepath.Dir(displayPath)), suggestions)
}

// WriteFile resolves and writes a target with snapshot capture.
func WriteFile(root string, store SnapshotStore, path, content string) (string, MutationOutcome, error) {
	bound, err := bindCanonicalTarget(root, "write_file", path)
	if err != nil {
		return "", mutationUntouched, err
	}
	outcome, err := writeBound(bound, store, []byte(content), false)
	if err != nil {
		return "", outcome, err
	}
	return fmt.Sprintf("Wrote %s.", path), outcome, nil
}

// EditFile resolves and edits a target with snapshot capture.
func EditFile(root string, store SnapshotStore, path, oldString, newString string, replaceAll bool) (string, MutationOutcome, error) {
	bound, err := bindCanonicalTarget(root, "edit_file", path)
	if err != nil {
		return "", mutationUntouched, err
	}
	var result *editResult
	outcome, err := mutateBound(bound, store, func() error {
		return preflightEditSnapshotTarget(bound.canonical)
	}, func() (mutationOutcome, []byte, bool, error) {
		res, produced, outcome, err := editMutation(bound.canonical, path, oldString, newString, replaceAll)
		result = res
		return outcome, produced, false, err
	})
	if err != nil {
		return "", outcome, err
	}
	return result.Result, outcome, nil
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
