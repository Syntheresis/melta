package tool

import (
	"fmt"
	"path/filepath"
)

// resolveApplyPatchTargetsFromParsed resolves every op (and Move
// destination) of an already-parsed patch into canonical targets. This is
// pure path/stat/canonicalization plus the collision and write-dir checks;
// no content is read. The V4A patch is parsed exactly once per call by the
// caller.
func resolveApplyPatchTargetsFromParsed(root string, p *patch, approved approvedBinding) ([]boundCall, error) {
	targets := make([]boundCall, 0, len(p.ops)*2)
	for _, op := range p.ops {
		target, err := bindCanonicalTarget(root, "apply_patch", op.path, approved)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
		if op.movePath != "" {
			target, err := bindCanonicalTarget(root, "apply_patch", op.movePath, approved)
			if err != nil {
				return nil, err
			}
			targets = append(targets, target)
		}
	}
	// Reject canonical-path collisions: two different raw paths (symlink
	// aliases, ./normalization, etc.) that resolve to the same file would
	// cause a deterministic partial apply because the second op's content
	// revalidation would fail after the first op mutated the file.
	seen := map[string]bool{}
	for _, t := range targets {
		key := filepath.Clean(t.canonical)
		if seen[key] {
			return nil, fmt.Errorf("apply_patch: multiple patch entries resolve to the same file: %s", t.canonical)
		}
		seen[key] = true
	}
	return targets, nil
}
