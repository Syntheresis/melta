package patch

// Presentation metadata shapes. They mirror the public edit-preview wire
// members and kind strings without importing the Runtime's private preview
// package: the Host renderers decode this metadata generically.

const (
	kindContext = "context"
	kindRemove  = "remove"
	kindAdd     = "add"
)

// preview is the per-file diff body.
type preview struct {
	Hunks []previewHunk `json:"hunks"`
}

type previewHunk struct {
	Rows []previewRow `json:"rows"`
}

type previewRow struct {
	Kind    string `json:"kind"`
	OldLine int    `json:"oldLine,omitempty"`
	NewLine int    `json:"newLine,omitempty"`
	Text    string `json:"text"`
}

// fileEntry is one file in the edit_preview_files list: the display path, its
// A/M/D tag and the captured diff body.
type fileEntry struct {
	Path    string  `json:"path"`
	Op      string  `json:"op"`
	Preview preview `json:"preview"`
}

// appliedFilePreview is the per-file data the engine captures so display
// metadata can be built without post-write disk reads. Pre is the pre-mutation
// content (nil for Add); Post is the post-mutation content (nil for Delete);
// Hunks is the list of applied hunks. A Move produces two entries:
// destination M and source D.
type appliedFilePreview struct {
	Op    string
	Path  string
	Pre   []string
	Post  []string
	Hunks []appliedHunkPreview
}

// appliedHunkPreview carries the matched pattern and new content of one
// applied hunk. StartLine is the 1-based line in Pre where the pattern was
// matched; Lines is the classified hunk in original order.
type appliedHunkPreview struct {
	StartLine int
	Old       []string
	New       []string
	Lines     []hunkLine
}

// previewMetadata converts captured per-file previews into the
// edit_preview_files metadata map (nil when there are no previews).
func previewMetadata(previews []appliedFilePreview) map[string]any {
	if len(previews) == 0 {
		return nil
	}
	files := make([]fileEntry, 0, len(previews))
	for _, p := range previews {
		files = append(files, fileEntry{Path: p.Path, Op: p.Op, Preview: buildPreviewFromCaptured(p)})
	}
	return map[string]any{"edit_preview_files": files}
}

// buildPreviewFromCaptured derives diff rows from the engine's captured
// pre/post/hunks data. Add produces a synthetic "all add" hunk from the post
// content; Update / Move destination builds hunks from the captured
// classified lines; Delete / Move source produces no hunks.
func buildPreviewFromCaptured(p appliedFilePreview) preview {
	if len(p.Hunks) == 0 {
		if p.Op != "A" {
			return preview{}
		}
		if len(p.Post) == 0 {
			return preview{}
		}
		rows := make([]previewRow, 0, len(p.Post))
		for i, l := range p.Post {
			rows = append(rows, previewRow{Kind: kindAdd, NewLine: i + 1, Text: l})
		}
		return preview{Hunks: []previewHunk{{Rows: rows}}}
	}
	hunks := make([]previewHunk, 0, len(p.Hunks))
	for _, h := range p.Hunks {
		rows := make([]previewRow, 0, len(h.Lines))
		oldLine := h.StartLine
		newLine := h.StartLine
		for _, l := range h.Lines {
			switch l.Kind {
			case lineContext:
				rows = append(rows, previewRow{Kind: kindContext, OldLine: oldLine, NewLine: newLine, Text: l.Text})
				oldLine++
				newLine++
			case lineRemove:
				rows = append(rows, previewRow{Kind: kindRemove, OldLine: oldLine, Text: l.Text})
				oldLine++
			case lineAdd:
				rows = append(rows, previewRow{Kind: kindAdd, NewLine: newLine, Text: l.Text})
				newLine++
			}
		}
		hunks = append(hunks, previewHunk{Rows: rows})
	}
	return preview{Hunks: hunks}
}
