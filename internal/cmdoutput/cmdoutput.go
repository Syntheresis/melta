// Package cmdoutput captures command output with bounded memory and
// consistent formatting for foreground and background commands.
package cmdoutput

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Options struct {
	// Directory is the exact spill directory: every private and visible
	// spill file is created under it.
	Directory    string
	SpillPrefix  string
	MaxBytes     int
	MaxLineChars int
	// OnChange, when non-nil, is called on its own goroutine after a write,
	// at most once per 250 ms: the first write arms one timer, the timer's
	// firing clears the armed state and calls OnChange, and a write during
	// the call arms the next one. Write never calls it directly, and no call
	// starts after Close returns.
	OnChange func()
}

// changeInterval is the minimum spacing between one capture's OnChange calls.
const changeInterval = 250 * time.Millisecond

type Capture struct {
	opts Options

	mu     sync.Mutex
	fmtMu  sync.Mutex
	stdout streamState
	stderr streamState

	totalBytes int64
	firstErr   error
	closed     bool

	visiblePath      string
	visibleReturned  bool
	lastFormattedLen int64
	cachedResult     string

	changeMu     sync.Mutex
	changeTimer  *time.Timer
	changeClosed bool
	changeCalls  sync.WaitGroup
}

type streamState struct {
	name      string
	buf       bytes.Buffer
	spillPath string
	spillFile *os.File
	spillLen  int64
}

type writer struct {
	c *Capture
	s *streamState
}

func NewCapture(opts Options) *Capture {
	return &Capture{
		opts:   opts,
		stdout: streamState{name: "stdout"},
		stderr: streamState{name: "stderr"},
	}
}

func (c *Capture) Stdout() io.Writer { return writer{c: c, s: &c.stdout} }

func (c *Capture) Stderr() io.Writer { return writer{c: c, s: &c.stderr} }

func (c *Capture) Len() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalBytes
}

func (c *Capture) Format() string {
	c.fmtMu.Lock()
	defer c.fmtMu.Unlock()

	snap := c.snapshot()
	if snap.maxBytes <= 0 || snap.totalBytes <= int64(snap.maxBytes) {
		return string(snap.stdout.buf) + string(snap.stderr.buf)
	}
	if snap.cachedResult != "" && snap.lastFormattedLen == snap.totalBytes {
		return snap.cachedResult
	}

	visiblePath := snap.visiblePath
	if visiblePath == "" {
		visiblePath = c.newVisiblePath()
		c.mu.Lock()
		if c.visiblePath == "" {
			c.visiblePath = visiblePath
		} else {
			visiblePath = c.visiblePath
		}
		c.mu.Unlock()
	}

	marker := ""
	saved := false
	if snap.firstErr == nil {
		if err := writeVisible(visiblePath, snap); err != nil {
			marker = fmt.Sprintf("[Output truncated. Full output (%d bytes) could not be saved: %v]", snap.totalBytes, err)
		} else {
			saved = true
			marker = fmt.Sprintf("[Output truncated. Full output (%d bytes) saved to: %s]", snap.totalBytes, visiblePath)
		}
	} else {
		marker = fmt.Sprintf("[Output truncated. Full output (%d bytes) could not be saved: %v]", snap.totalBytes, snap.firstErr)
	}

	preview := formatPreview(snap, marker)
	c.mu.Lock()
	c.cachedResult = preview
	c.lastFormattedLen = snap.totalBytes
	if saved {
		c.visibleReturned = true
	}
	c.mu.Unlock()
	return preview
}

// Preview returns the text Format would render for the output captured so
// far, without writing the visible spill file: under the cap it is exactly
// Format's output, and an over-cap preview reads only one bounded window at
// the start and one at the end of the output and marks the omission
// "[Output truncated. <n> bytes so far.]". After Close it reports absence.
func (c *Capture) Preview() (string, bool) {
	c.fmtMu.Lock()
	defer c.fmtMu.Unlock()

	snap := c.snapshot()
	if snap.closed {
		return "", false
	}
	if snap.maxBytes <= 0 || snap.totalBytes <= int64(snap.maxBytes) {
		return string(snap.stdout.buf) + string(snap.stderr.buf), true
	}

	lineCap := newPreview(snap.maxLineChars, snap.maxBytes).lineCap
	window := int64(10 * (4*lineCap + 1))
	kept := keptBytes(snap)
	marker := fmt.Sprintf("[Output truncated. %d bytes so far.]", snap.totalBytes)
	if kept <= 2*window {
		return formatPreview(snap, marker), true
	}

	head := newPreview(snap.maxLineChars, snap.maxBytes)
	_ = head.read(outputWindow(snap, 0, window))
	tail := newPreview(snap.maxLineChars, snap.maxBytes)
	_ = tail.read(outputWindow(snap, kept-window, kept))
	lines := append([]string{}, head.first...)
	lines = append(lines, marker)
	lines = append(lines, tail.last...)
	return strings.Join(lines, "\n"), true
}

func (c *Capture) Close() {
	c.fmtMu.Lock()
	defer c.fmtMu.Unlock()

	c.mu.Lock()
	c.closed = true
	stdoutFile := c.stdout.spillFile
	stderrFile := c.stderr.spillFile
	stdoutPath := c.stdout.spillPath
	stderrPath := c.stderr.spillPath
	visiblePath := c.visiblePath
	removeVisible := visiblePath != "" && !c.visibleReturned
	c.stdout.spillFile = nil
	c.stderr.spillFile = nil
	c.mu.Unlock()

	if stdoutFile != nil {
		_ = stdoutFile.Close()
	}
	if stderrFile != nil {
		_ = stderrFile.Close()
	}
	if stdoutPath != "" {
		_ = os.Remove(stdoutPath)
	}
	if stderrPath != "" {
		_ = os.Remove(stderrPath)
	}
	if removeVisible {
		_ = os.Remove(visiblePath)
	}
	c.closeChanges()
}

func (w writer) Write(p []byte) (int, error) {
	w.c.write(w.s, p)
	return len(p), nil
}

func (c *Capture) write(s *streamState, p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if len(p) == 0 {
		return
	}

	maxBytes := c.opts.MaxBytes
	if maxBytes <= 0 {
		_, _ = s.buf.Write(p)
		c.totalBytes += int64(len(p))
		c.armChange()
		return
	}

	remaining := int64(maxBytes) - c.totalBytes
	c.totalBytes += int64(len(p))
	if remaining > 0 {
		keep := int(remaining)
		if keep > len(p) {
			keep = len(p)
		}
		_, _ = s.buf.Write(p[:keep])
		p = p[keep:]
	}
	if len(p) > 0 {
		c.writeSpill(s, p)
	}
	c.armChange()
}

// armChange arms the one change timer after a write. A write while the timer
// is already pending belongs to that interval; the firing clears the timer
// before invoking OnChange, so a write during the call arms the next one.
func (c *Capture) armChange() {
	if c.opts.OnChange == nil {
		return
	}
	c.changeMu.Lock()
	defer c.changeMu.Unlock()
	if c.changeTimer != nil || c.changeClosed {
		return
	}
	c.changeTimer = time.AfterFunc(changeInterval, c.fireChange)
}

// fireChange is the one timer firing: it clears the timer before invoking
// OnChange on the timer's own goroutine.
func (c *Capture) fireChange() {
	c.changeMu.Lock()
	if c.changeClosed {
		c.changeMu.Unlock()
		return
	}
	c.changeTimer = nil
	c.changeCalls.Add(1)
	c.changeMu.Unlock()
	defer c.changeCalls.Done()
	c.opts.OnChange()
}

// closeChanges stops the timer and waits for an in-flight change call, so no
// call starts after Close returns.
func (c *Capture) closeChanges() {
	c.changeMu.Lock()
	c.changeClosed = true
	if c.changeTimer != nil {
		c.changeTimer.Stop()
		c.changeTimer = nil
	}
	c.changeMu.Unlock()
	c.changeCalls.Wait()
}

func (c *Capture) writeSpill(s *streamState, p []byte) {
	if c.firstErr != nil {
		return
	}
	if s.spillFile == nil {
		path, file, err := c.newPrivateSpill(s.name)
		if err != nil {
			c.firstErr = err
			return
		}
		s.spillPath = path
		s.spillFile = file
	}
	n, err := s.spillFile.Write(p)
	s.spillLen += int64(n)
	if err != nil {
		c.firstErr = err
	}
}

func (c *Capture) spillDir() string {
	return c.opts.Directory
}

func (c *Capture) newPrivateSpill(stream string) (string, *os.File, error) {
	dir := c.spillDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", nil, err
	}
	prefix := c.opts.SpillPrefix
	if prefix == "" {
		prefix = "cmd_output_"
	}
	for i := 0; i < 8; i++ {
		path := filepath.Join(dir, fmt.Sprintf("%scapture_%d_%d_%s.tmp", prefix, os.Getpid(), time.Now().UnixNano(), stream))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			return path, f, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, err
		}
	}
	return "", nil, fmt.Errorf("create private spill file: too many name collisions")
}

func (c *Capture) newVisiblePath() string {
	prefix := c.opts.SpillPrefix
	if prefix == "" {
		prefix = "cmd_output_"
	}
	ts := time.Now().UnixNano()
	return filepath.Join(c.spillDir(), fmt.Sprintf("%s%d_%x.txt", prefix, ts, ts%65536))
}

type streamSnapshot struct {
	buf       []byte
	spillPath string
	spillLen  int64
}

type snapshot struct {
	maxBytes         int
	maxLineChars     int
	totalBytes       int64
	firstErr         error
	visiblePath      string
	lastFormattedLen int64
	cachedResult     string
	closed           bool
	stdout           streamSnapshot
	stderr           streamSnapshot
}

func (c *Capture) snapshot() snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return snapshot{
		maxBytes:         c.opts.MaxBytes,
		maxLineChars:     c.opts.MaxLineChars,
		totalBytes:       c.totalBytes,
		firstErr:         c.firstErr,
		visiblePath:      c.visiblePath,
		lastFormattedLen: c.lastFormattedLen,
		cachedResult:     c.cachedResult,
		closed:           c.closed,
		stdout:           c.snapshotStream(c.stdout),
		stderr:           c.snapshotStream(c.stderr),
	}
}

func (c *Capture) snapshotStream(s streamState) streamSnapshot {
	return streamSnapshot{
		buf:       append([]byte(nil), s.buf.Bytes()...),
		spillPath: s.spillPath,
		spillLen:  s.spillLen,
	}
}

func writeVisible(path string, snap snapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, outputWindow(snap, 0, keptBytes(snap)))
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

// spillFile is the read-only handle Preview and Format's full copy use for one
// private spill file.
type spillFile interface {
	io.ReaderAt
	io.Closer
}

// openSpill is the package-internal seam over opening one private spill file
// for a bounded read; tests replace it to count bytes read from spill files.
var openSpill = func(path string) (spillFile, error) { return os.Open(path) }

// outputSpan is one contiguous run of the captured output in capture order: a
// retained buffer or a private spill file of the given length.
type outputSpan struct {
	data   []byte
	path   string
	length int64
}

func outputSpans(snap snapshot) []outputSpan {
	var spans []outputSpan
	if len(snap.stdout.buf) > 0 {
		spans = append(spans, outputSpan{data: snap.stdout.buf, length: int64(len(snap.stdout.buf))})
	}
	if snap.stdout.spillPath != "" && snap.stdout.spillLen > 0 {
		spans = append(spans, outputSpan{path: snap.stdout.spillPath, length: snap.stdout.spillLen})
	}
	if len(snap.stderr.buf) > 0 {
		spans = append(spans, outputSpan{data: snap.stderr.buf, length: int64(len(snap.stderr.buf))})
	}
	if snap.stderr.spillPath != "" && snap.stderr.spillLen > 0 {
		spans = append(spans, outputSpan{path: snap.stderr.spillPath, length: snap.stderr.spillLen})
	}
	return spans
}

// outputWindow returns the [start, end) byte window of the captured output in
// capture order, reading no spill byte outside the window or beyond its
// recorded length.
func outputWindow(snap snapshot, start, end int64) io.Reader {
	var readers []io.Reader
	offset := int64(0)
	for _, span := range outputSpans(snap) {
		spanStart := offset
		spanEnd := offset + span.length
		offset = spanEnd
		if end <= spanStart {
			break
		}
		if start >= spanEnd {
			continue
		}
		from := max(start, spanStart) - spanStart
		to := min(end, spanEnd) - spanStart
		if span.path != "" {
			readers = append(readers, spillWindow(span.path, from, to-from))
			continue
		}
		readers = append(readers, bytes.NewReader(span.data[from:to]))
	}
	return io.MultiReader(readers...)
}

func spillWindow(path string, offset, length int64) io.Reader {
	f, err := openSpill(path)
	if err != nil {
		return errorReader{err: err}
	}
	return closeAfterEOF{Reader: io.NewSectionReader(f, offset, length), closer: f}
}

type errorReader struct {
	err error
}

func (r errorReader) Read(_ []byte) (int, error) { return 0, r.err }

type closeAfterEOF struct {
	io.Reader
	closer io.Closer
}

func (r closeAfterEOF) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		_ = r.closer.Close()
	}
	return n, err
}

func keptBytes(snap snapshot) int64 {
	return int64(len(snap.stdout.buf)) + snap.stdout.spillLen + int64(len(snap.stderr.buf)) + snap.stderr.spillLen
}

func formatPreview(snap snapshot, marker string) string {
	p := newPreview(snap.maxLineChars, snap.maxBytes)
	_ = p.read(outputWindow(snap, 0, keptBytes(snap)))
	return p.format(marker)
}

type preview struct {
	lineCap int
	first   []string
	last    []string
	lines   int

	current       strings.Builder
	currentRunes  int
	currentHasAny bool
}

func newPreview(maxLineChars, maxBytes int) *preview {
	lineCap := maxLineChars
	if lineCap <= 0 && maxBytes > 0 {
		lineCap = maxBytes
	}
	return &preview{lineCap: lineCap}
}

func (p *preview) read(r io.Reader) error {
	br := bufio.NewReader(r)
	for {
		ch, _, err := br.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if ch == '\n' {
			p.finishLine()
			continue
		}
		p.currentHasAny = true
		p.currentRunes++
		if p.lineCap <= 0 || p.currentRunes <= p.lineCap {
			_, _ = p.current.WriteRune(ch)
		}
	}
	if p.currentHasAny {
		p.finishLine()
	}
	return nil
}

func (p *preview) finishLine() {
	line := p.current.String()
	if p.lineCap > 0 && p.currentRunes > p.lineCap {
		line += fmt.Sprintf("... [truncated %d chars]", p.currentRunes)
	}
	p.lines++
	if len(p.first) < 10 {
		p.first = append(p.first, line)
	}
	p.last = append(p.last, line)
	if len(p.last) > 10 {
		copy(p.last, p.last[1:])
		p.last = p.last[:10]
	}
	p.current.Reset()
	p.currentRunes = 0
	p.currentHasAny = false
}

func (p *preview) format(marker string) string {
	if p.lines <= 20 {
		lines := append([]string{}, p.first...)
		if p.lines > 10 {
			lines = append(lines, p.last[len(p.last)-(p.lines-10):]...)
		}
		lines = append(lines, marker)
		return strings.Join(lines, "\n")
	}
	lines := append([]string{}, p.first...)
	lines = append(lines, marker)
	lines = append(lines, p.last...)
	return strings.Join(lines, "\n")
}
