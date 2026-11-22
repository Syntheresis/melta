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
	"unicode/utf8"
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
	// the call arms the next one. Write never calls it directly. Close stops
	// the timer, and a firing that observes the capture closed calls nothing.
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
	// tail is the last MaxBytes bytes written to either stream in write
	// order, or every byte written when MaxBytes <= 0.
	tail []byte

	visiblePath      string
	visibleReturned  bool
	lastFormattedLen int64
	cachedResult     string

	changeMu     sync.Mutex
	changeTimer  *time.Timer
	changeClosed bool
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

// Preview returns the last MaxBytes bytes written in write order, starting at
// its first complete UTF-8 rune and preceded by
// "[Output truncated. <n> bytes so far.]" when bytes were dropped from its
// front. It reads no file and stays readable after Close.
func (c *Capture) Preview() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	tail := c.tail
	if maxBytes := c.opts.MaxBytes; maxBytes > 0 && len(tail) > maxBytes {
		tail = tail[len(tail)-maxBytes:]
	}
	start := 0
	for start < len(tail) && !utf8.RuneStart(tail[start]) {
		start++
	}
	text := string(tail[start:])
	if int64(len(tail)) < c.totalBytes {
		text = fmt.Sprintf("[Output truncated. %d bytes so far.]\n%s", c.totalBytes, text)
	}
	return text
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
	c.appendTail(p)

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

// appendTail appends p to the write-order tail and compacts it back to its
// last MaxBytes bytes only once it exceeds twice that bound, so writes pay the
// copy amortized and Preview trims the slack on read.
func (c *Capture) appendTail(p []byte) {
	maxBytes := c.opts.MaxBytes
	c.tail = append(c.tail, p...)
	if maxBytes > 0 && len(c.tail) > 2*maxBytes {
		c.tail = append(c.tail[:0], c.tail[len(c.tail)-maxBytes:]...)
	}
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

// fireChange is the one timer firing: a firing that observes the closed mark
// calls nothing; otherwise it clears the timer before invoking OnChange.
func (c *Capture) fireChange() {
	c.changeMu.Lock()
	if c.changeClosed {
		c.changeMu.Unlock()
		return
	}
	c.changeTimer = nil
	c.changeMu.Unlock()
	c.opts.OnChange()
}

// closeChanges marks the timer closed and stops it, so a pending firing calls
// nothing.
func (c *Capture) closeChanges() {
	c.changeMu.Lock()
	c.changeClosed = true
	if c.changeTimer != nil {
		c.changeTimer.Stop()
		c.changeTimer = nil
	}
	c.changeMu.Unlock()
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
	_, copyErr := io.Copy(f, outputReader(snap))
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func outputReader(snap snapshot) io.Reader {
	readers := []io.Reader{bytes.NewReader(snap.stdout.buf)}
	if snap.stdout.spillPath != "" && snap.stdout.spillLen > 0 {
		readers = append(readers, limitedFileReader(snap.stdout.spillPath, snap.stdout.spillLen))
	}
	readers = append(readers, bytes.NewReader(snap.stderr.buf))
	if snap.stderr.spillPath != "" && snap.stderr.spillLen > 0 {
		readers = append(readers, limitedFileReader(snap.stderr.spillPath, snap.stderr.spillLen))
	}
	return io.MultiReader(readers...)
}

func limitedFileReader(path string, n int64) io.Reader {
	f, err := os.Open(path)
	if err != nil {
		return errorReader{err: err}
	}
	return closeAfterEOF{Reader: &io.LimitedReader{R: f, N: n}, closer: f}
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

func formatPreview(snap snapshot, marker string) string {
	p := newPreview(snap.maxLineChars, snap.maxBytes)
	_ = p.read(outputReader(snap))
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
