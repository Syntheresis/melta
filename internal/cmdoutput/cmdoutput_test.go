package cmdoutput

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syntheresis/melta/internal/testassert"
)

// TestCapturePreviewKeepsFormatTailWhenSpillFails proves the preview's
// windows are measured over the kept output: when the spill cannot be created,
// Format still shows the retained tail, and Preview must render the same lines
// with only its running marker substituted.
func TestCapturePreviewKeepsFormatTailWhenSpillFails(t *testing.T) {
	home := t.TempDir()
	blocker := filepath.Join(home, "home-file")
	testassert.NoError(t, os.WriteFile(blocker, []byte("not a dir"), 0o600))
	c := NewCapture(Options{
		Directory:    filepath.Join(blocker, ".melta"),
		SpillPrefix:  "cmd_output_",
		MaxBytes:     100,
		MaxLineChars: 1,
	})
	defer c.Close()
	var full strings.Builder
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&full, "%c\n", 'a'+i%26)
	}
	_, _ = c.Stdout().Write([]byte(full.String()))

	preview, ok := c.Preview()
	testassert.Require(t, ok, "Preview reported absence while the capture was open")
	testassert.Require(t, strings.Contains(preview, "\no\n") && strings.HasSuffix(preview, "\nx"), "Preview = %q, want the retained tail Format shows after the spill failure", preview)
	format := c.Format()
	testassert.Require(t, strings.Contains(format, "could not be saved"), "Format = %q, want the could-not-save marker", format)
	testassert.Require(t, equalStrings(previewLines(preview), previewLines(format)), "Preview lines %v, want Format lines %v", previewLines(preview), previewLines(format))
}

// TestCaptureOnChangeThrottlesWithTrailingCall proves the change callback is
// never invoked by Write, runs at most once per 250 ms, and fires once more
// for a write that arrived while the previous call was running.
func TestCaptureOnChangeThrottlesWithTrailingCall(t *testing.T) {
	home := t.TempDir()
	called := make(chan time.Time, 16)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	c := NewCapture(Options{
		Directory:    filepath.Join(home, ".melta"),
		SpillPrefix:  "cmd_output_",
		MaxBytes:     0,
		MaxLineChars: 80,
		OnChange: func() {
			call := calls.Add(1)
			called <- time.Now()
			if call == 1 {
				close(started)
				<-release
			}
		},
	})
	defer c.Close()

	_, _ = c.Stdout().Write([]byte("one"))
	first := awaitChange(t, called)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the change call never started")
	}
	_, _ = c.Stdout().Write([]byte("two"))
	_, _ = c.Stdout().Write([]byte("three"))
	close(release)
	second := awaitChange(t, called)
	testassert.Require(t, second.Sub(first) >= 240*time.Millisecond, "change calls %s apart, want at most one per 250 ms", second.Sub(first))
	select {
	case <-called:
		t.Fatal("a write without a later interval produced another change call")
	case <-time.After(400 * time.Millisecond):
	}
}

// TestCaptureOnChangeStopsAtClose proves Close waits for an in-flight change
// call and no call starts after it returns.
func TestCaptureOnChangeStopsAtClose(t *testing.T) {
	t.Run("in-flight call", func(t *testing.T) {
		home := t.TempDir()
		var calls atomic.Int64
		started := make(chan struct{})
		release := make(chan struct{})
		c := NewCapture(Options{
			Directory:    filepath.Join(home, ".melta"),
			SpillPrefix:  "cmd_output_",
			MaxLineChars: 80,
			OnChange: func() {
				if calls.Add(1) == 1 {
					close(started)
					<-release
				}
			},
		})
		_, _ = c.Stdout().Write([]byte("one"))
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("the change call never started")
		}
		closed := make(chan struct{})
		go func() { c.Close(); close(closed) }()
		select {
		case <-closed:
			t.Fatal("Close returned while the change call was in flight")
		case <-time.After(100 * time.Millisecond):
		}
		close(release)
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close never returned after the change call finished")
		}
		_, _ = c.Stdout().Write([]byte("two"))
		time.Sleep(300 * time.Millisecond)
		testassert.Equal(t, calls.Load(), int64(1))
	})

	t.Run("armed call", func(t *testing.T) {
		home := t.TempDir()
		var calls atomic.Int64
		c := NewCapture(Options{
			Directory:    filepath.Join(home, ".melta"),
			SpillPrefix:  "cmd_output_",
			MaxLineChars: 80,
			OnChange:     func() { calls.Add(1) },
		})
		_, _ = c.Stdout().Write([]byte("one"))
		c.Close()
		time.Sleep(300 * time.Millisecond)
		testassert.Equal(t, calls.Load(), int64(0))
	})
}

// TestCapturePreviewMatchesFormat proves Preview renders the same lines as
// Format under and over the cap, writes no visible file, and reports absence
// after Close.
func TestCapturePreviewMatchesFormat(t *testing.T) {
	t.Run("under cap", func(t *testing.T) {
		home := t.TempDir()
		dir := filepath.Join(home, ".melta")
		c := NewCapture(Options{Directory: dir, SpillPrefix: "cmd_output_", MaxBytes: 1024, MaxLineChars: 80})
		defer c.Close()
		_, _ = c.Stdout().Write([]byte("hello\n"))
		_, _ = c.Stderr().Write([]byte("warn\n"))

		preview, ok := c.Preview()
		testassert.Require(t, ok && preview == "hello\nwarn\n", "Preview = (%q, %v), want the exact captured output", preview, ok)
		testassert.Equal(t, c.Format(), preview)
		assertNoFiles(t, dir)
	})

	t.Run("over cap within two windows", func(t *testing.T) {
		home := t.TempDir()
		dir := filepath.Join(home, ".melta")
		c := NewCapture(Options{Directory: dir, SpillPrefix: "cmd_output_", MaxBytes: 10, MaxLineChars: 5})
		defer c.Close()
		_, _ = c.Stdout().Write([]byte("abcdefg\n1234567\n"))

		before := dirEntries(t, dir)
		preview, ok := c.Preview()
		testassert.Require(t, ok, "Preview reported absence while the capture was open")
		want := "abcde... [truncated 7 chars]\n12345... [truncated 7 chars]\n[Output truncated. 16 bytes so far.]"
		testassert.Require(t, preview == want, "Preview = %q, want the bounded preview with the running marker %q", preview, want)
		testassert.Require(t, equalStrings(before, dirEntries(t, dir)), "Preview changed the spill directory from %v to %v", before, dirEntries(t, dir))
		format := c.Format()
		testassert.Require(t, strings.Contains(format, "Full output (16 bytes) saved to: "), "Format = %q, want the full-output marker after Preview wrote nothing", format)
		testassert.Require(t, equalStrings(previewLines(preview), previewLines(format)), "Preview lines %v, want Format lines %v", previewLines(preview), previewLines(format))
	})

	t.Run("over cap beyond two windows", func(t *testing.T) {
		home := t.TempDir()
		dir := filepath.Join(home, ".melta")
		c := NewCapture(Options{Directory: dir, SpillPrefix: "cmd_output_", MaxBytes: 200, MaxLineChars: 10})
		defer c.Close()
		full := numberedLines(200)
		_, _ = c.Stdout().Write([]byte(full))

		before := dirEntries(t, dir)
		preview, ok := c.Preview()
		testassert.Require(t, ok, "Preview reported absence while the capture was open")
		testassert.Require(t, strings.Contains(preview, "line 01") && strings.Contains(preview, "line 200") && !strings.Contains(preview, "line 11") && !strings.Contains(preview, "line 190"), "Preview = %q, want the first and last ten lines", preview)
		testassert.Require(t, strings.Contains(preview, fmt.Sprintf("[Output truncated. %d bytes so far.]", len(full))), "Preview = %q, want the running marker", preview)
		testassert.Require(t, equalStrings(before, dirEntries(t, dir)), "Preview changed the spill directory from %v to %v", before, dirEntries(t, dir))
		format := c.Format()
		testassert.Require(t, equalStrings(previewLines(preview), previewLines(format)), "Preview lines %v, want Format lines %v", previewLines(preview), previewLines(format))
	})

	t.Run("after close", func(t *testing.T) {
		home := t.TempDir()
		c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 8, MaxLineChars: 5})
		_, _ = c.Stdout().Write([]byte("abcdefghijkl"))
		c.Close()
		preview, ok := c.Preview()
		testassert.Require(t, preview == "" && !ok, "Preview after Close = (%q, %v), want absence", preview, ok)
	})
}

// TestCapturePreviewReadsBoundedSpillWindows proves Preview never reads more
// than its two windows from a spill file, whatever the captured total.
func TestCapturePreviewReadsBoundedSpillWindows(t *testing.T) {
	for _, lines := range []int{5000, 50000} {
		t.Run(fmt.Sprintf("%d lines", lines), func(t *testing.T) {
			home := t.TempDir()
			dir := filepath.Join(home, ".melta")
			c := NewCapture(Options{Directory: dir, SpillPrefix: "cmd_output_", MaxBytes: 64, MaxLineChars: 4})
			defer c.Close()
			full := strings.Repeat("0123456789abcdef\n", lines)
			_, _ = c.Stdout().Write([]byte(full))

			var read atomic.Int64
			original := openSpill
			openSpill = func(path string) (spillFile, error) {
				file, err := original(path)
				if err != nil {
					return nil, err
				}
				return countingSpill{spillFile: file, read: &read}, nil
			}
			defer func() { openSpill = original }()

			preview, ok := c.Preview()
			testassert.Require(t, ok, "Preview reported absence while the capture was open")
			testassert.Require(t, strings.Contains(preview, fmt.Sprintf("[Output truncated. %d bytes so far.]", len(full))), "Preview = %q, want the running marker with the full total", preview)
			window := int64(10 * (4*4 + 1))
			testassert.Require(t, read.Load() > 0 && read.Load() <= 2*window, "Preview read %d bytes from the spill, want within the two windows of %d", read.Load(), 2*window)
		})
	}
}

// awaitChange reads one change call within the test budget.
func awaitChange(t *testing.T, called <-chan time.Time) time.Time {
	t.Helper()
	select {
	case at := <-called:
		return at
	case <-time.After(5 * time.Second):
		t.Fatal("no change call arrived within the test budget")
		return time.Time{}
	}
}

// previewLines drops truncation marker lines and returns the rendered content
// lines of one preview or formatted output.
func previewLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "[Output truncated.") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	testassert.Require(t, err == nil, "ReadDir(%q): %v", dir, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

type countingSpill struct {
	spillFile
	read *atomic.Int64
}

func (c countingSpill) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.spillFile.ReadAt(p, off)
	c.read.Add(int64(n))
	return n, err
}

func TestCaptureBelowCapExactOutputNoSpill(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 1024, MaxLineChars: 80})
	defer c.Close()

	_, _ = c.Stdout().Write([]byte("hello\n"))
	_, _ = c.Stderr().Write([]byte("warn\n"))

	testassert.Equal(t, c.Format(), "hello\nwarn\n")
	assertNoFiles(t, filepath.Join(home, ".melta"))
}

func TestCaptureManyLinesAboveCapSpillsFullOutput(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 40, MaxLineChars: 80})
	defer c.Close()
	full := numberedLines(25)
	_, _ = c.Stdout().Write([]byte(full))

	result := c.Format()
	testassert.Require(t, strings.Contains(result, "line 01") && strings.Contains(result, "line 25") && !strings.Contains(result, "line 11"), "Format() = %q, want first and last lines; Format() = %q, want middle lines omitted", result, result)
	spillPath := extractSpillPath(t, result)
	testassert.Require(t, strings.HasPrefix(spillPath, filepath.Join(home, ".melta", "cmd_output_")), "spill path = %q, want cmd_output_ under home", spillPath)
	data, err := os.ReadFile(spillPath)
	testassert.Require(t, err == nil, "ReadFile(%q): %v", spillPath, err)
	if string(data) != full {
		t.Fatalf("spill content = %q, want full output", string(data))
	}
}

func TestCaptureFewLongLinesAboveCapTruncatesAndSpills(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 10, MaxLineChars: 5})
	defer c.Close()
	full := "abcdefg\n1234567\n"
	_, _ = c.Stdout().Write([]byte(full))

	result := c.Format()
	testassert.Require(t, strings.Contains(result, "abcde... [truncated 7 chars]"), "Format() = %q, want per-line truncation", result)
	data, err := os.ReadFile(extractSpillPath(t, result))
	testassert.NoError(t, err)
	if string(data) != full {
		t.Fatalf("spill = %q, want %q", string(data), full)
	}
}

func TestCaptureSharedBudgetAcrossStdoutAndStderr(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 6, MaxLineChars: 80})
	defer c.Close()
	_, _ = c.Stdout().Write([]byte("abcd"))
	_, _ = c.Stderr().Write([]byte("efgh"))

	c.mu.Lock()
	retained := c.stdout.buf.Len() + c.stderr.buf.Len()
	c.mu.Unlock()
	testassert.Equal(t, retained, 6)
	result := c.Format()
	spillPath := extractSpillPath(t, result)
	data, err := os.ReadFile(spillPath)
	testassert.NoError(t, err)
	if string(data) != "abcdefgh" {
		t.Fatalf("spill = %q, want stdout then stderr", string(data))
	}
}

func TestCaptureCrossingWriteUsesSingleBudget(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 5, MaxLineChars: 80})
	defer c.Close()
	_, _ = c.Stdout().Write([]byte("abcdef"))
	_, _ = c.Stderr().Write([]byte("ghij"))

	testassert.Equal(t, c.Len(), 10)
	data, err := os.ReadFile(extractSpillPath(t, c.Format()))
	testassert.NoError(t, err)
	if string(data) != "abcdefghij" {
		t.Fatalf("spill = %q, want full output", string(data))
	}
}

func TestCaptureDisabledCapReturnsFullOutputNoSpill(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 0, MaxLineChars: 3})
	defer c.Close()
	_, _ = c.Stdout().Write([]byte("abcdefg\n"))
	_, _ = c.Stderr().Write([]byte("1234567\n"))

	testassert.Equal(t, c.Format(), "abcdefg\n1234567\n")
	assertNoFiles(t, filepath.Join(home, ".melta"))
}

func TestCaptureRepeatedFormatReusesSpillPath(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 5, MaxLineChars: 80})
	defer c.Close()
	_, _ = c.Stdout().Write([]byte("first\nsecond\nthird\n"))

	first := c.Format()
	second := c.Format()
	testassert.Equal(t, first, second)
	testassert.Require(t, extractSpillPath(t, first) == extractSpillPath(t, second), "Format() did not reuse spill path")
}

func TestCaptureFinalSpillFailureReturnsExplicitMarker(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 4, MaxLineChars: 80})
	defer c.Close()
	_, _ = c.Stdout().Write([]byte("abcdefghi"))
	testassert.NoError(t, os.Chmod(filepath.Join(home, ".melta"), 0o500))
	defer func() { _ = os.Chmod(filepath.Join(home, ".melta"), 0o700) }()

	result := c.Format()
	testassert.Require(t, strings.Contains(result, "could not be saved:") && !strings.Contains(result, "saved to:"), "Format() = %q, want could-not-save marker; Format() = %q, should not claim saved path", result, result)
}

func TestCapturePrivateSpillFailureStaysBounded(t *testing.T) {
	homeFile := filepath.Join(t.TempDir(), "home-file")
	testassert.NoError(t, os.WriteFile(homeFile, []byte("not a dir"), 0o600))
	c := NewCapture(Options{Directory: filepath.Join(homeFile, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 4, MaxLineChars: 4})
	defer c.Close()
	_, _ = c.Stdout().Write([]byte("abcdefghijklmnopqrstuvwxyz"))

	result := c.Format()
	testassert.Require(t, strings.Contains(result, "could not be saved:") && !strings.Contains(result, "mnopqrstuvwxyz"), "Format() = %q, want could-not-save marker; Format() = %q, retained discarded tail after private spill failure", result, result)
}

func TestCaptureVeryLongLineTruncatedWithoutFullRetention(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 8, MaxLineChars: 5})
	defer c.Close()
	_, _ = c.Stdout().Write([]byte(strings.Repeat("x", 10000)))

	result := c.Format()
	testassert.Require(t, strings.Contains(result, "xxxxx... [truncated 10000 chars]") && len(result) <= 300, "Format() = %q, want long-line truncation; Format() length = %d, want bounded preview", result, len(result))
}

func TestCaptureConcurrentWritersAndFormat(t *testing.T) {
	home := t.TempDir()
	c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 64, MaxLineChars: 20})
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = c.Stdout().Write([]byte(fmt.Sprintf("out-%d-%d\n", i, j)))
				_, _ = c.Stderr().Write([]byte(fmt.Sprintf("err-%d-%d\n", i, j)))
				_ = c.Format()
			}
		}(i)
	}
	wg.Wait()
	testassert.Require(t, c.Len() != 0, "Len() = 0, want captured concurrent output")
}

func TestCaptureConcurrentFormatAndClose(t *testing.T) {
	for i := 0; i < 50; i++ {
		home := t.TempDir()
		c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 16, MaxLineChars: 20})
		_, _ = c.Stdout().Write([]byte(strings.Repeat("line\n", 1000)))

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = c.Format()
		}()
		go func() {
			defer wg.Done()
			<-start
			c.Close()
		}()
		close(start)
		wg.Wait()
	}
}

func TestCaptureCloseDeletesUnreturnedVisibleSpillAndKeepsReturned(t *testing.T) {
	t.Run("unreturned", func(t *testing.T) {
		home := t.TempDir()
		c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 4, MaxLineChars: 80})
		_, _ = c.Stdout().Write([]byte("abcdefghi"))
		c.Format()
		c.mu.Lock()
		c.visibleReturned = false
		path := c.visiblePath
		c.mu.Unlock()
		c.Close()
		{
			_, err := os.Stat(path)
			testassert.Require(t, os.IsNotExist(err), "visible spill after Close = %v, want removed", err)
		}
	})

	t.Run("returned", func(t *testing.T) {
		home := t.TempDir()
		c := NewCapture(Options{Directory: filepath.Join(home, ".melta"), SpillPrefix: "cmd_output_", MaxBytes: 4, MaxLineChars: 80})
		_, _ = c.Stdout().Write([]byte("abcdefghi"))
		path := extractSpillPath(t, c.Format())
		c.Close()
		{
			_, err := os.Stat(path)
			testassert.NoError(t, err)
		}
	})
}

func numberedLines(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "line %02d\n", i)
	}
	return b.String()
}

func extractSpillPath(t *testing.T, result string) string {
	t.Helper()
	marker := "saved to: "
	idx := strings.LastIndex(result, marker)
	testassert.Require(t, idx >= 0, "result = %q, missing spill marker", result)
	path := result[idx+len(marker):]
	if end := strings.IndexAny(path, "]\n"); end >= 0 {
		path = path[:end]
	}
	return strings.TrimSpace(path)
}

func assertNoFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return
	}
	testassert.Require(t, err == nil, "ReadDir(%q): %v", dir, err)
	testassert.Require(t, len(entries) == 0, "dir %q has files %v, want empty", dir, entries)
}
