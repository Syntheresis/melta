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

// TestCaptureOnChangeStopsAtClose proves Close before the interval elapses
// suppresses the armed call.
func TestCaptureOnChangeStopsAtClose(t *testing.T) {
	var calls atomic.Int64
	c := NewCapture(Options{
		Directory:    filepath.Join(t.TempDir(), ".melta"),
		SpillPrefix:  "cmd_output_",
		MaxLineChars: 80,
		OnChange:     func() { calls.Add(1) },
	})
	_, _ = c.Stdout().Write([]byte("one"))
	c.Close()
	time.Sleep(300 * time.Millisecond)
	testassert.Equal(t, calls.Load(), int64(0))
}

// previewWrite is one TestCapturePreview row's stream-tagged write.
type previewWrite struct {
	stderr bool
	text   string
}

// TestCapturePreview proves the live preview is the bounded write-order tail,
// read only from captured memory.
func TestCapturePreview(t *testing.T) {
	cases := []struct {
		name     string
		maxBytes int
		writes   []previewWrite
		close    bool
		want     string
		wantOK   bool
	}{
		{
			name:     "before any write",
			maxBytes: 16,
			wantOK:   true,
		},
		{
			name:     "under bound in write order",
			maxBytes: 64,
			writes: []previewWrite{
				{text: "out one\n"},
				{stderr: true, text: "err one\n"},
				{text: "out two\n"},
			},
			want:   "out one\nerr one\nout two\n",
			wantOK: true,
		},
		{
			name:     "disabled bound keeps every byte in write order",
			maxBytes: 0,
			writes: []previewWrite{
				{text: "first"},
				{stderr: true, text: "second"},
			},
			want:   "firstsecond",
			wantOK: true,
		},
		{
			name:     "past bound keeps the last bytes",
			maxBytes: 8,
			writes: []previewWrite{
				{text: "abcdef"},
				{stderr: true, text: "ghijkl"},
			},
			want:   "[Output truncated. 12 bytes so far.]\nefghijkl",
			wantOK: true,
		},
		{
			name:     "many small writes keep the last bounded bytes",
			maxBytes: 8,
			writes: []previewWrite{
				{text: "a"}, {text: "b"}, {text: "c"}, {text: "d"}, {text: "e"}, {text: "f"},
				{text: "g"}, {text: "h"}, {text: "i"}, {text: "j"}, {text: "k"}, {text: "l"},
				{text: "m"}, {text: "n"}, {text: "o"}, {text: "p"}, {text: "q"}, {text: "r"},
			},
			want:   "[Output truncated. 18 bytes so far.]\nklmnopqr",
			wantOK: true,
		},
		{
			name:     "cut inside a rune starts at the next rune",
			maxBytes: 3,
			writes: []previewWrite{
				{text: "éé"},
			},
			want:   "[Output truncated. 4 bytes so far.]\né",
			wantOK: true,
		},
		{
			name:     "after close",
			maxBytes: 8,
			writes: []previewWrite{
				{text: "abcdefghijkl"},
			},
			close:  true,
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), ".melta")
			c := NewCapture(Options{Directory: dir, SpillPrefix: "cmd_output_", MaxBytes: tc.maxBytes, MaxLineChars: 80})
			for _, write := range tc.writes {
				target := c.Stdout()
				if write.stderr {
					target = c.Stderr()
				}
				_, _ = target.Write([]byte(write.text))
			}
			if tc.close {
				c.Close()
			} else {
				defer c.Close()
			}

			before := dirEntries(t, dir)
			preview, ok := c.Preview()
			testassert.Require(t, ok == tc.wantOK && preview == tc.want, "Preview = (%q, %v), want (%q, %v)", preview, ok, tc.want, tc.wantOK)
			testassert.Require(t, equalStrings(before, dirEntries(t, dir)), "Preview changed the spill directory from %v to %v", before, dirEntries(t, dir))
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
