package harness

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Syntheresis/melta/internal/testassert"
)

// stubJobStopper satisfies the seam; Step 2 only proves a non-nil stopper is
// accepted and nil stays legal.
type stubJobStopper struct{}

func (stubJobStopper) StopJob(string, string) {}

var _ JobStopper = (*stubJobStopper)(nil)

func admitLocked(h *Harness, c *coordinator, kind memberKind, id string) (*backgroundMember, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return h.admitBackgroundMember(c, kind, id)
}

func claimLocked(h *Harness, c *coordinator, completionID string) (*backgroundMember, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return h.claimBackgroundMember(c, completionID)
}

var completionIDShape = regexp.MustCompile(`^[0-9a-f]{32}$`)

// TestDependenciesAcceptJobStopper proves the job-stop seam builds: the
// optional dependency accepts a stopper, and a harness constructed without
// one keeps the nil that StartJob will reject.
func TestDependenciesAcceptJobStopper(t *testing.T) {
	withJobs := newTestHarness(t, freshSessionStore(t), nil)
	withJobs.deps.Jobs = stubJobStopper{}
	testassert.Require(t, withJobs.deps.Jobs != nil, "Jobs dependency rejected a stopper")
	withoutJobs := newTestHarness(t, freshSessionStore(t), nil)
	testassert.Require(t, withoutJobs.deps.Jobs == nil, "Jobs dependency defaults to non-nil, want legal nil")
}

// TestAdmitBackgroundMemberInitializesGroup proves group initialization on the
// coordinatorFor construction site: the first admission creates the group, the
// member carries its reserved completion identity, an open done channel, and
// the unclaimed state; the coordinator's lifecycle starts open.
func TestAdmitBackgroundMemberInitializesGroup(t *testing.T) {
	h := newTestHarness(t, freshSessionStore(t), nil)
	c := leaseCoordinator(t, h, testSessionID)
	first, err := admitLocked(h, c, memberSession, "child-1")
	testassert.NoError(t, err)
	testassert.Require(t, c.group != nil && len(c.group.members) == 1, "first admission did not initialize the group: %+v", c.group)
	testassert.Require(t, first.kind == memberSession && first.id == "child-1", "member = %+v", first)
	testassert.Require(t, completionIDShape.MatchString(first.completionID) && !first.claimed, "completion id %q is not 32 lowercase hex; fresh member is claimed", first.completionID)
	select {
	case <-first.done:
		t.Fatalf("fresh member done channel is closed")
	default:
	}
	second, err := admitLocked(h, c, memberJob, "job-1")
	testassert.NoError(t, err)
	testassert.Require(t, len(c.group.members) == 2 && (c.group.members[first.completionID] == first && c.group.members[second.completionID] == second), "got %v, want %v; members are not keyed by completion id", len(c.group.members), 2)
}

// TestCreateSessionStartsOpenLifecycle proves the CreateSession construction
// site starts the background lifecycle open.
func TestCreateSessionStartsOpenLifecycle(t *testing.T) {
	h := newTestHarness(t, emptyStore(t), nil)
	rec, err := h.CreateSession(context.Background(), CreateSessionRequest{Workspace: "/tmp/works", Inputs: testInputs("coder")})
	testassert.NoError(t, err)
	c := leaseCoordinator(t, h, rec.Identity.SessionID)
	_, err = admitLocked(h, c, memberJob, "job-1")
	testassert.NoError(t, err)
}

// TestAdmitBackgroundMemberClosedAndArchivedRejection proves the lifecycle
// rejections: a non-open session uses Submit's archived error for both kinds,
// and a stopping or closed group rejects uniformly.
func TestAdmitBackgroundMemberClosedAndArchivedRejection(t *testing.T) {
	archived := validSessionRecord()
	now := time.Now().UTC()
	archived.State.Lifecycle = LifecycleArchived
	archived.State.ArchivedAt = &now
	h := newTestHarness(t, (&testGraph{session: archived}).storage(t), nil)
	c := leaseCoordinator(t, h, testSessionID)
	for _, kind := range []memberKind{memberSession, memberJob} {
		_, err := admitLocked(h, c, kind, "member")
		want := invalidInput("session %q is archived; admission requires an open Session", testSessionID)
		testassert.Require(t, err != nil && err.Error() == want.Error(), "%s admission on archived session = %v, want %v", kind, err, want)
		testassert.Require(t, c.group == nil, "rejected admission created a group")
	}

	h2 := newTestHarness(t, freshSessionStore(t), nil)
	c2 := leaseCoordinator(t, h2, testSessionID)
	c2.mu.Lock()
	c2.stop = &stopInterval{done: make(chan struct{})}
	c2.mu.Unlock()
	_, err := admitLocked(h2, c2, memberSession, "child")
	testassert.Require(t, err != nil && err.Error() == "harness: invalid storage request: background group is closed or stopping", "stopping group admission = %v", err)
	testassert.Require(t, c2.group == nil, "rejected admission created a group")
}

// TestAdmitBackgroundMemberAfterHarnessCancellation proves a start after
// harness cancellation is rejected with the raw context error before any
// member exists.
func TestAdmitBackgroundMemberAfterHarnessCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h, err := New(ctx, Dependencies{
		Storage: freshSessionStore(t),
		Prepare: func(context.Context, PreparationRequest) (PreparedExecution, error) {
			return PreparedExecution{}, errors.New("no preparation configured for this test")
		},
	})
	testassert.NoError(t, err)
	cancel()
	c := leaseCoordinator(t, h, testSessionID)
	c.mu.Lock()
	_, admitErr := h.admitBackgroundMember(c, memberSession, "child")
	c.mu.Unlock()
	testassert.Require(t, errors.Is(admitErr, context.Canceled) && admitErr.Error() == "context canceled", "admission after cancellation = %v, want the raw context.Canceled", admitErr)
	testassert.Require(t, c.group == nil, "rejected admission created a member")
}

// TestClaimBackgroundMember proves the first-writer claim: the first claim
// marks the member and keeps it in the map; a second claim or an unknown id
// returns not-claimed.
func TestClaimBackgroundMember(t *testing.T) {
	h := newTestHarness(t, freshSessionStore(t), nil)
	c := leaseCoordinator(t, h, testSessionID)
	{
		m, ok := claimLocked(h, c, "absent")
		testassert.Require(t, m == nil && !ok, "unknown claim = (%v, %v), want (nil, false)", m, ok)
	}
	m, err := admitLocked(h, c, memberJob, "job-1")
	testassert.NoError(t, err)
	got, ok := claimLocked(h, c, m.completionID)
	testassert.Require(t, ok && got == m, "first claim = (%v, %v), want the member", got, ok)
	testassert.Require(t, m.claimed, "claimed member is not marked claimed")
	c.mu.Lock()
	_, present := c.group.members[m.completionID]
	c.mu.Unlock()
	testassert.Require(t, present, "claimed member left the group before finishing")
	{
		got, ok := claimLocked(h, c, m.completionID)
		testassert.Require(t, got == nil && !ok, "second claim = (%v, %v), want (nil, false)", got, ok)
	}
}

// TestFinishBackgroundMember proves the terminal transition: the first finish
// removes the member and closes done; a second finish is a no-op.
func TestFinishBackgroundMember(t *testing.T) {
	h := newTestHarness(t, freshSessionStore(t), nil)
	c := leaseCoordinator(t, h, testSessionID)
	m, err := admitLocked(h, c, memberSession, "child-1")
	testassert.NoError(t, err)
	h.finishBackgroundMember(c, m)
	select {
	case <-m.done:
	default:
		t.Fatalf("finish did not close done")
	}
	c.mu.Lock()
	_, present := c.group.members[m.completionID]
	empty := len(c.group.members) == 0
	c.mu.Unlock()
	testassert.Require(t, !present, "finished member is still in the group")
	testassert.Require(t, empty, "group is not empty after finish")
	h.finishBackgroundMember(c, m) // must not panic or double-close
	c.mu.Lock()
	_, present = c.group.members[m.completionID]
	c.mu.Unlock()
	testassert.Require(t, !present, "no-op finish re-inserted the member")
}

// TestTruncateCompletionTextBounds proves the one report-body bound: a
// positive request bounds the body including the marker bytes, a zero request
// applies the 1 MiB bound, a multibyte cut retains a complete character, and
// every result is valid UTF-8.
func TestTruncateCompletionTextBounds(t *testing.T) {
	const marker = "\n[truncated]"
	for _, tc := range []struct {
		name  string
		text  string
		limit int
		want  string
	}{
		{"one byte", "text", 1, marker[:1]},
		{"below the marker", strings.Repeat("x", 20), 11, marker[:11]},
		{"marker exactly", strings.Repeat("x", 20), 12, marker},
		{"one prefix byte", strings.Repeat("x", 20), 13, "x" + marker},
		{"multibyte cut", "a" + strings.Repeat("é", 10), 14, "a" + marker},
		{"multibyte lead", strings.Repeat("é", 7), 13, marker},
		{"fits", "text", 4, "text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateCompletionText(tc.text, tc.limit)
			if got != tc.want {
				t.Fatalf("truncate(%q, %d) = %q, want %q", tc.text, tc.limit, got, tc.want)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("truncate(%q, %d) is not valid UTF-8: %q", tc.text, tc.limit, got)
			}
		})
	}
	t.Run("zero applies the 1 MiB bound", func(t *testing.T) {
		text := strings.Repeat("x", maxCompletionReportBytes+1)
		got := truncateCompletionText(text, 0)
		testassert.Equal(t, len(got), maxCompletionReportBytes)
		testassert.Require(t, strings.HasSuffix(got, marker), "zero-bound truncation lost the marker")
	})
}
