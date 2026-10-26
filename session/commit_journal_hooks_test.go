// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// Tests for the P2/P4 test pause points (commit_journal_hooks.go).
//
// "The send" and "the reply" of a component call live in config/schema
// (compMgr.dispatch), which this package cannot drive. Where a test needs
// them it models them with a counter placed exactly where dispatch places the
// real call: after BeforeDispatch returns (the send) and before AfterDispatch
// (the reply). The real dispatch loop, including the send-window re-record, is
// covered by config/schema's own tests (TestSendWindow*).

package session

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/danos/config/schema"
)

// ---- rig ---------------------------------------------------------------------

type hookClock struct {
	mu     sync.Mutex
	t      time.Time
	sleeps int
	// onSleep lets a test advance another clock together with this one.
	onSleep func(time.Duration)
}

func newHookClock() *hookClock { return &hookClock{t: time.Unix(1000, 0)} }
func (c *hookClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *hookClock) sleep(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.sleeps++
	f := c.onSleep
	c.mu.Unlock()
	if f != nil {
		f(d)
	}
}
func (c *hookClock) slept() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sleeps
}

type countIO struct {
	osHookIO
	stat, read, open int32
}

func (c *countIO) Stat(p string) (os.FileInfo, error) {
	atomic.AddInt32(&c.stat, 1)
	return c.osHookIO.Stat(p)
}
func (c *countIO) ReadFile(p string) ([]byte, error) {
	atomic.AddInt32(&c.read, 1)
	return c.osHookIO.ReadFile(p)
}
func (c *countIO) OpenFifoNonblock(p string) (io.WriteCloser, error) {
	atomic.AddInt32(&c.open, 1)
	return c.osHookIO.OpenFifoNonblock(p)
}
func (c *countIO) total() int32 {
	return atomic.LoadInt32(&c.stat) + atomic.LoadInt32(&c.read) + atomic.LoadInt32(&c.open)
}

type hookRig struct {
	t     *testing.T
	r     *JournalRecorder
	j     *Journal
	path  string // journal file
	lg    *testLog
	dir   string // control directory
	fifo  string
	clock *hookClock
	cio   *countIO
}

func newHookRig(t *testing.T) *hookRig {
	t.Helper()
	r, j, _, path, _, lg, _ := newRecorder(t)
	g := &hookRig{t: t, r: r, j: j, path: path, lg: lg, dir: t.TempDir(),
		clock: newHookClock(), cio: &countIO{}}
	g.fifo = filepath.Join(g.dir, "p.fifo")
	if err := syscall.Mkfifo(g.fifo, 0600); err != nil {
		t.Fatal(err)
	}
	r.hooks = newTestHooks(g.dir, g.cio, lg.logf)
	return g
}

// fake makes the hooks use the injected clock instead of real time.
func (g *hookRig) fake() {
	g.r.hooks.now, g.r.hooks.sleep = g.clock.now, g.clock.sleep
}

func (g *hookRig) control(seam hookSeam, content string) string {
	g.t.Helper()
	p := filepath.Join(g.dir, "pause-"+strings.ToLower(string(seam)))
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		g.t.Fatal(err)
	}
	return p
}

func (g *hookRig) disk() JournalState {
	g.t.Helper()
	var s JournalState
	if err := json.Unmarshal([]byte(readFileT(g.t, g.path)), &s); err != nil {
		g.t.Fatal(err)
	}
	return s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func countLines(s, sub string) int { return strings.Count(s, sub) }

// ---- inert by default ----------------------------------------------------------

func TestHooksAreInertWithoutTheSwitch(t *testing.T) {
	cio := &countIO{}
	old := hookFS
	hookFS = cio
	t.Cleanup(func() { hookFS = old })

	for _, v := range []string{"", "0", "true", "yes", "2", " 1"} {
		t.Setenv(hooksEnvVar, v)
		r, _, _, _, _, _, _ := newRecorder(t)
		if r.hooks != nil {
			t.Fatalf("%s=%q must leave the hooks off", hooksEnvVar, v)
		}
		if _, err := r.BeforeDispatch("alpha", schema.KindForward, 1); err != nil {
			t.Fatal(err)
		}
		r.AfterDispatch("alpha", schema.KindForward, 1, nil)
	}
	t.Logf("switch off: hook stat=%d read=%d open=%d (total %d) over %d Before+After pairs",
		cio.stat, cio.read, cio.open, cio.total(), 6)
	if cio.total() != 0 {
		t.Fatalf("hook system calls with the switch off: stat=%d read=%d open=%d",
			cio.stat, cio.read, cio.open)
	}

	// Positive control: the counter does see calls once the switch is on, so the
	// zero above is not a broken counter. No control file exists, so the cost
	// is one stat per pause point and nothing else.
	t.Setenv(hooksEnvVar, "1")
	r, _, _, _, _, _, _ := newRecorder(t)
	if r.hooks == nil {
		t.Fatal("switch on must build the hooks")
	}
	if _, err := r.BeforeDispatch("alpha", schema.KindForward, 1); err != nil {
		t.Fatal(err)
	}
	r.AfterDispatch("alpha", schema.KindForward, 1, nil)
	t.Logf("switch on, no control file: stat=%d read=%d open=%d", cio.stat, cio.read, cio.open)
	if cio.stat != 2 || cio.read != 0 || cio.open != 0 {
		t.Fatalf("switch on without a control file: stat=%d read=%d open=%d, want 2/0/0",
			cio.stat, cio.read, cio.open)
	}
}

func TestNilHooksPauseIsANoOp(t *testing.T) {
	var h *testHooks
	h.pause(hookP2, "alpha", schema.KindForward, 1, nil) // must not panic
}

// ---- P2 and P4 block and release -------------------------------------------------

func release(t *testing.T, fifo string) (stop func()) {
	t.Helper()
	f, err := os.OpenFile(fifo, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	return func() { f.Close() }
}

func TestP2BlocksAfterTheDurableRecordBeforeTheSendAndFifoReleasesIt(t *testing.T) {
	g := newHookRig(t)
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\n")

	var sent, done int32
	go func() {
		if _, err := g.r.BeforeDispatch("alpha", schema.KindForward, 7); err != nil {
			t.Error(err)
		}
		atomic.AddInt32(&sent, 1) // dispatch makes the Set call right after this returns
		atomic.AddInt32(&done, 1)
	}()

	waitFor(t, "the seam log line", func() bool {
		return strings.Contains(g.lg.joined(), "danos-test-hook: reached seam=P2")
	})
	time.Sleep(150 * time.Millisecond) // it must stay stopped, not merely be slow
	if atomic.LoadInt32(&done) != 0 || atomic.LoadInt32(&sent) != 0 {
		t.Fatal("BeforeDispatch returned although nobody released the FIFO: the send would have happened")
	}
	d := g.disk().Dispatch["alpha"]
	if !d.MayHaveBeenSent || d.Settled || d.Seq != 7 || d.Kind != schema.KindForward {
		t.Fatalf("on disk while paused: %+v", d)
	}
	line := g.lg.joined()
	t.Logf("while paused: sent=%d on-disk may_have_been_sent=%v settled=%v seq=%d\nlog: %s",
		atomic.LoadInt32(&sent), d.MayHaveBeenSent, d.Settled, d.Seq, line)
	for _, want := range []string{"pid=", "component=alpha", "kind=forward", "seq=7", "fifo=" + g.fifo} {
		if !strings.Contains(line, want) {
			t.Errorf("seam log line lacks %q: %s", want, line)
		}
	}

	stop := release(t, g.fifo)
	defer stop()
	waitFor(t, "release", func() bool { return atomic.LoadInt32(&done) == 1 })
	if atomic.LoadInt32(&sent) != 1 {
		t.Fatal("the send did not follow the release")
	}
	if !strings.Contains(g.lg.joined(), "danos-test-hook: released seam=P2") {
		t.Errorf("no release line: %s", g.lg.joined())
	}
	t.Logf("after cat fifo: sent=%d", atomic.LoadInt32(&sent))
}

func TestP4BlocksAfterTheReplyBeforeSettledAndFifoReleasesIt(t *testing.T) {
	g := newHookRig(t)
	g.control(hookP4, "component=alpha\nfifo="+g.fifo+"\n")

	var replied, done int32
	go func() {
		if _, err := g.r.BeforeDispatch("alpha", schema.KindForward, 9); err != nil {
			t.Error(err)
		}
		atomic.AddInt32(&replied, 1) // SetConfigForModel has returned: the component answered
		g.r.AfterDispatch("alpha", schema.KindForward, 9, nil)
		atomic.AddInt32(&done, 1)
	}()

	waitFor(t, "the seam log line", func() bool {
		return strings.Contains(g.lg.joined(), "danos-test-hook: reached seam=P4")
	})
	time.Sleep(150 * time.Millisecond)
	if atomic.LoadInt32(&done) != 0 {
		t.Fatal("AfterDispatch returned although nobody released the FIFO")
	}
	d := g.disk().Dispatch["alpha"]
	if atomic.LoadInt32(&replied) != 1 || !d.MayHaveBeenSent || d.Settled {
		t.Fatalf("while paused: replied=%d record %+v", replied, d)
	}
	t.Logf("while paused: replied=%d on-disk may_have_been_sent=%v settled=%v\nlog: %s",
		atomic.LoadInt32(&replied), d.MayHaveBeenSent, d.Settled, g.lg.joined())
	if !strings.Contains(g.lg.joined(), "callErr=<nil>") {
		t.Errorf("P4 line should carry the call error: %s", g.lg.joined())
	}

	stop := release(t, g.fifo)
	defer stop()
	waitFor(t, "release", func() bool { return atomic.LoadInt32(&done) == 1 })
	d = g.disk().Dispatch["alpha"]
	if !d.Settled || !d.MayHaveBeenSent {
		t.Fatalf("after release: %+v, want settled", d)
	}
	t.Logf("after cat fifo: on-disk settled=%v", d.Settled)
}

// ---- timeout, clamp, one-shot --------------------------------------------------------

func elapsed(g *hookRig, start time.Time) time.Duration { return g.clock.now().Sub(start) }

func TestNoReaderContinuesAfterTheTimeoutWithAWarning(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=3s\n")
	start := g.clock.now()
	if _, err := g.r.BeforeDispatch("alpha", schema.KindForward, 1); err != nil {
		t.Fatal(err)
	}
	if e := elapsed(g, start); e < 3*time.Second || e > 3*time.Second+hookPollInterval {
		t.Fatalf("simulated pause %s, want about 3s", e)
	}
	log := g.lg.joined()
	t.Logf("sleeps=%d simulated=%s\nlog: %s", g.clock.slept(), elapsed(g, start), log)
	if !strings.Contains(log, "danos-test-hook: warning: seam=P2 timed out after 3s with no reader; continuing") {
		t.Errorf("no timeout warning: %s", log)
	}
	if g.clock.slept() != 60 {
		t.Errorf("polled %d times, want 60 (3s / 50ms)", g.clock.slept())
	}
}

func TestTimeoutIsClampedToFifteenSeconds(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.control(hookP4, "component=alpha\nfifo="+g.fifo+"\ntimeout=99s\n")
	start := g.clock.now()
	g.r.AfterDispatch("alpha", schema.KindForward, 1, nil)
	e := elapsed(g, start)
	log := g.lg.joined()
	t.Logf("requested 99s, simulated pause %s\nlog: %s", e, log)
	if e < 15*time.Second || e > 15*time.Second+hookPollInterval {
		t.Fatalf("pause %s, want 15s", e)
	}
	if !strings.Contains(log, "timeout=15s") || !strings.Contains(log, "clamped") ||
		!strings.Contains(log, "timed out after 15s") {
		t.Errorf("log: %s", log)
	}
}

func TestDefaultTimeoutIsTenSeconds(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\n")
	start := g.clock.now()
	g.r.BeforeDispatch("alpha", schema.KindForward, 1)
	if e := elapsed(g, start); e < 10*time.Second || e > 10*time.Second+hookPollInterval {
		t.Fatalf("pause %s, want 10s", e)
	}
}

func TestControlFileFiresOnceAndARecreatedFileRearms(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	ctl := g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	g.r.BeforeDispatch("alpha", schema.KindForward, 1)
	first := g.clock.slept()
	if first == 0 || countLines(g.lg.joined(), "reached seam=P2") != 1 {
		t.Fatalf("first call did not fire: sleeps=%d log=%s", first, g.lg.joined())
	}
	g.r.BeforeDispatch("alpha", schema.KindForward, 1) // the schema layer's re-record
	if g.clock.slept() != first || countLines(g.lg.joined(), "reached seam=P2") != 1 {
		t.Fatalf("the same control file fired twice: sleeps %d->%d, log=%s",
			first, g.clock.slept(), g.lg.joined())
	}
	t.Logf("second call with the same file: sleeps stayed %d, reached lines=%d",
		g.clock.slept(), countLines(g.lg.joined(), "reached seam=P2"))

	if err := os.Remove(ctl); err != nil {
		t.Fatal(err)
	}
	ctl = g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	if err := os.Chtimes(ctl, time.Unix(1, 0), time.Unix(2000000000, 0)); err != nil {
		t.Fatal(err)
	}
	g.r.BeforeDispatch("alpha", schema.KindForward, 2)
	if countLines(g.lg.joined(), "reached seam=P2") != 2 {
		t.Fatalf("a re-created control file did not re-arm: %s", g.lg.joined())
	}
	t.Logf("after re-creating the file: reached lines=%d", countLines(g.lg.joined(), "reached seam=P2"))
}

// ---- nothing that is wrong may block or panic ------------------------------------------

func TestBadOrNonMatchingControlNeverBlocks(t *testing.T) {
	regular := func(g *hookRig) string {
		p := filepath.Join(g.dir, "regular")
		os.WriteFile(p, []byte("x"), 0644)
		return p
	}
	cases := []struct {
		name     string
		content  func(g *hookRig) string
		wantWarn string // substring of a "danos-test-hook: warning:" line, or "" for silent
	}{
		{"malformed line", func(g *hookRig) string { return "component=alpha\nthis is not key=value\x00\ngarbage" }, "malformed line"},
		{"unknown kind", func(g *hookRig) string { return "component=alpha\nkind=sideways\n" }, "unknown kind"},
		{"bad timeout", func(g *hookRig) string { return "component=alpha\nfifo=" + g.fifo + "\ntimeout=banana\n" }, "bad timeout"},
		{"zero timeout", func(g *hookRig) string { return "component=alpha\nfifo=" + g.fifo + "\ntimeout=0s\n" }, "bad timeout"},
		{"no component", func(g *hookRig) string { return "fifo=" + g.fifo + "\n" }, ""},
		{"empty file", func(g *hookRig) string { return "" }, ""},
		{"other component", func(g *hookRig) string { return "component=beta\nfifo=" + g.fifo + "\n" }, ""},
		{"other kind", func(g *hookRig) string { return "component=alpha\nkind=compensation\nfifo=" + g.fifo + "\n" }, ""},
		{"fifo does not exist", func(g *hookRig) string { return "component=alpha\nfifo=" + g.dir + "/nope\n" }, "cannot stat fifo"},
		{"fifo is a regular file", func(g *hookRig) string { return "component=alpha\nfifo=" + regular(g) + "\n" }, "is not a FIFO"},
		{"comments and blanks only", func(g *hookRig) string { return "# nothing\n\n   \n" }, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newHookRig(t)
			g.fake()
			g.control(hookP2, c.content(g))
			for i := 0; i < 2; i++ { // twice: a warning must not repeat
				if _, err := g.r.BeforeDispatch("alpha", schema.KindForward, 1); err != nil {
					t.Fatal(err)
				}
			}
			if g.clock.slept() != 0 {
				t.Fatalf("blocked: %d polls", g.clock.slept())
			}
			log := g.lg.joined()
			if strings.Contains(log, "reached seam") {
				t.Fatalf("must not pause: %s", log)
			}
			warns := countLines(log, "danos-test-hook: warning:")
			if c.wantWarn == "" && warns != 0 {
				t.Fatalf("unexpected warning: %s", log)
			}
			if c.wantWarn != "" && (warns != 1 || !strings.Contains(log, c.wantWarn)) {
				t.Fatalf("want exactly one warning containing %q, got: %s", c.wantWarn, log)
			}
			t.Logf("sleeps=%d warnings=%d %s", g.clock.slept(), warns, strings.ReplaceAll(log, "\n", " | "))
		})
	}
}

func TestControlFileThatIsADirectoryOrUnreadableNeverBlocks(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	if err := os.Mkdir(filepath.Join(g.dir, "pause-p2"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := g.r.BeforeDispatch("alpha", schema.KindForward, 1); err != nil {
		t.Fatal(err)
	}
	if g.clock.slept() != 0 || countLines(g.lg.joined(), "danos-test-hook: warning: cannot read") != 1 {
		t.Fatalf("sleeps=%d log=%s", g.clock.slept(), g.lg.joined())
	}
}

// ---- the hook does not change what the journal records ----------------------------------

func TestPausedRunLeavesTheSameFinalRecordAsAnUnhookedRun(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	g.control(hookP4, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	if _, err := g.r.BeforeDispatch("alpha", schema.KindForward, 5); err != nil {
		t.Fatal(err)
	}
	g.r.AfterDispatch("alpha", schema.KindForward, 5, nil)
	hooked := g.disk().Dispatch["alpha"]

	plain, _, _, ppath, _, _, _ := newRecorder(t)
	if _, err := plain.BeforeDispatch("alpha", schema.KindForward, 5); err != nil {
		t.Fatal(err)
	}
	plain.AfterDispatch("alpha", schema.KindForward, 5, nil)
	var ps JournalState
	if err := json.Unmarshal([]byte(readFileT(t, ppath)), &ps); err != nil {
		t.Fatal(err)
	}
	unhooked := ps.Dispatch["alpha"]

	for _, d := range []*JournalDispatch{&hooked, &unhooked} {
		d.TRecordMonoUs, d.Identity.ReadAtMonoUs = 0, 0 // clock values differ between the rigs
	}
	if hooked != unhooked {
		t.Fatalf("hooked %+v != unhooked %+v", hooked, unhooked)
	}
	if countLines(g.lg.joined(), "reached seam") != 2 {
		t.Fatalf("both seams should have fired: %s", g.lg.joined())
	}
	t.Logf("final record identical with and without the hooks: %+v", hooked)
}

// P2 held longer than the 2 s send window. The pause is inside BeforeDispatch,
// before it returns t, so the t it returns is older than the window and
// dispatch (component_compensation.go, the attempt loop) re-records; the
// second BeforeDispatch must not pause again and must return a fresh t with the
// same seq. The loop below repeats dispatch's attempt loop only to drive the
// recorder the way dispatch does; the real loop is exercised by config/schema's
// TestSendWindowReRecordSucceedsOnSecondAttempt.
func TestP2PauseBeyondTheSendWindowIsAReRecordNotALateSend(t *testing.T) {
	g := newHookRig(t)
	var nowUs int64 = 5000000
	var mu sync.Mutex
	mono := func() int64 { mu.Lock(); defer mu.Unlock(); nowUs += 10; return nowUs }
	g.r.mono = mono
	g.fake()
	g.clock.onSleep = func(d time.Duration) { mu.Lock(); nowUs += d.Microseconds(); mu.Unlock() }
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=3s\n")

	window := schema.DefaultCompensationPolicy().SendWindow.Microseconds()
	attempts, recorded := 0, false
	var tFinal int64
	for ; attempts < schema.DefaultCompensationPolicy().MaxRecordAttempts; attempts++ {
		tr, err := g.r.BeforeDispatch("alpha", schema.KindForward, 11)
		if err != nil {
			t.Fatal(err)
		}
		age := mono() - tr
		t.Logf("attempt %d: record age after BeforeDispatch = %dus (window %dus)", attempts+1, age, window)
		if age <= window {
			recorded, tFinal = true, tr
			break
		}
	}
	if !recorded || attempts != 1 {
		t.Fatalf("want the second attempt to land inside the window, got recorded=%v attempts(index)=%d", recorded, attempts)
	}
	if countLines(g.lg.joined(), "reached seam=P2") != 1 {
		t.Fatalf("P2 paused more than once: %s", g.lg.joined())
	}
	d := g.disk().Dispatch["alpha"]
	if d.Seq != 11 || d.TRecordMonoUs != tFinal || !d.MayHaveBeenSent || d.Settled {
		t.Fatalf("final record %+v, want seq 11, t=%d, unsettled", d, tFinal)
	}
}

func TestSeamsReturnPromptlyWhenNothingIsConfigured(t *testing.T) {
	g := newHookRig(t) // switch effectively on (hooks built), no control files
	start := time.Now()
	for i := 0; i < 50; i++ {
		g.r.BeforeDispatch("alpha", schema.KindForward, uint64(i))
		g.r.AfterDispatch("alpha", schema.KindForward, uint64(i), nil)
	}
	if time.Since(start) > 2*time.Second || g.cio.read != 0 || g.cio.open != 0 {
		t.Fatalf("took %s read=%d open=%d", time.Since(start), g.cio.read, g.cio.open)
	}
}
