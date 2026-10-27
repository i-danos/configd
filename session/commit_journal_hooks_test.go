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
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
	stat, read, open       int32
	create, rename, remove int32
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
func (c *countIO) CreateExclusive(p string, m os.FileMode) (io.WriteCloser, error) {
	atomic.AddInt32(&c.create, 1)
	return c.osHookIO.CreateExclusive(p, m)
}
func (c *countIO) Rename(a, b string) error {
	atomic.AddInt32(&c.rename, 1)
	return c.osHookIO.Rename(a, b)
}
func (c *countIO) Remove(p string) error {
	atomic.AddInt32(&c.remove, 1)
	return c.osHookIO.Remove(p)
}
func (c *countIO) total() int32 {
	return atomic.LoadInt32(&c.stat) + atomic.LoadInt32(&c.read) + atomic.LoadInt32(&c.open) +
		atomic.LoadInt32(&c.create) + atomic.LoadInt32(&c.rename) + atomic.LoadInt32(&c.remove)
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
	t.Logf("switch off: hook stat=%d read=%d open=%d create=%d rename=%d remove=%d (total %d) over %d Before+After pairs",
		cio.stat, cio.read, cio.open, cio.create, cio.rename, cio.remove, cio.total(), 6)
	if cio.total() != 0 {
		t.Fatalf("hook system calls with the switch off: stat=%d read=%d open=%d create=%d rename=%d remove=%d",
			cio.stat, cio.read, cio.open, cio.create, cio.rename, cio.remove)
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
	t.Logf("switch on, no control file: stat=%d read=%d open=%d create=%d rename=%d remove=%d",
		cio.stat, cio.read, cio.open, cio.create, cio.rename, cio.remove)
	if cio.stat != 2 || cio.read != 0 || cio.open != 0 || cio.create != 0 || cio.rename != 0 || cio.remove != 0 {
		t.Fatalf("switch on without a control file: stat=%d read=%d open=%d create=%d rename=%d remove=%d, want 2/0/0/0/0/0",
			cio.stat, cio.read, cio.open, cio.create, cio.rename, cio.remove)
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

// ---- the seam marker ---------------------------------------------------------------

// state returns the marker directory of the rig; mk creates it (a test image's tester does that by hand).
func (g *hookRig) state() string { return filepath.Join(g.dir, hookStateDirName) }
func (g *hookRig) mkState() {
	g.t.Helper()
	if err := os.Mkdir(g.state(), 0755); err != nil {
		g.t.Fatal(err)
	}
}

// markerFields parses a marker file into its key=value pairs and checks it is plain lines.
func markerFields(t *testing.T, path string) map[string]string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(b), "\n") {
		t.Fatalf("marker does not end with a newline: %q", b)
	}
	out := map[string]string{}
	for _, ln := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		i := strings.IndexByte(ln, '=')
		if i < 0 {
			t.Fatalf("marker line without '=': %q in %q", ln, b)
		}
		if _, dup := out[ln[:i]]; dup {
			t.Fatalf("duplicate key %q in %q", ln[:i], b)
		}
		out[ln[:i]] = ln[i+1:]
	}
	return out
}

func stateEntries(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range es {
		names = append(names, e.Name())
	}
	return names
}

func TestP2MarkerSaysNotSentAndIsThereWhileTheSeamHolds(t *testing.T) {
	g := newHookRig(t)
	g.mkState()
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\n")
	var done int32
	go func() {
		g.r.BeforeDispatch("alpha", schema.KindForward, 7)
		atomic.AddInt32(&done, 1)
	}()
	want := filepath.Join(g.state(), "P2-alpha-7.hit")
	waitFor(t, "the P2 marker", func() bool { _, err := os.Stat(want); return err == nil })
	if atomic.LoadInt32(&done) != 0 {
		t.Fatal("the marker appeared only after the seam was released")
	}
	f := markerFields(t, want)
	t.Logf("P2 marker while paused: %v", f)
	for k, v := range map[string]string{"seam": "P2", "component": "alpha", "kind": "forward", "seq": "7",
		"pid": strconv.Itoa(os.Getpid()), "call_err": "not_sent"} {
		if f[k] != v {
			t.Errorf("%s = %q, want %q", k, f[k], v)
		}
	}
	if _, err := time.Parse(time.RFC3339, f["at"]); err != nil {
		t.Errorf("at = %q is not RFC3339: %v", f["at"], err)
	}
	if len(f) != 7 {
		t.Errorf("want exactly seam, component, kind, seq, pid, call_err, at; got %v", f)
	}
	fi, _ := os.Stat(want)
	if fi.Mode().Perm() != 0644 {
		t.Errorf("marker mode %v, want 0644", fi.Mode().Perm())
	}
	if names := stateEntries(t, g.state()); len(names) != 1 {
		t.Errorf("state dir holds %v; no temporary file may be left", names)
	}
	stop := release(t, g.fifo)
	defer stop()
	waitFor(t, "release", func() bool { return atomic.LoadInt32(&done) == 1 })
	if !strings.Contains(g.lg.joined(), "marker="+want) {
		t.Errorf("the reached line should name the marker: %s", g.lg.joined())
	}
}

func TestP4MarkerCarriesTheRealCallErr(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"answered successfully", nil, "<nil>"},
		{"answered with an error", errors.New("component refused"), "component refused"},
		{"multi-line error stays one line", errors.New("first line\nsecond\r\nthird\tfield"), "first line second  third field"},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := newHookRig(t)
			g.fake()
			g.mkState()
			g.control(hookP4, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
			g.r.AfterDispatch("alpha", schema.KindForward, 3, c.err)
			f := markerFields(t, filepath.Join(g.state(), "P4-alpha-3.hit"))
			t.Logf("P4 marker: %v", f)
			if f["seam"] != "P4" || f["seq"] != "3" || f["call_err"] != c.want {
				t.Errorf("marker %v, want seam P4, seq 3, call_err %q", f, c.want)
			}
			if len(f) != 7 {
				t.Errorf("want 7 fields, got %v", f)
			}
		})
	}
}

func TestMarkerRenameFailureWarnsOnceAndDoesNotBlock(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.mkState()
	// a non-empty directory where the marker must go: rename(file, directory) fails in the real OS
	blocker := filepath.Join(g.state(), "P4-alpha-1.hit")
	if err := os.MkdirAll(filepath.Join(blocker, "inside"), 0755); err != nil {
		t.Fatal(err)
	}
	g.control(hookP4, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	start := g.clock.now()
	g.r.AfterDispatch("alpha", schema.KindForward, 1, nil) // must return
	log := g.lg.joined()
	t.Logf("log:\n%s", log)
	if n := countLines(log, "danos-test-hook: warning: marker: cannot rename"); n != 1 {
		t.Errorf("want exactly one rename warning, got %d", n)
	}
	if !strings.Contains(log, "reached seam=P4") || elapsed(g, start) < time.Second {
		t.Errorf("the pause itself must still happen after a marker failure (elapsed %s)", elapsed(g, start))
	}
	if strings.Contains(log, "marker=") {
		t.Errorf("no marker was written, the reached line must not claim one: %s", log)
	}
	names := stateEntries(t, g.state())
	if len(names) != 1 || names[0] != "P4-alpha-1.hit" {
		t.Errorf("the failed temporary file must be removed; state dir holds %v", names)
	}
}

func TestMarkerCreateFailureWarnsOnceAndDoesNotBlock(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.mkState()
	// a stale temporary that is a directory: Remove cannot clear it, CreateExclusive fails
	stale := filepath.Join(g.state(), ".P2-alpha-1.hit.tmp")
	if err := os.MkdirAll(filepath.Join(stale, "inside"), 0755); err != nil {
		t.Fatal(err)
	}
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	if _, err := g.r.BeforeDispatch("alpha", schema.KindForward, 1); err != nil {
		t.Fatal(err)
	}
	log := g.lg.joined()
	if n := countLines(log, "danos-test-hook: warning: marker: cannot create"); n != 1 {
		t.Errorf("want exactly one create warning, got %d: %s", n, log)
	}
	if !strings.Contains(log, "reached seam=P2") {
		t.Errorf("the pause must still happen: %s", log)
	}
}

func TestMarkerDirectoryMissingIsSilentAndDoesNotBlock(t *testing.T) {
	g := newHookRig(t) // no state/ directory: the tester did not ask for markers
	g.fake()
	g.control(hookP4, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	start := g.clock.now()
	g.r.AfterDispatch("alpha", schema.KindForward, 1, nil)
	log := g.lg.joined()
	if strings.Contains(log, "marker") {
		t.Errorf("a missing state directory must produce no marker message at all: %s", log)
	}
	if !strings.Contains(log, "reached seam=P4") || elapsed(g, start) < time.Second {
		t.Errorf("the pause must still happen (elapsed %s): %s", elapsed(g, start), log)
	}
	if _, err := os.Stat(g.state()); !os.IsNotExist(err) {
		t.Errorf("the hook must not create the state directory (stat err %v)", err)
	}
	if g.cio.create != 0 || g.cio.rename != 0 {
		t.Errorf("nothing may be created without the directory: create=%d rename=%d", g.cio.create, g.cio.rename)
	}
}

func TestStateIsNotADirectoryWarnsOnceAndDoesNotBlock(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	if err := os.WriteFile(g.state(), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	g.control(hookP4, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	g.r.AfterDispatch("alpha", schema.KindForward, 1, nil)
	log := g.lg.joined()
	if countLines(log, "marker: ") != 1 || !strings.Contains(log, "is not a directory") ||
		!strings.Contains(log, "reached seam=P4") {
		t.Errorf("log: %s", log)
	}
}

func TestNoMarkerWithoutAHitAndOnlyOneForARecordedRetry(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.mkState()
	// switch on, no control file: nothing is written
	g.r.BeforeDispatch("alpha", schema.KindForward, 1)
	g.r.AfterDispatch("alpha", schema.KindForward, 1, nil)
	// control names another component: nothing is written
	g.control(hookP2, "component=beta\nfifo="+g.fifo+"\ntimeout=1s\n")
	g.r.BeforeDispatch("alpha", schema.KindForward, 1)
	if g.cio.create != 0 || len(stateEntries(t, g.state())) != 0 {
		t.Fatalf("a marker was written without a hit: create=%d entries=%v", g.cio.create, stateEntries(t, g.state()))
	}
	// a hit, then the schema layer's re-record calls BeforeDispatch again with the same seq: still one marker
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	g.r.BeforeDispatch("alpha", schema.KindForward, 9)
	g.r.BeforeDispatch("alpha", schema.KindForward, 9)
	if g.cio.create != 1 || g.cio.rename != 1 {
		t.Errorf("create=%d rename=%d, want 1/1", g.cio.create, g.cio.rename)
	}
	if got := stateEntries(t, g.state()); len(got) != 1 || got[0] != "P2-alpha-9.hit" {
		t.Errorf("state dir: %v", got)
	}
}

func TestMarkerNeverGoesIntoTheJournalAndNamesCannotEscape(t *testing.T) {
	g := newHookRig(t)
	g.fake()
	g.mkState()
	g.control(hookP2, "component=alpha\nfifo="+g.fifo+"\ntimeout=1s\n")
	g.r.BeforeDispatch("alpha", schema.KindForward, 1)
	if strings.Contains(readFileT(t, g.path), "call_err") || strings.Contains(readFileT(t, g.path), ".hit") {
		t.Errorf("the marker must not touch commit-state.json: %s", readFileT(t, g.path))
	}
	for _, c := range []string{"../../etc/passwd", "a/b", "net.vyatta.vci.acmd.v1", "x y\n", ""} {
		n := markerName(hookP2, c, 4)
		if strings.ContainsAny(n, "/ \n") || !strings.HasSuffix(n, "-4.hit") || !strings.HasPrefix(n, "P2-") {
			t.Errorf("markerName(%q) = %q", c, n)
		}
	}
	if markerName(hookP4, "net.vyatta.vci.acmd.v1", 1) != "P4-net.vyatta.vci.acmd.v1-1.hit" {
		t.Errorf("a real component name must pass through unchanged")
	}
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
