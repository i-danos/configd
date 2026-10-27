// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// A02 phase 3: test pause points P2 and P4 in the dispatch recorder.
//
//	P2  JournalRecorder.BeforeDispatch, after the dispatch record is durable and
//	    before it returns, i.e. before the Set is sent.
//	P4  JournalRecorder.AfterDispatch, first statement: the component has
//	    replied, the record is not yet marked settled.
//
// They let an outside observer stop configd at exactly those points on a real
// system (and then kill -9 it) instead of racing a window of microseconds.
// See assessment-2026-10-18/P2P4-HOOK-DESIGN.md.
//
// The hooks are compiled in but inert. Both of these must hold for a pause:
//
//  1. DANOS_TEST_HOOKS=1 in configd's environment, read once when the recorder
//     is built (a systemd drop-in on a test image; nothing ships it);
//  2. a control file /etc/danos/test-hooks/pause-p2 (or pause-p4) naming the
//     component, created by hand on the test image.
//
// With (1) off the pause points are one nil comparison: no stat, no open, no
// allocation. The hooks never write the journal, never change a payload, an
// error or an outcome; they only delay.
//
// When a pause starts the hook also leaves a marker file, so an observer can tell
// which seam configd is parked at, and for P4 that the component had already
// answered, without reading journald (which does not survive under TCG):
//
//	<control dir>/state/<seam>-<component>-<seq>.hit     (one key=value per line)
//	seam=P4  component=...  kind=forward  seq=1  pid=...  call_err=<nil>  at=<RFC3339>
//
// call_err is the component's reply for P4 ("<nil>" = it answered successfully) and
// the fixed text "not_sent" for P2. The directory state/ exists only on a test image,
// created by hand like the rest of the control directory; if it is absent nothing is
// written and nothing is reported. The file is written to a temporary name and renamed
// (an observer never sees a partial file); a failure is one warning and the commit
// continues. Markers are never removed here, and never go into commit-state.json.
//
// The pause is bounded (default 10 s, at most 15 s -- below the 20 s
// compensation call deadline, so a P4 pause inside the call goroutine cannot
// turn a success into outcome_unknown) and a hook that cannot work (missing or
// wrong FIFO, malformed file) lets the commit continue at once.
//
// Control file, key=value lines, '#' comments, unknown keys ignored:
//
//	component=<model name as in commit-state.json>   required
//	kind=forward|compensation|reconcile              default forward
//	fifo=<path>                                      default <control file>.fifo
//	timeout=<duration>                               default 10s, clamped to 15s
//
// The seam polls open(fifo, O_WRONLY|O_NONBLOCK) every 50 ms: ENXIO (no reader)
// is "stopped here", a reader appearing releases it. A plain blocking open
// cannot be cancelled from Go, which would make the timeout impossible.
//
// A control file fires once: the seam remembers its (dev, ino, mtime) and arms
// again only for a re-created file. This is also what keeps P2 from pausing
// twice when the pause outlasts the 2 s send window and the schema layer
// re-records (component_compensation.go, dispatch).

package session

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/danos/config/schema"
)

const (
	hooksEnvVar        = "DANOS_TEST_HOOKS"
	hooksDir           = "/etc/danos/test-hooks"
	hookDefaultTimeout = 10 * time.Second
	hookMaxTimeout     = 15 * time.Second
	hookPollInterval   = 50 * time.Millisecond
	hookLogPrefix      = "danos-test-hook:"
	hookStateDirName   = "state"
)

type hookSeam string

const (
	hookP2 hookSeam = "P2"
	hookP4 hookSeam = "P4"
)

// hookIO is every system call the hooks make. A package variable so a test can
// count calls (the inert proof) or substitute them.
type hookIO interface {
	Stat(path string) (os.FileInfo, error)
	ReadFile(path string) ([]byte, error)
	// OpenFifoNonblock opens path write-only without blocking; it fails with
	// ENXIO while no reader has the FIFO open.
	OpenFifoNonblock(path string) (io.WriteCloser, error)
	// CreateExclusive, Rename and Remove write the seam marker atomically.
	CreateExclusive(path string, mode os.FileMode) (io.WriteCloser, error)
	Rename(oldpath, newpath string) error
	Remove(path string) error
}

type osHookIO struct{}

func (osHookIO) Stat(p string) (os.FileInfo, error) { return os.Stat(p) }
func (osHookIO) ReadFile(p string) ([]byte, error)  { return os.ReadFile(p) }
func (osHookIO) OpenFifoNonblock(p string) (io.WriteCloser, error) {
	return os.OpenFile(p, os.O_WRONLY|syscall.O_NONBLOCK, 0)
}

func (osHookIO) CreateExclusive(p string, mode os.FileMode) (io.WriteCloser, error) {
	return os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
}
func (osHookIO) Rename(a, b string) error { return os.Rename(a, b) }
func (osHookIO) Remove(p string) error    { return os.Remove(p) }

var hookFS hookIO = osHookIO{}

type hookFileKey struct {
	dev, ino uint64
	mtimeNs  int64
}

type testHooks struct {
	dir   string
	io    hookIO
	logf  func(format string, args ...interface{})
	sleep func(time.Duration)
	now   func() time.Time
	pid   int

	mu     sync.Mutex
	fired  map[hookSeam]hookFileKey
	warned map[string]bool
}

// hooksFromEnv returns nil (hooks off) unless DANOS_TEST_HOOKS is exactly "1".
func hooksFromEnv(getenv func(string) string, logf func(string, ...interface{})) *testHooks {
	if getenv(hooksEnvVar) != "1" {
		return nil
	}
	return newTestHooks(hooksDir, hookFS, logf)
}

func newTestHooks(dir string, hio hookIO, logf func(string, ...interface{})) *testHooks {
	return &testHooks{
		dir: dir, io: hio, logf: logf,
		sleep: time.Sleep, now: time.Now, pid: os.Getpid(),
		fired:  map[hookSeam]hookFileKey{},
		warned: map[string]bool{},
	}
}

type hookSpec struct {
	component string
	kind      schema.DispatchKind
	fifo      string
	timeout   time.Duration
	clamped   bool
}

// parseHookSpec reads a control file. Problems are returned, not logged, so the
// caller can de-duplicate them.
func parseHookSpec(data []byte, defaultFifo string) (hookSpec, []string) {
	spec := hookSpec{kind: schema.KindForward, fifo: defaultFifo, timeout: hookDefaultTimeout}
	var problems []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.IndexByte(line, '=')
		if i < 0 {
			problems = append(problems, fmt.Sprintf("malformed line %q", line))
			continue
		}
		k, v := strings.TrimSpace(line[:i]), strings.TrimSpace(line[i+1:])
		switch k {
		case "component":
			spec.component = v
		case "kind":
			switch schema.DispatchKind(v) {
			case schema.KindForward, schema.KindCompensation, schema.KindReconcile:
				spec.kind = schema.DispatchKind(v)
			default:
				problems = append(problems, fmt.Sprintf("unknown kind %q", v))
			}
		case "fifo":
			if v != "" {
				spec.fifo = v
			}
		case "timeout":
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				problems = append(problems, fmt.Sprintf("bad timeout %q", v))
				continue
			}
			if d > hookMaxTimeout {
				d, spec.clamped = hookMaxTimeout, true
			}
			spec.timeout = d
		}
	}
	return spec, problems
}

func (h *testHooks) warnOnce(msg string) {
	h.mu.Lock()
	seen := h.warned[msg]
	h.warned[msg] = true
	h.mu.Unlock()
	if !seen {
		h.logf("%s warning: %s", hookLogPrefix, msg)
	}
}

// pause is the pause point. It is safe on a nil receiver, which is the
// production case.
func (h *testHooks) pause(
	seam hookSeam,
	component string,
	kind schema.DispatchKind,
	seq uint64,
	callErr error,
) {
	if h == nil {
		return
	}
	ctl := filepath.Join(h.dir, "pause-"+strings.ToLower(string(seam)))
	fi, err := h.io.Stat(ctl)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			h.warnOnce(fmt.Sprintf("cannot stat %s: %s", ctl, err))
		}
		return
	}
	data, err := h.io.ReadFile(ctl)
	if err != nil {
		h.warnOnce(fmt.Sprintf("cannot read %s: %s", ctl, err))
		return
	}
	spec, problems := parseHookSpec(data, ctl+".fifo")
	if len(problems) > 0 {
		h.warnOnce(fmt.Sprintf("%s: %s", ctl, strings.Join(problems, "; ")))
		return
	}
	if spec.component == "" || spec.component != component || spec.kind != kind {
		return
	}

	key := keyOf(fi)
	h.mu.Lock()
	again := h.fired[seam] == key
	if !again {
		h.fired[seam] = key
	}
	h.mu.Unlock()
	if again {
		return
	}

	pi, err := h.io.Stat(spec.fifo)
	if err != nil {
		h.warnOnce(fmt.Sprintf("seam=%s: cannot stat fifo %s: %s; continuing", seam, spec.fifo, err))
		return
	}
	if pi.Mode()&os.ModeNamedPipe == 0 {
		h.warnOnce(fmt.Sprintf("seam=%s: %s is not a FIFO; continuing", seam, spec.fifo))
		return
	}

	marker := h.writeMarker(seam, component, kind, seq, callErr)

	extra := ""
	if marker != "" {
		extra = " marker=" + marker
	}
	if seam == hookP4 {
		extra += fmt.Sprintf(" callErr=%v", callErr)
	}
	if spec.clamped {
		extra += " (requested timeout clamped)"
	}
	h.logf("%s reached seam=%s pid=%d component=%s kind=%s seq=%d fifo=%s timeout=%s%s",
		hookLogPrefix, seam, h.pid, component, kind, seq, spec.fifo, spec.timeout, extra)

	deadline := h.now().Add(spec.timeout)
	for {
		w, err := h.io.OpenFifoNonblock(spec.fifo)
		if err == nil {
			w.Write([]byte("released\n")) // the reader may already be gone
			w.Close()
			h.logf("%s released seam=%s component=%s", hookLogPrefix, seam, component)
			return
		}
		if !errors.Is(err, syscall.ENXIO) {
			h.warnOnce(fmt.Sprintf("seam=%s: cannot open fifo %s: %s; continuing", seam, spec.fifo, err))
			return
		}
		if !h.now().Before(deadline) {
			h.logf("%s warning: seam=%s timed out after %s with no reader; continuing",
				hookLogPrefix, seam, spec.timeout)
			return
		}
		h.sleep(hookPollInterval)
	}
}

// oneLine makes a value fit on one key=value line.
func oneLine(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) > 400 {
		s = s[:400] + "..."
	}
	return s
}

// markerName is <seam>-<component>-<seq>.hit with anything outside [A-Za-z0-9._-]
// replaced, so a component name can never reach outside the state directory.
func markerName(seam hookSeam, component string, seq uint64) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		}
		return '_'
	}, component)
	return fmt.Sprintf("%s-%s-%d.hit", seam, safe, seq)
}

// writeMarker leaves the seam marker and returns its path, or "" when none was
// written. It never blocks the commit and never returns an error: a missing state
// directory is silent, any other failure is one warning.
func (h *testHooks) writeMarker(
	seam hookSeam,
	component string,
	kind schema.DispatchKind,
	seq uint64,
	callErr error,
) string {
	dir := filepath.Join(h.dir, hookStateDirName)
	fi, err := h.io.Stat(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			h.warnOnce(fmt.Sprintf("marker: cannot stat %s: %s", dir, err))
		}
		return ""
	}
	if !fi.IsDir() {
		h.warnOnce(fmt.Sprintf("marker: %s is not a directory", dir))
		return ""
	}

	callErrText := "not_sent" // P2: the call has not been made
	if seam == hookP4 {
		callErrText = "<nil>"
		if callErr != nil {
			callErrText = oneLine(callErr.Error())
		}
	}
	body := fmt.Sprintf("seam=%s\ncomponent=%s\nkind=%s\nseq=%d\npid=%d\ncall_err=%s\nat=%s\n",
		seam, oneLine(component), oneLine(string(kind)), seq, h.pid, callErrText,
		h.now().UTC().Format(time.RFC3339))

	name := markerName(seam, component, seq)
	final := filepath.Join(dir, name)
	tmp := filepath.Join(dir, "."+name+".tmp")
	h.io.Remove(tmp) // a leftover from a configd killed in the middle of this very write

	f, err := h.io.CreateExclusive(tmp, 0644)
	if err != nil {
		h.warnOnce(fmt.Sprintf("marker: cannot create %s: %s; continuing", tmp, err))
		return ""
	}
	if _, err := f.Write([]byte(body)); err != nil {
		f.Close()
		h.io.Remove(tmp)
		h.warnOnce(fmt.Sprintf("marker: cannot write %s: %s; continuing", tmp, err))
		return ""
	}
	if err := f.Close(); err != nil {
		h.io.Remove(tmp)
		h.warnOnce(fmt.Sprintf("marker: cannot close %s: %s; continuing", tmp, err))
		return ""
	}
	if err := h.io.Rename(tmp, final); err != nil {
		h.io.Remove(tmp)
		h.warnOnce(fmt.Sprintf("marker: cannot rename %s to %s: %s; continuing", tmp, final, err))
		return ""
	}
	return final
}

func keyOf(fi os.FileInfo) hookFileKey {
	k := hookFileKey{mtimeNs: fi.ModTime().UnixNano()}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		k.dev, k.ino = uint64(st.Dev), uint64(st.Ino)
	}
	return k
}
