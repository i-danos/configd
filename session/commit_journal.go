// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// A02 phase 3, stage 3-B1: the commit journal and the dispatch recorder.
//
// The journal is one small JSON file next to the runfile
// (/var/run/vyatta/configd/commit-state.json in production). It is rewritten
// WHOLESALE by every write, atomically (temp file in the same directory,
// created 0600, written completely, fsync, rename, directory fsync), so that a
// reader only ever sees a complete old or a complete new file.
//
// DispatchRecorder is the hook config/schema calls around every component
// call. BeforeDispatch persists a dispatch record BEFORE the call is made; if
// that fails the call must not be made. AfterDispatch only marks the record
// settled and is best effort.
//
// Not in this file (later stages): the commit record protocol, the fence
// gate, startup recovery, the reconcile entry.

package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/danos/config/schema"
)

const (
	journalVersion  = 1
	journalFileName = "commit-state.json"
)

// JournalPathFor returns where the journal lives for a given runfile: in the
// runfile's directory (both describe the same runtime and have the same
// lifetime).
func JournalPathFor(runfile string) string {
	return filepath.Join(filepath.Dir(runfile), journalFileName)
}

// ---- state ----------------------------------------------------------------

// JournalState is the whole content of the journal file.
type JournalState struct {
	Version  int                        `json:"version"`
	Commit   *JournalCommit             `json:"commit"`
	Dispatch map[string]JournalDispatch `json:"dispatch"`
	Dirty    map[string]JournalDirty    `json:"dirty"`
}

// JournalCommit is the commit-level record (used from stage 3-B2 on).
type JournalCommit struct {
	ID              string   `json:"id"`
	State           string   `json:"state"` // applying | persisting | persisted_unsynced
	Todo            []string `json:"todo"`
	FirstEmptyRtree bool     `json:"first_empty_rtree"`
}

// JournalDispatch is the per-component dispatch record, written before a call
// is made.
type JournalDispatch struct {
	Seq             uint64              `json:"seq"`
	Kind            schema.DispatchKind `json:"kind"`
	MayHaveBeenSent bool                `json:"may_have_been_sent"`
	TRecordMonoUs   int64               `json:"t_record_mono_us"`
	Identity        ServiceIdentity     `json:"identity"`
	Settled         bool                `json:"settled"`
}

// JournalDirty is one entry of the dirty set (used from stage 3-B2 on).
type JournalDirty struct {
	Class    schema.DirtyClass `json:"class"`
	Reason   string            `json:"reason"`
	CommitID string            `json:"commit_id"`
	Since    string            `json:"since"`
}

func newJournalState() JournalState {
	return JournalState{
		Version:  journalVersion,
		Dispatch: map[string]JournalDispatch{},
		Dirty:    map[string]JournalDirty{},
	}
}

func (s JournalState) clone() JournalState {
	c := JournalState{
		Version:  s.Version,
		Dispatch: make(map[string]JournalDispatch, len(s.Dispatch)),
		Dirty:    make(map[string]JournalDirty, len(s.Dirty)),
	}
	if s.Commit != nil {
		cc := *s.Commit
		cc.Todo = append([]string(nil), s.Commit.Todo...)
		c.Commit = &cc
	}
	for k, v := range s.Dispatch {
		c.Dispatch[k] = v
	}
	for k, v := range s.Dirty {
		c.Dirty[k] = v
	}
	return c
}

// ---- file system seam -------------------------------------------------------

type journalFile interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// journalFS is the small set of file operations the journal uses, so tests can
// inject faults at every step and run on a private directory.
type journalFS interface {
	// CreateExclusive creates a new file that must not exist, with mode set at
	// creation.
	CreateExclusive(path string, mode os.FileMode) (journalFile, error)
	Rename(oldpath, newpath string) error
	Remove(path string) error
	SyncDir(dir string) error
	ReadFile(path string) ([]byte, error)
}

type osJournalFS struct{}

func (osJournalFS) CreateExclusive(path string, mode os.FileMode) (journalFile, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
}
func (osJournalFS) Rename(o, n string) error             { return os.Rename(o, n) }
func (osJournalFS) Remove(path string) error             { return os.Remove(path) }
func (osJournalFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }
func (osJournalFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// JournalWriteError says which step of the atomic write failed. Replaced is
// true only when the new file is already in place (the directory sync after
// the rename failed): its content is complete but its durability is not
// confirmed. For every earlier step the old file is untouched.
type JournalWriteError struct {
	Step     string
	Err      error
	Replaced bool
}

func (e *JournalWriteError) Error() string {
	return fmt.Sprintf("journal write: %s: %s", e.Step, e.Err)
}
func (e *JournalWriteError) Unwrap() error { return e.Err }

// JournalCorruptError: the journal file exists but cannot be interpreted.
type JournalCorruptError struct {
	Path string
	Err  error
}

func (e *JournalCorruptError) Error() string {
	return fmt.Sprintf("journal %s is unreadable: %s", e.Path, e.Err)
}
func (e *JournalCorruptError) Unwrap() error { return e.Err }

// atomicWriteFile replaces path by data: the temp file is created 0600 in the
// same directory, written completely, synced and closed, then renamed over
// path, then the directory is synced. Any failure before the rename removes
// the temp file and leaves path as it was.
func atomicWriteFile(fs journalFS, path string, data []byte) error {
	tmp := path + ".tmp"
	// A temp file left by a crashed writer must not block the next write, and
	// its mode must not be inherited: start from nothing.
	if err := fs.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &JournalWriteError{Step: "remove stale temp", Err: err}
	}
	f, err := fs.CreateExclusive(tmp, 0600)
	if err != nil {
		return &JournalWriteError{Step: "create", Err: err}
	}
	fail := func(step string, err error) error {
		f.Close()
		fs.Remove(tmp)
		return &JournalWriteError{Step: step, Err: err}
	}
	for off := 0; off < len(data); {
		n, werr := f.Write(data[off:])
		if n > 0 {
			off += n
		}
		if werr != nil {
			return fail("write", werr)
		}
		if n == 0 {
			return fail("write", io.ErrShortWrite)
		}
	}
	if err := f.Sync(); err != nil {
		return fail("fsync", err)
	}
	if err := f.Close(); err != nil {
		fs.Remove(tmp)
		return &JournalWriteError{Step: "close", Err: err}
	}
	if err := fs.Rename(tmp, path); err != nil {
		fs.Remove(tmp)
		return &JournalWriteError{Step: "rename", Err: err}
	}
	if err := fs.SyncDir(filepath.Dir(path)); err != nil {
		return &JournalWriteError{Step: "directory fsync", Err: err, Replaced: true}
	}
	return nil
}

// ---- the journal ------------------------------------------------------------

// Journal owns the journal file. All methods are safe for concurrent use.
type Journal struct {
	mu    sync.Mutex
	path  string
	fs    journalFS
	state JournalState
	logf  func(format string, args ...interface{})
}

// OpenJournal opens (and loads, if it exists) the journal at path. The path is
// always explicit: nothing is opened implicitly.
func OpenJournal(path string) (*Journal, error) {
	return openJournal(path, osJournalFS{}, log.Printf)
}

func openJournal(path string, fs journalFS, logf func(string, ...interface{})) (*Journal, error) {
	j := &Journal{path: path, fs: fs, state: newJournalState(), logf: logf}
	data, err := fs.ReadFile(path)
	switch {
	case err == nil:
		st := newJournalState()
		if uerr := json.Unmarshal(data, &st); uerr != nil {
			return nil, &JournalCorruptError{Path: path, Err: uerr}
		}
		if st.Version != journalVersion {
			return nil, &JournalCorruptError{Path: path,
				Err: fmt.Errorf("unsupported version %d", st.Version)}
		}
		if st.Dispatch == nil {
			st.Dispatch = map[string]JournalDispatch{}
		}
		if st.Dirty == nil {
			st.Dirty = map[string]JournalDirty{}
		}
		j.state = st
	case errors.Is(err, os.ErrNotExist):
		// no journal yet: empty state, nothing is written until the first update
	default:
		return nil, fmt.Errorf("journal %s: %w", path, err)
	}
	return j, nil
}

// State returns a copy of the current state.
func (j *Journal) State() JournalState {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.state.clone()
}

// Update applies mutate to a copy of the state and persists the whole file. If
// mutate reports no change nothing is written. If the write fails before the
// rename the old file and the in-memory state are unchanged; if only the final
// directory sync fails the new file is in place and the in-memory state follows
// it (it mirrors the file); the error is returned either way.
func (j *Journal) Update(mutate func(*JournalState) bool) error {
	return j.update(mutate, false)
}

// UpdateKeepDirty is Update for the writes that carry the dirty set (end of a
// commit, end of a reconcile). When the file cannot be written, the dirty set
// the mutation computed is still adopted in memory (design 11.1: a component
// whose state could not be recorded "stays dirty in memory"), so the hint, the
// fence gate and reconcile keep seeing it. The commit and dispatch records are
// NOT adopted: a record that could not be written must not be claimed. The
// next successful write rewrites the whole file from this state (design 6.3,
// transition (a)).
func (j *Journal) UpdateKeepDirty(mutate func(*JournalState) bool) error {
	return j.update(mutate, true)
}

func (j *Journal) update(mutate func(*JournalState) bool, keepDirty bool) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	next := j.state.clone()
	if !mutate(&next) {
		return nil
	}
	next.Version = journalVersion
	data, err := json.Marshal(&next)
	if err != nil {
		if keepDirty {
			j.state.Dirty = next.Dirty
		}
		return &JournalWriteError{Step: "encode", Err: err}
	}
	data = append(data, '\n')
	if err := atomicWriteFile(j.fs, j.path, data); err != nil {
		var we *JournalWriteError
		if errors.As(err, &we) && we.Replaced {
			j.state = next
		} else if keepDirty {
			j.state.Dirty = next.Dirty
		}
		return err
	}
	j.state = next
	return nil
}

// ---- the dispatch recorder ---------------------------------------------------

// JournalRecorder implements schema.DispatchRecorder on top of the journal.
type JournalRecorder struct {
	j    *Journal
	ids  IdentityReader
	mono func() int64
	logf func(format string, args ...interface{})
}

var _ schema.DispatchRecorder = (*JournalRecorder)(nil)

// NewJournalRecorder builds the recorder. mono is the CLOCK_MONOTONIC clock in
// microseconds (schema.MonotonicNowUs in production); nil selects it.
func NewJournalRecorder(j *Journal, ids IdentityReader, mono func() int64) *JournalRecorder {
	if mono == nil {
		mono = schema.MonotonicNowUs
	}
	return &JournalRecorder{j: j, ids: ids, mono: mono, logf: j.logf}
}

// BeforeDispatch reads the service identity of the component, samples the
// record time t (after the read, so a slow read cannot use up the send window),
// persists dispatch[component] = {seq, kind, may_have_been_sent: true,
// t_record_mono_us: t, identity, settled: false} and only then returns t. A non-nil error means the record is NOT known to be durable: the caller
// must not send the call. An unreadable identity is recorded as
// "unknown-identity" (it never turns into a failure by itself).
func (r *JournalRecorder) BeforeDispatch(
	component string,
	kind schema.DispatchKind,
	seq uint64,
) (int64, error) {

	// Read the identity FIRST and sample t afterwards. The send window of the
	// schema layer is measured from t, so a slow identity read (it is bounded
	// by a 2 s timeout in the systemd reader) must not eat the window: t is
	// the start of the persist, and only the persist, rename and scheduling
	// are inside it.
	id, err := r.ids.Identity(component)
	readAt := r.mono()
	if err != nil {
		id = ServiceIdentity{} // unknown-identity
		r.logf("journal: service identity of %s unreadable (%s); recording unknown-identity",
			component, err)
	} else {
		id.Known = true
		id.ReadAtMonoUs = readAt
	}
	t := r.mono() // start of the record write
	rec := JournalDispatch{
		Seq:             seq,
		Kind:            kind,
		MayHaveBeenSent: true,
		TRecordMonoUs:   t,
		Identity:        id,
		Settled:         false,
	}
	if err := r.j.Update(func(s *JournalState) bool {
		s.Dispatch[component] = rec
		return true
	}); err != nil {
		return 0, err
	}
	return t, nil
}

// AfterDispatch marks the record of this very call settled. It is best effort:
// a failure is logged and the record simply stays settled:false (the
// conservative state). A record that was replaced by a newer dispatch is left
// alone.
func (r *JournalRecorder) AfterDispatch(
	component string,
	kind schema.DispatchKind,
	seq uint64,
	callErr error,
) {
	err := r.j.Update(func(s *JournalState) bool {
		d, ok := s.Dispatch[component]
		if !ok || d.Seq != seq || d.Kind != kind || d.Settled {
			return false
		}
		d.Settled = true
		s.Dispatch[component] = d
		return true
	})
	if err != nil {
		r.logf("journal: cannot mark the %s dispatch of %s (seq %d) settled: %s",
			kind, component, seq, err)
	}
}
