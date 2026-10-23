// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-B1 tests: the atomic journal writer, the dispatch recorder and the
// service identity reader. Every journal lives in a test-private directory
// (t.TempDir) behind an injectable file system; nothing here touches the
// host's /var/run or any real configd state.

package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	systemd "github.com/coreos/go-systemd/dbus"
	"github.com/danos/config/schema"
)

// ---- fault-injecting file system on a private directory --------------------

type faultFS struct {
	osJournalFS
	mu     sync.Mutex
	ops    []string
	failOn map[string]error // op name -> error (applies to every call of that op)
	// shortWrite makes every Write accept at most this many bytes (0 = all)
	shortWrite int
	// failWriteAfter fails the Write call number n (1-based) with an error, after
	// accepting nothing (0 = never)
	failWriteAfter int
	writes         int
}

func newFaultFS() *faultFS { return &faultFS{failOn: map[string]error{}} }

func (f *faultFS) log(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
	return f.failOn[op]
}

func (f *faultFS) opList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

func (f *faultFS) CreateExclusive(path string, mode os.FileMode) (journalFile, error) {
	if err := f.log("create"); err != nil {
		return nil, err
	}
	jf, err := f.osJournalFS.CreateExclusive(path, mode)
	if err != nil {
		return nil, err
	}
	return &faultFile{journalFile: jf, fs: f}, nil
}
func (f *faultFS) Rename(o, n string) error {
	if err := f.log("rename"); err != nil {
		return err
	}
	return f.osJournalFS.Rename(o, n)
}
func (f *faultFS) Remove(p string) error {
	if err := f.log("remove"); err != nil {
		return err
	}
	return f.osJournalFS.Remove(p)
}
func (f *faultFS) SyncDir(d string) error {
	if err := f.log("syncdir"); err != nil {
		return err
	}
	return f.osJournalFS.SyncDir(d)
}
func (f *faultFS) ReadFile(p string) ([]byte, error) {
	if err := f.log("read"); err != nil {
		return nil, err
	}
	return f.osJournalFS.ReadFile(p)
}

type faultFile struct {
	journalFile
	fs *faultFS
}

func (f *faultFile) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	f.fs.writes++
	n := f.fs.writes
	fail := f.fs.failWriteAfter
	short := f.fs.shortWrite
	f.fs.mu.Unlock()
	if err := f.fs.log("write"); err != nil {
		return 0, err
	}
	if fail > 0 && n == fail {
		return 0, errors.New("injected write error")
	}
	if short > 0 && len(p) > short {
		p = p[:short]
	}
	return f.journalFile.Write(p)
}
func (f *faultFile) Sync() error {
	if err := f.fs.log("fsync"); err != nil {
		return err
	}
	return f.journalFile.Sync()
}
func (f *faultFile) Close() error {
	if err := f.fs.log("close"); err != nil {
		f.journalFile.Close() // do not leak the descriptor in the test
		return err
	}
	return f.journalFile.Close()
}

// privateDir returns a journal directory that is guaranteed to be below the
// test's own temp dir, never the host's /var/run.
func privateDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	for _, bad := range []string{"/var/run", "/run"} {
		if strings.HasPrefix(d, bad+"/") || d == bad {
			t.Fatalf("test directory %s is a host runtime directory", d)
		}
	}
	return d
}

type testLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *testLog) logf(format string, a ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, a...))
}

func (l *testLog) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func newTestJournal(t *testing.T) (*Journal, *faultFS, string, *testLog) {
	t.Helper()
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	fs := newFaultFS()
	lg := &testLog{}
	j, err := openJournal(path, fs, lg.logf)
	if err != nil {
		t.Fatal(err)
	}
	return j, fs, path, lg
}

func readFileT(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func has2(ops []string, op string) bool {
	for _, o := range ops {
		if o == op {
			return true
		}
	}
	return false
}

// ---- atomic writer ---------------------------------------------------------

func TestAtomicWriteStepOrderModeAndNoTempLeft(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	fs := newFaultFS()
	if err := atomicWriteFile(fs, path, []byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	want := []string{"remove", "create", "write", "fsync", "close", "rename", "syncdir"}
	if got := fs.opList(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("steps: got %v want %v", got, want)
	}
	if readFileT(t, path) != "hello\n" {
		t.Error("content")
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0600 {
		t.Errorf("mode = %o, want 600", st.Mode().Perm())
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file left behind: %v", err)
	}
}

func TestAtomicWriteFailureAtEachStepKeepsTheOldFile(t *testing.T) {
	for _, step := range []string{"create", "write", "fsync", "close", "rename"} {
		t.Run(step, func(t *testing.T) {
			dir := privateDir(t)
			path := filepath.Join(dir, "commit-state.json")
			if err := os.WriteFile(path, []byte("OLD\n"), 0600); err != nil {
				t.Fatal(err)
			}
			fs := newFaultFS()
			fs.failOn[step] = errors.New("injected " + step)
			err := atomicWriteFile(fs, path, []byte("NEW\n"))
			var we *JournalWriteError
			if !errors.As(err, &we) {
				t.Fatalf("error = %v", err)
			}
			if we.Replaced {
				t.Error("the file must not be reported as replaced")
			}
			if !strings.Contains(we.Step, step) {
				t.Errorf("step = %q, want it to name %q", we.Step, step)
			}
			if readFileT(t, path) != "OLD\n" {
				t.Fatalf("old file changed: %q", readFileT(t, path))
			}
			if _, serr := os.Stat(path + ".tmp"); !os.IsNotExist(serr) {
				t.Errorf("temp file left behind after a failed %s", step)
			}
		})
	}
}

func TestAtomicWriteStaleTempRemoveFailureIsAnErrorAndOldFileStays(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	os.WriteFile(path, []byte("OLD\n"), 0600)
	fs := newFaultFS()
	fs.failOn["remove"] = errors.New("EPERM")
	err := atomicWriteFile(fs, path, []byte("NEW\n"))
	if err == nil || readFileT(t, path) != "OLD\n" {
		t.Fatalf("%v %q", err, readFileT(t, path))
	}
	if has2(fs.opList(), "create") {
		t.Error("nothing may be created after the stale temp could not be removed")
	}
}

func TestAtomicWriteStaleTempFromACrashDoesNotBlockOrLeakItsMode(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	if err := os.WriteFile(path+".tmp", []byte("garbage"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := atomicWriteFile(osJournalFS{}, path, []byte("fresh\n")); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(path)
	if readFileT(t, path) != "fresh\n" || st.Mode().Perm() != 0600 {
		t.Errorf("%q %o", readFileT(t, path), st.Mode().Perm())
	}
}

func TestAtomicWriteShortWritesAreCompleted(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	fs := newFaultFS()
	fs.shortWrite = 3
	data := strings.Repeat("0123456789", 20)
	if err := atomicWriteFile(fs, path, []byte(data)); err != nil {
		t.Fatal(err)
	}
	if readFileT(t, path) != data {
		t.Error("content incomplete after short writes")
	}
}

func TestAtomicWriteSecondShortWriteFailingKeepsTheOldFile(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	os.WriteFile(path, []byte("OLD\n"), 0600)
	fs := newFaultFS()
	fs.shortWrite = 3
	fs.failWriteAfter = 2
	err := atomicWriteFile(fs, path, []byte(strings.Repeat("x", 50)))
	if err == nil || readFileT(t, path) != "OLD\n" {
		t.Fatalf("%v %q", err, readFileT(t, path))
	}
}

func TestAtomicWriteDirectorySyncFailureReportsReplaced(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	os.WriteFile(path, []byte("OLD\n"), 0600)
	fs := newFaultFS()
	fs.failOn["syncdir"] = errors.New("EIO")
	err := atomicWriteFile(fs, path, []byte("NEW\n"))
	var we *JournalWriteError
	if !errors.As(err, &we) || !we.Replaced || we.Step != "directory fsync" {
		t.Fatalf("error = %v", err)
	}
	if readFileT(t, path) != "NEW\n" {
		t.Error("the complete new file is in place when only the directory sync failed")
	}
}

// ---- journal ---------------------------------------------------------------

func TestOpenJournalMissingFileIsEmptyAndWritesNothing(t *testing.T) {
	j, fs, path, _ := newTestJournal(t)
	if len(j.State().Dispatch) != 0 || len(j.State().Dirty) != 0 || j.State().Commit != nil {
		t.Errorf("state: %+v", j.State())
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("opening must not create the file")
	}
	if has2(fs.opList(), "create") {
		t.Error("no write on open")
	}
}

func TestOpenJournalLoadsAnExistingFileCorruptAndWrongVersion(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "commit-state.json")
	good := `{"version":1,"commit":{"id":"c1","state":"applying","todo":["a","b"],"first_empty_rtree":true},` +
		`"dispatch":{"a":{"seq":3,"kind":"forward","may_have_been_sent":true,"t_record_mono_us":42,` +
		`"identity":{"main_pid":7,"active_enter_mono_us":9,"read_at_mono_us":10},"settled":false},` +
		`"b":{"seq":4,"kind":"reconcile","may_have_been_sent":true,"t_record_mono_us":43,"identity":"unknown-identity","settled":true}},` +
		`"dirty":{"a":{"class":"unfenced","reason":"r","commit_id":"c1","since":"2026-10-20T00:00:00Z"}}}`
	os.WriteFile(path, []byte(good), 0600)
	j, err := openJournal(path, osJournalFS{}, func(string, ...interface{}) {})
	if err != nil {
		t.Fatal(err)
	}
	st := j.State()
	if st.Commit == nil || st.Commit.ID != "c1" || !st.Commit.FirstEmptyRtree || len(st.Commit.Todo) != 2 {
		t.Errorf("commit: %+v", st.Commit)
	}
	a := st.Dispatch["a"]
	if a.Seq != 3 || a.Kind != schema.KindForward || !a.Identity.Known || a.Identity.MainPID != 7 || a.Settled {
		t.Errorf("a: %+v", a)
	}
	if b := st.Dispatch["b"]; b.Identity.Known || !b.Settled || b.Kind != schema.KindReconcile {
		t.Errorf("b: %+v", b)
	}
	if st.Dirty["a"].Class != schema.DirtyUnfenced {
		t.Errorf("dirty: %+v", st.Dirty)
	}

	os.WriteFile(path, []byte("{not json"), 0600)
	_, err = openJournal(path, osJournalFS{}, func(string, ...interface{}) {})
	var ce *JournalCorruptError
	if !errors.As(err, &ce) {
		t.Errorf("corrupt: %v", err)
	}
	os.WriteFile(path, []byte(`{"version":99}`), 0600)
	if _, err = openJournal(path, osJournalFS{}, func(string, ...interface{}) {}); !errors.As(err, &ce) {
		t.Errorf("wrong version: %v", err)
	}
	fs := newFaultFS()
	fs.failOn["read"] = errors.New("EIO")
	if _, err = openJournal(path, fs, func(string, ...interface{}) {}); err == nil || errors.As(err, &ce) {
		t.Errorf("read error must be an error but not 'corrupt': %v", err)
	}
}

func TestUpdateFailureLeavesFileAndMemoryUnchanged(t *testing.T) {
	j, fs, path, _ := newTestJournal(t)
	if err := j.Update(func(s *JournalState) bool {
		s.Dirty["x"] = JournalDirty{Class: schema.DirtyUnknown}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	before := readFileT(t, path)
	fs.failOn["rename"] = errors.New("injected")
	err := j.Update(func(s *JournalState) bool {
		s.Dirty["y"] = JournalDirty{Class: schema.DirtyUnknown}
		return true
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if readFileT(t, path) != before {
		t.Error("old file changed")
	}
	if _, ok := j.State().Dirty["y"]; ok {
		t.Error("memory advanced although the write failed")
	}
}

func TestUpdateDirectorySyncFailureMirrorsTheFileInMemory(t *testing.T) {
	j, fs, path, _ := newTestJournal(t)
	fs.failOn["syncdir"] = errors.New("EIO")
	err := j.Update(func(s *JournalState) bool {
		s.Dirty["y"] = JournalDirty{Class: schema.DirtyUnknown}
		return true
	})
	if err == nil {
		t.Fatal("expected the directory sync error")
	}
	if !strings.Contains(readFileT(t, path), `"y"`) {
		t.Error("the new content is in the file")
	}
	if _, ok := j.State().Dirty["y"]; !ok {
		t.Error("memory must mirror the file that is now in place")
	}
}

func TestUpdateNoChangeWritesNothing(t *testing.T) {
	j, fs, _, _ := newTestJournal(t)
	opened := len(fs.opList()) // the read done by openJournal
	if err := j.Update(func(*JournalState) bool { return false }); err != nil {
		t.Fatal(err)
	}
	if got := fs.opList()[opened:]; len(got) != 0 {
		t.Errorf("ops after open: %v", got)
	}
}

// ---- dispatch recorder -----------------------------------------------------

type fakeIDs struct {
	mu    sync.Mutex
	ids   map[string]ServiceIdentity
	errs  map[string]error
	reads []string
}

func (f *fakeIDs) Identity(c string) (ServiceIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads = append(f.reads, c)
	if err := f.errs[c]; err != nil {
		return ServiceIdentity{}, err
	}
	return f.ids[c], nil
}

func newRecorder(t *testing.T) (*JournalRecorder, *Journal, *faultFS, string, *fakeIDs, *testLog, *int64) {
	j, fs, path, lg := newTestJournal(t)
	ids := &fakeIDs{ids: map[string]ServiceIdentity{
		"alpha": {MainPID: 100, ActiveEnterMonoUs: 5000},
		"beta":  {MainPID: 200, ActiveEnterMonoUs: 6000},
	}, errs: map[string]error{}}
	var clock int64 = 1000000
	mono := func() int64 { return atomic.AddInt64(&clock, 10) }
	return NewJournalRecorder(j, ids, mono), j, fs, path, ids, lg, &clock
}

func TestBeforeDispatchPersistsTheRecordBeforeReturning(t *testing.T) {
	r, j, _, path, ids, _, _ := newRecorder(t)
	var _ schema.DispatchRecorder = r
	tr, err := r.BeforeDispatch("alpha", schema.KindForward, 7)
	if err != nil {
		t.Fatal(err)
	}
	// the record is on disk by the time BeforeDispatch has returned
	var onDisk JournalState
	if err := json.Unmarshal([]byte(readFileT(t, path)), &onDisk); err != nil {
		t.Fatal(err)
	}
	d := onDisk.Dispatch["alpha"]
	if d.Seq != 7 || d.Kind != schema.KindForward || !d.MayHaveBeenSent || d.Settled {
		t.Fatalf("record: %+v", d)
	}
	if d.TRecordMonoUs != tr {
		t.Errorf("returned time %d != stored %d", tr, d.TRecordMonoUs)
	}
	if !d.Identity.Known || d.Identity.MainPID != 100 || d.Identity.ActiveEnterMonoUs != 5000 {
		t.Errorf("identity: %+v", d.Identity)
	}
	// The identity is read first and t is sampled afterwards (a slow read must
	// not use up the send window), so read_at is not later than the record time.
	if d.Identity.ReadAtMonoUs > d.TRecordMonoUs {
		t.Errorf("read_at %d must not be after the record time %d", d.Identity.ReadAtMonoUs, d.TRecordMonoUs)
	}
	if !strings.Contains(readFileT(t, path), `"may_have_been_sent":true`) ||
		!strings.Contains(readFileT(t, path), `"settled":false`) {
		t.Errorf("file: %s", readFileT(t, path))
	}
	if j.State().Dispatch["alpha"] != d {
		t.Error("memory differs from the file")
	}
	if len(ids.reads) != 1 || ids.reads[0] != "alpha" {
		t.Errorf("identity reads: %v", ids.reads)
	}
}

func TestBeforeDispatchRereadsTheIdentityForEveryCall(t *testing.T) {
	r, j, _, _, ids, _, _ := newRecorder(t)
	r.BeforeDispatch("alpha", schema.KindForward, 1)
	ids.mu.Lock()
	ids.ids["alpha"] = ServiceIdentity{MainPID: 101, ActiveEnterMonoUs: 7777}
	ids.mu.Unlock()
	r.BeforeDispatch("alpha", schema.KindCompensation, 2)
	d := j.State().Dispatch["alpha"]
	if d.Seq != 2 || d.Kind != schema.KindCompensation || d.Identity.MainPID != 101 ||
		d.Identity.ActiveEnterMonoUs != 7777 {
		t.Fatalf("second record: %+v", d)
	}
}

func TestBeforeDispatchUnreadableIdentityIsUnknownIdentityNotAFailure(t *testing.T) {
	r, j, _, path, ids, lg, _ := newRecorder(t)
	ids.errs["beta"] = errors.New("systemd unreachable")
	if _, err := r.BeforeDispatch("beta", schema.KindForward, 3); err != nil {
		t.Fatalf("an unreadable identity must not fail the record: %v", err)
	}
	if d := j.State().Dispatch["beta"]; d.Identity.Known || !d.MayHaveBeenSent || d.Settled {
		t.Fatalf("record: %+v", d)
	}
	if !strings.Contains(readFileT(t, path), `"identity":"unknown-identity"`) {
		t.Errorf("file: %s", readFileT(t, path))
	}
	if !strings.Contains(lg.joined(), "unknown-identity") {
		t.Errorf("not logged: %q", lg.joined())
	}
	// it survives a reload as unknown
	j2, err := openJournal(path, osJournalFS{}, func(string, ...interface{}) {})
	if err != nil {
		t.Fatal(err)
	}
	if j2.State().Dispatch["beta"].Identity.Known {
		t.Error("reloaded identity must still be unknown")
	}
}

func TestBeforeDispatchPersistFailureReturnsErrorAndKeepsTheOldFile(t *testing.T) {
	for _, step := range []string{"create", "write", "fsync", "close", "rename"} {
		t.Run(step, func(t *testing.T) {
			r, j, fs, path, _, _, _ := newRecorder(t)
			if _, err := r.BeforeDispatch("alpha", schema.KindForward, 1); err != nil {
				t.Fatal(err)
			}
			before := readFileT(t, path)
			fs.failOn[step] = errors.New("injected " + step)
			tr, err := r.BeforeDispatch("beta", schema.KindForward, 2)
			if err == nil {
				t.Fatal("a record that could not be persisted must be an error: the caller must not send")
			}
			if tr != 0 {
				t.Errorf("no record time may be returned with an error: %d", tr)
			}
			if readFileT(t, path) != before {
				t.Error("old file changed")
			}
			if _, ok := j.State().Dispatch["beta"]; ok {
				t.Error("memory holds a record that is not on disk")
			}
			var we *JournalWriteError
			if !errors.As(err, &we) {
				t.Errorf("error type: %v", err)
			}
		})
	}
}

func TestBeforeDispatchDirectorySyncFailureIsAnErrorToo(t *testing.T) {
	r, _, fs, _, _, _, _ := newRecorder(t)
	fs.failOn["syncdir"] = errors.New("EIO")
	if _, err := r.BeforeDispatch("alpha", schema.KindForward, 1); err == nil {
		t.Fatal("a record whose durability is not confirmed must not allow the send")
	}
}

func TestBeforeDispatchKeepsTheRestOfTheJournal(t *testing.T) {
	r, j, _, path, _, _, _ := newRecorder(t)
	j.Update(func(s *JournalState) bool {
		s.Commit = &JournalCommit{ID: "c9", State: "applying", Todo: []string{"alpha", "beta"}}
		s.Dirty["gamma"] = JournalDirty{Class: schema.DirtyUnfenced, Reason: "keep me"}
		return true
	})
	r.BeforeDispatch("alpha", schema.KindForward, 1)
	var on JournalState
	json.Unmarshal([]byte(readFileT(t, path)), &on)
	if on.Commit == nil || on.Commit.ID != "c9" || on.Dirty["gamma"].Reason != "keep me" {
		t.Errorf("file: %s", readFileT(t, path))
	}
}

func TestAfterDispatchMarksTheMatchingRecordSettled(t *testing.T) {
	r, j, _, path, _, _, _ := newRecorder(t)
	r.BeforeDispatch("alpha", schema.KindForward, 5)
	r.AfterDispatch("alpha", schema.KindForward, 5, nil)
	if !j.State().Dispatch["alpha"].Settled {
		t.Fatal("not settled")
	}
	var on JournalState
	json.Unmarshal([]byte(readFileT(t, path)), &on)
	d := on.Dispatch["alpha"]
	if !d.Settled || !d.MayHaveBeenSent || d.Seq != 5 {
		t.Errorf("on disk: %+v", d)
	}
}

func TestAfterDispatchOfAnErrorReplyAlsoSettles(t *testing.T) {
	r, j, _, _, _, _, _ := newRecorder(t)
	r.BeforeDispatch("alpha", schema.KindForward, 5)
	r.AfterDispatch("alpha", schema.KindForward, 5, errors.New("component said no"))
	if !j.State().Dispatch["alpha"].Settled {
		t.Error("a reply with an error is still a reply")
	}
}

func TestAfterDispatchIgnoresStaleOrUnknownRecordsWithoutWriting(t *testing.T) {
	r, j, fs, _, _, _, _ := newRecorder(t)
	r.BeforeDispatch("alpha", schema.KindForward, 5)
	r.BeforeDispatch("alpha", schema.KindCompensation, 6) // newer record replaces it
	writes := len(fs.opList())
	r.AfterDispatch("alpha", schema.KindForward, 5, nil)   // stale seq
	r.AfterDispatch("alpha", schema.KindReconcile, 6, nil) // wrong kind
	r.AfterDispatch("nosuch", schema.KindForward, 1, nil)  // unknown component
	if len(fs.opList()) != writes {
		t.Errorf("no journal write expected: %v", fs.opList()[writes:])
	}
	if j.State().Dispatch["alpha"].Settled {
		t.Error("the newer record must not be settled by an older call")
	}
	r.AfterDispatch("alpha", schema.KindCompensation, 6, nil)
	writes = len(fs.opList())
	r.AfterDispatch("alpha", schema.KindCompensation, 6, nil) // already settled
	if len(fs.opList()) != writes {
		t.Error("a second mark must not write again")
	}
}

func TestAfterDispatchFailureIsLoggedAndTheUnsettledRecordSurvives(t *testing.T) {
	for _, step := range []string{"create", "write", "fsync", "close", "rename"} {
		t.Run(step, func(t *testing.T) {
			r, j, fs, path, _, lg, _ := newRecorder(t)
			r.BeforeDispatch("alpha", schema.KindForward, 5)
			before := readFileT(t, path)
			fs.failOn[step] = errors.New("injected " + step)
			r.AfterDispatch("alpha", schema.KindForward, 5, nil) // must not panic or return an error
			if readFileT(t, path) != before {
				t.Error("the persisted record was changed or lost")
			}
			d := j.State().Dispatch["alpha"]
			if d.Settled || !d.MayHaveBeenSent || d.Seq != 5 {
				t.Errorf("memory: %+v (must stay the unsettled, may-have-been-sent record)", d)
			}
			if !strings.Contains(lg.joined(), "cannot mark") {
				t.Errorf("failure not logged: %q", lg.joined())
			}
			// later, with the fault gone, it can still be settled
			delete(fs.failOn, step)
			r.AfterDispatch("alpha", schema.KindForward, 5, nil)
			if !j.State().Dispatch["alpha"].Settled {
				t.Error("could not settle after recovery")
			}
		})
	}
}

func TestRecorderIsSafeForConcurrentUse(t *testing.T) {
	r, j, _, path, ids, _, _ := newRecorder(t)
	comps := []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"}
	for _, c := range comps {
		ids.ids[c] = ServiceIdentity{MainPID: 1}
	}
	var wg sync.WaitGroup
	for i, c := range comps {
		wg.Add(1)
		go func(i int, c string) {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				seq := uint64(i*100 + n)
				if _, err := r.BeforeDispatch(c, schema.KindForward, seq); err != nil {
					t.Error(err)
					return
				}
				r.AfterDispatch(c, schema.KindForward, seq, nil)
			}
		}(i, c)
	}
	wg.Wait()
	st := j.State()
	for i, c := range comps {
		d := st.Dispatch[c]
		if d.Seq != uint64(i*100+19) || !d.Settled {
			t.Errorf("%s: %+v", c, d)
		}
	}
	j2, err := openJournal(path, osJournalFS{}, func(string, ...interface{}) {})
	if err != nil {
		t.Fatal(err)
	}
	if len(j2.State().Dispatch) != len(comps) {
		t.Errorf("reloaded %d records", len(j2.State().Dispatch))
	}
}

// ---- identity -------------------------------------------------------------

// A slow identity read (the systemd reader may take up to its 2 s timeout)
// must not consume the schema layer's 2 s send window: the returned record
// time is sampled AFTER the read, i.e. at the start of the persist.
func TestBeforeDispatchSlowIdentityReadStillReturnsAFreshRecordTime(t *testing.T) {
	j, _, _, lg := newTestJournal(t)
	var clock int64 = 10000000
	mono := func() int64 { return atomic.AddInt64(&clock, 10) }
	slow := &slowIDs{advance: func() { atomic.AddInt64(&clock, 3000000) }} // the read takes 3 s
	r := NewJournalRecorder(j, slow, mono)
	_ = lg
	startOfCall := atomic.LoadInt64(&clock)
	tr, err := r.BeforeDispatch("alpha", schema.KindForward, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tr-startOfCall < 3000000 {
		t.Fatalf("t = %d is not after the 3 s identity read (call started at %d)", tr, startOfCall)
	}
	// what the schema layer checks right after BeforeDispatch returns:
	// mono_now - t <= sendWindow (2 s)
	const sendWindowUs = 2000000
	if now := mono(); now-tr > sendWindowUs {
		t.Fatalf("the slow read ate the send window: now-t = %d us", now-tr)
	}
	d := j.State().Dispatch["alpha"]
	if d.TRecordMonoUs != tr || d.Identity.ReadAtMonoUs >= tr {
		t.Errorf("record: %+v (returned t %d)", d, tr)
	}
	// an unreadable identity that also took 3 s behaves the same way
	slow.err = errors.New("timed out")
	tr2, err := r.BeforeDispatch("beta", schema.KindForward, 2)
	if err != nil {
		t.Fatal(err)
	}
	if now := mono(); now-tr2 > sendWindowUs {
		t.Errorf("slow failing read ate the send window: %d", now-tr2)
	}
	if j.State().Dispatch["beta"].Identity.Known {
		t.Error("unreadable identity must be recorded as unknown")
	}
}

type slowIDs struct {
	advance func()
	err     error
}

func (s *slowIDs) Identity(string) (ServiceIdentity, error) {
	s.advance()
	if s.err != nil {
		return ServiceIdentity{}, s.err
	}
	return ServiceIdentity{MainPID: 1, ActiveEnterMonoUs: 2}, nil
}

// The systemd dial is bounded by the read context, but must not be tied to
// it: the connection's life follows the context it was created with, so the
// dial runs in the background and the caller merely stops waiting.
func TestSystemdConnectionDialIsBoundedByTheReadContext(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	dials := 0
	p := &systemdProps{dialFn: func(ctx context.Context) (*systemd.Conn, error) {
		mu.Lock()
		dials++
		mu.Unlock()
		<-release
		return nil, errors.New("systemd not reachable")
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := p.connection(ctx)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the read context deadline", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the read waited for the dial instead of stopping at its deadline")
	}
	// a second reader while the dial is still running shares it
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if _, err := p.connection(ctx2); err == nil {
		t.Fatal("expected a timeout")
	}
	mu.Lock()
	if dials != 1 {
		t.Errorf("dial started %d times, want one shared dial", dials)
	}
	mu.Unlock()
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		running := p.dial != nil
		p.mu.Unlock()
		if !running || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// after the failed dial finished, the next read starts a new one
	release2 := make(chan struct{})
	close(release2)
	p.dialFn = func(ctx context.Context) (*systemd.Conn, error) {
		mu.Lock()
		dials++
		mu.Unlock()
		return nil, errors.New("still unreachable")
	}
	if _, err := p.connection(context.Background()); err == nil || !strings.Contains(err.Error(), "still unreachable") {
		t.Errorf("second dial: %v", err)
	}
	mu.Lock()
	if dials != 2 {
		t.Errorf("dials = %d, want 2", dials)
	}
	mu.Unlock()
}

// The whole identity read, including the connection, shares the 2 s context of
// the reader: a dial that hangs does not hang the dispatch.
func TestSystemdIdentityReaderWithAHangingDialReturnsWithinItsTimeout(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	props := &systemdProps{dialFn: func(ctx context.Context) (*systemd.Conn, error) {
		<-block
		return nil, errors.New("late")
	}}
	r := &SystemdIdentityReader{props: props, timeout: 50 * time.Millisecond,
		unitFor: func(string) string { return "u" }}
	start := time.Now()
	id, err := r.Identity("c")
	if err == nil || id.Known {
		t.Fatalf("%+v %v", id, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the identity read did not stop at its own timeout")
	}
}

func TestIdentityJSONRoundTripAndUnknown(t *testing.T) {
	known := ServiceIdentity{Known: true, MainPID: 5, ActiveEnterMonoUs: 6, ReadAtMonoUs: 7}
	b, err := json.Marshal(known)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"main_pid":5,"active_enter_mono_us":6,"read_at_mono_us":7}` {
		t.Errorf("known: %s", b)
	}
	var back ServiceIdentity
	if err := json.Unmarshal(b, &back); err != nil || back != known {
		t.Errorf("round trip: %+v %v", back, err)
	}
	b, _ = json.Marshal(ServiceIdentity{})
	if string(b) != `"unknown-identity"` {
		t.Errorf("unknown: %s", b)
	}
	back = ServiceIdentity{Known: true}
	if err := json.Unmarshal(b, &back); err != nil || back.Known {
		t.Errorf("unknown round trip: %+v %v", back, err)
	}
	if err := json.Unmarshal([]byte(`"something-else"`), &back); err == nil {
		t.Error("an unexpected identity string must be rejected")
	}
}

type fakeProps struct {
	pid, enter  uint64
	pidErr      error
	enterErr    error
	units       []string
	sawDeadline bool
}

func (f *fakeProps) MainPID(ctx context.Context, unit string) (uint64, error) {
	f.units = append(f.units, unit)
	if _, ok := ctx.Deadline(); ok {
		f.sawDeadline = true
	}
	return f.pid, f.pidErr
}
func (f *fakeProps) ActiveEnterMonotonicUs(ctx context.Context, unit string) (uint64, error) {
	return f.enter, f.enterErr
}

func TestSystemdIdentityReaderMapsUnitsAndBoundsTheRead(t *testing.T) {
	props := &fakeProps{pid: 321, enter: 987654}
	r := &SystemdIdentityReader{props: props, timeout: 1,
		unitFor: func(c string) string {
			return map[string]string{"net.vyatta.vci.kdump.v1": "vci-kdump", "x": "other.service"}[c]
		}}
	id, err := r.Identity("net.vyatta.vci.kdump.v1")
	if err != nil || !id.Known || id.MainPID != 321 || id.ActiveEnterMonoUs != 987654 {
		t.Fatalf("%+v %v", id, err)
	}
	if props.units[0] != "vci-kdump.service" {
		t.Errorf("unit = %q", props.units[0])
	}
	if !props.sawDeadline {
		t.Error("the read must be bounded by a context deadline")
	}
	r.Identity("x")
	if props.units[1] != "other.service" {
		t.Errorf("unit = %q", props.units[1])
	}
}

func TestSystemdIdentityReaderErrorsAreErrorsNotZeroIdentities(t *testing.T) {
	unit := func(string) string { return "u" }
	for name, p := range map[string]*fakeProps{
		"MainPID":     {pidErr: errors.New("no such unit")},
		"ActiveEnter": {enterErr: errors.New("timeout")},
	} {
		r := &SystemdIdentityReader{props: p, unitFor: unit, timeout: 1}
		if id, err := r.Identity("c"); err == nil || id.Known {
			t.Errorf("%s: %+v %v", name, id, err)
		}
	}
	r := &SystemdIdentityReader{props: &fakeProps{}, unitFor: func(string) string { return "" }, timeout: 1}
	if _, err := r.Identity("c"); err == nil {
		t.Error("an unmapped component has no identity")
	}
}

func TestJournalPathIsNextToTheRunfileAndNeverImplicit(t *testing.T) {
	if got := JournalPathFor("/var/run/vyatta/configd/running.config"); got != "/var/run/vyatta/configd/commit-state.json" {
		t.Errorf("path = %s", got)
	}
	if got := JournalPathFor("session_test.runfile"); got != "commit-state.json" {
		t.Errorf("relative runfile: %s", got)
	}
}
