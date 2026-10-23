// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-B2 tests: CommitMgr.commit wired to the commit journal and the
// dispatch recorder. The journal lives in a test-private directory next to a
// test-private runfile; the recorder is a stub, so no host /var/run and no
// systemd is involved. Components are the scripted failingOps of the phase-2
// tests under the real compMgr.

package session_test

import (
	"bytes"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danos/config/schema"
	"github.com/danos/configd/session"
	"github.com/danos/configd/session/sessiontest"
	"github.com/danos/mgmterror"
	"github.com/danos/utils/exec"
	"github.com/danos/vci/conf"
	"github.com/danos/yang/data/datanode"
	"github.com/danos/yang/testutils"
)

type stubRecorder struct {
	mu         sync.Mutex
	before     int
	after      int
	beforeErr  error
	onAfter    func()
	afterFired bool
}

func (r *stubRecorder) BeforeDispatch(string, schema.DispatchKind, uint64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.before++
	if r.beforeErr != nil {
		return 0, r.beforeErr
	}
	return schema.MonotonicNowUs(), nil
}

func (r *stubRecorder) AfterDispatch(string, schema.DispatchKind, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.after++
	if r.onAfter != nil && !r.afterFired {
		r.afterFired = true
		r.onAfter()
	}
}

// checkedOnly hides every method of the real manager that is not in
// schema.ComponentManager, so it does not implement CompensatingComponentManager.
type checkedOnly struct {
	schema.ComponentManager
	inner        schema.CheckedComponentManager
	checkedCalls int
}

func (c *checkedOnly) ComponentSetRunningChecked(m schema.ModelSet, n datanode.DataNode,
	ch *map[string]bool, l schema.CommitTimeLogFn) ([]*exec.Output, []error) {
	c.checkedCalls++
	return c.inner.ComponentSetRunningChecked(m, n, ch, l)
}

type journalRun struct {
	outs     []*exec.Output
	errs     []error
	ok       bool
	calls    []string
	log      string
	runfile  string
	before   string // runfileState before the commit
	srv      *sessiontest.TstSrv
	sess     *session.Session
	applied  map[string]string // config each component accepted
	payloads map[string][]string
}

func (r *journalRun) runfileSame() bool { return runfileState(r.runfile) == r.before }

func (r *journalRun) journalPath() string { return session.JournalPathFor(r.runfile) }

func (r *journalRun) journalState(t *testing.T) session.JournalState {
	j, err := session.OpenJournal(r.journalPath())
	if err != nil {
		t.Fatalf("open journal %s: %v", r.journalPath(), err)
	}
	return j.State()
}

// journalCommit runs one commit in a fresh session. setup may replace the
// component manager and wire the journal seams before the commit.
func journalCommit(t *testing.T, fail map[string]error,
	setup func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager))) *journalRun {
	t.Helper()
	return journalCommitCfg(t, emptyConfig, config, false, fail, setup)
}

// journalCommitCfg is journalCommit with its own running configuration and
// candidate, optionally with a fourth component that has no ordering edges.
func journalCommitCfg(t *testing.T, running, candidate string, withFourth bool, fail map[string]error,
	setup func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager))) *journalRun {
	t.Helper()
	return journalCommitOpts(t, running, candidate, withFourth, fail, false, setup)
}

// journalCommitOpts also lets the scripted failure apply to the first call only.
func journalCommitOpts(t *testing.T, running, candidate string, withFourth bool, fail map[string]error,
	failOnce bool, setup func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager))) *journalRun {
	t.Helper()
	sch, comps := schemas, []string{
		firstTestComp.String(), secondTestComp.String(), thirdTestComp.String()}
	if withFourth {
		sch = append(append([]*sessiontest.TestSchema(nil), schemas...),
			sessiontest.NewTestSchema("vyatta-test-fourth-v1", "fourth").
				AddSchemaSnippet(fourthSchema))
		comps = append(comps, fourthTestComp.String())
		if journalWithFifth {
			sch = append(sch, sessiontest.NewTestSchema("vyatta-test-fifth-v1", "fifth").
				AddSchemaSnippet(fifthSchema))
			comps = append(comps, fifthTestComp.String())
		}
	}
	ts := sessiontest.NewTestSpec(t).
		SetSchemaDefsByRef(sch).
		SetComponents(conf.BaseModelSet, comps).
		SetConfig(running)
	srv, sess := ts.Init()
	runfile := filepath.Join(t.TempDir(), "session_test.runfile")
	srv.Ctx.Config.Runfile = runfile

	tcm := ts.GetCompMgr()
	ops := &failingOps{OperationsManager: tcm, results: fail, failOnce: failOnce}
	srv.Ctx.CompMgr = schema.NewCompMgr(ops, tcm, srv.MsFull, tcm.GetComponentNSMappings())
	var logbuf bytes.Buffer
	srv.Ctx.Dlog = log.New(&logbuf, "", 0)

	if setup != nil {
		setup(srv, func(cm schema.ComponentManager) { srv.Ctx.CompMgr = cm })
	}
	srv.LoadConfig(t, candidate, sess)
	r := &journalRun{runfile: runfile, before: runfileState(runfile), srv: srv, sess: sess}
	logbuf.Reset()
	if journalReconcile {
		var outs []*exec.Output
		var err error
		if journalStartup {
			outs, err = srv.Cmgr.StartupPass(srv.Ctx)
		} else {
			outs, err = srv.Cmgr.Reconcile(srv.Ctx)
		}
		r.outs, r.ok = outs, err == nil
		if err != nil {
			r.errs = []error{err}
		}
	} else {
		r.outs, r.errs, r.ok = sess.Commit(srv.Ctx, "message", false)
	}
	r.log = logbuf.String()
	r.calls = ops.calls
	r.applied = ops.applied
	r.payloads = ops.payloads
	return r
}

func (r *journalRun) noLaterStages(t *testing.T) {
	t.Helper()
	if strings.Contains(r.log, logCommitScripts) {
		t.Errorf("commit scripts ran")
	}
	if strings.Contains(r.log, logPostHooks) {
		t.Errorf("post-hooks ran")
	}
	if !r.runfileSame() {
		t.Errorf("the running file was written")
	}
	for _, v := range []string{"someValue", "anotherValue"} {
		if strings.Contains(serialiseCfg(r.srv.Cmgr.Running(), r.srv.Ms), v) {
			t.Errorf("running advanced (has %q)", v)
		}
	}
}

// (1) W1 cannot be written: nothing is called, nothing after it runs.
func TestCommitJournalW1FailureFailsClosed(t *testing.T) {
	failing := true
	rec := &stubRecorder{}
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetJournalOpener(srv.Cmgr, session.FaultOpener(&failing, "create"))
		session.SetDispatchRecorder(srv.Cmgr, rec)
	})
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must fail; ok=%v errs=%v", r.ok, r.errs)
	}
	if !strings.Contains(r.errs[0].Error(), "no component was called") {
		t.Errorf("error does not say that no component was called: %v", r.errs)
	}
	if len(r.calls) != 0 || rec.before != 0 {
		t.Errorf("a component was called after the W1 failure: calls=%v before=%d", r.calls, rec.before)
	}
	r.noLaterStages(t)
	if _, err := os.Stat(r.journalPath()); err == nil {
		t.Errorf("a journal file exists although every write failed")
	}
}

// (2) The recorder refuses to persist a dispatch record: the call is not made.
func TestCommitJournalBeforeDispatchErrorFailsClosed(t *testing.T) {
	rec := &stubRecorder{beforeErr: errors.New("journal disk full")}
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetDispatchRecorder(srv.Cmgr, rec)
	})
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must fail; ok=%v errs=%v", r.ok, r.errs)
	}
	if rec.before == 0 {
		t.Fatalf("the recorder was never asked")
	}
	if len(r.calls) != 0 {
		t.Errorf("a component was called although its record was not durable: %v", r.calls)
	}
	r.noLaterStages(t)
	if st := r.journalState(t); st.Commit != nil {
		t.Errorf("the commit record is still open after the failed commit: %+v", st.Commit)
	}
}

// (3) A manager that is only a CheckedComponentManager keeps the phase-2
// path and creates no journal.
func TestCommitCheckedOnlyManagerUsesCheckedPathWithoutJournal(t *testing.T) {
	var co *checkedOnly
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager)) {
		real := srv.Ctx.CompMgr
		co = &checkedOnly{ComponentManager: real, inner: real.(schema.CheckedComponentManager)}
		if _, ok := schema.ComponentManager(co).(schema.CompensatingComponentManager); ok {
			t.Fatalf("harness: the wrapper must not be a CompensatingComponentManager")
		}
		wrap(co)
	})
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if co.checkedCalls != 1 {
		t.Errorf("ComponentSetRunningChecked calls = %d, want 1", co.checkedCalls)
	}
	if _, err := os.Stat(r.journalPath()); !os.IsNotExist(err) {
		t.Errorf("the checked path created a journal file (stat err=%v)", err)
	}
	if r.runfileSame() {
		t.Errorf("a successful commit must write the running file")
	}
}

// (4) Success: existing order is kept and W2 clears the commit record.
func TestCommitJournalSuccessWritesW2(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetDispatchRecorder(srv.Cmgr, rec)
	})
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) == 0 || rec.before != len(r.calls) {
		t.Errorf("every component call needs a record first: calls=%v before=%d", r.calls, rec.before)
	}
	if !strings.Contains(r.log, logCommitScripts) || !strings.Contains(r.log, logPostHooks) {
		t.Errorf("commit scripts / post-hooks did not run")
	}
	if r.runfileSame() {
		t.Errorf("the running file was not written")
	}
	st := r.journalState(t)
	if st.Commit != nil {
		t.Errorf("W2 left a commit record: %+v", st.Commit)
	}
	if len(st.Dirty) != 0 {
		t.Errorf("a clean commit left dirty entries: %+v", st.Dirty)
	}
	// The runfile is the rendering of the new running tree, byte for byte what
	// Show returns once that tree is stored.
	if want, err := r.sess.Show(r.srv.Ctx, []string{}, false, false); err != nil {
		t.Errorf("show: %v", err)
	} else if got, _ := os.ReadFile(r.runfile); string(got) != want {
		t.Errorf("runfile differs from Show of the running tree:\n%q\n%q", got, want)
	}
	raw, err := os.ReadFile(r.journalPath())
	if err != nil || !strings.Contains(string(raw), `"commit":null`) {
		t.Errorf("journal file lacks \"commit\":null (err=%v): %s", err, raw)
	}
	if len(allOutputText(r.outs)) > 0 && strings.Contains(allOutputText(r.outs), "WARNING") {
		t.Errorf("unexpected warning: %s", allOutputText(r.outs))
	}
}

// (5) A component fails: the schema call has compensated; nothing after it
// runs, W2 clears the commit record and the dirty set comes from the report.
func TestCommitJournalComponentFailureDoesNotReachRunning(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommit(t, map[string]error{compThird: errors.New("third down")},
		func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
			session.SetDispatchRecorder(srv.Cmgr, rec)
		})
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must fail; ok=%v errs=%v", r.ok, r.errs)
	}
	if !strings.Contains(r.errs[0].Error(), compThird) {
		t.Errorf("error does not name the failing component: %v", r.errs)
	}
	r.noLaterStages(t)
	st := r.journalState(t)
	if st.Commit != nil {
		t.Errorf("W2 left a commit record: %+v", st.Commit)
	}
	if len(st.Dirty) == 0 {
		t.Errorf("W2 did not record the dirty set from the report")
	}
	for c, d := range st.Dirty {
		if d.CommitID == "" || d.Class == "" {
			t.Errorf("dirty entry %s incomplete: %+v", c, d)
		}
	}
}

// (6) W2 cannot be written after a successful persist: the commit stays
// successful and the response carries a specific warning, not an error.
// Journal writes of a commit: W1, "persisting", W2 (the stub recorder writes none).
func TestCommitJournalW2FailureWarnsButKeepsCommit(t *testing.T) {
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetJournalOpener(srv.Cmgr, session.FaultOpenerFromRename(3))
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
	})
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("a W2 failure must not fail the commit: ok=%v errs=%v", r.ok, r.errs)
	}
	if !strings.Contains(allOutputText(r.outs), "the commit journal could not be updated (end of the commit)") {
		t.Errorf("no W2 warning in the response: %q", allOutputText(r.outs))
	}
	if r.runfileSame() || !r.runningAdvanced() {
		t.Errorf("the successful apply was not kept")
	}
}

// countingComp counts CompensateReport calls of the real manager.
type countingComp struct {
	schema.ComponentManager
	schema.CompensatingComponentManager
	compensated int
}

func (c *countingComp) CompensateReport(m schema.ModelSet, prev func() datanode.DataNode,
	applied *schema.ApplyReport, pol schema.CompensationPolicy, rec schema.DispatchRecorder,
	l schema.CommitTimeLogFn) (*schema.ApplyReport, []error) {
	c.compensated++
	return c.CompensatingComponentManager.CompensateReport(m, prev, applied, pol, rec, l)
}

func withCounting(cc **countingComp) func(*sessiontest.TstSrv, func(schema.ComponentManager)) {
	return func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager)) {
		real := srv.Ctx.CompMgr
		*cc = &countingComp{ComponentManager: real,
			CompensatingComponentManager: real.(schema.CompensatingComponentManager)}
		wrap(*cc)
	}
}

func (r *journalRun) runningAdvanced() bool {
	run := serialiseCfg(r.srv.Cmgr.Running(), r.srv.Ms)
	return strings.Contains(run, "someValue") || strings.Contains(run, "anotherValue")
}

func (r *journalRun) checkPersistFailed(t *testing.T, step string, cc *countingComp) {
	t.Helper()
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must fail; ok=%v errs=%v", r.ok, r.errs)
	}
	msg := r.errs[0].Error()
	if !strings.Contains(msg, "could not persist the running configuration: "+step+": ") {
		t.Errorf("first error lacks the persist step %q: %s", step, msg)
	}
	if len(r.errs) < 2 || !strings.Contains(r.errs[1].Error(), "compensation") {
		t.Errorf("the response lacks the compensation report: %v", r.errs)
	}
	if !r.runfileSame() {
		t.Errorf("the runfile changed")
	}
	if r.runningAdvanced() {
		t.Errorf("running was not restored / advanced")
	}
	if strings.Contains(r.log, logPostHooks) {
		t.Errorf("post-hooks ran")
	}
	if cc.compensated != 1 {
		t.Errorf("CompensateReport calls = %d, want 1", cc.compensated)
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(r.runfile), "*.tmp")); len(left) != 0 {
		t.Errorf("temp file left behind: %v", left)
	}
}

// (B3-1) create/write/fsync/rename failures of the runfile persist.
func TestCommitPersistFailureCompensatesAndRestoresRunning(t *testing.T) {
	for _, step := range []string{"create", "write", "fsync", "rename"} {
		t.Run(step, func(t *testing.T) {
			var cc *countingComp
			counting := withCounting(&cc)
			r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager)) {
				counting(srv, wrap)
				session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
				session.SetRunfileFaults(srv.Cmgr, step)
			})
			r.checkPersistFailed(t, step, cc)
			st := r.journalState(t)
			if st.Commit != nil {
				t.Errorf("W2 left a commit record: %+v", st.Commit)
			}
			if len(st.Dirty) == 0 {
				t.Errorf("W2 did not record the dirty set of the compensation report")
			}
		})
	}
}

// (B3-1b) the journal cannot record "persisting": handled as a persist failure,
// and the W2 failure after it stays a warning that does not hide the failure.
func TestCommitPersistingRecordFailureCompensatesAndWarns(t *testing.T) {
	var cc *countingComp
	counting := withCounting(&cc)
	// journal writes: W1 ok, "persisting" fails, the W2 after the compensation fails too
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager)) {
		counting(srv, wrap)
		session.SetJournalOpener(srv.Cmgr, session.FaultOpenerFromRename(2))
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
	})
	r.checkPersistFailed(t, "journal", cc)
	if !strings.Contains(allOutputText(r.outs), "the commit journal could not be updated") {
		t.Errorf("no journal warning next to the persist failure: %q", allOutputText(r.outs))
	}
}

// (B3-2) directory fsync failure after a successful rename.
func TestCommitPersistDirSyncFailureIsSuccessWithWarning(t *testing.T) {
	var cc *countingComp
	counting := withCounting(&cc)
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, wrap func(schema.ComponentManager)) {
		counting(srv, wrap)
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		session.SetRunfileFaults(srv.Cmgr, "syncdir")
	})
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("a directory sync failure must not fail the commit: ok=%v errs=%v", r.ok, r.errs)
	}
	if !strings.Contains(allOutputText(r.outs), "directory sync failed") {
		t.Errorf("no directory-sync warning: %q", allOutputText(r.outs))
	}
	if !r.runningAdvanced() {
		t.Errorf("running must be the new tree")
	}
	if r.runfileSame() {
		t.Errorf("the new runfile must be kept")
	}
	if cc.compensated != 0 {
		t.Errorf("a kept commit was compensated")
	}
	if !strings.Contains(r.log, logPostHooks) {
		t.Errorf("post-hooks did not run")
	}
	st := r.journalState(t)
	if st.Commit == nil || st.Commit.State != "persisted_unsynced" {
		t.Errorf("commit.state = %+v, want persisted_unsynced", st.Commit)
	}
}

// ---- stage 3-B4: ordinary-commit fence gate -------------------------------

// seedUnfencedCall records an earlier call to compSecond whose outcome is
// unknown: dirty "unknown", a sent and unsettled dispatch at t.
func seedUnfencedCall(runfile string, t int64) error {
	return seedUnfencedComp(runfile, compSecond, t)
}

func seedUnfencedComp(runfile, comp string, t int64) error {
	return session.SeedJournal(runfile, func(s *session.JournalState) {
		s.Dirty[comp] = session.JournalDirty{Class: schema.DirtyUnknown, CommitID: "old"}
		s.Dispatch[comp] = session.JournalDispatch{Seq: 1, Kind: schema.KindForward,
			MayHaveBeenSent: true, TRecordMonoUs: t}
	})
}

func fenceSetup(t *testing.T, enterAfterRecord int64, rec *stubRecorder) func(*sessiontest.TstSrv, func(schema.ComponentManager)) {
	return fenceSetupFor(t, compSecond, enterAfterRecord, rec)
}

func fenceSetupFor(t *testing.T, comp string, enterAfterRecord int64, rec *stubRecorder) func(*sessiontest.TstSrv, func(schema.ComponentManager)) {
	const recordT = int64(5_000_000)
	return func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		if err := seedUnfencedComp(srv.Ctx.Config.Runfile, comp, recordT); err != nil {
			t.Fatal(err)
		}
		session.SetIdentityReader(srv.Cmgr, session.IdentityFunc(
			func(string) (session.ServiceIdentity, error) {
				return session.ServiceIdentity{Known: true, MainPID: 9,
					ActiveEnterMonoUs: recordT + enterAfterRecord}, nil
			}))
		session.SetDispatchRecorder(srv.Cmgr, rec)
	}
}

// (B4-2) An unfenced component in todo (also changed by this commit) fails the
// commit before pre-hooks, W1 and any component call; the journal is not written.
func TestCommitFenceGateRefusesUnfencedComponentInTodo(t *testing.T) {
	rec := &stubRecorder{}
	setup := fenceSetup(t, 1_000_000 /* inside the window */, rec)
	var journalBefore []byte
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, w func(schema.ComponentManager)) {
		setup(srv, w)
		journalBefore, _ = os.ReadFile(session.JournalPathFor(srv.Ctx.Config.Runfile))
	})
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must be refused; ok=%v errs=%v", r.ok, r.errs)
	}
	msg := r.errs[0].Error()
	for _, need := range []string{compSecond, "may still be executing an earlier Set", "not fenced", "restart"} {
		if !strings.Contains(msg, need) {
			t.Errorf("error lacks %q: %s", need, msg)
		}
	}
	if len(r.calls) != 0 || rec.before != 0 {
		t.Errorf("a component was called: calls=%v records=%d", r.calls, rec.before)
	}
	if count(r.log, logPreHooks) != 0 {
		t.Errorf("pre-hooks ran")
	}
	r.noLaterStages(t)
	after, _ := os.ReadFile(r.journalPath())
	if string(after) != string(journalBefore) {
		t.Errorf("the journal was written:\nbefore %s\nafter  %s", journalBefore, after)
	}
}

// (B4-3) The same component, fenced by a later restart, takes the normal
// journaled success path.
func TestCommitFenceGateAllowsComponentFencedByLaterIdentity(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommit(t, nil, fenceSetup(t, 3_000_001 /* beyond window + margin */, rec))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) == 0 || rec.before != len(r.calls) {
		t.Errorf("journaled path not taken: calls=%v records=%d", r.calls, rec.before)
	}
	if count(r.log, logPreHooks) != 1 || count(r.log, logPostHooks) != 1 {
		t.Errorf("hooks did not run once each")
	}
	st := r.journalState(t)
	if st.Commit != nil || len(st.Dirty) != 0 {
		t.Errorf("W2 state wrong: commit=%+v dirty=%+v", st.Commit, st.Dirty)
	}
}

// ---- stage 3-B5: blocking dirty components extend the fence set -----------

const (
	compFourth = "net.vyatta.test.fourth"

	fourthSchema = `
container fourth {
	leaf fourthLeaf {
		type string;
	}
}`
)

// The fourth component has no Before/After edge: independent of the others.
var fourthTestComp = conf.CreateTestDotComponentFile("fourth").AddBaseModel()

// Apply order is second, third, first (third: after second, before first), so
// second is a prerequisite of first. Running holds second only; the candidate
// adds first, so this commit changes first and nothing else.
var (
	runningSecondOnly = testutils.Root(
		testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))
	candidateChangesFirst = testutils.Root(
		testutils.Cont("first", testutils.Leaf("firstLeaf", "someValue")),
		testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))
)

// (B5-1) An unfenced dirty prerequisite that this commit does not change
// refuses the commit before anything happens.
func TestCommitFenceGateRefusesUnfencedDirtyPrerequisite(t *testing.T) {
	rec := &stubRecorder{}
	setup := fenceSetupFor(t, compSecond, 1_000_000 /* inside the window */, rec)
	var journalBefore []byte
	r := journalCommitCfg(t, runningSecondOnly, candidateChangesFirst, false, nil,
		func(srv *sessiontest.TstSrv, w func(schema.ComponentManager)) {
			setup(srv, w)
			journalBefore, _ = os.ReadFile(session.JournalPathFor(srv.Ctx.Config.Runfile))
		})
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must be refused; ok=%v errs=%v", r.ok, r.errs)
	}
	msg := r.errs[0].Error()
	for _, need := range []string{compSecond, "prerequisite", "not fenced", "restart"} {
		if !strings.Contains(msg, need) {
			t.Errorf("error lacks %q: %s", need, msg)
		}
	}
	if len(r.calls) != 0 || rec.before != 0 {
		t.Errorf("a component was called: calls=%v records=%d", r.calls, rec.before)
	}
	if count(r.log, logPreHooks) != 0 || strings.Contains(r.log, logCommitScripts) ||
		strings.Contains(r.log, logPostHooks) {
		t.Errorf("a hook or script ran")
	}
	if !r.runfileSame() {
		t.Errorf("the runfile was written")
	}
	if after, _ := os.ReadFile(r.journalPath()); string(after) != string(journalBefore) {
		t.Errorf("the journal was written:\nbefore %s\nafter  %s", journalBefore, after)
	}
}

// (B5-2/B6-1) The same prerequisite, fenced by a later restart, passes the
// gate and is re-applied through the forced argument, before the changed
// component, with its running configuration.
func TestCommitFenceGateAllowsFencedDirtyPrerequisite(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommitCfg(t, runningSecondOnly, candidateChangesFirst, false, nil,
		fenceSetupFor(t, compSecond, 3_000_001 /* beyond window + margin */, rec))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) == 0 || rec.before != len(r.calls) {
		t.Errorf("journaled path not taken: calls=%v records=%d", r.calls, rec.before)
	}
	if countOf(r.calls, compThird) != 0 {
		t.Errorf("a prerequisite-only commit must not force third: %v", r.calls)
	}
	iSecond, iFirst := -1, -1
	for i, c := range r.calls {
		switch c {
		case compSecond:
			iSecond = i
		case compFirst:
			iFirst = i
		}
	}
	if iSecond < 0 || iFirst < 0 || iSecond > iFirst {
		t.Errorf("the prerequisite must be called before the changed component: %v", r.calls)
	}
	if !strings.Contains(r.applied[compSecond], "secondLeaf") ||
		!strings.Contains(r.applied[compSecond], "someValue") {
		t.Errorf("the prerequisite did not receive its running config: %q", r.applied[compSecond])
	}
	if count(r.log, logPreHooks) != 1 || count(r.log, logPostHooks) != 1 {
		t.Errorf("hooks did not run once each")
	}
	st := r.journalState(t)
	if st.Commit != nil {
		t.Errorf("W2 left a commit record: %+v", st.Commit)
	}
	if _, still := st.Dirty[compSecond]; still {
		t.Errorf("the applied prerequisite is still dirty: %+v", st.Dirty)
	}
}

// (B6-2) The re-apply of the prerequisite fails: the commit fails closed, the
// changed component is not called, the repair step is neither sent prevCfg nor
// treated as clean, and the prerequisite stays dirty. Run twice: with a
// component whose every Set fails, and with one whose SECOND Set would succeed
// (a compensation call would then succeed and wrongly clean it).
func TestCommitForcedPrerequisiteFailureFailsClosed(t *testing.T) {
	for _, once := range []bool{false, true} {
		name := "every Set fails"
		if once {
			name = "a second Set would succeed"
		}
		t.Run(name, func(t *testing.T) {
			rec := &stubRecorder{}
			r := journalCommitOpts(t, runningSecondOnly, candidateChangesFirst, false,
				map[string]error{compSecond: errors.New("second down")}, once,
				fenceSetupFor(t, compSecond, 3_000_001, rec))
			if r.ok || len(r.errs) == 0 {
				t.Fatalf("commit must fail; ok=%v errs=%v", r.ok, r.errs)
			}
			if len(r.calls) != 1 || r.calls[0] != compSecond {
				t.Fatalf("exactly one forward call to the prerequisite expected: %v", r.calls)
			}
			if rec.before != 1 {
				t.Errorf("dispatch records = %d, want 1", rec.before)
			}
			if p := r.payloads[compSecond]; len(p) != 1 ||
				!strings.Contains(p[0], "secondLeaf") || !strings.Contains(p[0], "someValue") {
				t.Errorf("payload is not the running config: %q", p)
			}
			if strings.Contains(r.log, logCommitScripts) || strings.Contains(r.log, logPostHooks) {
				t.Errorf("commit scripts or post-hooks ran")
			}
			if !r.runfileSame() {
				t.Errorf("the runfile was written")
			}
			if strings.Contains(serialiseCfg(r.srv.Cmgr.Running(), r.srv.Ms), "firstLeaf") {
				t.Errorf("running advanced to the candidate")
			}
			st := r.journalState(t)
			if st.Commit != nil {
				t.Errorf("the commit record was not closed: %+v", st.Commit)
			}
			if _, dirty := st.Dirty[compSecond]; !dirty {
				t.Errorf("the failed prerequisite is no longer dirty: %+v", st.Dirty)
			}
			if !strings.Contains(r.errs[0].Error(), compSecond) {
				t.Errorf("error does not name the prerequisite: %v", r.errs)
			}
		})
	}
}

// (B5-3) An unfenced dirty component that is independent of the changed set
// does not fail the gate.
func TestCommitFenceGateIgnoresUnfencedIndependentDirtyComponent(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommitCfg(t, runningSecondOnly, candidateChangesFirst, true, nil,
		fenceSetupFor(t, compFourth, 1_000_000 /* inside the window: unfenced */, rec))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("an independent unfenced component must not fail the commit: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) == 0 || rec.before != len(r.calls) {
		t.Errorf("journaled path not taken: calls=%v records=%d", r.calls, rec.before)
	}
	for _, c := range r.calls {
		if c == compFourth {
			t.Errorf("the independent component was applied: %v", r.calls)
		}
	}
}

// ---- stage 3-B7: a fenced dirty bridge is re-applied through forced --------

// Running holds only third's configuration, the candidate (`config`) adds
// first's and second's: C = {second, first}; third (after second, before
// first) is in Anc(C) and Desc(C): a bridge when dirty.
var runningThirdOnly = testutils.Root(
	testutils.Cont("first",
		testutils.Cont("third", testutils.Leaf("thirdLeaf", "anotherValue"))))

func indexOf(calls []string, c string) int {
	for i, x := range calls {
		if x == c {
			return i
		}
	}
	return -1
}

func countOf(calls []string, c string) int {
	n := 0
	for _, x := range calls {
		if x == c {
			n++
		}
	}
	return n
}

// (B7-1) Success: the bridge is called once, between second and first, with
// its running configuration, and is clean afterwards.
func TestCommitFencedDirtyBridgeIsReappliedBetweenChangedComponents(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommitCfg(t, runningThirdOnly, config, false, nil,
		fenceSetupFor(t, compThird, 3_000_001, rec))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) != 3 || r.calls[0] != compSecond || r.calls[1] != compThird || r.calls[2] != compFirst {
		t.Fatalf("calls = %v, want [second third first] with the bridge called once", r.calls)
	}
	if rec.before != 3 {
		t.Errorf("dispatch records = %d, want 3", rec.before)
	}
	if p := r.payloads[compThird]; len(p) != 1 ||
		!strings.Contains(p[0], "thirdLeaf") || !strings.Contains(p[0], "anotherValue") {
		t.Errorf("the bridge did not receive its running config: %q", p)
	}
	if count(r.log, logPreHooks) != 1 || count(r.log, logPostHooks) != 1 {
		t.Errorf("hooks did not run once each")
	}
	st := r.journalState(t)
	if st.Commit != nil {
		t.Errorf("W2 left a commit record: %+v", st.Commit)
	}
	if _, still := st.Dirty[compThird]; still {
		t.Errorf("the re-applied bridge is still dirty: %+v", st.Dirty)
	}
}

// (B7-2) The bridge's Set fails: closed failure, first not called, second (a
// changed component that was applied) compensated with prevCfg, the bridge
// called exactly once and never sent prevCfg, and it stays dirty.
func TestCommitFencedDirtyBridgeFailureFailsClosedWithoutBridgeCompensation(t *testing.T) {
	for _, once := range []bool{false, true} {
		name := "every Set fails"
		if once {
			name = "a second Set would succeed"
		}
		t.Run(name, func(t *testing.T) {
			rec := &stubRecorder{}
			r := journalCommitOpts(t, runningThirdOnly, config, false,
				map[string]error{compThird: errors.New("third down")}, once,
				fenceSetupFor(t, compThird, 3_000_001, rec))
			if r.ok || len(r.errs) == 0 {
				t.Fatalf("commit must fail; ok=%v errs=%v", r.ok, r.errs)
			}
			if n := countOf(r.calls, compThird); n != 1 {
				t.Errorf("the bridge must be called exactly once, got %d: %v", n, r.calls)
			}
			if countOf(r.calls, compFirst) != 0 {
				t.Errorf("first must not be called: %v", r.calls)
			}
			if p := r.payloads[compThird]; len(p) != 1 || !strings.Contains(p[0], "anotherValue") {
				t.Errorf("the bridge payload is not its running config: %q", p)
			}
			// second: forward, then its compensation with the previous config
			p := r.payloads[compSecond]
			if len(p) != 2 || !strings.Contains(p[0], "someValue") || strings.Contains(p[1], "someValue") {
				t.Errorf("second must be applied and then compensated with prevCfg: %q", p)
			}
			if strings.Contains(r.log, logCommitScripts) || strings.Contains(r.log, logPostHooks) {
				t.Errorf("commit scripts or post-hooks ran")
			}
			if !r.runfileSame() {
				t.Errorf("the runfile was written")
			}
			if run := serialiseCfg(r.srv.Cmgr.Running(), r.srv.Ms); strings.Contains(run, "firstLeaf") ||
				strings.Contains(run, "secondLeaf") {
				t.Errorf("running advanced to the candidate")
			}
			st := r.journalState(t)
			if st.Commit != nil {
				t.Errorf("the commit record was not closed: %+v", st.Commit)
			}
			if _, dirty := st.Dirty[compThird]; !dirty {
				t.Errorf("the failed bridge is no longer dirty: %+v", st.Dirty)
			}
		})
	}
}

// (B7-3) An unfenced dirty bridge is refused before any call.
func TestCommitFenceGateRefusesUnfencedDirtyBridge(t *testing.T) {
	rec := &stubRecorder{}
	setup := fenceSetupFor(t, compThird, 1_000_000 /* inside the window */, rec)
	var journalBefore []byte
	r := journalCommitCfg(t, runningThirdOnly, config, false, nil,
		func(srv *sessiontest.TstSrv, w func(schema.ComponentManager)) {
			setup(srv, w)
			journalBefore, _ = os.ReadFile(session.JournalPathFor(srv.Ctx.Config.Runfile))
		})
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must be refused; ok=%v errs=%v", r.ok, r.errs)
	}
	if msg := r.errs[0].Error(); !strings.Contains(msg, compThird) || !strings.Contains(msg, "bridge") ||
		!strings.Contains(msg, "not fenced") {
		t.Errorf("error does not name the unfenced bridge: %s", msg)
	}
	if len(r.calls) != 0 || rec.before != 0 || count(r.log, logPreHooks) != 0 {
		t.Errorf("something ran: calls=%v records=%d", r.calls, rec.before)
	}
	if after, _ := os.ReadFile(r.journalPath()); string(after) != string(journalBefore) {
		t.Errorf("the journal was written")
	}
}

// ---- stage 3-B8: a fenced dirty dependant is re-applied through forced -----

// Running holds only third; the candidate keeps third's leaf and adds only
// secondLeaf: C = {second}; third (after second) is in Desc(C), and not in
// Anc(C) because first is not changed: a dependant when dirty.
var candidateChangesSecondOnly = testutils.Root(
	testutils.Cont("first",
		testutils.Cont("third", testutils.Leaf("thirdLeaf", "anotherValue"))),
	testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))

// (B8-3a) The fixture classifies third as a dependant: proven by the refusal
// text of an unfenced dependant, which is not called and leaves the journal alone.
func TestCommitFenceGateRefusesUnfencedDirtyDependant(t *testing.T) {
	rec := &stubRecorder{}
	setup := fenceSetupFor(t, compThird, 1_000_000 /* inside the window */, rec)
	var journalBefore []byte
	r := journalCommitCfg(t, runningThirdOnly, candidateChangesSecondOnly, false, nil,
		func(srv *sessiontest.TstSrv, w func(schema.ComponentManager)) {
			setup(srv, w)
			journalBefore, _ = os.ReadFile(session.JournalPathFor(srv.Ctx.Config.Runfile))
		})
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("commit must be refused; ok=%v errs=%v", r.ok, r.errs)
	}
	msg := r.errs[0].Error()
	if !strings.Contains(msg, compThird) || !strings.Contains(msg, "dependant") ||
		strings.Contains(msg, "bridge") || strings.Contains(msg, "prerequisite of") {
		t.Errorf("third must be named as a dependant: %s", msg)
	}
	if len(r.calls) != 0 || rec.before != 0 || count(r.log, logPreHooks) != 0 {
		t.Errorf("something ran: calls=%v records=%d", r.calls, rec.before)
	}
	if after, _ := os.ReadFile(r.journalPath()); string(after) != string(journalBefore) {
		t.Errorf("the journal was written")
	}
}

// (B8-1) Success: the dependant is called once, after second, first is not
// called, payload is its running config, and it is clean afterwards.
func TestCommitFencedDirtyDependantIsReappliedAfterChangedComponent(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommitCfg(t, runningThirdOnly, candidateChangesSecondOnly, false, nil,
		fenceSetupFor(t, compThird, 3_000_001, rec))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) != 2 || r.calls[0] != compSecond || r.calls[1] != compThird {
		t.Fatalf("calls = %v, want [second third] (first not called, third once)", r.calls)
	}
	if rec.before != 2 {
		t.Errorf("dispatch records = %d, want 2", rec.before)
	}
	if p := r.payloads[compThird]; len(p) != 1 ||
		!strings.Contains(p[0], "thirdLeaf") || !strings.Contains(p[0], "anotherValue") {
		t.Errorf("the dependant did not receive its running config: %q", p)
	}
	if count(r.log, logPreHooks) != 1 || count(r.log, logPostHooks) != 1 {
		t.Errorf("hooks did not run once each")
	}
	st := r.journalState(t)
	if st.Commit != nil {
		t.Errorf("W2 left a commit record: %+v", st.Commit)
	}
	if _, still := st.Dirty[compThird]; still {
		t.Errorf("the re-applied dependant is still dirty: %+v", st.Dirty)
	}
}

// (B8-2) The dependant's Set fails: closed failure, first not called, second
// compensated with prevCfg, the dependant called exactly once (never sent
// prevCfg) and still dirty.
func TestCommitFencedDirtyDependantFailureFailsClosedWithoutDependantCompensation(t *testing.T) {
	for _, once := range []bool{false, true} {
		name := "every Set fails"
		if once {
			name = "a second Set would succeed"
		}
		t.Run(name, func(t *testing.T) {
			rec := &stubRecorder{}
			r := journalCommitOpts(t, runningThirdOnly, candidateChangesSecondOnly, false,
				map[string]error{compThird: errors.New("third down")}, once,
				fenceSetupFor(t, compThird, 3_000_001, rec))
			if r.ok || len(r.errs) == 0 {
				t.Fatalf("commit must fail; ok=%v errs=%v", r.ok, r.errs)
			}
			if n := countOf(r.calls, compThird); n != 1 {
				t.Errorf("the dependant must be called exactly once, got %d: %v", n, r.calls)
			}
			if countOf(r.calls, compFirst) != 0 {
				t.Errorf("first must not be called: %v", r.calls)
			}
			if p := r.payloads[compThird]; len(p) != 1 || !strings.Contains(p[0], "anotherValue") {
				t.Errorf("the dependant payload is not its running config: %q", p)
			}
			p := r.payloads[compSecond]
			if len(p) != 2 || !strings.Contains(p[0], "someValue") || strings.Contains(p[1], "someValue") {
				t.Errorf("second must be applied and then compensated with prevCfg: %q", p)
			}
			if strings.Contains(r.log, logCommitScripts) || strings.Contains(r.log, logPostHooks) {
				t.Errorf("commit scripts or post-hooks ran")
			}
			if !r.runfileSame() {
				t.Errorf("the runfile was written")
			}
			if strings.Contains(serialiseCfg(r.srv.Cmgr.Running(), r.srv.Ms), "secondLeaf") {
				t.Errorf("running advanced to the candidate")
			}
			st := r.journalState(t)
			if st.Commit != nil {
				t.Errorf("the commit record was not closed: %+v", st.Commit)
			}
			if _, dirty := st.Dirty[compThird]; !dirty {
				t.Errorf("the failed dependant is no longer dirty: %+v", st.Dirty)
			}
		})
	}
}

// ---- stage 3-B9: opportunistic repair of a fenced dirty independent --------

// fourth has no ordering edges. Both trees carry the same fourthLeaf so that
// fourth is not changed and its payload is visible: C = {first} (second is
// unchanged), fourth is independent when dirty.
var (
	runningWithFourth = testutils.Root(
		testutils.Cont("fourth", testutils.Leaf("fourthLeaf", "fourthValue")),
		testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))
	candidateChangesFirstWithFourth = testutils.Root(
		testutils.Cont("first", testutils.Leaf("firstLeaf", "someValue")),
		testutils.Cont("fourth", testutils.Leaf("fourthLeaf", "fourthValue")),
		testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))
)

// (B9-1) A fenced dirty independent is repaired once with its running config;
// the changed component is applied; the prerequisites are not touched.
func TestCommitFencedDirtyIndependentIsRepairedOpportunistically(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		fenceSetupFor(t, compFourth, 3_000_001, rec))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if countOf(r.calls, compFourth) != 1 || countOf(r.calls, compFirst) != 1 ||
		countOf(r.calls, compSecond) != 0 || countOf(r.calls, compThird) != 0 || len(r.calls) != 2 {
		t.Fatalf("calls = %v, want one call each to fourth and first only", r.calls)
	}
	// OrderedComponents decides the order; record it
	t.Logf("call order: %v", r.calls)
	if p := r.payloads[compFourth]; len(p) != 1 || !strings.Contains(p[0], "fourthLeaf") ||
		!strings.Contains(p[0], "fourthValue") {
		t.Errorf("fourth did not receive its running config exactly once: %q", p)
	}
	st := r.journalState(t)
	if st.Commit != nil {
		t.Errorf("W2 left a commit record: %+v", st.Commit)
	}
	if _, still := st.Dirty[compFourth]; still {
		t.Errorf("the repaired independent is still dirty: %+v", st.Dirty)
	}
}

// (B9-2) The repair fails: the commit still succeeds, errs stay empty, the
// outputs name the component, it is called exactly once (never sent prevCfg),
// first is applied and not compensated, fourth stays dirty.
func TestCommitFencedDirtyIndependentRepairFailureIsOnlyAWarning(t *testing.T) {
	for _, once := range []bool{false, true} {
		name := "every Set fails"
		if once {
			name = "a second Set would succeed"
		}
		t.Run(name, func(t *testing.T) {
			rec := &stubRecorder{}
			r := journalCommitOpts(t, runningWithFourth, candidateChangesFirstWithFourth, true,
				map[string]error{compFourth: errors.New("fourth down")}, once,
				fenceSetupFor(t, compFourth, 3_000_001, rec))
			if !r.ok || len(r.errs) != 0 {
				t.Fatalf("the commit must succeed without errors: ok=%v errs=%v", r.ok, r.errs)
			}
			if !strings.Contains(allOutputText(r.outs), compFourth) ||
				!strings.Contains(allOutputText(r.outs), "WARNING") {
				t.Errorf("outputs do not warn about %s: %q", compFourth, allOutputText(r.outs))
			}
			if n := countOf(r.calls, compFourth); n != 1 {
				t.Errorf("fourth must be called exactly once, got %d: %v", n, r.calls)
			}
			if p := r.payloads[compFirst]; len(p) != 1 {
				t.Errorf("first must be applied once and not compensated: %q", p)
			}
			if !strings.Contains(r.log, logCommitScripts) || !strings.Contains(r.log, logPostHooks) {
				t.Errorf("the commit did not run to the end")
			}
			st := r.journalState(t)
			if st.Commit != nil {
				t.Errorf("the commit record was not closed: %+v", st.Commit)
			}
			if _, dirty := st.Dirty[compFourth]; !dirty {
				t.Errorf("the failed independent is no longer dirty: %+v", st.Dirty)
			}
		})
	}
}

// (B9-3) An unfenced independent is not called, the commit succeeds, and the
// outputs say why it was skipped.
func TestCommitUnfencedIndependentIsSkippedWithWarning(t *testing.T) {
	rec := &stubRecorder{}
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		fenceSetupFor(t, compFourth, 1_000_000, rec))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if countOf(r.calls, compFourth) != 0 {
		t.Errorf("an unfenced independent was called: %v", r.calls)
	}
	if !strings.Contains(allOutputText(r.outs), compFourth) || !strings.Contains(allOutputText(r.outs), "not fenced") {
		t.Errorf("no skip warning: %q", allOutputText(r.outs))
	}
	if _, dirty := r.journalState(t).Dirty[compFourth]; !dirty {
		t.Errorf("the skipped independent is no longer dirty")
	}
}

// A prerequisite-only commit never calls an independent that is not dirty.
func TestCommitWithoutDirtyIndependentDoesNotCallFourth(t *testing.T) {
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
			session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		})
	if !r.ok || countOf(r.calls, compFourth) != 0 {
		t.Errorf("ok=%v calls=%v", r.ok, r.calls)
	}
}

// ---- stage 3-B10: an independent is blocked by its failed independent prerequisite

const (
	compFifth = "net.vyatta.test.fifth"

	fifthSchema = `
container fifth {
	leaf fifthLeaf {
		type string;
	}
}`
)

// fifth must be applied after fourth; neither has an edge to first, second or
// third. Used only by the 3-B10 tests (journalWithFifth).
var fifthTestComp = conf.CreateTestDotComponentFile("fifth").AddBaseModel().SetAfter("fourth")

var journalWithFifth bool

var (
	runningWithFourthFifth = testutils.Root(
		testutils.Cont("fifth", testutils.Leaf("fifthLeaf", "fifthValue")),
		testutils.Cont("fourth", testutils.Leaf("fourthLeaf", "fourthValue")),
		testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))
	candidateChangesFirstWithFourthFifth = testutils.Root(
		testutils.Cont("fifth", testutils.Leaf("fifthLeaf", "fifthValue")),
		testutils.Cont("first", testutils.Leaf("firstLeaf", "someValue")),
		testutils.Cont("fourth", testutils.Leaf("fourthLeaf", "fourthValue")),
		testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))
)

// seedDirty marks comp dirty/unknown with a sent, unsettled dispatch at t; the
// identity reader returns, per component, a start `enter` microseconds after t.
func fenceSetupTwo(t *testing.T, enters map[string]int64) func(*sessiontest.TstSrv, func(schema.ComponentManager)) {
	const recordT = int64(5_000_000)
	return func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		for c := range enters {
			if err := seedUnfencedComp(srv.Ctx.Config.Runfile, c, recordT); err != nil {
				t.Fatal(err)
			}
		}
		session.SetIdentityReader(srv.Cmgr, session.IdentityFunc(
			func(c string) (session.ServiceIdentity, error) {
				return session.ServiceIdentity{Known: true, MainPID: 9,
					ActiveEnterMonoUs: recordT + enters[c]}, nil
			}))
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
	}
}

func withFifth(t *testing.T) {
	journalWithFifth = true
	t.Cleanup(func() { journalWithFifth = false })
}

const fencedLater, unfencedInside = int64(3_000_001), int64(1_000_000)

// (B10-2) Both fenced, fourth succeeds: fifth follows it; first is applied.
func TestCommitIndependentFollowsItsRepairedIndependentPrerequisite(t *testing.T) {
	withFifth(t)
	r := journalCommitCfg(t, runningWithFourthFifth, candidateChangesFirstWithFourthFifth, true, nil,
		fenceSetupTwo(t, map[string]int64{compFourth: fencedLater, compFifth: fencedLater}))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) != 3 || countOf(r.calls, compFirst) != 1 || countOf(r.calls, compFourth) != 1 ||
		countOf(r.calls, compFifth) != 1 || indexOf(r.calls, compFourth) > indexOf(r.calls, compFifth) {
		t.Fatalf("calls = %v, want first, fourth and fifth once each, fourth before fifth", r.calls)
	}
	if p := r.payloads[compFifth]; len(p) != 1 || !strings.Contains(p[0], "fifthValue") {
		t.Errorf("fifth did not receive its running config: %q", p)
	}
	st := r.journalState(t)
	if st.Commit != nil || len(st.Dirty) != 0 {
		t.Errorf("state after the commit: commit=%+v dirty=%+v", st.Commit, st.Dirty)
	}
}

// (B10-1) Both fenced, fourth fails: fifth is blocked, not called, warned about.
func TestCommitIndependentBlockedByFailedIndependentPrerequisite(t *testing.T) {
	for _, once := range []bool{false, true} {
		name := "every Set fails"
		if once {
			name = "a second Set would succeed"
		}
		t.Run(name, func(t *testing.T) {
			withFifth(t)
			r := journalCommitOpts(t, runningWithFourthFifth, candidateChangesFirstWithFourthFifth, true,
				map[string]error{compFourth: errors.New("fourth down")}, once,
				fenceSetupTwo(t, map[string]int64{compFourth: fencedLater, compFifth: fencedLater}))
			if !r.ok || len(r.errs) != 0 {
				t.Fatalf("the commit must succeed without errors: ok=%v errs=%v", r.ok, r.errs)
			}
			if countOf(r.calls, compFifth) != 0 {
				t.Errorf("fifth was called although fourth failed: %v", r.calls)
			}
			if countOf(r.calls, compFourth) != 1 || len(r.payloads[compFourth]) != 1 {
				t.Errorf("fourth must be called exactly once: %v", r.calls)
			}
			if len(r.payloads[compFirst]) != 1 {
				t.Errorf("first must be applied once and not compensated: %q", r.payloads[compFirst])
			}
			out := allOutputText(r.outs)
			if !strings.Contains(out, "blocked by "+compFourth) || !strings.Contains(out, compFifth) {
				t.Errorf("no 'blocked by' warning naming fourth and fifth: %q", out)
			}
			st := r.journalState(t)
			if st.Commit != nil {
				t.Errorf("the commit record was not closed: %+v", st.Commit)
			}
			for _, c := range []string{compFourth, compFifth} {
				if _, dirty := st.Dirty[c]; !dirty {
					t.Errorf("%s is no longer dirty: %+v", c, st.Dirty)
				}
			}
		})
	}
}

// (B10-3) Fourth is dirty and unfenced (not repaired): fifth, although fenced,
// is not called. Path: configd keeps fifth out of Opportunistic (the schema
// loop cannot see an unfenced skip).
func TestCommitIndependentBlockedByUnfencedIndependentPrerequisite(t *testing.T) {
	withFifth(t)
	r := journalCommitCfg(t, runningWithFourthFifth, candidateChangesFirstWithFourthFifth, true, nil,
		fenceSetupTwo(t, map[string]int64{compFourth: unfencedInside, compFifth: fencedLater}))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("commit failed: ok=%v errs=%v", r.ok, r.errs)
	}
	if countOf(r.calls, compFourth) != 0 || countOf(r.calls, compFifth) != 0 || countOf(r.calls, compFirst) != 1 {
		t.Errorf("calls = %v, want first only", r.calls)
	}
	out := allOutputText(r.outs)
	if !strings.Contains(out, "blocked by "+compFourth) {
		t.Errorf("no 'blocked by' warning: %q", out)
	}
	st := r.journalState(t)
	for _, c := range []string{compFourth, compFifth} {
		if _, dirty := st.Dirty[c]; !dirty {
			t.Errorf("%s is no longer dirty: %+v", c, st.Dirty)
		}
	}
}

// ---- stage 3-B11: the internal reconcile apply ------------------------------

var journalReconcile bool

func reconcileMode(t *testing.T) {
	journalReconcile = true
	t.Cleanup(func() { journalReconcile = false })
}

func hasLine(r *journalRun, prefix, comp string) bool {
	for _, o := range r.outs {
		if o != nil && strings.HasPrefix(o.Output, prefix) && strings.Contains(o.Output, comp) {
			return true
		}
	}
	return false
}

func (r *journalRun) dirtyClass(t *testing.T, c string) (schema.DirtyClass, bool) {
	d, ok := r.journalState(t).Dirty[c]
	return d.Class, ok
}

// (B11-1) fenced dirty fourth: re-applied with the RUNNING value, not the
// candidate's; clean afterwards; the runfile is untouched.
func TestReconcileFencedDirtyComponentIsAppliedWithRunningConfig(t *testing.T) {
	reconcileMode(t)
	rec := &stubRecorder{}
	candidate := testutils.Root(
		testutils.Cont("first", testutils.Leaf("firstLeaf", "someValue")),
		testutils.Cont("fourth", testutils.Leaf("fourthLeaf", "candidateValue")),
		testutils.Cont("second", testutils.Leaf("secondLeaf", "someValue")))
	setup := fenceSetupFor(t, compFourth, fencedLater, rec)
	r := journalCommitCfg(t, runningWithFourth, candidate, true, nil, setup)
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) != 1 || r.calls[0] != compFourth {
		t.Fatalf("calls = %v, want fourth once", r.calls)
	}
	if p := r.payloads[compFourth]; len(p) != 1 || !strings.Contains(p[0], "fourthValue") ||
		strings.Contains(p[0], "candidateValue") {
		t.Errorf("payload is not the running config: %q", p)
	}
	if !hasLine(r, "Reconciled:", compFourth) {
		t.Errorf("no 'Reconciled' line: %q", allOutputText(r.outs))
	}
	if _, d := r.dirtyClass(t, compFourth); d {
		t.Errorf("fourth is still dirty")
	}
	if !r.runfileSame() {
		t.Errorf("the runfile was written")
	}
	if strings.Contains(serialiseCfg(r.srv.Cmgr.Running(), r.srv.Ms), "candidateValue") {
		t.Errorf("running changed")
	}
	if strings.Contains(r.log, logPreHooks) || strings.Contains(r.log, logCommitScripts) || strings.Contains(r.log, logPostHooks) {
		t.Errorf("a hook or script ran")
	}
}

// (B11-2) the Set fails: called once, still dirty, a line, no error.
func TestReconcileFailureIsAReportedLine(t *testing.T) {
	for _, once := range []bool{false, true} {
		name := "every Set fails"
		if once {
			name = "a second Set would succeed"
		}
		t.Run(name, func(t *testing.T) {
			reconcileMode(t)
			r := journalCommitOpts(t, runningWithFourth, candidateChangesFirstWithFourth, true,
				map[string]error{compFourth: errors.New("fourth down")}, once,
				fenceSetupFor(t, compFourth, fencedLater, &stubRecorder{}))
			if !r.ok || len(r.errs) != 0 {
				t.Fatalf("ok=%v errs=%v", r.ok, r.errs)
			}
			if countOf(r.calls, compFourth) != 1 || len(r.calls) != 1 {
				t.Errorf("calls = %v, want fourth once", r.calls)
			}
			if !hasLine(r, "Not reconciled:", compFourth) {
				t.Errorf("no 'Not reconciled' line: %q", allOutputText(r.outs))
			}
			if _, d := r.dirtyClass(t, compFourth); !d {
				t.Errorf("fourth is no longer dirty")
			}
		})
	}
}

// (B11-3) unfenced: not called, still dirty, reported not fenced.
func TestReconcileUnfencedComponentIsNotCalled(t *testing.T) {
	reconcileMode(t)
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		fenceSetupFor(t, compFourth, unfencedInside, &stubRecorder{}))
	if !r.ok || len(r.errs) != 0 || len(r.calls) != 0 {
		t.Fatalf("ok=%v errs=%v calls=%v", r.ok, r.errs, r.calls)
	}
	if !hasLine(r, "Not reconciled:", compFourth) || !strings.Contains(allOutputText(r.outs), "not fenced") {
		t.Errorf("lines: %q", allOutputText(r.outs))
	}
	if _, d := r.dirtyClass(t, compFourth); !d {
		t.Errorf("fourth is no longer dirty")
	}
}

// (B11-4) fourth unfenced, fifth (after fourth) fenced: neither is called.
func TestReconcileBlockedByUnfencedPrerequisite(t *testing.T) {
	reconcileMode(t)
	withFifth(t)
	r := journalCommitCfg(t, runningWithFourthFifth, candidateChangesFirstWithFourthFifth, true, nil,
		fenceSetupTwo(t, map[string]int64{compFourth: unfencedInside, compFifth: fencedLater}))
	if !r.ok || len(r.errs) != 0 || len(r.calls) != 0 {
		t.Fatalf("ok=%v errs=%v calls=%v", r.ok, r.errs, r.calls)
	}
	if !strings.Contains(allOutputText(r.outs), "Not reconciled: "+compFifth+" (blocked by "+compFourth+")") {
		t.Errorf("fifth is not reported as blocked by fourth: %q", allOutputText(r.outs))
	}
	for _, c := range []string{compFourth, compFifth} {
		if _, d := r.dirtyClass(t, c); !d {
			t.Errorf("%s is no longer dirty", c)
		}
	}
}

// (B11-5) boot_partial is never reconciled and keeps its class.
func TestReconcileSkipsBootPartial(t *testing.T) {
	reconcileMode(t)
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
			if err := session.SeedJournal(srv.Ctx.Config.Runfile, func(s *session.JournalState) {
				s.Dirty[compFourth] = session.JournalDirty{Class: schema.DirtyBootPartial, CommitID: "boot"}
			}); err != nil {
				t.Fatal(err)
			}
			session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		})
	if !r.ok || len(r.errs) != 0 || len(r.calls) != 0 {
		t.Fatalf("ok=%v errs=%v calls=%v", r.ok, r.errs, r.calls)
	}
	if !hasLine(r, "Not reconciled:", compFourth) || !strings.Contains(allOutputText(r.outs), "boot_partial") {
		t.Errorf("lines: %q", allOutputText(r.outs))
	}
	if c, d := r.dirtyClass(t, compFourth); !d || c != schema.DirtyBootPartial {
		t.Errorf("class = %q (dirty %v), want boot_partial", c, d)
	}
}

// ---- stage 3-B15: the ordinary-commit hint line ----------------------------

const reconcileHint = "run 'reconcile' to repair"

func outsHave(r *journalRun, sub string) bool { return strings.Contains(allOutputText(r.outs), sub) }

// (B15-1) A successful commit that warns about a dirty component carries the
// hint as an out line; errs gains nothing.
func TestCommitSuccessWithDirtyWarningCarriesReconcileHint(t *testing.T) {
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		fenceSetupFor(t, compFourth, unfencedInside, &stubRecorder{}))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("ok=%v errs=%v", r.ok, r.errs)
	}
	if !outsHave(r, "WARNING: dirty component "+compFourth) {
		t.Fatalf("the existing warning is gone: %q", allOutputText(r.outs))
	}
	if !outsHave(r, reconcileHint) {
		t.Errorf("no hint in the outputs: %q", allOutputText(r.outs))
	}
}

// (B15-2) A failing commit that leaves a dirty component: the hint is an out
// line; the errors are the pre-existing ones, without the hint.
func TestCommitFailureLeavingDirtyComponentCarriesReconcileHint(t *testing.T) {
	r := journalCommitOpts(t, runningSecondOnly, candidateChangesFirst, false,
		map[string]error{compSecond: errors.New("second down")}, false,
		fenceSetupFor(t, compSecond, 3_000_001, &stubRecorder{}))
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("the commit must fail: ok=%v errs=%v", r.ok, r.errs)
	}
	if !outsHave(r, reconcileHint) {
		t.Errorf("no hint in the outputs: %q", allOutputText(r.outs))
	}
	for _, e := range r.errs {
		if strings.Contains(e.Error(), "reconcile") {
			t.Errorf("the hint leaked into an error: %s", e)
		}
	}
	if !strings.Contains(r.errs[0].Error(), compSecond) {
		t.Errorf("the original error text changed: %s", r.errs[0])
	}
}

// A refused commit (unfenced prerequisite) also leaves a dirty component.
func TestCommitRefusedByFenceGateCarriesReconcileHint(t *testing.T) {
	r := journalCommitCfg(t, runningSecondOnly, candidateChangesFirst, false, nil,
		fenceSetupFor(t, compSecond, 1_000_000, &stubRecorder{}))
	if r.ok || !outsHave(r, reconcileHint) {
		t.Errorf("ok=%v outs=%q", r.ok, allOutputText(r.outs))
	}
	if strings.Contains(r.errs[0].Error(), "reconcile") {
		t.Errorf("the hint leaked into the refusal: %s", r.errs[0])
	}
}

// (B15-3) No dirty components: the phrase is absent, on success and on failure.
func TestCommitWithoutDirtyComponentsHasNoReconcileHint(t *testing.T) {
	ok := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
			session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		})
	if !ok.ok || outsHave(ok, "reconcile") {
		t.Errorf("success: ok=%v outs=%q", ok.ok, allOutputText(ok.outs))
	}
	failing := true
	bad := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetJournalOpener(srv.Cmgr, session.FaultOpener(&failing, "create"))
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
	})
	if bad.ok || outsHave(bad, "reconcile") {
		t.Errorf("failure without dirty components: ok=%v outs=%q", bad.ok, allOutputText(bad.outs))
	}
}

// ---- stage 3-B16: the startup pass and the opt-in commit gate ----------------

var journalStartup bool

func startupMode(t *testing.T) {
	journalReconcile, journalStartup = true, true
	t.Cleanup(func() { journalReconcile, journalStartup = false, false })
}

// armGate arms the gate with an injected clock and returns the clock handle.
func armGate(srv *sessiontest.TstSrv, clock *time.Time) {
	session.ArmStartupGate(srv.Cmgr, func() time.Time { return *clock })
}

func gateRefusalText(t *testing.T, r *journalRun) string {
	t.Helper()
	if len(r.errs) == 0 {
		return ""
	}
	if e, ok := r.errs[0].(*mgmterror.OperationFailedApplicationError); ok {
		return e.Message
	}
	return r.errs[0].Error()
}

// (B16-1) Armed, before StartupPass: refused with the exact text, nothing sent,
// no hook, no hint line; 59 s are not enough.
func TestStartupGateRefusesCommitBeforeThePass(t *testing.T) {
	clock := time.Unix(1000, 0)
	rec := &stubRecorder{}
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		armGate(srv, &clock)
		clock = clock.Add(59 * time.Second)
		session.SetDispatchRecorder(srv.Cmgr, rec)
	})
	if r.ok || gateRefusalText(t, r) != "startup reconcile has not finished" || len(r.errs) != 1 {
		t.Fatalf("ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) != 0 || rec.before != 0 || count(r.log, logPreHooks) != 0 {
		t.Errorf("something ran: calls=%v records=%d", r.calls, rec.before)
	}
	if outsHave(r, "reconcile") {
		t.Errorf("the refusal carries a hint: %q", allOutputText(r.outs))
	}
	if _, err := os.Stat(r.journalPath()); err == nil {
		t.Errorf("a journal file was created")
	}
	if !r.runfileSame() {
		t.Errorf("the runfile was written")
	}
}

// (B16-2) After StartupPass returned (also with an error), the gate lets the
// same commit through.
func TestStartupGateOpensAfterThePass(t *testing.T) {
	clock := time.Unix(1000, 0)
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		armGate(srv, &clock)
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		if _, err := srv.Cmgr.StartupPass(srv.Ctx); err != nil {
			t.Fatalf("StartupPass: %v", err)
		}
	})
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("the gate refused after the pass: ok=%v errs=%v", r.ok, r.errs)
	}

	// an error return of the pass also finishes it
	r2 := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		armGate(srv, &clock)
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		if err := os.WriteFile(session.JournalPathFor(srv.Ctx.Config.Runfile), []byte("{ not json"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := srv.Cmgr.StartupPass(srv.Ctx); err == nil {
			t.Fatalf("StartupPass should fail on a corrupt journal")
		}
	})
	if gateRefusalText(t, r2) == "startup reconcile has not finished" {
		t.Errorf("the gate stayed closed after an error return")
	}
}

// (B16-3) Armed, the pass never called, 60 s on the injected clock: not refused.
func TestStartupGateOpensAfterTheBudget(t *testing.T) {
	clock := time.Unix(1000, 0)
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		armGate(srv, &clock)
		clock = clock.Add(60 * time.Second)
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
	})
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("the gate refused at 60 s: ok=%v errs=%v", r.ok, r.errs)
	}
}

// (B16-4) StartupPass on a fenced dirty component: the Reconciled line, no
// commit record, no runfile.
func TestStartupPassReconcilesFencedComponentWithoutCommitRecord(t *testing.T) {
	startupMode(t)
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		fenceSetupFor(t, compFourth, fencedLater, &stubRecorder{}))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) != 1 || r.calls[0] != compFourth || !hasLine(r, "Reconciled:", compFourth) {
		t.Errorf("calls=%v outs=%q", r.calls, allOutputText(r.outs))
	}
	if st := r.journalState(t); st.Commit != nil {
		t.Errorf("StartupPass wrote a commit record: %+v", st.Commit)
	}
	if !r.runfileSame() {
		t.Errorf("the runfile was written")
	}
}

// (B16-5) Unfenced and boot_partial components are not sent by the pass.
func TestStartupPassDoesNotSendUnfencedOrBootPartial(t *testing.T) {
	startupMode(t)
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		fenceSetupFor(t, compFourth, unfencedInside, &stubRecorder{}))
	if !r.ok || len(r.calls) != 0 || !hasLine(r, "Not reconciled:", compFourth) {
		t.Errorf("unfenced: ok=%v calls=%v outs=%q", r.ok, r.calls, allOutputText(r.outs))
	}
	b := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
			if err := session.SeedJournal(srv.Ctx.Config.Runfile, func(s *session.JournalState) {
				s.Dirty[compFourth] = session.JournalDirty{Class: schema.DirtyBootPartial, CommitID: "boot"}
			}); err != nil {
				t.Fatal(err)
			}
			session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		})
	if !b.ok || len(b.calls) != 0 {
		t.Errorf("boot_partial: ok=%v calls=%v", b.ok, b.calls)
	}
	if c, d := b.dirtyClass(t, compFourth); !d || c != schema.DirtyBootPartial {
		t.Errorf("class = %q (dirty %v)", c, d)
	}
}

// ---- stage 3-B17: crashSettle as a startup-only prelude ----------------------

// settleRecorder records the sleeper calls and whether the component had been
// called by then (a sleep after the Set would be a sleep inside the apply).
type settleRecorder struct {
	durations []time.Duration
	compCalls func() int
	atSleep   []int
}

func (s *settleRecorder) sleep(d time.Duration) {
	s.durations = append(s.durations, d)
	s.atSleep = append(s.atSleep, s.compCalls())
}

// (B17-1) Disarmed: the sleeper is not involved and a fenced dirty component is
// reconciled immediately (no seam set at all).
func TestCrashSettleDisarmedStartupPassReconcilesImmediately(t *testing.T) {
	startupMode(t)
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		fenceSetupFor(t, compFourth, fencedLater, &stubRecorder{}))
	if !r.ok || len(r.calls) != 1 || !hasLine(r, "Reconciled:", compFourth) {
		t.Errorf("ok=%v calls=%v outs=%q", r.ok, r.calls, allOutputText(r.outs))
	}
}

// (B17-2) Armed: the injected sleeper is called once with exactly 30 s, before
// the first dispatch record / component call; outputs are Reconcile's.
func TestCrashSettleArmedSleepsOnceBeforeReconcile(t *testing.T) {
	startupMode(t)
	sr := &stubRecorder{}
	rec := &settleRecorder{compCalls: func() int {
		sr.mu.Lock()
		defer sr.mu.Unlock()
		return sr.before
	}}
	setup := fenceSetupFor(t, compFourth, fencedLater, sr)
	run := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		func(srv *sessiontest.TstSrv, w func(schema.ComponentManager)) {
			setup(srv, w)
			session.SetCrashSettle(srv.Cmgr, rec.sleep)
		})
	if len(rec.durations) != 1 || rec.durations[0] != 30*time.Second {
		t.Fatalf("sleeper calls = %v, want exactly one of 30s", rec.durations)
	}
	if rec.atSleep[0] != 0 {
		t.Errorf("the sleep happened after %d dispatch record(s)", rec.atSleep[0])
	}
	if sr.before != 1 {
		t.Errorf("the reconcile did not dispatch after the sleep (records %d)", sr.before)
	}
	if !run.ok || len(run.calls) != 1 || !hasLine(run, "Reconciled:", compFourth) {
		t.Errorf("ok=%v calls=%v outs=%q", run.ok, run.calls, allOutputText(run.outs))
	}
}

// (B17-3, B17-4) Armed, identity unchanged, component unfenced: 30 s did not
// fence it; nothing is sent; its class is unchanged; no commit record, no runfile.
func TestCrashSettleDoesNotFenceAndWritesNothing(t *testing.T) {
	startupMode(t)
	rec := &settleRecorder{compCalls: func() int { return 0 }}
	setup := fenceSetupFor(t, compFourth, unfencedInside, &stubRecorder{})
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		func(srv *sessiontest.TstSrv, w func(schema.ComponentManager)) {
			setup(srv, w)
			session.SetCrashSettle(srv.Cmgr, rec.sleep)
		})
	if len(rec.durations) != 1 || rec.durations[0] != 30*time.Second {
		t.Fatalf("sleeper calls = %v", rec.durations)
	}
	if !r.ok || len(r.calls) != 0 || !hasLine(r, "Not reconciled:", compFourth) {
		t.Errorf("ok=%v calls=%v outs=%q", r.ok, r.calls, allOutputText(r.outs))
	}
	if c, d := r.dirtyClass(t, compFourth); !d || c != schema.DirtyUnknown {
		t.Errorf("class = %q (dirty %v), want unknown unchanged", c, d)
	}
	if st := r.journalState(t); st.Commit != nil {
		t.Errorf("a commit record was written: %+v", st.Commit)
	}
	if !r.runfileSame() {
		t.Errorf("the runfile was written")
	}
}

// The seam is not used by an ordinary commit.
func TestCrashSettleIsNotCalledByACommit(t *testing.T) {
	rec := &settleRecorder{compCalls: func() int { return 0 }}
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetCrashSettle(srv.Cmgr, rec.sleep)
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
	})
	if !r.ok || len(rec.durations) != 0 {
		t.Errorf("ok=%v sleeper calls=%v", r.ok, rec.durations)
	}
}
