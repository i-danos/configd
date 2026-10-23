// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-B21: the existing StartupPass consumes the dirty set that the first
// journal open classified (design 6.3 transition (b), then 6.4 items 2 and 3).
// No production code is involved beyond StartupPass, Reconcile and journalFor.

package session_test

import (
	"os"
	"strings"
	"testing"

	"github.com/danos/config/schema"
	"github.com/danos/configd/session"
	"github.com/danos/configd/session/sessiontest"
)

const crashWindowClass = schema.DirtyClass("crash_window") // the unexported dirtyCrashWindow value

// seedOpenCommit plants an open applying commit (and a dirty entry that is no
// component) in the runfile's journal, through the public journal API only.
func seedOpenCommit(t *testing.T, runfile string, firstEmpty bool, todo ...string) {
	t.Helper()
	if err := session.SeedJournal(runfile, func(s *session.JournalState) {
		s.Commit = &session.JournalCommit{ID: "c-open", State: "applying", Todo: todo, FirstEmptyRtree: firstEmpty}
		s.Dirty["net.vyatta.test.other"] = session.JournalDirty{
			Class: schema.DirtyUnverified, Reason: "keep", CommitID: "old", Since: "t0"}
	}); err != nil {
		t.Fatal(err)
	}
}

// openCommitSetup: second is unfenced (identity inside the window), fourth is
// fenced (identity later), both have an unsettled dispatch record; the open
// commit lists both. Nothing here calls journalFor/Reconcile/commit/StartupPass.
func openCommitSetup(t *testing.T, sr *stubRecorder, firstEmpty bool, extra func(*sessiontest.TstSrv)) func(*sessiontest.TstSrv, func(schema.ComponentManager)) {
	return func(srv *sessiontest.TstSrv, w func(schema.ComponentManager)) {
		fenceSetupTwo(t, map[string]int64{compSecond: unfencedInside, compFourth: fencedLater})(srv, w)
		session.SetDispatchRecorder(srv.Cmgr, sr)
		seedOpenCommit(t, srv.Ctx.Config.Runfile, firstEmpty, compSecond, compFourth)
		if session.CachedJournal(srv.Cmgr) != nil {
			t.Fatalf("the journal was already opened before the startup pass")
		}
		if extra != nil {
			extra(srv)
		}
	}
}

// checkAfterFirstPass is rule 1 of the stage.
func checkAfterFirstPass(t *testing.T, st session.JournalState, sr *stubRecorder) {
	t.Helper()
	if sr.before != 1 {
		t.Errorf("dispatch records = %d, want exactly one (the fenced todo)", sr.before)
	}
	if _, dirty := st.Dirty[compFourth]; dirty {
		t.Errorf("the fenced todo is still dirty: %+v", st.Dirty[compFourth])
	}
	d := st.Dirty[compSecond]
	if d.Class != crashWindowClass || d.Reason != "open commit at restart" || d.CommitID != "c-open" || d.Since != "" {
		t.Errorf("unfenced todo: %+v, want crash_window / open commit at restart / c-open / empty since", d)
	}
	if o := st.Dirty["net.vyatta.test.other"]; o != (session.JournalDirty{
		Class: schema.DirtyUnverified, Reason: "keep", CommitID: "old", Since: "t0"}) {
		t.Errorf("the non-todo dirty entry changed: %+v", o)
	}
	if st.Commit != nil {
		t.Errorf("the stale commit record was not consumed by the classification: %+v", st.Commit)
	}
}

// (B21-1)
func TestStartupPassConsumesTheDirtySetClassifiedByTheFirstOpen(t *testing.T) {
	startupMode(t)
	sr := &stubRecorder{}
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		openCommitSetup(t, sr, false, nil))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("ok=%v errs=%v", r.ok, r.errs)
	}
	if p := r.payloads[compFourth]; len(p) != 1 || !strings.Contains(p[0], "fourthValue") {
		t.Errorf("the fenced todo did not get its running config: %q", p)
	}
	if !hasLine(r, "Reconciled:", compFourth) || !hasLine(r, "Not reconciled:", compSecond) {
		t.Errorf("outputs: %q", allOutputText(r.outs))
	}
	if len(r.calls) != 1 || r.calls[0] != compFourth {
		t.Errorf("calls = %v, want the fenced todo once", r.calls)
	}
	checkAfterFirstPass(t, r.journalState(t), sr)

	// memory-only mutation of the unsent todo in the cached journal, then a
	// second pass: it must not classify again (that would overwrite the reason)
	m := r.srv.Cmgr
	if !session.MutateCachedDirty(m, compSecond, session.JournalDirty{
		Class: crashWindowClass, Reason: "mutated", CommitID: "c-open"}) {
		t.Fatalf("MutateCachedDirty returned false: no cached journal")
	}
	if _, err := m.StartupPass(r.srv.Ctx); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	cached := session.CachedJournal(m).State()
	if cached.Dirty[compSecond].Reason != "mutated" {
		t.Errorf("the second pass classified again: %+v", cached.Dirty[compSecond])
	}
	if _, dirty := cached.Dirty[compFourth]; dirty {
		t.Errorf("the reconciled todo became dirty again")
	}
	if sr.before != 1 {
		t.Errorf("the second pass sent something: records=%d", sr.before)
	}
	if fresh := r.journalState(t); fresh.Dirty[compSecond].Reason != "open commit at restart" {
		t.Errorf("a fresh open of the file shows %q, want the classified reason", fresh.Dirty[compSecond].Reason)
	}
}

// (B21-2) first_empty_rtree: boot_partial, not sent, record consumed.
func TestStartupPassKeepsBootPartialOfAFirstEmptyRtreeCommit(t *testing.T) {
	startupMode(t)
	sr := &stubRecorder{}
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		openCommitSetup(t, sr, true, nil))
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("ok=%v errs=%v", r.ok, r.errs)
	}
	if sr.before != 0 || len(r.calls) != 0 {
		t.Errorf("something was sent: records=%d calls=%v", sr.before, r.calls)
	}
	st := r.journalState(t)
	for _, c := range []string{compSecond, compFourth} {
		d := st.Dirty[c]
		if d.Class != schema.DirtyBootPartial || d.Reason != "open commit at restart" || d.CommitID != "c-open" {
			t.Errorf("%s: %+v, want boot_partial", c, d)
		}
	}
	if st.Commit != nil {
		t.Errorf("the stale commit record was not consumed by the classification: %+v", st.Commit)
	}
}

// (B21-3) A rename fault on the first open: error, nothing sent, nothing
// cached, file unchanged; a later pass without the fault classifies and applies rule 1.
func TestStartupPassFailsClosedWhenTheFirstOpenCannotClassify(t *testing.T) {
	startupMode(t)
	sr := &stubRecorder{}
	var before []byte
	r := journalCommitCfg(t, runningWithFourth, candidateChangesFirstWithFourth, true, nil,
		openCommitSetup(t, sr, false, func(srv *sessiontest.TstSrv) {
			session.SetJournalOpener(srv.Cmgr, session.FaultOpenerFromRename(1))
			before, _ = os.ReadFile(session.JournalPathFor(srv.Ctx.Config.Runfile))
		}))
	if r.ok || len(r.errs) == 0 {
		t.Fatalf("StartupPass must return the error: ok=%v errs=%v", r.ok, r.errs)
	}
	if sr.before != 0 || len(r.calls) != 0 {
		t.Errorf("something was sent: records=%d calls=%v", sr.before, r.calls)
	}
	if session.CachedJournal(r.srv.Cmgr) != nil {
		t.Errorf("a journal was cached after the failed classification")
	}
	after, _ := os.ReadFile(r.journalPath())
	if string(before) != string(after) || len(before) == 0 {
		t.Errorf("the old file changed")
	}
	// the recorder is built together with the cached journal; the 3-B20 test
	// TestJournalForRenameFaultLeavesNothingCachedAndRetries asserts it stays nil.

	session.SetJournalOpener(r.srv.Cmgr, nil) // later open without the fault
	if _, err := r.srv.Cmgr.StartupPass(r.srv.Ctx); err != nil {
		t.Fatalf("later pass: %v", err)
	}
	checkAfterFirstPass(t, r.journalState(t), sr)
}
