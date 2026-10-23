// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// Stage 3-B12 tests: the Reconcile RPC of the dispatcher. One component, a
// private t.TempDir runfile/journal, no real systemd. Not part of the broken
// server suite: run with -run Reconcile.

package server_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/danos/config/auth"
	"github.com/danos/config/schema"
	"github.com/danos/configd/server"
	"github.com/danos/configd/session"
	"github.com/danos/configd/session/sessiontest"
	"github.com/danos/vci/conf"
	"github.com/danos/yang/testutils"
)

const rcComp = "net.vyatta.test.first"

const rcSchema = `
container first {
	leaf firstLeaf {
		type string;
	}
}`

var rcTestComp = conf.CreateTestDotComponentFile("first").AddBaseModel()

// rcOps wraps the in-memory component manager: counts calls, records the
// payloads, can fail every Set, and can hold a Set until released.
type rcOps struct {
	schema.OperationsManager
	mu       sync.Mutex
	payloads []string
	fail     error
	entered  chan struct{} // closed on the first Set when non-nil
	release  chan struct{} // the Set waits for it when non-nil
	once     sync.Once
}

func (o *rcOps) SetConfigForModel(model string, obj interface{}) error {
	if o.entered != nil {
		o.once.Do(func() { close(o.entered) })
		<-o.release
	}
	o.mu.Lock()
	if s, ok := obj.(string); ok {
		o.payloads = append(o.payloads, s)
	}
	o.mu.Unlock()
	if o.fail != nil {
		return o.fail
	}
	return o.OperationsManager.SetConfigForModel(model, obj)
}

func (o *rcOps) calls() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.payloads...)
}

type rcEnv struct {
	d       *server.Disp
	ops     *rcOps
	runfile string
}

// rcSetup builds a dispatcher on a compensating component manager with the
// running config firstLeaf=runningValue and a private runfile directory.
func rcSetup(t *testing.T, a auth.Auther, configd bool, ops *rcOps) *rcEnv {
	t.Helper()
	ts := sessiontest.NewTestSpec(t).
		SetSchemaDefsByRef([]*sessiontest.TestSchema{
			sessiontest.NewTestSchema("vyatta-test-first-v1", "first").AddSchemaSnippet(rcSchema)}).
		SetComponents(conf.BaseModelSet, []string{rcTestComp.String()}).
		SetConfig(testutils.Root(testutils.Cont("first", testutils.Leaf("firstLeaf", "runningValue")))).
		SetAuther(a, configd, false)
	srv, _ := ts.Init()
	runfile := filepath.Join(t.TempDir(), "session_test.runfile")
	srv.Ctx.Config.Runfile = runfile
	tcm := ts.GetCompMgr()
	ops.OperationsManager = tcm
	srv.Ctx.CompMgr = schema.NewCompMgr(ops, tcm, srv.MsFull, tcm.GetComponentNSMappings())
	return &rcEnv{
		d:       server.NewDispatcher(srv.Smgr, srv.Cmgr, srv.Ms, srv.MsFull, srv.Ctx),
		ops:     ops,
		runfile: runfile,
	}
}

// seedDirty marks the component dirty (class unknown, no dispatch record, so
// the fence gate lets it through).
func (e *rcEnv) seedDirty(t *testing.T) {
	t.Helper()
	j, err := session.OpenJournal(session.JournalPathFor(e.runfile))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Update(func(s *session.JournalState) bool {
		s.Dirty[rcComp] = session.JournalDirty{Class: schema.DirtyUnknown, CommitID: "old"}
		return true
	}); err != nil {
		t.Fatal(err)
	}
}

func (e *rcEnv) dirty(t *testing.T) bool {
	t.Helper()
	j, err := session.OpenJournal(session.JournalPathFor(e.runfile))
	if err != nil {
		t.Fatal(err)
	}
	_, d := j.State().Dirty[rcComp]
	return d
}

const rcSID = "rcsid"

// (1) AAA: command "commit", argument "reconcile", exactly.
func TestReconcileRpcAaaIsCommitReconcile(t *testing.T) {
	a := auth.TestAutherAllowAll()
	e := rcSetup(t, a, false /* our auther is used */, &rcOps{})
	dispTestSetupSession(t, e.d, rcSID)
	clearAllCmdRequestsAndUserAuditLogs(a)
	if _, err := e.d.Reconcile(rcSID, false); err != nil {
		t.Fatalf("Reconcile: %s", err)
	}
	assertCommandAaaNoSecrets(t, a, []string{"commit", "reconcile"})
}

// (2) While a commit holds the loop, Reconcile is refused and applies nothing.
func TestReconcileRpcRefusedWhileCommitInProgress(t *testing.T) {
	ops := &rcOps{entered: make(chan struct{}), release: make(chan struct{})}
	e := rcSetup(t, auth.TestAutherAllowAll(), true, ops)
	dispTestSetupSession(t, e.d, rcSID)
	dispTestSet(t, e.d, rcSID, "first/firstLeaf/candidateValue")
	e.seedDirty(t)

	done := make(chan error, 1)
	go func() { _, err := e.d.Commit(rcSID, "", false); done <- err }()
	<-ops.entered // the commit is inside a component Set

	out, err := e.d.Reconcile(rcSID, false)
	if err == nil || !strings.Contains(err.Error(), "Commit already in progress") {
		t.Errorf("reply %q err %v, want 'Commit already in progress'", out, err)
	}
	close(ops.release)
	// The dispatcher's Save after the commit needs a clean /config (the known
	// environment failure of the server suite); the commit itself ran.
	if cerr := <-done; cerr != nil {
		t.Logf("commit returned (Save is outside this test): %s", cerr)
	}
	if n := len(ops.calls()); n != 1 {
		t.Errorf("component calls = %d, want 1 (the commit's own; no reconcile apply)", n)
	}
}

// (3) The candidate differs: the apply payload is the running value.
func TestReconcileRpcAppliesRunningConfigNotCandidate(t *testing.T) {
	e := rcSetup(t, auth.TestAutherAllowAll(), true, &rcOps{})
	dispTestSetupSession(t, e.d, rcSID)
	dispTestSet(t, e.d, rcSID, "first/firstLeaf/candidateValue")
	e.seedDirty(t)
	out, err := e.d.Reconcile(rcSID, false)
	if err != nil {
		t.Fatalf("Reconcile: %s", err)
	}
	p := e.ops.calls()
	if len(p) != 1 || !strings.Contains(p[0], "runningValue") || strings.Contains(p[0], "candidateValue") {
		t.Errorf("payload %q is not the running config", p)
	}
	if !strings.Contains(out, "Reconciled: "+rcComp) {
		t.Errorf("reply lacks 'Reconciled:': %q", out)
	}
	if e.dirty(t) {
		t.Errorf("the component is still dirty")
	}
	if _, err := os.Stat(e.runfile); !os.IsNotExist(err) {
		t.Errorf("the runfile was written (stat err %v)", err)
	}
}

// (4) The Set fails: text in the reply, no RPC error, still dirty.
func TestReconcileRpcFailedApplyIsTextNotError(t *testing.T) {
	e := rcSetup(t, auth.TestAutherAllowAll(), true, &rcOps{fail: errors.New("first down")})
	dispTestSetupSession(t, e.d, rcSID)
	e.seedDirty(t)
	out, err := e.d.Reconcile(rcSID, false)
	if err != nil {
		t.Fatalf("a component that stays dirty must not be an RPC error: %s", err)
	}
	if !strings.Contains(out, "Not reconciled: "+rcComp) {
		t.Errorf("reply lacks 'Not reconciled:': %q", out)
	}
	if !e.dirty(t) {
		t.Errorf("the component is no longer dirty")
	}
}

// (5) The journal cannot be opened: RPC error, no component called.
func TestReconcileRpcJournalOpenFailureIsAnError(t *testing.T) {
	e := rcSetup(t, auth.TestAutherAllowAll(), true, &rcOps{})
	dispTestSetupSession(t, e.d, rcSID)
	if err := os.WriteFile(session.JournalPathFor(e.runfile), []byte("{ not json"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := e.d.Reconcile(rcSID, false); err == nil || !strings.Contains(err.Error(), "journal") {
		t.Errorf("err = %v, want a journal error", err)
	}
	if n := len(e.ops.calls()); n != 0 {
		t.Errorf("components called: %d", n)
	}
}
