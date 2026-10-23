// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// Stage 3-B27: Disp.ConfirmedCommit (confirmed, timeout 60) whose only extra
// information is a warning about an independent dirty component takes the
// dispatcher's Save branch (errs empty) and does not print the non-fatal
// sentence. A nil error does not prove that setConfirmedCommitTimeout ran: its
// error is discarded by the dispatcher.

package server_test

import (
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

const (
	ccX = "net.vyatta.test.first"  // the only changed component
	ccD = "net.vyatta.test.second" // dirty, not in the candidate diff, no edge to X
)

const ccSecondSchema = `
container second {
	leaf secondLeaf {
		type string;
	}
}`

var ccSecondComp = conf.CreateTestDotComponentFile("second").AddBaseModel()

// ccOps counts the Sets per component and fails every Set of one model.
type ccOps struct {
	schema.OperationsManager
	mu       sync.Mutex
	payloads map[string][]string
	failD    bool
}

func (o *ccOps) SetConfigForModel(model string, obj interface{}) error {
	o.mu.Lock()
	if o.payloads == nil {
		o.payloads = map[string][]string{}
	}
	if s, ok := obj.(string); ok {
		o.payloads[model] = append(o.payloads[model], s)
	}
	o.mu.Unlock()
	if o.failD && model == ccD {
		return ccErrStr("second down")
	}
	return o.OperationsManager.SetConfigForModel(model, obj)
}

type ccErrStr string

func (e ccErrStr) Error() string { return string(e) }

func TestConfirmedCommitWithIndependentDirtyWarningTakesTheSaveBranch(t *testing.T) {
	ops := &ccOps{failD: true}
	ts := sessiontest.NewTestSpec(t).
		SetSchemaDefsByRef([]*sessiontest.TestSchema{
			sessiontest.NewTestSchema("vyatta-test-first-v1", "first").AddSchemaSnippet(rcSchema),
			sessiontest.NewTestSchema("vyatta-test-second-v1", "second").AddSchemaSnippet(ccSecondSchema)}).
		SetComponents(conf.BaseModelSet, []string{rcTestComp.String(), ccSecondComp.String()}).
		SetConfig(testutils.Root(
			testutils.Cont("first", testutils.Leaf("firstLeaf", "runningValue")),
			testutils.Cont("second", testutils.Leaf("secondLeaf", "dValue")))).
		SetAuther(auth.TestAutherAllowAll(), true, false)
	srv, _ := ts.Init()
	runfile := t.TempDir() + "/session_test.runfile"
	srv.Ctx.Config.Runfile = runfile
	tcm := ts.GetCompMgr()
	ops.OperationsManager = tcm
	srv.Ctx.CompMgr = schema.NewCompMgr(ops, tcm, srv.MsFull, tcm.GetComponentNSMappings())
	d := server.NewDispatcher(srv.Smgr, srv.Cmgr, srv.Ms, srv.MsFull, srv.Ctx)

	j, err := session.OpenJournal(session.JournalPathFor(runfile))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Update(func(s *session.JournalState) bool {
		s.Dirty[ccD] = session.JournalDirty{Class: schema.DirtyUnknown, CommitID: "old"}
		return true
	}); err != nil {
		t.Fatal(err)
	}

	dispTestSetupSession(t, d, rcSID)
	dispTestSet(t, d, rcSID, "first/firstLeaf/newValue")

	out, err := d.ConfirmedCommit(rcSID, "", true, "60", "", "", false)
	if err != nil {
		t.Fatalf("ConfirmedCommit returned an error: %v\nreply: %q", err, out)
	}
	if !strings.Contains(out, "WARNING: repair of dirty component "+ccD) {
		t.Errorf("reply lacks the dirty-component warning: %q", out)
	}
	if !strings.Contains(out, "run 'reconcile' to repair") {
		t.Errorf("reply lacks the reconcile hint: %q", out)
	}
	if strings.Contains(out, "Commit succeeded (non-fatal failures detected).") {
		t.Errorf("the non-fatal branch was taken (no Save): %q", out)
	}
	ops.mu.Lock()
	x := ops.payloads[ccX]
	ops.mu.Unlock()
	if len(x) != 1 || !strings.Contains(x[0], "newValue") {
		t.Errorf("X was not applied once with the new value: %q", x)
	}
}
