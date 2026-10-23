// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// Stage 3-B25: an ordinary Disp.Commit whose only extra information is a
// warning about an independent dirty component takes the dispatcher's Save
// branch (errs empty) and does not print the non-fatal sentence.

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
	dwX = "net.vyatta.test.first"  // the only changed component
	dwD = "net.vyatta.test.second" // dirty, not in the candidate diff, no edge to X
)

const dwSecondSchema = `
container second {
	leaf secondLeaf {
		type string;
	}
}`

var dwSecondComp = conf.CreateTestDotComponentFile("second").AddBaseModel()

// dwOps counts the Sets per component and fails every Set of one model.
type dwOps struct {
	schema.OperationsManager
	mu       sync.Mutex
	payloads map[string][]string
	failD    bool
}

func (o *dwOps) SetConfigForModel(model string, obj interface{}) error {
	o.mu.Lock()
	if o.payloads == nil {
		o.payloads = map[string][]string{}
	}
	if s, ok := obj.(string); ok {
		o.payloads[model] = append(o.payloads[model], s)
	}
	o.mu.Unlock()
	if o.failD && model == dwD {
		return errString("second down")
	}
	return o.OperationsManager.SetConfigForModel(model, obj)
}

type errString string

func (e errString) Error() string { return string(e) }

func TestCommitWithIndependentDirtyWarningTakesTheSaveBranch(t *testing.T) {
	ops := &dwOps{failD: true}
	ts := sessiontest.NewTestSpec(t).
		SetSchemaDefsByRef([]*sessiontest.TestSchema{
			sessiontest.NewTestSchema("vyatta-test-first-v1", "first").AddSchemaSnippet(rcSchema),
			sessiontest.NewTestSchema("vyatta-test-second-v1", "second").AddSchemaSnippet(dwSecondSchema)}).
		SetComponents(conf.BaseModelSet, []string{rcTestComp.String(), dwSecondComp.String()}).
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
		s.Dirty[dwD] = session.JournalDirty{Class: schema.DirtyUnknown, CommitID: "old"}
		return true
	}); err != nil {
		t.Fatal(err)
	}

	dispTestSetupSession(t, d, rcSID)
	dispTestSet(t, d, rcSID, "first/firstLeaf/newValue")

	out, err := d.Commit(rcSID, "", false)
	if err != nil {
		t.Fatalf("Commit returned an error (the Save branch did not return ok): %v\nreply: %q", err, out)
	}
	if !strings.Contains(out, "WARNING: repair of dirty component "+dwD) {
		t.Errorf("reply lacks the dirty-component warning: %q", out)
	}
	if !strings.Contains(out, "run 'reconcile' to repair") {
		t.Errorf("reply lacks the reconcile hint: %q", out)
	}
	if strings.Contains(out, "Commit succeeded (non-fatal failures detected).") {
		t.Errorf("the non-fatal branch was taken (no Save): %q", out)
	}
	ops.mu.Lock()
	x := ops.payloads[dwX]
	ops.mu.Unlock()
	if len(x) != 1 || !strings.Contains(x[0], "newValue") {
		t.Errorf("X was not applied once with the new value: %q", x)
	}
}
