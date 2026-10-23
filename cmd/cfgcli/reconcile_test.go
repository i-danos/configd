// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"errors"
	"strings"
	"testing"
)

func TestReconcileIsAStaticCommandWithoutConfigMgmt(t *testing.T) {
	overrideConfigMgmtCheck(func(cfgManager) bool { return false })
	defer resetConfigMgmtCheck()
	if err := updateDynamicCommands(newTestClient(t)); err != nil {
		t.Fatal(err)
	}
	c, ok := Commands["reconcile"]
	if !ok {
		t.Fatalf("reconcile is not in the command map without config-mgmt")
	}
	if _, ok := populateCommands()["reconcile"]; !ok {
		t.Errorf("reconcile is not in the static map")
	}
	if !strings.Contains(c.Help, "running configuration") || !strings.Contains(c.Help, "not a commit") {
		t.Errorf("help does not say what it does: %q", c.Help)
	}
	if err := c.ValidFn(&Ctx{Args: []string{"reconcile", "extra"}, Prefix: "x"}); err == nil {
		t.Errorf("an argument must be rejected")
	}
	if err := c.ValidFn(&Ctx{Args: []string{"reconcile"}}); err != nil {
		t.Errorf("no argument must be valid: %v", err)
	}
}

// The run seam asks configd to reconcile with the commit debug flag and
// nothing else: the other testClient methods panic, so SessionChanged, Commit,
// CommitConfirm or Save would fail the test. The candidate is never looked at.
func TestReconcileRunCallsOnlyReconcile(t *testing.T) {
	tc := newTestClient(t)
	tc.reconcileOut = "Reconciled: x\n"
	out, err := reconcileRunInternal(&Ctx{Client: tc})
	if err != nil || out != "Reconciled: x\n" {
		t.Fatalf("out %q err %v", out, err)
	}
	if len(tc.reconcileCalls) != 1 || tc.reconcileCalls[0] {
		t.Errorf("calls = %v, want one call with debug=false", tc.reconcileCalls)
	}
	t.Setenv("COMMIT_DEBUG", "1")
	reconcileRunInternal(&Ctx{Client: tc})
	if len(tc.reconcileCalls) != 2 || !tc.reconcileCalls[1] {
		t.Errorf("calls = %v, want debug=true the second time", tc.reconcileCalls)
	}
}

func TestReconcileRunPassesTheErrorThrough(t *testing.T) {
	tc := newTestClient(t)
	tc.reconcileErr = errors.New("unknown method Reconcile")
	out, err := reconcileRunInternal(&Ctx{Client: tc})
	if err == nil || err.Error() != "unknown method Reconcile" || out != "" {
		t.Errorf("out %q err %v, want the peer error unchanged", out, err)
	}
}
