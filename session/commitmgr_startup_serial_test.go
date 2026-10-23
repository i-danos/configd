// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-B22: StartupSerialised runs StartupPass on the existing commit loop.

package session_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danos/config/schema"
	"github.com/danos/configd/session"
	"github.com/danos/configd/session/sessiontest"
)

const gateText = "startup reconcile has not finished"

func armedSetup(t *testing.T, extra func(*sessiontest.TstSrv)) func(*sessiontest.TstSrv, func(schema.ComponentManager)) {
	return func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		clock := time.Unix(1000, 0) // never advanced: only a finished pass can open the gate
		session.ArmStartupGate(srv.Cmgr, func() time.Time { return clock })
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		extra(srv)
	}
}

// (B22-1) StartupSerialised runs the pass: the gate is open for the commit.
func TestStartupSerialisedFinishesTheGate(t *testing.T) {
	r := journalCommit(t, nil, armedSetup(t, func(srv *sessiontest.TstSrv) {
		if _, err := srv.Cmgr.StartupSerialised(srv.Ctx); err != nil {
			t.Fatalf("StartupSerialised: %v", err)
		}
	}))
	if gateRefusalText(t, r) == gateText {
		t.Fatalf("the commit was refused by the startup gate after StartupSerialised")
	}
	if !r.ok || len(r.errs) != 0 {
		t.Errorf("ok=%v errs=%v", r.ok, r.errs)
	}
}

// (B22-2) While the first startup pass is blocked inside the loop (crashSettle
// blocks on a channel), a second one is refused and runs nothing.
func TestStartupSerialisedIsRefusedWhileAPassRuns(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	settleCalls := 0
	var wg sync.WaitGroup
	var firstErr error
	r := journalCommit(t, nil, armedSetup(t, func(srv *sessiontest.TstSrv) {
		session.SetCrashSettle(srv.Cmgr, func(time.Duration) {
			mu.Lock()
			settleCalls++
			n := settleCalls
			mu.Unlock()
			if n == 1 {
				close(entered)
			}
			<-release
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, firstErr = srv.Cmgr.StartupSerialised(srv.Ctx)
		}()
		<-entered // the first pass is inside the loop, blocked in crashSettle
		_, err := srv.Cmgr.StartupSerialised(srv.Ctx)
		if err == nil || !strings.Contains(err.Error(), "Commit already in progress") {
			t.Errorf("second StartupSerialised: err = %v, want 'Commit already in progress'", err)
		}
		mu.Lock()
		if settleCalls != 1 {
			t.Errorf("the refused call ran a pass: crashSettle calls = %d", settleCalls)
		}
		mu.Unlock()
		close(release)
		wg.Wait() // no leaked goroutine: the first call returns after the release
		if firstErr != nil {
			t.Errorf("first StartupSerialised: %v", firstErr)
		}
	}))
	if gateRefusalText(t, r) == gateText || !r.ok {
		t.Errorf("after the first pass finished the gate must be open: ok=%v errs=%v", r.ok, r.errs)
	}
}

// (B22-3) ReconcileSerialised does not finish the gate: the commit is refused.
func TestReconcileSerialisedDoesNotFinishTheGate(t *testing.T) {
	r := journalCommit(t, nil, armedSetup(t, func(srv *sessiontest.TstSrv) {
		if _, err := srv.Cmgr.ReconcileSerialised(srv.Ctx); err != nil {
			t.Fatalf("ReconcileSerialised: %v", err)
		}
	}))
	if r.ok || gateRefusalText(t, r) != gateText || len(r.errs) != 1 {
		t.Errorf("the commit must still be refused by the gate: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(r.calls) != 0 {
		t.Errorf("a component was called: %v", r.calls)
	}
}
