// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-B23: ArmProductionStartup with injected clock and sleeper (no real
// sleep; RunStartup is never called from a test).

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

func prodArmedSetup(extra func(*sessiontest.TstSrv), clock *time.Time, sleep func(time.Duration)) func(*sessiontest.TstSrv, func(schema.ComponentManager)) {
	return func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
		srv.Cmgr.ArmProductionStartup(func() time.Time { return *clock }, sleep)
		extra(srv)
	}
}

// (B23-1) frozen clock + instant sleeper: StartupSerialised finishes the gate.
func TestArmProductionStartupThenStartupSerialisedOpensTheGate(t *testing.T) {
	clock := time.Unix(1000, 0)
	var slept []time.Duration
	r := journalCommit(t, nil, prodArmedSetup(func(srv *sessiontest.TstSrv) {
		if _, err := srv.Cmgr.StartupSerialised(srv.Ctx); err != nil {
			t.Fatalf("StartupSerialised: %v", err)
		}
	}, &clock, func(d time.Duration) { slept = append(slept, d) }))
	if gateRefusalText(t, r) == gateText || !r.ok {
		t.Errorf("the commit was refused or failed after the pass: ok=%v errs=%v", r.ok, r.errs)
	}
	if len(slept) != 1 || slept[0] != 30*time.Second {
		t.Errorf("crashSettle calls = %v, want one of 30s", slept)
	}
}

// (B23-2) channel sleeper: a second pass is refused while the first is blocked.
func TestArmProductionStartupSerialisesWithAChannelSleeper(t *testing.T) {
	clock := time.Unix(1000, 0)
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	var wg sync.WaitGroup
	var firstErr error
	sleeper := func(time.Duration) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			close(entered)
		}
		<-release
	}
	r := journalCommit(t, nil, prodArmedSetup(func(srv *sessiontest.TstSrv) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, firstErr = srv.Cmgr.StartupSerialised(srv.Ctx)
		}()
		<-entered
		_, err := srv.Cmgr.StartupSerialised(srv.Ctx)
		if err == nil || !strings.Contains(err.Error(), "Commit already in progress") {
			t.Errorf("second StartupSerialised: err = %v", err)
		}
		mu.Lock()
		if calls != 1 {
			t.Errorf("crashSettle ran %d times, want once", calls)
		}
		mu.Unlock()
		close(release)
		wg.Wait()
		if firstErr != nil {
			t.Errorf("first StartupSerialised: %v", firstErr)
		}
	}, &clock, sleeper))
	if gateRefusalText(t, r) == gateText || !r.ok {
		t.Errorf("the gate must be open after the first pass: ok=%v errs=%v", r.ok, r.errs)
	}
}

// (B23-3) No ArmProductionStartup: the constructor is still disarmed.
func TestNewCommitMgrIsStillDisarmed(t *testing.T) {
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		session.SetDispatchRecorder(srv.Cmgr, &stubRecorder{})
	})
	if gateRefusalText(t, r) == gateText || !r.ok {
		t.Errorf("a plain NewCommitMgr refused the commit: ok=%v errs=%v", r.ok, r.errs)
	}
}
