// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 3, stage 3-B16: the startup pass entry and an opt-in commit gate
// (design 6.4 item 3: startup is the same apply as reconcile; commits are
// accepted after the pass or its 60 s budget). Nothing here is wired into a
// constructor or main: the gate is disarmed by default and only a test seam
// arms it. It is not a process-start hook and not crashSettle.

package session

import (
	"errors"
	"sync"
	"time"

	"github.com/danos/configd"
	"github.com/danos/utils/exec"
)

// startupBudget is the time after arming at which commits are accepted even if
// the pass never finished (design 6.4 item 3).
const startupBudget = 60 * time.Second

// crashSettleWait is the bounded wait of design 6.4.
const crashSettleWait = 30 * time.Second

const startupGateText = "startup reconcile has not finished"

// startupGate is the opt-in gate. The zero value is disarmed.
type startupGate struct {
	mu    sync.Mutex
	armed bool
	done  bool
	since time.Time
	now   func() time.Time
}

func (g *startupGate) arm(now func() time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if now == nil {
		now = time.Now
	}
	g.armed, g.done, g.now, g.since = true, false, now, now()
}

func (g *startupGate) finish() {
	g.mu.Lock()
	g.done = true
	g.mu.Unlock()
}

// refusal returns the gate error while the gate is armed, the pass has not
// returned and the budget has not elapsed; nil otherwise.
func (g *startupGate) refusal() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed || g.done {
		return nil
	}
	if g.now().Sub(g.since) >= startupBudget {
		return nil
	}
	return errors.New(startupGateText)
}

// StartupPass is the startup entry: the same apply as Reconcile, outputs and
// error returned unchanged. It does not sleep, does not touch a commit
// record, the runfile, the running tree or Save. An error return also
// finishes the pass for the gate. A later wiring stage must enter through
// ReconcileSerialised (the commit loop), not call StartupPass beside a
// running commit.
func (m *CommitMgr) StartupPass(sctx *configd.Context) ([]*exec.Output, error) {
	// crashSettle (design 6.4): a bounded wait before the first fence
	// evaluation so slow handlers can finish. Opt-in, only here; it reads no
	// identity, writes nothing and never marks a component fenced.
	if m.crashSettle != nil {
		m.crashSettle(crashSettleWait)
	}
	outs, err := m.Reconcile(sctx)
	m.startup.finish()
	return outs, err
}
