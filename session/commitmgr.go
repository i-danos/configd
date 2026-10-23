// Copyright (c) 2018-2021, AT&T Intellectual Property. All rights reserved.
//
// Copyright (c) 2014-2017 by Brocade Communications Systems, Inc.
// All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

package session

import (
	"os"
	"os/user"
	"strconv"
	"time"

	"github.com/danos/config/data"
	"github.com/danos/config/diff"
	"github.com/danos/config/schema"
	"github.com/danos/config/union"
	"github.com/danos/configd"
	"github.com/danos/configd/common"
	"github.com/danos/mgmterror"
	"github.com/danos/utils/exec"
)

type commitmgrreq struct {
	sid     string
	ctx     *configd.Context
	t       *data.Node
	message string
	debug   bool
	resp    chan *commitresp
	// reconcile: run the internal reconcile apply instead of a commit (it goes
	// through the same loop, so it shares the in-progress rule).
	reconcile bool
	// startup: run the startup pass (StartupPass) through the same loop; it is
	// checked before reconcile, and only StartupSerialised sets it.
	startup bool
}

type commitresp struct {
	out []*exec.Output
	err []error
	ok  bool
}

type CommitMgr struct {
	running   *data.AtomicNode
	effective *Session
	schema    schema.ModelSet
	reqch     chan commitmgrreq
	hadcommit bool

	// Stage 3-B2: the commit journal and the dispatch recorder, used only when
	// the component manager implements schema.CompensatingComponentManager.
	// Opened on first use (journalFor); the fields are seams for tests.
	journal     *Journal
	recorder    schema.DispatchRecorder
	openJournal func(path string) (*Journal, error)
	runfileFS   journalFS      // nil: the real file system
	identities  IdentityReader // nil: systemd
	startup     startupGate    // opt-in commit gate, disarmed by default
	// crashSettle: opt-in startup prelude (design 6.4); nil = disarmed.
	crashSettle func(time.Duration)
}

func NewCommitMgr(running *data.AtomicNode, schema schema.ModelSet) *CommitMgr {
	c := &CommitMgr{
		running: running,
		schema:  schema,
		reqch:   make(chan commitmgrreq),
	}
	go c.run()
	return c
}

func (m *CommitMgr) SetEffective(effective *Session) {
	m.effective = effective
}

func (m *CommitMgr) writeRunning(ctx *configd.Context) error {
	f, err := os.Create(ctx.Config.Runfile)
	if err != nil {
		return err
	}
	defer f.Close()
	// The running file contains the running configuration with secrets, and
	// should definitely NOT be world readable.
	err = f.Chmod(0600)
	if err != nil {
		return err
	}
	//Effective and running are equivalent here use that
	//fact to avoid creating another union tree.
	out, err := m.effective.Show(ctx, []string{}, false, false)
	if err != nil {
		return err
	}
	_, err = f.WriteString(out)
	return err
}

func (m *CommitMgr) commit(sid string, sctx *configd.Context, candidate *data.Node, message string, debug bool) *commitresp {
	//"and now for the subtle bit..."
	//This is important so it deserves an explanation.
	//In order for the defaults to be propagated to the upper layers correctly
	//We need the running tree (with no defaults instanciated) this is m.Running().
	//m.Running() does an atomic dereference of the running tree pointer, so we can only
	//call it once, and must store the returned value on the stack.
	//'run' contains the running tree with the instanciated defaults.
	//'mcan' contains the current candidate tree with the instanciated defaults, in order
	//to properly instanciate the default state in this tree it must be merged with the running
	//tree without the default values in it.
	overallStart := time.Now()
	rtree := m.Running()

	var run *data.Node

	firstCommit := !m.hadcommit
	if !m.hadcommit {
		// Very first commit. To ensure that defaults values
		// are configured, set running tree with no defaults
		// instantiated. When compared to candiate tree, the
		// defaults will be seen as a difference.
		m.hadcommit = true
		run = union.NewNode(nil, rtree, m.schema, nil, 0).MergeWithoutDefaults()
	} else {
		run = union.NewNode(nil, rtree, m.schema, nil, 0).Merge()
	}

	ucan := union.NewNode(candidate, rtree, m.schema, nil, 0)
	mcan := ucan.Merge()
	// debug-level logging should be enabled if the debug flag passed in is
	// set OR if configd 'commit' logging is set to debug level.
	debug = debug || common.LoggingIsEnabledAtLevel(
		common.LevelDebug, common.TypeCommit)
	mustThreshold, _ := common.LoggingValueAndStatus(common.TypeMust)
	ctx := newctx(sid, sctx, m.effective, mcan, run, m.schema, message,
		debug, mustThreshold)
	ctx.LogCommitMsg("Starting validation and commit")
	outs, errs, ok := ctx.validate()
	if !ok {
		if firstCommit {
			// Nothing was committed: the next attempt is still the first.
			m.hadcommit = false
		}
		return &commitresp{out: outs, err: errs, ok: ok}
	}

	// Opt-in startup gate (design 6.4 item 3): disarmed unless a test arms it.
	// Nothing was sent and no hook ran yet.
	if gerr := m.startup.refusal(); gerr != nil {
		e := mgmterror.NewOperationFailedApplicationError()
		e.Message = gerr.Error()
		errs = append(errs, e)
		if firstCommit {
			m.hadcommit = false
		}
		return &commitresp{out: outs, err: errs, ok: false}
	}

	// Create environment for hooks
	commitStart := time.Now()
	env := make([]string, 0)
	uid := strconv.FormatUint(uint64(sctx.Uid), 10)
	user, lookuperr := user.LookupId(uid)
	if lookuperr != nil {
		err := mgmterror.NewOperationFailedApplicationError()
		err.Message = "Could not lookup UID"
		errs = append(errs, err)
		if firstCommit {
			m.hadcommit = false
		}
		return &commitresp{out: outs, err: errs, ok: false}
	}
	env = append(env, "COMMIT_USER="+user.Username)
	env = append(env, "PATH=/bin:/usr/bin:/sbin:/usr/sbin:/opt/vyatta/bin:/opt/vyatta/sbin")

	// Fence gate (design 6.4, ordinary commit): before the pre-hooks, W1 and
	// any component call. Journaled path only; reads the journal, writes nothing.
	changedNSMap := diff.CreateChangedNSMap(mcan, run, m.schema, nil)
	var forced, soft []string
	if _, isComp := sctx.CompMgr.(schema.CompensatingComponentManager); isComp {
		var ferrs []error
		if forced, ferrs = m.fenceCheck(sctx, changedNSMap); len(ferrs) > 0 {
			errs = append(errs, ferrs...)
			if firstCommit {
				m.hadcommit = false
			}
			ctx.LogCommitTime("Commit OVERALL", commitStart)
			outs = append(outs, m.reconcileHint()...)
			return &commitresp{out: outs, err: errs, ok: false}
		}
		// Dirty independent components: opportunistic repair when fenced,
		// a warning otherwise (design 7.2 item 5); never a refusal.
		var swarns []*exec.Output
		soft, swarns = m.opportunisticPlan(sctx, changedNSMap)
		outs = append(outs, swarns...)
	}

	// Run pre-hooks
	hout, herr := ctx.execute_hooks("/etc/commit/pre-hooks.d", env)
	outs = append(outs, hout)
	if herr != nil {
		errs = append(errs, herr)
	}
	ctx.LogCommitTime("Pre-commit hooks", commitStart)

	// Can't use AppendOutput because ctx.commit signature is different
	var couts []*exec.Output
	var cerrs []error
	var jr *compensatingResult
	if comp, ok := sctx.CompMgr.(schema.CompensatingComponentManager); ok {
		// Journaled path (design 11.2, steps 3-4): commit record, then the
		// compensating apply with the dispatch recorder. Any failure ends the
		// commit before the commit scripts, the running store, the runfile and
		// the post-hooks.
		cr := m.applyCompensating(comp, sctx, ctx, ucan, rtree, changedNSMap, forced, soft, firstCommit)
		outs = append(outs, cr.outs...)
		if cr.failed {
			errs = append(errs, cr.errs...)
			if firstCommit {
				// Nothing was committed: the next attempt is still the first.
				m.hadcommit = false
			}
			ctx.LogCommitTime("Commit OVERALL", commitStart)
			outs = append(outs, m.reconcileHint()...)
			return &commitresp{out: outs, err: errs, ok: false}
		}
		jr = cr
	} else {
		checkedCompMgr, supported := sctx.CompMgr.(schema.CheckedComponentManager)
		if !supported {
			err := mgmterror.NewOperationFailedApplicationError()
			err.Message = "Component manager does not support fail-closed configuration application"
			errs = append(errs, err)
			if firstCommit {
				m.hadcommit = false
			}
			ctx.LogCommitTime("Commit OVERALL", commitStart)
			return &commitresp{out: outs, err: errs, ok: false}
		}
		couts, compErrs := checkedCompMgr.ComponentSetRunningChecked(
			m.schema, ucan, changedNSMap, ctx.LogCommitTime)
		outs = append(outs, couts...)
		if len(compErrs) > 0 {
			// A component failed to apply the configuration. Fail closed: do not
			// run the commit scripts, do not store or write the running config
			// and do not run post-hooks. The candidate is kept (the session only
			// resets it on success). Components applied before the failure are
			// NOT rolled back; the error text names them and says whether the
			// failing component's outcome is unknown.
			errs = append(errs, compErrs...)
			if firstCommit {
				// Nothing was committed: the next attempt is still the first.
				m.hadcommit = false
			}
			ctx.LogCommitTime("Commit OVERALL", commitStart)
			return &commitresp{out: outs, err: errs, ok: false}
		}

	}

	couts, cerrs, _ = ctx.commit(&env)
	outs = append(outs, couts...)
	errs = append(errs, cerrs...)

	writeStart := time.Now()
	effective := m.effective.MergeTreeWithoutDefaults(ctx.ctx)
	m.effective.Discard(ctx.ctx) //we got what we needed
	if jr != nil {
		// Journaled path (design 11.2, steps 6-8): render, persist, and only
		// then store the new running tree; a persist failure compensates.
		pouts, perrs, pfailed := m.persistJournaled(jr, sctx, ctx, effective)
		outs = append(outs, pouts...)
		if pfailed {
			errs = append(errs, perrs...)
			if firstCommit {
				m.hadcommit = false
			}
			ctx.LogCommitTime("Write config", writeStart)
			ctx.LogCommitTime("Commit OVERALL", commitStart)
			outs = append(outs, m.reconcileHint()...)
			return &commitresp{out: outs, err: errs, ok: false}
		}
		ctx.LogCommitTime("Write config", writeStart)
		// W2: the commit record is cleared (unless persisted_unsynced was
		// written) and the applied components are no longer dirty. A failure is
		// reported, not hidden, and does not undo the successful apply.
		if !jr.unsynced {
			if err := journalFinish(jr.j, jr.id, jr.rep, true, ""); err != nil {
				outs = append(outs, journalWarning("end of the commit", err))
			}
		}
		// A successful commit that emitted a dirty-component warning says how
		// to repair (design 2 and 7.1).
		if hasDirtyWarning(outs) {
			outs = append(outs, m.reconcileHint()...)
		}
	} else {
		m.running.Store(effective)
		m.writeRunning(ctx.ctx)
		ctx.LogCommitTime("Write config", writeStart)
	}

	// Run post-hooks after we've written out the running cfg
	postCmtHookStart := time.Now()
	env = append(env, "COMMIT_COMMENT="+ctx.message)
	hout, herr = ctx.execute_hooks("/etc/commit/post-hooks.d", env)
	outs = append(outs, hout)
	if herr != nil {
		errs = append(errs, herr)
	}

	ctx.LogCommitTime("Post-commit hooks", postCmtHookStart)
	ctx.LogCommitTime("Commit OVERALL", commitStart)
	ctx.LogCommitTime("End of validation and commit", overallStart)

	// errs here are warnings, so we return true in all cases as the commit
	// will have been committed if we have got this far.
	return &commitresp{out: outs, err: errs, ok: true}
}

func (m *CommitMgr) run() {
	var done struct{}
	var inCommit bool
	donech := make(chan struct{})
	for {
		select {
		case req := <-m.reqch:
			if inCommit {
				err := mgmterror.NewResourceDeniedProtocolError()
				err.Message = "Commit already in progress"
				req.resp <- MakeCommitError(err)
				break
			}
			inCommit = true
			go func(r commitmgrreq) {
				var resp *commitresp
				if r.startup {
					outs, err := m.StartupPass(r.ctx)
					resp = &commitresp{out: outs, ok: err == nil}
					if err != nil {
						resp.err = []error{err}
					}
				} else if r.reconcile {
					outs, err := m.Reconcile(r.ctx)
					resp = &commitresp{out: outs, ok: err == nil}
					if err != nil {
						resp.err = []error{err}
					}
				} else {
					resp = m.commit(r.sid, r.ctx, r.t, r.message, r.debug)
				}
				donech <- done
				r.resp <- resp
			}(req)
		case <-donech:
			inCommit = false
		}
	}
}

func (m *CommitMgr) Commit(sid string, ctx *configd.Context, candidate *data.Node, message string, debug bool) *commitresp {
	respch := make(chan *commitresp)
	m.reqch <- commitmgrreq{
		sid:     sid,
		ctx:     ctx,
		t:       candidate,
		resp:    respch,
		message: message,
		debug:   debug,
	}
	return <-respch
}

// ReconcileSerialised runs the internal reconcile apply (Reconcile) through the
// commit loop: it is refused with "Commit already in progress" while a commit
// (or another reconcile) runs, and it is the only way the RPC reaches Reconcile.
func (m *CommitMgr) ReconcileSerialised(ctx *configd.Context) ([]*exec.Output, error) {
	respch := make(chan *commitresp)
	m.reqch <- commitmgrreq{ctx: ctx, resp: respch, reconcile: true}
	resp := <-respch
	if len(resp.err) > 0 {
		return resp.out, resp.err[0]
	}
	return resp.out, nil
}

// StartupSerialised runs StartupPass through the commit loop: refused with
// "Commit already in progress" while a commit, a reconcile or another startup
// pass runs, and it is the entry the later main-wiring stage must call (not
// StartupPass beside a running commit, not ReconcileSerialised, which keeps
// calling Reconcile and therefore does not finish the startup gate).
func (m *CommitMgr) StartupSerialised(ctx *configd.Context) ([]*exec.Output, error) {
	respch := make(chan *commitresp)
	m.reqch <- commitmgrreq{ctx: ctx, resp: respch, startup: true}
	resp := <-respch
	if len(resp.err) > 0 {
		return resp.out, resp.err[0]
	}
	return resp.out, nil
}

func (m *CommitMgr) Running() *data.Node {
	return m.running.Load()
}

func MakeCommitError(err error) *commitresp {
	return &commitresp{
		err: []error{err},
	}
}
