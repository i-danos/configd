// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 3, stage 3-B2: the part of CommitMgr.commit that uses the commit
// journal and the dispatch recorder. It only runs when the component manager
// implements schema.CompensatingComponentManager; otherwise commit keeps the
// phase-2 path (ComponentSetRunningChecked) with no journal at all.

package session

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/danos/config/data"
	"github.com/danos/config/schema"
	"github.com/danos/config/union"
	"github.com/danos/configd"
	"github.com/danos/mgmterror"
	"github.com/danos/utils/exec"
	"github.com/danos/yang/data/datanode"
)

var commitSeq uint64

func newCommitID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), atomic.AddUint64(&commitSeq, 1))
}

// journalFor returns the journal and the recorder, opening them on first use.
// The journal lives next to the runfile this process already uses.
func (m *CommitMgr) journalFor(sctx *configd.Context) (*Journal, schema.DispatchRecorder, error) {
	if m.journal == nil {
		open := m.openJournal
		if open == nil {
			open = OpenJournal
		}
		j, err := open(JournalPathFor(sctx.Config.Runfile))
		if err != nil {
			return nil, nil, err
		}
		// First open by this process (design 6.3, transition (b)): an open
		// commit record belongs to a previous process. A failure here leaves
		// m.journal and m.recorder unset, so the caller fails closed.
		if err := recoverJournalAtStart(j); err != nil {
			return nil, nil, err
		}
		m.journal = j
	}
	if m.recorder == nil {
		unitFor := func(string) string { return "" }
		if sctx.Config != nil && sctx.Config.ServiceUnitFor != nil {
			unitFor = sctx.Config.ServiceUnitFor
		}
		m.recorder = NewJournalRecorder(m.journal, m.identityReader(unitFor), nil)
	}
	return m.journal, m.recorder, nil
}

func isEmptyTree(t *data.Node) bool {
	return t == nil || t.NumChildren() == 0
}

// commitTodo lists the components this commit will apply, in apply order, for
// the journal's commit record. schema's own list is private; this one is
// built from the same exported mappings (changed namespaces plus the default
// component). The report's real results are what W2 uses.
func commitTodo(cm schema.ComponentManager, changed *map[string]bool) []string {
	mp := cm.GetComponentNSMappings()
	if mp == nil {
		return nil
	}
	ordered := mp.OrderedComponents()
	if changed == nil {
		return append([]string(nil), ordered...)
	}
	set := map[string]bool{mp.DefaultComponent(): true}
	for ns := range *changed {
		if name, ok := mp.GetModelNameForNamespace(ns); ok {
			set[name] = true
		}
	}
	var todo []string
	for _, c := range ordered {
		if set[c] {
			todo = append(todo, c)
		}
	}
	return todo
}

// journalBegin is W1: the commit record, written before any component call.
func journalBegin(j *Journal, id string, todo []string, firstEmpty bool) error {
	return j.Update(func(s *JournalState) bool {
		s.Commit = &JournalCommit{ID: id, State: "applying",
			Todo: append([]string(nil), todo...), FirstEmptyRtree: firstEmpty}
		return true
	})
}

// journalFinish is W2: the commit record is cleared and the dirty set follows
// the report. committed says the commit went through: then every component
// that was applied is no longer dirty.
//
// keepState, when not empty, leaves the commit record in place with that state
// instead of clearing it (persisted_unsynced, design 6.3).
func journalFinish(j *Journal, id string, rep *schema.ApplyReport, committed bool, keepState string) error {
	return j.UpdateKeepDirty(func(s *JournalState) bool {
		if keepState != "" && s.Commit != nil {
			s.Commit.State = keepState
		} else {
			s.Commit = nil
		}
		if rep == nil {
			return true
		}
		for _, c := range rep.Clean {
			delete(s.Dirty, c)
		}
		if committed {
			for _, r := range rep.Results {
				if r.Forward == schema.OutcomeApplied {
					delete(s.Dirty, r.Component)
				}
			}
		}
		now := time.Now().UTC().Format(time.RFC3339)
		for _, d := range rep.Dirty {
			s.Dirty[d.Component] = JournalDirty{Class: d.Class, Reason: d.Reason,
				CommitID: id, Since: now}
		}
		return true
	})
}

func journalWarning(what string, err error) *exec.Output {
	return &exec.Output{Output: fmt.Sprintf(
		"WARNING: the commit journal could not be updated (%s): %s; "+
			"after a restart the components of this commit are treated as unknown",
		what, err)}
}

// compensatingResult is what applyCompensating hands back to commit.
type compensatingResult struct {
	failed bool
	outs   []*exec.Output
	errs   []error
	j      *Journal
	id     string
	rep    *schema.ApplyReport
	cm     schema.CompensatingComponentManager
	pol    schema.CompensationPolicy
	prev   func() datanode.DataNode
	// unsynced: the runfile was renamed but its directory sync failed.
	unsynced bool
}

// applyCompensating is steps 3-4 of design section 11.2: the commit record
// (W1), then ComponentSetRunningCompensated with the dispatch recorder. A
// failure is final for the commit: nothing after it (commit scripts, running
// store, runfile, post-hooks) runs. A forward failure is compensated inside
// the schema call; there is no second compensation loop here.
func (m *CommitMgr) applyCompensating(
	cm schema.CompensatingComponentManager,
	sctx *configd.Context,
	ctx *commitctx,
	ucan union.Node,
	rtree *data.Node,
	changedNSMap *map[string]bool,
	forced []string,
	soft []string,
	firstCommit bool,
) *compensatingResult {

	res := &compensatingResult{}
	fail := func(msg string) *compensatingResult {
		e := mgmterror.NewOperationFailedApplicationError()
		e.Message = msg
		res.failed = true
		res.errs = append(res.errs, e)
		return res
	}

	j, rec, err := m.journalFor(sctx)
	if err != nil {
		return fail(fmt.Sprintf("cannot open the commit journal: %s; no component was called", err))
	}
	res.j = j
	res.id = newCommitID()

	firstEmpty := firstCommit && isEmptyTree(rtree)
	todo := todoWithForced(sctx.CompMgr, changedNSMap, append(append([]string(nil), forced...), soft...))
	if err := journalBegin(j, res.id, todo, firstEmpty); err != nil {
		return fail(fmt.Sprintf("cannot record the commit in the journal: %s; no component was called", err))
	}

	pol := schema.CompensationPolicy{SkipCompensation: firstEmpty, Opportunistic: soft}
	prev := func() datanode.DataNode {
		return union.NewNode(nil, rtree, m.schema, nil, 0)
	}
	rep, cerrs := cm.ComponentSetRunningCompensated(
		m.schema, ucan, prev, changedNSMap, forced, pol, rec, ctx.LogCommitTime)
	res.rep = rep
	res.outs = append(res.outs, softWarnings(rep)...)
	res.cm, res.pol, res.prev = cm, pol, prev
	if len(cerrs) > 0 {
		res.failed = true
		res.errs = append(res.errs, cerrs...)
		if err := journalFinish(j, res.id, rep, false, ""); err != nil {
			res.outs = append(res.outs, journalWarning("end of a failed commit", err))
		}
	}
	return res
}

// renderRunning renders the running text of the tree that is about to become
// running, without storing it first: the effective session's candidate is
// empty at this point, so its Show is the union of an empty overlay with the
// stored running tree, which is what this builds directly from the tree.
func renderRunning(effective *data.Node, sch schema.ModelSet) (string, error) {
	return union.NewNode(data.New("root"), effective, sch, nil, 0).Show([]string{})
}

// persistRunning replaces the runfile by the rendering of effective: the
// render error is returned like every file error. The write is the journal's
// atomic one: temp file 0600 in the runfile directory, write all bytes, fsync,
// close, rename, directory fsync; the temp file is removed when a step before
// the rename fails. A *JournalWriteError names the step; Replaced is true only
// when the new runfile is in place but the directory fsync failed.
func persistRunning(fs journalFS, runfile string, effective *data.Node, sch schema.ModelSet) error {
	text, err := renderRunning(effective, sch)
	if err != nil {
		return &JournalWriteError{Step: "render", Err: err}
	}
	return atomicWriteFile(fs, runfile, []byte(text))
}

func persistFailure(err error) string {
	var we *JournalWriteError
	if errors.As(err, &we) {
		return fmt.Sprintf("could not persist the running configuration: %s: %s", we.Step, we.Err)
	}
	return fmt.Sprintf("could not persist the running configuration: %s", err)
}

// persistJournaled is steps 6-8 of design 11.2 for the journaled path. It
// returns the extra outputs and, when the commit failed, the errors (the
// running tree was never stored, the runfile is unchanged, the applied
// components were compensated once through CompensateReport).
func (m *CommitMgr) persistJournaled(cr *compensatingResult, sctx *configd.Context,
	ctx *commitctx, effective *data.Node) (outs []*exec.Output, errs []error, failed bool) {

	fs := m.runfileFS
	if fs == nil {
		fs = osJournalFS{}
	}
	var perr error
	// commit.state = persisting; a journal failure here is handled like a persist
	// failure: the components are already applied and must be compensated.
	if err := cr.j.Update(func(s *JournalState) bool {
		if s.Commit == nil {
			s.Commit = &JournalCommit{ID: cr.id}
		}
		s.Commit.State = "persisting"
		return true
	}); err != nil {
		perr = &JournalWriteError{Step: "journal", Err: err}
	} else {
		perr = persistRunning(fs, sctx.Config.Runfile, effective, m.schema)
	}

	var we *JournalWriteError
	if perr == nil {
		m.running.Store(effective)
		return nil, nil, false
	}
	if errors.As(perr, &we) && we.Replaced {
		// The new runfile is in place; only its durability is unconfirmed.
		m.running.Store(effective)
		if err := journalFinish(cr.j, cr.id, cr.rep, true, "persisted_unsynced"); err != nil {
			outs = append(outs, journalWarning("persisted_unsynced", err))
		}
		outs = append(outs, &exec.Output{Output: fmt.Sprintf(
			"WARNING: the running configuration was written but the directory sync failed (%s); "+
				"the commit is kept and the journal marks it persisted_unsynced", we.Err)})
		cr.unsynced = true
		return outs, nil, false
	}

	e := mgmterror.NewOperationFailedApplicationError()
	e.Message = persistFailure(perr)
	errs = append(errs, e)
	rep2, cerrs := cr.cm.CompensateReport(m.schema, cr.prev, cr.rep, cr.pol, m.recorder, ctx.LogCommitTime)
	errs = append(errs, cerrs...)
	if err := journalFinish(cr.j, cr.id, rep2, false, ""); err != nil {
		outs = append(outs, journalWarning("end of a commit that could not be persisted", err))
	}
	return outs, errs, true
}

func (m *CommitMgr) identityReader(unitFor func(string) string) IdentityReader {
	if m.identities != nil {
		return m.identities
	}
	return NewSystemdIdentityReader(unitFor)
}

// fenceCheck is the ordinary-commit fence gate of design 6.4 for todo plus the
// blocking dirty components (fenceTargets). It opens the journal (which creates
// no file) and writes nothing. On success it returns the forced list: the dirty
// components whose role is exactly "prerequisite", "bridge" or "dependant" (design
// 7.2 items 3, 2 and 4), which the commit re-applies
// through the forced argument of ComponentSetRunningCompensated.
func (m *CommitMgr) fenceCheck(sctx *configd.Context, changed *map[string]bool) ([]string, []error) {
	j, _, err := m.journalFor(sctx)
	if err != nil {
		e := mgmterror.NewOperationFailedApplicationError()
		e.Message = fmt.Sprintf("cannot open the commit journal: %s; no component was called", err)
		return nil, []error{e}
	}
	unitFor := sctx.Config.ServiceUnitFor
	st := j.State()
	targets := fenceTargets(sctx.CompMgr.GetComponentNSMappings(), st.Dirty, commitTodo(sctx.CompMgr, changed))
	names := make([]string, 0, len(targets))
	roles := map[string]string{}
	for _, t := range targets {
		names = append(names, t.Component)
		roles[t.Component] = t.Role
	}
	var unfenced []FenceVerdict
	for _, v := range fenceGate(st, m.identityReader(unitFor), names) {
		if !v.Fenced {
			unfenced = append(unfenced, v)
		}
	}
	if len(unfenced) > 0 {
		return nil, []error{fenceRefusal(unfenced, roles, unitFor)}
	}
	var forced []string
	for _, t := range targets {
		if t.Role == "prerequisite" || t.Role == "bridge" || t.Role == "dependant" {
			forced = append(forced, t.Component)
		}
	}
	return forced, nil
}

// todoWithForced is the commit record's todo: commitTodo plus the forced
// prerequisites, in apply order, so a crash during a repair call is visible.
func todoWithForced(cm schema.ComponentManager, changed *map[string]bool, forced []string) []string {
	base := commitTodo(cm, changed)
	if len(forced) == 0 {
		return base
	}
	want := map[string]bool{}
	for _, c := range base {
		want[c] = true
	}
	for _, c := range forced {
		want[c] = true
	}
	var out []string
	if mp := cm.GetComponentNSMappings(); mp != nil {
		for _, c := range mp.OrderedComponents() {
			if want[c] {
				out = append(out, c)
			}
		}
	}
	return out
}

// opportunisticPlan picks the dirty independent components (design 7.2 item 5)
// that are fenced: they are repaired opportunistically. An unfenced one is
// skipped with a warning in the outputs; it never fails the commit. Reads the
// journal state only.
func (m *CommitMgr) opportunisticPlan(sctx *configd.Context, changed *map[string]bool) (soft []string, warns []*exec.Output) {
	j, _, err := m.journalFor(sctx)
	if err != nil {
		return nil, nil // fenceCheck has already reported a journal problem
	}
	st := j.State()
	indep := independentTargets(sctx.CompMgr.GetComponentNSMappings(), st.Dirty, commitTodo(sctx.CompMgr, changed))
	isIndep := map[string]bool{}
	for _, c := range indep {
		isIndep[c] = true
	}
	inSoft := map[string]bool{}
	mp := sctx.CompMgr.GetComponentNSMappings()
	for _, v := range fenceGate(st, m.identityReader(sctx.Config.ServiceUnitFor), indep) {
		if v.Fenced {
			// A prerequisite that is itself an independent dirty component
			// but is not going to be repaired (unfenced, or itself blocked)
			// blocks this one (design 7.2 item 5). Failures during the apply
			// are handled by the schema loop.
			blocker := ""
			for _, p := range mp.Prerequisites(v.Component) {
				if isIndep[p] && !inSoft[p] {
					blocker = p
					break
				}
			}
			if blocker == "" {
				inSoft[v.Component] = true
				soft = append(soft, v.Component)
				continue
			}
			warns = append(warns, &exec.Output{Output: fmt.Sprintf(
				"WARNING: dirty component %s was not repaired: blocked by %s", v.Component, blocker)})
			continue
		}
		warns = append(warns, &exec.Output{Output: fmt.Sprintf(
			"WARNING: dirty component %s was not repaired: it may still be executing an earlier Set "+
				"and is not fenced (%s); restart its service to allow the repair", v.Component, v.Reason)})
	}
	return soft, warns
}

// softWarnings turns the failed opportunistic repairs of a report into output
// warnings (never errors).
func softWarnings(rep *schema.ApplyReport) []*exec.Output {
	if rep == nil {
		return nil
	}
	var out []*exec.Output
	for _, r := range rep.Results {
		if r.Soft && r.Forward == schema.OutcomeNotAttempted && strings.HasPrefix(r.Note, "blocked by ") {
			out = append(out, &exec.Output{Output: fmt.Sprintf(
				"WARNING: dirty component %s was not repaired: %s", r.Component, r.Note)})
			continue
		}
		if r.Soft && r.Forward != schema.OutcomeApplied && r.Forward != schema.OutcomeNotAttempted {
			out = append(out, &exec.Output{Output: fmt.Sprintf(
				"WARNING: repair of dirty component %s failed (%s): %v; it stays dirty",
				r.Component, r.Forward, r.ForwardErr)})
		}
	}
	return out
}

// reconcileHintText is the phrase of design 2 / 7.1.
const reconcileHintText = "run 'reconcile' to repair"

// reconcileHint is one extra out line, only when the journal still lists a
// dirty component. It is never an error and never replaces one.
func (m *CommitMgr) reconcileHint() []*exec.Output {
	if m.journal == nil || len(m.journal.State().Dirty) == 0 {
		return nil
	}
	return []*exec.Output{{Output: "Hint: " + reconcileHintText + " the dirty components"}}
}

// hasDirtyWarning reports whether outs already carries one of the
// dirty-component WARNING lines of this file.
func hasDirtyWarning(outs []*exec.Output) bool {
	for _, o := range outs {
		if o != nil && (strings.HasPrefix(o.Output, "WARNING: dirty component") ||
			strings.HasPrefix(o.Output, "WARNING: repair of dirty component")) {
			return true
		}
	}
	return false
}
