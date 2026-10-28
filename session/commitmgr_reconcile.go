// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 3, stage 3-B11: the internal reconcile apply (design 6.4 item 2,
// 7.1 behaviour, section 5 boot_partial exclusion). It is what a future
// Reconcile RPC and the startup pass will call; nothing registers it yet. It
// takes no lock of its own (the caller serialises, like for commit), never
// reads the candidate, runs no hooks or scripts, does not Save, creates no
// confirmed-commit timer and writes neither the runfile nor the running tree.

package session

import (
	"fmt"

	"github.com/danos/config/schema"
	"github.com/danos/config/union"
	"github.com/danos/configd"
	"github.com/danos/utils/exec"
	"github.com/danos/yang/data/datanode"
)

func reconcileLine(format string, args ...interface{}) *exec.Output {
	return &exec.Output{Output: fmt.Sprintf(format, args...)}
}

// Reconcile re-applies the running configuration to the dirty components that
// are fenced, not boot_partial and not blocked by a dirty prerequisite that is
// itself left out. C is empty: every reconcilable dirty component is
// independent. A component that stays dirty is an output line, never an error;
// the error is non-nil only when the journal cannot be opened or the component
// manager cannot reconcile.
func (m *CommitMgr) Reconcile(sctx *configd.Context) ([]*exec.Output, error) {
	cm, ok := sctx.CompMgr.(schema.CompensatingComponentManager)
	if !ok {
		return nil, fmt.Errorf("the component manager does not support reconcile")
	}
	j, rec, err := m.journalFor(sctx)
	if err != nil {
		return nil, fmt.Errorf("cannot open the commit journal: %w", err)
	}
	st := j.State()
	mp := sctx.CompMgr.GetComponentNSMappings()
	var dirty []string
	if mp != nil {
		for _, c := range mp.OrderedComponents() {
			if _, d := st.Dirty[c]; d {
				dirty = append(dirty, c)
			}
		}
	}
	if len(dirty) == 0 {
		return []*exec.Output{reconcileLine("No dirty components to reconcile")}, nil
	}

	var outs []*exec.Output
	skipped := map[string]bool{}
	var cand []string
	verdicts := map[string]FenceVerdict{}
	for _, v := range fenceGate(st, m.identityReader(sctx.Config.ServiceUnitFor), dirty) {
		verdicts[v.Component] = v
	}
	for _, c := range dirty {
		d := st.Dirty[c]
		switch {
		case d.Class == schema.DirtyBootPartial:
			skipped[c] = true
			outs = append(outs, reconcileLine(
				"Not reconciled: %s (boot_partial: the boot configuration is retried by a commit, not reconciled)", c))
		case !verdicts[c].Fenced:
			skipped[c] = true
			outs = append(outs, reconcileLine(
				"Not reconciled: %s (not fenced: it may still be executing an earlier Set; restart its service)", c))
		default:
			cand = append(cand, c)
		}
	}
	// A fenced component whose prerequisite is dirty and left out is not
	// passed to the schema (which would not know it was left out).
	var list []string
	inList := map[string]bool{}
	for _, c := range cand {
		blocker := ""
		for _, p := range mp.Prerequisites(c) {
			if _, pd := st.Dirty[p]; pd && !inList[p] {
				blocker = p
				break
			}
		}
		if blocker != "" {
			outs = append(outs, reconcileLine("Not reconciled: %s (blocked by %s)", c, blocker))
			continue
		}
		inList[c] = true
		list = append(list, c)
	}
	if len(list) == 0 {
		return outs, nil
	}

	target := func() datanode.DataNode {
		return union.NewNode(nil, m.Running(), m.schema, nil, 0)
	}
	rep, _ := cm.ComponentReconcile(m.schema, target, list, schema.CompensationPolicy{}, rec, nil)
	for _, r := range rep.Results {
		if r.Forward == schema.OutcomeApplied &&
			(r.Verified == schema.VerifyMatched || r.Verified == schema.VerifyNotVerifiable) {
			outs = append(outs, reconcileLine("Reconciled: %s", r.Component))
			continue
		}
		reason := string(r.Forward)
		switch {
		case r.Note != "":
			reason = r.Note
		case r.Forward == schema.OutcomeApplied:
			reason = "read-back " + string(r.Verified)
		case r.ForwardErr != nil:
			reason = r.ForwardErr.Error()
		}
		outs = append(outs, reconcileLine("Not reconciled: %s (%s)", r.Component, reason))
	}
	id := newCommitID()
	if err := j.UpdateKeepDirty(func(s *JournalState) bool {
		for _, c := range rep.Clean {
			delete(s.Dirty, c)
		}
		for _, d := range rep.Dirty {
			if d.Class == schema.DirtyBlocked {
				continue // keep the earlier class (schema's rule for DirtyBlocked)
			}
			old := s.Dirty[d.Component]
			s.Dirty[d.Component] = JournalDirty{Class: d.Class, Reason: d.Reason,
				CommitID: id, Since: old.Since}
		}
		return true
	}); err != nil {
		outs = append(outs, journalWarning("end of the reconcile", err))
	}
	return outs, nil
}
