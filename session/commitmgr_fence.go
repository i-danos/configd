// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 3, stages 3-B4/3-B5: the fence gate of the ordinary commit (design
// section 6.4, item 1) for the components of the commit's todo plus the dirty
// components that section 7.2 classifies as blocking (fenceTargets). The gate
// only checks; repair of blocking components is limited to prerequisites (3-B6,
// the forced argument) and is done by the commit, not here. There is no
// crashSettle wait: the gate never waits.

package session

import (
	"fmt"
	"strings"
	"time"

	"github.com/danos/config/schema"
	"github.com/danos/mgmterror"
)

const (
	// fenceSendWindow is the schema layer's default send window (2 s,
	// CompensationPolicy.SendWindow); fenceMargin is the 1 s margin of 6.4.
	fenceSendWindow = 2 * time.Second
	fenceMargin     = time.Second
)

// FenceVerdict is the outcome for one component.
type FenceVerdict struct {
	Component string
	Fenced    bool
	Reason    string
}

// dirtyCrashWindow is the class design 6.1 assigns after a restart; schema has
// no constant for it (written by the later recovery stage).
const dirtyCrashWindow schema.DirtyClass = "crash_window"

// fenceRequiring: a dirty class that says a call may still be executing in the
// component (design 6.1). Components without such a dirty entry are not
// fence-requiring and therefore fenced.
func fenceRequiring(c schema.DirtyClass) bool {
	switch c {
	case schema.DirtyUnknown, schema.DirtyUnsettled, schema.DirtyUnfenced, dirtyCrashWindow:
		return true
	}
	return false
}

// fenceGate evaluates the verdict of every listed component against the journal
// state. The identity is read (through ids) only when everything else leaves
// the component unproven. The gate itself never marks a component dirty and
// never waits.
func fenceGate(st JournalState, ids IdentityReader, components []string) []FenceVerdict {
	out := make([]FenceVerdict, 0, len(components))
	for _, c := range components {
		out = append(out, fenceOne(st, ids, c))
	}
	return out
}

func fenceOne(st JournalState, ids IdentityReader, c string) FenceVerdict {
	v := func(fenced bool, why string) FenceVerdict { return FenceVerdict{c, fenced, why} }
	d, dirty := st.Dirty[c]
	if !dirty || !fenceRequiring(d.Class) {
		return v(true, "not fence-requiring")
	}
	disp, ok := st.Dispatch[c]
	if !ok {
		return v(true, "no dispatch record")
	}
	if !disp.MayHaveBeenSent {
		return v(true, "call was not sent")
	}
	if disp.Settled {
		return v(true, "call settled")
	}
	if ids == nil {
		return v(false, "identity cannot be read")
	}
	id, err := ids.Identity(c)
	if err != nil || !id.Known {
		return v(false, "identity cannot be read")
	}
	limit := disp.TRecordMonoUs + (fenceSendWindow + fenceMargin).Microseconds()
	if id.ActiveEnterMonoUs > limit {
		return v(true, "service restarted after the call could have been delivered")
	}
	return v(false, "service not restarted since the call, or restarted inside the uncertain window")
}

// fenceRefusal builds the commit error for the unfenced components.
func fenceRefusal(unfenced []FenceVerdict, roles map[string]string, unitFor func(string) string) error {
	var parts []string
	for _, u := range unfenced {
		unit := "its service"
		if unitFor != nil {
			if s := unitFor(u.Component); s != "" {
				unit = "the service " + s
			}
		}
		role := ""
		if r := roles[u.Component]; r != "" && r != "changed" {
			role = " [blocking " + r + " of this commit]"
		}
		parts = append(parts, fmt.Sprintf(
			"component %s%s may still be executing an earlier Set and is not fenced (%s); "+
				"restart %s (systemctl restart) and retry", u.Component, role, u.Reason, unit))
	}
	e := mgmterror.NewOperationFailedApplicationError()
	e.Message = strings.Join(parts, "; ") + "; nothing was sent and no hook or script ran"
	return e
}

// fenceTarget is a component the ordinary-commit gate checks and why.
type fenceTarget struct {
	Component string
	Role      string // changed | bridge | prerequisite | dependant
}

// fenceTargets is the extended fence set of design 6.4 item 1: the components
// of todo plus every dirty component that section 7.2 classifies as blocking
// for the changed set C (= todo): bridge (in both Anc(C) and Desc(C)),
// prerequisite (in Anc(C)) or dependant (in Desc(C)). An independent dirty
// component is not a target. Nothing is applied for the added components here.
func fenceTargets(mp *schema.ComponentMappings, dirty map[string]JournalDirty, todo []string) []fenceTarget {
	var out []fenceTarget
	inC := map[string]bool{}
	for _, c := range todo {
		inC[c] = true
		out = append(out, fenceTarget{c, "changed"})
	}
	if mp == nil {
		return out
	}
	anc := map[string]bool{}
	for _, c := range todo {
		for _, p := range mp.Prerequisites(c) {
			anc[p] = true
		}
	}
	// deterministic order: the apply order of the components
	for _, d := range mp.OrderedComponents() {
		if _, isDirty := dirty[d]; !isDirty || inC[d] {
			continue
		}
		desc := false
		for _, p := range mp.Prerequisites(d) {
			if inC[p] {
				desc = true
				break
			}
		}
		switch {
		case anc[d] && desc:
			out = append(out, fenceTarget{d, "bridge"})
		case anc[d]:
			out = append(out, fenceTarget{d, "prerequisite"})
		case desc:
			out = append(out, fenceTarget{d, "dependant"})
		}
	}
	return out
}

// independentTargets returns the dirty components that are independent of the
// changed set C = todo (design 7.2 item 5): not in C, not in Anc(C), not in
// Desc(C). They are not fence targets of the commit and not forced: they get an
// opportunistic repair (schema CompensationPolicy.Opportunistic) when fenced.
func independentTargets(mp *schema.ComponentMappings, dirty map[string]JournalDirty, todo []string) []string {
	if mp == nil {
		return nil
	}
	blocking := map[string]bool{}
	for _, t := range fenceTargets(mp, dirty, todo) {
		blocking[t.Component] = true
	}
	var out []string
	for _, d := range mp.OrderedComponents() {
		if _, isDirty := dirty[d]; isDirty && !blocking[d] {
			out = append(out, d)
		}
	}
	return out
}
