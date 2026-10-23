// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 3, stage 3-B18: restart classification of an open commit record
// (design 6.3, stale intent transition (b)). After a restart memory is lost,
// so every todo component of a commit record that is still in the journal
// becomes crash_window (boot_partial when the commit started on an empty
// running tree). A pure function on the journal state: nothing calls it yet,
// and the same-process next-commit path must never call it (transition (a)).

package session

import "github.com/danos/config/schema"

const openCommitAtRestart = "open commit at restart"

// recoverOpenCommit classifies the todo components of an open commit record.
// It only mutates st: no file I/O, no clock, no identity read, no fence gate,
// no reconcile. The commit record itself stays in place. It returns true when
// at least one component was classified.
func recoverOpenCommit(st *JournalState) bool {
	if st == nil || st.Commit == nil {
		return false
	}
	switch st.Commit.State {
	case "applying", "persisting", "persisted_unsynced":
	default:
		return false
	}
	if len(st.Commit.Todo) == 0 {
		return false
	}
	class := dirtyCrashWindow
	if st.Commit.FirstEmptyRtree {
		class = schema.DirtyBootPartial
	}
	if st.Dirty == nil {
		st.Dirty = map[string]JournalDirty{}
	}
	for _, c := range st.Commit.Todo {
		st.Dirty[c] = JournalDirty{Class: class, Reason: openCommitAtRestart,
			CommitID: st.Commit.ID, Since: ""}
	}
	return true
}
