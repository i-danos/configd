// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-F (F8): the stale commit record left by a crash is consumed by the
// restart classification (design 6.3, transition (b)); it must not survive to
// re-dirty components that were already reconciled at the next restart, and the
// next commit of the same process rewrites the whole file (transition (a)).

package session

import (
	"testing"

	"github.com/danos/config/schema"
)

func TestRestartClassificationConsumesTheStaleCommitRecord(t *testing.T) {
	for _, state := range []string{"applying", "persisting", "persisted_unsynced"} {
		t.Run(state, func(t *testing.T) {
			path := plantJournal(t, state, false)
			if err := recoverJournalAtStart(openAt(t, path)); err != nil {
				t.Fatal(err)
			}
			st := reopen(t, path)
			if st.Commit != nil {
				t.Errorf("the stale %s record is still in the journal: %+v", state, st.Commit)
			}
			for _, c := range []string{"a", "b"} {
				if st.Dirty[c].Class != dirtyCrashWindow {
					t.Errorf("%s was not classified crash_window: %+v", c, st.Dirty[c])
				}
			}
		})
	}
}

func TestSecondRestartDoesNotRedirtyReconciledComponents(t *testing.T) {
	for _, firstEmpty := range []bool{false, true} {
		path := plantJournal(t, "applying", firstEmpty)
		if err := recoverJournalAtStart(openAt(t, path)); err != nil {
			t.Fatal(err)
		}
		// A reconcile (or a committed commit) repairs both components.
		j := openAt(t, path)
		if err := j.Update(func(s *JournalState) bool {
			delete(s.Dirty, "a")
			delete(s.Dirty, "b")
			return true
		}); err != nil {
			t.Fatal(err)
		}
		// configd restarts again.
		if err := recoverJournalAtStart(openAt(t, path)); err != nil {
			t.Fatal(err)
		}
		st := reopen(t, path)
		for _, c := range []string{"a", "b"} {
			if d, ok := st.Dirty[c]; ok {
				t.Errorf("firstEmpty=%v: %s was dirtied again by the stale record: %+v", firstEmpty, c, d)
			}
		}
	}
}

func TestNextCommitOfTheSameProcessRewritesTheFileAndClearsTheStaleRecord(t *testing.T) {
	path := plantJournal(t, "persisting", false)
	j := openAt(t, path) // same process: no restart classification
	if err := journalBegin(j, "c-next", []string{"a"}, false); err != nil {
		t.Fatal(err)
	}
	rep := &schema.ApplyReport{}
	if err := journalFinish(j, "c-next", rep, true, ""); err != nil {
		t.Fatal(err)
	}
	st := reopen(t, path)
	if st.Commit != nil {
		t.Errorf("the stale record survived the next commit: %+v", st.Commit)
	}
	if st.Dirty["other"].Reason != "keep" {
		t.Errorf("unrelated dirty entry lost: %+v", st.Dirty["other"])
	}
}
