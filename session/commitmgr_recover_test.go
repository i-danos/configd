// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package session

import (
	"reflect"
	"testing"

	"github.com/danos/config/schema"
)

func openState(state string, firstEmpty bool) *JournalState {
	st := newJournalState()
	st.Commit = &JournalCommit{ID: "c-77", State: state, Todo: []string{"a", "b"},
		FirstEmptyRtree: firstEmpty}
	st.Dispatch["a"] = JournalDispatch{Seq: 3, Kind: schema.KindForward,
		MayHaveBeenSent: true, TRecordMonoUs: 42, Settled: false}
	st.Dirty["other"] = JournalDirty{Class: schema.DirtyUnverified, Reason: "keep", CommitID: "old", Since: "t0"}
	return &st
}

func wantClass(t *testing.T, st *JournalState, class schema.DirtyClass) {
	t.Helper()
	for _, c := range []string{"a", "b"} {
		d := st.Dirty[c]
		if d.Class != class || d.Reason != "open commit at restart" || d.CommitID != "c-77" || d.Since != "" {
			t.Errorf("%s: %+v, want class %s, reason 'open commit at restart', commit c-77, empty since", c, d, class)
		}
	}
}

func TestRecoverOpenCommitClassifiesTodoAsCrashWindow(t *testing.T) {
	for _, state := range []string{"applying", "persisting", "persisted_unsynced"} {
		t.Run(state, func(t *testing.T) {
			st := openState(state, false)
			disp := st.Dispatch["a"]
			commit := *st.Commit
			if !recoverOpenCommit(st) {
				t.Fatalf("returned false")
			}
			wantClass(t, st, dirtyCrashWindow)
			if st.Dispatch["a"] != disp || len(st.Dispatch) != 1 || st.Dispatch["a"].Settled {
				t.Errorf("dispatch changed: %+v", st.Dispatch)
			}
			if d := st.Dirty["other"]; d != (JournalDirty{Class: schema.DirtyUnverified, Reason: "keep", CommitID: "old", Since: "t0"}) {
				t.Errorf("non-todo dirty changed: %+v", d)
			}
			if st.Commit == nil || !reflect.DeepEqual(*st.Commit, commit) {
				t.Errorf("commit record changed: %+v", st.Commit)
			}
		})
	}
}

func TestRecoverOpenCommitFirstEmptyRtreeIsBootPartial(t *testing.T) {
	st := openState("applying", true)
	if !recoverOpenCommit(st) {
		t.Fatal("returned false")
	}
	wantClass(t, st, schema.DirtyBootPartial) // a dispatch record exists on "a": still boot_partial
	if st.Commit == nil || st.Commit.State != "applying" {
		t.Errorf("commit record changed: %+v", st.Commit)
	}
}

func TestRecoverOpenCommitDoesNothingWithoutAnOpenRecord(t *testing.T) {
	var nilState *JournalState
	cases := map[string]*JournalState{"nil state": nilState}
	nilCommit := openState("applying", false)
	nilCommit.Commit = nil
	cases["nil commit"] = nilCommit
	empty := openState("applying", false)
	empty.Commit.Todo = nil
	cases["empty todo"] = empty
	done := openState("done", false)
	cases["state done"] = done
	cases["state empty"] = openState("", false)
	for name, st := range cases {
		t.Run(name, func(t *testing.T) {
			var before JournalState
			if st != nil {
				before = st.clone()
			}
			if recoverOpenCommit(st) {
				t.Errorf("returned true")
			}
			if st != nil && !reflect.DeepEqual(*st, before) {
				t.Errorf("state mutated: %+v vs %+v", *st, before)
			}
		})
	}
}

func TestRecoverOpenCommitTwiceKeepsCrashWindowAndTheRecord(t *testing.T) {
	st := openState("persisting", false)
	recoverOpenCommit(st)
	if !recoverOpenCommit(st) {
		t.Fatal("second call returned false")
	}
	wantClass(t, st, dirtyCrashWindow)
	if st.Commit == nil || st.Commit.State != "persisting" {
		t.Errorf("commit record cleared or changed: %+v", st.Commit)
	}
}

func TestRecoverOpenCommitWritesIntoANilDirtyMap(t *testing.T) {
	st := &JournalState{Commit: &JournalCommit{ID: "c-77", State: "applying", Todo: []string{"a", "b"}}}
	if st.Dirty != nil || st.Dispatch != nil {
		t.Fatal("setup")
	}
	if !recoverOpenCommit(st) {
		t.Fatal("returned false")
	}
	wantClass(t, st, dirtyCrashWindow)
	if st.Dispatch != nil {
		t.Errorf("a nil dispatch map was initialised")
	}
}
