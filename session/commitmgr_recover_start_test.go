// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package session

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/danos/config/schema"
)

func discardLog(string, ...interface{}) {}

// plantJournal writes an open commit record into a fresh journal file in a
// private directory and returns the path.
func plantJournal(t *testing.T, state string, firstEmpty bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "commit-state.json")
	j, err := OpenJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Update(func(s *JournalState) bool {
		s.Commit = &JournalCommit{ID: "c-9", State: state, Todo: []string{"a", "b"}, FirstEmptyRtree: firstEmpty}
		s.Dispatch["a"] = JournalDispatch{Seq: 5, Kind: schema.KindForward, MayHaveBeenSent: true,
			TRecordMonoUs: 77, Settled: false}
		s.Dirty["other"] = JournalDirty{Class: schema.DirtyUnverified, Reason: "keep", CommitID: "old", Since: "t0"}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return path
}

func reopen(t *testing.T, path string) JournalState {
	t.Helper()
	j, err := openJournal(path, osJournalFS{}, discardLog)
	if err != nil {
		t.Fatal(err)
	}
	return j.State()
}

func openAt(t *testing.T, path string) *Journal {
	t.Helper()
	j, err := openJournal(path, osJournalFS{}, discardLog)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func checkClassified(t *testing.T, st JournalState, class schema.DirtyClass, state string) {
	t.Helper()
	for _, c := range []string{"a", "b"} {
		d := st.Dirty[c]
		if d.Class != class || d.Reason != "open commit at restart" || d.CommitID != "c-9" || d.Since != "" {
			t.Errorf("%s: %+v, want class %s", c, d, class)
		}
	}
	if d := st.Dirty["other"]; d != (JournalDirty{Class: schema.DirtyUnverified, Reason: "keep", CommitID: "old", Since: "t0"}) {
		t.Errorf("non-todo dirty changed: %+v", d)
	}
	disp := st.Dispatch["a"]
	if disp.Seq != 5 || !disp.MayHaveBeenSent || disp.Settled || disp.TRecordMonoUs != 77 || len(st.Dispatch) != 1 {
		t.Errorf("dispatch changed: %+v", st.Dispatch)
	}
	// Since stage 3-F (F8) the restart classification consumes the stale commit
	// record: it is cleared in the same atomic write that stores the classes.
	if st.Commit != nil {
		t.Errorf("the stale %s commit record was not consumed: %+v", state, st.Commit)
	}
}

func TestRecoverJournalAtStartPersistsCrashWindow(t *testing.T) {
	for _, state := range []string{"applying", "persisting", "persisted_unsynced"} {
		t.Run(state, func(t *testing.T) {
			path := plantJournal(t, state, false)
			if err := recoverJournalAtStart(openAt(t, path)); err != nil {
				t.Fatal(err)
			}
			checkClassified(t, reopen(t, path), dirtyCrashWindow, state)
		})
	}
}

func TestRecoverJournalAtStartFirstEmptyRtreePersistsBootPartial(t *testing.T) {
	path := plantJournal(t, "applying", true)
	if err := recoverJournalAtStart(openAt(t, path)); err != nil {
		t.Fatal(err)
	}
	checkClassified(t, reopen(t, path), schema.DirtyBootPartial, "applying")
}

func TestRecoverJournalAtStartNilJournalIsAnError(t *testing.T) {
	if err := recoverJournalAtStart(nil); err == nil {
		t.Errorf("a nil journal must be an error")
	}
}

func TestRecoverJournalAtStartWritesNothingWhenThereIsNothingToClassify(t *testing.T) {
	cases := map[string]func(*JournalState){
		"commit nil": func(s *JournalState) { s.Commit = nil },
		"empty todo": func(s *JournalState) { s.Commit = &JournalCommit{ID: "c", State: "applying"} },
		"state done": func(s *JournalState) {
			s.Commit = &JournalCommit{ID: "c", State: "done", Todo: []string{"a"}}
		},
	}
	for name, set := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "commit-state.json")
			j := openAt(t, path)
			if err := j.Update(func(s *JournalState) bool {
				set(s)
				s.Dirty["other"] = JournalDirty{Class: schema.DirtyUnverified, Reason: "keep"}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(path)
			if err := recoverJournalAtStart(openAt(t, path)); err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(path)
			if string(before) != string(after) {
				t.Errorf("file changed:\n%s\n%s", before, after)
			}
		})
	}
	// a missing file is not this function's job: nothing is created
	missing := filepath.Join(t.TempDir(), "commit-state.json")
	if err := recoverJournalAtStart(openAt(t, missing)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Errorf("a journal file was created (stat err %v)", err)
	}
}

func TestRecoverJournalAtStartTwiceKeepsTheClassificationAndWritesOnce(t *testing.T) {
	path := plantJournal(t, "persisting", false)
	for i := 0; i < 2; i++ {
		if err := recoverJournalAtStart(openAt(t, path)); err != nil {
			t.Fatal(err)
		}
	}
	checkClassified(t, reopen(t, path), dirtyCrashWindow, "persisting")
}

func TestRecoverJournalAtStartRenameFaultKeepsFileAndMemory(t *testing.T) {
	path := plantJournal(t, "applying", false)
	before, _ := os.ReadFile(path)
	fs := newFaultFS()
	j, err := openJournal(path, fs, discardLog)
	if err != nil {
		t.Fatal(err)
	}
	pre := j.State()
	fs.failOn["rename"] = errors.New("injected rename failure")
	if err := recoverJournalAtStart(j); err == nil {
		t.Fatalf("the rename fault must be returned")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Errorf("the old file changed")
	}
	if !reflect.DeepEqual(j.State(), pre) {
		t.Errorf("in-memory state moved: %+v vs %+v", j.State(), pre)
	}
	if left, _ := filepath.Glob(path + ".tmp"); len(left) != 0 {
		t.Errorf("temp file left: %v", left)
	}
}
