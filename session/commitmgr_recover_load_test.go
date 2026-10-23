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
	"github.com/danos/configd"
)

func loadCtx(path string) *configd.Context {
	return &configd.Context{Config: &configd.Config{Runfile: filepath.Join(filepath.Dir(path), "running")}}
}

func TestJournalForClassifiesAnOpenCommitOnFirstOpenOnly(t *testing.T) {
	path := plantJournal(t, "applying", false)
	m := &CommitMgr{}
	ctx := loadCtx(path)
	j, _, err := m.journalFor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkClassified(t, reopen(t, path), dirtyCrashWindow, "applying")
	if m.journal != j {
		t.Fatalf("the journal was not cached")
	}
	// mutate only the cached in-memory state; a second call must not run the
	// classification again (it would overwrite the entry)
	j.mu.Lock()
	j.state.Dirty["a"] = JournalDirty{Class: schema.DirtyUnverified, Reason: "mutated"}
	j.mu.Unlock()
	if _, _, err := m.journalFor(ctx); err != nil {
		t.Fatal(err)
	}
	if d := m.journal.State().Dirty["a"]; d.Reason != "mutated" {
		t.Errorf("the second journalFor classified again: %+v", d)
	}
}

func TestJournalForPersistsTheSameClassForEveryOpenState(t *testing.T) {
	for _, state := range []string{"persisting", "persisted_unsynced"} {
		t.Run(state, func(t *testing.T) {
			path := plantJournal(t, state, false)
			if _, _, err := (&CommitMgr{}).journalFor(loadCtx(path)); err != nil {
				t.Fatal(err)
			}
			checkClassified(t, reopen(t, path), dirtyCrashWindow, state)
		})
	}
}

func TestJournalForFirstEmptyRtreeIsBootPartial(t *testing.T) {
	path := plantJournal(t, "applying", true)
	if _, _, err := (&CommitMgr{}).journalFor(loadCtx(path)); err != nil {
		t.Fatal(err)
	}
	checkClassified(t, reopen(t, path), schema.DirtyBootPartial, "applying")
}

func TestJournalForRenameFaultLeavesNothingCachedAndRetries(t *testing.T) {
	path := plantJournal(t, "applying", false)
	before, _ := os.ReadFile(path)
	fs := newFaultFS()
	fs.failOn["rename"] = errors.New("injected rename failure")
	m := &CommitMgr{openJournal: func(p string) (*Journal, error) { return openJournal(p, fs, discardLog) }}
	ctx := loadCtx(path)
	if _, _, err := m.journalFor(ctx); err == nil {
		t.Fatalf("the rename fault must be returned")
	}
	if m.journal != nil || m.recorder != nil {
		t.Errorf("journal/recorder cached after a failed classification")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Errorf("the old file changed")
	}
	// later open without the fault classifies
	delete(fs.failOn, "rename")
	if _, _, err := m.journalFor(ctx); err != nil {
		t.Fatal(err)
	}
	checkClassified(t, reopen(t, path), dirtyCrashWindow, "applying")
}

func TestJournalForNothingToClassifyWritesNothingAndCaches(t *testing.T) {
	cases := map[string]func(*JournalState){
		"commit nil": func(s *JournalState) { s.Commit = nil },
		"empty todo": func(s *JournalState) { s.Commit = &JournalCommit{ID: "c", State: "applying"} },
		"state done": func(s *JournalState) { s.Commit = &JournalCommit{ID: "c", State: "done", Todo: []string{"a"}} },
	}
	for name, set := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "commit-state.json")
			if err := openAt(t, path).Update(func(s *JournalState) bool {
				set(s)
				s.Dirty["other"] = JournalDirty{Class: schema.DirtyUnverified, Reason: "keep"}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(path)
			m := &CommitMgr{}
			if _, _, err := m.journalFor(loadCtx(path)); err != nil {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(path)
			if string(before) != string(after) || m.journal == nil {
				t.Errorf("file changed or journal not cached")
			}
		})
	}
}

func TestJournalForDoesNotCreateAMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commit-state.json")
	m := &CommitMgr{}
	if _, _, err := m.journalFor(loadCtx(path)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("a journal file was created (stat err %v)", err)
	}
}

// journalBegin on an open journal is the same-process path: it replaces the
// commit record and does not classify the previous todo.
func TestJournalBeginDoesNotClassify(t *testing.T) {
	path := plantJournal(t, "applying", false)
	j := openAt(t, path) // not via journalFor: no first-open classification
	if err := journalBegin(j, "c-new", []string{"x"}, false); err != nil {
		t.Fatal(err)
	}
	st := reopen(t, path)
	if st.Commit == nil || st.Commit.ID != "c-new" || !reflect.DeepEqual(st.Commit.Todo, []string{"x"}) {
		t.Errorf("commit record: %+v", st.Commit)
	}
	for _, c := range []string{"a", "b"} {
		if d, ok := st.Dirty[c]; ok {
			t.Errorf("%s was classified by journalBegin: %+v", c, d)
		}
	}
}
