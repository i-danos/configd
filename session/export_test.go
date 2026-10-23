// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package session

import (
	"errors"
	"os"
	"time"

	"github.com/danos/config/schema"
)

// SetJournalOpener makes the commit manager open its journal through open
// instead of OpenJournal (stage 3-B2 tests).
func SetJournalOpener(m *CommitMgr, open func(path string) (*Journal, error)) {
	m.openJournal = open
}

// SetDispatchRecorder presets the recorder the commit manager passes to the
// component manager.
func SetDispatchRecorder(m *CommitMgr, r schema.DispatchRecorder) { m.recorder = r }

// FaultOpener opens a real journal on the given path behind the fault file
// system, with the listed ops failing ("create", "rename", "write", ...).
// *failing is consulted at every call so a test can arm the fault late.
func FaultOpener(failing *bool, ops ...string) func(string) (*Journal, error) {
	return func(path string) (*Journal, error) {
		fs := newFaultFS()
		j, err := openJournal(path, &armedFS{faultFS: fs, armed: failing, ops: ops},
			func(string, ...interface{}) {})
		return j, err
	}
}

// armedFS applies the faults only while *armed is true.
type armedFS struct {
	*faultFS
	armed *bool
	ops   []string
}

func (a *armedFS) apply() {
	a.faultFS.mu.Lock()
	defer a.faultFS.mu.Unlock()
	a.faultFS.failOn = map[string]error{}
	if *a.armed {
		for _, o := range a.ops {
			a.faultFS.failOn[o] = errors.New("injected " + o + " failure")
		}
	}
}

func (a *armedFS) CreateExclusive(p string, m os.FileMode) (journalFile, error) {
	a.apply()
	return a.faultFS.CreateExclusive(p, m)
}
func (a *armedFS) Rename(o, n string) error { a.apply(); return a.faultFS.Rename(o, n) }

// SetRunfileFaults makes the journaled path persist the runfile through a
// fault file system that fails the listed ops ("create", "write", "fsync",
// "rename", "syncdir"); the returned function lists the ops that were called.
func SetRunfileFaults(m *CommitMgr, ops ...string) func() []string {
	fs := newFaultFS()
	for _, o := range ops {
		fs.failOn[o] = errors.New("injected " + o + " failure")
	}
	m.runfileFS = fs
	return fs.opList
}

// FaultOpenerFromRename opens a real journal whose atomic writes fail at the
// rename from the n-th journal write on (1-based): n=3 fails the third write.
func FaultOpenerFromRename(n int) func(string) (*Journal, error) {
	return func(path string) (*Journal, error) {
		return openJournal(path, &nthRenameFS{osJournalFS: osJournalFS{}, from: n},
			func(string, ...interface{}) {})
	}
}

type nthRenameFS struct {
	osJournalFS
	from, seen int
}

func (f *nthRenameFS) Rename(o, n string) error {
	f.seen++
	if f.seen >= f.from {
		return errors.New("injected rename failure")
	}
	return f.osJournalFS.Rename(o, n)
}

// SetIdentityReader replaces the systemd identity reader of the commit manager
// (used by the recorder and by the fence gate).
func SetIdentityReader(m *CommitMgr, r IdentityReader) { m.identities = r }

// SeedJournal writes the journal of the given runfile directly (test setup).
func SeedJournal(runfile string, mutate func(*JournalState)) error {
	j, err := OpenJournal(JournalPathFor(runfile))
	if err != nil {
		return err
	}
	return j.Update(func(s *JournalState) bool { mutate(s); return true })
}

// IdentityFunc adapts a function to the IdentityReader interface.
type IdentityFunc func(component string) (ServiceIdentity, error)

func (f IdentityFunc) Identity(c string) (ServiceIdentity, error) { return f(c) }

// ArmStartupGate arms the opt-in startup gate with an injected clock.
func ArmStartupGate(m *CommitMgr, now func() time.Time) { m.startup.arm(now) }

// SetCrashSettle arms the opt-in crashSettle prelude with an injected sleeper.
func SetCrashSettle(m *CommitMgr, sleep func(time.Duration)) { m.crashSettle = sleep }
