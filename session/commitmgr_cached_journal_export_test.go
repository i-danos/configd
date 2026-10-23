// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package session

// CachedJournal returns the journal the commit manager has cached, or nil.
// Test-only.
func CachedJournal(m *CommitMgr) *Journal {
	if m == nil {
		return nil
	}
	return m.journal
}

// MutateCachedDirty assigns a dirty entry in the cached journal's in-memory
// state only: no Update, no file write. False when there is no cached journal.
// Test-only.
func MutateCachedDirty(m *CommitMgr, component string, d JournalDirty) bool {
	if m == nil || m.journal == nil {
		return false
	}
	m.journal.mu.Lock()
	m.journal.state.Dirty[component] = d
	m.journal.mu.Unlock()
	return true
}
