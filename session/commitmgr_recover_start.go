// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 3, stage 3-B19 (record consumed since 3-F): write the restart
// classification of an open commit record (recoverOpenCommit, design 6.3
// transition (b)) back through the journal, once, on an already-open Journal. Not a process-start hook and not
// openJournal; nothing calls it yet, and the same-process next-commit path
// must never call it (transition (a)).

package session

import "errors"

// recoverJournalAtStart applies recoverOpenCommit to the journal and, in the
// same atomic write, consumes the stale commit record (A02 stage 3-F, F8): the
// classification is now durable in the dirty set, and a record that outlived it
// would re-dirty already reconciled components (boot_partial ones for good) at
// every later restart. The record is not touched when there was nothing to
// classify. Journal.Update writes nothing when there is nothing to do, and
// follows its own rules for a failed write (old file and in-memory state kept
// before the rename).
func recoverJournalAtStart(j *Journal) error {
	if j == nil {
		return errors.New("recover journal at start: no journal")
	}
	return j.Update(func(st *JournalState) bool {
		if !recoverOpenCommit(st) {
			return false
		}
		st.Commit = nil
		return true
	})
}
