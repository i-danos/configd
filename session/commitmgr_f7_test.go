// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-F (F7): when the journal cannot be written, the components whose
// compensation was skipped must stay dirty in memory (class unfenced) so that
// the hint, the next commit and `reconcile` still see them (design 11.1).

package session_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/danos/config/schema"
	"github.com/danos/configd/session"
	"github.com/danos/configd/session/sessiontest"
)

// compRefusingRecorder behaves like a journal that became unwritable after the
// forward phase: forward and reconcile records succeed, compensation records
// fail. It writes no file.
type compRefusingRecorder struct{ mu sync.Mutex }

func (r *compRefusingRecorder) BeforeDispatch(c string, k schema.DispatchKind, seq uint64) (int64, error) {
	if k == schema.KindCompensation {
		return 0, errors.New("no space left on device")
	}
	return schema.MonotonicNowUs(), nil
}

func (r *compRefusingRecorder) AfterDispatch(string, schema.DispatchKind, uint64, error) {}

func TestJournalUnwritableAfterApplyKeepsSkippedComponentsDirtyInMemory(t *testing.T) {
	r := journalCommit(t, nil, func(srv *sessiontest.TstSrv, _ func(schema.ComponentManager)) {
		// Journal writes: 1 = commit record (ok), 2 = "persisting" (ok),
		// 3 = the end-of-commit record after the compensation report (fails).
		session.SetJournalOpener(srv.Cmgr, session.FaultOpenerFromRename(3))
		session.SetDispatchRecorder(srv.Cmgr, &compRefusingRecorder{})
		session.SetRunfileFaults(srv.Cmgr, "rename")
	})
	if r.ok {
		t.Fatalf("the commit must fail (runfile persist fault)")
	}
	all := allOutputText(r.outs)
	if !strings.Contains(all, "the commit journal could not be updated") {
		t.Fatalf("expected the journal warning, got %q", all)
	}
	if !strings.Contains(all, "run 'reconcile'") {
		t.Errorf("the failed commit must hint at reconcile while components are dirty: %q", all)
	}

	outs, err := r.srv.Cmgr.Reconcile(r.srv.Ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	text := allOutputText(outs)
	if strings.Contains(text, "No dirty components to reconcile") {
		t.Fatalf("reconcile must not claim nothing is dirty while compensation was skipped: %q", text)
	}
	if !strings.Contains(text, "Reconciled:") && !strings.Contains(text, "Not reconciled:") {
		t.Errorf("reconcile reported neither success nor a reason: %q", text)
	}
}
