// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

// Stage 3-B4 tests: the verdict table of the fence gate (injected journal state
// and identity reader; no systemd, no files).

package session

import (
	"errors"
	"testing"

	"github.com/danos/config/schema"
)

func TestFenceGateVerdictTable(t *testing.T) {
	const (
		rec    = int64(10_000_000)
		window = int64(3_000_000) // sendWindow 2 s + margin 1 s
	)
	dirtyUnknown := map[string]JournalDirty{"c": {Class: schema.DirtyUnknown}}
	disp := func(sent, settled bool) map[string]JournalDispatch {
		return map[string]JournalDispatch{"c": {MayHaveBeenSent: sent, Settled: settled, TRecordMonoUs: rec}}
	}
	id := func(enter int64) func() (ServiceIdentity, error) {
		return func() (ServiceIdentity, error) {
			return ServiceIdentity{Known: true, MainPID: 7, ActiveEnterMonoUs: enter}, nil
		}
	}
	cases := []struct {
		name  string
		dirty map[string]JournalDirty
		disp  map[string]JournalDispatch
		id    func() (ServiceIdentity, error)
		want  bool
	}{
		{"not fence-requiring (no dirty entry)", nil, disp(true, false), id(rec), true},
		{"not fence-requiring (compensation_failed)",
			map[string]JournalDirty{"c": {Class: schema.DirtyCompensationFailed}}, disp(true, false), id(rec), true},
		{"dispatch absent", dirtyUnknown, nil, id(rec), true},
		{"may_have_been_sent false", dirtyUnknown, disp(false, false), id(rec), true},
		{"settled", dirtyUnknown, disp(true, true), id(rec), true},
		{"identity later than the window", dirtyUnknown, disp(true, false), id(rec + window + 1), true},
		{"identity exactly at the window edge", dirtyUnknown, disp(true, false), id(rec + window), false},
		{"identity inside the window", dirtyUnknown, disp(true, false), id(rec + window - 1), false},
		{"equal identity (same start as the record)", dirtyUnknown, disp(true, false), id(rec), false},
		{"unreadable identity", dirtyUnknown, disp(true, false),
			func() (ServiceIdentity, error) { return ServiceIdentity{}, errors.New("no bus") }, false},
		{"identity not known, no error", dirtyUnknown, disp(true, false),
			func() (ServiceIdentity, error) { return ServiceIdentity{}, nil }, false},
		{"crash_window requires a fence",
			map[string]JournalDirty{"c": {Class: dirtyCrashWindow}}, disp(true, false), id(rec), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := newJournalState()
			for k, v := range c.dirty {
				st.Dirty[k] = v
			}
			for k, v := range c.disp {
				st.Dispatch[k] = v
			}
			reads := 0
			ids := identityFn(func(string) (ServiceIdentity, error) { reads++; return c.id() })
			v := fenceGate(st, ids, []string{"c"})
			if len(v) != 1 || v[0].Fenced != c.want {
				t.Fatalf("verdict = %+v, want fenced=%v", v, c.want)
			}
		})
	}
	// the identity is read only when nothing else decides
	st := newJournalState()
	st.Dirty["c"] = JournalDirty{Class: schema.DirtyUnknown}
	st.Dispatch["c"] = JournalDispatch{MayHaveBeenSent: false}
	fenceGate(st, identityFn(func(string) (ServiceIdentity, error) {
		t.Errorf("identity read although the call was not sent")
		return ServiceIdentity{}, nil
	}), []string{"c"})
}

type identityFn func(string) (ServiceIdentity, error)

func (f identityFn) Identity(c string) (ServiceIdentity, error) { return f(c) }
