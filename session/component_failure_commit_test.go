// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 2: a COMMIT must fail closed when a component fails to apply its
// configuration. A scripted OperationsManager injects the failures under the
// real compMgr, CommitMgr and Session, with no bus, service or VM. Contract:
// ok=false, the error names the failing component, later components are not
// attempted, configd's running config is not advanced or written, the candidate
// is kept so the commit can be retried, and nothing is rolled back (the error
// says which components were already applied).

package session_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danos/config/schema"
	"github.com/danos/configd/session/sessiontest"
	"github.com/danos/godbus/dbus"
	"github.com/danos/utils/exec"
	"github.com/danos/vci/conf"
	"github.com/danos/yang/testutils"
)

const (
	compFirst  = "net.vyatta.test.first"
	compSecond = "net.vyatta.test.second"
	compThird  = "net.vyatta.test.third"
)

// failingOps delegates to the in-memory test component manager but scripts the
// result of SetConfigForModel per component. A failing component commits
// nothing, like a component that rejected or never received the config.
type failingOps struct {
	schema.OperationsManager
	results map[string]error
	calls   []string
	applied map[string]string // config each component actually accepted
	// payloads: the config string of every call, in order, per component.
	payloads map[string][]string
	// failOnce: the scripted error applies to the first call of a component
	// only; later calls to it succeed.
	failOnce bool
	seen     map[string]int
}

func (f *failingOps) SetConfigForModel(model string, obj interface{}) error {
	f.calls = append(f.calls, model)
	if f.payloads == nil {
		f.payloads = map[string][]string{}
		f.seen = map[string]int{}
	}
	if s, ok := obj.(string); ok {
		f.payloads[model] = append(f.payloads[model], s)
	}
	f.seen[model]++
	if err := f.results[model]; err != nil && (!f.failOnce || f.seen[model] == 1) {
		return err
	}
	if f.applied == nil {
		f.applied = map[string]string{}
	}
	if s, ok := obj.(string); ok {
		f.applied[model] = s
	}
	return f.OperationsManager.SetConfigForModel(model, obj)
}

type commitOutcome struct {
	outs        []*exec.Output
	errs        []error
	ok          bool
	runningHas  func(string) bool // is a value present in configd's running tree
	compHas     func(model, sub string) bool
	calls       []string
	runfileSame bool // the running file on disk was not touched
	retry       func() *commitOutcome
	// What this commit logged through the session debug log. The commit
	// scripts stage logs "execute sets"; the pre/post hooks stage logs
	// "Executing <dir> hooks". They are the observable evidence that
	// ctx.commit and the hooks did, or did not, run.
	log string
}

const (
	logCommitScripts = "execute sets"
	logPreHooks      = "Executing /etc/commit/pre-hooks.d hooks"
	logPostHooks     = "Executing /etc/commit/post-hooks.d hooks"
)

func count(log, what string) int { return strings.Count(log, what) }

// commitWithFailures loads `config` into a fresh session, makes the listed
// components fail, commits, and reports what the caller sees plus whether the
// running configuration advanced.
func commitWithFailures(t *testing.T, fail map[string]error) *commitOutcome {
	ts := sessiontest.NewTestSpec(t).
		SetSchemaDefsByRef(schemas).
		SetComponents(conf.BaseModelSet, []string{
			firstTestComp.String(), secondTestComp.String(), thirdTestComp.String()}).
		SetConfig(emptyConfig)
	return commitScenario(t, ts, config, fail)
}

func commitScenario(t *testing.T, ts *sessiontest.TestSpec, cfg string,
	fail map[string]error) *commitOutcome {
	srv, sess := ts.Init()
	// Keep the running file (and the commit journal next to it) out of the
	// source tree.
	srv.Ctx.Config.Runfile = filepath.Join(t.TempDir(), "session_test.runfile")

	tcm := ts.GetCompMgr()
	ops := &failingOps{OperationsManager: tcm, results: fail}
	srv.Ctx.CompMgr = schema.NewCompMgr(ops, tcm, srv.MsFull, tcm.GetComponentNSMappings())

	var logbuf bytes.Buffer
	srv.Ctx.Dlog = log.New(&logbuf, "", 0)

	srv.LoadConfig(t, cfg, sess)
	runBefore := runfileState(srv.Ctx.Config.Runfile)
	logbuf.Reset()
	outs, errs, ok := sess.Commit(srv.Ctx, "message", false)
	firstLog := logbuf.String()

	running := serialiseCfg(srv.Cmgr.Running(), srv.Ms)
	return &commitOutcome{
		outs: outs, errs: errs, ok: ok, calls: ops.calls, log: firstLog,
		runfileSame: runfileState(srv.Ctx.Config.Runfile) == runBefore,
		// Clear the faults and commit the SAME session again: only works if
		// the failed commit kept the candidate.
		retry: func() *commitOutcome {
			for k := range ops.results {
				delete(ops.results, k)
			}
			ops.calls = nil
			logbuf.Reset()
			outs, errs, ok := sess.Commit(srv.Ctx, "message", false)
			running := serialiseCfg(srv.Cmgr.Running(), srv.Ms)
			return &commitOutcome{outs: outs, errs: errs, ok: ok, calls: ops.calls,
				log:        logbuf.String(),
				runningHas: func(s string) bool { return strings.Contains(running, s) },
				compHas: func(model, sub string) bool {
					return strings.Contains(ops.applied[model], sub)
				}}
		},
		runningHas: func(s string) bool { return strings.Contains(running, s) },
		compHas: func(model, sub string) bool {
			return strings.Contains(ops.applied[model], sub)
		},
	}
}

// runfileState captures the on-disk running file (content and mtime) so a test
// can tell whether a commit rewrote it.
func runfileState(path string) string {
	fi, err := os.Stat(path)
	if err != nil {
		return "absent"
	}
	b, _ := os.ReadFile(path)
	return fi.ModTime().String() + "|" + string(b)
}

func allOutputText(outs []*exec.Output) string {
	var b strings.Builder
	for _, o := range outs {
		if o != nil {
			b.WriteString(o.Output)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func TestCommitAllComponentsSucceed(t *testing.T) {
	r := commitWithFailures(t, nil)
	if !r.ok || len(r.errs) != 0 {
		t.Fatalf("control: ok=%v errs=%v", r.ok, r.errs)
	}
	for _, v := range []string{"someValue", "anotherValue"} {
		if !r.runningHas(v) {
			t.Errorf("control: running lacks %q", v)
		}
	}
	if !r.compHas(compSecond, "secondLeaf") {
		t.Errorf("control: second component did not receive its config")
	}
	if r.runfileSame {
		t.Errorf("control: a successful commit must write the running file")
	}
}

// Every failure class must fail the commit and leave running untouched.
func TestCommitComponentFailureClassesAllFailClosed(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		unknown bool
	}{
		{"ordinary", errors.New("second: cannot apply"), false},
		{"dbus value", dbus.Error{Name: "org.freedesktop.DBus.Error.ServiceUnknown",
			Body: []interface{}{"not activatable"}}, false},
		{"dbus pointer", dbus.NewError("org.freedesktop.DBus.Error.ServiceUnknown",
			[]interface{}{"gone"}), false},
		{"dbus wrapped", fmt.Errorf("call: %w", dbus.Error{
			Name: "org.freedesktop.DBus.Error.Failed", Body: []interface{}{"x"}}), false},
		{"context deadline", context.DeadlineExceeded, true},
		{"os deadline", os.ErrDeadlineExceeded, true},
		{"dbus NoReply", dbus.Error{Name: "org.freedesktop.DBus.Error.NoReply",
			Body: []interface{}{"no reply"}}, true},
		{"dbus NoReply pointer", dbus.NewError("org.freedesktop.DBus.Error.NoReply",
			[]interface{}{"no reply"}), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := commitWithFailures(t, map[string]error{compSecond: c.err})
			if r.ok {
				t.Fatalf("commit must fail when a component fails; errs=%v", r.errs)
			}
			if len(r.errs) == 0 {
				t.Fatalf("a failed commit must carry an error")
			}
			msg := fmt.Sprint(r.errs)
			if !strings.Contains(msg, compSecond) {
				t.Errorf("error must name the failing component: %s", msg)
			}
			if c.unknown != strings.Contains(msg, "OUTCOME UNKNOWN") {
				t.Errorf("unknown-outcome flag wrong (want %v): %s", c.unknown, msg)
			}
			// running is unchanged: nothing from the candidate is in it
			for _, v := range []string{"someValue", "anotherValue"} {
				if r.runningHas(v) {
					t.Errorf("running advanced (has %q) although a component failed", v)
				}
			}
			if !r.runfileSame {
				t.Errorf("the running file was written although a component failed")
			}
		})
	}
}

// Partial application: the components before the failure keep the config
// (no rollback yet), the failing one and the rest do not, and the error says so.
func TestCommitComponentFailureStopsLaterComponentsAndReportsPartialState(t *testing.T) {
	// apply order is second, third, first (see TestConfigSetOrder)
	r := commitWithFailures(t, map[string]error{compThird: errors.New("third down")})
	if r.ok {
		t.Fatalf("commit must fail")
	}
	if len(r.calls) != 2 || r.calls[0] != compSecond || r.calls[1] != compThird {
		t.Fatalf("components after the failure must not be attempted; calls=%v", r.calls)
	}
	if !r.compHas(compSecond, "secondLeaf") {
		t.Errorf("harness: the first component should have applied")
	}
	if r.compHas(compFirst, "firstLeaf") || r.compHas(compThird, "thirdLeaf") {
		t.Errorf("failing and later components must not hold the config")
	}
	msg := fmt.Sprint(r.errs)
	for _, need := range []string{
		compThird,
		// the real compMgr now takes the compensating path; its report
		// (here: compensation skipped by policy on the first commit) names
		// the components whose state may differ from running
		"The runtime state of: " + compSecond + ", " + compThird + " may differ",
		"not attempted: " + compFirst} {
		if !strings.Contains(msg, need) {
			t.Errorf("error lacks %q: %s", need, msg)
		}
	}
	if r.runningHas("secondLeaf") {
		t.Errorf("running must not advance even though one component applied")
	}
}

func TestCommitFailureKeepsCandidateSoRetryWorks(t *testing.T) {
	r := commitWithFailures(t, map[string]error{compSecond: errors.New("second down")})
	if r.ok {
		t.Fatalf("first commit must fail")
	}
	r2 := r.retry()
	if !r2.ok || len(r2.errs) != 0 {
		t.Fatalf("retry after the fault cleared must succeed: ok=%v errs=%v", r2.ok, r2.errs)
	}
	for _, v := range []string{"someValue", "anotherValue"} {
		if !r2.runningHas(v) {
			t.Errorf("after the retry running lacks %q", v)
		}
	}
	if !r2.compHas(compSecond, "secondLeaf") {
		t.Errorf("after the retry the component must hold its config")
	}
}

// ---- direct evidence of what the commit pipeline did ------------------------------

// Success: the normal pipeline runs, once each.
func TestCommitSuccessRunsCommitScriptsAndHooksExactlyOnce(t *testing.T) {
	r := commitWithFailures(t, nil)
	if !r.ok {
		t.Fatalf("control: %v", r.errs)
	}
	for what, want := range map[string]int{
		logCommitScripts: 1, logPreHooks: 1, logPostHooks: 1} {
		if got := count(r.log, what); got != want {
			t.Errorf("success: %q logged %d times, want %d\n%s", what, got, want, r.log)
		}
	}
}

// Component failure: pre-hooks already ran (they precede the apply), but the
// commit scripts and the post-hooks must not.
func TestCommitComponentFailureInvokesNeitherCommitScriptsNorPostHooks(t *testing.T) {
	r := commitWithFailures(t, map[string]error{compSecond: errors.New("second down")})
	if r.ok {
		t.Fatalf("commit must fail")
	}
	if got := count(r.log, logCommitScripts); got != 0 {
		t.Errorf("ctx.commit ran %d times despite the component failure:\n%s", got, r.log)
	}
	if got := count(r.log, logPostHooks); got != 0 {
		t.Errorf("post-hooks ran %d times despite the component failure:\n%s", got, r.log)
	}
	if got := count(r.log, logPreHooks); got != 1 {
		t.Errorf("pre-hooks ran %d times, want 1 (they precede the apply)", got)
	}
}

// The retry after a failed commit runs the whole pipeline normally, once.
func TestCommitRetryAfterFailureRunsPipelineOnce(t *testing.T) {
	r := commitWithFailures(t, map[string]error{compSecond: errors.New("second down")})
	r2 := r.retry()
	if !r2.ok {
		t.Fatalf("retry must succeed: %v", r2.errs)
	}
	for what, want := range map[string]int{
		logCommitScripts: 1, logPreHooks: 1, logPostHooks: 1} {
		if got := count(r2.log, what); got != want {
			t.Errorf("retry: %q logged %d times, want %d", what, got, want)
		}
	}
	for _, v := range []string{"someValue", "anotherValue"} {
		if !r2.runningHas(v) {
			t.Errorf("retry: running lacks %q", v)
		}
	}
}

// ---- first-commit failure must leave the commit manager "never committed" -----

// Two components: "dflt" is configured explicitly; "dflt2" has nothing but a
// default value, so it only counts as changed on the very first commit (defaults
// appear as differences against a defaults-free running tree).
const (
	defaultsSchema = `
container dflt {
	leaf kept {
		type string;
	}
}`
	defaults2Schema = `
container dflt2 {
	leaf withDefault {
		type string;
		default "dv2";
	}
}`
	compDflt  = "net.vyatta.test.dflt"
	compDflt2 = "net.vyatta.test.dflt2"
)

func defaultsSpec(t *testing.T) *sessiontest.TestSpec {
	return sessiontest.NewTestSpec(t).
		SetSchemaDefsByRef([]*sessiontest.TestSchema{
			sessiontest.NewTestSchema("vyatta-test-dflt-v1", "dflt").
				AddSchemaSnippet(defaultsSchema),
			sessiontest.NewTestSchema("vyatta-test-dflt2-v1", "dflt2").
				AddSchemaSnippet(defaults2Schema)}).
		SetComponents(conf.BaseModelSet, []string{
			conf.CreateTestDotComponentFile("dflt").AddBaseModel().String(),
			conf.CreateTestDotComponentFile("dflt2").AddBaseModel().String()}).
		SetConfig(emptyConfig)
}

var defaultsConfig = testutils.Root(
	testutils.Cont("dflt", testutils.Leaf("kept", "x")))

// The very first commit pushes default values to the components. If that first
// commit fails, the retry must still be treated as the first commit, otherwise
// a component whose only change is a default is never configured.
func TestCommitFirstCommitDeliversDefaults(t *testing.T) {
	r := commitScenario(t, defaultsSpec(t), defaultsConfig, nil)
	if !r.ok {
		t.Fatalf("control: %v", r.errs)
	}
	if !r.compHas(compDflt2, "dv2") {
		t.Fatalf("control: the first commit must deliver the default to the component")
	}
}

func TestCommitFirstCommitFailureStillDeliversDefaultsOnRetry(t *testing.T) {
	r := commitScenario(t, defaultsSpec(t), defaultsConfig,
		map[string]error{compDflt: errors.New("dflt down")})
	if r.ok {
		t.Fatalf("first commit must fail")
	}
	if r.compHas(compDflt2, "dv2") || r.compHas(compDflt, "kept") {
		t.Fatalf("harness: nothing may be applied before the failing component")
	}
	r2 := r.retry()
	if !r2.ok {
		t.Fatalf("retry must succeed: %v", r2.errs)
	}
	if !r2.compHas(compDflt2, "dv2") {
		t.Errorf("retry after a failed FIRST commit must still deliver the default "+
			"(hadcommit must have been reset); calls=%v", r2.calls)
	}
}

// A first commit that fails validation never reached the components, so the
// retry must still be the first commit and deliver the defaults.
func TestCommitFirstCommitValidationFailureStillDeliversDefaultsOnRetry(t *testing.T) {
	ts := sessiontest.NewTestSpec(t).
		SetSchemaDefsByRef([]*sessiontest.TestSchema{
			sessiontest.NewTestSchema("vyatta-test-dflt-v1", "dflt").
				AddSchemaSnippet(`
container dflt {
	leaf kept {
		type string;
		must ". != 'bad'" {
			error-message "bad value";
		}
	}
}`),
			sessiontest.NewTestSchema("vyatta-test-dflt2-v1", "dflt2").
				AddSchemaSnippet(defaults2Schema)}).
		SetComponents(conf.BaseModelSet, []string{
			conf.CreateTestDotComponentFile("dflt").AddBaseModel().String(),
			conf.CreateTestDotComponentFile("dflt2").AddBaseModel().String()}).
		SetConfig(emptyConfig)
	srv, sess := ts.Init()
	tcm := ts.GetCompMgr()
	ops := &failingOps{OperationsManager: tcm, results: map[string]error{}}
	srv.Ctx.CompMgr = schema.NewCompMgr(ops, tcm, srv.MsFull, tcm.GetComponentNSMappings())
	srv.LoadConfig(t, testutils.Root(
		testutils.Cont("dflt", testutils.Leaf("kept", "bad"))), sess)

	if _, _, ok := sess.Commit(srv.Ctx, "message", false); ok {
		t.Fatalf("first commit must fail validation")
	}
	if len(ops.calls) != 0 {
		t.Fatalf("validation failure must not reach components; calls=%v", ops.calls)
	}
	if err := sess.Delete(srv.Ctx, []string{"dflt", "kept"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := sess.Set(srv.Ctx, []string{"dflt", "kept", "x"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, errs, ok := sess.Commit(srv.Ctx, "message", false); !ok {
		t.Fatalf("retry must succeed: %v", errs)
	}
	if !strings.Contains(ops.applied[compDflt2], "dv2") {
		t.Errorf("retry after a validation-failed FIRST commit must deliver the "+
			"default; calls=%v", ops.calls)
	}
}
