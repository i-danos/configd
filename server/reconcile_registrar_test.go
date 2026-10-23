// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

package server_test

import (
	"io/ioutil"
	"log"
	"net"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danos/configd"
	"github.com/danos/configd/server"
	"github.com/danos/configd/session/sessiontest"
)

// The registrar built by NewSrv (the constructor the binary uses) must contain
// "Reconcile" and no second reconcile-like name.
func TestReconcileIsRegisteredOnce(t *testing.T) {
	srvT, _ := sessiontest.NewTestSpec(t).SetSingleSchema(defaultSchema).SetConfig(emptyConfig).Init()
	dir := t.TempDir()
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, "configd.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &configd.Config{Runfile: filepath.Join(dir, "running"), Yangdir: "../yang"}
	s := server.NewSrv(l, srvT.Ms, srvT.MsFull, u.Username, cfg,
		log.New(ioutil.Discard, "", 0), srvT.Ctx.CompMgr)
	names := s.MethodNames()
	n := 0
	for _, m := range names {
		if m == "Reconcile" {
			n++
		} else if strings.Contains(strings.ToLower(m), "reconcile") {
			t.Errorf("a second reconcile-like method is registered: %s", m)
		}
	}
	if n != 1 {
		t.Errorf("Reconcile registered %d times", n)
	}
}
