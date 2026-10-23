// Copyright (c) 2018-2019, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only
//
// Functions required for server_test but not part of public API.

package server

import (
	"io/ioutil"
	"os"
	"sync"

	"github.com/danos/config/schema"
	"github.com/danos/configd"
	"github.com/danos/configd/session"
)

func (d *Disp) CallRpcWithCaller(
	moduleIdOrNamespace, rpcName, args, encoding string,
	vrc VciRpcCaller,
) (string, error) {
	return d.callRpcInternal(moduleIdOrNamespace, rpcName, args, encoding, vrc)
}

func (d *Disp) SchemaGetUnescaped(modOrSubmod string) (string, error) {
	schema, err := d.getModuleOrSubmoduleSchema(modOrSubmod)
	if err != nil {
		return "", err
	}
	return schema, nil
}

var (
	stubOnce sync.Once
	stubPath string
)

// stubConfigMgmtScript returns a stand-in for vyatta-config-mgmt.pl (shipped
// by vyatta-cfg-system, absent from unit-test environments) that accepts the
// commit-confirm action, so dispatcher tests do not depend on the host.
func stubConfigMgmtScript() string {
	stubOnce.Do(func() {
		f, err := ioutil.TempFile("", "stub-config-mgmt-*.sh")
		if err != nil {
			return
		}
		f.WriteString("#!/bin/sh\nexit 0\n")
		f.Close()
		os.Chmod(f.Name(), 0700)
		stubPath = f.Name()
	})
	return stubPath
}

func NewDispatcher(
	smgr *session.SessionMgr,
	cmgr *session.CommitMgr,
	ms schema.ModelSet,
	msFull schema.ModelSet,
	ctx *configd.Context,
) *Disp {
	return &Disp{
		smgr:   smgr,
		cmgr:   cmgr,
		ms:     ms,
		msFull: msFull,
		ctx:    ctx,

		configMgmtScript: stubConfigMgmtScript(),
	}
}

// MethodNames lists the methods the registrar built by NewSrv exposes.
func (s *Srv) MethodNames() []string {
	var names []string
	for n := range s.m {
		names = append(names, n)
	}
	return names
}
