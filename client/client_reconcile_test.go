// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

package client_test

import (
	"encoding/json"
	"net"
	"path/filepath"
	"testing"

	"github.com/danos/configd/client"
	"github.com/danos/configd/rpc"
)

// peer accepts one connection on a private socket, records the request and
// answers with the given response.
func peer(t *testing.T, resp rpc.Response) (addr string, got chan rpc.Request) {
	t.Helper()
	addr = filepath.Join(t.TempDir(), "configd.sock")
	l, err := net.Listen("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	got = make(chan rpc.Request, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var req rpc.Request
		if err := json.NewDecoder(c).Decode(&req); err != nil {
			return
		}
		got <- req
		resp.Id = req.Id
		json.NewEncoder(c).Encode(&resp)
	}()
	return addr, got
}

func reconcileCall(t *testing.T, resp rpc.Response, debug bool) (string, error, rpc.Request) {
	addr, got := peer(t, resp)
	c, err := client.Connect(client.Address(addr), client.SessionID("sid-1"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Reconcile(debug)
	return out, err, <-got
}

func TestClientReconcileEncodesMethodAndArgs(t *testing.T) {
	out, err, req := reconcileCall(t, rpc.Response{Result: "Reconciled: x\n"}, true)
	if err != nil || out != "Reconciled: x\n" {
		t.Fatalf("out %q err %v", out, err)
	}
	if req.Method != "Reconcile" {
		t.Errorf("method = %q, want Reconcile", req.Method)
	}
	if len(req.Args) != 2 || req.Args[0] != "sid-1" || req.Args[1] != true {
		t.Errorf("args = %#v, want [sid-1 true]", req.Args)
	}
	_, _, req = reconcileCall(t, rpc.Response{Result: ""}, false)
	if len(req.Args) != 2 || req.Args[1] != false {
		t.Errorf("args = %#v, want debug=false", req.Args)
	}
}

func TestClientReconcilePeerErrorAndWrongType(t *testing.T) {
	if _, err, _ := reconcileCall(t, rpc.Response{Error: "Commit already in progress"}, false); err == nil ||
		err.Error() != "Commit already in progress" {
		t.Errorf("err = %v", err)
	}
	if out, err, _ := reconcileCall(t, rpc.Response{Result: true}, false); err == nil || out != "" {
		t.Errorf("a non-string result must be an error: %q %v", out, err)
	}
}
