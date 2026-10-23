// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

// Service identity for the dispatch journal: what identifies "this run of the
// component's service". A restart changes ActiveEnterTimestampMonotonic (and
// normally MainPID), which is what the fence gate (a later stage) compares.

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	systemd "github.com/coreos/go-systemd/dbus"
)

// ServiceIdentity identifies one run of a component's service. The zero value
// (Known == false) is the "unknown-identity" of the journal: the identity could
// not be read, so no later comparison can prove a restart.
type ServiceIdentity struct {
	Known             bool
	MainPID           int64
	ActiveEnterMonoUs int64
	ReadAtMonoUs      int64
}

const unknownIdentity = "unknown-identity"

type identityJSON struct {
	MainPID           int64 `json:"main_pid"`
	ActiveEnterMonoUs int64 `json:"active_enter_mono_us"`
	ReadAtMonoUs      int64 `json:"read_at_mono_us"`
}

// MarshalJSON writes the string "unknown-identity" when the identity is not
// known, an object otherwise (design section 6.1).
func (i ServiceIdentity) MarshalJSON() ([]byte, error) {
	if !i.Known {
		return json.Marshal(unknownIdentity)
	}
	return json.Marshal(identityJSON{i.MainPID, i.ActiveEnterMonoUs, i.ReadAtMonoUs})
}

func (i *ServiceIdentity) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		if s != unknownIdentity {
			return fmt.Errorf("identity string %q is not %q", s, unknownIdentity)
		}
		*i = ServiceIdentity{}
		return nil
	}
	var o identityJSON
	if err := json.Unmarshal(b, &o); err != nil {
		return err
	}
	*i = ServiceIdentity{Known: true, MainPID: o.MainPID,
		ActiveEnterMonoUs: o.ActiveEnterMonoUs, ReadAtMonoUs: o.ReadAtMonoUs}
	return nil
}

// IdentityReader reads the current identity of a component's service. An error
// means "cannot be read"; the recorder then stores unknown-identity.
type IdentityReader interface {
	Identity(component string) (ServiceIdentity, error)
}

// unitProperties is the part of the systemd connection the reader uses, so it
// can be tested without systemd.
type unitProperties interface {
	MainPID(ctx context.Context, unit string) (uint64, error)
	ActiveEnterMonotonicUs(ctx context.Context, unit string) (uint64, error)
}

// SystemdIdentityReader reads MainPID and ActiveEnterTimestampMonotonic of the
// component's service unit over the systemd private socket. Every read is
// bounded by a timeout so a stuck systemd cannot stall a dispatch.
type SystemdIdentityReader struct {
	props   unitProperties
	unitFor func(component string) string
	timeout time.Duration
}

// NewSystemdIdentityReader returns a reader for real systemd. unitFor maps a
// component (model) name to its service name, with or without ".service".
func NewSystemdIdentityReader(unitFor func(component string) string) *SystemdIdentityReader {
	return &SystemdIdentityReader{
		props:   &systemdProps{},
		unitFor: unitFor,
		timeout: 2 * time.Second,
	}
}

func (r *SystemdIdentityReader) Identity(component string) (ServiceIdentity, error) {
	unit := r.unitFor(component)
	if unit == "" {
		return ServiceIdentity{}, fmt.Errorf("no service unit known for %q", component)
	}
	if !strings.HasSuffix(unit, ".service") {
		unit += ".service"
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	pid, err := r.props.MainPID(ctx, unit)
	if err != nil {
		return ServiceIdentity{}, fmt.Errorf("MainPID of %s: %w", unit, err)
	}
	enter, err := r.props.ActiveEnterMonotonicUs(ctx, unit)
	if err != nil {
		return ServiceIdentity{}, fmt.Errorf("ActiveEnterTimestampMonotonic of %s: %w", unit, err)
	}
	return ServiceIdentity{Known: true, MainPID: int64(pid), ActiveEnterMonoUs: int64(enter)}, nil
}

// dialGiveUp bounds the background dial to systemd. The connection's lifetime
// is tied to the context it is created with (godbus closes the connection when
// that context ends), so the dial must NOT use the short per-read context; it
// runs in the background with its own context and the caller only waits for it
// as long as its read context allows.
const dialGiveUp = 10 * time.Second

type dialState struct {
	done chan struct{}
	conn *systemd.Conn
	err  error
}

// systemdProps is the real implementation: one lazily opened connection to
// systemd, dropped after an error so the next read reconnects.
type systemdProps struct {
	mu     sync.Mutex
	conn   *systemd.Conn
	cancel context.CancelFunc
	dial   *dialState
	// dialFn opens the connection (systemd.NewSystemdConnectionContext in
	// production; injectable for tests).
	dialFn func(ctx context.Context) (*systemd.Conn, error)
}

// connection returns the cached connection, or waits for a (single, shared)
// background dial, but never longer than ctx allows.
func (p *systemdProps) connection(ctx context.Context) (*systemd.Conn, error) {
	p.mu.Lock()
	if p.conn != nil {
		c := p.conn
		p.mu.Unlock()
		return c, nil
	}
	d := p.dial
	if d == nil {
		d = &dialState{done: make(chan struct{})}
		p.dial = d
		go p.runDial(d)
	}
	p.mu.Unlock()
	select {
	case <-d.done:
		if d.err != nil {
			return nil, d.err
		}
		return d.conn, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("connecting to systemd: %w", ctx.Err())
	}
}

func (p *systemdProps) runDial(d *dialState) {
	dial := p.dialFn
	if dial == nil {
		dial = systemd.NewSystemdConnectionContext
	}
	dctx, cancel := context.WithCancel(context.Background())
	var finished int32
	giveUp := time.AfterFunc(dialGiveUp, func() {
		if atomic.LoadInt32(&finished) == 0 {
			cancel()
		}
	})
	c, err := dial(dctx)
	atomic.StoreInt32(&finished, 1)
	giveUp.Stop()
	p.mu.Lock()
	d.conn, d.err = c, err
	if err == nil {
		p.conn, p.cancel = c, cancel // the context now owns the connection's life
	} else {
		cancel()
	}
	p.dial = nil
	p.mu.Unlock()
	close(d.done)
}

func (p *systemdProps) drop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
}

func (p *systemdProps) uint(ctx context.Context, get func(c *systemd.Conn) (*systemd.Property, error)) (uint64, error) {
	c, err := p.connection(ctx)
	if err != nil {
		return 0, err
	}
	prop, err := get(c)
	if err != nil {
		p.drop()
		return 0, err
	}
	switch v := prop.Value.Value().(type) {
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	default:
		return 0, fmt.Errorf("property %s has unexpected type %T", prop.Name, v)
	}
}

func (p *systemdProps) MainPID(ctx context.Context, unit string) (uint64, error) {
	return p.uint(ctx, func(c *systemd.Conn) (*systemd.Property, error) {
		return c.GetServicePropertyContext(ctx, unit, "MainPID")
	})
}

func (p *systemdProps) ActiveEnterMonotonicUs(ctx context.Context, unit string) (uint64, error) {
	return p.uint(ctx, func(c *systemd.Conn) (*systemd.Property, error) {
		return c.GetUnitPropertyContext(ctx, unit, "ActiveEnterTimestampMonotonic")
	})
}
