// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: LGPL-2.1-only

// A02 phase 3, stage 3-B23: production arming of the startup gate and of
// crashSettle (design 6.4 item 3). Only server.Srv.RunStartup calls it; no
// constructor does, so every CommitMgr stays disarmed until then.

package session

import "time"

// ArmProductionStartup arms the startup commit gate and the crashSettle
// prelude. A nil now means time.Now, a nil sleep means time.Sleep. It runs no
// pass: the caller enters through StartupSerialised.
func (m *CommitMgr) ArmProductionStartup(now func() time.Time, sleep func(time.Duration)) {
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = time.Sleep
	}
	m.startup.arm(now)
	m.crashSettle = sleep
}
