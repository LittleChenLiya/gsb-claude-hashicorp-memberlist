// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import "time"

// clock abstracts the time functions that drive state transitions so that
// tests can run against a controllable clock.
type clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
	AfterFunc(d time.Duration, f func()) timer
}

// timer is the minimal portion of *time.Timer used by the state machine.
type timer interface {
	Stop() bool
	Reset(d time.Duration) bool
}

// realClock delegates to the standard time package.
type realClock struct{}

func (realClock) Now() time.Time                            { return time.Now() }
func (realClock) Since(t time.Time) time.Duration           { return time.Since(t) }
func (realClock) AfterFunc(d time.Duration, f func()) timer { return time.AfterFunc(d, f) }
