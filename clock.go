// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import "time"

// clock abstracts wall-clock time and timer creation so that the protocol's
// time-dependent behavior (notably suspicion timers) can be driven
// deterministically in tests with a mock clock.
type clock interface {
	// Now returns the current time.
	Now() time.Time

	// AfterFunc waits for the duration to elapse and then calls f. The
	// returned timer can be stopped or reset.
	AfterFunc(d time.Duration, f func()) timer
}

// timer is the subset of *time.Timer used by memberlist.
type timer interface {
	// Stop prevents the timer from firing. It returns true if the call
	// stops the timer, false if the timer has already fired or been stopped.
	Stop() bool

	// Reset changes the timer to expire after duration d. It returns true
	// if the timer had been active, false if it had fired or been stopped.
	Reset(d time.Duration) bool
}

// realClock is the default clock, backed by the real system clock.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) timer {
	return &realTimer{t: time.AfterFunc(d, f)}
}

// realTimer wraps a *time.Timer to satisfy the timer interface.
type realTimer struct {
	t *time.Timer
}

func (r *realTimer) Stop() bool { return r.t.Stop() }

func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
