// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"sync"
	"time"
)

// mockClock is a deterministic clock for tests. Timers scheduled through it
// do not fire until the test explicitly advances the clock past their due
// time, which makes time-dependent behavior (such as the suspicion timeout)
// fully reproducible.
type mockClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*mockTimer
}

// newMockClock returns a mock clock frozen at the given start time.
func newMockClock(start time.Time) *mockClock {
	return &mockClock{now: start}
}

// Now implements clock.
func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc implements clock. The returned timer only fires when the clock
// is advanced past its scheduled time.
func (c *mockClock) AfterFunc(d time.Duration, f func()) timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &mockTimer{clock: c, when: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock forward by d and synchronously fires every timer
// that is due, in chronological order. Timers scheduled by a firing timer's
// own callback are also fired if already due.
func (c *mockClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	for {
		var due *mockTimer
		for _, t := range c.timers {
			if t.stopped || t.fired {
				continue
			}
			if !t.when.After(c.now) {
				if due == nil || t.when.Before(due.when) {
					due = t
				}
			}
		}
		if due == nil {
			c.mu.Unlock()
			return
		}
		due.fired = true
		f := due.f
		c.mu.Unlock()
		f()
		c.mu.Lock()
	}
}

// mockTimer is a timer controlled by a mockClock.
type mockTimer struct {
	clock   *mockClock
	when    time.Time
	f       func()
	stopped bool
	fired   bool
}

// Stop implements timer.
func (t *mockTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

// Reset implements timer.
func (t *mockTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.when = t.clock.now.Add(d)
	t.stopped = false
	t.fired = false
	return wasActive
}
