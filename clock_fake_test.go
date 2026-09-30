// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"sort"
	"sync"
	"time"
)

// fakeTimer is a timer driven by fakeClock.
type fakeTimer struct {
	clock    *fakeClock
	deadline time.Time
	fn       func()
	fired    bool
	stopped  bool
}

// Stop prevents the timer from firing. It mirrors time.Timer.Stop: it returns
// true if the call stops the timer before it fired, and false if the timer
// has already fired, been stopped, or is scheduled to fire immediately while
// advancing the clock.
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	delete(t.clock.pending, t)
	return true
}

// Reset changes the timer deadline relative to the current fake time.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	active := !t.fired && !t.stopped
	delete(t.clock.pending, t)
	t.stopped = false
	t.fired = false
	t.deadline = t.clock.now.Add(d)
	t.clock.pending[t] = struct{}{}
	return active
}

// fakeClock is a manually advanced clock used to drive state transitions and
// suspicion timers deterministically. It does not advance on its own.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	pending map[*fakeTimer]struct{}
}

// newFakeClock returns a clock frozen at the given time.
func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{
		now:     start,
		pending: make(map[*fakeTimer]struct{}),
	}
}

// Now returns the current fake time.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Since returns the elapsed fake time since t.
func (c *fakeClock) Since(t time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Sub(t)
}

// AfterFunc schedules f to run after d of fake time has elapsed.
func (c *fakeClock) AfterFunc(d time.Duration, f func()) timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{
		clock:    c,
		deadline: c.now.Add(d),
		fn:       f,
	}
	c.pending[t] = struct{}{}
	return t
}

// Advance moves the fake clock forward by d, running every timer whose
// deadline is reached in deadline order. Callbacks run synchronously.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)

	var due []*fakeTimer
	for t := range c.pending {
		if !t.deadline.After(c.now) {
			due = append(due, t)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].deadline.Before(due[j].deadline)
	})
	c.mu.Unlock()

	for _, t := range due {
		c.fireDue(t)
	}
}

// fireDue fires t if it is still pending and due, temporarily releasing the
// clock lock so callbacks may reschedule or stop timers.
func (c *fakeClock) fireDue(t *fakeTimer) {
	c.mu.Lock()
	if t.stopped || t.fired {
		c.mu.Unlock()
		return
	}
	if _, ok := c.pending[t]; !ok || t.deadline.After(c.now) {
		c.mu.Unlock()
		return
	}
	fn := t.fn
	t.fired = true
	delete(c.pending, t)
	c.mu.Unlock()

	fn()
}
