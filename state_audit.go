// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import "time"

// StateTransitionEvent records a single local judgment that a member's state
// changed (alive -> suspect -> dead/left) along with the incarnation number
// of the member at the time of the transition and the time at which the
// transition was observed locally.
//
// The audit log is append-only: an event is recorded only when the local
// state of a member actually changes. Duplicate gossip messages (e.g. an
// identical suspect or dead message) and stale messages carrying an old
// incarnation number do not produce events and never roll back state.
type StateTransitionEvent struct {
	// Node is the name of the member whose state changed.
	Node string

	// From is the state the member transitioned away from.
	From NodeStateType

	// To is the state the member transitioned to.
	To NodeStateType

	// Incarnation is the incarnation number of the member at the time of
	// the transition.
	Incarnation uint32

	// Time is the local time at which the transition was observed.
	Time time.Time
}

// stateTransitionLog is an append-only, read-by-copy audit trail of member
// state transitions observed by this node.
type stateTransitionLog struct {
	events []StateTransitionEvent
}

// append records a new transition event.
func (l *stateTransitionLog) append(e StateTransitionEvent) {
	l.events = append(l.events, e)
}

// snapshot returns a copy of the events recorded so far. The returned slice
// is independent of the log and will not be mutated by memberlist.
func (l *stateTransitionLog) snapshot() []StateTransitionEvent {
	out := make([]StateTransitionEvent, len(l.events))
	copy(out, l.events)
	return out
}
