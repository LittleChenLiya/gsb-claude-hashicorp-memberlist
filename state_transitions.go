// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import "time"

// maxStateTransitions is the maximum number of member state transition events
// retained locally for audit. The oldest events are discarded once the log
// fills up.
const maxStateTransitions = 4096

// StateTransitionReason describes why a member state transition was recorded.
type StateTransitionReason uint8

const (
	// TransitionReasonGossip is used for transitions driven by state
	// information gossiped by another node (suspect or dead messages).
	TransitionReasonGossip StateTransitionReason = iota + 1

	// TransitionReasonSuspicionTimeout is used when the local suspicion
	// timer for a node expires and the node is locally judged dead.
	TransitionReasonSuspicionTimeout

	// TransitionReasonRecovery is used when a suspect or dead node
	// refutes its accusation with a newer alive message and becomes alive
	// again.
	TransitionReasonRecovery

	// TransitionReasonLeave is used when a node sends a dead message about
	// itself, i.e. an intentional leave.
	TransitionReasonLeave
)

// String returns a human readable representation of the transition reason.
func (r StateTransitionReason) String() string {
	switch r {
	case TransitionReasonGossip:
		return "gossip"
	case TransitionReasonSuspicionTimeout:
		return "suspicion-timeout"
	case TransitionReasonRecovery:
		return "recovery"
	case TransitionReasonLeave:
		return "leave"
	default:
		return "unknown"
	}
}

// StateTransitionEvent is a read-only audit record of a local member state
// transition. Events are only appended when the local Memberlist actually
// changes its view of a node, so repeated gossip for the same state and
// messages with stale incarnation numbers never produce events.
type StateTransitionEvent struct {
	// Sequence is the local, monotonically increasing sequence number of
	// the event across all members.
	Sequence uint64

	// NodeName is the name of the member whose state changed.
	NodeName string

	// From is the previous local state of the member.
	From NodeStateType

	// To is the new local state of the member (StateAlive, StateSuspect,
	// StateDead or StateLeft).
	To NodeStateType

	// Incarnation is the incarnation number of the member after the
	// transition.
	Incarnation uint32

	// Reason describes what triggered the transition.
	Reason StateTransitionReason

	// Source is the name of the node that originated the message driving
	// the transition (the "from" field of a suspect/dead message). It is
	// empty for a local suspicion timeout or a self-originated recovery.
	Source string

	// Timestamp is the local time at which the transition was recorded.
	Timestamp time.Time
}

// StateTransitions returns a copy of the locally recorded member state
// transition events, oldest first. The returned slice is safe to use after
// the call and is never mutated by the Memberlist.
func (m *Memberlist) StateTransitions() []StateTransitionEvent {
	m.transitionLock.Lock()
	defer m.transitionLock.Unlock()

	out := make([]StateTransitionEvent, len(m.transitions))
	copy(out, m.transitions)
	return out
}

// StateTransitionsForNode returns a copy of the locally recorded state
// transition events for the named node, oldest first. The returned slice is
// safe to use after the call and is never mutated by the Memberlist.
func (m *Memberlist) StateTransitionsForNode(nodeName string) []StateTransitionEvent {
	m.transitionLock.Lock()
	defer m.transitionLock.Unlock()

	var out []StateTransitionEvent
	for _, e := range m.transitions {
		if e.NodeName == nodeName {
			out = append(out, e)
		}
	}
	return out
}

// recordStateTransition appends an audit event for a member state change. It
// is a defensive no-op when there is no effective change, which guards
// against duplicate events from repeated gossip. It must be called while
// nodeLock is held so transitions for a member cannot interleave.
func (m *Memberlist) recordStateTransition(state *nodeState, from, to NodeStateType, reason StateTransitionReason, source string, at time.Time) {
	if state == nil || from == to {
		return
	}

	m.transitionLock.Lock()
	m.transitionSeq++
	ev := StateTransitionEvent{
		Sequence:    m.transitionSeq,
		NodeName:    state.Name,
		From:        from,
		To:          to,
		Incarnation: state.Incarnation,
		Reason:      reason,
		Source:      source,
		Timestamp:   at,
	}
	if len(m.transitions) >= maxStateTransitions {
		copy(m.transitions, m.transitions[1:])
		m.transitions[len(m.transitions)-1] = ev
	} else {
		m.transitions = append(m.transitions, ev)
	}
	m.transitionLock.Unlock()
}
