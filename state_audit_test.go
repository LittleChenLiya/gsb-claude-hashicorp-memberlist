// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"testing"
	"time"
)

// injectPacket encodes msg as msgType and writes it to the peer's mock
// transport, so it travels the full packet path (packetListen -> ingest ->
// handleCommand -> packetHandler -> state transition).
func injectPacket(t *testing.T, from *MockTransport, to *MockTransport, msgType messageType, msg any, conf *Config) {
	t.Helper()
	buf, err := encode(msgType, msg, conf.MsgpackUseNewTimeFormat)
	if err != nil {
		t.Fatalf("failed to encode %v: %v", msgType, err)
	}
	if _, err := from.WriteTo(buf.Bytes(), to.addr.String()); err != nil {
		t.Fatalf("failed to inject packet: %v", err)
	}
}

// waitForState polls until the named node reaches the expected state or the
// deadline expires. A node that is not yet known is treated as being in an
// unknown state and the wait continues.
func waitForState(t *testing.T, m *Memberlist, node string, want NodeStateType) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m.nodeLock.RLock()
		state, ok := m.nodeMap[node]
		var got NodeStateType
		if ok {
			got = state.State
		}
		m.nodeLock.RUnlock()
		if ok && got == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	m.nodeLock.RLock()
	state, ok := m.nodeMap[node]
	var got NodeStateType
	if ok {
		got = state.State
	}
	m.nodeLock.RUnlock()
	t.Fatalf("node %q did not reach state %v (got %v, known=%v)", node, want, got, ok)
}

// waitForEventCount polls until the audit log has at least n events.
func waitForEventCount(t *testing.T, m *Memberlist, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(m.StateTransitionEvents()) >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected at least %d state transition events, got %d", n, len(m.StateTransitionEvents()))
}

// TestMemberlist_StateTransitionAudit is the local acceptance test for the
// member state transition audit. It drives a single memberlist instance with
// a mock clock and injected gossip packets and verifies that:
//
//  1. each real state change (alive -> suspect -> alive recovery -> suspect
//     -> dead) is recorded as an ordered, read-only audit event;
//  2. duplicate gossip messages do not produce duplicate events;
//  3. messages carrying an old incarnation number do not roll back state or
//     produce events.
func TestMemberlist_StateTransitionAudit(t *testing.T) {
	// Wire up two mock transports: t1 is the memberlist under test, t2 is
	// the peer that injects gossip packets.
	net := &MockNetwork{}
	t1 := net.NewTransport("node1")
	t2 := net.NewTransport("node2")

	m := GetMemberlist(t, func(c *Config) {
		c.Transport = t1
		c.ProbeInterval = time.Second
		c.SuspicionMult = 1 // k = 0: suspicion timer fires at min time
		c.SuspicionMaxTimeoutMult = 1
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	// Replace the real clock with a deterministic mock clock frozen at a
	// known time. All state timestamps and the suspicion timer are driven
	// by this clock.
	clock := newMockClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	m.clock = clock

	const node = "test"
	vsn := m.config.BuildVsnArray()

	// 1) Inject an alive packet for the node at incarnation 1.
	injectPacket(t, t2, t1, aliveMsg, &alive{
		Node: node, Addr: []byte{127, 0, 0, 1}, Port: 8000,
		Incarnation: 1, Vsn: vsn,
	}, m.config)
	waitForState(t, m, node, StateAlive)
	waitForEventCount(t, m, 1)

	// 2) Inject a suspect packet at incarnation 1 -> alive -> suspect.
	injectPacket(t, t2, t1, suspectMsg, &suspect{
		Node: node, Incarnation: 1, From: "node2",
	}, m.config)
	waitForState(t, m, node, StateSuspect)
	waitForEventCount(t, m, 2)

	// 3) Inject a duplicate suspect (same incarnation) and an old
	//    incarnation suspect. Neither should change state or add events.
	injectPacket(t, t2, t1, suspectMsg, &suspect{
		Node: node, Incarnation: 1, From: "node2",
	}, m.config)
	injectPacket(t, t2, t1, suspectMsg, &suspect{
		Node: node, Incarnation: 0, From: "node2",
	}, m.config)
	// Give the packet handler time to process the duplicates.
	time.Sleep(50 * time.Millisecond)
	if got := m.getNodeState(node); got != StateSuspect {
		t.Fatalf("duplicate/old suspect changed state to %v", got)
	}
	if events := m.StateTransitionEvents(); len(events) != 2 {
		t.Fatalf("duplicate/old suspect produced events: %d total", len(events))
	}

	// 4) Inject an alive packet at incarnation 2 -> recovery: suspect ->
	//    alive.
	injectPacket(t, t2, t1, aliveMsg, &alive{
		Node: node, Addr: []byte{127, 0, 0, 1}, Port: 8000,
		Incarnation: 2, Vsn: vsn,
	}, m.config)
	waitForState(t, m, node, StateAlive)
	waitForEventCount(t, m, 3)

	// 5) Inject an old incarnation alive (incarnation 1). It must not roll
	//    back the state or add an event.
	injectPacket(t, t2, t1, aliveMsg, &alive{
		Node: node, Addr: []byte{127, 0, 0, 1}, Port: 8000,
		Incarnation: 1, Vsn: vsn,
	}, m.config)
	time.Sleep(50 * time.Millisecond)
	if got := m.getNodeState(node); got != StateAlive {
		t.Fatalf("old incarnation alive rolled state back to %v", got)
	}
	if events := m.StateTransitionEvents(); len(events) != 3 {
		t.Fatalf("old incarnation alive produced events: %d total", len(events))
	}

	// 6) Inject a suspect packet at incarnation 2 -> alive -> suspect
	//    again, starting a fresh suspicion timer.
	injectPacket(t, t2, t1, suspectMsg, &suspect{
		Node: node, Incarnation: 2, From: "node2",
	}, m.config)
	waitForState(t, m, node, StateSuspect)
	waitForEventCount(t, m, 4)

	// 7) Advance the mock clock past the suspicion timeout. The timer fires
	//    and the node is marked dead.
	clock.Advance(5 * time.Second)
	waitForState(t, m, node, StateDead)
	waitForEventCount(t, m, 5)

	// Verify the final member state.
	if got := m.getNodeState(node); got != StateDead {
		t.Fatalf("final state = %v, want %v", got, StateDead)
	}

	// Verify the audit event order and contents.
	events := m.StateTransitionEvents()
	expected := []StateTransitionEvent{
		{Node: node, From: StateDead, To: StateAlive, Incarnation: 1},
		{Node: node, From: StateAlive, To: StateSuspect, Incarnation: 1},
		{Node: node, From: StateSuspect, To: StateAlive, Incarnation: 2},
		{Node: node, From: StateAlive, To: StateSuspect, Incarnation: 2},
		{Node: node, From: StateSuspect, To: StateDead, Incarnation: 2},
	}
	if len(events) != len(expected) {
		t.Fatalf("got %d events, want %d: %+v", len(events), len(expected), events)
	}
	for i, want := range expected {
		got := events[i]
		if got.Node != want.Node || got.From != want.From || got.To != want.To || got.Incarnation != want.Incarnation {
			t.Fatalf("event %d = %+v, want %+v", i, got, want)
		}
		if got.Time.IsZero() {
			t.Fatalf("event %d has zero time", i)
		}
	}

	// Verify the read-only guarantee: mutating the returned slice must not
	// affect memberlist's internal log.
	snapshot := m.StateTransitionEvents()
	snapshot[0].Node = "mutated"
	if again := m.StateTransitionEvents(); again[0].Node != node {
		t.Fatalf("audit log is not read-only: %q", again[0].Node)
	}

	// Print the audit trail so the behavior difference is directly visible.
	t.Logf("member state transition audit trail for %q:", node)
	for i, e := range events {
		t.Logf("  [%d] %s -> %s (incarnation %d) at %s",
			i, e.From.metricsString(), e.To.metricsString(), e.Incarnation, e.Time.Format(time.RFC3339))
	}
	t.Logf("final member state: %s", m.getNodeState(node).metricsString())
}

// TestMemberlist_StateTransitionAudit_OldIncarnationNoRollback is a focused
// regression test: after a node is dead, an alive message carrying an old
// incarnation number must not resurrect it.
func TestMemberlist_StateTransitionAudit_OldIncarnationNoRollback(t *testing.T) {
	net := &MockNetwork{}
	t1 := net.NewTransport("node1")
	t2 := net.NewTransport("node2")

	m := GetMemberlist(t, func(c *Config) {
		c.Transport = t1
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	const node = "test"
	vsn := m.config.BuildVsnArray()

	// Bring the node alive at incarnation 10, then mark it dead.
	injectPacket(t, t2, t1, aliveMsg, &alive{
		Node: node, Addr: []byte{127, 0, 0, 1}, Port: 8000,
		Incarnation: 10, Vsn: vsn,
	}, m.config)
	waitForState(t, m, node, StateAlive)

	injectPacket(t, t2, t1, deadMsg, &dead{
		Node: node, Incarnation: 10, From: "node2",
	}, m.config)
	waitForState(t, m, node, StateDead)

	eventsBefore := len(m.StateTransitionEvents())

	// An old incarnation alive message must not roll the state back.
	injectPacket(t, t2, t1, aliveMsg, &alive{
		Node: node, Addr: []byte{127, 0, 0, 1}, Port: 8000,
		Incarnation: 9, Vsn: vsn,
	}, m.config)
	time.Sleep(50 * time.Millisecond)

	if got := m.getNodeState(node); got != StateDead {
		t.Fatalf("old incarnation alive rolled dead node back to %v", got)
	}
	if events := m.StateTransitionEvents(); len(events) != eventsBefore {
		t.Fatalf("old incarnation alive produced %d new events", len(events)-eventsBefore)
	}

	// A same-incarnation alive also must not resurrect a dead node.
	injectPacket(t, t2, t1, aliveMsg, &alive{
		Node: node, Addr: []byte{127, 0, 0, 1}, Port: 8000,
		Incarnation: 10, Vsn: vsn,
	}, m.config)
	time.Sleep(50 * time.Millisecond)
	if got := m.getNodeState(node); got != StateDead {
		t.Fatalf("same incarnation alive resurrected dead node to %v", got)
	}
	if events := m.StateTransitionEvents(); len(events) != eventsBefore {
		t.Fatalf("same incarnation alive produced %d new events", len(events)-eventsBefore)
	}

	t.Logf("old-incarnation guards held: node remains %s with %d audit events",
		m.getNodeState(node).metricsString(), len(m.StateTransitionEvents()))
}

// TestMemberlist_StateTransitionAudit_DuplicateGossipNoDuplicateEvent verifies
// that repeated identical gossip messages do not produce duplicate audit
// events.
func TestMemberlist_StateTransitionAudit_DuplicateGossipNoDuplicateEvent(t *testing.T) {
	net := &MockNetwork{}
	t1 := net.NewTransport("node1")
	t2 := net.NewTransport("node2")

	m := GetMemberlist(t, func(c *Config) {
		c.Transport = t1
	})
	defer func() {
		if err := m.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}()

	const node = "test"
	vsn := m.config.BuildVsnArray()

	// Bring the node alive, then dead.
	injectPacket(t, t2, t1, aliveMsg, &alive{
		Node: node, Addr: []byte{127, 0, 0, 1}, Port: 8000,
		Incarnation: 1, Vsn: vsn,
	}, m.config)
	waitForState(t, m, node, StateAlive)

	injectPacket(t, t2, t1, deadMsg, &dead{
		Node: node, Incarnation: 1, From: "node2",
	}, m.config)
	waitForState(t, m, node, StateDead)

	eventsBefore := len(m.StateTransitionEvents())

	// Replay the same dead message several times.
	for i := 0; i < 5; i++ {
		injectPacket(t, t2, t1, deadMsg, &dead{
			Node: node, Incarnation: 1, From: "node2",
		}, m.config)
	}
	time.Sleep(100 * time.Millisecond)

	if got := m.getNodeState(node); got != StateDead {
		t.Fatalf("duplicate dead gossip changed state to %v", got)
	}
	if events := m.StateTransitionEvents(); len(events) != eventsBefore {
		t.Fatalf("duplicate dead gossip produced %d new events", len(events)-eventsBefore)
	}

	t.Logf("duplicate gossip guard held: node remains %s with %d audit events",
		m.getNodeState(node).metricsString(), len(m.StateTransitionEvents()))
}
