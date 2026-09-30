package memberlist

//PLACEHOLDER

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitFor polls cond until it becomes true or the (real time) deadline is
// reached. Packet injection is processed on the memberlist handler
// goroutines, while all state-change timestamps come from the fake clock.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", msg)
		}
		time.Sleep(time.Millisecond)
	}
}

// mustEncodePacket encodes a gossip message for injection into a transport.
func mustEncodePacket(t *testing.T, mt messageType, v any) []byte {
	t.Helper()
	buf, err := encode(mt, v, true)
	require.NoError(t, err)
	return buf.Bytes()
}

// newAuditMemberlist builds a memberlist on a MockNetwork with a frozen clock
// and returns it together with the local transport, a peer transport used to
// inject gossip, and the fake clock.
func newAuditMemberlist(t *testing.T, start time.Time) (*Memberlist, *fakeClock, *MockTransport, *MockTransport) {
	t.Helper()

	network := &MockNetwork{}
	local := network.NewTransport("m")
	peer := network.NewTransport("peer")

	c := testConfig(t)
	c.Transport = local
	c.Name = "m"
	c.ProbeInterval = time.Second
	c.SuspicionMult = 4
	c.Logger = log.New(log.Writer(), "audit:", log.LstdFlags)

	clk := newFakeClock(start)
	m, err := newMemberlist(c)
	require.NoError(t, err)
	m.clock = clk
	t.Cleanup(func() { require.NoError(t, m.Shutdown()) })

	return m, clk, local, peer
}

// inject encodes and delivers a packet from peer to the local memberlist.
func inject(t *testing.T, peer, local *MockTransport, buf []byte) {
	t.Helper()
	_, err := peer.WriteToAddress(buf, Address{
		Addr: local.addr.String(),
		Name: local.addr.name,
	})
	require.NoError(t, err)
}

// nodeStateOf returns the state of a node or -1 if it is not yet known.
func nodeStateOf(m *Memberlist, name string) NodeStateType {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	if n, ok := m.nodeMap[name]; ok {
		return n.State
	}
	return NodeStateType(-1)
}

// TestStateTransitionAudit_Lifecycle drives the full alive -> suspect ->
// recovery (alive) -> suspect -> dead lifecycle through packet injection on
// the mock transport while advancing a fake clock. It verifies the event
// order, the member state at every step, and that duplicate gossip and old
// incarnations neither repeat events nor roll back state.
func TestStateTransitionAudit_Lifecycle(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	m, clk, local, peer := newAuditMemberlist(t, t0)

	target := "test"
	targetAddr := []byte{127, 0, 0, 1}
	targetPort := uint16(1234)
	vsn := m.config.BuildVsnArray()

	// Discovery: an alive message for a brand new node makes it alive but
	// must not produce an audit event.
	inject(t, peer, local, mustEncodePacket(t, aliveMsg, &alive{
		Incarnation: 1,
		Node:        target,
		Addr:        targetAddr,
		Port:        targetPort,
		Vsn:         vsn,
	}))
	waitFor(t, func() bool { return nodeStateOf(m, target) == StateAlive }, "test node alive")
	require.Empty(t, m.StateTransitions(), "discovery must not be audited")

	// Suspect at t0+1s.
	clk.Advance(time.Second)
	inject(t, peer, local, mustEncodePacket(t, suspectMsg, &suspect{
		Incarnation: 1,
		Node:        target,
		From:        "peer",
	}))
	waitFor(t, func() bool { return nodeStateOf(m, target) == StateSuspect }, "test node suspect")
	require.Equal(t, StateSuspect, nodeStateOf(m, target))

	// Repeated gossip for the same suspicion must not repeat the event.
	inject(t, peer, local, mustEncodePacket(t, suspectMsg, &suspect{
		Incarnation: 1,
		Node:        target,
		From:        "peer", // duplicate confirmation from the same node
	}))
	inject(t, peer, local, mustEncodePacket(t, suspectMsg, &suspect{
		Incarnation: 0, // stale incarnation must be ignored
		Node:        target,
		From:        "peer",
	}))

	// Suspect timeout is 4 * log10(max(1,1)) * 1s = 4s; advancing well short
	// of that must leave the node suspect.
	clk.Advance(2 * time.Second)
	require.Equal(t, StateSuspect, nodeStateOf(m, target))

	// Recovery at t0+3s via a newer alive message.
	inject(t, peer, local, mustEncodePacket(t, aliveMsg, &alive{
		Incarnation: 2,
		Node:        target,
		Addr:        targetAddr,
		Port:        targetPort,
		Vsn:         vsn,
	}))
	waitFor(t, func() bool { return nodeStateOf(m, target) == StateAlive }, "test node recovered")

	// Replaying the same alive message must not emit another event.
	inject(t, peer, local, mustEncodePacket(t, aliveMsg, &alive{
		Incarnation: 2,
		Node:        target,
		Addr:        targetAddr,
		Port:        targetPort,
		Vsn:         vsn,
	}))

	// Suspect again at t0+4s.
	clk.Advance(time.Second)
	inject(t, peer, local, mustEncodePacket(t, suspectMsg, &suspect{
		Incarnation: 2,
		Node:        target,
		From:        "peer",
	}))
	waitFor(t, func() bool { return nodeStateOf(m, target) == StateSuspect }, "test node suspect again")

	// Advance past the suspicion timeout (4s). The stale timer from the
	// first suspicion fires too, but its change time no longer matches so it
	// is a no-op; only the current suspicion declares the node dead locally.
	clk.Advance(5 * time.Second)
	waitFor(t, func() bool { return nodeStateOf(m, target) == StateDead }, "test node dead")

	// Gossiped dead messages after the local judgement and stale incarnations
	// must neither repeat events nor roll the state back.
	inject(t, peer, local, mustEncodePacket(t, deadMsg, &dead{
		Incarnation: 2,
		Node:        target,
		From:        "peer",
	}))
	inject(t, peer, local, mustEncodePacket(t, suspectMsg, &suspect{
		Incarnation: 1,
		Node:        target,
		From:        "peer",
	}))
	inject(t, peer, local, mustEncodePacket(t, aliveMsg, &alive{
		Incarnation: 1,
		Node:        target,
		Addr:        targetAddr,
		Port:        targetPort,
		Vsn:         vsn,
	}))

	// Recovery from dead at t0+10s with a newer incarnation.
	clk.Advance(time.Second)
	inject(t, peer, local, mustEncodePacket(t, aliveMsg, &alive{
		Incarnation: 3,
		Node:        target,
		Addr:        targetAddr,
		Port:        targetPort,
		Vsn:         vsn,
	}))
	waitFor(t, func() bool { return nodeStateOf(m, target) == StateAlive }, "dead node revived")

	// Give the late injected messages a moment to settle and assert the full
	// audit trail.
	waitFor(t, func() bool { return len(m.StateTransitions()) == 5 }, "all five events recorded")

	events := m.StateTransitions()
	require.Len(t, events, 5)

	expected := []struct {
		from, to    NodeStateType
		reason      StateTransitionReason
		source      string
		incarnation uint32
		when        time.Time
	}{
		{StateAlive, StateSuspect, TransitionReasonGossip, "peer", 1, t0.Add(time.Second)},
		{StateSuspect, StateAlive, TransitionReasonRecovery, "", 2, t0.Add(3 * time.Second)},
		{StateAlive, StateSuspect, TransitionReasonGossip, "peer", 2, t0.Add(4 * time.Second)},
		{StateSuspect, StateDead, TransitionReasonSuspicionTimeout, m.config.Name, 2, t0.Add(9 * time.Second)},
		{StateDead, StateAlive, TransitionReasonRecovery, "", 3, t0.Add(10 * time.Second)},
	}
	for i, want := range expected {
		got := events[i]
		require.Equalf(t, uint64(i+1), got.Sequence, "event %d sequence", i)
		require.Equalf(t, target, got.NodeName, "event %d node", i)
		require.Equalf(t, want.from, got.From, "event %d from-state", i)
		require.Equalf(t, want.to, got.To, "event %d to-state", i)
		require.Equalf(t, want.reason, got.Reason, "event %d reason", i)
		require.Equalf(t, want.source, got.Source, "event %d source", i)
		require.Equalf(t, want.incarnation, got.Incarnation, "event %d incarnation", i)
		require.Truef(t, got.Timestamp.Equal(want.when), "event %d timestamp %v != %v", i, got.Timestamp, want.when)
	}

	// Timestamps must be non-decreasing and match the recorded changes.
	for i := 1; i < len(events); i++ {
		require.Falsef(t, events[i].Timestamp.Before(events[i-1].Timestamp),
			"event %d timestamp regressed", i)
	}

	// The per-node view must contain the same events in the same order.
	nodeEvents := m.StateTransitionsForNode(target)
	require.Equal(t, events, nodeEvents)
	require.Empty(t, m.StateTransitionsForNode("nobody"))

	// Returned slices are independent copies and must not corrupt the log.
	events[0] = StateTransitionEvent{}
	nodeEvents[0] = StateTransitionEvent{}
	fresh := m.StateTransitions()
	require.Equal(t, target, fresh[0].NodeName)
	require.Equal(t, uint64(1), fresh[0].Sequence)
}

// TestStateTransitionAudit_DeadReasons directly verifies the audit reasons for
// gossiped deaths and intentional leaves, plus duplicate and stale guards.
func TestStateTransitionAudit_DeadReasons(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	m, clk, _, _ := newAuditMemberlist(t, t0)

	// A peer reported dead by another node.
	m.aliveNode(&alive{
		Incarnation: 1, Node: "deadpeer",
		Addr: []byte{127, 0, 0, 1}, Port: 1, Vsn: m.config.BuildVsnArray(),
	}, nil, false)
	clk.Advance(time.Second)
	m.deadNode(&dead{Incarnation: 1, Node: "deadpeer", From: "other"})

	// A node that announces its own leave.
	m.aliveNode(&alive{
		Incarnation: 1, Node: "leaver",
		Addr: []byte{127, 0, 0, 2}, Port: 2, Vsn: m.config.BuildVsnArray(),
	}, nil, false)
	clk.Advance(time.Second)
	m.deadNode(&dead{Incarnation: 1, Node: "leaver", From: "leaver"})

	// Duplicate and stale death information must be ignored entirely.
	m.deadNode(&dead{Incarnation: 1, Node: "deadpeer", From: "other"})
	m.deadNode(&dead{Incarnation: 0, Node: "deadpeer", From: "other"})
	m.deadNode(&dead{Incarnation: 1, Node: "leaver", From: "leaver"})
	m.deadNode(&dead{Incarnation: 0, Node: "leaver", From: "leaver"})

	// Stale gossip must not roll back a dead node.
	m.suspectNode(&suspect{Incarnation: 0, Node: "deadpeer", From: "other"})
	m.aliveNode(&alive{
		Incarnation: 0, Node: "deadpeer",
		Addr: []byte{127, 0, 0, 1}, Port: 1, Vsn: m.config.BuildVsnArray(),
	}, nil, false)
	require.Equal(t, StateDead, m.getNodeState("deadpeer"))
	require.Equal(t, StateLeft, m.getNodeState("leaver"))

	events := m.StateTransitions()
	require.Len(t, events, 2)

	require.Equal(t, StateAlive, events[0].From)
	require.Equal(t, StateDead, events[0].To)
	require.Equal(t, TransitionReasonGossip, events[0].Reason)
	require.Equal(t, "other", events[0].Source)
	require.Equal(t, uint32(1), events[0].Incarnation)
	require.True(t, events[0].Timestamp.Equal(t0.Add(time.Second)))

	require.Equal(t, StateAlive, events[1].From)
	require.Equal(t, StateLeft, events[1].To)
	require.Equal(t, TransitionReasonLeave, events[1].Reason)
	require.Equal(t, "leaver", events[1].Source)
	require.True(t, events[1].Timestamp.Equal(t0.Add(2*time.Second)))
}
