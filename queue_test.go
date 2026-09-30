// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"
)

// for testing only
func (q *TransmitLimitedQueue) orderedView() []*limitedBroadcast {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*limitedBroadcast, 0, q.lenLocked())
	q.walkReadOnlyLocked(true, func(cur *limitedBroadcast) bool {
		out = append(out, cur)
		return true
	})

	return out
}

// countingBroadcast is a NamedBroadcast that counts how many times
// Finished was invoked.
type countingBroadcast struct {
	name     string
	msg      []byte
	finished int
}

func (b *countingBroadcast) Invalidates(other Broadcast) bool {
	ob, ok := other.(*countingBroadcast)
	if !ok {
		return false
	}
	return b.name == ob.name
}

func (b *countingBroadcast) Message() []byte { return b.msg }
func (b *countingBroadcast) Name() string    { return b.name }
func (b *countingBroadcast) Finished()       { b.finished++ }

// assertNoGhostKeys verifies that every dedupe tracking entry points at an
// item that is still present in the queue, i.e. there are no ghost keys.
func assertNoGhostKeys(t *testing.T, q *TransmitLimitedQueue) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	for name, cur := range q.tm {
		if cur == nil {
			t.Fatalf("ghost key %q: nil tracking entry", name)
		}
		if got := q.tq.Get(cur); got != cur {
			t.Fatalf("ghost key %q: tracked item is not in the queue", name)
		}
	}
}

func TestTransmitLimited_DedupeWindow(t *testing.T) {
	// retransmitLimit = RetransmitMult * ceil(log10(NumNodes+1)) = 2
	q := &TransmitLimitedQueue{
		RetransmitMult: 1,
		NumNodes:       func() int { return 10 },
		DedupeWindow:   time.Second,
	}

	a1 := &countingBroadcast{name: "a", msg: []byte("a1")}
	a2 := &countingBroadcast{name: "a", msg: []byte("a2")}
	a3 := &countingBroadcast{name: "a", msg: []byte("a3")}
	b1 := &countingBroadcast{name: "b", msg: []byte("b1")}

	// Enqueue the same key three times and a different key once.
	q.QueueBroadcast(a1)
	q.QueueBroadcast(a2) // replaces a1 (unsent, within window)
	q.QueueBroadcast(a3) // replaces a2 (unsent, within window)
	q.QueueBroadcast(b1)

	require.Equal(t, 2, q.NumQueued(), "only the latest 'a' and 'b' should be queued")
	require.Equal(t, 1, a1.finished, "a1 must be finished exactly once when replaced")
	require.Equal(t, 1, a2.finished, "a2 must be finished exactly once when replaced")
	require.Equal(t, 0, a3.finished, "a3 must not be finished before its time")
	require.Equal(t, 0, b1.finished, "b1 must not be finished before its time")
	assertNoGhostKeys(t, q)

	// First simulated send: newest first -> b1, a3. Both are reinserted.
	first := q.GetBroadcasts(0, 1000)
	require.Equal(t, [][]byte{[]byte("b1"), []byte("a3")}, first, "first send should carry the latest values")
	require.Equal(t, 0, a3.finished)
	require.Equal(t, 0, b1.finished)

	// Second simulated send: both reach the retransmit limit and finish.
	second := q.GetBroadcasts(0, 1000)
	require.Equal(t, [][]byte{[]byte("b1"), []byte("a3")}, second, "second send should retransmit the latest values")
	require.Equal(t, 1, a3.finished, "a3 must be finished exactly once at the transmit limit")
	require.Equal(t, 1, b1.finished, "b1 must be finished exactly once at the transmit limit")

	// Nothing left to send, and no ghost keys.
	require.Equal(t, 0, q.NumQueued())
	require.Empty(t, q.GetBroadcasts(0, 1000))
	assertNoGhostKeys(t, q)
}

func TestTransmitLimited_DedupeWindow_Invalidation(t *testing.T) {
	q := &TransmitLimitedQueue{
		RetransmitMult: 1,
		NumNodes:       func() int { return 10 },
		DedupeWindow:   time.Second,
	}

	a1 := &countingBroadcast{name: "a", msg: []byte("a1")}
	a2 := &countingBroadcast{name: "a", msg: []byte("a2")}
	b1 := &countingBroadcast{name: "b", msg: []byte("b1")}

	q.QueueBroadcast(a1)
	q.QueueBroadcast(a2) // replaces a1
	q.QueueBroadcast(b1)

	require.Equal(t, 2, q.NumQueued())
	require.Equal(t, 1, a1.finished)
	require.Equal(t, 0, a2.finished)
	require.Equal(t, 0, b1.finished)

	// Simulate invalidating everything that is still queued.
	q.Prune(0)

	require.Equal(t, 0, q.NumQueued())
	require.Equal(t, 1, a1.finished, "a1 was already finished by replacement")
	require.Equal(t, 1, a2.finished, "a2 must be finished exactly once by invalidation")
	require.Equal(t, 1, b1.finished, "b1 must be finished exactly once by invalidation")
	assertNoGhostKeys(t, q)
}

func TestTransmitLimited_DedupeWindow_Expiry(t *testing.T) {
	q := &TransmitLimitedQueue{
		RetransmitMult: 1,
		NumNodes:       func() int { return 10 },
		DedupeWindow:   50 * time.Millisecond,
	}

	a1 := &countingBroadcast{name: "a", msg: []byte("a1")}
	q.QueueBroadcast(a1)

	// Let the dedupe window lapse before re-enqueueing the same key.
	time.Sleep(60 * time.Millisecond)

	a2 := &countingBroadcast{name: "a", msg: []byte("a2")}
	q.QueueBroadcast(a2) // outside the window: a1 keeps gossiping

	require.Equal(t, 2, q.NumQueued(), "aged-out broadcast must not be replaced")
	require.Equal(t, 0, a1.finished, "a1 must not be finished when outside the window")
	require.Equal(t, 0, a2.finished)
	assertNoGhostKeys(t, q)
}

func TestTransmitLimited_DedupeWindow_SentItem(t *testing.T) {
	q := &TransmitLimitedQueue{
		RetransmitMult: 3,
		NumNodes:       func() int { return 10 },
		DedupeWindow:   time.Second,
	}

	a1 := &countingBroadcast{name: "a", msg: []byte("a1")}
	q.QueueBroadcast(a1)
	q.GetBroadcasts(0, 1000) // a1 is sent once and reinserted
	require.Equal(t, 1, q.NumQueued())

	a2 := &countingBroadcast{name: "a", msg: []byte("a2")}
	q.QueueBroadcast(a2) // a1 was already sent: it must not be replaced

	require.Equal(t, 2, q.NumQueued(), "already-sent broadcast must not be replaced")
	require.Equal(t, 0, a1.finished, "a1 must not be finished after being sent")
	require.Equal(t, 0, a2.finished)
	assertNoGhostKeys(t, q)
}

func TestTransmitLimited_DedupeWindow_Capacity(t *testing.T) {
	q := &TransmitLimitedQueue{
		RetransmitMult: 1,
		NumNodes:       func() int { return 10 },
		DedupeWindow:   time.Second,
		DedupeMaxKeys:  2,
	}

	a := &countingBroadcast{name: "a", msg: []byte("a")}
	b := &countingBroadcast{name: "b", msg: []byte("b")}
	c := &countingBroadcast{name: "c", msg: []byte("c")}

	q.QueueBroadcast(a)
	q.QueueBroadcast(b)
	q.QueueBroadcast(c) // exceeds DedupeMaxKeys: 'a' tracking is evicted

	require.Equal(t, 3, q.NumQueued(), "eviction only drops tracking, not the broadcast")
	assertNoGhostKeys(t, q)

	// Re-enqueue 'a': it was evicted from tracking, so no replacement occurs.
	a2 := &countingBroadcast{name: "a", msg: []byte("a2")}
	q.QueueBroadcast(a2)
	require.Equal(t, 4, q.NumQueued())
	require.Equal(t, 0, a.finished)
	require.Equal(t, 0, a2.finished)
	assertNoGhostKeys(t, q)

	// Prune down to a single survivor: pruned items' keys must not linger.
	q.Prune(1)
	require.Equal(t, 1, q.NumQueued())
	assertNoGhostKeys(t, q)
}

func TestTransmitLimited_DedupeWindow_Disabled(t *testing.T) {
	// With the window disabled (zero value) the legacy behavior is preserved:
	// any new message invalidates the previous same-key message.
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	a1 := &countingBroadcast{name: "a", msg: []byte("a1")}
	a2 := &countingBroadcast{name: "a", msg: []byte("a2")}
	q.QueueBroadcast(a1)
	q.QueueBroadcast(a2)

	require.Equal(t, 1, q.NumQueued())
	require.Equal(t, 1, a1.finished, "legacy invalidation must finish the old message exactly once")
	require.Equal(t, 0, a2.finished)
}

func TestLimitedBroadcastLess(t *testing.T) {
	cases := []struct {
		Name string
		A    *limitedBroadcast // lesser
		B    *limitedBroadcast
	}{
		{
			"diff-transmits",
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100},
			&limitedBroadcast{transmits: 1, msgLen: 10, id: 100},
		},
		{
			"same-transmits--diff-len",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 10, id: 100},
		},
		{
			"same-transmits--same-len--diff-id",
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 100},
			&limitedBroadcast{transmits: 0, msgLen: 12, id: 90},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			a, b := c.A, c.B

			require.True(t, a.Less(b))

			tree := btree.New(32)

			tree.ReplaceOrInsert(b)
			tree.ReplaceOrInsert(a)

			min := tree.Min().(*limitedBroadcast)
			require.Equal(t, a.transmits, min.transmits)
			require.Equal(t, a.msgLen, min.msgLen)
			require.Equal(t, a.id, min.id)

			max := tree.Max().(*limitedBroadcast)
			require.Equal(t, b.transmits, max.transmits)
			require.Equal(t, b.msgLen, max.msgLen)
			require.Equal(t, b.id, max.id)
		})
	}
}

func TestTransmitLimited_Queue(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 1 }}
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", nil, nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump := q.orderedView()
	if dump[0].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
	if dump[1].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[2].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}

	// Should invalidate previous message
	q.QueueBroadcast(&memberlistBroadcast{"test", nil, nil})

	if q.NumQueued() != 3 {
		t.Fatalf("bad len")
	}
	dump = q.orderedView()
	if dump[0].b.(*memberlistBroadcast).node != "foo" {
		t.Fatalf("missing foo")
	}
	if dump[1].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[2].b.(*memberlistBroadcast).node != "test" {
		t.Fatalf("missing test")
	}
}

func TestTransmitLimited_GetBroadcasts(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 3, NumNodes: func() int { return 10 }}

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// 2 byte overhead per message, should get all 4 messages
	all := q.GetBroadcasts(2, 80)
	require.Equal(t, 4, len(all), "missing messages: %v", prettyPrintMessages(all))

	// 3 byte overhead, should only get 3 messages back
	partial := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial), "missing messages: %v", prettyPrintMessages(partial))
}

func TestTransmitLimited_GetBroadcasts_Limit(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	require.Equal(t, int64(0), q.idGen, "the id generator seed starts at zero")
	require.Equal(t, 2, retransmitLimit(q.RetransmitMult, q.NumNodes()), "sanity check transmit limits")

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	require.Equal(t, int64(4), q.idGen, "we handed out 4 IDs")

	// 3 byte overhead, should only get 3 messages back
	partial1 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial1), "missing messages: %v", prettyPrintMessages(partial1))

	require.Equal(t, int64(4), q.idGen, "id generator doesn't reset until empty")

	partial2 := q.GetBroadcasts(3, 80)
	require.Equal(t, 3, len(partial2), "missing messages: %v", prettyPrintMessages(partial2))

	require.Equal(t, int64(4), q.idGen, "id generator doesn't reset until empty")

	// Only two not expired
	partial3 := q.GetBroadcasts(3, 80)
	require.Equal(t, 2, len(partial3), "missing messages: %v", prettyPrintMessages(partial3))

	require.Equal(t, int64(4), q.idGen, "id generator stays monotonic when empty")

	// Should get nothing
	partial5 := q.GetBroadcasts(3, 80)
	require.Equal(t, 0, len(partial5), "missing messages: %v", prettyPrintMessages(partial5))

	require.Equal(t, int64(4), q.idGen, "id generator stays monotonic when empty")
}

func prettyPrintMessages(msgs [][]byte) []string {
	var out []string
	for _, msg := range msgs {
		out = append(out, "'"+string(msg)+"'")
	}
	return out
}

func TestTransmitLimited_Prune(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	ch1 := make(chan struct{}, 1)
	ch2 := make(chan struct{}, 1)

	// 18 bytes per message
	q.QueueBroadcast(&memberlistBroadcast{"test", []byte("1. this is a test."), ch1})
	q.QueueBroadcast(&memberlistBroadcast{"foo", []byte("2. this is a test."), ch2})
	q.QueueBroadcast(&memberlistBroadcast{"bar", []byte("3. this is a test."), nil})
	q.QueueBroadcast(&memberlistBroadcast{"baz", []byte("4. this is a test."), nil})

	// Keep only 2
	q.Prune(2)

	require.Equal(t, 2, q.NumQueued())

	// Should notify the first two
	select {
	case <-ch1:
	default:
		t.Fatalf("expected invalidation")
	}
	select {
	case <-ch2:
	default:
		t.Fatalf("expected invalidation")
	}

	dump := q.orderedView()

	if dump[0].b.(*memberlistBroadcast).node != "bar" {
		t.Fatalf("missing bar")
	}
	if dump[1].b.(*memberlistBroadcast).node != "baz" {
		t.Fatalf("missing baz")
	}
}

func TestTransmitLimited_ordering(t *testing.T) {
	q := &TransmitLimitedQueue{RetransmitMult: 1, NumNodes: func() int { return 10 }}

	insert := func(name string, transmits int) {
		q.queueBroadcast(&memberlistBroadcast{name, []byte(name), make(chan struct{})}, transmits)
	}

	insert("node0", 0)
	insert("node1", 10)
	insert("node2", 3)
	insert("node3", 4)
	insert("node4", 7)

	dump := q.orderedView()

	if dump[0].transmits != 10 {
		t.Fatalf("bad val %v, %d", dump[0].b.(*memberlistBroadcast).node, dump[0].transmits)
	}
	if dump[1].transmits != 7 {
		t.Fatalf("bad val %v, %d", dump[7].b.(*memberlistBroadcast).node, dump[7].transmits)
	}
	if dump[2].transmits != 4 {
		t.Fatalf("bad val %v, %d", dump[2].b.(*memberlistBroadcast).node, dump[2].transmits)
	}
	if dump[3].transmits != 3 {
		t.Fatalf("bad val %v, %d", dump[3].b.(*memberlistBroadcast).node, dump[3].transmits)
	}
	if dump[4].transmits != 0 {
		t.Fatalf("bad val %v, %d", dump[4].b.(*memberlistBroadcast).node, dump[4].transmits)
	}
}
