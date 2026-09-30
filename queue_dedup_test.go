// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// dedupTestBroadcast is a KeyedBroadcast used to exercise the optional
// dedup window of TransmitLimitedQueue.
type dedupTestBroadcast struct {
	key      string
	msg      []byte
	finished int32
}

func (b *dedupTestBroadcast) DedupKey() string           { return b.key }
func (b *dedupTestBroadcast) Message() []byte            { return b.msg }
func (b *dedupTestBroadcast) Invalidates(Broadcast) bool { return false }
func (b *dedupTestBroadcast) Finished()                  { atomic.AddInt32(&b.finished, 1) }
func (b *dedupTestBroadcast) finishedCount() int         { return int(atomic.LoadInt32(&b.finished)) }

// TestTransmitLimitedQueue_DedupWindow enqueues the same key three times and
// a different key once, then simulates sends and invalidation, asserting on
// the emitted messages and the Finished callback counts.
func TestTransmitLimitedQueue_DedupWindow(t *testing.T) {
	q := &TransmitLimitedQueue{
		RetransmitMult: 3,
		NumNodes:       func() int { return 10 },
		DedupWindow:    2,
	}

	// Enqueue the same key three times and a different key once.
	a1 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v1")}
	a2 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v2")}
	a3 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v3")}
	b1 := &dedupTestBroadcast{key: "beta", msg: []byte("beta-v1")}
	q.QueueBroadcast(a1)
	q.QueueBroadcast(a2)
	q.QueueBroadcast(a3)
	q.QueueBroadcast(b1)

	// Only the newest value per key may remain queued.
	require.Equal(t, 2, q.NumQueued())
	// Each replaced broadcast is finished exactly once.
	require.Equal(t, 1, a1.finishedCount())
	require.Equal(t, 1, a2.finishedCount())
	require.Equal(t, 0, a3.finishedCount())
	require.Equal(t, 0, b1.finishedCount())

	// Simulate a send round: only the newest values are emitted and, since
	// the retransmit limit has not been reached, nothing is finished yet.
	msgs := q.GetBroadcasts(0, 1024)
	require.ElementsMatch(t, [][]byte{[]byte("alpha-v3"), []byte("beta-v1")}, msgs)
	require.Equal(t, 1, a1.finishedCount())
	require.Equal(t, 1, a2.finishedCount())
	require.Equal(t, 0, a3.finishedCount())
	require.Equal(t, 0, b1.finishedCount())
	require.Equal(t, 2, q.NumQueued())

	// Simulate invalidation: a newer value for "alpha" replaces the unsent
	// one even after it has already been transmitted once.
	a4 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v4")}
	q.QueueBroadcast(a4)
	require.Equal(t, 2, q.NumQueued())
	require.Equal(t, 1, a3.finishedCount())
	require.Equal(t, 0, a4.finishedCount())

	// Force the retransmit limit to 1 so the next send finishes everything
	// still queued; the dedup window must not retain ghost keys afterwards.
	q.RetransmitMult = 0
	msgs = q.GetBroadcasts(0, 1024)
	require.ElementsMatch(t, [][]byte{[]byte("alpha-v4"), []byte("beta-v1")}, msgs)
	require.Equal(t, 0, q.NumQueued())
	require.Equal(t, 1, a4.finishedCount())
	require.Equal(t, 1, b1.finishedCount())
	require.Empty(t, q.dedup, "dedup window must not retain ghost keys")
}

// TestTransmitLimitedQueue_DedupWindowCapacity verifies that once the dedup
// window is full, broadcasts with unseen keys are still queued (without
// dedup tracking) and that no ghost keys are left behind after the queue
// drains.
func TestTransmitLimitedQueue_DedupWindowCapacity(t *testing.T) {
	q := &TransmitLimitedQueue{
		RetransmitMult: 3,
		NumNodes:       func() int { return 10 },
		DedupWindow:    1,
	}

	a1 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v1")}
	b1 := &dedupTestBroadcast{key: "beta", msg: []byte("beta-v1")}
	b2 := &dedupTestBroadcast{key: "beta", msg: []byte("beta-v2")}
	q.QueueBroadcast(a1) // tracked: fills the only window slot
	q.QueueBroadcast(b1) // window full: queued untracked
	q.QueueBroadcast(b2) // untracked, so NOT deduplicated against b1

	require.Equal(t, 3, q.NumQueued())
	require.Equal(t, 0, b1.finishedCount())
	require.Len(t, q.dedup, 1)

	// The tracked key still deduplicates even while the window is full.
	a2 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v2")}
	q.QueueBroadcast(a2)
	require.Equal(t, 3, q.NumQueued())
	require.Equal(t, 1, a1.finishedCount())
	require.Equal(t, 0, a2.finishedCount())

	// Drain the queue completely; every broadcast is finished exactly once
	// and the window holds no ghost keys.
	q.RetransmitMult = 0
	msgs := q.GetBroadcasts(0, 1024)
	require.ElementsMatch(t, [][]byte{[]byte("alpha-v2"), []byte("beta-v1"), []byte("beta-v2")}, msgs)
	require.Equal(t, 0, q.NumQueued())
	require.Equal(t, 1, a1.finishedCount())
	require.Equal(t, 1, a2.finishedCount())
	require.Equal(t, 1, b1.finishedCount())
	require.Equal(t, 1, b2.finishedCount())
	require.Empty(t, q.dedup, "dedup window must not retain ghost keys")
}

// TestTransmitLimitedQueue_DedupWindowDisabled verifies the window is
// inert when not configured.
func TestTransmitLimitedQueue_DedupWindowDisabled(t *testing.T) {
	q := &TransmitLimitedQueue{
		RetransmitMult: 3,
		NumNodes:       func() int { return 10 },
	}

	a1 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v1")}
	a2 := &dedupTestBroadcast{key: "alpha", msg: []byte("alpha-v2")}
	q.QueueBroadcast(a1)
	q.QueueBroadcast(a2)

	require.Equal(t, 2, q.NumQueued())
	require.Equal(t, 0, a1.finishedCount())
	require.Equal(t, 0, a2.finishedCount())
	require.Empty(t, q.dedup)
}
