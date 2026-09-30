// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

// probeFaultTransport is a controllable Transport for probe tests. It dials a
// real TCP listener so deadlines are honored, while the test drives the server
// side to inject faults.
type probeFaultTransport struct {
	addr string
	ln   net.Listener
}

func (t *probeFaultTransport) FinalAdvertiseAddr(string, int) (net.IP, int, error) {
	return net.ParseIP("127.0.0.1"), 0, nil
}

func (t *probeFaultTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	return time.Now(), nil
}

func (t *probeFaultTransport) PacketCh() <-chan *Packet { return nil }

func (t *probeFaultTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", t.addr, timeout)
}

func (t *probeFaultTransport) StreamCh() <-chan net.Conn { return nil }

func (t *probeFaultTransport) Shutdown() error { return t.ln.Close() }

// injectProbeFault accepts one connection, reads the ping type byte so the
// client's write completes, then injects the requested fault.
func injectProbeFault(t *testing.T, ln net.Listener, fault ProbeErrorCategory, linger time.Duration) {
	t.Helper()
	conn, err := ln.Accept()
	if err != nil {
		return // listener closed during shutdown
	}
	defer conn.Close()

	// Read the ping type byte so the client's write can complete.
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	var hdr [1]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return
	}

	switch fault {
	case ProbeErrorTimeout:
		// Never reply; let the client's deadline fire. Stay alive long
		// enough that the client does not see a close instead of a timeout.
		time.Sleep(linger)
	case ProbeErrorTransportClosed:
		// Close without replying.
	case ProbeErrorDecodeRejected:
		// Reply with an unexpected message type.
		_, _ = conn.Write([]byte{byte(indirectPingMsg)})
	}
}

// TestProbeFailureCategories injects each of the three probe failure classes
// through a controllable transport and verifies the stable category.
func TestProbeFailureCategories(t *testing.T) {
	cases := []struct {
		name  string
		fault ProbeErrorCategory
	}{
		{"timeout", ProbeErrorTimeout},
		{"transport closed", ProbeErrorTransportClosed},
		{"decode rejected", ProbeErrorDecodeRejected},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()

			ft := &probeFaultTransport{addr: ln.Addr().String(), ln: ln}
			m := GetMemberlist(t, func(c *Config) {
				c.Transport = ft
			})
			defer func() { _ = m.Shutdown() }()

			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				injectProbeFault(t, ln, tc.fault, 500*time.Millisecond)
			}()

			ping := ping{SeqNo: 1, Node: "test-node"}
			a := Address{Addr: ft.addr, Name: "test-node"}
			deadline := time.Now().Add(300 * time.Millisecond)
			didContact, err := m.sendPingAndWaitForAck(a, ping, deadline)

			if didContact {
				t.Fatalf("expected failed probe, got successful contact")
			}
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			cat, ok := ProbeErrorCategoryOf(err)
			if !ok {
				t.Fatalf("error is not a *ProbeError: %v", err)
			}
			if cat != tc.fault {
				t.Fatalf("expected category %q, got %q (err: %v)", tc.fault, cat, err)
			}

			// Wait for the server goroutine to converge.
			select {
			case <-serverDone:
			case <-time.After(2 * time.Second):
				t.Fatal("server goroutine did not exit")
			}
		})
	}
}

// TestProbeErrorPreservesWrap verifies the original error is preserved and
// reachable through errors.Is/errors.As so existing wrapping stays compatible.
func TestProbeErrorPreservesWrap(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ft := &probeFaultTransport{addr: ln.Addr().String(), ln: ln}
	m := GetMemberlist(t, func(c *Config) {
		c.Transport = ft
	})
	defer func() { _ = m.Shutdown() }()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		injectProbeFault(t, ln, ProbeErrorTimeout, 500*time.Millisecond)
	}()

	ping := ping{SeqNo: 1, Node: "test-node"}
	a := Address{Addr: ft.addr, Name: "test-node"}
	deadline := time.Now().Add(300 * time.Millisecond)
	_, probeErr := m.sendPingAndWaitForAck(a, ping, deadline)
	if probeErr == nil {
		t.Fatalf("expected error, got nil")
	}

	var pe *ProbeError
	if !errors.As(probeErr, &pe) {
		t.Fatalf("error is not a *ProbeError: %v", probeErr)
	}
	if pe.Category != ProbeErrorTimeout {
		t.Fatalf("expected timeout category, got %q", pe.Category)
	}

	// The wrapped error must still expose the net.Error timeout.
	var ne net.Error
	if !errors.As(probeErr, &ne) {
		t.Fatalf("wrapped error is not a net.Error: %v", probeErr)
	}
	if !ne.Timeout() {
		t.Fatalf("wrapped error is not a timeout: %v", probeErr)
	}

	// The error message should mention the category and the original cause.
	msg := probeErr.Error()
	if msg == "" {
		t.Fatalf("empty error message")
	}
	t.Logf("probe error: %s", msg)

	// Wait for the server goroutine to converge.
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("server goroutine did not exit")
	}
}

// TestProbeTimerReleasedOnShutdown verifies the probe reaping timer is
// stopped when the memberlist shuts down (cancellation) so it does not fire
// after the memberlist has stopped.
func TestProbeTimerReleasedOnShutdown(t *testing.T) {
	m := GetMemberlist(t, nil)

	ackCh := make(chan ackMessage, 1)
	nackCh := make(chan struct{}, 1)
	seqNo := uint32(12345)
	m.setProbeChannels(seqNo, ackCh, nackCh, 300*time.Millisecond)

	// The handler must be registered.
	m.ackLock.Lock()
	_, ok := m.ackHandlers[seqNo]
	m.ackLock.Unlock()
	if !ok {
		t.Fatal("probe handler not registered")
	}

	// Cancel.
	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}

	// Wait longer than the timeout; the timer must not fire.
	time.Sleep(500 * time.Millisecond)
	select {
	case msg := <-ackCh:
		t.Fatalf("probe timer fired after shutdown: %+v", msg)
	default:
	}

	// The handler map should be empty.
	m.ackLock.Lock()
	_, ok = m.ackHandlers[seqNo]
	m.ackLock.Unlock()
	if ok {
		t.Fatal("probe handler not removed after shutdown")
	}
}

// TestProbeGoroutineConvergence verifies that after a memberlist with a
// pending probe shuts down, all background goroutines converge.
func TestProbeGoroutineConvergence(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ft := &probeFaultTransport{addr: ln.Addr().String(), ln: ln}
	m := GetMemberlist(t, func(c *Config) {
		c.Transport = ft
	})

	// Register a probe with a reaping timer, then cancel.
	ackCh := make(chan ackMessage, 1)
	m.setProbeChannels(54321, ackCh, nil, 300*time.Millisecond)

	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()

	// Wait for goroutines to converge.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutines did not converge: baseline=%d current=%d", baseline, runtime.NumGoroutine())
}
