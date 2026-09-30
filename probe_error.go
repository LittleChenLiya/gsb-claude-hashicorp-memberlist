// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
)

// ProbeErrorCategory is a stable, machine-readable identifier for the class
// of failure that caused a probe to fail. Callers can use it to decide how to
// react to a probe failure instead of matching error strings.
type ProbeErrorCategory string

const (
	// ProbeErrorTimeout indicates the probe exceeded its deadline while
	// dialing, reading or writing (the remote end did not answer in time).
	ProbeErrorTimeout ProbeErrorCategory = "timeout"

	// ProbeErrorTransportClosed indicates the underlying transport was
	// closed before the probe could complete: the peer closed the
	// connection, the transport was shut down, or the connection was reset
	// or broken.
	ProbeErrorTransportClosed ProbeErrorCategory = "transport-closed"

	// ProbeErrorDecodeRejected indicates the peer answered but the reply
	// failed protocol validation: an unexpected message type, a sequence
	// number mismatch, a payload that could not be decoded, or a message
	// that failed decompression/decryption checks.
	ProbeErrorDecodeRejected ProbeErrorCategory = "decode-rejected"
)

// ProbeError wraps a probe failure together with its stable category. The
// wrapped error is preserved and reachable through errors.Unwrap, errors.Is
// and errors.As, so existing error wrapping and retry behavior is unchanged.
type ProbeError struct {
	// Category is the stable classification of the probe failure.
	Category ProbeErrorCategory

	// Err is the underlying error that caused the probe to fail.
	Err error
}

// Error implements the error interface.
func (e *ProbeError) Error() string {
	return fmt.Sprintf("probe %s: %v", e.Category, e.Err)
}

// Unwrap returns the underlying error so errors.Is/errors.As keep working.
func (e *ProbeError) Unwrap() error { return e.Err }

// ClassifyProbeError wraps err in a ProbeError carrying the appropriate
// category. A nil error returns nil. An error that already is (or wraps) a
// *ProbeError is returned unchanged.
func ClassifyProbeError(err error) error {
	if err == nil {
		return nil
	}
	var pe *ProbeError
	if errors.As(err, &pe) {
		return err
	}
	return &ProbeError{Category: classifyProbeError(err), Err: err}
}

// ProbeErrorCategoryOf returns the stable category of a probe error. The
// second return value is false if err is not (and does not wrap) a
// *ProbeError.
func ProbeErrorCategoryOf(err error) (ProbeErrorCategory, bool) {
	var pe *ProbeError
	if errors.As(err, &pe) {
		return pe.Category, true
	}
	return "", false
}

// classifyProbeError maps a raw transport/decode error to a category.
func classifyProbeError(err error) ProbeErrorCategory {
	switch {
	case isTimeoutError(err):
		return ProbeErrorTimeout
	case isTransportClosedError(err):
		return ProbeErrorTransportClosed
	default:
		return ProbeErrorDecodeRejected
	}
}

// isTimeoutError reports whether err is a deadline/timeout error.
func isTimeoutError(err error) bool {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// isTransportClosedError reports whether err indicates the transport was
// closed, reset or broken before a complete reply was received.
func isTransportClosedError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	// On Unix, errors.Is matches the syscall errors directly.
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	// On Windows, syscall.ECONNRESET/EPIPE are invented application-error
	// values (>= 1<<29) that do not match the actual WSA errno carried by the
	// net package. Fall back to matching the stable platform messages.
	msg := err.Error()
	for _, s := range []string{
		"connection reset",
		"broken pipe",
		"use of closed network connection",
		"forcibly closed",
		"connection was aborted",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
