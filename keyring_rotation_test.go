// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

// TestKeyring_Fingerprint verifies that key fingerprints are deterministic,
// non-reversible, distinct for distinct keys, and that invalid keys are
// rejected.
func TestKeyring_Fingerprint(t *testing.T) {
	key := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	fp, err := FingerprintKey(key)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if fp == "" {
		t.Fatal("expected non-empty fingerprint")
	}
	if len(fp) != fingerprintBytes*2 {
		t.Fatalf("expected %d hex characters, got %q", fingerprintBytes*2, fp)
	}

	// Deterministic.
	fp2, err := FingerprintKey(key)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if fp != fp2 {
		t.Fatalf("fingerprint not deterministic: %q vs %q", fp, fp2)
	}

	// Distinct keys get distinct fingerprints.
	other := []byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	otherFp, err := FingerprintKey(other)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if fp == otherFp {
		t.Fatalf("distinct keys produced the same fingerprint: %q", fp)
	}

	// Invalid key sizes are rejected.
	for _, bad := range [][]byte{nil, []byte("short"), make([]byte, 33)} {
		if _, err := FingerprintKey(bad); err == nil {
			t.Fatalf("expected error for key of length %d", len(bad))
		}
	}
}

// TestKeyring_RotationReadiness runs the full key rotation lifecycle with two
// test keys (add -> switch primary -> remove) and checks:
//   - the readiness report only exposes non-reversible fingerprints
//   - old packets remain receivable while the old key stays on the ring
//   - the reception window closes once the old key is removed
//   - failure paths: removing the primary (last in-use) key is rejected,
//     removing from an empty ring is rejected without panicking
func TestKeyring_RotationReadiness(t *testing.T) {
	oldKey := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	newKey := []byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

	plaintext := []byte("this is a plain text message")
	extra := []byte("random data")

	oldFp, err := FingerprintKey(oldKey)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	newFp, err := FingerprintKey(newKey)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Start with the old key as the primary.
	keyring, err := NewKeyring(nil, oldKey)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	report := keyring.RotationReadiness()
	if report.TotalKeys != 1 {
		t.Fatalf("expected 1 key, got %d", report.TotalKeys)
	}
	if report.Primary != oldFp {
		t.Fatalf("expected primary %q, got %q", oldFp, report.Primary)
	}
	if len(report.BackupKeys) != 0 {
		t.Fatalf("expected no backup keys, got %v", report.BackupKeys)
	}
	assertNoKeyMaterial(t, report, oldKey)

	// Add the new key; the old key remains primary.
	if err := keyring.AddKey(newKey); err != nil {
		t.Fatalf("err: %s", err)
	}
	report = keyring.RotationReadiness()
	if report.TotalKeys != 2 {
		t.Fatalf("expected 2 keys, got %d", report.TotalKeys)
	}
	if report.Primary != oldFp {
		t.Fatalf("expected primary %q, got %q", oldFp, report.Primary)
	}
	if len(report.BackupKeys) != 1 || report.BackupKeys[0] != newFp {
		t.Fatalf("expected backup [%q], got %v", newFp, report.BackupKeys)
	}
	assertNoKeyMaterial(t, report, oldKey)
	assertNoKeyMaterial(t, report, newKey)

	// Encrypt a packet with each key while both are installed.
	var oldBuf, newBuf bytes.Buffer
	if err := encryptPayload(1, oldKey, plaintext, extra, &oldBuf); err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := encryptPayload(1, newKey, plaintext, extra, &newBuf); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Switch the primary to the new key.
	if err := keyring.UseKey(newKey); err != nil {
		t.Fatalf("err: %s", err)
	}
	report = keyring.RotationReadiness()
	if report.Primary != newFp {
		t.Fatalf("expected primary %q, got %q", newFp, report.Primary)
	}
	if len(report.BackupKeys) != 1 || report.BackupKeys[0] != oldFp {
		t.Fatalf("expected backup [%q], got %v", oldFp, report.BackupKeys)
	}
	assertNoKeyMaterial(t, report, oldKey)
	assertNoKeyMaterial(t, report, newKey)

	// The old packet is still receivable: the old key is still installed as a
	// backup and may decrypt older packets within the rotation window.
	if msg, err := decryptPayload(keyring.GetKeys(), oldBuf.Bytes(), extra); err != nil {
		t.Fatalf("old packet should still be receivable during the rotation window: %v", err)
	} else if !bytes.Equal(msg, plaintext) {
		t.Fatalf("bad: %v", msg)
	}
	if msg, err := decryptPayload(keyring.GetKeys(), newBuf.Bytes(), extra); err != nil {
		t.Fatalf("new packet should be receivable: %v", err)
	} else if !bytes.Equal(msg, plaintext) {
		t.Fatalf("bad: %v", msg)
	}

	// Remove the old key; the old-packet reception window is now closed.
	if err := keyring.RemoveKey(oldKey); err != nil {
		t.Fatalf("err: %s", err)
	}
	report = keyring.RotationReadiness()
	if report.TotalKeys != 1 {
		t.Fatalf("expected 1 key, got %d", report.TotalKeys)
	}
	if report.Primary != newFp {
		t.Fatalf("expected primary %q, got %q", newFp, report.Primary)
	}
	if len(report.BackupKeys) != 0 {
		t.Fatalf("expected no backup keys, got %v", report.BackupKeys)
	}
	assertNoKeyMaterial(t, report, oldKey)
	assertNoKeyMaterial(t, report, newKey)

	if _, err := decryptPayload(keyring.GetKeys(), oldBuf.Bytes(), extra); err == nil {
		t.Fatal("expected old packet to be undecryptable after the old key is removed")
	}
	// The new packet still decrypts fine.
	if msg, err := decryptPayload(keyring.GetKeys(), newBuf.Bytes(), extra); err != nil {
		t.Fatalf("new packet should still be receivable: %v", err)
	} else if !bytes.Equal(msg, plaintext) {
		t.Fatalf("bad: %v", msg)
	}

	// Failure path: removing the primary (the last key in use) must be rejected.
	if err := keyring.RemoveKey(newKey); err == nil {
		t.Fatal("expected removal of the primary key to be rejected")
	}
	report = keyring.RotationReadiness()
	if report.TotalKeys != 1 || report.Primary != newFp {
		t.Fatalf("keyring changed after rejected removal: %+v", report)
	}

	// Failure path: removing from an empty keyring must be rejected, not panic.
	empty, err := NewKeyring(nil, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := empty.RemoveKey(oldKey); err == nil {
		t.Fatal("expected removal from an empty keyring to be rejected")
	}
	report = empty.RotationReadiness()
	if report.TotalKeys != 0 || report.Primary != "" || len(report.BackupKeys) != 0 {
		t.Fatalf("unexpected report for empty keyring: %+v", report)
	}
}

// TestKeyring_RotationReadiness_MultipleBackups verifies that every non-primary
// key installed on the ring is reported as a backup that may still receive
// old packets.
func TestKeyring_RotationReadiness_MultipleBackups(t *testing.T) {
	keyring, err := NewKeyring(TestKeys, TestKeys[0])
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	report := keyring.RotationReadiness()
	if report.TotalKeys != len(TestKeys) {
		t.Fatalf("expected %d keys, got %d", len(TestKeys), report.TotalKeys)
	}

	primaryFp, err := FingerprintKey(TestKeys[0])
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if report.Primary != primaryFp {
		t.Fatalf("expected primary %q, got %q", primaryFp, report.Primary)
	}

	if len(report.BackupKeys) != len(TestKeys)-1 {
		t.Fatalf("expected %d backup keys, got %d", len(TestKeys)-1, len(report.BackupKeys))
	}
	for i, key := range TestKeys[1:] {
		fp, err := FingerprintKey(key)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if report.BackupKeys[i] != fp {
			t.Fatalf("backup key %d: expected %q, got %q", i, fp, report.BackupKeys[i])
		}
	}

	for _, key := range TestKeys {
		assertNoKeyMaterial(t, report, key)
	}
}

// assertNoKeyMaterial fails the test if the rendered report contains the raw
// key bytes in any common encoding.
func assertNoKeyMaterial(t *testing.T, report KeyRotationReport, keys ...[]byte) {
	t.Helper()
	rendered := fmt.Sprintf("%+v", report)
	for _, key := range keys {
		if strings.Contains(rendered, string(key)) {
			t.Fatalf("report leaked raw key bytes: %q", rendered)
		}
		if strings.Contains(rendered, hex.EncodeToString(key)) {
			t.Fatalf("report leaked key material in hex form: %q", rendered)
		}
	}
}
