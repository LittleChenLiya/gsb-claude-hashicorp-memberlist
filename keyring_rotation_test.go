// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// rotationOldKey and rotationNewKey are two locally controlled,
// deterministic AES-128 test keys used to drive the full rotation
// lifecycle: the old key starts as primary and the new key is staged,
// promoted, then the old key is retired.
var (
	rotationOldKey = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
	rotationNewKey = []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}
)

// encryptedTestFixture produces a packet encrypted with the given key that
// the keyring must be able to decrypt.
func encryptedTestFixture(t *testing.T, key, plaintext, extra []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := encryptPayload(1, key, plaintext, extra, &buf); err != nil {
		t.Fatalf("err: %v", err)
	}
	return buf.Bytes()
}

// TestKeyring_RotationLifecycle drives add, cut-over and delete with two
// test keys, and checks the readiness report plus the old-packet acceptance
// window at every stage.
func TestKeyring_RotationLifecycle(t *testing.T) {
	plaintext := []byte("rotation lifecycle message")
	extra := []byte("rotation aad")

	// Start with a single (old) primary key.
	keyring, err := NewKeyring(nil, rotationOldKey)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	report, err := keyring.RotationReadiness()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if report.PrimaryKeyID != KeyFingerprint(rotationOldKey) {
		t.Fatalf("expected old key fingerprint as primary, got %q", report.PrimaryKeyID)
	}
	if len(report.LegacyKeyIDs) != 0 || !report.Ready {
		t.Fatalf("expected a converged single-key ring, got %+v", report)
	}

	// Stage the new key: AddKey must not change the primary key.
	if err := keyring.AddKey(rotationNewKey); err != nil {
		t.Fatalf("err: %s", err)
	}
	if got := keyring.GetPrimaryKey(); !bytes.Equal(got, rotationOldKey) {
		t.Fatalf("primary key changed after AddKey: %v", got)
	}

	report, err = keyring.RotationReadiness()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if report.PrimaryKeyID != KeyFingerprint(rotationOldKey) {
		t.Fatalf("unexpected primary fingerprint: %q", report.PrimaryKeyID)
	}
	if len(report.LegacyKeyIDs) != 1 ||
		report.LegacyKeyIDs[0] != KeyFingerprint(rotationNewKey) {
		t.Fatalf("expected staged new key as sole legacy key, got %+v", report)
	}
	if report.Ready {
		t.Fatalf("ring must not be ready while a staged key is installed")
	}

	// Cut over: the new key becomes primary. Packets encrypted with the new
	// key decrypt, and old packets encrypted with the old (now legacy) key
	// must still be decryptable during the overlap window.
	if err := keyring.UseKey(rotationNewKey); err != nil {
		t.Fatalf("err: %s", err)
	}

	report, err = keyring.RotationReadiness()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if report.PrimaryKeyID != KeyFingerprint(rotationNewKey) {
		t.Fatalf("expected new key fingerprint as primary, got %q", report.PrimaryKeyID)
	}
	if len(report.LegacyKeyIDs) != 1 ||
		report.LegacyKeyIDs[0] != KeyFingerprint(rotationOldKey) {
		t.Fatalf("expected old key as the legacy key, got %+v", report)
	}
	if report.Ready {
		t.Fatalf("ring must not be ready while the old key accepts packets")
	}
	t.Logf("during rotation window: %s", report)

	oldPacket := encryptedTestFixture(t, rotationOldKey, plaintext, extra)
	newPacket := encryptedTestFixture(t, rotationNewKey, plaintext, extra)
	for name, packet := range map[string][]byte{"old": oldPacket, "new": newPacket} {
		msg, err := decryptPayload(keyring.GetKeys(), packet, extra)
		if err != nil {
			t.Fatalf("%s key packet failed to decrypt during overlap window: %s", name, err)
		}
		if !bytes.Equal(msg, plaintext) {
			t.Fatalf("bad %s key plaintext: %v", name, msg)
		}
	}

	// Retire the old key: it is now a non-primary key, so removal succeeds.
	if err := keyring.RemoveKey(rotationOldKey); err != nil {
		t.Fatalf("err: %s", err)
	}

	report, err = keyring.RotationReadiness()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if report.PrimaryKeyID != KeyFingerprint(rotationNewKey) {
		t.Fatalf("unexpected primary after retirement: %q", report.PrimaryKeyID)
	}
	if len(report.LegacyKeyIDs) != 0 {
		t.Fatalf("expected no legacy keys, got %+v", report.LegacyKeyIDs)
	}
	if !report.Ready {
		t.Fatalf("ring should report ready once only the new key remains")
	}
	t.Logf("after retirement: %s", report)

	// The old-packet acceptance window is now closed: a packet encrypted
	// before cut-over must no longer decrypt.
	if _, err := decryptPayload(keyring.GetKeys(), oldPacket, extra); err == nil {
		t.Fatalf("old packet unexpectedly decrypted after old key removal")
	}
	// Packets on the new primary still decrypt.
	if _, err := decryptPayload(keyring.GetKeys(), newPacket, extra); err != nil {
		t.Fatalf("new primary packet failed to decrypt: %s", err)
	}
}

// TestKeyring_RotationReadiness_RejectDeletingLastKey covers the failure
// path: deleting the primary key, the sole remaining key, an uninstalled
// key, or an invalid-length key must all be rejected and leave the ring
// usable.
func TestKeyring_RotationReadiness_RejectDeletingLastKey(t *testing.T) {
	keyring, err := NewKeyring([][]byte{rotationNewKey}, rotationOldKey)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	// Removing the active primary key is refused, even with a backup present.
	if err := keyring.RemoveKey(rotationOldKey); err == nil {
		t.Fatalf("expected error removing the primary key")
	}

	// Reduce the ring to a single key, then removing that last key is
	// refused even though it is also the primary key.
	if err := keyring.RemoveKey(rotationNewKey); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := keyring.RemoveKey(rotationOldKey); err == nil {
		t.Fatalf("expected error removing the last active key")
	}

	// A refused deletion must not have damaged the ring.
	report, err := keyring.RotationReadiness()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if report.PrimaryKeyID != KeyFingerprint(rotationOldKey) || !report.Ready {
		t.Fatalf("ring not intact after refused deletion: %+v", report)
	}
	packet := encryptedTestFixture(t, rotationOldKey, []byte("still alive"), nil)
	if _, err := decryptPayload(keyring.GetKeys(), packet, nil); err != nil {
		t.Fatalf("keyring unusable after refused deletion: %s", err)
	}

	// Removing a key that was never installed is an error rather than a
	// silent no-op.
	if err := keyring.RemoveKey(rotationNewKey); err == nil {
		t.Fatalf("expected error removing an uninstalled key")
	}

	// Removing an invalid-length key is an error.
	if err := keyring.RemoveKey([]byte("too-short")); err == nil {
		t.Fatalf("expected error removing an invalid key")
	}
}

// TestKeyring_RotationReadiness_EmptyRing covers the failure path on a ring
// with encryption disabled.
func TestKeyring_RotationReadiness_EmptyRing(t *testing.T) {
	keyring, err := NewKeyring(nil, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if _, err := keyring.RotationReadiness(); err == nil {
		t.Fatalf("expected error asking for readiness of an empty ring")
	}

	// Deleting from an empty ring must return an error instead of panicking.
	if err := keyring.RemoveKey(rotationOldKey); err == nil {
		t.Fatalf("expected error removing a key from an empty ring")
	}
}

// TestKeyring_RotationReadiness_NoKeyLeakage asserts that the readiness
// report, including its rendered string form, exposes only fingerprints and
// never the raw key bytes, in either binary or hex-encoded form.
func TestKeyring_RotationReadiness_NoKeyLeakage(t *testing.T) {
	keyring, err := NewKeyring(nil, rotationOldKey)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := keyring.AddKey(rotationNewKey); err != nil {
		t.Fatalf("err: %s", err)
	}

	report, err := keyring.RotationReadiness()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	rendered := report.String()
	t.Logf("rendered report: %s", rendered)

	for i, key := range [][]byte{rotationOldKey, rotationNewKey} {
		if bytes.Contains([]byte(rendered), key) {
			t.Fatalf("report leaks raw bytes of key %d", i)
		}
		hexKey := strings.ToUpper(hex.EncodeToString(key))
		if strings.Contains(strings.ToUpper(rendered), hexKey) {
			t.Fatalf("report leaks hex-encoded bytes of key %d", i)
		}
	}

	if report.PrimaryKeyID != KeyFingerprint(rotationOldKey) {
		t.Fatalf("bad primary fingerprint")
	}
	if !strings.Contains(rendered, report.PrimaryKeyID) ||
		!strings.Contains(rendered, report.LegacyKeyIDs[0]) {
		t.Fatalf("rendered report missing key fingerprints: %s", rendered)
	}
	if !strings.Contains(rendered, "ready=false") {
		t.Fatalf("rendered report missing readiness flag: %s", rendered)
	}
}

// TestKeyFingerprint checks the fingerprint properties relied on by reports.
func TestKeyFingerprint(t *testing.T) {
	if got := KeyFingerprint(rotationOldKey); got != KeyFingerprint(rotationOldKey) {
		t.Fatalf("fingerprint is not stable: %q", got)
	}
	if KeyFingerprint(rotationOldKey) == KeyFingerprint(rotationNewKey) {
		t.Fatalf("distinct keys produced identical fingerprints")
	}
	if got := len(KeyFingerprint(rotationOldKey)); got != keyFingerprintLength*2 {
		t.Fatalf("expected %d hex chars, got %d", keyFingerprintLength*2, got)
	}
	if _, err := hex.DecodeString(KeyFingerprint(rotationOldKey)); err != nil {
		t.Fatalf("fingerprint is not valid hex: %s", err)
	}
}
