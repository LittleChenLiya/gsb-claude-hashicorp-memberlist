// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package memberlist

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

type Keyring struct {
	// Keys stores the key data used during encryption and decryption. It is
	// ordered in such a way where the first key (index 0) is the primary key,
	// which is used for encrypting messages, and is the first key tried during
	// message decryption.
	keys [][]byte

	// The keyring lock is used while performing IO operations on the keyring.
	l sync.Mutex
}

// Init allocates substructures
func (k *Keyring) init() {
	k.keys = make([][]byte, 0)
}

// NewKeyring constructs a new container for a set of encryption keys. The
// keyring contains all key data used internally by memberlist.
//
// While creating a new keyring, you must do one of:
//   - Omit keys and primary key, effectively disabling encryption
//   - Pass a set of keys plus the primary key
//   - Pass only a primary key
//
// If only a primary key is passed, then it will be automatically added to the
// keyring. If creating a keyring with multiple keys, one key must be designated
// primary by passing it as the primaryKey. If the primaryKey does not exist in
// the list of secondary keys, it will be automatically added at position 0.
//
// A key should be either 16, 24, or 32 bytes to select AES-128,
// AES-192, or AES-256.
func NewKeyring(keys [][]byte, primaryKey []byte) (*Keyring, error) {
	keyring := &Keyring{}
	keyring.init()

	if len(keys) > 0 || len(primaryKey) > 0 {
		if len(primaryKey) == 0 {
			return nil, fmt.Errorf("empty primary key not allowed")
		}
		if err := keyring.AddKey(primaryKey); err != nil {
			return nil, err
		}
		for _, key := range keys {
			if err := keyring.AddKey(key); err != nil {
				return nil, err
			}
		}
	}

	return keyring, nil
}

// ValidateKey will check to see if the key is valid and returns an error if not.
//
// key should be either 16, 24, or 32 bytes to select AES-128,
// AES-192, or AES-256.
func ValidateKey(key []byte) error {
	if l := len(key); l != 16 && l != 24 && l != 32 {
		return fmt.Errorf("key size must be 16, 24 or 32 bytes")
	}
	return nil
}

// AddKey will install a new key on the ring. Adding a key to the ring will make
// it available for use in decryption. If the key already exists on the ring,
// this function will just return noop.
//
// key should be either 16, 24, or 32 bytes to select AES-128,
// AES-192, or AES-256.
func (k *Keyring) AddKey(key []byte) error {
	if err := ValidateKey(key); err != nil {
		return err
	}

	k.l.Lock()
	defer k.l.Unlock()

	// No-op if key is already installed
	for _, installedKey := range k.keys {
		if bytes.Equal(installedKey, key) {
			return nil
		}
	}

	keys := append(k.keys, key)
	primaryKey := k.getPrimaryKeyLocked()
	if primaryKey == nil {
		primaryKey = key
	}
	k.installKeysLocked(keys, primaryKey)
	return nil
}

// UseKey changes the key used to encrypt messages. This is the only key used to
// encrypt messages, so peers should know this key before this method is called.
func (k *Keyring) UseKey(key []byte) error {
	k.l.Lock()
	defer k.l.Unlock()
	for _, installedKey := range k.keys {
		if bytes.Equal(key, installedKey) {
			k.installKeysLocked(k.keys, key)
			return nil
		}
	}
	return fmt.Errorf("requested key is not in the keyring")
}

// RemoveKey drops a key from the keyring.
//
// Removal is refused when the key is the currently active primary key at
// position 0 (promote a replacement with UseKey first), when it would leave
// the keyring with no key at all, or when the key is not installed on the
// ring. This guarantees that an encrypted keyring always keeps at least one
// usable key.
func (k *Keyring) RemoveKey(key []byte) error {
	if err := ValidateKey(key); err != nil {
		return err
	}

	k.l.Lock()
	defer k.l.Unlock()

	if len(k.keys) == 0 {
		return fmt.Errorf("removing a key is not allowed: keyring is empty")
	}
	if len(k.keys) == 1 {
		return fmt.Errorf("removing the last active key is not allowed")
	}
	if bytes.Equal(key, k.keys[0]) {
		return fmt.Errorf("removing the primary key is not allowed")
	}

	for i, installedKey := range k.keys {
		if bytes.Equal(key, installedKey) {
			keys := append(k.keys[:i], k.keys[i+1:]...)
			k.installKeysLocked(keys, k.keys[0])
			return nil
		}
	}
	return fmt.Errorf("requested key is not in the keyring")
}

// installKeysLocked will take out a lock on the keyring, and replace the keys with a
// new set of keys. The key indicated by primaryKey will be installed as the new
// primary key. The caller must be holding the lock.
func (k *Keyring) installKeysLocked(keys [][]byte, primaryKey []byte) {
	newKeys := [][]byte{primaryKey}
	for _, key := range keys {
		if !bytes.Equal(key, primaryKey) {
			newKeys = append(newKeys, key)
		}
	}
	k.keys = newKeys
}

// GetKeys returns the current set of keys on the ring.
func (k *Keyring) GetKeys() [][]byte {
	k.l.Lock()
	defer k.l.Unlock()

	return k.keys
}

// GetPrimaryKey returns the key on the ring at position 0. This is the key used
// for encrypting messages, and is the first key tried for decrypting messages.
func (k *Keyring) GetPrimaryKey() (key []byte) {
	k.l.Lock()
	defer k.l.Unlock()
	return k.getPrimaryKeyLocked()
}

// getPrimaryKeyLocked must be called while holding the mutex
func (k *Keyring) getPrimaryKeyLocked() (key []byte) {
	if len(k.keys) > 0 {
		key = k.keys[0]
	}
	return
}

// keyFingerprintLength is the number of SHA-256 bytes rendered in a key
// fingerprint. The bytes are hex encoded, so each fingerprint is twice this
// length as a string.
const keyFingerprintLength = 8

// KeyFingerprint returns a short, non-reversible identifier for an encryption
// key, suitable for logs and readiness reports. It is computed from the first
// bytes of the SHA-256 digest of the key material, so it never exposes the
// raw key bytes and cannot be used to reconstruct the key.
//
// This is intended as a correlation handle (for example, comparing the key a
// peer reports as primary against the local keyring), not as a security
// boundary.
func KeyFingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:keyFingerprintLength])
}

// KeyRotationReadiness describes whether the keyring is in a safe state with
// respect to a key rotation. All key references are fingerprints produced by
// KeyFingerprint; the report never contains raw key bytes.
type KeyRotationReadiness struct {
	// PrimaryKeyID identifies the key at position 0, which is used to
	// encrypt outgoing messages and is the first key tried for decryption.
	PrimaryKeyID string

	// LegacyKeyIDs identifies every non-primary key still installed on the
	// ring. Incoming packets encrypted with any of these keys can still be
	// decrypted, so they bound the window in which old packets encrypted
	// before a primary-key cut-over are accepted. Remove them once all peers
	// have converged on the new primary key.
	LegacyKeyIDs []string

	// Ready is true only when the primary key is the sole installed key. At
	// that point no old packets (encrypted with a retired key) can be
	// decrypted and the rotation is fully converged. It is false while any
	// legacy key still accepts old packets.
	Ready bool
}

// String renders the readiness report as a single human-readable line using
// only key fingerprints, never raw key bytes.
func (r *KeyRotationReadiness) String() string {
	return fmt.Sprintf(
		"key rotation readiness: primary=%s legacy_keys=[%s] ready=%t",
		r.PrimaryKeyID,
		strings.Join(r.LegacyKeyIDs, ","),
		r.Ready,
	)
}

// RotationReadiness returns a KeyRotationReadiness snapshot for the keyring.
// The snapshot reports the fingerprint of the current primary key and the
// fingerprints of the backup keys that can still decrypt old packets.
//
// It returns an error when the keyring holds no keys, since a keyring without
// keys cannot serve encrypted traffic and has no primary key to rotate to.
// The report and its string form never contain raw key bytes.
func (k *Keyring) RotationReadiness() (*KeyRotationReadiness, error) {
	k.l.Lock()
	defer k.l.Unlock()

	if len(k.keys) == 0 {
		return nil, fmt.Errorf("keyring has no keys installed")
	}

	report := &KeyRotationReadiness{
		PrimaryKeyID: KeyFingerprint(k.keys[0]),
		LegacyKeyIDs: make([]string, 0, len(k.keys)-1),
		Ready:        len(k.keys) == 1,
	}
	for _, key := range k.keys[1:] {
		report.LegacyKeyIDs = append(report.LegacyKeyIDs, KeyFingerprint(key))
	}
	return report, nil
}
