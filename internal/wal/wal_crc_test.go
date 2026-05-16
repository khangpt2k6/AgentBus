package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestV3CRCDetectsCorruption proves that a single bit flip inside the payload
// of a v3 record is caught at replay time.
func TestV3CRCDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := log.AppendRecord(Record{
		Topic:     "orders",
		Partition: 0,
		Payload:   []byte("hello world"),
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Sanity: clean replay succeeds.
	var seen []Record
	if err := Replay(path, func(r Record) error {
		seen = append(seen, r)
		return nil
	}); err != nil {
		t.Fatalf("clean replay: %v", err)
	}
	if len(seen) != 1 || string(seen[0].Payload) != "hello world" {
		t.Fatalf("clean replay returned %v", seen)
	}

	// Corrupt one payload byte. The CRC must catch it.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Payload sits after 2-byte magic + 22-byte header + len("orders") topic
	// + 0-byte key. Flip a byte well inside the payload region.
	corruptIdx := 2 + 22 + len("orders") + 0 + 4 // 4 bytes into "hello world"
	if corruptIdx >= len(raw) {
		t.Fatalf("file shorter than expected: %d bytes", len(raw))
	}
	raw[corruptIdx] ^= 0x01
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write corrupted: %v", err)
	}

	err = Replay(path, func(r Record) error { return nil })
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("expected ErrCorruptRecord on tampered payload, got %v", err)
	}
}

// TestV3RejectsOversizedPayload guards against a tampered header with a
// payloadLen large enough to cause a multi-GiB allocation.
func TestV3RejectsOversizedPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	// Build a minimal-but-invalid V3 header that advertises a huge payload.
	// We write the raw bytes directly to avoid going through AppendRecord
	// (which enforces MaxPayloadSize on the write side).
	raw := make([]byte, 0, 64)
	raw = append(raw, 'G', 'W')        // magic
	raw = append(raw, 3, 0)            // version 3, padding
	raw = append(raw, 0, 0, 0, 0, 0, 0, 0, 0) // timestamp
	raw = append(raw, 0, 0, 0, 0)      // partition
	raw = append(raw, 0, 0)            // topicLen = 0
	raw = append(raw, 0, 0)            // keyLen = 0
	raw = append(raw, 0xFF, 0xFF, 0xFF, 0xFF) // payloadLen ≈ 4 GiB
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	err := Replay(path, func(r Record) error { return nil })
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("expected ErrCorruptRecord on oversized payloadLen, got %v", err)
	}
}
