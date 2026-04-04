package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	messages := []struct {
		topic   string
		payload string
	}{
		{"orders", "buy AAPL 100"},
		{"orders", "sell TSLA 50"},
		{"events", "user_signup"},
	}
	for _, m := range messages {
		if err := log.AppendRecord(Record{
			Topic:     m.topic,
			Partition: 0,
			Payload:   []byte(m.payload),
		}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var recovered []Record
	err = Replay(path, func(r Record) error {
		recovered = append(recovered, r)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(recovered) != len(messages) {
		t.Fatalf("replayed %d records, want %d", len(recovered), len(messages))
	}
	for i, rec := range recovered {
		if rec.Topic != messages[i].topic {
			t.Errorf("[%d] topic: got %q, want %q", i, rec.Topic, messages[i].topic)
		}
		if string(rec.Payload) != messages[i].payload {
			t.Errorf("[%d] payload: got %q, want %q", i, string(rec.Payload), messages[i].payload)
		}
		if rec.Partition != 0 {
			t.Errorf("[%d] partition: got %d, want 0", i, rec.Partition)
		}
		if rec.Timestamp <= 0 {
			t.Errorf("[%d] timestamp should be positive, got %d", i, rec.Timestamp)
		}
	}
}

func TestReplayNonExistent(t *testing.T) {
	err := Replay(filepath.Join(t.TempDir(), "not-found.wal"), func(r Record) error {
		t.Fatal("should not be called")
		return nil
	})
	if err != nil {
		t.Fatalf("replay nonexistent should return nil, got %v", err)
	}
}

func TestReplayTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trunc.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = log.AppendRecord(Record{Topic: "t", Partition: 0, Payload: []byte("full record")})
	_ = log.Close()

	// chop off a few bytes from the end to simulate crash mid-write
	data, _ := os.ReadFile(path)
	_ = os.WriteFile(path, data[:len(data)-3], 0o644)

	var count int
	err = Replay(path, func(r Record) error {
		count++
		return nil
	})
	// should not error — just stop at the truncated record
	if err != nil {
		t.Fatalf("replay truncated: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected truncated tail to be skipped, got %d records", count)
	}
}

func TestReplayTruncatedStrict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "strict.wal")

	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.AppendRecord(Record{Topic: "orders", Partition: 1, Payload: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-2], 0o644); err != nil {
		t.Fatal(err)
	}

	err = ReplayWithOptions(path, ReplayOptions{AllowPartialTail: false}, func(r Record) error { return nil })
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF, got %v", err)
	}
}

func TestReplayOldFormatCompatibility(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.wal")

	if err := writeLegacyRecord(path, "legacy-topic", []byte("legacy-payload")); err != nil {
		t.Fatal(err)
	}

	var got []Record
	if err := Replay(path, func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 {
		t.Fatalf("replayed %d records, want 1", len(got))
	}
	if got[0].Topic != "legacy-topic" {
		t.Fatalf("topic = %q, want legacy-topic", got[0].Topic)
	}
	if got[0].Partition != -1 {
		t.Fatalf("partition = %d, want -1", got[0].Partition)
	}
	if string(got[0].Payload) != "legacy-payload" {
		t.Fatalf("payload = %q, want legacy-payload", string(got[0].Payload))
	}
}

func TestOpenWithSyncOptions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sync.wal")
	log, err := OpenWithOptions(path, Options{
		SyncMode:     SyncInterval,
		SyncInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open with options: %v", err)
	}
	if err := log.AppendRecord(Record{Topic: "t", Partition: 0, Payload: []byte("x")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestParseSyncMode(t *testing.T) {
	mode, err := ParseSyncMode(" always ")
	if err != nil {
		t.Fatal(err)
	}
	if mode != SyncAlways {
		t.Fatalf("mode = %q, want %q", mode, SyncAlways)
	}
	if _, err := ParseSyncMode("invalid"); !errors.Is(err, ErrInvalidSyncMode) {
		t.Fatalf("expected ErrInvalidSyncMode, got %v", err)
	}
}

func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")
	log, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	payload := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = log.AppendRecord(Record{Topic: "bench-topic", Partition: 0, Payload: payload})
	}
}

func writeLegacyRecord(path, topic string, payload []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	topicBytes := []byte(topic)
	header := make([]byte, 14)
	binary.BigEndian.PutUint64(header[0:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint16(header[8:10], uint16(len(topicBytes)))
	binary.BigEndian.PutUint32(header[10:14], uint32(len(payload)))
	if _, err := f.Write(header); err != nil {
		return err
	}
	if _, err := f.Write(topicBytes); err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		return err
	}
	return nil
}
