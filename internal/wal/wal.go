package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// crcTable uses CRC32-Castagnoli (CRC32C) — same polynomial as iSCSI, ext4,
// and modern hardware (SSE4.2 has a single-instruction implementation).
// Detects most flip-bit corruption that a noisy disk or partial write leaves
// behind, in roughly free time on modern CPUs.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

var ErrCorruptRecord = errors.New("wal: corrupt record")
var ErrInvalidSyncMode = errors.New("wal: invalid sync mode")
var ErrPayloadTooLarge = errors.New("wal: payload exceeds maximum size")

// MaxPayloadSize bounds per-record payload to defend replay against tampered
// or otherwise malformed records that encode a giant length. Without this,
// a corrupt header with payloadLen = 0xFFFFFFFF would trigger a 4GiB
// allocation attempt during replay.
const MaxPayloadSize = 64 << 20 // 64 MiB

// Record stores one append-only WAL entry.
type Record struct {
	Timestamp int64
	Topic     string
	Key       string
	Partition int32
	Payload   []byte
}

type SyncMode string

const (
	SyncNone     SyncMode = "none"
	SyncAlways   SyncMode = "always"
	SyncInterval SyncMode = "interval"
)

type Options struct {
	SyncMode     SyncMode
	SyncInterval time.Duration
}

type ReplayOptions struct {
	AllowPartialTail bool
}

var DefaultReplayOptions = ReplayOptions{
	AllowPartialTail: true,
}

// Log is an append-only WAL implementation with group-commit fsync.
//
// Concurrent writers are coalesced into a single fsync per "generation".
// While one goroutine performs the fsync syscall (mutex released), other
// writers may continue appending bytes into the next generation's buffer.
// This amortizes a single ~ms fsync cost across many concurrent appends,
// turning N sequential fsyncs into roughly ceil(N / batch_size).
type Log struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer

	syncMode     SyncMode
	syncInterval time.Duration
	lastSync     time.Time

	// Group commit state (guarded by mu).
	syncCond   *sync.Cond
	pendingGen uint64 // generation incoming writes are tagged with
	syncedGen  uint64 // last generation whose fsync has completed
	syncing    bool   // a goroutine is mid Flush+fsync for some gen
	syncErr    error  // result of the last completed sync round
	fatal      bool   // an unrecoverable IO error has poisoned the log
}

func Open(path string) (*Log, error) {
	return OpenWithOptions(path, Options{})
}

func OpenWithOptions(path string, opts Options) (*Log, error) {
	mode := opts.SyncMode
	if mode == "" {
		mode = SyncNone
	}
	if mode != SyncNone && mode != SyncAlways && mode != SyncInterval {
		return nil, ErrInvalidSyncMode
	}
	if mode == SyncInterval && opts.SyncInterval <= 0 {
		opts.SyncInterval = 250 * time.Millisecond
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{
		f:            f,
		w:            bufio.NewWriterSize(f, 1<<20),
		syncMode:     mode,
		syncInterval: opts.SyncInterval,
		lastSync:     time.Now(),
		pendingGen:   1,
	}
	l.syncCond = sync.NewCond(&l.mu)
	return l, nil
}

func (l *Log) Append(topic string, payload []byte) error {
	return l.AppendRecord(Record{
		Timestamp: time.Now().UnixNano(),
		Topic:     topic,
		Partition: -1,
		Payload:   payload,
	})
}

func (l *Log) AppendRecord(rec Record) error {
	if rec.Partition == 0 && rec.Topic == "" {
		return ErrCorruptRecord
	}
	if rec.Timestamp == 0 {
		rec.Timestamp = time.Now().UnixNano()
	}
	if rec.Partition < -1 {
		return ErrCorruptRecord
	}

	topicBytes := []byte(rec.Topic)
	keyBytes := []byte(rec.Key)
	if len(topicBytes) > 0xFFFF {
		return ErrCorruptRecord
	}
	if len(keyBytes) > 0xFFFF {
		return ErrCorruptRecord
	}
	if len(rec.Payload) > MaxPayloadSize {
		return ErrPayloadTooLarge
	}

	header := make([]byte, 24)
	copy(header[0:2], []byte{'G', 'W'})
	header[2] = 3 // version 3: V2 layout + trailing 4-byte CRC32C
	header[3] = 0
	binary.BigEndian.PutUint64(header[4:12], uint64(rec.Timestamp))
	binary.BigEndian.PutUint32(header[12:16], uint32(rec.Partition))
	binary.BigEndian.PutUint16(header[16:18], uint16(len(topicBytes)))
	binary.BigEndian.PutUint16(header[18:20], uint16(len(keyBytes)))
	binary.BigEndian.PutUint32(header[20:24], uint32(len(rec.Payload)))

	// CRC covers everything between the magic and the CRC itself: version
	// byte through end of payload. Computed before taking the WAL mutex so
	// the hot path under lock is minimal.
	h := crc32.New(crcTable)
	h.Write(header[2:])
	h.Write(topicBytes)
	h.Write(keyBytes)
	h.Write(rec.Payload)
	var crcBytes [4]byte
	binary.BigEndian.PutUint32(crcBytes[:], h.Sum32())

	l.mu.Lock()
	if l.fatal {
		err := l.syncErr
		l.mu.Unlock()
		return err
	}
	if _, err := l.w.Write(header); err != nil {
		l.poisonLocked(err)
		l.mu.Unlock()
		return err
	}
	if _, err := l.w.Write(topicBytes); err != nil {
		l.poisonLocked(err)
		l.mu.Unlock()
		return err
	}
	if _, err := l.w.Write(keyBytes); err != nil {
		l.poisonLocked(err)
		l.mu.Unlock()
		return err
	}
	if _, err := l.w.Write(rec.Payload); err != nil {
		l.poisonLocked(err)
		l.mu.Unlock()
		return err
	}
	if _, err := l.w.Write(crcBytes[:]); err != nil {
		l.poisonLocked(err)
		l.mu.Unlock()
		return err
	}

	myGen := l.pendingGen
	needSync := l.shouldSyncLocked()
	if !needSync {
		l.mu.Unlock()
		return nil
	}
	return l.groupCommitLocked(myGen)
}

// shouldSyncLocked decides whether the current append must trigger an fsync
// for SyncAlways, or whether the configured interval has elapsed for SyncInterval.
func (l *Log) shouldSyncLocked() bool {
	switch l.syncMode {
	case SyncAlways:
		return true
	case SyncInterval:
		return time.Since(l.lastSync) >= l.syncInterval
	}
	return false
}

// groupCommitLocked is invoked with l.mu held. It returns with l.mu released.
// Multiple concurrent callers are coalesced: only one performs Flush+Sync per
// generation, and the rest piggyback on that completed round.
func (l *Log) groupCommitLocked(myGen uint64) error {
	for {
		if l.fatal {
			err := l.syncErr
			l.mu.Unlock()
			return err
		}
		if l.syncedGen >= myGen {
			err := l.syncErr
			l.mu.Unlock()
			return err
		}
		if l.syncing {
			l.syncCond.Wait()
			continue
		}

		// Become the syncer for the current pending generation. Flush bufio
		// while holding the lock (bufio.Writer is not concurrent-safe), then
		// release the lock during the slow fsync syscall so subsequent
		// Append callers can fill the next generation's buffer.
		l.syncing = true
		syncingGen := l.pendingGen
		if err := l.w.Flush(); err != nil {
			l.poisonLocked(err)
			l.syncing = false
			l.syncCond.Broadcast()
			l.mu.Unlock()
			return err
		}
		l.pendingGen++ // future writes belong to the next generation
		l.mu.Unlock()

		syncErr := l.f.Sync()

		l.mu.Lock()
		if syncErr != nil {
			l.poisonLocked(syncErr)
			l.syncing = false
			l.syncCond.Broadcast()
			l.mu.Unlock()
			return syncErr
		}
		l.syncedGen = syncingGen
		l.syncErr = nil
		l.lastSync = time.Now()
		l.syncing = false
		l.syncCond.Broadcast()
		// Loop: re-check whether myGen has now been covered (it has, if
		// myGen <= syncingGen, which is always true here).
	}
}

func (l *Log) poisonLocked(err error) {
	l.fatal = true
	l.syncErr = err
}

func (l *Log) Close() error {
	l.mu.Lock()
	for l.syncing {
		l.syncCond.Wait()
	}
	if l.w != nil {
		if err := l.w.Flush(); err != nil {
			l.mu.Unlock()
			return err
		}
	}
	if l.f != nil {
		if l.syncMode != SyncNone {
			if err := l.f.Sync(); err != nil {
				l.mu.Unlock()
				return err
			}
		}
		f := l.f
		l.f = nil
		l.mu.Unlock()
		return f.Close()
	}
	l.mu.Unlock()
	return nil
}

func ParseSyncMode(v string) (SyncMode, error) {
	mode := SyncMode(strings.ToLower(strings.TrimSpace(v)))
	if mode == "" {
		return SyncNone, nil
	}
	if mode != SyncNone && mode != SyncAlways && mode != SyncInterval {
		return "", ErrInvalidSyncMode
	}
	return mode, nil
}

func Replay(path string, fn func(Record) error) error {
	return ReplayWithOptions(path, DefaultReplayOptions, fn)
}

func ReplayWithOptions(path string, opts ReplayOptions, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	for {
		prefix := make([]byte, 2)
		_, err := io.ReadFull(r, prefix)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
				return nil
			}
			return err
		}

		if prefix[0] == 'G' && prefix[1] == 'W' {
			rec, err := readV2Record(r, opts)
			if err != nil {
				return err
			}
			if rec == nil {
				return nil
			}
			if err := fn(*rec); err != nil {
				return err
			}
			continue
		}

		rec, err := readV1Record(r, opts, prefix)
		if err != nil {
			return err
		}
		if rec == nil {
			return nil
		}
		if err := fn(*rec); err != nil {
			return err
		}
	}
}

func readV2Record(r *bufio.Reader, opts ReplayOptions) (*Record, error) {
	// Use stack-allocated array to avoid heap allocation per record.
	var header [22]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
			return nil, nil
		}
		return nil, err
	}
	version := header[0]
	if version != 2 && version != 3 {
		return nil, ErrCorruptRecord
	}
	topicLen := int(binary.BigEndian.Uint16(header[14:16]))
	keyLen := int(binary.BigEndian.Uint16(header[16:18]))
	payloadLen := int(binary.BigEndian.Uint32(header[18:22]))
	// Bound allocation before touching memory — a corrupt header could
	// otherwise request gigabytes.
	if payloadLen < 0 || payloadLen > MaxPayloadSize {
		return nil, ErrCorruptRecord
	}
	body := make([]byte, topicLen+keyLen+payloadLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
			return nil, nil
		}
		return nil, err
	}
	// V3 records carry a CRC32C trailer covering header (less magic) + body.
	// V2 records have none — they predate the checksum field.
	if version == 3 {
		var crcBuf [4]byte
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
				return nil, nil
			}
			return nil, err
		}
		want := binary.BigEndian.Uint32(crcBuf[:])
		h := crc32.New(crcTable)
		h.Write(header[:])
		h.Write(body)
		if h.Sum32() != want {
			return nil, ErrCorruptRecord
		}
	}
	topicEnd := topicLen
	keyEnd := topicLen + keyLen
	rec := &Record{
		Timestamp: int64(binary.BigEndian.Uint64(header[2:10])),
		Partition: int32(binary.BigEndian.Uint32(header[10:14])),
		Topic:     string(body[:topicEnd]),
		Key:       string(body[topicEnd:keyEnd]),
		Payload:   body[keyEnd:], // body is not reused; safe to slice directly
	}
	return rec, nil
}

func readV1Record(r *bufio.Reader, opts ReplayOptions, prefix []byte) (*Record, error) {
	var headerRest [12]byte
	if _, err := io.ReadFull(r, headerRest[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
			return nil, nil
		}
		return nil, err
	}
	// Reconstruct full 14-byte header on the stack.
	var header [14]byte
	copy(header[:2], prefix)
	copy(header[2:], headerRest[:])
	topicLen := int(binary.BigEndian.Uint16(header[8:10]))
	payloadLen := int(binary.BigEndian.Uint32(header[10:14]))
	if topicLen < 0 || payloadLen < 0 || payloadLen > MaxPayloadSize {
		return nil, ErrCorruptRecord
	}
	body := make([]byte, topicLen+payloadLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
			return nil, nil
		}
		return nil, err
	}
	rec := &Record{
		Timestamp: int64(binary.BigEndian.Uint64(header[0:8])),
		Topic:     string(body[:topicLen]),
		Partition: -1,
		Payload:   body[topicLen:], // body is not reused; safe to slice directly
	}
	return rec, nil
}
