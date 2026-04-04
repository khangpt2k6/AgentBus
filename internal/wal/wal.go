package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

var ErrCorruptRecord = errors.New("wal: corrupt record")
var ErrInvalidSyncMode = errors.New("wal: invalid sync mode")

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

// Log is a very small append-only WAL implementation.
type Log struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer

	syncMode     SyncMode
	syncInterval time.Duration
	lastSync     time.Time
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
	return &Log{
		f:            f,
		w:            bufio.NewWriterSize(f, 1<<20),
		syncMode:     mode,
		syncInterval: opts.SyncInterval,
		lastSync:     time.Now(),
	}, nil
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
	l.mu.Lock()
	defer l.mu.Unlock()

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

	header := make([]byte, 24)
	copy(header[0:2], []byte{'G', 'W'})
	header[2] = 2
	header[3] = 0
	binary.BigEndian.PutUint64(header[4:12], uint64(rec.Timestamp))
	binary.BigEndian.PutUint32(header[12:16], uint32(rec.Partition))
	binary.BigEndian.PutUint16(header[16:18], uint16(len(topicBytes)))
	binary.BigEndian.PutUint16(header[18:20], uint16(len(keyBytes)))
	binary.BigEndian.PutUint32(header[20:24], uint32(len(rec.Payload)))

	if _, err := l.w.Write(header); err != nil {
		return err
	}
	if _, err := l.w.Write(topicBytes); err != nil {
		return err
	}
	if _, err := l.w.Write(keyBytes); err != nil {
		return err
	}
	if _, err := l.w.Write(rec.Payload); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.maybeSyncLocked()
}

func (l *Log) maybeSyncLocked() error {
	switch l.syncMode {
	case SyncAlways:
		if err := l.f.Sync(); err != nil {
			return err
		}
		l.lastSync = time.Now()
	case SyncInterval:
		if time.Since(l.lastSync) >= l.syncInterval {
			if err := l.f.Sync(); err != nil {
				return err
			}
			l.lastSync = time.Now()
		}
	}
	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w != nil {
		if err := l.w.Flush(); err != nil {
			return err
		}
	}
	if l.f != nil {
		if l.syncMode != SyncNone {
			if err := l.f.Sync(); err != nil {
				return err
			}
		}
		return l.f.Close()
	}
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
	header := make([]byte, 22)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
			return nil, nil
		}
		return nil, err
	}
	if header[0] != 2 {
		return nil, ErrCorruptRecord
	}
	topicLen := int(binary.BigEndian.Uint16(header[14:16]))
	keyLen := int(binary.BigEndian.Uint16(header[16:18]))
	payloadLen := int(binary.BigEndian.Uint32(header[18:22]))
	if topicLen < 0 || keyLen < 0 || payloadLen < 0 {
		return nil, ErrCorruptRecord
	}
	body := make([]byte, topicLen+keyLen+payloadLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
			return nil, nil
		}
		return nil, err
	}
	topicEnd := topicLen
	keyEnd := topicLen + keyLen
	rec := &Record{
		Timestamp: int64(binary.BigEndian.Uint64(header[2:10])),
		Partition: int32(binary.BigEndian.Uint32(header[10:14])),
		Topic:     string(body[:topicEnd]),
		Key:       string(body[topicEnd:keyEnd]),
		Payload:   append([]byte(nil), body[keyEnd:]...),
	}
	return rec, nil
}

func readV1Record(r *bufio.Reader, opts ReplayOptions, prefix []byte) (*Record, error) {
	headerRest := make([]byte, 12)
	if _, err := io.ReadFull(r, headerRest); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && opts.AllowPartialTail {
			return nil, nil
		}
		return nil, err
	}
	header := make([]byte, 14)
	copy(header[:2], prefix)
	copy(header[2:], headerRest)
	topicLen := int(binary.BigEndian.Uint16(header[8:10]))
	payloadLen := int(binary.BigEndian.Uint32(header[10:14]))
	if topicLen < 0 || payloadLen < 0 {
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
		Payload:   append([]byte(nil), body[topicLen:]...),
	}
	return rec, nil
}
