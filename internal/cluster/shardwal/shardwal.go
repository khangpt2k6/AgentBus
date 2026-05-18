// Package shardwal provides per-shard append-only logs used by the cluster
// data plane. Each shard's records live in <dir>/shard-N.wal. Records are
// length-prefixed payloads with a CRC32C trailer.
//
// The package exposes:
//   - Append: durable write, returns the assigned offset
//   - Replay: read all records from a starting offset
//   - Subscribe: backfill from a starting offset, then receive live appends
//
// Subscribe wakes consumers via a per-shard sync.Cond signaled by Append.
package shardwal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// MaxPayloadSize bounds per-record payload to defend replay against
// tampered or otherwise malformed records.
const MaxPayloadSize = 64 << 20 // 64 MiB

// Record is one shardwal entry.
type Record struct {
	Offset  uint64
	Payload []byte
}

// Shard is a single per-shard append-only log handle.
type Shard struct {
	id     uint32
	path   string

	mu       sync.Mutex
	f        *os.File
	w        *bufio.Writer
	tail     uint64
	cond     *sync.Cond
	closed   bool
}

// Open returns a *Shard rooted at <dir>/shard-<id>.wal. Creates the file
// if missing; replays the existing file to recompute the tail offset.
func Open(dir string, id uint32) (*Shard, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir shardwal: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("shard-%d.wal", id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open shardwal: %w", err)
	}
	s := &Shard{
		id:   id,
		path: path,
		f:    f,
		w:    bufio.NewWriterSize(f, 1<<16),
	}
	s.cond = sync.NewCond(&s.mu)
	if err := s.recoverTail(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("recover tail: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek end: %w", err)
	}
	return s, nil
}

// recoverTail walks the file and counts valid records. Called once on open.
func (s *Shard) recoverTail() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(s.f, 1<<16)
	var count uint64
	for {
		var hdr [8]byte
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil // partial tail
		}
		payloadLen := binary.BigEndian.Uint32(hdr[0:4])
		if payloadLen > MaxPayloadSize {
			return fmt.Errorf("recover: record too large (%d > %d)", payloadLen, MaxPayloadSize)
		}
		body := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil // partial tail
		}
		want := binary.BigEndian.Uint32(hdr[4:8])
		if crc32.Checksum(body, crcTable) != want {
			return fmt.Errorf("recover: CRC mismatch at offset %d", count)
		}
		count++
	}
	s.tail = count
	return nil
}

// Append writes payload as the next record and returns its offset.
func (s *Shard) Append(payload []byte) (uint64, error) {
	if len(payload) > MaxPayloadSize {
		return 0, fmt.Errorf("payload too large: %d", len(payload))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("shardwal: closed")
	}

	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	if _, err := s.w.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := s.w.Write(payload); err != nil {
		return 0, err
	}
	if err := s.w.Flush(); err != nil {
		return 0, err
	}
	if err := s.f.Sync(); err != nil {
		return 0, err
	}
	off := s.tail
	s.tail++
	s.cond.Broadcast()
	return off, nil
}

// Tail returns the next offset to be written.
func (s *Shard) Tail() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tail
}

// Replay calls fn for every record with offset >= fromOffset.
func (s *Shard) Replay(fromOffset uint64, fn func(offset uint64, payload []byte) error) error {
	rf, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer rf.Close()
	r := bufio.NewReaderSize(rf, 1<<16)
	var offset uint64
	for {
		var hdr [8]byte
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return nil
		}
		payloadLen := binary.BigEndian.Uint32(hdr[0:4])
		if payloadLen > MaxPayloadSize {
			return fmt.Errorf("replay: record too large at offset %d", offset)
		}
		body := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil
		}
		want := binary.BigEndian.Uint32(hdr[4:8])
		if crc32.Checksum(body, crcTable) != want {
			return fmt.Errorf("replay: CRC mismatch at offset %d", offset)
		}
		if offset >= fromOffset {
			if err := fn(offset, body); err != nil {
				return err
			}
		}
		offset++
	}
}

// Subscribe streams records starting at fromOffset, blocking the goroutine
// inside Subscribe until ctx is done. Returns a buffered channel and a
// cancel function that shuts down the streaming goroutine.
func (s *Shard) Subscribe(ctx context.Context, fromOffset uint64) (<-chan Record, func()) {
	ch := make(chan Record, 128)
	subCtx, subCancel := context.WithCancel(ctx)

	go func() {
		defer close(ch)
		next := fromOffset

		// Backfill from disk first.
		if err := s.Replay(fromOffset, func(off uint64, payload []byte) error {
			select {
			case <-subCtx.Done():
				return io.EOF
			case ch <- Record{Offset: off, Payload: append([]byte(nil), payload...)}:
				next = off + 1
				return nil
			}
		}); err != nil && err != io.EOF {
			return
		}
		if subCtx.Err() != nil {
			return
		}

		// Live tail loop.
		for {
			s.mu.Lock()
			for !s.closed && s.tail <= next {
				if subCtx.Err() != nil {
					s.mu.Unlock()
					return
				}
				s.cond.Wait()
			}
			if s.closed {
				s.mu.Unlock()
				return
			}
			currentTail := s.tail
			s.mu.Unlock()

			if err := s.Replay(next, func(off uint64, payload []byte) error {
				if off >= currentTail {
					return io.EOF
				}
				select {
				case <-subCtx.Done():
					return io.EOF
				case ch <- Record{Offset: off, Payload: append([]byte(nil), payload...)}:
					next = off + 1
					return nil
				}
			}); err != nil && err != io.EOF {
				return
			}
		}
	}()

	return ch, subCancel
}

// Close flushes and closes the underlying file. Any in-flight Subscribe
// goroutines exit cleanly.
func (s *Shard) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cond.Broadcast()
	err := s.w.Flush()
	if err == nil {
		err = s.f.Close()
	}
	s.mu.Unlock()
	return err
}
