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
//
// Appends are durable via group-commit fsync: concurrent writers are coalesced
// into one fsync per "generation". While one goroutine runs the fsync syscall
// (mutex released), other writers fill the next generation's buffer. This turns
// N sequential fsyncs into roughly ceil(N / batch), the same technique the main
// WAL uses. Append returns only after the generation containing its record has
// been fsynced, so quorum/ack semantics still rest on durable data.
type Shard struct {
	id   uint32
	path string

	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	tail   uint64 // records appended (buffered); the next offset to assign
	synced uint64 // records durably fsynced; what Subscribe may expose
	cond   *sync.Cond
	closed bool

	// Group-commit fsync state (guarded by mu).
	syncCond   *sync.Cond
	pendingGen uint64
	syncedGen  uint64
	syncing    bool
	syncErr    error
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
	s.syncCond = sync.NewCond(&s.mu)
	s.pendingGen = 1
	if err := s.recoverTail(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("recover tail: %w", err)
	}
	// Everything recovered from disk is already durable.
	s.synced = s.tail
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

// Append writes payload as the next record and returns its offset. It blocks
// until the record is durably fsynced, but coalesces its fsync with any other
// concurrent appends via group commit.
func (s *Shard) Append(payload []byte) (uint64, error) {
	if len(payload) > MaxPayloadSize {
		return 0, fmt.Errorf("payload too large: %d", len(payload))
	}

	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("shardwal: closed")
	}
	if _, err := s.w.Write(hdr[:]); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if _, err := s.w.Write(payload); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	off := s.tail
	s.tail++
	myGen := s.pendingGen
	// groupCommitLocked returns with s.mu released.
	if err := s.groupCommitLocked(myGen); err != nil {
		return 0, err
	}
	return off, nil
}

// groupCommitLocked is called with s.mu held and returns with it released.
// Concurrent callers are coalesced: one performs Flush+Sync per generation, the
// rest piggyback on that completed round. On success it advances s.synced (the
// durable record count) and wakes Subscribe consumers.
func (s *Shard) groupCommitLocked(myGen uint64) error {
	for {
		if s.syncedGen >= myGen {
			err := s.syncErr
			s.mu.Unlock()
			return err
		}
		if s.syncing {
			s.syncCond.Wait()
			continue
		}

		// Become the syncer for the current pending generation. Flush bufio
		// under the lock (it is not concurrent-safe), then release the lock for
		// the slow fsync so later appends fill the next generation's buffer.
		s.syncing = true
		syncingGen := s.pendingGen
		flushedTail := s.tail
		if err := s.w.Flush(); err != nil {
			s.syncErr = err
			s.syncing = false
			s.syncCond.Broadcast()
			s.mu.Unlock()
			return err
		}
		s.pendingGen++
		s.mu.Unlock()

		syncErr := s.f.Sync()

		s.mu.Lock()
		if syncErr != nil {
			s.syncErr = syncErr
			s.syncing = false
			s.syncCond.Broadcast()
			s.mu.Unlock()
			return syncErr
		}
		s.syncedGen = syncingGen
		s.synced = flushedTail
		s.syncErr = nil
		s.syncing = false
		s.syncCond.Broadcast() // wake other committers
		s.cond.Broadcast()     // wake Subscribe consumers: data is now durable
	}
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

		// Live tail loop. Only durably-synced records are exposed (s.synced),
		// so a consumer never reads data that a failed fsync could lose.
		for {
			s.mu.Lock()
			for !s.closed && s.synced <= next {
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
			currentTail := s.synced
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
	// Wait for any in-flight group-commit fsync to finish so we don't close the
	// file out from under it.
	for s.syncing {
		s.syncCond.Wait()
	}
	s.closed = true
	s.cond.Broadcast()
	s.syncCond.Broadcast()
	err := s.w.Flush()
	if err == nil {
		err = s.f.Sync()
	}
	if err == nil {
		err = s.f.Close()
	}
	s.mu.Unlock()
	return err
}
