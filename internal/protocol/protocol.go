// Package protocol defines the binary wire format for GoQueue.
//
// Frame layout (all big-endian):
//
//	┌──────────┬──────────┬──────────────┬─────────────┬─────────┐
//	│ Magic(2) │  Op(1)   │ TopicLen(1)  │ PayloadLen(4)│ Data... │
//	└──────────┴──────────┴──────────────┴─────────────┴─────────┘
//
// Magic: 0x474B ("GK")
// Op:    PUBLISH=0x01, SUBSCRIBE=0x02, ACK=0x03, FETCH=0x04, ERROR=0x05
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	Magic uint16 = 0x474B // "GK"

	OpPublish      byte = 0x01
	OpSubscribe    byte = 0x02
	OpAck          byte = 0x03
	OpFetch        byte = 0x04
	OpError        byte = 0x05
	OpMessage      byte = 0x06 // server → client delivery
	OpBatchPublish byte = 0x07
	OpBatchAck     byte = 0x08
)

var (
	ErrBadMagic   = errors.New("protocol: invalid magic bytes")
	ErrTopicEmpty = errors.New("protocol: topic is empty")
	ErrBadPayload = errors.New("protocol: malformed payload")
)

// Frame is a decoded protocol message.
type Frame struct {
	Op      byte
	Topic   string
	Payload []byte
}

// Encode writes a Frame to w.
func Encode(w io.Writer, f *Frame) error {
	topic := []byte(f.Topic)
	// magic(2) + op(1) + topicLen(1) + payloadLen(4) + topic + payload
	buf := make([]byte, 8+len(topic)+len(f.Payload))
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = f.Op
	buf[3] = byte(len(topic))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(f.Payload)))
	copy(buf[8:], topic)
	copy(buf[8+len(topic):], f.Payload)
	_, err := w.Write(buf)
	return err
}

// Decode reads one Frame from r.
func Decode(r io.Reader) (*Frame, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	magic := binary.BigEndian.Uint16(header[0:2])
	if magic != Magic {
		return nil, ErrBadMagic
	}

	op := header[2]
	topicLen := int(header[3])
	payloadLen := int(binary.BigEndian.Uint32(header[4:8]))

	body := make([]byte, topicLen+payloadLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	return &Frame{
		Op:      op,
		Topic:   string(body[:topicLen]),
		Payload: body[topicLen:],
	}, nil
}

// BatchMessage is used by fetch responses.
type BatchMessage struct {
	Offset  int64
	Payload []byte
}

// EncodeBatchPayload encodes N payloads as:
// count(2) + repeated(payloadLen(4) + payload).
func EncodeBatchPayload(payloads [][]byte) []byte {
	size := 2
	for _, p := range payloads {
		size += 4 + len(p)
	}
	out := make([]byte, size)
	binary.BigEndian.PutUint16(out[0:2], uint16(len(payloads)))
	pos := 2
	for _, p := range payloads {
		binary.BigEndian.PutUint32(out[pos:pos+4], uint32(len(p)))
		pos += 4
		copy(out[pos:pos+len(p)], p)
		pos += len(p)
	}
	return out
}

func DecodeBatchPayload(payload []byte) ([][]byte, error) {
	if len(payload) < 2 {
		return nil, ErrBadPayload
	}
	n := int(binary.BigEndian.Uint16(payload[0:2]))
	pos := 2
	out := make([][]byte, 0, n)
	for range n {
		if pos+4 > len(payload) {
			return nil, ErrBadPayload
		}
		l := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if l < 0 || pos+l > len(payload) {
			return nil, ErrBadPayload
		}
		out = append(out, append([]byte(nil), payload[pos:pos+l]...))
		pos += l
	}
	if pos != len(payload) {
		return nil, ErrBadPayload
	}
	return out, nil
}

// EncodeBatchAck encodes first/last offsets and message count.
func EncodeBatchAck(firstOffset, lastOffset int64, count int) []byte {
	out := make([]byte, 18)
	binary.BigEndian.PutUint64(out[0:8], uint64(firstOffset))
	binary.BigEndian.PutUint64(out[8:16], uint64(lastOffset))
	binary.BigEndian.PutUint16(out[16:18], uint16(count))
	return out
}

func DecodeBatchAck(payload []byte) (firstOffset, lastOffset int64, count int, err error) {
	if len(payload) != 18 {
		return 0, 0, 0, ErrBadPayload
	}
	firstOffset = int64(binary.BigEndian.Uint64(payload[0:8]))
	lastOffset = int64(binary.BigEndian.Uint64(payload[8:16]))
	count = int(binary.BigEndian.Uint16(payload[16:18]))
	return firstOffset, lastOffset, count, nil
}

// EncodeFetchRequest encodes offset and maxCount.
func EncodeFetchRequest(offset int64, maxCount int) []byte {
	out := make([]byte, 12)
	binary.BigEndian.PutUint64(out[0:8], uint64(offset))
	binary.BigEndian.PutUint32(out[8:12], uint32(maxCount))
	return out
}

func DecodeFetchRequest(payload []byte) (offset int64, maxCount int, err error) {
	if len(payload) != 12 {
		return 0, 0, ErrBadPayload
	}
	offset = int64(binary.BigEndian.Uint64(payload[0:8]))
	maxCount = int(binary.BigEndian.Uint32(payload[8:12]))
	return offset, maxCount, nil
}

// EncodeFetchResponse encodes messages as:
// count(2) + repeated(offset(8)+payloadLen(4)+payload).
func EncodeFetchResponse(messages []BatchMessage) []byte {
	size := 2
	for _, m := range messages {
		size += 12 + len(m.Payload)
	}
	out := make([]byte, size)
	binary.BigEndian.PutUint16(out[0:2], uint16(len(messages)))
	pos := 2
	for _, m := range messages {
		binary.BigEndian.PutUint64(out[pos:pos+8], uint64(m.Offset))
		pos += 8
		binary.BigEndian.PutUint32(out[pos:pos+4], uint32(len(m.Payload)))
		pos += 4
		copy(out[pos:pos+len(m.Payload)], m.Payload)
		pos += len(m.Payload)
	}
	return out
}

func DecodeFetchResponse(payload []byte) ([]BatchMessage, error) {
	if len(payload) < 2 {
		return nil, ErrBadPayload
	}
	n := int(binary.BigEndian.Uint16(payload[0:2]))
	pos := 2
	out := make([]BatchMessage, 0, n)
	for range n {
		if pos+12 > len(payload) {
			return nil, ErrBadPayload
		}
		offset := int64(binary.BigEndian.Uint64(payload[pos : pos+8]))
		pos += 8
		l := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if l < 0 || pos+l > len(payload) {
			return nil, ErrBadPayload
		}
		out = append(out, BatchMessage{
			Offset:  offset,
			Payload: append([]byte(nil), payload[pos:pos+l]...),
		})
		pos += l
	}
	if pos != len(payload) {
		return nil, ErrBadPayload
	}
	return out, nil
}
