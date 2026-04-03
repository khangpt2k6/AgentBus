package protocol

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	cases := []Frame{
		{Op: OpPublish, Topic: "orders", Payload: []byte("buy AAPL 100")},
		{Op: OpSubscribe, Topic: "events", Payload: []byte("payment-service")},
		{Op: OpAck, Topic: "orders", Payload: make([]byte, 8)},
		{Op: OpMessage, Topic: "x", Payload: []byte("tiny")},
		{Op: OpError, Topic: "err", Payload: []byte("something broke")},
	}

	for _, want := range cases {
		var buf bytes.Buffer
		if err := Encode(&buf, &want); err != nil {
			t.Fatalf("encode: %v", err)
		}
		got, err := Decode(&buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Op != want.Op {
			t.Errorf("op: got %02x, want %02x", got.Op, want.Op)
		}
		if got.Topic != want.Topic {
			t.Errorf("topic: got %q, want %q", got.Topic, want.Topic)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Errorf("payload: got %q, want %q", got.Payload, want.Payload)
		}
	}
}

func TestDecodeBadMagic(t *testing.T) {
	buf := bytes.NewReader([]byte{0xFF, 0xFF, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 'x', 'y'})
	_, err := Decode(buf)
	if err != ErrBadMagic {
		t.Errorf("expected ErrBadMagic, got %v", err)
	}
}

func TestEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	want := Frame{Op: OpFetch, Topic: "logs", Payload: nil}
	if err := Encode(&buf, &want); err != nil {
		t.Fatal(err)
	}
	got, err := Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Payload) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(got.Payload))
	}
}

func TestBatchPayloadRoundTrip(t *testing.T) {
	in := [][]byte{
		[]byte("a"),
		[]byte("hello"),
		[]byte("world"),
	}
	enc := EncodeBatchPayload(in)
	out, err := DecodeBatchPayload(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("batch len: got %d want %d", len(out), len(in))
	}
	for i := range in {
		if !bytes.Equal(in[i], out[i]) {
			t.Fatalf("batch[%d]: got %q want %q", i, out[i], in[i])
		}
	}
}

func TestFetchResponseRoundTrip(t *testing.T) {
	in := []BatchMessage{
		{Offset: 10, Payload: []byte("foo")},
		{Offset: 11, Payload: []byte("bar")},
	}
	enc := EncodeFetchResponse(in)
	out, err := DecodeFetchResponse(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("len: got %d want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].Offset != in[i].Offset || !bytes.Equal(out[i].Payload, in[i].Payload) {
			t.Fatalf("msg[%d] mismatch", i)
		}
	}
}

func BenchmarkEncodeDecode(b *testing.B) {
	frame := Frame{Op: OpPublish, Topic: "bench-topic", Payload: make([]byte, 256)}
	var buf bytes.Buffer
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		_ = Encode(&buf, &frame)
		_, _ = Decode(&buf)
	}
}
