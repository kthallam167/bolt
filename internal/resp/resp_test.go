package resp

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestEncodeDecodeCommandRoundTrip(t *testing.T) {
	cases := [][]string{
		{"PING"},
		{"SET", "foo", "bar"},
		{"SET", "foo", "bar", "EX", "10"},
		{"GET", "foo"},
		{"DEL", "a", "b", "c"},
		{"SET", "empty", ""},
	}
	for _, args := range cases {
		encoded := EncodeCommand(args)
		got, err := ReadCommand(bufio.NewReader(bytes.NewReader(encoded)))
		if err != nil {
			t.Fatalf("ReadCommand(%q): %v", args, err)
		}
		if !reflect.DeepEqual(got, args) {
			t.Errorf("round trip mismatch: got %v, want %v", got, args)
		}
	}
}

func TestReadCommandInline(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader([]byte("PING\r\n")))
	got, err := ReadCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"PING"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReadCommandEmptyLineSkipped(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader([]byte("\r\n")))
	got, err := ReadCommand(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty result for blank line, got %v", got)
	}
}

func TestReadCommandEOF(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader(nil))
	_, err := ReadCommand(r)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadCommandMalformed(t *testing.T) {
	// Array header claims 2 elements but only bulk-string headers are
	// missing their '$' prefix.
	r := bufio.NewReader(bytes.NewReader([]byte("*2\r\nnotbulk\r\nfoo\r\n")))
	_, err := ReadCommand(r)
	if err != ErrProtocol {
		t.Fatalf("expected ErrProtocol, got %v", err)
	}
}

func TestWriteAndReadReplies(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	if err := WriteSimpleString(w, "OK"); err != nil {
		t.Fatal(err)
	}
	if err := WriteError(w, "ERR boom"); err != nil {
		t.Fatal(err)
	}
	if err := WriteInteger(w, 42); err != nil {
		t.Fatal(err)
	}
	if err := WriteBulkString(w, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := WriteNilBulk(w); err != nil {
		t.Fatal(err)
	}
	if err := WriteArray(w, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	r := bufio.NewReader(&buf)

	v, err := ReadReply(r)
	if err != nil || v != "OK" {
		t.Fatalf("simple string: got %v, %v", v, err)
	}

	_, err = ReadReply(r)
	if err == nil || err.Error() != "ERR boom" {
		t.Fatalf("error reply: got %v", err)
	}

	v, err = ReadReply(r)
	if err != nil || v != int64(42) {
		t.Fatalf("integer: got %v, %v", v, err)
	}

	v, err = ReadReply(r)
	if err != nil || v != "hello" {
		t.Fatalf("bulk string: got %v, %v", v, err)
	}

	v, err = ReadReply(r)
	if err != nil || v != nil {
		t.Fatalf("nil bulk: got %v, %v", v, err)
	}

	v, err = ReadReply(r)
	if err != nil {
		t.Fatalf("array: %v", err)
	}
	arr, ok := v.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("array reply mismatch: %v", arr)
	}
}

func TestReadCommandOversizedArrayRejected(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader([]byte("*99999999\r\n")))
	_, err := ReadCommand(r)
	if err != ErrProtocol {
		t.Fatalf("expected ErrProtocol for oversized array, got %v", err)
	}
}
