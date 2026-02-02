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

