package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New(4)
	s.Set("foo", []byte("bar"), 0)
	v, ok := s.Get("foo")
	if !ok || string(v) != "bar" {
		t.Fatalf("got %q, %v", v, ok)
	}

	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected missing key to be absent")
	}
}

func TestExpiry(t *testing.T) {
	s := New(4)
	s.Set("foo", []byte("bar"), 10*time.Millisecond)

	if _, ok := s.Get("foo"); !ok {
		t.Fatal("expected key to exist before TTL elapses")
	}

	time.Sleep(30 * time.Millisecond)

	if _, ok := s.Get("foo"); ok {
		t.Fatal("expected key to be expired")
	}
}

