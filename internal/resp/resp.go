// Package resp implements enough of the Redis RESP2 wire protocol to serve
// real Redis clients (including redis-cli) and to encode the append-only
// file log using the same framing.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
)

// ErrProtocol is returned when malformed RESP framing is encountered.
var ErrProtocol = errors.New("resp: protocol error")

// maxBulkLen bounds bulk string / array sizes accepted from the wire so a
// malicious or buggy client can't force an unbounded allocation.
const maxBulkLen = 512 * 1024 * 1024 // 512MB, matches Redis's default proto-max-bulk-len

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", ErrProtocol
	}
	return line[:len(line)-2], nil
}

