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

// ReadCommand reads one client request: either a RESP array of bulk strings
// (the format every real client, including redis-cli, sends) or a single
// inline line (space-separated, for plain `nc`/`telnet` debugging). It
// returns a nil/empty slice for a blank line, which callers should skip.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if line == "" {
		return nil, nil
	}
	if line[0] != '*' {
		return strings.Fields(line), nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 || n > 1<<20 {
		return nil, ErrProtocol
	}
	args := make([]string, n)
	for i := 0; i < n; i++ {
		typeLine, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(typeLine) == 0 || typeLine[0] != '$' {
			return nil, ErrProtocol
		}
		length, err := strconv.Atoi(typeLine[1:])
		if err != nil || length < 0 || length > maxBulkLen {
			return nil, ErrProtocol
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:length])
	}
	return args, nil
}

// EncodeCommand encodes args as a RESP array of bulk strings. It is used
// both to frame outgoing client requests (bolt-cli, bolt-bench) and to
// serialize commands into the append-only file.
func EncodeCommand(args []string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('*')
	buf.WriteString(strconv.Itoa(len(args)))
	buf.WriteString("\r\n")
	for _, a := range args {
		buf.WriteByte('$')
		buf.WriteString(strconv.Itoa(len(a)))
		buf.WriteString("\r\n")
		buf.WriteString(a)
		buf.WriteString("\r\n")
	}
	return buf.Bytes()
}

