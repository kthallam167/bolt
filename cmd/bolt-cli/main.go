// Command bolt-cli is a minimal interactive client for a bolt (or any
// RESP2-compatible, e.g. Redis) server, so the project can be explored
// without needing redis-cli installed. It also accepts a single command
// from argv for scripting, e.g. `bolt-cli GET foo`.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/kthallam167/bolt/internal/resp"
)

func main() {
	addr := flag.String("addr", "localhost:6380", "server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "could not connect:", err)
		os.Exit(1)
	}
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	if args := flag.Args(); len(args) > 0 {
		if err := runOne(r, w, args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("bolt-cli connected to %s. Ctrl-D to exit.\n", *addr)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("%s> ", *addr)
		if !scanner.Scan() {
			fmt.Println()
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		args := strings.Fields(line)
		if err := runOne(r, w, args); err != nil {
			fmt.Fprintln(os.Stderr, "(error)", err)
		}
	}
}
}
