// Command bolt-bench is a small load generator for a bolt (or Redis)
// server, modeled loosely on redis-benchmark: N concurrent connections each
// fire a share of total requests, optionally batched into pipelines, and
// aggregate throughput/latency are reported at the end.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kthallam167/bolt/internal/resp"
)

func main() {
	addr := flag.String("addr", "localhost:6380", "server address")
	clients := flag.Int("c", 50, "number of concurrent connections")
	requests := flag.Int("n", 100000, "total number of requests across all connections")
	valueSize := flag.Int("d", 64, "value size in bytes for SET")
	pipeline := flag.Int("pipeline", 1, "number of commands to pipeline per round trip (1 = no pipelining)")
	mode := flag.String("mode", "mixed", "workload: set|get|mixed")
	flag.Parse()

	if *clients <= 0 || *requests <= 0 {
		fmt.Fprintln(os.Stderr, "-c and -n must be positive")
		os.Exit(1)
	}

	perWorker := *requests / *clients
	total := perWorker * *clients

	var wg sync.WaitGroup
	results := make(chan workerResult, *clients)

	fmt.Printf("bolt-bench: %d clients, %d requests total (%d/client), mode=%s, pipeline=%d, value-size=%dB\n",
		*clients, total, perWorker, *mode, *pipeline, *valueSize)

	start := time.Now()
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			res, err := runWorker(id, *addr, perWorker, *valueSize, *pipeline, *mode)
			if err != nil {
				fmt.Fprintf(os.Stderr, "worker %d: %v\n", id, err)
				return
			}
			results <- res
		}(i)
	}
	wg.Wait()
	close(results)
	elapsed := time.Since(start)

	var completed int
	var latencies []time.Duration
	for r := range results {
		completed += r.count
		latencies = append(latencies, r.latencies...)
	}

	opsPerSec := float64(completed) / elapsed.Seconds()
	fmt.Printf("\ncompleted %d ops in %s -> %.0f ops/sec\n", completed, elapsed, opsPerSec)

}
type workerResult struct {
	count     int
	latencies []time.Duration // only populated when pipeline==1
}

func runWorker(id int, addr string, opsPerWorker, valueSize, pipeline int, mode string) (workerResult, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return workerResult{}, err
	}
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	r := bufio.NewReaderSize(conn, 64*1024)
	w := bufio.NewWriterSize(conn, 64*1024)
	value := make([]byte, valueSize)
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	for i := range value {
		value[i] = byte('a' + rng.Intn(26))
	}
	valStr := string(value)

	batch := pipeline
	if batch < 1 {
		batch = 1
	}

	res := workerResult{}
	done := 0
	for done < opsPerWorker {
		n := batch
		if done+n > opsPerWorker {
			n = opsPerWorker - done
		}

		start := time.Now()
		for i := 0; i < n; i++ {
			key := "bench:" + strconv.Itoa(id) + ":" + strconv.Itoa(done+i)
			var args []string
			switch pickOp(mode, done+i) {
			case "set":
				args = []string{"SET", key, valStr}
			default:
				args = []string{"GET", key}
			}
			if _, err := w.Write(resp.EncodeCommand(args)); err != nil {
				return res, err
			}
		}
		if err := w.Flush(); err != nil {
			return res, err
		}
		for i := 0; i < n; i++ {
			if _, err := resp.ReadReply(r); err != nil {
				return res, err
			}
		}
		if batch == 1 {
			res.latencies = append(res.latencies, time.Since(start))
		}
		res.count += n
		done += n
	}
	return res, nil
}

func pickOp(mode string, i int) string {
	switch mode {
	case "set":
		return "set"
	case "get":
		return "get"
	default:
		if i%2 == 0 {
			return "set"
		}
		return "get"
	}
}
