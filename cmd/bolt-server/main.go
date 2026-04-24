// Command bolt-server runs the bolt TCP key-value server.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kthallam167/bolt/internal/aof"
	"github.com/kthallam167/bolt/internal/server"
	"github.com/kthallam167/bolt/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "TCP listen address")
	aofPath := flag.String("aof", "bolt.aof", "path to the append-only file (empty disables persistence)")
	fsyncPolicy := flag.String("aof-fsync", "everysec", "AOF fsync policy: always|everysec|no")
	shards := flag.Int("shards", 32, "number of store shards (rounded up to a power of two)")
	rewritePercent := flag.Int("aof-rewrite-percentage", 100, "auto-trigger AOF rewrite once the file grows this %% beyond its size after the last rewrite (0 disables)")
	rewriteMinSize := flag.Int64("aof-rewrite-min-size", 1<<20, "minimum AOF size in bytes before auto rewrite can trigger")
	activeExpiryInterval := flag.Duration("active-expiry-interval", 100*time.Millisecond, "interval between background TTL sweeps")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags)

	policy, err := aof.ParseFsyncPolicy(*fsyncPolicy)
	if err != nil {
		logger.Fatal(err)
	}

	st := store.New(*shards)
	srv := server.New(server.Config{
		Addr:                 *addr,
		Store:                st,
		Logger:               logger,
		AOFRewritePercentage: *rewritePercent,
		AOFRewriteMinSize:    *rewriteMinSize,
	})

	if *aofPath != "" {
		logger.Printf("loading AOF from %s", *aofPath)
		start := time.Now()
		n, err := aof.Replay(*aofPath, srv.ApplyReplay)
		if err != nil {
			logger.Fatalf("aof replay failed: %v", err)
		}
		logger.Printf("replayed %d commands (%d keys) in %s", n, st.Len(), time.Since(start))

		a, err := aof.Open(*aofPath, policy)
		if err != nil {
			logger.Fatalf("aof open failed: %v", err)
		}
		defer a.Close()
		size, _ := a.Size()
		srv.SetAOF(a, size)
	}

	stop := make(chan struct{})
	go st.RunActiveExpiry(*activeExpiryInterval, stop)
	defer close(stop)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Printf("received %s, shutting down", sig)
		srv.Shutdown()
	}()

	if err := srv.ListenAndServe(); err != nil {
		logger.Fatal(err)
	}
	logger.Println("bolt stopped")
}
