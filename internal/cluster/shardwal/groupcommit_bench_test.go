package shardwal

// Direct measurement of the shard-log group-commit win. At concurrency 1 there
// is nothing to coalesce, so each append pays its own fsync (the old behavior).
// As concurrency rises, concurrent appends share one fsync per generation and
// throughput climbs while per-append p99 stays bounded.
//
// Run:
//   GOQUEUE_BENCH=1 go test ./internal/cluster/shardwal -run TestShardGroupCommitReport -count=1 -v

import (
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestShardGroupCommitReport(t *testing.T) {
	if os.Getenv("GOQUEUE_BENCH") == "" {
		t.Skip("set GOQUEUE_BENCH=1 to run shard group-commit report")
	}

	concurrency := []int{1, 8, 32, 64}
	const perWriter = 1000
	payload := make([]byte, 256)

	t.Logf("shardwal append throughput (real disk, 256B, %d appends/writer):", perWriter)
	t.Logf("  (writers=1 = no batching possible = old fsync-per-append baseline)")
	var base float64
	for _, c := range concurrency {
		sh, err := Open(t.TempDir(), 1)
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		var wg sync.WaitGroup
		latCh := make(chan []time.Duration, c)
		start := time.Now()
		for w := 0; w < c; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lats := make([]time.Duration, 0, perWriter)
				for i := 0; i < perWriter; i++ {
					s := time.Now()
					if _, err := sh.Append(payload); err != nil {
						t.Error(err)
						return
					}
					lats = append(lats, time.Since(s))
				}
				latCh <- lats
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		close(latCh)
		_ = sh.Close()

		all := make([]time.Duration, 0, c*perWriter)
		for l := range latCh {
			all = append(all, l...)
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		rate := float64(c*perWriter) / elapsed.Seconds()
		if c == 1 {
			base = rate
		}
		p99 := time.Duration(0)
		if len(all) > 0 {
			p99 = all[int(0.99*float64(len(all)-1))]
		}
		speedup := 1.0
		if base > 0 {
			speedup = rate / base
		}
		t.Logf("  writers=%3d -> %10.0f appends/sec  p99=%-9s  %.1fx vs baseline",
			c, rate, p99.Round(time.Microsecond), speedup)
	}
}
