// metricsampler polls a broker's Prometheus endpoint during a load test
// and reports server-side workflow rates: goqueue_wf_events_total deltas
// (events/sec, the authoritative throughput number) and the
// goqueue_wf_executions gauges (concurrent in-flight executions).
//
// Usage:
//
//	go run ./load/metricsampler [-url http://127.0.0.1:2112/metrics] [-interval 2s] [-out samples.csv]
//
// Stop with Ctrl+C; it prints peak sustained windows on exit.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

type sample struct {
	at         time.Time
	eventsTot  float64
	pending    float64
	running    float64
	retrying   float64
	completed  float64
	failed     float64
}

func scrape(url string) (sample, error) {
	resp, err := http.Get(url)
	if err != nil {
		return sample{}, err
	}
	defer resp.Body.Close()
	s := sample{at: time.Now()}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "goqueue_wf_events_total"):
			s.eventsTot += lastField(line)
		case strings.HasPrefix(line, "goqueue_wf_executions"):
			v := lastField(line)
			switch {
			case strings.Contains(line, `status="pending"`):
				s.pending = v
			case strings.Contains(line, `status="running"`):
				s.running = v
			case strings.Contains(line, `status="retrying"`):
				s.retrying = v
			case strings.Contains(line, `status="completed"`):
				s.completed = v
			case strings.Contains(line, `status="failed"`):
				s.failed = v
			}
		}
	}
	return s, sc.Err()
}

func lastField(line string) float64 {
	i := strings.LastIndexByte(line, ' ')
	if i < 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(line[i+1:], 64)
	return v
}

// peakWindow returns the highest average events/sec over any contiguous
// window of at least d.
func peakWindow(samples []sample, d time.Duration) float64 {
	best := 0.0
	for i := 0; i < len(samples); i++ {
		for j := i + 1; j < len(samples); j++ {
			span := samples[j].at.Sub(samples[i].at)
			if span < d {
				continue
			}
			rate := (samples[j].eventsTot - samples[i].eventsTot) / span.Seconds()
			if rate > best {
				best = rate
			}
			break // longer spans from i only dilute the peak; move i forward
		}
	}
	return best
}

func main() {
	url := flag.String("url", "http://127.0.0.1:2112/metrics", "broker metrics endpoint")
	interval := flag.Duration("interval", 2*time.Second, "poll interval")
	out := flag.String("out", "", "optional CSV output path")
	flag.Parse()

	var csv *os.File
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, "create csv:", err)
			os.Exit(1)
		}
		defer f.Close()
		csv = f
		fmt.Fprintln(csv, "unix_ms,events_total,events_per_sec,pending,running,retrying,completed,failed,inflight")
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	var samples []sample
	var peakInflight float64
	t := time.NewTicker(*interval)
	defer t.Stop()

	fmt.Printf("%-9s %12s %10s %9s %9s %9s %10s\n", "t", "events_tot", "ev/s", "pending", "running", "inflight", "completed")
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-t.C:
			s, err := scrape(*url)
			if err != nil {
				fmt.Fprintln(os.Stderr, "scrape:", err)
				continue
			}
			rate := 0.0
			if n := len(samples); n > 0 {
				prev := samples[n-1]
				rate = (s.eventsTot - prev.eventsTot) / s.at.Sub(prev.at).Seconds()
			}
			inflight := s.pending + s.running + s.retrying
			if inflight > peakInflight {
				peakInflight = inflight
			}
			samples = append(samples, s)
			elapsed := s.at.Sub(samples[0].at).Round(time.Second)
			fmt.Printf("%-9s %12.0f %10.0f %9.0f %9.0f %9.0f %10.0f\n",
				elapsed, s.eventsTot, rate, s.pending, s.running, inflight, s.completed)
			if csv != nil {
				fmt.Fprintf(csv, "%d,%.0f,%.1f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f\n",
					s.at.UnixMilli(), s.eventsTot, rate, s.pending, s.running, s.retrying, s.completed, s.failed, inflight)
			}
		}
	}

	if len(samples) >= 2 {
		first, last := samples[0], samples[len(samples)-1]
		total := last.eventsTot - first.eventsTot
		span := last.at.Sub(first.at).Seconds()
		fmt.Printf("\n--- summary over %.0fs ---\n", span)
		fmt.Printf("events appended:       %.0f (avg %.0f/s)\n", total, total/span)
		fmt.Printf("peak 10s window:       %.0f events/s\n", peakWindow(samples, 10*time.Second))
		fmt.Printf("peak 30s window:       %.0f events/s\n", peakWindow(samples, 30*time.Second))
		fmt.Printf("peak in-flight:        %.0f executions (pending+running+retrying)\n", peakInflight)
		fmt.Printf("completed total:       %.0f\n", last.completed)
		fmt.Printf("failed total:          %.0f\n", last.failed)
	}
}
