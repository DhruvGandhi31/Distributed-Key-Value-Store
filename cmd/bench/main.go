// Command kvbench measures a running cluster: sequential write latency,
// concurrent write throughput and, when given the demo token, leader
// failover time. It talks to the client HTTP API directly (not through the
// SDK) so that redirects and retries don't hide in the timings.
//
// Every write it makes is a real committed write, and the failover phase
// really disconnects the leader, so point it at a scratch cluster rather
// than one people are using:
//
//	kvbench -nodes http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003 -token <demo token>
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var httpClient = &http.Client{
	Timeout:   2 * time.Second,
	Transport: &http.Transport{MaxIdleConnsPerHost: 128},
}

func main() {
	nodesFlag := flag.String("nodes", "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003", "comma-separated client API addresses of every node")
	writes := flag.Int("writes", 1000, "sequential writes for the latency test")
	duration := flag.Duration("duration", 10*time.Second, "how long each throughput run lasts")
	workersFlag := flag.String("workers", "8,32", "comma-separated concurrent client counts for the throughput test")
	token := flag.String("token", os.Getenv("KV_DEMO_TOKEN"), "demo token; enables the failover test (defaults to $KV_DEMO_TOKEN)")
	trials := flag.Int("trials", 10, "failover trials (needs -token)")
	flag.Parse()

	nodes := strings.Split(*nodesFlag, ",")
	leader, err := waitLeader(nodes, "", 10*time.Second)
	if err != nil {
		fail(err)
	}
	fmt.Printf("leader: %s\n\n", leader)

	if err := benchLatency(leader, *writes); err != nil {
		fail(err)
	}
	for _, w := range strings.Split(*workersFlag, ",") {
		var n int
		if _, err := fmt.Sscan(w, &n); err != nil || n < 1 {
			fail(fmt.Errorf("bad -workers value %q", w))
		}
		benchThroughput(leader, n, *duration)
	}
	if *token == "" {
		fmt.Println("\nfailover: skipped (pass -token or set KV_DEMO_TOKEN to enable)")
		return
	}
	if err := benchFailover(nodes, *token, *trials); err != nil {
		fail(err)
	}
}

func benchLatency(leader string, n int) error {
	lat := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		if err := put(leader, fmt.Sprintf("bench-seq-%d", i)); err != nil {
			return fmt.Errorf("sequential write %d: %w", i, err)
		}
		lat = append(lat, time.Since(start))
	}
	sortDurations(lat)
	fmt.Printf("sequential writes  n=%d  p50=%v  p95=%v  p99=%v\n",
		n, round(pct(lat, 0.50)), round(pct(lat, 0.95)), round(pct(lat, 0.99)))
	return nil
}

func benchThroughput(leader string, workers int, d time.Duration) {
	var ok, failed atomic.Int64
	var wg sync.WaitGroup
	stop := time.Now().Add(d)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				if put(leader, fmt.Sprintf("bench-c%d-%d", w, i)) == nil {
					ok.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("concurrent writes  workers=%d  %.0f writes/s  (ok=%d errors=%d)\n",
		workers, float64(ok.Load())/d.Seconds(), ok.Load(), failed.Load())
}

// benchFailover cuts the leader off via /demo/isolate and times how long the
// rest of the cluster takes to elect a new leader and to accept a write.
func benchFailover(nodes []string, token string, trials int) error {
	var elect, write []time.Duration
	for i := 0; i < trials; i++ {
		old, err := waitLeader(nodes, "", 10*time.Second)
		if err != nil {
			return err
		}
		if err := isolate(old, token); err != nil {
			return err
		}
		start := time.Now()
		next, err := waitLeader(nodes, old, 10*time.Second)
		if err != nil {
			return err
		}
		elect = append(elect, time.Since(start))
		for put(next, fmt.Sprintf("bench-failover-%d", i)) != nil {
			if time.Since(start) > 10*time.Second {
				return errors.New("no write accepted within 10s of failover")
			}
			time.Sleep(5 * time.Millisecond)
		}
		write = append(write, time.Since(start))

		// Let the old leader heal and step down before the next trial.
		for {
			s, err := status(old)
			if err == nil && s.IsolatedMS == 0 && s.State != "Leader" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
	}
	sortDurations(elect)
	sortDurations(write)
	fmt.Printf("\nfailover  trials=%d\n", trials)
	fmt.Printf("  new leader elected:   min=%v  median=%v  max=%v\n", round(elect[0]), round(pct(elect, 0.5)), round(elect[len(elect)-1]))
	fmt.Printf("  first write accepted: min=%v  median=%v  max=%v\n", round(write[0]), round(pct(write, 0.5)), round(write[len(write)-1]))
	return nil
}

type nodeStatus struct {
	State      string `json:"state"`
	Term       uint64 `json:"term"`
	IsolatedMS int64  `json:"isolated_ms"`
}

func status(node string) (nodeStatus, error) {
	var s nodeStatus
	resp, err := httpClient.Get(node + "/cluster/status")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// waitLeader polls until some node other than skip is a connected leader. An
// isolated old leader still reports Leader, so it is ignored, and if two
// nodes claim leadership the higher term wins.
func waitLeader(nodes []string, skip string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		best, bestTerm := "", uint64(0)
		for _, n := range nodes {
			if n == skip {
				continue
			}
			if s, err := status(n); err == nil && s.State == "Leader" && s.IsolatedMS == 0 && s.Term >= bestTerm {
				best, bestTerm = n, s.Term
			}
		}
		if best != "" {
			return best, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader within %v", timeout)
}

func put(node, key string) error {
	req, err := http.NewRequest(http.MethodPut, node+"/kv/"+key, bytes.NewReader([]byte("value-0123456789")))
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("PUT %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func isolate(node, token string) error {
	req, err := http.NewRequest(http.MethodPost, node+"/demo/isolate?seconds=3", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Demo-Token", token)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("isolate %s: status %d (wrong token?)", node, resp.StatusCode)
	}
	return nil
}

func sortDurations(d []time.Duration) { slices.Sort(d) }

func pct(sorted []time.Duration, p float64) time.Duration {
	return sorted[int(float64(len(sorted)-1)*p)]
}

func round(d time.Duration) time.Duration { return d.Round(10 * time.Microsecond) }

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
