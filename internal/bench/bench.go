package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	pb "chord/gen/chordpb"
	"chord/internal/client"
	"chord/internal/workload"
)

type Config struct {
	Entry        string  `json:"entry"`
	Files        int     `json:"files"`
	Requests     int     `json:"requests"`
	Concurrency  int     `json:"concurrency"`
	Warmup       int     `json:"warmup"`
	Distribution string  `json:"distribution"` // uniform, zipf or both
	ZipfS        float64 `json:"zipf_s"`
	Seed         int64   `json:"seed"`
	Out          string  `json:"-"`
}

type Sample struct {
	Index     int    `json:"index"`
	File      int    `json:"file"`
	Entry     string `json:"entry"`
	OK        bool   `json:"ok"`
	LatencyUs int64  `json:"latency_us"`
	Hops      int    `json:"hops"`
	FromCache bool   `json:"from_cache"`
}

type Summary struct {
	Distribution string  `json:"distribution"`
	Mode         string  `json:"mode"`
	Requests     int     `json:"requests"`
	Errors       int     `json:"errors"`
	AvgMs        float64 `json:"avg_ms"`
	MinMs        float64 `json:"min_ms"`
	MaxMs        float64 `json:"max_ms"`
	P50Ms        float64 `json:"p50_ms"`
	P95Ms        float64 `json:"p95_ms"`
	P99Ms        float64 `json:"p99_ms"`
	AvgHops      float64 `json:"avg_hops"`
	Throughput   float64 `json:"throughput_rps"`
	HitRatePct   float64 `json:"cache_hit_rate_pct"`
	DurationSec  float64 `json:"duration_sec"`
}

type Pass struct {
	Summary Summary  `json:"summary"`
	Samples []Sample `json:"samples"`
}

type Report struct {
	Config Config `json:"config"`
	Nodes  int    `json:"nodes"`
	Passes []Pass `json:"passes"`
}

type plan struct {
	files   []int
	entries []string
}

func Run(ctx context.Context, cfg Config) error {
	cs := client.New()
	defer cs.Close()

	nodes, err := client.WalkRing(ctx, cs, cfg.Entry)
	if err != nil {
		return err
	}
	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.Self.Addr
	}
	fmt.Printf("Ring has %d nodes; %d requests, concurrency %d, %d files\n", len(nodes), cfg.Requests, cfg.Concurrency, cfg.Files)

	dists := []string{cfg.Distribution}
	if cfg.Distribution == "both" {
		dists = []string{"uniform", "zipf"}
	}

	report := Report{Config: cfg, Nodes: len(nodes)}
	for _, dist := range dists {
		fmt.Printf("\n=== %s distribution ===\n", dist)
		p := buildPlan(dist, cfg, cfg.Requests, cfg.Seed, addrs)

		fmt.Printf("warm-up (%d requests, cache bypassed)...\n", cfg.Warmup)
		runPass(ctx, cs, cfg, buildPlan(dist, cfg, cfg.Warmup, cfg.Seed+1, addrs), true)

		if err := client.FlushAll(ctx, cs, nodes); err != nil {
			return err
		}
		fmt.Println("running pass with cache OFF...")
		off := runPass(ctx, cs, cfg, p, true)
		off.Summary.Distribution, off.Summary.Mode = dist, "cache-off"

		if err := client.FlushAll(ctx, cs, nodes); err != nil {
			return err
		}
		fmt.Println("running pass with cache ON...")
		on := runPass(ctx, cs, cfg, p, false)
		on.Summary.Distribution, on.Summary.Mode = dist, "cache-on"

		printComparison(dist, off.Summary, on.Summary)
		report.Passes = append(report.Passes, off, on)
	}
	return save(cfg.Out, report)
}

func buildPlan(dist string, cfg Config, n int, seed int64, addrs []string) plan {
	r := rand.New(rand.NewSource(seed))
	p := plan{files: make([]int, n), entries: make([]string, n)}
	var zipf *rand.Zipf
	if dist == "zipf" {
		zipf = rand.NewZipf(r, cfg.ZipfS, 1, uint64(cfg.Files-1))
	}
	for i := 0; i < n; i++ {
		if zipf != nil {
			p.files[i] = int(zipf.Uint64())
		} else {
			p.files[i] = r.Intn(cfg.Files)
		}
		p.entries[i] = addrs[r.Intn(len(addrs))]
	}
	return p
}

func runPass(ctx context.Context, cs *client.Conns, cfg Config, p plan, bypass bool) Pass {
	n := len(p.files)
	samples := make([]Sample, n)
	var next atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				samples[i] = doRequest(ctx, cs, i, p.files[i], p.entries[i], bypass)
			}
		}()
	}
	wg.Wait()
	return Pass{Summary: summarize(samples, time.Since(start)), Samples: samples}
}

func doRequest(ctx context.Context, cs *client.Conns, i, file int, entry string, bypass bool) Sample {
	s := Sample{Index: i, File: file, Entry: entry}
	cl, err := cs.File(entry)
	if err != nil {
		return s
	}
	t0 := time.Now()
	resp, err := cl.GetFile(ctx, &pb.GetFileRequest{Name: workload.FileName(file), BypassCache: bypass})
	s.LatencyUs = time.Since(t0).Microseconds()
	if err != nil || !resp.Found {
		return s
	}
	s.OK, s.Hops, s.FromCache = true, int(resp.HopCount), resp.FromCache
	return s
}

func summarize(samples []Sample, elapsed time.Duration) Summary {
	s := Summary{Requests: len(samples), DurationSec: elapsed.Seconds()}
	var lat []float64
	var hops, hits int
	for _, x := range samples {
		if !x.OK {
			s.Errors++
			continue
		}
		lat = append(lat, float64(x.LatencyUs)/1000)
		hops += x.Hops
		if x.FromCache {
			hits++
		}
	}
	if len(lat) == 0 {
		return s
	}
	sort.Float64s(lat)
	sum := 0.0
	for _, v := range lat {
		sum += v
	}
	ok := float64(len(lat))
	s.AvgMs, s.MinMs, s.MaxMs = sum/ok, lat[0], lat[len(lat)-1]
	s.P50Ms, s.P95Ms, s.P99Ms = percentile(lat, 50), percentile(lat, 95), percentile(lat, 99)
	s.AvgHops = float64(hops) / ok
	s.HitRatePct = 100 * float64(hits) / ok
	s.Throughput = ok / elapsed.Seconds()
	return s
}

func percentile(sorted []float64, p float64) float64 {
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

func printComparison(dist string, off, on Summary) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "\nMETRIC (%s)\tCACHE OFF\tCACHE ON\tCHANGE\n", dist)
	row := func(name string, a, b float64, unit string) {
		change := "-"
		if a != 0 {
			change = fmt.Sprintf("%+.1f%%", (b-a)/a*100)
		}
		fmt.Fprintf(w, "%s\t%.2f%s\t%.2f%s\t%s\n", name, a, unit, b, unit, change)
	}
	row("avg latency", off.AvgMs, on.AvgMs, " ms")
	row("min latency", off.MinMs, on.MinMs, " ms")
	row("max latency", off.MaxMs, on.MaxMs, " ms")
	row("p50 latency", off.P50Ms, on.P50Ms, " ms")
	row("p95 latency", off.P95Ms, on.P95Ms, " ms")
	row("p99 latency", off.P99Ms, on.P99Ms, " ms")
	row("avg hops/lookup", off.AvgHops, on.AvgHops, "")
	row("throughput", off.Throughput, on.Throughput, " req/s")
	row("cache hit rate", off.HitRatePct, on.HitRatePct, " %")
	fmt.Fprintf(w, "errors\t%d\t%d\t\n", off.Errors, on.Errors)
	w.Flush()
}

func save(path string, r Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	fmt.Printf("\nraw results saved to %s\n", path)
	return nil
}
