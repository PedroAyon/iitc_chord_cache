package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	pb "chord/gen/chordpb"
	"chord/internal/bench"
	"chord/internal/chord"
	"chord/internal/client"
)

const usageText = `Usage: chord-cli [-node host:port] <command> [flags] [args]

Commands:
  put <file>                        upload one file
  upload -dir <dir> [-workers N]    upload every file in a directory
  get [-bypass-cache] [-out f] <name>  retrieve a file and print routing telemetry
  ring                              show the ring in order
  flush                             flush the cache on every node
  bench [flags]                     run the cache ON/OFF benchmark (see bench -h)
`

func main() {
	node := flag.String("node", getenv("CHORD_NODE", "localhost:7000"), "address of any ring node")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usageText); flag.PrintDefaults() }
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	var err error
	switch args[0] {
	case "put":
		err = cmdPut(ctx, *node, args[1:])
	case "upload":
		err = cmdUpload(ctx, *node, args[1:])
	case "get":
		err = cmdGet(ctx, *node, args[1:])
	case "ring":
		err = cmdRing(ctx, *node)
	case "flush":
		err = cmdFlush(ctx, *node)
	case "bench":
		err = cmdBench(ctx, *node, args[1:])
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdPut(ctx context.Context, node string, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: put <file>")
	}
	cs := client.New()
	defer cs.Close()
	resp, err := putFile(ctx, cs, node, args[0])
	if err != nil {
		return err
	}
	fmt.Printf("stored %s on %s hops=%d path=%s server_time=%s\n",
		filepath.Base(args[0]), resp.Owner.Addr, resp.HopCount, shortPath(resp.Path), time.Duration(resp.ElapsedNs))
	return nil
}

func putFile(ctx context.Context, cs *client.Conns, node, path string) (*pb.PutFileResponse, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cl, err := cs.File(node)
	if err != nil {
		return nil, err
	}
	return cl.PutFile(ctx, &pb.PutFileRequest{Name: filepath.Base(path), Data: data})
}

func cmdUpload(ctx context.Context, node string, args []string) error {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	dir := fs.String("dir", "", "directory of files to upload")
	workers := fs.Int("workers", 16, "concurrent uploads")
	fs.Parse(args)
	if *dir == "" {
		return fmt.Errorf("-dir is required")
	}
	entries, err := os.ReadDir(*dir)
	if err != nil {
		return err
	}
	var paths []string
	for _, e := range entries {
		if !e.IsDir() {
			paths = append(paths, filepath.Join(*dir, e.Name()))
		}
	}

	cs := client.New()
	defer cs.Close()
	var next, failed, hops atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(paths) {
					return
				}
				resp, err := putFile(ctx, cs, node, paths[i])
				if err != nil {
					failed.Add(1)
					fmt.Fprintf(os.Stderr, "upload %s: %v\n", paths[i], err)
					continue
				}
				hops.Add(int64(resp.HopCount))
			}
		}()
	}
	wg.Wait()
	ok := int64(len(paths)) - failed.Load()
	fmt.Printf("uploaded %d/%d files in %s (avg hops %.2f)\n", ok, len(paths), time.Since(start).Round(time.Millisecond), float64(hops.Load())/float64(max(ok, 1)))
	if failed.Load() > 0 {
		return fmt.Errorf("%d uploads failed", failed.Load())
	}
	return nil
}

func cmdGet(ctx context.Context, node string, args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	bypass := fs.Bool("bypass-cache", false, "skip the node's cache")
	out := fs.String("out", "", "write the file to this path")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: get [-bypass-cache] [-out path] <name>")
	}
	cs := client.New()
	defer cs.Close()
	cl, err := cs.File(node)
	if err != nil {
		return err
	}
	t0 := time.Now()
	resp, err := cl.GetFile(ctx, &pb.GetFileRequest{Name: fs.Arg(0), BypassCache: *bypass})
	rtt := time.Since(t0)
	if err != nil {
		return err
	}
	if !resp.Found {
		return fmt.Errorf("%s not found on %s", fs.Arg(0), resp.Owner.Addr)
	}
	cache := "MISS"
	switch {
	case *bypass:
		cache = "BYPASS"
	case resp.FromCache:
		cache = "HIT"
	}
	fmt.Printf("file:     %s (%d bytes)\n", fs.Arg(0), len(resp.Data))
	fmt.Printf("owner:    %s (%s)\n", resp.Owner.Addr, chord.ShortHex(resp.Owner.Id))
	fmt.Printf("hops:     %d\n", resp.HopCount)
	fmt.Printf("path:     %s\n", shortPath(resp.Path))
	fmt.Printf("cache:    %s\n", cache)
	fmt.Printf("latency:  %s (node: %s)\n", rtt, time.Duration(resp.ElapsedNs))
	if resp.TraceId != "" {
		fmt.Printf("trace_id: %s\n", resp.TraceId)
	}
	if *out != "" {
		return os.WriteFile(*out, resp.Data, 0o644)
	}
	return nil
}

func cmdRing(ctx context.Context, node string) error {
	cs := client.New()
	defer cs.Close()
	states, err := client.WalkRing(ctx, cs, node)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "#\tNODE_ID\tADDRESS\tRANGE\tFILES\tCACHE\tHITS\tMISSES\tHIT%\tENTRIES")
	totalFiles := int64(0)
	for i, s := range states {
		pred := "?"
		if s.HasPredecessor {
			pred = chord.ShortHex(s.Predecessor.Id)
		}
		self := chord.ShortHex(s.Self.Id)
		total := s.CacheHits + s.CacheMisses
		rate := 0.0
		if total > 0 {
			rate = 100 * float64(s.CacheHits) / float64(total)
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t(%s, %s]\t%d\t%v\t%d\t%d\t%.1f\t%d\n",
			i+1, self, s.Self.Addr, pred, self, s.FileCount, onOff(s.CacheEnabled), s.CacheHits, s.CacheMisses, rate, s.CacheSize)
		totalFiles += s.FileCount
	}
	w.Flush()
	fmt.Printf("\n%d nodes, %d files stored\n", len(states), totalFiles)
	return nil
}

func cmdFlush(ctx context.Context, node string) error {
	cs := client.New()
	defer cs.Close()
	states, err := client.WalkRing(ctx, cs, node)
	if err != nil {
		return err
	}
	if err := client.FlushAll(ctx, cs, states); err != nil {
		return err
	}
	fmt.Printf("flushed caches on %d nodes\n", len(states))
	return nil
}

func cmdBench(ctx context.Context, node string, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	cfg := bench.Config{Entry: node}
	fs.IntVar(&cfg.Files, "files", 500, "number of seeded files (names file-00000.dat ...)")
	fs.IntVar(&cfg.Requests, "requests", 5000, "lookups per pass (M)")
	fs.IntVar(&cfg.Concurrency, "concurrency", 32, "concurrent clients")
	fs.IntVar(&cfg.Warmup, "warmup", 500, "warm-up requests")
	fs.StringVar(&cfg.Distribution, "dist", "both", "uniform, zipf or both")
	fs.Float64Var(&cfg.ZipfS, "zipf-s", 1.1, "Zipf exponent (must be > 1)")
	fs.Int64Var(&cfg.Seed, "seed", 1, "random seed")
	fs.StringVar(&cfg.Out, "out", "results/bench.json", "raw results file")
	fs.Parse(args)
	if cfg.ZipfS <= 1 || cfg.Files < 1 || cfg.Concurrency < 1 {
		return fmt.Errorf("need -zipf-s > 1, -files >= 1, -concurrency >= 1")
	}
	return bench.Run(ctx, cfg)
}

func shortPath(path []string) string {
	short := make([]string, len(path))
	for i, p := range path {
		raw, _ := hex.DecodeString(p)
		short[i] = chord.ShortHex(raw)
	}
	return strings.Join(short, " -> ")
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
