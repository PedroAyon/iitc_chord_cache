package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"chord/internal/workload"
)

func main() {
	count := flag.Int("count", 500, "number of files to generate")
	out := flag.String("out", "./files", "output directory")
	minSize := flag.Int("min-size", 1024, "minimum file size in bytes")
	maxSize := flag.Int("max-size", 256*1024, "maximum file size in bytes")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	if *minSize <= 0 || *maxSize < *minSize {
		fmt.Fprintln(os.Stderr, "invalid size range")
		os.Exit(2)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	r := rand.New(rand.NewSource(*seed))
	total := 0
	for i := 0; i < *count; i++ {
		buf := make([]byte, *minSize+r.Intn(*maxSize-*minSize+1))
		r.Read(buf)
		if err := os.WriteFile(filepath.Join(*out, workload.FileName(i)), buf, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		total += len(buf)
	}
	fmt.Printf("generated %d files (%.1f MB) in %s\n", *count, float64(total)/1e6, *out)
}
