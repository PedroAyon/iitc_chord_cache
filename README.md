# Chord DHT with per-node caching

A distributed file store in Go built on the Chord DHT (Stoica et al.), with an optional per-node
[Otter](https://github.com/maypok86/otter) cache, gRPC/Protobuf communication, Docker Compose scaling,
a CLI client and a cache ON/OFF benchmark.

## Layout

```
proto/chord.proto          RingService (node<->node) and FileService (client/control)
cmd/chord-node/            node daemon
cmd/chord-cli/             CLI: put, upload, get, ring, flush, bench
cmd/datagen/               synthetic file generator
internal/chord/            ids, finger table, stabilization, storage, cache, gRPC server, tracing
internal/client/           connection cache and ring walking for the CLI/benchmark
internal/bench/            benchmark suite
internal/logging/          slog setup (LOG_LEVEL, LOG_FORMAT)
internal/workload/         shared synthetic file naming
```

Generated protobuf code (`gen/`) is not committed; the Docker build produces it with `protoc`.

## Prerequisites & Setup
- Docker with Docker Compose v2
- Go 1.27.4 or newer (needed only for the IDE and local tests, not for running the cluster)
- make
- protoc:
  - macOS: `brew install protobuf`
  - Debian/Ubuntu: `sudo apt install protobuf-compiler` 
  - Windows: `choco install protoc, or use WSL`
- Install the protobuf Go plugins:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
```
Make sure $(go env GOPATH)/bin is on your PATH. Otherwise, protoc can't find the plugins:

``` bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

- Generate the protobuf Go Code
``` bash
make proto
```
- Download the dependencies and create go.sum

```bash
go mod tidy
```

## Quick start

```sh
make up WORKERS=9          # 1 bootstrap + 9 workers = 10 nodes, waits for stabilization
make seed FILES=500        # generate 500 files (1-256 KB) and upload them
make ring                  # nodes in ring order: ranges, file counts, cache stats
make bench                 # uniform + zipf, cache OFF vs ON
make results               # copy raw results to ./results/bench.json
make down                  # stop containers
make clean                 # also remove volumes and images
```

CLI examples (via `docker compose run --rm chord-cli ...`):

```sh
chord-cli get file-00042.dat                # miss on first read, hit on the second
chord-cli get -bypass-cache file-00042.dat
chord-cli put ./report.txt
chord-cli flush
chord-cli bench -dist zipf -requests 20000 -concurrency 64
```
Flags go before the file name (`get -bypass-cache name`). `-node host:port` (or `CHORD_NODE`) selects the entry node.

## How it works

- **IDs:** NodeID = SHA-1(`IP:Port`), KeyID = SHA-1(filename), 160-bit ring using `math/big`.
- **Routing:** predecessor, successor and a 160-entry finger table (entry 1 is the successor, maintained by
  `stabilize`; entries 2-160 by `fix_fingers`). `find_successor` forwards recursively, so a lookup returns
  the ordered node path and hop count (`len(path)-1`, i.e. overlay forwards; the final fetch is not counted).
- **Ring maintenance:** `create`, `join` (find successor, pull keys in `(predecessor, self]` through the
  streaming `TransferKeys` RPC, notify), plus background `stabilize`, `notify`, `fix_fingers`, `check_predecessor`.
- **Storage:** one file per KeyID under `/var/lib/chord/data` inside each container.
- **Cache:** each node caches `KeyID -> owner address` in Otter (bounded, TTL default 20 s). A hit skips the
  O(log N) routing and goes straight to the owner. The owner verifies responsibility; if it is no longer
  responsible (or unreachable) the entry is dropped and the key is re-resolved.
- **Benchmark:** warm-up (cache bypassed), flush, M lookups with `bypass_cache=true`, flush and reset counters,
  the same M lookups with the cache on. Both passes use the identical request sequence and entry nodes
  (spread randomly over all nodes) and the same log level. Reports avg/min/max/p50/p95/p99, avg hops,
  throughput and hit rate; raw per-request samples go to JSON.

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `LOG_LEVEL` | `WARN` | `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `LOG_FORMAT` | `json` | `json` or `text` |
| `CACHE_ENABLED` | `true` | turn the Otter cache on/off for the whole cluster |
| `CACHE_CAPACITY` | `10000` | max entries per node |
| `CACHE_TTL` | `20s` | entry lifetime |
| `BOOTSTRAP_ADDR` | empty | empty creates a ring, otherwise join through this address |
| `ADVERTISE_ADDR` | auto | `ip:port` used for the NodeID (default: container IP + `PORT`) |
| `PORT` | `7000` | gRPC port |
| `NET_DELAY` | `0.25ms` | Network Latency Simulation|
| `NET_JITTER` | `0.05ms` | Network Latency Fluctuation|
