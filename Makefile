WORKERS ?= 9
FILES   ?= 500
REQUESTS ?= 5000
CONCURRENCY ?= 32
DIST    ?= both
WAIT    ?= 15
BENCH_OUT ?= bench.json

CLI = docker compose run --rm chord-cli

.PHONY: proto build up scale seed gen ring bench flush logs results test down clean

# Optional: generate Go code locally (the Docker build does this on its own).
proto:
	mkdir -p gen/chordpb
	protoc --go_out=. --go_opt=module=chord --go-grpc_out=. --go-grpc_opt=module=chord proto/chord.proto

build:
	docker compose --profile tools build

# Bootstrap node + $(WORKERS) workers, then wait for the ring to stabilize.
up:
	docker compose up -d --build --scale chord-node=$(WORKERS)
	@echo "waiting $(WAIT)s for the ring to stabilize..."
	@sleep $(WAIT)
	@$(CLI) ring

scale:
	docker compose up -d --no-recreate --scale chord-node=$(WORKERS)

gen:
	docker compose run --rm --entrypoint datagen chord-cli -count $(FILES) -out /work/files

seed: gen
	$(CLI) upload -dir /work/files

ring:
	$(CLI) ring

flush:
	$(CLI) flush

bench:
	$(CLI) bench -files $(FILES) -requests $(REQUESTS) -concurrency $(CONCURRENCY) -dist $(DIST) -out /work/results/bench.json

logs:
	docker compose logs -f

# Copy the raw benchmark results out of the work volume into ./results.
results:
	mkdir -p results
	docker compose run --rm --entrypoint cat chord-cli /work/results/bench.json > results/$(BENCH_OUT)

test:
	go test ./internal/chord/...

down:
	docker compose --profile tools down

clean:
	docker compose --profile tools down -v --rmi local
