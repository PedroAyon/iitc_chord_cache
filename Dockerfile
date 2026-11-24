FROM golang:1.27.4 AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
WORKDIR /src
COPY go.mod ./
COPY proto ./proto
RUN mkdir -p gen/chordpb && protoc \
    --go_out=. --go_opt=module=chord \
    --go-grpc_out=. --go-grpc_opt=module=chord \
    proto/chord.proto
COPY cmd ./cmd
COPY internal ./internal
RUN go mod tidy
RUN go test ./internal/chord/... \
    && CGO_ENABLED=0 go build -o /out/chord-node ./cmd/chord-node \
    && CGO_ENABLED=0 go build -o /out/chord-cli ./cmd/chord-cli \
    && CGO_ENABLED=0 go build -o /out/datagen ./cmd/datagen

FROM alpine:3.20 AS node
RUN apk add --no-cache iproute2-tc # installs the Linux Traffic Control (tc) utility
COPY --from=build /out/chord-node /usr/local/bin/chord-node
COPY docker/node_latency_entrypoint.sh /entrypoint.sh
EXPOSE 7000
ENTRYPOINT ["/entrypoint.sh"]

FROM alpine:3.20 AS cli
COPY --from=build /out/chord-cli /out/datagen /usr/local/bin/
WORKDIR /work
ENTRYPOINT ["chord-cli"]
