package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"chord/internal/chord"
	"chord/internal/logging"
)

func main() {
	log := logging.New()
	cfg := loadConfig()
	bootstrap := os.Getenv("BOOTSTRAP_ADDR")

	node, err := chord.NewNode(cfg, log)
	if err != nil {
		log.Error("node init failed", "err", err)
		os.Exit(1)
	}
	defer node.Close()

	lis, err := net.Listen("tcp", ":"+getenv("PORT", "7000"))
	if err != nil {
		log.Error("listen failed", "err", err)
		os.Exit(1)
	}
	var opts []grpc.ServerOption
	if node.Debug() {
		opts = append(opts, grpc.UnaryInterceptor(chord.ServerTraceInterceptor()))
	}
	srv := grpc.NewServer(opts...)
	chord.Register(srv, node)
	go srv.Serve(lis)

	log.Info("node started", "component", "node", "addr", cfg.Addr, "cache_enabled", cfg.CacheEnabled, "bootstrap", bootstrap)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if bootstrap == "" {
		node.Create()
	} else if err := joinWithRetry(ctx, node, bootstrap, log); err != nil {
		log.Error("join failed", "err", err)
		os.Exit(1)
	}
	node.Start(ctx)

	<-ctx.Done()
	srv.GracefulStop()
}

func joinWithRetry(ctx context.Context, node *chord.Node, bootstrap string, log *slog.Logger) error {
	var err error
	for i := range 60 {
		if err = node.Join(ctx, bootstrap); err == nil {
			return nil
		}
		log.Warn("join attempt failed", "component", "node", "attempt", i+1, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return err
}

func loadConfig() chord.Config {
	return chord.Config{
		Addr:                     advertiseAddr(getenv("PORT", "7000")),
		DataDir:                  getenv("DATA_DIR", "/var/lib/chord/data"),
		CacheEnabled:             getenv("CACHE_ENABLED", "true") == "true",
		CacheCapacity:            getenvInt("CACHE_CAPACITY", 10000),
		CacheTTL:                 getenvDuration("CACHE_TTL", 20*time.Second),
		RPCTimeout:               getenvDuration("RPC_TIMEOUT", 3*time.Second),
		StabilizeInterval:        getenvDuration("STABILIZE_INTERVAL", 200*time.Millisecond),
		FixFingersInterval:       getenvDuration("FIX_FINGERS_INTERVAL", 50*time.Millisecond),
		CheckPredecessorInterval: getenvDuration("CHECK_PREDECESSOR_INTERVAL", time.Second),
	}
}

func advertiseAddr(port string) string {
	if v := os.Getenv("ADVERTISE_ADDR"); v != "" {
		return v
	}
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && ipn.IP.To4() != nil {
			return net.JoinHostPort(ipn.IP.String(), port)
		}
	}
	return net.JoinHostPort("127.0.0.1", port)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
