package chord

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const traceHeader = "trace-id"

type traceKey struct{}

func TraceID(ctx context.Context) string {
	id, _ := ctx.Value(traceKey{}).(string)
	return id
}

func newTraceID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ServerTraceInterceptor extracts the trace id from metadata and creates one
// for client-facing calls. It is installed only when LOG_LEVEL=DEBUG.
func ServerTraceInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get(traceHeader); len(v) > 0 {
				id = v[0]
			}
		}
		if id == "" && strings.HasPrefix(info.FullMethod, "/chord.FileService/") {
			id = newTraceID()
		}
		if id != "" {
			ctx = context.WithValue(ctx, traceKey{}, id)
		}
		return handler(ctx, req)
	}
}

// ClientTraceInterceptor forwards the trace id of ctx to the next hop.
func ClientTraceInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id := TraceID(ctx); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, traceHeader, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
