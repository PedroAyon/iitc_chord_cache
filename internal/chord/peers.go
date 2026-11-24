package chord

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "chord/gen/chordpb"
)

// Peers caches one gRPC connection per remote address.
type Peers struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

func NewPeers(propagateTrace bool) *Peers {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if propagateTrace {
		opts = append(opts, grpc.WithUnaryInterceptor(ClientTraceInterceptor()))
	}
	return &Peers{conns: make(map[string]*grpc.ClientConn), opts: opts}
}

func (p *Peers) ring(addr string) (pb.RingServiceClient, error) {
	p.mu.RLock()
	c, ok := p.conns[addr]
	p.mu.RUnlock()
	if !ok {
		p.mu.Lock()
		var err error
		if c, ok = p.conns[addr]; !ok {
			c, err = grpc.NewClient(addr, p.opts...)
			if err == nil {
				p.conns[addr] = c
			}
		}
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	return pb.NewRingServiceClient(c), nil
}

func (p *Peers) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}
