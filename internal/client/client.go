package client

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "chord/gen/chordpb"
)

// Conns caches one FileService client per node address.
type Conns struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.FileServiceClient
}

func New() *Conns {
	return &Conns{conns: map[string]*grpc.ClientConn{}, clients: map[string]pb.FileServiceClient{}}
}

func (c *Conns) File(addr string) (pb.FileServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cl, ok := c.clients[addr]; ok {
		return cl, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	cl := pb.NewFileServiceClient(conn)
	c.clients[addr] = cl
	return cl, nil
}

func (c *Conns) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
}

// WalkRing follows successor pointers from start and returns every node's state,
// ordered by NodeID.
func WalkRing(ctx context.Context, c *Conns, start string) ([]*pb.NodeState, error) {
	var states []*pb.NodeState
	seen := map[string]bool{}
	addr := start
	for {
		cl, err := c.File(addr)
		if err != nil {
			return nil, err
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		st, err := cl.GetNodeState(cctx, &pb.Empty{})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("node %s: %w", addr, err)
		}
		// Key on the node's own address: the entry address may be a DNS name.
		if seen[st.Self.Addr] {
			break
		}
		seen[st.Self.Addr] = true
		states = append(states, st)
		addr = st.Successor.Addr
	}
	sort.Slice(states, func(i, j int) bool { return bytes.Compare(states[i].Self.Id, states[j].Self.Id) < 0 })
	return states, nil
}

func FlushAll(ctx context.Context, c *Conns, nodes []*pb.NodeState) error {
	for _, n := range nodes {
		cl, err := c.File(n.Self.Addr)
		if err != nil {
			return err
		}
		if _, err := cl.FlushCache(ctx, &pb.Empty{}); err != nil {
			return fmt.Errorf("flush %s: %w", n.Self.Addr, err)
		}
	}
	return nil
}
