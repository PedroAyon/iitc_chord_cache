package chord

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"time"

	pb "chord/gen/chordpb"
)

const maxHops = 256

type Ref struct {
	ID   *big.Int
	Addr string
}

func NewRef(addr string) Ref { return Ref{ID: HashID(addr), Addr: addr} }

func refFromPB(p *pb.NodeInfo) Ref { return Ref{ID: IDFromBytes(p.Id), Addr: p.Addr} }

func (r Ref) pb() *pb.NodeInfo { return &pb.NodeInfo{Id: IDBytes(r.ID), Addr: r.Addr} }

type Config struct {
	Addr                     string
	DataDir                  string
	CacheEnabled             bool
	CacheCapacity            int
	CacheTTL                 time.Duration
	RPCTimeout               time.Duration
	StabilizeInterval        time.Duration
	FixFingersInterval       time.Duration
	CheckPredecessorInterval time.Duration
}

type Node struct {
	cfg     Config
	self    Ref
	selfHex string
	log     *slog.Logger
	debug   bool
	store   *Storage
	cache   *Cache
	peers   *Peers

	mu         sync.RWMutex
	pred       *Ref
	fingers    [M]*Ref // fingers[i] = successor(n + 2^i); fingers[0] is the immediate successor
	nextFinger int
}

func NewNode(cfg Config, log *slog.Logger) (*Node, error) {
	store, err := NewStorage(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	cache, err := NewCache(cfg.CacheEnabled, cfg.CacheCapacity, cfg.CacheTTL)
	if err != nil {
		return nil, err
	}
	self := NewRef(cfg.Addr)
	selfHex := IDHex(self.ID)
	debug := log.Enabled(context.Background(), slog.LevelDebug)
	n := &Node{
		cfg:     cfg,
		self:    self,
		selfHex: selfHex,
		log:     log.With("node_id", selfHex),
		debug:   debug,
		store:   store,
		cache:   cache,
		peers:   NewPeers(debug),
	}
	n.fingers[0] = &self
	return n, nil
}

func (n *Node) Debug() bool { return n.debug }

func (n *Node) Close() {
	n.peers.Close()
	n.cache.Close()
}

func (n *Node) successor() Ref {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return *n.fingers[0]
}

func (n *Node) setSuccessor(r Ref) {
	n.mu.Lock()
	n.fingers[0] = &r
	n.mu.Unlock()
}

func (n *Node) predecessor() *Ref {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.pred
}

// Create bootstraps a new ring containing only this node.
func (n *Node) Create() {
	n.setSuccessor(n.self)
	n.log.Info("ring created", "component", "ring", "addr", n.self.Addr)
}

// Join enters an existing ring through bootstrapAddr and pulls the keys this node now owns.
func (n *Node) Join(ctx context.Context, bootstrapAddr string) error {
	succ, _, err := n.remoteFindSuccessor(ctx, Ref{Addr: bootstrapAddr}, n.self.ID, 0)
	if err != nil {
		return fmt.Errorf("find successor via %s: %w", bootstrapAddr, err)
	}
	n.setSuccessor(succ)
	moved, err := n.pullKeys(ctx, succ)
	if err != nil {
		return fmt.Errorf("transfer keys from %s: %w", succ.Addr, err)
	}
	if err := n.remoteNotify(ctx, succ); err != nil {
		return fmt.Errorf("notify %s: %w", succ.Addr, err)
	}
	n.log.Info("joined ring", "component", "ring", "successor", succ.Addr, "keys_received", moved)
	return nil
}

func (n *Node) pullKeys(ctx context.Context, from Ref) (int, error) {
	c, err := n.peers.ring(from.Addr)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	stream, err := c.TransferKeys(ctx, &pb.TransferKeysRequest{NewNode: n.self.pb()})
	if err != nil {
		return 0, err
	}
	count := 0
	for {
		rec, err := stream.Recv()
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		if err := n.store.Put(IDFromBytes(rec.KeyId), rec.Data); err != nil {
			return count, err
		}
		count++
	}
}

// Start launches the periodic maintenance goroutines.
func (n *Node) Start(ctx context.Context) {
	go n.every(ctx, n.cfg.StabilizeInterval, n.stabilize)
	go n.every(ctx, n.cfg.FixFingersInterval, n.fixFinger)
	go n.every(ctx, n.cfg.CheckPredecessorInterval, n.checkPredecessor)
}

func (n *Node) every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

// FindSuccessor resolves the node responsible for key. It forwards recursively and
// returns the successor together with the ordered list of node IDs that handled it.
func (n *Node) FindSuccessor(ctx context.Context, key *big.Int, hops int) (Ref, []string, error) {
	if hops > maxHops {
		return Ref{}, nil, fmt.Errorf("lookup exceeded %d hops", maxHops)
	}
	succ := n.successor()
	if inHalfOpen(key, n.self.ID, succ.ID) {
		if n.debug && TraceID(ctx) != "" {
			n.log.Debug("lookup resolved", "component", "routing", "trace_id", TraceID(ctx), "hop_count", hops, "key_id", IDHex(key), "successor", succ.Addr)
		}
		return succ, []string{n.selfHex}, nil
	}

	next := n.closestPrecedingNode(key)
	if next.ID.Cmp(n.self.ID) == 0 {
		next = succ
	}
	if n.debug && TraceID(ctx) != "" {
		n.log.Debug("forwarding lookup", "component", "routing", "trace_id", TraceID(ctx), "hop_count", hops, "key_id", IDHex(key), "next", next.Addr)
	}
	res, path, err := n.remoteFindSuccessor(ctx, next, key, hops+1)
	if err != nil && next.ID.Cmp(succ.ID) != 0 {
		n.dropFinger(next)
		res, path, err = n.remoteFindSuccessor(ctx, succ, key, hops+1)
	}
	if err != nil {
		return Ref{}, nil, err
	}
	return res, append([]string{n.selfHex}, path...), nil
}

func (n *Node) closestPrecedingNode(key *big.Int) Ref {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for i := M - 1; i >= 0; i-- {
		if f := n.fingers[i]; f != nil && inOpen(f.ID, n.self.ID, key) {
			return *f
		}
	}
	return n.self
}

func (n *Node) dropFinger(dead Ref) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := 1; i < M; i++ {
		if f := n.fingers[i]; f != nil && f.ID.Cmp(dead.ID) == 0 {
			n.fingers[i] = nil
		}
	}
}

func (n *Node) stabilize(ctx context.Context) {
	succ := n.successor()
	var x *Ref
	if succ.ID.Cmp(n.self.ID) == 0 {
		x = n.predecessor()
	} else {
		var err error
		if x, err = n.remoteGetPredecessor(ctx, succ); err != nil {
			n.log.Warn("successor unreachable", "component", "ring", "successor", succ.Addr, "err", err)
			return
		}
	}
	if x != nil && inOpen(x.ID, n.self.ID, succ.ID) {
		n.setSuccessor(*x)
		succ = *x
	}
	if succ.ID.Cmp(n.self.ID) != 0 {
		if err := n.remoteNotify(ctx, succ); err != nil {
			n.log.Warn("notify failed", "component", "ring", "successor", succ.Addr, "err", err)
		}
	}
}

func (n *Node) notify(candidate Ref) {
	if candidate.ID.Cmp(n.self.ID) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pred == nil || inOpen(candidate.ID, n.pred.ID, n.self.ID) {
		n.pred = &candidate
	}
}

// fixFinger refreshes one finger per call, cycling through entries 2..160.
func (n *Node) fixFinger(ctx context.Context) {
	n.mu.Lock()
	if n.nextFinger < 1 || n.nextFinger >= M {
		n.nextFinger = 1
	}
	i := n.nextFinger
	n.nextFinger++
	n.mu.Unlock()

	ref, _, err := n.FindSuccessor(ctx, fingerStart(n.self.ID, i), 0)
	if err != nil {
		return
	}
	n.mu.Lock()
	n.fingers[i] = &ref
	n.mu.Unlock()
}

func (n *Node) checkPredecessor(ctx context.Context) {
	p := n.predecessor()
	if p == nil {
		return
	}
	if err := n.remotePing(ctx, p.Addr); err != nil {
		n.mu.Lock()
		if n.pred != nil && n.pred.ID.Cmp(p.ID) == 0 {
			n.pred = nil
		}
		n.mu.Unlock()
		n.log.Warn("predecessor failed", "component", "ring", "predecessor", p.Addr)
	}
}

func (n *Node) responsibleFor(key *big.Int) bool {
	p := n.predecessor()
	return p == nil || inHalfOpen(key, p.ID, n.self.ID)
}
