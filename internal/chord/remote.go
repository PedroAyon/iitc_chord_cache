package chord

import (
	"context"
	"math/big"

	pb "chord/gen/chordpb"
)

type fetchResult struct {
	responsible bool
	found       bool
	data        []byte
}

func (n *Node) remoteFindSuccessor(ctx context.Context, to Ref, key *big.Int, hops int) (Ref, []string, error) {
	c, err := n.peers.ring(to.Addr)
	if err != nil {
		return Ref{}, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := c.FindSuccessor(ctx, &pb.FindSuccessorRequest{KeyId: IDBytes(key), HopCount: int32(hops)})
	if err != nil {
		return Ref{}, nil, err
	}
	return refFromPB(resp.Successor), resp.Path, nil
}

func (n *Node) remoteGetPredecessor(ctx context.Context, to Ref) (*Ref, error) {
	c, err := n.peers.ring(to.Addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := c.GetPredecessor(ctx, &pb.Empty{})
	if err != nil || !resp.HasPredecessor {
		return nil, err
	}
	r := refFromPB(resp.Predecessor)
	return &r, nil
}

func (n *Node) remoteNotify(ctx context.Context, to Ref) error {
	c, err := n.peers.ring(to.Addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	_, err = c.Notify(ctx, &pb.NotifyRequest{Node: n.self.pb()})
	return err
}

func (n *Node) remotePing(ctx context.Context, addr string) error {
	c, err := n.peers.ring(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	_, err = c.Ping(ctx, &pb.Empty{})
	return err
}

func (n *Node) storeAt(ctx context.Context, addr string, key *big.Int, data []byte) error {
	if addr == n.self.Addr {
		return n.store.Put(key, data)
	}
	c, err := n.peers.ring(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	_, err = c.StoreLocal(ctx, &pb.StoreLocalRequest{KeyId: IDBytes(key), Data: data})
	return err
}

func (n *Node) fetchLocal(key *big.Int) (fetchResult, error) {
	if !n.responsibleFor(key) {
		return fetchResult{}, nil
	}
	data, found, err := n.store.Get(key)
	return fetchResult{responsible: true, found: found, data: data}, err
}

func (n *Node) fetch(ctx context.Context, addr string, key *big.Int) (fetchResult, error) {
	if addr == n.self.Addr {
		return n.fetchLocal(key)
	}
	c, err := n.peers.ring(addr)
	if err != nil {
		return fetchResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	defer cancel()
	resp, err := c.FetchLocal(ctx, &pb.FetchLocalRequest{KeyId: IDBytes(key)})
	if err != nil {
		return fetchResult{}, err
	}
	return fetchResult{responsible: resp.Responsible, found: resp.Found, data: resp.Data}, nil
}
