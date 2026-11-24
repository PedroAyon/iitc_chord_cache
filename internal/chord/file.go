package chord

import (
	"context"
	"errors"
	"time"
)

type GetResult struct {
	Found     bool
	Data      []byte
	Owner     Ref
	Path      []string
	FromCache bool
	Elapsed   time.Duration
}

type PutResult struct {
	Owner   Ref
	Path    []string
	Elapsed time.Duration
}

// GetFile serves a client read. With the cache enabled and not bypassed, the cached
// owner address replaces the O(log N) lookup; a stale entry is dropped and re-resolved.
func (n *Node) GetFile(ctx context.Context, name string, bypassCache bool) (GetResult, error) {
	start := time.Now()
	key := HashID(name)
	useCache := n.cache.Enabled() && !bypassCache

	if useCache {
		if addr, ok := n.cache.Get(key); ok {
			r, err := n.fetch(ctx, addr, key)
			if err == nil && r.responsible {
				n.cache.hits.Add(1)
				if n.debug {
					n.log.Debug("cache hit", "component", "cache", "trace_id", TraceID(ctx), "key_id", IDHex(key), "owner", addr)
				}
				return GetResult{Found: r.found, Data: r.data, Owner: NewRef(addr), Path: []string{n.selfHex}, FromCache: true, Elapsed: time.Since(start)}, nil
			}
			n.cache.Delete(key)
			if n.debug {
				n.log.Debug("cache entry invalidated", "component", "cache", "trace_id", TraceID(ctx), "key_id", IDHex(key), "owner", addr)
			}
		}
		n.cache.misses.Add(1)
		if n.debug {
			n.log.Debug("cache miss", "component", "cache", "trace_id", TraceID(ctx), "key_id", IDHex(key))
		}
	}

	owner, path, err := n.FindSuccessor(ctx, key, 0)
	if err != nil {
		return GetResult{}, err
	}
	r, err := n.fetch(ctx, owner.Addr, key)
	if err != nil {
		return GetResult{}, err
	}
	if !r.responsible {
		return GetResult{}, errors.New("resolved owner is not responsible for the key (ring unstable)")
	}
	if useCache {
		n.cache.Set(key, owner.Addr)
	}
	return GetResult{Found: r.found, Data: r.data, Owner: owner, Path: path, Elapsed: time.Since(start)}, nil
}

func (n *Node) PutFile(ctx context.Context, name string, data []byte) (PutResult, error) {
	start := time.Now()
	key := HashID(name)
	owner, path, err := n.FindSuccessor(ctx, key, 0)
	if err != nil {
		return PutResult{}, err
	}
	if err := n.storeAt(ctx, owner.Addr, key, data); err != nil {
		return PutResult{}, err
	}
	return PutResult{Owner: owner, Path: path, Elapsed: time.Since(start)}, nil
}
