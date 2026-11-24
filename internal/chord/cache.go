package chord

import (
	"math/big"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter"
)

// Cache maps a KeyID to the address of the node that was responsible for it.
type Cache struct {
	enabled      bool
	store        otter.Cache[string, string]
	hits, misses atomic.Int64
}

func NewCache(enabled bool, capacity int, ttl time.Duration) (*Cache, error) {
	c := &Cache{enabled: enabled}
	if !enabled {
		return c, nil
	}
	store, err := otter.MustBuilder[string, string](capacity).WithTTL(ttl).Build()
	if err != nil {
		return nil, err
	}
	c.store = store
	return c, nil
}

func (c *Cache) Enabled() bool { return c.enabled }

func (c *Cache) Get(key *big.Int) (string, bool) {
	return c.store.Get(string(IDBytes(key)))
}

func (c *Cache) Set(key *big.Int, ownerAddr string) {
	c.store.Set(string(IDBytes(key)), ownerAddr)
}

func (c *Cache) Delete(key *big.Int) {
	c.store.Delete(string(IDBytes(key)))
}

func (c *Cache) Size() int {
	if !c.enabled {
		return 0
	}
	return c.store.Size()
}

// Flush clears all entries and resets hit/miss counters.
func (c *Cache) Flush() {
	if c.enabled {
		c.store.Clear()
	}
	c.hits.Store(0)
	c.misses.Store(0)
}

func (c *Cache) Close() {
	if c.enabled {
		c.store.Close()
	}
}
