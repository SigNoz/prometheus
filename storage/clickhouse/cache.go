package clickhouse

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

type Cache struct {
	*lru.Cache
	rw           *sync.RWMutex
	evictedItems map[interface{}]interface{}
}

// NewCache creates a Cache.
func NewCache(size int) (*Cache, error) {
	evictedItems := make(map[interface{}]interface{})
	rw := new(sync.RWMutex)
	lruCache, err := lru.NewWithEvict(size, func(key interface{}, value interface{}) {
		rw.Lock()
		evictedItems[key] = value
		rw.Unlock()
	})
	if err != nil {
		return nil, err
	}

	return &Cache{
		Cache:        lruCache,
		evictedItems: evictedItems,
		rw:           rw,
	}, nil
}

// RemoveEvictedItems cleans all the evicted items.
func (c *Cache) RemoveEvictedItems() {
	c.rw.Lock()
	// we need to keep the original pointer to evictedItems map as it is used in the closure of lru.NewWithEvict
	for k := range c.evictedItems {
		delete(c.evictedItems, k)
	}
	c.rw.Unlock()
}

// Get retrieves an item from the LRU cache or evicted items.
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	if val, ok := c.Cache.Get(key); ok {
		return val, ok
	}
	c.rw.RLock()
	val, ok := c.evictedItems[key]
	// if the item is in the evicted items, we need to add it back to the LRU cache
	if ok {
		delete(c.evictedItems, key)
		c.Cache.Add(key, val)
	}
	c.rw.RUnlock()
	return val, ok
}

// Purge removes all the items from the LRU cache and evicted items.
func (c *Cache) Purge() {
	c.Cache.Purge()
	c.RemoveEvictedItems()
}
