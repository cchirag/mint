package diskview

import (
	"errors"
	"fmt"
	"sync"

	"github.com/edsrzf/mmap-go"
)

// ErrCacheMiss is returned when a requested cache entry is not found.
var ErrCacheMiss = errors.New("cache miss")

// CacheNode represents a single node in the doubly-linked list used by the LRU cache.
// Each node stores an ID, associated data, and pointers to the next and previous nodes.
type CacheNode struct {
	id   int64
	data mmap.MMap
	next *CacheNode
	prev *CacheNode
}

// Cache implements a thread-safe Least Recently Used (LRU) cache.
// It uses a doubly-linked list for maintaining access order and a map for O(1) lookups.
// The most recently accessed items are kept at the front of the list, while the least
// recently accessed items are at the back and evicted when capacity is reached.
type Cache struct {
	mu     sync.Mutex
	lookup map[int64]*CacheNode
	head   *CacheNode
	tail   *CacheNode
	config Config
}

// NewCache creates and initializes a new LRU cache with the given configuration.
// If MaxCapacity is not set in the config, it defaults to 10.
// The cache uses sentinel head and tail nodes to simplify list operations.
func NewCache(config Config) *Cache {
	if config.MaxCapacity == 0 {
		config.MaxCapacity = 10
	}

	head := &CacheNode{}
	tail := &CacheNode{}

	head.next = tail
	tail.prev = head

	cache := &Cache{
		lookup: make(map[int64]*CacheNode, config.MaxCapacity),
		config: config,
		head:   head,
		tail:   tail,
	}
	return cache
}

// Get retrieves the data associated with the given id from the cache.
// If found, the entry is moved to the front of the LRU list (marked as recently used).
// Returns ErrCacheMiss if the id is not found in the cache.
// This operation is thread-safe.
func (l *Cache) Get(id int64) (mmap.MMap, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if node, ok := l.lookup[id]; ok {
		l.moveToFront(node)
		return node.data, nil
	}
	return nil, ErrCacheMiss
}

// Set adds or updates an entry in the cache with the given id and data.
// If the id already exists, its data is updated and the entry is moved to the front.
// If the cache is at capacity, the least recently used entry is evicted.
// This operation is thread-safe.
func (l *Cache) Set(id int64, data mmap.MMap) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var err error

	if node, ok := l.lookup[id]; ok {
		node.data = data
		l.moveToFront(node)
		return nil
	}

	if len(l.lookup) >= l.config.MaxCapacity {
		node := l.removeFromBack()
		err = node.data.Unmap()
		delete(l.lookup, node.id)
	}
	node := &CacheNode{
		id:   id,
		data: data,
	}
	l.insertAtFront(node)
	l.lookup[id] = node
	return err
}

// Close unmaps all cached memory-mapped regions and releases all cache resources.
// It iterates through all cached entries, unmapping each memory-mapped region and
// clearing the node pointers. The lookup map is reset and the sentinel head and tail
// nodes are set to nil.
//
// If any unmap operation fails, Close records the first error encountered but continues
// to unmap and clean up remaining entries to prevent resource leaks. The first error
// is then returned to the caller.
//
// After Close is called, the cache is in an invalid state and should not be used.
// Any subsequent operations on the cache will result in undefined behavior.
//
// This method is thread-safe.
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for _, value := range c.lookup {
		if err := value.data.Unmap(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to unmap page %d: %w", value.id, err)
		}

		value.next, value.prev = nil, nil
	}
	c.lookup = make(map[int64]*CacheNode)
	c.head = nil
	c.tail = nil
	return firstErr
}

// insertAtFront adds the given node to the front of the doubly-linked list,
// immediately after the sentinel head node.
func (l *Cache) insertAtFront(node *CacheNode) {
	node.next = l.head.next
	node.prev = l.head
	l.head.next.prev = node
	l.head.next = node
}

// removeFromBack removes and returns the node at the back of the list
// (the least recently used entry), just before the sentinel tail node.
func (l *Cache) removeFromBack() *CacheNode {
	last := l.tail.prev
	last.prev.next = l.tail
	l.tail.prev = last.prev
	last.prev, last.next = nil, nil
	return last
}

// moveToFront moves the given node to the front of the doubly-linked list,
// marking it as the most recently used entry. If the node is already at the front,
// this is a no-op.
func (l *Cache) moveToFront(node *CacheNode) {
	if node == l.head.next {
		return
	}
	node.prev.next = node.next
	node.next.prev = node.prev

	l.insertAtFront(node)
}
