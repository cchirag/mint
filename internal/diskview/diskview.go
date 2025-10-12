package diskview

import (
	"fmt"

	"github.com/edsrzf/mmap-go"
)

// Config holds configuration options for the DiskViewer.
type Config struct {
	// MaxCapacity is the maximum number of pages to keep in the LRU cache.
	// Defaults to 10 if not specified.
	MaxCapacity int
}

// DefaultConfig provides sensible defaults for DiskViewer configuration.
var DefaultConfig Config = Config{
	MaxCapacity: 10,
}

// DiskViewer provides a page-based view of a disk file with LRU caching.
//
// Thread Safety:
// DiskViewer is internally thread-safe for concurrent reads and writes to
// different pages. However, for transactional semantics (e.g., atomic multi-page
// operations, isolation), a higher-level transaction layer should coordinate access.
//
// The internal locks in DiskViewer protect:
// - Cache consistency (LRU eviction, map updates)
// - File metadata consistency (page count, file info)
// - Safe concurrent mmap operations
//
// The internal locks do NOT provide:
// - Transaction isolation
// - Atomic multi-page operations
// - Serializable access to page contents
type DiskViewer struct {
	cache *Cache
	pager *Pager
}

// New creates a new DiskViewer for the given source file.
// The source file is opened in read-write mode and will be created if it doesn't exist.
// Returns an error if the file cannot be opened or if initialization fails.
func New(source string, config Config) (*DiskViewer, error) {
	dv := new(DiskViewer)
	dv.cache = NewCache(config)
	pager, err := NewPager(source)
	if err != nil {
		return nil, err
	}
	dv.pager = pager
	return dv, nil
}

// Read retrieves the page with the given ID.
// It first checks the cache, and if not found, loads the page from disk
// and adds it to the cache. Returns the memory-mapped page data.
func (d *DiskViewer) Read(id int64) (mmap.MMap, error) {
	if data, err := d.cache.Get(id); err == nil {
		return data, nil
	}

	data, err := d.pager.GetPage(id)
	if err != nil {
		return nil, err
	}

	err = d.cache.Set(id, data)
	if err != nil {
		data.Unmap()
		return nil, err
	}

	return data, nil
}

// Create allocates a new page on disk by writing zeros.
// It handles partial writes by continuing until the full page is written.
// Returns the ID of the newly created page.
func (d *DiskViewer) Create() (int64, error) {
	remaining := d.pager.pageSize
	offset := d.pager.PageCount() * int64(d.pager.pageSize)

	for remaining > 0 {
		n, err := d.fill(remaining, offset)
		if err != nil {
			return 0, fmt.Errorf("failed to write page at offset %d: %w", offset, err)
		}
		remaining -= n
		offset += int64(n)
	}

	if err := d.pager.RefreshInfo(); err != nil {
		return 0, fmt.Errorf("failed to refresh file info: %w", err)
	}
	id := d.pager.PageCount() - 1

	d.cache.Get(id)

	return d.pager.PageCount() - 1, nil
}

// fill writes count zero bytes at the given offset.
// Returns the number of bytes written and any error encountered.
// May return a partial write count if an error occurs.
func (d *DiskViewer) fill(count int, offset int64) (int, error) {
	data := make([]byte, count)
	n, err := d.pager.file.WriteAt(data, offset)
	if err != nil {
		return n, err
	}
	return n, nil
}

// Close releases all resources held by the DiskViewer.
// This includes closing the underlying file and unmapping any cached pages.
func (d *DiskViewer) Close() error {
	if err := d.cache.Close(); err != nil {
		return err
	}
	return d.pager.Close()
}
