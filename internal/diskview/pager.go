package diskview

import (
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

// Pager manages access to pages within a disk file using memory mapping.
// It handles page-level I/O and maintains information about the file size
// and page boundaries.
type Pager struct {
	source   string
	file     *os.File
	pageSize int
	mu       sync.RWMutex
}

// NewPager creates a new Pager for the given source file.
// The file is opened in read-write mode and will be created if it doesn't exist.
// The page size is set to the system's page size.
func NewPager(source string) (*Pager, error) {
	file, err := os.OpenFile(source, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	pager := &Pager{
		source:   source,
		file:     file,
		pageSize: os.Getpagesize(),
	}
	return pager, nil
}

// GetPage returns a memory-mapped view of the page with the given ID.
// The returned mmap.MMap should be unmapped when no longer needed to avoid
// resource leaks.
func (p *Pager) GetPage(id int64) (mmap.MMap, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	offset := id * int64(p.pageSize)
	region, err := mmap.MapRegion(p.file, p.pageSize, mmap.RDWR, 0, offset)
	if err != nil {
		return nil, err
	}
	return region, nil
}

// PageCount returns the number of complete pages in the file.
// Partial pages at the end are not counted.
func (p *Pager) PageCount() (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size() / int64(p.pageSize), nil
}

// Write writes count zero bytes at the given offset.
// Returns the number of bytes written and any error encountered.
// May return a partial write count if an error occurs.
func (p *Pager) Write(count int, offset int64) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data := make([]byte, count)
	n, err := p.file.WriteAt(data, offset)
	if err != nil {
		return n, err
	}

	return n, nil
}

// Close closes the underlying file.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Close()
}
