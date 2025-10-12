package diskview

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

// Pager manages access to pages within a disk file using memory mapping.
// It handles page-level I/O and maintains information about the file size
// and page boundaries.
type Pager struct {
	source   string
	file     *os.File
	info     os.FileInfo
	pageSize int
}

// NewPager creates a new Pager for the given source file.
// The file is opened in read-write mode and will be created if it doesn't exist.
// The page size is set to the system's page size.
func NewPager(source string) (*Pager, error) {
	file, err := os.OpenFile(source, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	pager := &Pager{
		source:   source,
		file:     file,
		pageSize: os.Getpagesize(),
		info:     info,
	}
	return pager, nil
}

// GetPage returns a memory-mapped view of the page with the given ID.
// The returned mmap.MMap should be unmapped when no longer needed to avoid
// resource leaks.
func (p *Pager) GetPage(id int64) (mmap.MMap, error) {
	offset := id * int64(p.pageSize)
	region, err := mmap.MapRegion(p.file, p.pageSize, mmap.RDWR, 0, offset)
	if err != nil {
		return nil, err
	}
	return region, nil
}

// PageCount returns the number of complete pages in the file.
// Partial pages at the end are not counted.
func (p *Pager) PageCount() int64 {
	size := p.info.Size()
	return size / int64(p.pageSize)
}

// RefreshInfo updates the cached file information.
// This should be called after operations that change the file size.
func (p *Pager) RefreshInfo() error {
	info, err := p.file.Stat()
	if err != nil {
		return err
	}
	p.info = info
	return nil
}

// Close closes the underlying file.
func (p *Pager) Close() error {
	return p.file.Close()
}
