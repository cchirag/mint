package main

import (
	"fmt"
	"unsafe"
)

func main() {
	type Header struct {
		PageID        uint64
		NextPageID    uint64
		PrevPageID    uint64
		Checksum      uint64
		HeaderVersion uint16
		PageType      uint16
		Reserved      [28]byte
	}

	size := unsafe.Sizeof(Header{})
	fmt.Println(size)
}
