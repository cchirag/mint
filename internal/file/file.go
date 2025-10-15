// This package contains the implementation for all the file operations.
// File
package file

import "os"

// File is the basic struct that holds the reference to the actual file
type File struct {
	path string
	flag int
	perm os.FileMode
	file *os.File
}

func Open(path string, permission os.FileMode) (*File, error) {
	file, err := os.OpenFile(path, os.O_CREATE, permission)
	if err != nil {
		return nil, err
	}

	return &File{path: path, flag: os.O_CREATE, perm: permission, file: file}, nil
}

func (f *File) Close() error {
	return f.file.Close()
}
