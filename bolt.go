// +build !linux

package bolt

import (
	"io"
	"os"
)

func fdatasync(f *os.File) error {
	return f.Sync()
}

func writeAvoidingCache(p []byte, w io.Writer) (n int, err error) {
	// no optimization
	return w.Write(p)
}
