package bolt

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

var _zero uintptr

// TODO remove as soon as Go has syscall.Mincore
//
// TODO is this really guaranteed to be same across architectures, or
// were we just lucky?
func mincore(b []byte, vec []byte) (err error) {
	var _p0 unsafe.Pointer
	if len(b) > 0 {
		_p0 = unsafe.Pointer(&b[0])
	} else {
		_p0 = unsafe.Pointer(&_zero)
	}
	var _p1 unsafe.Pointer
	if len(vec) > 0 {
		_p1 = unsafe.Pointer(&vec[0])
	} else {
		_p1 = unsafe.Pointer(&_zero)
	}
	_, _, e1 := syscall.Syscall6(syscall.SYS_MINCORE, uintptr(_p0), uintptr(len(b)), uintptr(_p1), uintptr(len(vec)), 0, 0)
	if e1 != 0 {
		err = e1
	}
	return
}

// writeAvoidingCache writes p to w while trying to minimize the
// affects on the caching of the working set of p.
func writeAvoidingCache(p []byte, w io.Writer) (n int, err error) {
	// If one day Linux supports POSIX_FADV_NOREUSE for posix_fadvise,
	// we could avoid the mincore+madvise dance. Including deployment
	// delays, that day isn't realistically even close.
	//
	// Until then, we live with a benign race where we might cause a
	// cache drop for a page that gets cached concurrently while we're
	// doing a write. No data is lost.

	var kernelPageSize = syscall.Getpagesize()

	// Max number of kernel pages to process per iteration. This
	// affects the size of the writes and the inMemory buffer, nothing
	// else. Must be small enough that maxPages * kernelPageSize does
	// not overflow int.
	const maxPages = 4096
	// What pages were memory resident before we started the write.
	var inMemory [maxPages]byte
	var inMemoryValid bool

	var chunk []byte
	var written int

	if offset := int(uintptr(unsafe.Pointer(&p[0])) & uintptr(kernelPageSize-1)); offset != 0 {
		// write first partial page
		n, err = w.Write(p[:kernelPageSize-offset])
		p = p[n:]
	}

	for err == nil && len(p) > 0 {
		// p is now guaranteed to contain aligned to kernel pages,
		// with a potential partial last page
		chunk = p

		// limit number of kernel pages per iteration
		if maxLen := maxPages * kernelPageSize; len(chunk) > maxLen {
			chunk = chunk[:maxLen]
		}

		// record which pages of the chunk are in cached in memory currently
		inMemoryValid = true
		if err := mincore(chunk, inMemory[:]); err != nil {
			// mincore errors are not fatal, just carry on.
			// maybe put metrics here?
			inMemoryValid = false
		}

		written, err = w.Write(chunk)
		n += written

		if inMemoryValid {
			// drop those pages from cache which weren't there when we started the write

			// round up to cover final partial page
			pages := (len(chunk) + (kernelPageSize - 1)) / kernelPageSize
			for start := 0; start < pages; start++ {
				if inMemory[start]&1 == 0 {
					// combine pages into a single syscall where possible
					end := start
					for end+1 < pages && inMemory[end+1]&1 == 0 {
						end++
					}
					// Note: Linux source comments say MADV_DONTNEED
					// discards dirty data.
					//
					//     https://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/tree/mm/madvise.c?id=refs/tags/v3.15-rc7#n254
					//     https://sourceware.org/bugzilla/show_bug.cgi?id=3458
					//     https://groups.google.com/d/topic/fa.linux.kernel/87LMU7I4tqo/discussion
					//     https://groups.google.com/d/topic/fa.linux.kernel/-NPoCvqJkCk/discussion
					//
					// In Bolt's case, there is no dirty data, so this should be safe.
					// Just remember, Linux madvise(2) is not posix_madvise(3).
					if err := syscall.Madvise(chunk[start*kernelPageSize:end*kernelPageSize], syscall.MADV_DONTNEED); err != nil {
						// madvise errors are not really fatal, just lead to worse performance.
						// maybe put metrics here?
					}
					// skip ahead to where the next possible hit is
					start = end + 1
				}
			}
		}

		p = p[written:]
	}

	return n, err
}
