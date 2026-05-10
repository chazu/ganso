//go:build darwin || linux || freebsd || openbsd || netbsd

package honker

import (
	"os"
	"syscall"
)

// fileIno extracts the inode number from os.FileInfo on unix systems.
func fileIno(info os.FileInfo) uint64 {
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		return sys.Ino
	}
	return 0
}
