//go:build !(darwin || linux || freebsd || openbsd || netbsd)

package honker

import "os"

// fileIno returns 0 on non-unix platforms; the watcher falls back to
// size+modtime comparison for file identity.
func fileIno(info os.FileInfo) uint64 {
	return 0
}
