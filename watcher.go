package ganso

import (
	"log"
	"os"
	"sync"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// UpdateWatcher polls PRAGMA data_version on a dedicated read-only connection
// to detect when any process commits to the database. On change it fans out a
// wake signal to all subscribers.
type UpdateWatcher struct {
	path         string
	pollInterval time.Duration
	stop         chan struct{}
	stopped      chan struct{}

	mu     sync.Mutex
	nextID uint64
	subs   map[uint64]chan struct{} // subscriber channels, cap=1
}

// NewUpdateWatcher creates and starts an UpdateWatcher. It opens its own
// read-only connection for PRAGMA polling.
func NewUpdateWatcher(db *Database) *UpdateWatcher {
	w := &UpdateWatcher{
		path:         db.path,
		pollInterval: db.pollInterval,
		stop:         make(chan struct{}),
		stopped:      make(chan struct{}),
		subs:         make(map[uint64]chan struct{}),
	}
	if w.pollInterval <= 0 {
		w.pollInterval = time.Millisecond
	}
	go w.run()
	return w
}

// Subscribe returns a channel that receives a signal whenever a database
// commit is detected, and an unsubscribe function to remove the subscription.
// The channel has capacity 1 so rapid commits are coalesced.
func (w *UpdateWatcher) Subscribe() (ch <-chan struct{}, unsubscribe func()) {
	c := make(chan struct{}, 1)

	w.mu.Lock()
	id := w.nextID
	w.nextID++
	w.subs[id] = c
	w.mu.Unlock()

	return c, func() {
		w.mu.Lock()
		delete(w.subs, id)
		w.mu.Unlock()
	}
}

// Stop signals the polling goroutine to exit and waits for it to finish.
func (w *UpdateWatcher) Stop() {
	select {
	case <-w.stop:
		// already stopped
		return
	default:
		close(w.stop)
	}
	<-w.stopped
}

// notifyAll does a non-blocking send to every subscriber channel.
// Capacity-1 channels naturally coalesce bursts of commits.
func (w *UpdateWatcher) notifyAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, ch := range w.subs {
		select {
		case ch <- struct{}{}:
		default: // already pending, coalesce
		}
	}
}

// fileIdentity holds enough information to detect if the database file has
// been replaced (e.g. by an atomic rename).
type fileIdentity struct {
	ino    uint64
	size   int64
	modNs  int64 // ModTime in UnixNano as fallback
}

func getFileIdentity(path string) (fileIdentity, error) {
	info, err := os.Stat(path)
	if err != nil {
		return fileIdentity{}, err
	}
	fid := fileIdentity{
		size:  info.Size(),
		modNs: info.ModTime().UnixNano(),
	}
	fid.ino = fileIno(info)
	return fid, nil
}

func (a fileIdentity) equal(b fileIdentity) bool {
	// If we have inode info, compare that (most reliable on unix).
	if a.ino != 0 && b.ino != 0 {
		return a.ino == b.ino
	}
	// Fallback: compare size + mod time.
	return a.size == b.size && a.modNs == b.modNs
}

// openWatcherConn opens a dedicated read-only connection for PRAGMA polling.
func openWatcherConn(path string) (*sqlite.Conn, error) {
	return sqlite.OpenConn(path,
		sqlite.OpenReadOnly|sqlite.OpenNoMutex|sqlite.OpenURI,
	)
}

// readDataVersion executes PRAGMA data_version and returns the result.
func readDataVersion(conn *sqlite.Conn) (int64, error) {
	var version int64
	err := sqlitex.Execute(conn, "PRAGMA data_version;", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			version = stmt.ColumnInt64(0)
			return nil
		},
	})
	return version, err
}

// run is the main polling loop, executed in its own goroutine.
func (w *UpdateWatcher) run() {
	defer close(w.stopped)

	conn, err := openWatcherConn(w.path)
	if err != nil {
		log.Printf("ganso: watcher: failed to open connection: %v", err)
		return
	}
	defer conn.Close()

	lastVersion, err := readDataVersion(conn)
	if err != nil {
		log.Printf("ganso: watcher: initial PRAGMA data_version failed: %v", err)
		return
	}

	initialFID, err := getFileIdentity(w.path)
	if err != nil {
		log.Printf("ganso: watcher: initial file identity check failed: %v", err)
		return
	}

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	var tickCount uint64

	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			tickCount++

			version, err := readDataVersion(conn)
			if err != nil {
				// Connection may be broken. Try to reconnect.
				log.Printf("ganso: watcher: PRAGMA data_version error: %v; reconnecting", err)
				conn.Close()

				// Conservative wake: something changed, we just can't tell what.
				w.notifyAll()

				conn, err = openWatcherConn(w.path)
				if err != nil {
					log.Printf("ganso: watcher: reconnect failed: %v", err)
					// Back off briefly before the next tick retries.
					continue
				}
				lastVersion, _ = readDataVersion(conn)
				continue
			}

			if version != lastVersion {
				lastVersion = version
				w.notifyAll()
			}

			// Dead-man's switch: every ~100 ticks, verify the database file
			// hasn't been replaced out from under us.
			if tickCount%100 == 0 {
				fid, err := getFileIdentity(w.path)
				if err != nil {
					// The database file is gone (store closed / temp dir removed).
					// Stop watching quietly rather than spamming the log every
					// 100 ticks or leaking the goroutine.
					return
				}
				if !initialFID.equal(fid) {
					// The file was replaced out from under us (e.g. a backup
					// restore). Stop watching this stale handle — do NOT
					// log.Fatalf, which would kill the entire host process.
					log.Printf("ganso: watcher: database file replaced; stopping watcher for %s", w.path)
					return
				}
			}
		}
	}
}
