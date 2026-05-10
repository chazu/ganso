package honker

import (
	"fmt"
	"strings"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const defaultPragmas = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
PRAGMA cache_size = -32000;
PRAGMA temp_store = MEMORY;
PRAGMA wal_autocheckpoint = 10000;
`

const bootstrapSQL = `
CREATE TABLE IF NOT EXISTS _honker_notifications (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    channel  TEXT    NOT NULL,
    payload  TEXT    NOT NULL DEFAULT '',
    created_at TEXT  NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS _honker_notifications_channel_id
    ON _honker_notifications (channel, id);

CREATE TABLE IF NOT EXISTS _honker_live (
    id               TEXT    PRIMARY KEY,
    queue            TEXT    NOT NULL,
    payload          TEXT    NOT NULL DEFAULT '',
    state            TEXT    NOT NULL DEFAULT 'pending',
    priority         INTEGER NOT NULL DEFAULT 0,
    run_at           TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    worker_id        TEXT,
    claim_expires_at TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    created_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    expires_at       TEXT
);
CREATE INDEX IF NOT EXISTS _honker_live_claim
    ON _honker_live (queue, priority DESC, run_at, id)
    WHERE state IN ('pending','processing');
CREATE INDEX IF NOT EXISTS _honker_live_pending_deadline
    ON _honker_live (queue, run_at)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS _honker_live_processing_deadline
    ON _honker_live (queue, claim_expires_at)
    WHERE state = 'processing';

CREATE TABLE IF NOT EXISTS _honker_dead (
    id           TEXT    PRIMARY KEY,
    queue        TEXT    NOT NULL,
    payload      TEXT    NOT NULL DEFAULT '',
    priority     INTEGER NOT NULL DEFAULT 0,
    run_at       TEXT    NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    last_error   TEXT    NOT NULL DEFAULT '',
    created_at   TEXT    NOT NULL,
    died_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS _honker_locks (
    name       TEXT PRIMARY KEY,
    owner      TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _honker_rate_limits (
    name         TEXT    NOT NULL,
    window_start TEXT    NOT NULL,
    count        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (name, window_start)
);

CREATE TABLE IF NOT EXISTS _honker_scheduler_tasks (
    name        TEXT PRIMARY KEY,
    queue       TEXT    NOT NULL,
    cron_expr   TEXT    NOT NULL,
    payload     TEXT    NOT NULL DEFAULT '',
    priority    INTEGER NOT NULL DEFAULT 0,
    expires_s   INTEGER NOT NULL DEFAULT 0,
    next_fire_at TEXT   NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS _honker_results (
    job_id     TEXT PRIMARY KEY,
    value      TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS _honker_stream (
    "offset"   INTEGER PRIMARY KEY AUTOINCREMENT,
    topic      TEXT    NOT NULL,
    key        TEXT    NOT NULL DEFAULT '',
    payload    TEXT    NOT NULL DEFAULT '',
    created_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS _honker_stream_topic_offset
    ON _honker_stream (topic, "offset");

CREATE TABLE IF NOT EXISTS _honker_stream_consumers (
    name   TEXT    NOT NULL,
    topic  TEXT    NOT NULL,
    "offset" INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (name, topic)
);

CREATE VIEW IF NOT EXISTS _honker_jobs AS
    SELECT id, queue, payload, state, priority, run_at, worker_id,
           claim_expires_at, attempts, max_attempts, '' AS last_error,
           created_at, NULL AS died_at
    FROM _honker_live
    UNION ALL
    SELECT id, queue, payload, 'dead' AS state, priority, run_at, NULL AS worker_id,
           NULL AS claim_expires_at, attempts, max_attempts, last_error,
           created_at, died_at
    FROM _honker_dead;
`

// applyPragmas executes each PRAGMA statement individually outside of any
// transaction. PRAGMAs like journal_mode and synchronous cannot be changed
// inside a transaction, so we must not use ExecScript (which wraps in a
// SAVEPOINT).
func applyPragmas(conn *sqlite.Conn) error {
	for _, line := range strings.Split(defaultPragmas, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if err := sqlitex.ExecuteTransient(conn, line, nil); err != nil {
			return fmt.Errorf("pragma %q: %w", line, err)
		}
	}
	return nil
}

// bootstrapSchema applies pragmas and creates all tables/indexes on a connection.
func bootstrapSchema(conn *sqlite.Conn) error {
	if err := applyPragmas(conn); err != nil {
		return err
	}
	return sqlitex.ExecScript(conn, bootstrapSQL)
}
