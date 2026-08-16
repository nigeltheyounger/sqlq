package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	_ "modernc.org/sqlite"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS sqlq_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue TEXT NOT NULL,
  payload BLOB NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('ready', 'running', 'succeeded', 'failed')) DEFAULT 'ready',
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts INTEGER NOT NULL DEFAULT 25 CHECK (max_attempts > 0),
  run_at INTEGER NOT NULL,
  lease_until INTEGER,
  lease_token TEXT,
  last_error TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  finished_at INTEGER
);

CREATE INDEX IF NOT EXISTS sqlq_ready_jobs
  ON sqlq_jobs(queue, priority DESC, run_at, id)
  WHERE state = 'ready';

CREATE INDEX IF NOT EXISTS sqlq_running_jobs
  ON sqlq_jobs(queue, lease_until, id)
  WHERE state = 'running';
`

var memoryDatabaseID atomic.Uint64

// Open opens a SQLite database at path, initializes the queue schema, and
// configures every connection with a busy timeout. File-backed databases also
// use WAL mode. The returned Queue owns the database and must be closed.
func Open(ctx context.Context, path string) (*Queue, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlq: database path is required")
	}

	dsn := sqliteDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlq: open database: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlq: connect to database: %w", err)
	}

	q, err := New(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	q.ownsDB = true
	return q, nil
}

func sqliteDSN(path string) string {
	values := make(url.Values)
	values.Add("_pragma", "busy_timeout(5000)")

	var databaseURL url.URL
	if path == ":memory:" {
		databaseURL = url.URL{
			Scheme: "file",
			Opaque: "sqlq-memory-" + strconv.FormatUint(memoryDatabaseID.Add(1), 10),
		}
		values.Set("mode", "memory")
		values.Set("cache", "shared")
	} else {
		databaseURL = url.URL{Scheme: "file", Path: path}
		values.Add("_pragma", "journal_mode(WAL)")
	}
	databaseURL.RawQuery = values.Encode()
	return databaseURL.String()
}
