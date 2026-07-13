package sqlq

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS jobs (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	queue        TEXT    NOT NULL DEFAULT 'default',
	payload      TEXT    NOT NULL,
	status       TEXT    NOT NULL DEFAULT 'pending', -- pending | claimed | done | dead
	attempts     INTEGER NOT NULL DEFAULT 0,
	max_attempts INTEGER NOT NULL DEFAULT 5,
	run_at       INTEGER NOT NULL,                   -- unix ms; claimable once now >= run_at
	claimed_at   INTEGER,
	worker_id    TEXT,
	error        TEXT,
	created_at   INTEGER NOT NULL,
	updated_at   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_claimable
	ON jobs (queue, status, run_at);

CREATE TABLE IF NOT EXISTS dead_letters (
	id       INTEGER PRIMARY KEY,
	queue    TEXT    NOT NULL,
	payload  TEXT    NOT NULL,
	attempts INTEGER NOT NULL,
	error    TEXT,
	died_at  INTEGER NOT NULL
);
`

func OpenDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return db, nil
}

