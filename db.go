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