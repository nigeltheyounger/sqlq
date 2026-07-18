// Package sqlq implements a small, durable job queue backed by SQLite.
package sqlq

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

var (
	ErrNotFound = errors.New("sqlq: job not found")
	ErrLeaseLost = errors.New("sqlq: job lease lost")
)

const defaultQueue = "default"
type Queue struct {
	db  *sql.DB
	now func() time.Time
}
func New(ctx context.Context, db *sql.DB) (*Queue, error) {
	if db == nil {
		return nil, errors.New("sqlq: nil database")
	}
	q := &Queue{db: db, now: time.Now}
	if err := q.Init(ctx); err != nil {
		return nil, err
	}
	return q, nil
}
// Init configures SQLite and creates the queue tables.
func (q *Queue) Init(ctx context.Context) error {
	if q == nil || q.db == nil {
		return errors.New("sqlq: nil queue")
	}
	if _, err := q.db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
		return fmt.Errorf("sqlq: enable WAL: %w", err)
	}
	if _, err := q.db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		return fmt.Errorf("sqlq: set busy timeout: %w", err)
	}
	_, err := q.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS sqlq_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue TEXT NOT NULL,
  payload BLOB NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('ready', 'running', 'done')) DEFAULT 'ready',
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 25,
  run_at INTEGER NOT NULL,
  lease_until INTEGER,
  lease_token TEXT,
  last_error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS sqlq_ready_jobs
  ON sqlq_jobs(queue, state, run_at, priority DESC, id);
CREATE INDEX IF NOT EXISTS sqlq_expired_leases
  ON sqlq_jobs(state, lease_until);
`)
	if err != nil {
		return fmt.Errorf("sqlq: create schema: %w", err)
	}
	return nil
}
type EnqueueOptions struct {
	Queue       string
	Priority    int
	RunAt       time.Time
	MaxAttempts int
}
type Job struct {
	ID          int64
	Queue       string
	Payload     []byte
	Priority    int
	Attempts    int
	MaxAttempts int
	RunAt       time.Time
	LeaseUntil  time.Time
	LeaseToken  string
	LastError   string
	CreatedAt   time.Time
}
func (q *Queue) Enqueue(ctx context.Context, payload []byte, options EnqueueOptions) (int64, error) {
	if q == nil || q.db == nil {
		return 0, errors.New("sqlq: nil queue")
	}
	now := q.now().UTC()
	queue := options.Queue
	if queue == "" {
		queue = defaultQueue
	}
	runAt := options.RunAt
	if runAt.IsZero() {
		runAt = now
	}
	maxAttempts := options.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 25
	}
	if maxAttempts < 1 {
		return 0, errors.New("sqlq: max attempts must be positive")
	}
	result, err := q.db.ExecContext(ctx, `INSERT INTO sqlq_jobs
 (queue, payload, priority, max_attempts, run_at, created_at, updated_at)
 VALUES (?, ?, ?, ?, ?, ?, ?)`, queue, append([]byte(nil), payload...), options.Priority,
		maxAttempts, unixNano(runAt), unixNano(now), unixNano(now))
	if err != nil {
		return 0, fmt.Errorf("sqlq: enqueue: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("sqlq: enqueue id: %w", err)
	}
	return id, nil
}

func (q *Queue) Claim(ctx context.Context, queue, worker string, lease time.Duration) (*Job, error) {
	if q == nil || q.db == nil {
		return nil, errors.New("sqlq: nil queue")
	}
	if worker == "" {
		return nil, errors.New("sqlq: worker is required")
	}
	if lease <= 0 {
		return nil, errors.New("sqlq: lease must be positive")
	}
	if queue == "" {
		queue = defaultQueue
	}
	now := q.now().UTC()
	until := now.Add(lease)
	
	token, err := leaseToken(worker)
	if err != nil {
		return nil, err
	}
	row := q.db.QueryRowContext(ctx, `
UPDATE sqlq_jobs
SET state = 'running', attempts = attempts + 1, lease_until = ?, lease_token = ?, updated_at = ?
WHERE id = (
  SELECT id FROM sqlq_jobs
  WHERE queue = ?
    AND attempts < max_attempts
    AND ((state = 'ready' AND run_at <= ?) OR (state = 'running' AND lease_until <= ?))
  ORDER BY priority DESC, run_at, id
  LIMIT 1
)
AND attempts < max_attempts
AND ((state = 'ready' AND run_at <= ?) OR (state = 'running' AND lease_until <= ?))
RETURNING id, queue, payload, priority, attempts, max_attempts, run_at, lease_until, lease_token, last_error, created_at`,
		unixNano(until), token, unixNano(now), queue, unixNano(now), unixNano(now), unixNano(now), unixNano(now))
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlq: claim: %w", err)
	}
	return job, nil
}
func (q *Queue) Ack(ctx context.Context, job *Job) error {
	return q.finish(ctx, job, "done", "", time.Time{})
}
func (q *Queue) Retry(ctx context.Context, job *Job, delay time.Duration, cause error) error {
	if delay < 0 {
		return errors.New("sqlq: retry delay cannot be negative")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return q.finish(ctx, job, "ready", message, q.now().Add(delay))
}
func (q *Queue) Extend(ctx context.Context, job *Job, lease time.Duration) error {
	if job == nil || job.ID == 0 || job.LeaseToken == "" {
		return ErrLeaseLost
	}
	if lease <= 0 {
		return errors.New("sqlq: lease must be positive")
	}
	now := q.now().UTC()
	until := now.Add(lease)
	result, err := q.db.ExecContext(ctx, `UPDATE sqlq_jobs SET lease_until = ?, updated_at = ?
WHERE id = ? AND state = 'running' AND lease_token = ?`, unixNano(until), unixNano(now), job.ID, job.LeaseToken)
	if err != nil {
		return fmt.Errorf("sqlq: extend lease: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlq: extend lease result: %w", err)
	}
	if n != 1 {
		return ErrLeaseLost
	}
	job.LeaseUntil = until
	return nil
}

func (q *Queue) finish(ctx context.Context, job *Job, state, lastError string, runAt time.Time) error {
	if q == nil || q.db == nil {
		return errors.New("sqlq: nil queue")
	}
	if job == nil || job.ID == 0 || job.LeaseToken == "" {
		return ErrLeaseLost
	}
	now := q.now().UTC()
	var result sql.Result
	var err error
	if state == "done" {
		result, err = q.db.ExecContext(ctx, `UPDATE sqlq_jobs SET state = 'done', lease_until = NULL,
lease_token = NULL, updated_at = ? WHERE id = ? AND state = 'running' AND lease_token = ?`, unixNano(now), job.ID, job.LeaseToken)
	} else {
		result, err = q.db.ExecContext(ctx, `UPDATE sqlq_jobs SET state = 'ready', run_at = ?, lease_until = NULL,
lease_token = NULL, last_error = ?, updated_at = ? WHERE id = ? AND state = 'running' AND lease_token = ?`, unixNano(runAt), lastError, unixNano(now), job.ID, job.LeaseToken)
	}
	if err != nil {
		return fmt.Errorf("sqlq: finish job: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlq: finish job result: %w", err)
	}
	if n != 1 {
		return ErrLeaseLost
	}
	return nil
}

type rowScanner interface{ Scan(...any) error }

func scanJob(row rowScanner) (*Job, error) {
	var job Job
	var runAt, leaseUntil, createdAt int64
	if err := row.Scan(&job.ID, &job.Queue, &job.Payload, &job.Priority, &job.Attempts, &job.MaxAttempts,
		&runAt, &leaseUntil, &job.LeaseToken, &job.LastError, &createdAt); err != nil {
		return nil, err
	}
	job.Payload = append([]byte(nil), job.Payload...)
	job.RunAt = fromUnixNano(runAt)
	job.LeaseUntil = fromUnixNano(leaseUntil)
	job.CreatedAt = fromUnixNano(createdAt)
	return &job, nil
}

func unixNano(t time.Time) int64 { return t.UTC().UnixNano() }
func fromUnixNano(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(0, v).UTC()
}

func leaseToken(worker string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("sqlq: generate lease token: %w", err)
	}
	return worker + ":" + hex.EncodeToString(b), nil
}
