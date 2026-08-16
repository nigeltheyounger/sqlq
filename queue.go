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
	// ErrNotFound is returned when a requested job does not exist.
	ErrNotFound = errors.New("sqlq: job not found")
	// ErrLeaseLost is returned when a worker no longer owns a live job lease.
	ErrLeaseLost = errors.New("sqlq: job lease lost")
)

const (
	// DefaultQueue is used when no queue name is supplied.
	DefaultQueue = "default"
	// DefaultMaxAttempts is used when EnqueueOptions.MaxAttempts is zero.
	DefaultMaxAttempts = 25
)

// State describes the lifecycle state of a Job.
type State string

const (
	StateReady     State = "ready"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
)

// Queue stores and leases jobs in a SQLite database.
type Queue struct {
	db     *sql.DB
	now    func() time.Time
	ownsDB bool
}

// New initializes a Queue using an existing SQLite database. The caller
// retains ownership of db and is responsible for closing it.
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

// Close closes a database opened by Open. It is a no-op for queues created
// with New because the caller owns that database.
func (q *Queue) Close() error {
	if q == nil || q.db == nil || !q.ownsDB {
		return nil
	}
	return q.db.Close()
}

// Init creates the queue tables and indexes if they do not already exist.
func (q *Queue) Init(ctx context.Context) error {
	if q == nil || q.db == nil {
		return errors.New("sqlq: nil queue")
	}
	if _, err := q.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("sqlq: initialize schema: %w", err)
	}
	return nil
}

// EnqueueOptions controls where and when a job runs.
type EnqueueOptions struct {
	Queue       string
	Priority    int
	RunAt       time.Time
	MaxAttempts int
}

// Job is a durable unit of work. A running job can only be acknowledged,
// retried, failed, or extended by the worker holding its current lease token.
type Job struct {
	ID          int64
	Queue       string
	Payload     []byte
	State       State
	Priority    int
	Attempts    int
	MaxAttempts int
	RunAt       time.Time
	LeaseUntil  time.Time
	LeaseToken  string
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	FinishedAt  time.Time
}

// Enqueue stores a ready job and returns its database ID.
func (q *Queue) Enqueue(ctx context.Context, payload []byte, options EnqueueOptions) (int64, error) {
	if q == nil || q.db == nil {
		return 0, errors.New("sqlq: nil queue")
	}

	now := q.now().UTC()
	queue := options.Queue
	if queue == "" {
		queue = DefaultQueue
	}
	runAt := options.RunAt
	if runAt.IsZero() {
		runAt = now
	}
	maxAttempts := options.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts < 1 {
		return 0, errors.New("sqlq: max attempts must be positive")
	}

	payloadCopy := append([]byte{}, payload...)
	result, err := q.db.ExecContext(ctx, `INSERT INTO sqlq_jobs
 (queue, payload, state, priority, max_attempts, run_at, created_at, updated_at)
 VALUES (?, ?, 'ready', ?, ?, ?, ?, ?)`, queue, payloadCopy, options.Priority,
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

// Claim atomically leases the next available job. Jobs are ordered by
// priority, scheduled time, and ID. It returns (nil, nil) when none is ready.
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
		queue = DefaultQueue
	}

	now := q.now().UTC()
	if err := q.expireExhausted(ctx, now); err != nil {
		return nil, err
	}

	token, err := leaseToken(worker)
	if err != nil {
		return nil, err
	}
	until := now.Add(lease)
	row := q.db.QueryRowContext(ctx, `
UPDATE sqlq_jobs
SET state = 'running', attempts = attempts + 1, lease_until = ?, lease_token = ?,
    finished_at = NULL, updated_at = ?
WHERE id = (
  SELECT id FROM sqlq_jobs
  WHERE queue = ?
    AND attempts < max_attempts
    AND ((state = 'ready' AND run_at <= ?)
      OR (state = 'running' AND lease_until <= ?))
  ORDER BY priority DESC, run_at, id
  LIMIT 1
)
AND attempts < max_attempts
AND ((state = 'ready' AND run_at <= ?)
  OR (state = 'running' AND lease_until <= ?))
RETURNING id, queue, payload, state, priority, attempts, max_attempts, run_at,
          lease_until, lease_token, last_error, created_at, updated_at, finished_at`,
		unixNano(until), token, unixNano(now), queue, unixNano(now), unixNano(now),
		unixNano(now), unixNano(now))

	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlq: claim: %w", err)
	}
	return job, nil
}

// Get returns a job by ID, including terminal jobs.
func (q *Queue) Get(ctx context.Context, id int64) (*Job, error) {
	if q == nil || q.db == nil {
		return nil, errors.New("sqlq: nil queue")
	}
	if id <= 0 {
		return nil, ErrNotFound
	}
	row := q.db.QueryRowContext(ctx, `SELECT id, queue, payload, state, priority, attempts,
max_attempts, run_at, lease_until, lease_token, last_error, created_at, updated_at, finished_at
FROM sqlq_jobs WHERE id = ?`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("sqlq: get job: %w", err)
	}
	return job, nil
}

// Ack marks a leased job as successfully completed.
func (q *Queue) Ack(ctx context.Context, job *Job) error {
	lastError := ""
	if job != nil {
		lastError = job.LastError
	}
	return q.finish(ctx, job, StateSucceeded, lastError)
}

// Retry releases a leased job and schedules another attempt after delay. If
// the job has exhausted MaxAttempts, Retry marks it as failed instead.
func (q *Queue) Retry(ctx context.Context, job *Job, delay time.Duration, cause error) error {
	if q == nil || q.db == nil {
		return errors.New("sqlq: nil queue")
	}
	if delay < 0 {
		return errors.New("sqlq: retry delay cannot be negative")
	}
	if !validLease(job) {
		return ErrLeaseLost
	}

	now := q.now().UTC()
	runAt := now.Add(delay)
	var state string
	var storedRunAt int64
	var finishedAt sql.NullInt64
	err := q.db.QueryRowContext(ctx, `UPDATE sqlq_jobs
SET state = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'ready' END,
    run_at = CASE WHEN attempts >= max_attempts THEN run_at ELSE ? END,
    lease_until = NULL, lease_token = NULL, last_error = ?, updated_at = ?,
    finished_at = CASE WHEN attempts >= max_attempts THEN ? ELSE NULL END
WHERE id = ? AND state = 'running' AND lease_token = ? AND lease_until > ?
RETURNING state, run_at, finished_at`,
		unixNano(runAt), errorMessage(cause), unixNano(now), unixNano(now),
		job.ID, job.LeaseToken, unixNano(now)).Scan(&state, &storedRunAt, &finishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("sqlq: retry job: %w", err)
	}

	job.State = State(state)
	job.RunAt = fromUnixNano(storedRunAt)
	job.LeaseUntil = time.Time{}
	job.LeaseToken = ""
	job.LastError = errorMessage(cause)
	job.UpdatedAt = now
	job.FinishedAt = fromNullUnixNano(finishedAt)
	return nil
}

// Fail marks a leased job as permanently failed without another attempt.
func (q *Queue) Fail(ctx context.Context, job *Job, cause error) error {
	return q.finish(ctx, job, StateFailed, errorMessage(cause))
}

// Extend replaces a live job lease with a new lease measured from now.
func (q *Queue) Extend(ctx context.Context, job *Job, lease time.Duration) error {
	if q == nil || q.db == nil {
		return errors.New("sqlq: nil queue")
	}
	if !validLease(job) {
		return ErrLeaseLost
	}
	if lease <= 0 {
		return errors.New("sqlq: lease must be positive")
	}

	now := q.now().UTC()
	until := now.Add(lease)
	result, err := q.db.ExecContext(ctx, `UPDATE sqlq_jobs
SET lease_until = ?, updated_at = ?
WHERE id = ? AND state = 'running' AND lease_token = ? AND lease_until > ?`,
		unixNano(until), unixNano(now), job.ID, job.LeaseToken, unixNano(now))
	if err != nil {
		return fmt.Errorf("sqlq: extend lease: %w", err)
	}
	if err := requireLease(result); err != nil {
		return err
	}
	job.LeaseUntil = until
	job.UpdatedAt = now
	return nil
}

func (q *Queue) finish(
	ctx context.Context,
	job *Job,
	state State,
	lastError string,
) error {
	if q == nil || q.db == nil {
		return errors.New("sqlq: nil queue")
	}
	if !validLease(job) {
		return ErrLeaseLost
	}

	now := q.now().UTC()
	result, err := q.db.ExecContext(ctx, `UPDATE sqlq_jobs
SET state = ?, lease_until = NULL, lease_token = NULL,
    last_error = ?, updated_at = ?, finished_at = ?
WHERE id = ? AND state = 'running' AND lease_token = ? AND lease_until > ?`,
		string(state), lastError, unixNano(now), unixNano(now),
		job.ID, job.LeaseToken, unixNano(now))
	if err != nil {
		return fmt.Errorf("sqlq: finish job: %w", err)
	}
	if err := requireLease(result); err != nil {
		return err
	}

	job.State = state
	job.LeaseUntil = time.Time{}
	job.LeaseToken = ""
	job.LastError = lastError
	job.UpdatedAt = now
	job.FinishedAt = now
	return nil
}

func (q *Queue) expireExhausted(ctx context.Context, now time.Time) error {
	_, err := q.db.ExecContext(ctx, `UPDATE sqlq_jobs
SET state = 'failed', lease_until = NULL, lease_token = NULL,
    last_error = CASE WHEN last_error = '' THEN 'maximum attempts exhausted' ELSE last_error END,
    updated_at = ?, finished_at = ?
WHERE attempts >= max_attempts
  AND (state = 'ready' OR (state = 'running' AND lease_until <= ?))`,
		unixNano(now), unixNano(now), unixNano(now))
	if err != nil {
		return fmt.Errorf("sqlq: expire exhausted jobs: %w", err)
	}
	return nil
}

func requireLease(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlq: inspect lease update: %w", err)
	}
	if n != 1 {
		return ErrLeaseLost
	}
	return nil
}

func validLease(job *Job) bool {
	return job != nil && job.ID > 0 && job.LeaseToken != ""
}

type rowScanner interface{ Scan(...any) error }

func scanJob(row rowScanner) (*Job, error) {
	var job Job
	var state string
	var runAt, createdAt, updatedAt int64
	var leaseUntil, finishedAt sql.NullInt64
	var leaseToken, lastError sql.NullString
	if err := row.Scan(&job.ID, &job.Queue, &job.Payload, &state, &job.Priority, &job.Attempts,
		&job.MaxAttempts, &runAt, &leaseUntil, &leaseToken, &lastError, &createdAt,
		&updatedAt, &finishedAt); err != nil {
		return nil, err
	}
	job.Payload = append([]byte(nil), job.Payload...)
	job.State = State(state)
	job.RunAt = fromUnixNano(runAt)
	job.LeaseUntil = fromNullUnixNano(leaseUntil)
	job.LeaseToken = leaseToken.String
	job.LastError = lastError.String
	job.CreatedAt = fromUnixNano(createdAt)
	job.UpdatedAt = fromUnixNano(updatedAt)
	job.FinishedAt = fromNullUnixNano(finishedAt)
	return &job, nil
}

func unixNano(t time.Time) int64 { return t.UTC().UnixNano() }

func fromUnixNano(v int64) time.Time {
	return time.Unix(0, v).UTC()
}

func fromNullUnixNano(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return fromUnixNano(v.Int64)
}

func leaseToken(worker string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("sqlq: generate lease token: %w", err)
	}
	return worker + ":" + hex.EncodeToString(b), nil
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
