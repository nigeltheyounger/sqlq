# sqlq

`sqlq` is a small, durable job queue for Go backed by SQLite. It is intended
for applications that need background work without operating a separate queue
service.

The current v0.1 scope is deliberately narrow:

- named queues
- scheduled jobs
- integer priorities
- atomic claims
- expiring worker leases
- retry limits
- explicit success and failure states
- a pure-Go SQLite driver

## Requirements

- Go 1.25 or newer

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/nigeltheyounger/sqlq"
)

func main() {
	ctx := context.Background()

	queue, err := sqlq.Open(ctx, "jobs.db")
	if err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	_, err = queue.Enqueue(ctx, []byte(`{"report_id":42}`), sqlq.EnqueueOptions{
		Queue:       "reports",
		MaxAttempts: 3,
	})
	if err != nil {
		log.Fatal(err)
	}

	job, err := queue.Claim(ctx, "reports", "worker-1", 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if job == nil {
		return // no job is currently ready
	}

	if processErr := process(job.Payload); processErr != nil {
		// Retry marks the job failed when MaxAttempts has been reached.
		if err := queue.Retry(ctx, job, time.Minute, processErr); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := queue.Ack(ctx, job); err != nil {
		log.Fatal(err)
	}
}

func process(payload []byte) error {
	return nil
}
```

Run the included demo:

```sh
go run ./cmd/sqlq-demo -payload "send weekly report"
```

It creates `sqlq-demo.db` in the current directory, enqueues one job, claims
it, and acknowledges it.

## Processing model

`sqlq` provides **at-least-once delivery**. A worker claims a job for a fixed
lease period. If it crashes, another worker can reclaim the job after that
lease expires. Job handlers should therefore be idempotent.

Claims are ordered by:

1. highest priority
2. earliest scheduled time
3. lowest job ID

Every claim increments `Attempts`. A worker may then:

- call `Ack` after successful processing;
- call `Retry` with a delay after a temporary failure;
- call `Fail` after a permanent failure; or
- call `Extend` before a long-running lease expires.

All worker mutations require the live lease token returned by `Claim`. An
expired or superseded lease returns `sqlq.ErrLeaseLost`.

## Opening a queue

`Open` is the normal entry point. It owns the underlying database connection,
enables WAL mode for file-backed databases, applies a busy timeout to every
connection, and creates the schema.

Use `New` when an application already owns a configured `*sql.DB`. A queue
created with `New` does not close that database.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
```

The tests cover the full lifecycle, scheduling and priority, named queues,
retry exhaustion, lease expiry, persistence, validation, and concurrent
claims.

## Current boundaries

This package does not yet provide a polling worker loop, metrics, retention or
purging APIs, schema upgrades, or distributed coordination beyond SQLite's
locking. Keep the database on a filesystem with reliable SQLite file locking;
network filesystems are generally a poor fit.

Those capabilities can be added incrementally once the core API has real usage
and measured requirements.
