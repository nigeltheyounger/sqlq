package sqlq

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var testNow = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

func testQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := Open(context.Background(), filepath.Join(t.TempDir(), "queue file.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Errorf("close queue: %v", err)
		}
	})
	q.now = func() time.Time { return testNow }
	return q
}

func TestEnqueueClaimAckAndGet(t *testing.T) {
	q := testQueue(t)
	id, err := q.Enqueue(context.Background(), []byte("hello"), EnqueueOptions{Priority: 2})
	if err != nil {
		t.Fatal(err)
	}

	job, err := q.Claim(context.Background(), "", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.ID != id || string(job.Payload) != "hello" {
		t.Fatalf("unexpected job: %#v", job)
	}
	if job.State != StateRunning || job.Attempts != 1 || job.LeaseToken == "" {
		t.Fatalf("unexpected lease: %#v", job)
	}

	if err := q.Ack(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if job.State != StateSucceeded || job.FinishedAt.IsZero() || job.LeaseToken != "" {
		t.Fatalf("unexpected acknowledged job: %#v", job)
	}

	stored, err := q.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != StateSucceeded || !stored.FinishedAt.Equal(testNow) {
		t.Fatalf("unexpected stored job: %#v", stored)
	}
	if next, err := q.Claim(context.Background(), "", "worker-b", time.Minute); err != nil || next != nil {
		t.Fatalf("claim after ack = %#v, %v", next, err)
	}
}

func TestSchedulingPriorityAndQueues(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	lowID, err := q.Enqueue(ctx, []byte("low"), EnqueueOptions{Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	highID, err := q.Enqueue(ctx, []byte("high"), EnqueueOptions{
		Priority: 100,
		RunAt:    testNow.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ctx, []byte("email"), EnqueueOptions{Queue: "emails"}); err != nil {
		t.Fatal(err)
	}

	job, err := q.Claim(ctx, "", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.ID != lowID {
		t.Fatalf("first claim = %#v, want job %d", job, lowID)
	}
	if err := q.Ack(ctx, job); err != nil {
		t.Fatal(err)
	}
	if job, err = q.Claim(ctx, "", "worker", time.Minute); err != nil || job != nil {
		t.Fatalf("claim before scheduled time = %#v, %v", job, err)
	}

	q.now = func() time.Time { return testNow.Add(time.Hour) }
	job, err = q.Claim(ctx, "", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.ID != highID {
		t.Fatalf("scheduled claim = %#v, want job %d", job, highID)
	}
	if email, err := q.Claim(ctx, "emails", "email-worker", time.Minute); err != nil || email == nil {
		t.Fatalf("email claim = %#v, %v", email, err)
	}
}

func TestRetryAndMaxAttempts(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	id, err := q.Enqueue(ctx, []byte("work"), EnqueueOptions{MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}

	first, err := q.Claim(ctx, "", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first.MaxAttempts = 1 // The database, not mutable Job fields, decides exhaustion.
	if err := q.Retry(ctx, first, 0, errors.New("temporary")); err != nil {
		t.Fatal(err)
	}
	if first.State != StateReady || first.LastError != "temporary" {
		t.Fatalf("unexpected retried job: %#v", first)
	}

	second, err := q.Claim(ctx, "", "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.Attempts != 2 {
		t.Fatalf("unexpected second attempt: %#v", second)
	}
	second.MaxAttempts = 999
	if err := q.Retry(ctx, second, 0, errors.New("still failing")); err != nil {
		t.Fatal(err)
	}
	if second.State != StateFailed || second.LastError != "still failing" {
		t.Fatalf("unexpected exhausted job: %#v", second)
	}

	stored, err := q.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != StateFailed || stored.Attempts != 2 || stored.FinishedAt.IsZero() {
		t.Fatalf("unexpected stored failure: %#v", stored)
	}
	if job, err := q.Claim(ctx, "", "worker-c", time.Minute); err != nil || job != nil {
		t.Fatalf("claim after exhaustion = %#v, %v", job, err)
	}
}

func TestLeaseExpiryOwnershipAndExtension(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, []byte("work"), EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	old, err := q.Claim(ctx, "", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	q.now = func() time.Time { return testNow.Add(2 * time.Minute) }
	if err := q.Ack(ctx, old); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ack expired lease error = %v", err)
	}
	if err := q.Extend(ctx, old, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("extend expired lease error = %v", err)
	}

	current, err := q.Claim(ctx, "", "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if current == nil || current.Attempts != 2 || current.LeaseToken == old.LeaseToken {
		t.Fatalf("unexpected reclaimed job: %#v", current)
	}
	if err := q.Ack(ctx, old); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old worker ack error = %v", err)
	}
	if err := q.Extend(ctx, current, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if want := testNow.Add(7 * time.Minute); !current.LeaseUntil.Equal(want) {
		t.Fatalf("extended until %v, want %v", current.LeaseUntil, want)
	}
}

func TestFail(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	id, err := q.Enqueue(ctx, nil, EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(ctx, "", "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Fail(ctx, job, errors.New("invalid payload")); err != nil {
		t.Fatal(err)
	}
	stored, err := q.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != StateFailed || stored.LastError != "invalid payload" {
		t.Fatalf("unexpected failed job: %#v", stored)
	}
}

func TestConcurrentClaimOnlyLeasesOnce(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, []byte("once"), EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}

	const workers = 12
	start := make(chan struct{})
	results := make(chan *Job, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			<-start
			job, err := q.Claim(ctx, "", fmt.Sprintf("worker-%d", worker), time.Minute)
			if err != nil {
				errs <- err
				return
			}
			results <- job
		}(i)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Errorf("claim: %v", err)
	}
	claimed := 0
	for job := range results {
		if job != nil {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed %d times, want 1", claimed)
	}
}

func TestValidationAndNotFound(t *testing.T) {
	q := testQueue(t)
	ctx := context.Background()

	if _, err := q.Enqueue(ctx, nil, EnqueueOptions{MaxAttempts: -1}); err == nil {
		t.Fatal("expected max attempts error")
	}
	if _, err := q.Claim(ctx, "", "", time.Minute); err == nil {
		t.Fatal("expected worker error")
	}
	if _, err := q.Claim(ctx, "", "worker", 0); err == nil {
		t.Fatal("expected lease error")
	}
	if _, err := q.Get(ctx, 999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing job error = %v", err)
	}
	if _, err := Open(ctx, ""); err == nil {
		t.Fatal("expected empty path error")
	}
}

func TestOpenPersistsJobs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "persistent.db")
	q, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	id, err := q.Enqueue(ctx, []byte("persistent"), EnqueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	job, err := q.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(job.Payload) != "persistent" || job.State != StateReady {
		t.Fatalf("unexpected persisted job: %#v", job)
	}
}
