package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func testQueue(t *testing.T) *Queue {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	q, err := New(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestEnqueueClaimAck(t *testing.T) {
	q := testQueue(t)
	id, err := q.Enqueue(context.Background(), []byte("hello"), EnqueueOptions{Priority: 2})
	if err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(context.Background(), "", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.ID != id || string(job.Payload) != "hello" || job.Attempts != 1 {
		t.Fatalf("unexpected job: %#v", job)
	}
	if err := q.Ack(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	job, err = q.Claim(context.Background(), "", "worker-b", time.Minute)
	if err != nil || job != nil {
		t.Fatalf("claim after ack = %#v, %v", job, err)
	}
}

func TestRetryAndLeaseOwnership(t *testing.T) {
	q := testQueue(t)
	if _, err := q.Enqueue(context.Background(), []byte("x"), EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	job, err := q.Claim(context.Background(), "", "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Retry(context.Background(), job, 0, errors.New("temporary")); err != nil {
		t.Fatal(err)
	}
	again, err := q.Claim(context.Background(), "", "b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if again == nil || again.Attempts != 2 {
		t.Fatalf("unexpected retry: %#v", again)
	}
	if err := q.Ack(context.Background(), job); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old lease ack error = %v", err)
	}
}
