package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/nigeltheyounger/sqlq"
)

func main() {
	databasePath := flag.String("db", "sqlq-demo.db", "path to the SQLite database")
	payload := flag.String("payload", "hello from sqlq", "job payload")
	flag.Parse()

	ctx := context.Background()
	queue, err := sqlq.Open(ctx, *databasePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := queue.Close(); err != nil {
			log.Printf("close queue: %v", err)
		}
	}()

	id, err := queue.Enqueue(ctx, []byte(*payload), sqlq.EnqueueOptions{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("enqueued job %d\n", id)

	job, err := queue.Claim(ctx, "", "demo-worker", 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if job == nil {
		fmt.Println("no job ready")
		return
	}
	fmt.Printf("claimed job %d: %s\n", job.ID, job.Payload)

	if err := queue.Ack(ctx, job); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("acknowledged job %d\n", job.ID)
}
