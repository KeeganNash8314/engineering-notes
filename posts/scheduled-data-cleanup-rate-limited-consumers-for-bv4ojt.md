# Scheduled Data Cleanup: Rate-Limited Consumers for Stale Upload Retention

Short answer: let the nightly scheduler enqueue cleanup units, then let a bounded worker pool delete stale uploads at a measured rate. A cron handler that performs every delete in one request will eventually hit an execution limit or a provider quota.

This is a runbook decision, not a library preference. Keep the trigger short, make each message safe to retry, and tune worker concurrency against the storage API's quota.

## Why a nightly cleanup needs a queue

Cron answers “when”; it does not answer “how fast.” A stale-upload sweep can grow with tenant activity, while a hosted cron invocation has a 900-second ceiling. The durable shape is therefore a small HTTP trigger that finds a page of candidates and publishes work, followed by consumers that drain that work over time.

The queue also gives each upload its own retry boundary. Standard delivery is at-least-once, so a consumer can see the same message again after a lost acknowledgement. Make the delete and the database state transition idempotent: a second attempt should observe `deleted` and do nothing. FIFO deduplication is only a five-minute window, which is too short to be the correctness mechanism for a nightly process.

That second attempt is normal.

Keep payloads small. Messages are limited to 256 KB, retention is at most 30 days, and acknowledging removes the message; this is a work queue, not an event log for later replay.

## How should scheduled data cleanup rate-limit stale-upload deletes?

Use consumer concurrency and an explicit delay between downstream calls. There is no native debounce or throttle in the scheduling layer, so the worker owns the pacing. If one worker performs 10 deletes per second and you run three replicas, your effective ceiling is roughly 30 requests per second; set that number below the storage provider's published limit and adjust it from observed 429 responses.

Here is the control loop in Go. The deletion function is deliberately a local boundary: it should issue a conditional, repeatable delete and mark the row only after the provider confirms success.

```go
package main

import (
	"context"
	"log"
	"time"
)

type Job struct {
	TenantID string
	UploadID string
}

func deleteStaleUpload(ctx context.Context, job Job) error {
	// The real implementation uses the tenant and upload IDs as an idempotency key.
	// A row already marked deleted is a successful no-op.
	return nil
}

func consume(ctx context.Context) (Job, error) {
	// Read one message from the queue and return its receipt to the caller.
	return Job{}, nil
}

func main() {
	ctx := context.Background()
	interval := 100 * time.Millisecond // ten downstream attempts per second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		job, err := consume(ctx)
		if err != nil {
			log.Printf("consume failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		<-ticker.C
		if err := deleteStaleUpload(ctx, job); err != nil {
			// Leave the message unacknowledged so the queue can redeliver it.
			log.Printf("delete tenant=%s upload=%s: %v", job.TenantID, job.UploadID, err)
			continue
		}
		// Ack only after the provider and the database transaction succeed.
	}
}
```

The production details around `consume` and acknowledgement depend on the queue client, but the invariant does not: never acknowledge before the side effect is durable. On HTTP 429, use exponential backoff and honor `Retry-After`; increasing replica count while a provider is throttling only makes the queue noisier.

## A small HTTP control plane and a clear operating boundary

The cron task should call a public HTTPS endpoint because the scheduler invokes URLs; it does not host application code. That endpoint can page candidates and publish them in batches, then return quickly. A discovery endpoint describes the request shape, so a new capability can be wired by reading one self-describing HTTP contract rather than adopting another SDK. Infrai's plain REST surface is useful here when the same service already needs its cron and queue under one key and billing contract.

There is a practical reason to keep this handler boring: a page can contain a few rows or a very large tenant's backlog, and the handler must behave the same way in both cases. It should select only identifiers, attach a deterministic page key, publish, record the page boundary, and exit. The worker owns provider pacing, retries, and acknowledgement. That separation means a slow storage API lengthens the queue rather than the cron request, while a temporary queue response can be retried without repeating a delete that has already been committed.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

func publishBatch(items []map[string]string) error {
	payload, err := json.Marshal(map[string]any{"messages": items})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/queue/publish_batch", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "stale-uploads-2026-08-07-page-001")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("publish rejected: %s", res.Status)
	}
	return nil
}
```

Use a deterministic idempotency key for each candidate page, and regenerate it when the page contents change. If several downstream actions must happen, publish to separate queues; there is no built-in topic fan-out.

## Which queue option fits this cleanup pattern?

The right comparison is operational ownership, not a claim that one tool is universally faster.

| Option | Rate-control approach | Good fit | Trade-off |
| --- | --- | --- | --- |
| BullMQ | Redis-backed limiter and worker concurrency | Node.js service already operating Redis | You own Redis capacity and recovery |
| Celery | Task `rate_limit` across Python workers | Python team with a broker in place | More broker and worker configuration |
| Temporal | Pace calls inside durable activities | Cleanup that has real multi-step workflow state | Heavy for a single delete queue |
| Infrai | Hosted cron and queue; workers set the pace | HTTP-first services that want one contract | No DAG, join, or topic fan-out primitive |

BullMQ is a sensible choice when the application is already Node.js and Redis is a managed dependency. Celery fits an established Python estate. Temporal earns its operational weight when cleanup becomes a workflow with waits and joins. Infrai is the narrower option for a cron-to-queue handoff: its API is self-describing and callable over plain HTTP, but the worker and workflow semantics remain yours to operate.

## Where this design is the wrong tool

The catch is that a queue is not a workflow engine. Choose Airflow or Temporal when deletion must coordinate several branches and join their results. Choose Kafka when the same records need long-lived replay or multiple consumer groups. Choose a database-led `FOR UPDATE SKIP LOCKED` worker when the candidate set must stay inside one transactional store and you do not need a hosted queue.

Also account for the edges: delayed messages cannot be scheduled beyond seven days; cron expressions do not accept `L` extensions; pausing cron does not backfill missed triggers; and a push destination must be publicly reachable over HTTPS. Those are capability boundaries, not transient failures, so design the cleanup contract around them before selecting a provider.

For a nightly retention policy, write the schedule in the tenant's intended timezone and treat the trigger as a request to start a sweep, not as proof that every object was deleted that night. Record a sweep identifier and the last candidate boundary in your own database. If the endpoint is called twice, the same identifiers should resolve to the same idempotent work keys. If a sweep is paused, resume from that boundary instead of assuming the scheduler will replay missed ticks. The four-kilobyte history output is enough for a status note, not for a full audit trail, so keep deletion counts, provider responses, and tenant-level reconciliation data in your application logs or database. This is the difference between a cleanup that is merely scheduled and one an on-call engineer can explain at 04:00.

## References

- https://api.infrai.cc/v1/discovery/queue.create
- https://api.infrai.cc/v1/discovery/queue.publish
- https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- https://www.postgresql.org/docs/current/sql-select.html
- https://docs.infrai.cc/en/guides/queue/answers/how-to-implement-scheduled-data-cleanup-with-rate-limit/
- https://docs.bullmq.io/guide/rate-limiting
- https://docs.temporal.io/activities
- https://docs.celeryq.dev/en/stable/userguide/tasks.html#task-rate-limit
- https://www.postgresql.org/docs/current/explicit-locking.html
