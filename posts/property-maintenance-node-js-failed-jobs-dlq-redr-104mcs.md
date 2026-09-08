# Property Maintenance: Node.js Failed Jobs, DLQ Redrive, and Idempotent Webhooks

**Short answer:** For a rate-limited property-management worker pool, use at-least-once delivery with an idempotent business effect: retry temporary failures with bounded backoff, quarantine exhausted or invalid work in a dead-letter queue, and redrive the same job identity only after review.

I've been paged by missed jobs, and I got burned by duplicate deliveries. The useful incident lesson wasn't that either queue behavior was surprising. It was that the consumer had treated receipt, business commit, and acknowledgement as one fuzzy event. During a drain of maintenance webhook work, a process can stop after committing a dispatch but before its acknowledgement reaches the broker. The next worker then sees the job again. If the handler cannot recognize the original job ID, a routine recovery becomes a second contractor visit or a repeated tenant message. **The queue may deliver twice; the lease update, vendor dispatch, or tenant notification must happen once.**

That crash window is the invariant. Design for it first.

## How should a Node.js queue retry failed webhook jobs and redrive a dead letter queue?

Separate delivery state from business state. A worker reserves one job, validates it, attempts the effect under a stable idempotency key, and acknowledges only after the effect and the key are durably committed together. A retry retains the same key. A redrive retains it too.

The decision tree is small:

1. If the payload is invalid, dead-letter it with a reason that is safe to retain.
2. If a dependency is temporarily unavailable or rate-limiting the pool, schedule a later attempt and release worker capacity.
3. If the effect commits, acknowledge the delivery.
4. If the attempt budget is exhausted, dead-letter the original job rather than looping in the hot queue.

Don't ack before the transaction. Also don't hold every delivery open while sleeping through backoff; that consumes a scarce worker slot and makes a rate limit look like a capacity shortage. The retry scheduler can publish a later attempt, but it must preserve the job ID and record that scheduling decision durably enough to avoid silently losing the task between those two actions.

RabbitMQ documents manual consumer acknowledgements and explains that unacknowledged deliveries are automatically requeued when a channel or connection closes. That behavior protects against loss, but it also makes redelivery normal. An idempotent consumer is therefore the business-side complement to broker acknowledgements, not an optional optimization.

## Tenant data, deduplication, and durable effects

For a property-management example, consider a job with ID `maintenance-req-4817` that changes a repair request from `approved` to `dispatched`. The idempotency record and status transition belong in one database transaction. If the worker stops after commit and the delivery returns, the unique job ID turns the second execution into a successful no-op; the consumer can acknowledge it without contacting the contractor again.

The placement matters. A cache entry written before the database update can suppress work that never committed. A marker written after the update leaves a gap where a duplicate can repeat the effect. Putting both writes in one transaction closes those two application-level gaps. External side effects are harder: when the downstream webhook accepts an idempotency key, send the stable job ID; when it doesn't, store an outbox record transactionally and let a dedicated dispatcher own delivery status. I'm not sure every third-party contractor system will honor such a key, so that capability has to be verified during integration rather than assumed (and recorded in the service inventory).

Keep the broker adapter narrow. Although the reader's service may be Node.js, the recovery contract is language-independent; this Go example makes the ordering explicit without binding it to a vendor SDK. This is boring — deliberately so — because recovery code should reveal its ordering at a glance.

```go
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrTemporary = errors.New("temporary dependency failure")
var ErrInvalid = errors.New("invalid job")

type Job struct {
	ID       string
	Attempt  int
	RequestID string
}

type Queue interface {
	Receive(context.Context) (Job, error)
	Ack(context.Context, Job) error
	Retry(context.Context, Job, time.Duration) error
	DeadLetter(context.Context, Job, string) error
}

type Effects interface {
	// ApplyOnce commits the job ID and business mutation in one transaction.
	ApplyOnce(context.Context, Job) error
}

type Policy struct {
	MaxAttempts int
	Backoff     func(attempt int) time.Duration
}

func ProcessOne(ctx context.Context, q Queue, effects Effects, policy Policy) error {
	job, err := q.Receive(ctx)
	if err != nil {
		return fmt.Errorf("receive: %w", err)
	}

	err = effects.ApplyOnce(ctx, job)
	switch {
	case err == nil:
		return q.Ack(ctx, job)
	case errors.Is(err, ErrInvalid):
		return q.DeadLetter(ctx, job, "invalid payload")
	case errors.Is(err, ErrTemporary) && job.Attempt+1 < policy.MaxAttempts:
		return q.Retry(ctx, job, policy.Backoff(job.Attempt+1))
	default:
		return q.DeadLetter(ctx, job, "attempt budget exhausted")
	}
}
```

The adapter's `Retry` operation must not acknowledge the current delivery until the delayed replacement is durably accepted. Its `DeadLetter` operation needs the same transfer guarantee. Some brokers provide native dead-lettering; another implementation may use an outbox-backed publisher. The interface expresses the guarantee the application needs without pretending every transport has identical commands.

## Drain the pool without creating a retry storm

Delivery guarantees are inseparable from admission control. Give the active queue a fixed concurrency budget, then reduce that budget when the downstream system applies rate pressure. Add randomized exponential backoff, a maximum attempt count, and a ceiling on retry delay. The numbers are deployment settings, not universal constants; derive them from the downstream contract and the maximum acceptable age of a maintenance request.

Fairness deserves an explicit decision. Emergency access jobs should not sit behind a large batch of routine inspection notices, but strict priority can starve ordinary work. Separate lanes with reserved worker capacity are easier to reason about than one priority number that operations cannot explain during an incident. Watch queue age per lane, not just total depth.

No tight loops.

For observability, attach the stable job ID, attempt number, original enqueue time, queue age, outcome class, and redrive batch ID to structured events. Alerting should distinguish a growing active backlog from new DLQ entries: the first points to capacity or downstream pressure, while the second points to jobs that automatic policy has stopped handling. Avoid payloads in routine logs when they contain tenant or property details.

Redrive is an operator action, not another retry timer. The runbook should require a reason classification, confirmation that the underlying condition has changed, a bounded batch size, and a way to pause. Reusing the original ID is non-negotiable. Minting a new ID defeats the duplicate guard exactly when historical work is being replayed.

## Test each crash window before deployment

The happy-path unit test is weak evidence. Inject a stop immediately after the database commit but before `Ack`, deliver the same job again, and assert that the business row changes once while both processing attempts finish safely. Then inject a failure after retry publication but before acknowledgement and confirm that duplicate scheduled attempts still converge on one effect.

Test malformed payloads, exhausted attempts, worker restarts, lost connections, and a DLQ redrive mixed with fresh traffic. For the rate-limited pool, run a load test in which the downstream allowance drops while the input rate stays constant. The expected result isn't zero backlog; it is bounded concurrency, increasing queue age that remains visible, no busy retry loop, and recovery without duplicate effects after the allowance returns.

This is also where broker semantics need to be checked against the implementation. Consumer acknowledgement documentation describes transport behavior, while task frameworks such as Celery offer a higher-level worker model. Neither choice removes the need to test the application's transaction boundary. Your mileage may vary on shutdown timing and retry primitives, so verify the actual adapter with process termination tests rather than mocks alone.

## Migration beyond a work queue: history becomes the product

The catch is that an ack-and-delete work queue is not a retained event history. It is **not suitable when multiple independent consumers must replay the full maintenance stream**, when a job is a long-running workflow with compensation across several steps, or when an audit requires reconstructing state from an immutable log. Use a retained event log for independent replay, and use a workflow engine when timers, joins, and compensating actions are part of the domain rather than incidental worker code.

Stick with a simple work queue when each task has one bounded owner, retries can be classified, the side effect has a real idempotency boundary, and operators can inspect and redrive quarantined work. The delivery contract then stays modest and honest: transport recovery may repeat execution, but application design prevents repeated business outcomes.

## Sources

- https://www.rabbitmq.com/docs/confirms
- https://docs.celeryq.dev/en/stable/getting-started/introduction.html
