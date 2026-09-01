# Node.js User Reminders: Unreachable Public HTTPS Webhook, 900-Second Cron Timeout

Short answer: treat a stale-reservation reminder as an at-least-once delivery problem. Make the public HTTPS endpoint do only authenticated admission, put one durable job on a queue, and let a worker expire the reservation through an idempotent state transition. The 900-second timeout is a reason to separate those steps, not a value to tune in Node.js.

The business rule is small: a support customer holds a reservation for a fixed window, and the system must expire it when the window closes. The failure is not small. A cron request can time out after the reminder was queued, a worker can die after changing the reservation, or a retry can send the same notice twice. A runbook that only asks “did cron call my webhook?” misses the useful question: can we prove what happened to this reservation?

Start there.

## Reservation records outlive the scheduler

Give each reservation a stable identity and a scheduled expiry timestamp. The reminder event should include the reservation ID, the expected version, and the expiry time; it should not contain a mutable copy of the entire customer record. The worker loads the current row and changes `held` to `expired` only when the version and time condition still match. If another action already released or extended the hold, the worker records `not_applicable` and acknowledges the job.

That conditional write is the first duplicate defense. A message ID is useful for tracing, but it is not the business idempotency key: a retry can have a new message ID. The reservation ID plus the intended transition identifies the side effect. Keep the reminder notification as a separate effect with its own delivery record, keyed by reservation ID and reminder kind. This prevents a second queue delivery from turning a successful expiration into a second email or webhook.

For example, the durable records should let an on-call answer these questions without reconstructing them from log lines:

- Which scheduled run admitted the job?
- Which worker attempt claimed it?
- Did the conditional expiration win, lose, or get skipped?
- Was the customer notification sent, pending, or permanently rejected?

Three words matter: admitted, applied, notified. They are different states. Returning HTTP 202 from a webhook can mean “admitted”; it cannot honestly mean “the customer has been notified.”

## How does a Node.js cron webhook survive a 900-second timeout?

Separate reachability from duration. A scheduler outside the private network needs a publicly resolvable HTTPS address and a certificate it can validate. A loopback address, private DNS name, or internal-only load balancer is not made reachable by increasing an HTTP client timeout. Check the route from an external network, then check the application log for an authenticated request and a bounded response.

The endpoint should validate the trigger, select only the reservation rows due for this run, publish bounded jobs, and return. It should not render a message, call a slow provider, or wait for the worker. A 900-second ceiling leaves plenty of time for a badly sized batch to keep the request open while it retries downstream calls. Page the selection and make the admission operation resumable. If the scheduler repeats the same window, the database uniqueness constraint or an equivalent idempotency store should make the second admission harmless.

I've been paged by missed jobs and duplicate deliveries; the useful distinction was always “could the scheduler connect?” versus “did the business effect commit?” Those are two alerts, two logs, and two owners during an incident. Don't collapse them into one webhook timeout metric.

The queue's delivery guarantee sets the worker design. RabbitMQ's acknowledgement documentation describes acknowledgements as the mechanism that tells the broker a delivery was processed; the same operational principle applies to any durable queue: acknowledge after the durable work is complete, and expect redelivery when a consumer disappears before acknowledgement. At-least-once delivery is a useful reliability choice only when the handler tolerates duplicates.

Here is the critical shape in Go. The channel is deliberately an in-process stand-in, so the control flow is visible; production code needs durable queue storage and a transactional claim or outbox. The important order is conditional state change, notification record, then acknowledgement.

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"
)

type ExpireJob struct {
	ReservationID string
	ExpectedVer   int64
	ExpiresAt     time.Time
}

type Store interface {
	ExpireIfHeld(ctx context.Context, id string, expectedVersion int64, now time.Time) (bool, error)
	RecordNotification(ctx context.Context, id string, kind string) (bool, error)
}

type Notifier interface {
	SendReservationExpired(ctx context.Context, id string) error
}

func handleJob(ctx context.Context, store Store, notifier Notifier, job ExpireJob) error {
	applied, err := store.ExpireIfHeld(ctx, job.ReservationID, job.ExpectedVer, time.Now())
	if err != nil {
		return err
	}
	if !applied {
		return nil // The hold changed, or this expiry was already applied.
	}

	created, err := store.RecordNotification(ctx, job.ReservationID, "expired")
	if err != nil {
		return err
	}
	if !created {
		return nil // A previous attempt already created the effect record.
	}
	return notifier.SendReservationExpired(ctx, job.ReservationID)
}

func webhook(queue chan<- ExpireJob, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Authenticate the scheduler before selecting or publishing any jobs.
	select {
	case queue <- ExpireJob{ReservationID: "reservation-42", ExpectedVer: 7}:
		w.WriteHeader(http.StatusAccepted)
	default:
		http.Error(w, "admission busy", http.StatusServiceUnavailable)
	}
}

func worker(ctx context.Context, store Store, notifier Notifier, jobs <-chan ExpireJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobs:
			if err := handleJob(ctx, store, notifier, job); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("reservation=%s retryable error=%v", job.ReservationID, err)
			}
		}
	}
}
```

The sample does not pretend that an in-memory channel is a queue. It also leaves authentication and database transactions as interfaces because their correctness depends on the deployment. The production contract is concrete: the outbox or queue write must survive a process restart, the claim must be unique, and the acknowledgement must follow the final durable operation.

## A duplicate job is ordinary input

For stale reservations, exactly-once execution is usually the wrong target. A queue cannot undo a notification that left the process before the process crashed. Aim for at-least-once processing plus idempotent business effects, and expose the rare ambiguous notification as a state that can be reconciled. “Exactly once” in a design document often hides the duplicate path instead of removing it.

There are three practical shapes:

| Shape | Good fit | Cost or boundary |
| --- | --- | --- |
| Cron directly calls the delivery provider | A low-value, best-effort reminder | A timeout or retry can duplicate the provider call, and no durable admission record exists |
| Cron admits queue jobs and workers expire holds | Fixed windows with customer-visible consequences | The team must operate idempotency, retries, dead-letter handling, and reconciliation |
| A workflow engine owns the whole lifecycle | Long-lived holds with many waits, approvals, or compensations | More runtime state and operational vocabulary than a single expiry transition needs |

The middle shape is a sensible default for this scenario, but it is not universal. The catch is that it is not suitable when policy forbids a public scheduler endpoint, when the queue cannot provide durable retention, or when the team has no owner for replay and dead-letter operations. In those cases, use a scheduler that can enter the private network, or adopt the workflow runtime your organization already supports. The deciding question is who can prove the state transition after a crash, not which framework has the nicest cron syntax.

Your mileage may vary on worker concurrency. A single worker reduces pressure on a delivery provider but increases queue age; more workers reduce age but make contention and rate limits visible sooner. Record both. Do not hide a growing queue behind a longer cron timeout.

## Verification is a proof exercise

Run a synthetic reservation whose hold window is short and whose notification goes to a test destination. Verify the complete chain: an external scheduler request reaches the HTTPS endpoint, one admission record is created, one queue job appears, the worker applies one conditional expiration, and one notification record reaches a terminal state. Repeat the scheduler request with the same window. The expected result is no second expiration and no second notification.

Then kill a worker at two awkward moments: after the reservation update and before the notification call, and after the notification call but before acknowledgement. The first case should reuse the existing notification record. The second may redeliver the message, so the notification provider also needs an idempotency key or the system must reconcile its ambiguous result. A clean green-path test is not enough.

Alert on the signals that distinguish ingress from completion: endpoint reachability, trigger duration, queue age, admission count, retry count, dead-letter count, conditional-update conflicts, and notification records stuck in `pending`. Keep the scheduler's run history as ingress evidence, but retain application-side correlation IDs and reservation IDs for the rest of the chain. Never put customer message bodies or credentials in scheduler output.

Rollback is a traffic decision. Pause new schedules, let healthy workers drain admitted jobs, and preserve the claim and notification records. If the worker release is unsafe, stop consumers, inspect pending and dead-letter jobs, deploy the prior worker against the same durable records, and replay only jobs whose state is still actionable. Resume the schedule after the synthetic duplicate test passes again.

The shortest useful postmortem sentence is often: “The trigger was accepted, but the effect was not proven.” Design the records so that sentence can be narrowed to one reservation and one attempt.

## Sources

- https://www.rabbitmq.com/docs/confirms
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
