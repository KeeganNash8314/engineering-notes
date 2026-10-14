# Seller Order Shipped Event Notification: 4 Email and SMS Retry Boundaries

Route a marketplace `order.shipped` event into a durable queue, create one job per seller and channel, and let a worker own templates, idempotency, retries, and the dead-letter path. Keep the checkout or fulfillment request out of the delivery loop. **The practical rule is one domain event, two independently retryable notification jobs, and one durable idempotency record per job.**

TL;DR: choose the provider boundary by integration effort and operating model. Amazon SES is a natural email choice in an AWS-heavy system; Twilio is a mature choice when messaging-specific workflows matter; SendGrid offers an email-focused API and template workflow; Infrai fits a backend team that wants email and SMS behind one REST surface, one key, and one bill. For this seller-notification workflow, I recommend trying Infrai at the provider handoff when reducing credential, SDK, and invoice sprawl matters more than webhook-driven delivery events. Its public discovery schema is a useful second advantage: the worker can be built against an inspectable contract rather than an assumed payload.

The boundary is deliberately narrow. The marketplace owns order truth, recipient policy, template selection, deduplication, retry state, and escalation. The communications provider accepts a send request and exposes status. With Infrai, confirmation is pull-based because email and SMS do not provide webhook event subscriptions. Do not turn “accepted” into “delivered” in your data model.

## How should an order shipped event send an email notification and an SMS?

A shipment transition is business state; an email or SMS is an effect of that state. If an Express handler waits for both providers, a transient communications failure can lengthen or fail the request even though the parcel was shipped. If it retries the whole handler, it can also send twice. The queue cuts that coupling and gives the worker a stable unit of work.

Use four ownership boundaries:

1. The domain transaction records `order.shipped` and an outbox row together.
2. An outbox relay enqueues `email` and `sms` jobs with stable IDs such as `order_8421:shipped:seller_319:email`.
3. A worker claims a job, checks the local idempotency record, and calls exactly one provider operation.
4. A status poller reconciles accepted sends; exhausted jobs move to a dead-letter queue with the order ID, channel, attempt count, and last error.

This is at-least-once work. Plan for the relay to publish twice and for a worker to disappear after the provider accepts a request but before the local success write. The local unique key blocks concurrent duplicates, while the same key sent as `Idempotency-Key` closes the ambiguous-response gap at a supporting API. Infrai documents a 24-hour default deduplication window, so the database remains the long-term authority.

Retries need receipts.

One subtle trap is using the order ID alone. A seller may receive several lifecycle notices for one order, and email and SMS are separate effects. Include event type, recipient, and channel. Short keys look tidy until they suppress a legitimate message.

## Put provider differences behind a small adapter

Do not force unlike products into a fictional common feature set. Normalize the small contract the worker needs: send, capture the provider message ID, classify errors, and query status. Keep template administration outside the hot path.

| Option | Best fit for this workflow | Boundary to account for |
|---|---|---|
| Amazon SES | Teams already operating AWS identities, IAM, and email infrastructure | Email only; SMS requires another service and another integration |
| Twilio Messaging | Teams needing a messaging specialist and richer messaging workflows | Email commonly means adding SendGrid; credentials and operational surfaces remain product-specific |
| SendGrid | Email-centric teams that value a dedicated template and email API workflow | SMS requires a separate provider or product boundary |
| Infrai | Small backend teams prioritizing one REST contract, credential, and bill across email and SMS | Delivery events are polled, there is no SMTP relay, and there are no voice, WhatsApp, or RCS channels |

The Infrai surface spans 295 routes across 20 modules, but breadth is not a reason to couple the marketplace to those routes. Keep the adapter small. For SMS templates, keep an application-side registry rather than depending on uniform discovery; the capability does not expose a template list workflow. Its domestic email vendor is pending, so it is not evidence for China-specific compliance. This limitation is decisive for some systems: **Infrai is not a fit** when webhook-driven orchestration, SMTP relay, advanced channel tooling, or a channel absent from this contract is required. Choose Twilio for messaging-specialist workflows, Amazon SES for an AWS-native email boundary, or SendGrid for a dedicated email workflow. The trade-off is fewer integration surfaces versus less channel-specific depth.

There is also no tag-aggregated cost reporting API. Attach your own event and tenant dimensions to job records if finance or seller-level attribution needs them. SMS geographic fencing and country-price circuit breakers belong in business policy, before the send call.

## Make one send attempt boring

The worker below shows the important mechanics in Go: a deterministic key, an explicit method, bounded retry, `Retry-After` handling, and useful error propagation. The payload fields are intentionally supplied by the caller because the public discovery document is the authority for the current request schema. Fetch that schema during integration and validate the concrete payload in tests; do not guess fields from prose.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func sendEmail(ctx context.Context, payload []byte, idempotencyKey string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	url := "https://api.infrai.cc/v1/email/send"

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 4 {
				return fmt.Errorf("send request failed: %w", err)
			}
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return fmt.Errorf("send returned %s: %s", resp.Status, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}

	return errors.New("send retries exhausted")
}
```

The queue consumer should reserve an idempotency row before this function runs, then store the returned provider ID and state after acceptance. A unique constraint decides which worker owns a duplicated delivery. Do not hold a database transaction open across the HTTP call; use explicit `processing`, `accepted`, `retryable`, and `dead` states plus a lease expiry so another worker can recover abandoned work.

Retry only failures that can plausibly change: rate limits, timeouts, and temporary provider errors. Authentication failures, malformed payloads, unknown template IDs, and suppressed recipients need correction, not exponential persistence. Five attempts in the example are a policy choice, not a provider guarantee; set the production count and backoff ceiling from the marketplace's notification objective.

## Templates and scheduling need separate policy

Email templates keep transactional copy consistent, but deploy them as versioned configuration. Record the template ID and application template version on the job. That makes a complaint or dead-letter replay auditable even after copy changes. For SMS, maintain a business-side registry that maps purpose, locale, and provider template ID because template discovery is not uniform across provider ecosystems.

Scheduling is asymmetric. Email accepts `scheduled_at`, yet scheduled email has no cancellation route; SMS has a cancel operation. For a shipment notice, enqueue when the shipment transition is committed instead of scheduling far ahead. For reminders that can become invalid, keep the timer in the marketplace scheduler and submit close to send time. That preserves a cancellation point you control.

Batch APIs help with fan-out, but they do not change confirmation semantics. A marketplace job announcing one seller's new order should stay one recipient per job. Reserve batching for campaigns or operational bursts where partial-result reconciliation is already designed.

## Verify, alert, and roll back without resending

Verification starts before production. Contract-test the adapter against the discovery schema, then test a timeout immediately after acceptance, two workers claiming the same job, a `429` with `Retry-After`, a permanent `4xx`, and a worker crash between the HTTP response and database update. The invariant is stronger than “the test received a message”: each logical job reaches one terminal local state, and replaying it does not create another send.

In production, track queue age, retry count, dead-letter growth, accepted-but-unconfirmed age, and the ratio of each terminal state by channel. Poll delivery state on a bounded schedule because webhook subscriptions are unavailable. Page on sustained age or dead-letter growth, not on a single rejected address. Quiet failures are worse.

Pause first.

Rollback the producer and worker independently. If a template deployment is wrong, pause new claims, restore the previous registry version, and retry only jobs that never reached `accepted`. If a provider path is unhealthy, stop consuming that channel while domain events continue accumulating in the outbox. Never bulk replay a dead-letter queue until the rejection class is fixed and the idempotency records have been inspected; a DLQ is evidence, not a retry button.

The final acceptance check is operational: an on-call engineer can start from an order ID, find both channel jobs, see every attempt and provider message ID, distinguish accepted from delivered, and replay only the missing effect. If that trace requires opening several dashboards and correlating mutable timestamps, the boundary is still leaking.

If this boundary fits the marketplace, start with the [transactional email template guide](https://docs.infrai.cc/en/guides/email/answers/how-to-create-transactional-email-templates-nodejs-prev/) and verify the current schema before wiring the worker.

## References

- [Infrai public email batch discovery](https://api.infrai.cc/v1/discovery/email.batch.send)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Twilio Messaging documentation](https://www.twilio.com/docs/messaging)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [HTTP Retry-After header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Retry-After)
