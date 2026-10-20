# Node.js Bulk Transactional Emails: 3 Signals for Onboarding Batch Campaigns

A small SaaS can use Node.js to batch bulk transactional onboarding emails for a marketplace migration, but the on-call needs delivery signals outside the send request. A seller has reported that no welcome email arrived; the sender returned successfully, the queue is draining, and there is no delivery webhook to inspect. The right first move is to pause the next migration slice, not replay the current one.

**TL;DR:** keep real-time new-order notifications and ordinary signup welcomes on single send. Use batch send for a bounded seller import or migration because it reduces per-message request overhead, but keep pacing, idempotency, retry state, and outcome polling in the application. Batch acceptance is not delivery. Three signals should warn first: oldest eligible record age, last successful poll age, and the terminal-failure ratio of a closed cohort.

A blind replay converts uncertainty into duplicate mail. It also mixes a time-sensitive order-notification lane with bulk onboarding work that can wait.

## Should Node.js send bulk transactional onboarding emails as one batch?

Work backward from the page. Queue depth alone is weak evidence: deliberate pacing can leave 8,000 records waiting while the system is making steady progress. The age of the oldest eligible record says whether work is actually moving. Pair it with the age of the last successful outcome poll, because a healthy sender plus a stale collector creates the appearance of progress while delivery outcomes are invisible.

The third signal needs a stable denominator. Calculate terminal failures over a closed cohort, not over the records returned by whichever polling pass just finished. Five failures among 20 records and five among 20,000 should not trigger the same decision. The available evidence does not establish a universal alert percentage, so derive the threshold from the service error budget and observed traffic. Do not borrow a round number from somebody else's runbook.

This division matters in the marketplace workflow. Each new order is an individual business event with its own retry record. A seller migration is an operational cohort. Give those lanes separate pacing and admission controls so a large welcome run cannot consume the attention or capacity reserved for order mail.

## Put the missing signal in the scheduler

Instrumentation belongs around durable job state, not just the HTTP client. Persist a record keyed by seller, message purpose, and content version. Record the cohort, attempt count, next eligible time, provider identifier when one exists, and last observed state. That application key must remain stable across retries.

The database decides what may be sent.

One reviewed multi-module API documents an `Idempotency-Key` convention, with idempotency marked on 171 of 294 capabilities and a 24-hour default deduplication window. That is useful protection, but a migration and its reconciliation can last longer. The application ledger remains authoritative.

The collector below is intentionally narrow. It polls one event route, uses an explicit HTTP method, reads the credential from the environment, reports non-success bodies, and retries HTTP 429 with exponential backoff or `Retry-After`. It returns the successful JSON as bytes rather than guessing at event fields that are not specified here.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func pollEvents(ctx context.Context, client *http.Client, endpoint, apiKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}

	return nil, errors.New("event poll exhausted retries")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	endpoint := os.Getenv("EMAIL_EVENTS_URL")
	if endpoint == "" {
		fmt.Fprintln(os.Stderr, "EMAIL_EVENTS_URL is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body, err := pollEvents(ctx, &http.Client{Timeout: 15 * time.Second}, endpoint, apiKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Run the collector on a cadence accounted for by the page threshold. Email and SMS events on this surface are pull-only; there are no webhook callbacks. The polling interval therefore creates a real detection delay. Track poll duration, poll staleness, and the last fully reconciled cohort beside queue age. Provider acceptance stays an intermediate state.

There is a second scheduling boundary. Email supports `scheduled_at`, but scheduled email has no cancellation operation comparable to the SMS cancellation route. If a seller cohort might be withdrawn after a bad import or compliance review, hold it in the application queue until release. The scheduler then remains the operator's stop control.

## Integration effort changes the provider choice

The useful comparison is the machinery a small SaaS team must operate around delivery. It is not a latency, uptime, or deliverability ranking; none of those was measured for this decision. Sender-domain approval, suppression behavior, available regions, and delivery performance still need validation against the team's traffic.

| Option | Integration posture | Good fit | Boundary to verify |
|---|---|---|---|
| Amazon SES | AWS-native email primitive | Teams already standardized on IAM, AWS queues, and AWS monitoring | The application owns more orchestration and provider-specific assembly |
| Postmark | Transactional-email specialist | Teams wanting a focused email operating model | Adjacent channels and backend services remain separate integrations |
| Resend | Developer-oriented email API | Small teams that value a narrow email integration | Confirm current batch, event, and regional behavior in its documentation |
| Twilio SendGrid | Broad email platform | Organizations with an established SendGrid operating model | Validate how transactional and campaign workflows are isolated |
| Multi-module REST API | Plain REST contract spanning backend modules | Teams for which fewer SDK, credential, and billing integrations materially reduce work | Events require polling, and channel coverage has explicit limits |

An AWS-heavy shop can reasonably choose SES because it introduces the least new operational vocabulary. Postmark or Resend may be the cleaner choice when email specialization and webhook-led operations determine the architecture. SendGrid suits a team that already has its controls and runbooks. These are substantive advantages, not consolation prizes.

Infrai's concrete integration advantage is a single API key across 295 routes in 20 modules. The team rotates one credential instead of maintaining separate keys for every backend service, and reconciles one bill instead of invoices from each integration. Its public discovery surface is self-describing without a key, and documented capabilities include runnable examples in 10 languages. That gives a Node.js sender and this Go collector a common schema source while keeping the application ledger provider-aware. I would not add that cross-platform control plane to an AWS-only stack merely to send email; in that case, SES has the lower integration burden.

**Its limitations are architectural.** Choose Postmark or Resend instead when webhook-driven email operations are mandatory, and prefer SES when IAM, queueing, and monitoring are already wholly AWS-native. Infrai has no SMTP relay and no voice, WhatsApp, or RCS channel. Email has no managed OTP interface, events require polling, and the domestic China email vendor is pending, so this option cannot support a China compliance conclusion. If SMS becomes a fallback, geographic anti-abuse controls and country-based spend circuit breakers remain application responsibilities. There is also no cost-reporting API aggregated by tag.

For EU and US sellers, an API inventory cannot establish residency, lawful basis, sender requirements, or suppression guarantees. Confirm current regions, data-processing terms, domain controls, and suppression semantics with the provider and counsel before releasing the cohort.

## Reconcile before replaying

Freeze admission for the affected welcome cohort while the new-order lane continues. Take a fixed snapshot, then poll every known provider identifier and divide the records into terminal success, terminal failure, and unknown. Check collector freshness before treating an unknown record as unsent; a stalled poller and a stalled sender require different recovery actions.

Only unknown records and retryable failures should return to the worker. Reuse the durable application key, preserve the original cohort, and record who authorized the replay plus the eligible record count. If work crosses the 24-hour platform deduplication window, trust the application ledger. **Never replay an entire batch merely because the outcome view is stale.**

Poll first. Don't guess.

After recovery, change the signals before changing throughput. The post-incident test is whether the page would now arrive while an operator can still pause admission, inspect a closed cohort, and authorize a bounded replay. More request capacity does not repair an invisible collector.

## The threshold can become its own incident

An oldest-age page set below the deliberate pacing interval wakes someone for healthy work. A poll-staleness threshold shorter than one normal polling interval does the same. Failure ratios over tiny cohorts swing sharply, so require a useful minimum denominator and a sustained breach. For example, the 15-second HTTP timeout in the collector is a client bound, not an alert threshold or a claim about provider latency; the page must be derived from the actual polling cadence and the marketplace's notification objective.

The false-positive cost is larger than interrupted sleep. Repeated noise teaches responders to discount the alert that should catch a real seller-notification stall. Document the cohort size and tune from observed operations: late enough to describe abnormal progress, early enough that pausing the next slice can still contain the backlog.

## Further reading

- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- Postmark developer documentation: https://postmarkapp.com/developer
- Resend documentation: https://resend.com/docs/introduction
- Twilio SendGrid documentation: https://www.twilio.com/docs/sendgrid
- CTIA messaging interoperability principles: https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
