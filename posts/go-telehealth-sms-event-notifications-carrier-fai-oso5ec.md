# Go Telehealth SMS Event Notifications — Carrier Failure Troubleshooting US/EU Codes

A healthtech pipeline should treat generated report attachments and telehealth SMS event notifications as two delivery paths with one shared rule: SMS failures are evidence gaps until the attempt is correlated. A resend is a new attempt, not proof that the first attempt failed. The operational choice that matters is the integration boundary. Keep generation, message policy, transport submission, and delivery evidence separate, then make every transition idempotent and observable.

A useful short answer is to stop diagnosing from the user-facing timeout. Correlate the verification attempt with the provider submission, carrier response, sender-registration state, and final delivery event. For an email report, also record the attachment digest and MIME metadata. That evidence tells you whether the failure occurred before submission, at filtering, or after the destination accepted the message.

I have been paged for missed jobs and duplicate deliveries, so my first question is deliberately boring: which exact attempt are we discussing? A login user may have requested three codes; a clinician may have clicked “send report” twice while the first attachment was still being generated. Without an attempt identifier, the incident log turns those actions into one misleading story. I first thought the timeout was the failure; the ledger showed that the remote system had already accepted the message, which changed the response from another submission to status reconciliation.

Wrong attempt, wrong diagnosis.

## How should event notifications handle SMS failures across carriers?

Start with a small event ledger. It should be append-only from the application’s point of view, and it should retain the provider’s opaque message identifier without treating that identifier as a delivery guarantee. A practical record includes:

- `notification_id`, `purpose`, and `attempt`;
- a keyed recipient reference, destination country, and channel;
- the template or message-policy version;
- sender identity, registration state, and transport submission result;
- timestamps for enqueue, submission, carrier or mailbox acceptance, and final status;
- a redacted reason code and a link to the relevant trace.

Do not store the one-time password in this ledger. OWASP recommends protecting reset and verification tokens with controls such as expiration, single use, consistent responses, and rate limiting. The same boundary is useful here: the event ledger explains a delivery attempt without becoming a second secret store.

For a report attachment, record a SHA-256 digest, byte length, content type, and filename policy before submission. The digest answers “which report did we send?” when a user reports a bad attachment. It also makes a retry decision safer: a retry can reuse the immutable artifact instead of regenerating a report with a different timestamp or dataset. The email itself still needs correctly constructed MIME parts; the MIME specifications define the transfer and multipart mechanisms that mail clients use to interpret an attachment.

The ledger does not need to expose the full phone number or email address. A stable internal recipient key plus a display-safe suffix is enough for most on-call work. Retain raw transport payloads only under the organization’s access and retention policy. Health data turns a convenient debug table into a privacy boundary quickly.

## Which retry boundary prevents duplicate notifications?

A resend policy should distinguish user intent from transport recovery. A fresh code request can invalidate the previous code, but a transport retry for the same attempt should not create a second valid code. For a report, “send again” can point to the same content digest while creating a new delivery attempt. Those are different operations and should have different audit entries.

I start with a state machine rather than a retry loop:

```go
type DeliveryState string

const (
	Queued     DeliveryState = "queued"
	Submitted  DeliveryState = "submitted"
	Accepted   DeliveryState = "accepted"
	Delivered  DeliveryState = "delivered"
	Failed     DeliveryState = "failed"
	Suppressed DeliveryState = "suppressed"
)

type Attempt struct {
	ID             string
	NotificationID string
	Channel        string
	AttemptNumber  int
	ContentDigest  string
	IdempotencyKey string
	State          DeliveryState
	ProviderID     string
	FailureCode    string
}

func retryable(a Attempt) bool {
	switch a.State {
	case Queued, Submitted:
		return true
	case Failed:
		return a.FailureCode == "temporary_transport" ||
			a.FailureCode == "throttled"
	default:
		return false
	}
}
```

The exact state names are local choices. The important distinction is between a temporary transport problem and a policy decision such as suppression, invalid destination, or carrier rejection. Blindly retrying the latter amplifies traffic and can make filtering more likely. A backoff schedule needs a ceiling, jitter, and a total deadline; it also needs a per-recipient limit so a stuck downstream system cannot turn one click into a notification storm.

The queue is not the audit trail.

The idempotency key should be derived from the operation’s meaning. For a transport retry, use the notification ID, attempt number, channel, and content digest. For a new user-requested code, use a new attempt number and a new token record. Store the result of the submission under that key before acknowledging the queue message. If the worker crashes after submission and before acknowledgment, the next worker can look up the key instead of guessing whether it should send again.

That last sentence is the edge that matters in production. Queue acknowledgment is not delivery confirmation. A successful HTTP response is not carrier delivery confirmation. A carrier delivery event is not proof that a person saw the code. Each boundary deserves its own timestamp and status.

## Why do US and EU paths disagree?

Carrier filtering is not a single switch that an application can inspect. It is an outcome influenced by sender identity, traffic pattern, message content, destination, registration or trust requirements, and the receiving network’s policy. The same body can behave differently across countries or carriers. That is why a single global retry rule produces poor evidence. Segment dashboards by destination country, carrier when available, sender identity, and failure class.

Sender registration is an integration dependency, not a checkbox in application configuration. Before production traffic, define who owns the sender record, what evidence is required, how changes are approved, and how long a new identity may remain in a pending state. Keep the registration reference and its effective scope in configuration metadata. When a message is rejected, log the registration version used by that attempt. Otherwise the on-call engineer cannot tell whether the request used the newly approved sender or a stale deployment value.

Signatures need the same treatment. If the downstream event includes a signature, verify it against the raw request bytes, the documented signing input, and a trusted key or secret; reject stale timestamps and replayed event IDs. Do this before JSON decoding changes whitespace or field ordering. A signature failure is an authenticity problem, while a carrier rejection is a delivery-policy problem. Combining both under “SMS failed” hides the action.

For email, sender authentication and complaint handling are separate from SMS registration. Sender guidance published by Yahoo describes authentication, reputation, complaint, and unsubscribe expectations for mail senders; it is useful context, but it does not define a universal SMS rule. Keep channel-specific policy in separate runbooks. Shared observability is valuable. Shared assumptions are not.

## How should the Go worker make retries safe?

The worker should claim one attempt, load the immutable payload, submit it with the idempotency key, and persist the result. The transport interface stays generic so a test double can return accepted, rejected, timed out, and malformed responses without contacting a live network.

```go
type TransportResult struct {
	ProviderID string
	Accepted   bool
	Code       string
}

type Transport interface {
	Submit(ctx context.Context, a Attempt, payload []byte) (TransportResult, error)
}

func deliver(ctx context.Context, t Transport, store Store, a Attempt, payload []byte) error {
	if result, ok := store.ResultFor(a.IdempotencyKey); ok {
		return store.RecordObservedResult(a.ID, result)
	}

	result, err := t.Submit(ctx, a, payload)
	if err != nil {
		return store.RecordFailure(a.ID, "temporary_transport")
	}
	if !result.Accepted {
		return store.RecordFailure(a.ID, result.Code)
	}
	return store.RecordSubmission(a.ID, result.ProviderID, a.IdempotencyKey)
}
```

This sketch assumes `Store` methods are transactional and that `ResultFor` is protected by a unique constraint on the idempotency key. The transport must also honor that key or provide an equivalent deduplication contract; otherwise the application can prevent duplicate local submissions but cannot retract a duplicate already accepted downstream. That is an integration boundary worth testing explicitly, not a detail to infer from a happy-path response.

Tests should cover the awkward timing windows: timeout after remote acceptance, worker crash before acknowledgment, duplicate queue delivery, a stale verification code, a changed report digest, an invalid signature, and a delivery event that arrives before the submission transaction is visible to the read model. The last case is common in eventually consistent event systems. Store the event, then reconcile it when the attempt appears.

I also put a bounded manual replay command behind authorization. It accepts an attempt ID and a reason, never a raw destination, and creates a new audit event. Manual replay is a recovery tool. It should not bypass suppression, rate limits, sender scope, or the same idempotency rules used by the worker.

## Which integration boundary fits the team?

The least integration effort is not always the fewest lines of client code. A managed transport may remove protocol and carrier plumbing while adding requirements around sender onboarding, webhook verification, status vocabulary, regional availability, and account-level limits. A self-hosted or directly integrated path may offer more control but transfers registration, deliverability, certificate, queue, and on-call work to the team. The right comparison is the operational surface your team can own at 03:00.

Compare candidates against the same test matrix: attachment size and MIME handling for email, sender identity and registration scope for SMS, idempotency behavior, timeout semantics, signed event verification, status granularity, regional routing, exportable evidence, sandbox behavior, and incident support. Ask for failure examples, not only a successful send. A system that reports only “accepted” may be perfectly adequate for low-risk notices and insufficient for login verification or regulated report delivery.

Run a staged test with synthetic recipients in the US and EU, a fixed report artifact, and a known set of verification attempts. Keep the test data non-clinical. Measure integration work in engineer-hours and runbook steps, not only initial setup time. A small adapter with weak status mapping can cost more over a quarter than a larger adapter whose events map cleanly to your ledger.

The decision rule I use is plain: choose the boundary that preserves evidence and makes the failure class actionable. If the team cannot tell “not submitted,” “accepted then filtered,” “registration mismatch,” and “user requested a newer code” apart, the integration is incomplete regardless of how short the code is.

The limitation is operational ownership. A managed path can reduce integration work, but it cannot decide which report artifact is authoritative, whether a user is allowed another code, or how long health data belongs in an audit record. A self-hosted path gives more control and carries more registration, deliverability, and on-call work. This design is a poor fit for a team that cannot staff those controls; a smaller, narrower integration with fewer channels may be the safer choice.

In a 2026 runbook, that tradeoff belongs beside the escalation threshold, not in a marketing comparison.

## Sources

- OWASP, “Forgot Password Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- Yahoo, “Sender best practices and requirements”: https://senders.yahooinc.com/best-practices/
- RFC 2045, “Multipurpose Internet Mail Extensions (MIME) Part One”: https://www.rfc-editor.org/rfc/rfc2045
- RFC 2046, “Multipurpose Internet Mail Extensions (MIME) Part Two”: https://www.rfc-editor.org/rfc/rfc2046
- RFC 9421, “HTTP Message Signatures”: https://www.rfc-editor.org/rfc/rfc9421
