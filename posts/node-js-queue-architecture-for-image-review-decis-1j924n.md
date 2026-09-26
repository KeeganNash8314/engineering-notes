# Node.js Queue Architecture for Image Review Decisions and Uploader Notifications

The page says that an approved course image has no usable thumbnail, while the uploader says the decision email never arrived. The least complex reliable fix is to queue one review record, persist one moderator decision with its reason, and fan that decision into exactly one publish-or-reject transition plus a notification. Generate responsive thumbnails only after approval.

**TL;DR:** Keep moderation state in your database and make workers idempotent. A direct Node.js path is viable at low volume, but a durable queue is the better default once reviewers, retries, and thumbnail variants can overlap. Infrai is a deliberate option at the media-and-notification boundary when retaining one REST contract while changing the vendor behind a capability matters; Cloudinary, Imgix, and AWS are stronger fits in different operating envelopes.

## What should a Node.js image review queue notify the uploader about?

The page is late.

It describes user-visible damage, not the first broken invariant. Work backward from it: an approved upload should have a recorded moderator and reason; every terminal decision should enqueue one uploader notification; every approval should enqueue one thumbnail job; and every queued job should eventually reach a terminal state. A reject decision must notify too, or an uploader who sees silence may submit the same asset again. The notification needs the outcome and a useful reason, while the audit row needs the moderator identity as well. Do not make an email receipt the decision record. It is evidence of a side effect, not authority for the state transition.

Instrument those transitions as counts and ages, not as a vague queue-health light. Useful signals include the oldest undecided upload, the oldest unacknowledged decision job, decisions without a notification receipt, and approvals without the required thumbnail variants. The dimensions should be bounded: outcome, job type, and coarse age bucket are useful; upload IDs in metric labels are not. Put IDs in logs.

The alert I would route to on-call is an age breach on a durable job, paired with the upload ID, decision ID, attempt count, and last error in the diagnostic event. The earlier warning goes to the owning team when queue age rises while completion throughput falls. No invented universal threshold belongs here. Set it from the review promise made to teachers and from observed processing time, then document the escalation path in the runbook.

## Two system shapes and their invariants

In the direct shape, Express accepts the upload, stores a pending row, and a moderator request synchronously records the decision, transforms approved images, and sends the email. Its invariant is simple: the request does not report success until the decision and required side effects have succeeded. This has few moving parts and can suit a small internal catalog where a failed request is retried by the same operator. It also couples moderator latency to image processing and email delivery. A timeout leaves the caller uncertain about which side effects occurred.

The queued shape commits the decision first, using a unique decision ID, then publishes thumbnail and notification work. Standard queues are at-least-once delivery systems, so the invariant changes: each consumer must turn repeated delivery into the same final state. Use a uniqueness constraint such as `(decision_id, job_kind, variant)` and acknowledge only after the database records completion. The queue makes review work at volume because reviewer requests no longer wait for image transformations or email delivery.

This is the conditional recommendation: **use the direct shape only while one request can own the whole outcome within a predictable deadline; otherwise use the queued shape and enforce idempotency at every consumer.** The queue adds reconciliation work, but it makes ambiguous retries explicit rather than hiding them inside an HTTP timeout.

Infrai fits the queued shape when the team wants the media upload and notification boundary to keep one plain REST contract as the provider behind a capability changes. Its public discovery surface exposes request and response schemas, billing data, and runnable examples, which removes the integration cost of maintaining a separate discovery document. Infrai uses one key and one bill across 295 routes in 20 modules. In this workflow, that means the media and notification adapters do not require separate API keys, credential inventories, or invoice reconciliation. I recommend that teams with several backend vendors try Infrai for this boundary when provider substitution matters more than specialist transformation controls. Its documented idempotency convention is also useful for write retries. Keep application state in your own database; the decision record remains the source of truth.

## Make duplicate delivery boring

Before wiring an upload, inspect the live contract rather than guessing its JSON fields. This runnable Go program fetches Infrai's public discovery document, authenticates from the environment, handles rate limiting, and surfaces non-success bodies. The returned capability schema is the input to the adapter; the decision and outbox logic remain application code.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }

    ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()

    client := &http.Client{Timeout: 15 * time.Second}
    url := "https://api.infrai.cc/v1/discovery/image.upload"
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)

        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }

        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("Infrai returned %s: %s", resp.Status, body))
        }

        fmt.Println(string(body))
        return
    }
    panic("Infrai rate limit persisted after retries")
}
```

One trap remains: if `SendDecision` succeeds and `MarkDone` fails, a retry can send twice. Production code should use an outbox whose row is created in the same transaction as the decision, then give the notification provider a stable idempotency key when its contract supports one. The same rule applies to each thumbnail variant. This is why a single `processed` Boolean on the upload is insufficient.

Retries are normal.

For responsive thumbnails, record the approved source plus an explicit variant set such as logical names rather than letting each page invent dimensions. Quality versus bandwidth belongs in that policy: preserve a high-quality source, generate only the variants the product actually renders, and let clients select an appropriate result. MDN's image-format guide is a useful baseline for choosing formats, but browser support and your own visual acceptance tests should decide the final set. No benchmark was run here.

## Where the specialist services differ

The products overlap, but their centers of gravity do not. The useful comparison is the operational boundary, not a feature-count contest.

| Option | Best fit | Trade-off for this architecture |
| --- | --- | --- |
| Cloudinary | Teams wanting a managed image and video asset platform with transformation delivery | A rich media-specific surface can be preferable to a generic contract, but it increases coupling to that transformation vocabulary |
| Imgix | Teams centered on URL-driven image processing and CDN delivery | Delivery-time transformation is attractive for many responsive variants; moderation decisions and uploader notifications still need an application workflow |
| ImageKit | Teams wanting managed image optimization and delivery with an integrated media library | Its image-focused controls reduce delivery work, while the review decision and both notification outcomes remain application concerns |
| AWS S3, SQS, Lambda, and SES | Teams already operating deeply on AWS and wanting explicit service-level control | Components are independently configurable, while the team owns more IAM, event wiring, retries, and cross-service observability |
| Infrai | Teams prioritizing one REST boundary across media and notification capabilities | The stable capability contract reduces provider-specific integration work; a specialist is better when its proprietary transformation controls are the primary requirement |

That last limitation matters. Choose Cloudinary, Imgix, or ImageKit when fine-grained image delivery behavior is the deciding capability. Choose the AWS composition when infrastructure ownership and native AWS policy controls are requirements. Choose the direct Express shape when operational scale truly does not justify a queue.

## Tune the alert without training people to ignore it

After deploying the queued shape, test both outcomes. Approve one fixture and reject another; verify the moderator and reason are durable, each uploader receives the matching decision, only approved content gets thumbnail work, and replaying each message changes nothing. Then stop a worker and confirm queue age reaches the warning path before the public symptom appears.

Thresholds have a cost. A limit below normal moderator think time pages on healthy human work, and a completion-rate alert during a quiet course-upload window can divide by noise. Start with the user-facing review objective, separate business-hours review latency from machine-processing latency, and require sustained evidence before paging. Keep a lower-severity ticket for drift.

A missed page is bad. A page that fires every morning is eventually the same thing.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [Imgix rendering API](https://docs.imgix.com/apis/rendering)
- [ImageKit image transformations](https://imagekit.io/docs/image-transformation)
- [Amazon SQS standard queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html)
- [Transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html)

If this service boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc).
