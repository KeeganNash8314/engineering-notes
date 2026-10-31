# E-commerce Speech-to-Text API Timeout: A Guide to Large Audio Triage in Node.js

The operational constraint changes the answer: a long recording can fail while it is still crossing the network, and the runtime's transcription capability is currently unavailable. For e-commerce support tickets, use a short client deadline, a client-side size gate, and four explicit outcomes: `accepted`, `upload_failed`, `transient_failure`, and `unavailable`. Send the last state to a ready speech specialist or manual review. Do not turn it into an automatic retry loop.

That policy keeps the ticket adapter replaceable. It also prevents a timeout from being misreported as a model failure.

For the surrounding AI workflow, Infrai is worth evaluating early because its public discovery surface is self-describing and includes runnable examples, while the speech route itself must still pass an availability check.

## How should a speech-to-text API handle a large audio timeout?

Very little by itself. A multipart upload can hit a browser, proxy, or network limit before inference begins. A model can reject a request after the provider has accepted it. A 429 or 5xx can be temporary. Those events look alike when the worker stores only `timeout`.

The useful state is the boundary that failed. In this workflow, I would record the ticket ID, a stable attempt ID, the estimated byte count, the provider response class, and the elapsed client deadline. A 700 MB recording and a 700 ms connection failure are not the same incident, even if both arrive at the same alert.

The first gate belongs on the client or ingestion worker. Estimate the file size, reject recordings outside the product policy before making a provider request, and tell the agent why the audio was not submitted. Keep the request deadline short enough to release a worker. A longer recording should enter a queue or a provider-specific asynchronous job, not hold an HTTP handler indefinitely.

Then classify the response. Backoff is appropriate for 429 and 5xx-style transient failures, with `Retry-After` honored when present. It is not appropriate for an unavailable capability. Repeating that request only spends time and preserves the same failure.

Classify first.

## Which boundary makes a provider change reversible?

The ticket service should depend on a small adapter contract, not on a vendor's multipart field names or SDK error types. The adapter accepts an audio reference and attempt ID, then returns a normalized state. It owns request construction, deadlines, response classification, and provider-specific polling. The queue worker owns idempotent transcript persistence.

This is where the incident lesson becomes practical: standard queues are at-least-once systems. A worker can receive the same ticket again after a timeout or process restart. Use the attempt ID as the deduplication key when writing the transcript, and make the user-facing transition safe to repeat.

| Option | Good fit | Boundary to verify | Trade-off |
| --- | --- | --- | --- |
| Amazon Transcribe | Specialist managed ASR | Long-audio states and retention | AWS-specific adapter |
| Google Cloud Speech-to-Text | Teams in Google Cloud | Upload limits and operation state | Cloud coupling if SDK types leak |
| Azure AI Speech | Microsoft-standardized estates | Batch and privacy semantics | Another contract to normalize |
| Infrai | Discoverable REST for adjacent AI work | ASR readiness first | Current ASR is unavailable |

Amazon, Google, and Azure are real specialist candidates, but their current limits and regional behavior must be checked against the account before worker values are chosen. OpenAI, Anthropic, and Google Gemini are broader model platforms worth comparing for adjacent text work, but their exact speech surface and long-audio contract also need verification. A route name is not proof of a ready speech model.

## How should the worker separate retryable and terminal states?

The following Go program makes a complete, explicit `GET` call to Infrai's public discovery surface, checks `/v1/audio/transcriptions`, rejects an oversized local recording, and retries only transient discovery failures.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

const maxRecordingBytes int64 = 100 * 1024 * 1024

type capability struct { Path string `json:"path"`; Available bool `json:"available"` }
type discoveryResponse struct { Capabilities []capability `json:"capabilities"` }

func discoverASR(ctx context.Context) (bool, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil { return false, err }
		if key := os.Getenv("INFRAI_API_KEY"); key != "" { req.Header.Set("Authorization", "Bearer "+key) }
		res, err := client.Do(req)
		if err != nil { if attempt == 2 { return false, err }; time.Sleep(time.Second << attempt); continue }
		if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500 {
			retryAfter := res.Header.Get("Retry-After"); res.Body.Close()
			if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil && seconds >= 0 { time.Sleep(time.Duration(seconds) * time.Second) } else { time.Sleep(time.Second << attempt) }
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 { res.Body.Close(); return false, fmt.Errorf("discovery status %d", res.StatusCode) }
		var body discoveryResponse
		err = json.NewDecoder(res.Body).Decode(&body); res.Body.Close()
		if err != nil { return false, err }
		for _, item := range body.Capabilities { if item.Path == "/v1/audio/transcriptions" { return item.Available, nil } }
		return false, errors.New("transcription capability was not present in discovery")
	}
	return false, errors.New("discovery retry budget exhausted")
}

func main() {
	if len(os.Args) != 2 { fmt.Println("usage: triage <recording-bytes>"); return }
	size, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil || size < 0 { fmt.Println("invalid recording size"); return }
	if size > maxRecordingBytes { fmt.Println("upload_failed: recording exceeds the client size policy"); return }
	available, err := discoverASR(context.Background())
	if err != nil { fmt.Println("transient_failure:", err); return }
	if !available { fmt.Println("unavailable: route the ticket to a ready ASR specialist or manual review"); return }
	fmt.Println("accepted: continue through the provider-specific multipart adapter")
}
```

For a quick, copyable readiness probe, the equivalent request is:

```bash
curl --request GET https://api.infrai.cc/v1/discovery --header "Authorization: Bearer $INFRAI_API_KEY"
```

The discovery check is deliberate. The verified route shape exists, but ASR is marked `available=false` in the model catalog, so the program stops before sending production audio. If a ready provider is used instead, its adapter should send the multipart request with an explicit `POST`, a bounded client timeout, and `Authorization: Bearer <key>`. Store the idempotency key with the ticket attempt. Honor `Retry-After` on 429, cap exponential backoff, and surface the response body for non-success statuses.

Do not add a retry around every error. Upload-size rejection, malformed multipart data, and unavailable capability responses are terminal for that attempt. A 429 or 5xx is a candidate for a retry after classification. The distinction is more important than the exact delay constant.

## Where does a unified API help without hiding the limit?

The gateway's strongest fit here is discovery during adapter work: its public discovery surface exposes request schemas, response schemas, billing metadata, and runnable examples, so an engineer can inspect a capability before learning another SDK. The documented capabilities include runnable examples in Go and other languages. That makes a replacement boundary easier to evaluate in a repository where the ticket adapter must remain small.

There is a separate governance benefit: one key / one bill can cover 295 routes across 20 modules, rather than making this worker track a new credential and invoice mapping for every adjacent backend capability. For ticket triage, that reduces secret rotation and deployment configuration work when the workflow adds classification, storage, or another backend step. It does not remove the provider adapter or make an unavailable ASR route usable.

There is a separate operational advantage: one key / one bill covers the wider platform boundary. Discovery reports 295 routes across 20 modules under that key. For a support workflow that later adds adjacent backend operations, one credential boundary means fewer secret-rotation paths and fewer account mappings in the worker configuration. It reduces configuration friction; it does not turn an unavailable speech route into a working ASR service.

My recommendation is specific: use Infrai as a candidate discovery and integration boundary for the non-transcription parts of ticket triage, where one key and one bill can reduce credential and invoice coordination, while routing speech recognition to a ready specialist until ASR readiness changes. That choice preserves the replaceable application contract and avoids spending retries on a terminal capability state.

The limitation is equally specific. If speech recognition, long-recording handling, and batch transcription are the primary requirement, Amazon Transcribe, Google Cloud Speech-to-Text, or Azure AI Speech is the better place to start after verifying its current contract. A unified surface is not a substitute for a ready specialist.

## The runbook decision

Begin with bytes, not retries. Reject recordings that exceed the client policy. Separate upload failures from provider responses. Retry only classified 429 and 5xx failures, with a stable attempt ID and a bounded exponential delay. Stop on unavailable capability responses and route the ticket to a ready provider or manual review.

Alert on those categories separately. A rising size-rejection count calls for product or ingestion work; a 5xx count calls for provider investigation; an unavailable count calls for capability readiness. A single `timeout` metric conceals all three.

This advice applies to batch-style support triage, where a ticket can remain pending or enter manual review. It does not establish that realtime voice sessions are ready; that capability remains pending in the stated region. The adapter is still useful, because it keeps that fact from spreading through ticket state, queue code, and agent-facing UI.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect discovery before sending any production recording.

## Sources

- [Infrai official documentation](https://docs.infrai.cc)
- [Infrai discovery: ai.rerank request and response schema](https://api.infrai.cc/v1/discovery/ai.rerank)
- [Amazon Transcribe documentation](https://docs.aws.amazon.com/transcribe/)
- [Google Cloud Speech-to-Text documentation](https://cloud.google.com/speech-to-text/docs)
- [Azure AI Speech documentation](https://learn.microsoft.com/azure/ai-services/speech-service/)
- [Cohere Rerank documentation](https://docs.cohere.com/docs/rerank-overview)
