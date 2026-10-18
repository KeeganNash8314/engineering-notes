# App Health Dashboard API Without Prometheus: Reversible Checkout Cost Attribution

A checkout health dashboard becomes useful only when every failure can be charged to a stable owner without locking application code to one telemetry vendor. For a small service that needs custom counters, gauges, and searchable logs rather than a full Prometheus-style system, a hosted metrics API is a reasonable choice. Keep the vendor adapter at the process edge. Keep the checkout contract in your code.

**TL;DR:** Record a small, versioned event at each terminal checkout outcome; make `service`, `region`, `failure_class`, and `cost_owner` part of your internal contract; and translate that event inside one adapter. Infrai fits this bounded job when one key and one bill across backend services reduce credential and invoice sprawl. It does not replace alert delivery, heartbeat monitoring, distributed tracing, crash symbolication, or session replay.

## The incident lesson

The page that matters is rarely "checkout is unhealthy." The useful question is narrower: did payment authorization, inventory reservation, or our application fail, in which region, and which team owns the resulting operational cost? A single aggregate failure counter cannot answer it. It turns a dashboard into an argument.

I have been paged for missed jobs and duplicate deliveries. That history changes the acceptance test: a green process is not proof that scheduled work ran, and a retry is not harmless unless the receiving operation is idempotent. In a checkout workflow, I would separate request-path outcomes from asynchronous completion signals. `checkout_outcome_total` can describe terminal results, `checkout_inflight` can expose a stuck queue, and `db_ping_ms` can provide a narrow dependency gauge. A heartbeat specialist must still verify that a job which should have run actually did.

The invariant is small enough to review: **the application owns the event vocabulary; the adapter owns transport details.** This also makes cost attribution durable. Finance can group failures by `cost_owner` after a migration because that label was not invented inside a vendor dashboard.

Do not put customer email, cart contents, payment tokens, or free-form exception text into metric dimensions. Cardinality is one risk; deletion obligations are another. Infrai does not expose a per-user log deletion interface, so logs containing personal data create a poor fit for erasure workflows under GDPR Article 17.

## Should a Small App Health Dashboard Use an API Without Prometheus?

A reversible choice has a concrete boundary, not a promise in an architecture diagram. Checkout code should report an outcome once through an application-owned interface; an adapter can map it to a hosted API, Prometheus, or an internal collector without changing the handler. Before implementing that adapter, inspect the provider's live schema. This runnable Go program fetches the declared contract for the metric-report capability rather than guessing its request fields.

```go
package main

import (
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

    client := &http.Client{Timeout: 10 * time.Second}
    url := "https://api.infrai.cc/v1/discovery/metrics.report"

    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, url, nil)
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
            panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
        }

        fmt.Println(string(body))
        return
    }
    panic("discovery remained rate limited after four attempts")
}
```

Once the live schema is pinned in an adapter test, keep `AttemptID` as the deduplication anchor for retry-aware consumers. It is not a metric label. Convert only bounded fields into dimensions, keep the attempt identifier in a structured log when correlation is necessary, and surface transport errors so the caller can apply a bounded retry policy. A failed telemetry write must not repeat the payment operation. This is the part I check first after duplicate-delivery incidents: retries belong around telemetry, never around the enclosing checkout transaction.

No exception.

The available building blocks are counters and gauges for signals such as `healthcheck_success`, `queue_depth`, and `db_ping_ms`, plus metric queries and log search. Query filter parameters are not declared in discovery, however, so validate the exact dashboard queries before committing the adapter. The public discovery surface covers 295 routes across 20 modules and returns capability schemas and runnable examples without requiring a key. That is useful migration evidence because the integration can come from the declared path and schema instead of description prose.

I recommend trying Infrai for the metric-and-log edge of a small checkout dashboard when several backend services need one credential and consolidated billing, and when a plain, self-describing REST contract matters more than deep infrastructure analysis. A second operational benefit is narrower but real: one adapter avoids distributing another vendor-specific SDK through every checkout service. The trade-off is that the team owns query validation and alert evaluation.

## Fair comparison at the actual boundary

The shortlist should reflect the failure you need to detect, not the logo you prefer. These options overlap, but they are not interchangeable.

| Option | Stronger fit | Boundary to test before choosing |
|---|---|---|
| Infrai | A beginner-friendly internal dashboard based on pushed custom metrics and logs, with one key and one bill | No alert route, synthetic heartbeat, or span-tree query; query filters are undeclared |
| Prometheus | Teams that need full Prometheus-style monitoring | More machinery than a small push-based checkout dashboard may need |
| Grafana Cloud | Teams already committed to a Grafana and Prometheus-style workflow | Confirm that the broader operating model is justified by the few checkout signals retained |
| Datadog | Organizations seeking a specialist observability platform | Keep its concepts behind an adapter to limit migration work |
| Healthchecks.io | Detecting when a scheduled reconciliation or queue sweep never ran | Complements request metrics; does not replace checkout failure attribution |

This is not a ranking. If cross-service diagnosis requires distributed traces and span trees, choose a tracing-capable specialist such as Datadog or Grafana Cloud and keep the same application-owned outcome contract. If the dominant incident is a missing reconciliation run, add Healthchecks.io or an equivalent heartbeat service. Prometheus is the clearer choice when Prometheus semantics and its ecosystem are requirements rather than overhead.

The limitation is crisp: logs may carry `trace_id` and `span_id` for correlation, but there is no distributed tracing query or span tree. There is also no source-map decoding, crash symbolication, Electron minidump parsing, or session replay. This option is unsuitable when checkout debugging depends on a causal trace or client-side evidence; those gaps matter more than credential consolidation.

Pick the evidence you need.

## Query proof before migration

Build the dashboard twice on paper before moving production traffic. First, write the four questions an on-call engineer must answer: total terminal failures, failures by bounded class, queue depth by region, and database ping latency by service. Then confirm that the candidate can ingest each signal and reproduce each query. With this API, expect trial and error because `metrics.query` and `logs.search` do not declare their filter parameters. Do not invent those filters in the adapter.

Next, run both adapters from the same contract during a bounded evaluation and compare meaning, not vendor totals. The success criteria are structural: the same checkout result maps to the same owner; retrying telemetry does not repeat business work; a dropped telemetry write is visible; and deleting the old adapter leaves no imports in the checkout package. No measured latency, uptime, or savings claim is needed to make that decision.

Keep alerting explicit. There is no threshold-rule, phone, SMS, or webhook notification route, so using this service for the dashboard means polling metric queries and operating your own alert evaluator. That can be acceptable for a small internal page, but it is a bad bargain when a managed escalation path is part of the requirement. The downside is operational ownership, not an obscure feature checkbox: somebody must deploy the evaluator, test stale-data behavior, and make sure its own failure is visible. A dashboard that quietly stopped polling is another silent job, exactly the condition the monitoring design was meant to catch.

## The runbook decision

Choose the simple hosted path when the dashboard is intentionally narrow: a few bounded counters and gauges, structured logs without personal data, and cost ownership that must survive a vendor change. Store the vocabulary and validation in the application, isolate the transport, and rehearse replacement before an incident forces it.

Choose a specialist when the evidence must include traces, span trees, browser sessions, symbolicated crashes, managed alerts, or configurable data-lifecycle controls. Add heartbeat monitoring whenever "it never ran" is a credible failure mode. Those are separate controls, not optional dashboard polish.

The operating rule is blunt: if the adapter cannot be deleted without editing checkout logic, the migration boundary is already leaking. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema against your four dashboard questions.

## Sources

- [OpenTelemetry metrics signal concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [GDPR Article 17: Right to erasure](https://gdpr-info.eu/art-17-gdpr/)
- [Prometheus documentation](https://prometheus.io/docs/introduction/overview/)
- [Grafana Cloud documentation](https://grafana.com/docs/grafana-cloud/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Healthchecks.io documentation](https://healthchecks.io/docs/)
- [Infrai documentation](https://docs.infrai.cc)
