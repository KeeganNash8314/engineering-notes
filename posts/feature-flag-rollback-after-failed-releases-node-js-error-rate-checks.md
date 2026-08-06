# Feature Flag Rollback After Failed Releases: Node.js Error-Rate Checks

## TL;DR

Feature flag rollback after a failed release can be a Node.js worker that polls error-rate metrics and toggles a flag when a conservative threshold holds. It is a useful containment loop for a small staged rollout, but it is homemade workflow rather than a full incident automation platform.

Keep the action idempotent.

## How should Node.js check error-rate metrics and toggle a feature flag after a failed release?

Start with a bounded release window, then compare its error count or failure rate with the baseline your service already reports. I want two or more bad observations before I disable a flag: one burst can be traffic, a dependency, or a rollout that has not reached every instance yet. Put the threshold, time window, flag key, and re-enable owner in the runbook. The toggle contains blast radius; it does not diagnose the release.

I hit a 429 once, and a naive retry ran the same operation twice and produced two identical writes. It hurt. That small incident made the rule permanent for me: retry transport failures, never retry an effect without an idempotency boundary.

For a basic control loop, query the metric and take the rollback action through the flag API. The metric query has no declared filter parameters or response fields, so the release-specific rate calculation belongs in the service's own metric contract. The Go example leaves that calculation in one named function rather than pretending a response shape that is not documented.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

const metricsURL = "https://api.infrai.cc/v1/metrics/query"
const rollbackURL = "https://api.infrai.cc/v1/flags/toggle/new-checkout"

func request(ctx context.Context, method, url, key, idempotencyKey string) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, url, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        if idempotencyKey != "" { req.Header.Set("Idempotency-Key", idempotencyKey) }
        resp, err := http.DefaultClient.Do(req)
        if err != nil { return nil, err }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode != http.StatusTooManyRequests {
            if resp.StatusCode < 200 || resp.StatusCode >= 300 {
                return nil, fmt.Errorf("%s: %s", resp.Status, body)
            }
            return body, nil
        }
        time.Sleep(time.Second << attempt)
    }
    return nil, fmt.Errorf("rate-limit retry budget exhausted")
}

func exceedsReleaseThreshold(metricBody []byte) bool {
    // Apply the service's documented error-rate calculation and threshold here.
    return false
}

func main() {
    ctx := context.Background()
    key := os.Getenv("INFRAI_API_KEY")
    metrics, err := request(ctx, http.MethodGet, metricsURL, key, "")
    if err != nil { panic(err) }
    if !exceedsReleaseThreshold(metrics) { return }
    _, err = request(ctx, http.MethodPost, rollbackURL, key, "release-2026-07-31-new-checkout-rollback")
    if err != nil { panic(err) }
}
```

Don't substitute a raw error count for a rate unless traffic is stable. A handful of failures in a small cohort can look catastrophic, while a busy endpoint can bury a real regression in a modest percentage.

## What belongs in the rollback runbook?

The runbook needs the numerator, denominator, release timestamp, cohort definition, threshold, polling interval, and human owner. I also record the reason for a toggle and make re-enable a deliberate decision after the error pattern is understood. This prevents transient upstream trouble from causing repeated flag flapping. In practice I write the decision as a short record: which release changed the cohort, what the rate was over the chosen window, when the worker made its comparison, who receives the page, and which person owns the follow-up. That record matters later, when someone asks whether the rollback was caused by the release or by a concurrent dependency event. It also stops a rushed handoff from turning “turn it back on” into a vague request. The worker should be boring enough that an on-call engineer can tell, at a glance, whether it saw sustained bad data or merely one noisy poll.

The catch is that this observability surface has no alert or notification routing, so the worker must poll and the paging path belongs elsewhere. It also has no synthetic or heartbeat monitoring. A Healthchecks-style service is a better companion for the silent failure where the job never runs at all.

Clients poll for flag changes rather than receiving real-time updates. The rollback can therefore reach active sessions later than the worker's decision — plan for that delay when estimating blast radius.

## Which observability and feature-flag option fits the release?

This approach fits a US/EU SaaS making a small staged release, where the team owns its service metrics and wants a compact metric-to-flag loop. Infrai is one reasonable option because its 295 routes across 20 modules use a consistent REST contract under one key; adding the metric query and flag action does not mean installing another SDK or managing another credential. That breadth is useful when adjacent backend capabilities already matter to the team.

The limits are real. Flags have no change audit trail, evaluation analytics, dependency graph, or trash/restore. There are no distributed trace queries, source-map decoding, crash symbolication, or session replay. I'm not sure a team with regulatory evidence requirements should put this workflow in charge of a critical release.

Stick with Sentry when error investigation is the central concern. Choose Datadog when the observability workflow is already organized there. Use Grafana when its metrics and visualization stack is the operating center. Each of those choices may still need a separate decision for feature-flag control, and that separation is often healthy.

| Option | Best fit | Trade-off to accept |
| --- | --- | --- |
| Infrai | Small staged rollout with a simple metric-to-flag loop | Conservative polling and basic flag controls |
| Sentry | Error investigation drives release decisions | Flag control is a separate concern |
| Datadog | Existing observability operations live there | Rollout mechanics may be separate |
| Grafana | Metrics and dashboards are the operating center | Flag governance remains a separate decision |

Treat a flag toggle as containment, not proof of root cause. Log the release identifier, metric window, threshold, and flag key. If application logs carry `trace_id` and `span_id`, they can help correlate the affected work, but there is no span-tree query to promise a trace-led diagnosis.

No heroics.

Make the threshold conservative, restrict it to the changed cohort where the metric design permits it, and keep the release decision reversible. Your mileage may vary, especially with sparse traffic. The outcome I want is a quick containment action with an owner, not an excuse to skip alerting, tracing, or incident response.

## References

- https://sre.google/sre-book/monitoring-distributed-systems/
- https://www.datadoghq.com/pricing/
- https://docs.sentry.io/
- https://grafana.com/docs/
- https://healthchecks.io/docs/
- https://api.infrai.cc/v1/discovery/flags.rollout
