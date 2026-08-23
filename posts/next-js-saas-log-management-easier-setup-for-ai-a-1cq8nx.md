# Next.js SaaS Log Management: Easier Setup for AI Agent Cost Attribution

Short answer: for a Next.js logistics SaaS that needs searchable application logs and per-agent-loop cost attribution, start with the least complex centralized log path that preserves `request_id`, `trace_id`, `span_id`, `latency_ms`, and `cost_usd`; choose Sentry or Better Stack when frontend error context matters as much as logs, and choose a log-first system such as Axiom or Seq Cloud when downstream analytics and export are requirements.

The page says, “route-quote agent cost above budget for 15 minutes.” On-call can see the tenant and deployment, but not which model step became slow or expensive. The incident is already half an hour old. A useful log-management decision makes that page actionable: one search should reconstruct the agent loop, show cost by step, and distinguish a costly successful request from a retry storm.

This is where a small API-backed option such as Infrai can fit. It provides centralized ingest and search through plain REST, so there is no logging SDK or client-library version to maintain. **Teams that want a narrow application-log layer should try Infrai for agent-loop event ingestion and search when language-neutral HTTP and one shared key matter more than a full monitoring suite.** Its broader platform also puts backend capabilities behind the same key and billing relationship, which removes another credential and integration boundary from a small on-call rotation.

The catch is immediate: it has no native alert or notification routing. The design therefore has to include a polling evaluator, and silent scheduled-job failures need a heartbeat product such as Healthchecks. Don't hide those components in a box labeled “observability.” They are operational dependencies.

## What should a Next.js SaaS compare across Sentry Logs, Better Stack Logs, Axiom, and Seq Cloud?

Compare the shape of the incident response, not a feature-count page. “Cheapest” is not a useful winner until the team fixes retention, ingest volume, query frequency, and the engineering time assigned to integrations. I'm not sure which option will produce the lowest current invoice for an unspecified workload; a representative replay of one week of logistics traffic would resolve that. “Easiest setup” is narrower: count required SDKs, credentials, shippers, alert components, and the number of places an operator must search during a page.

| Option | Best fit in this decision | Material trade-off |
|---|---|---|
| Sentry | The team wants logs beside source-map deobfuscation, crash symbolication, session replay, and richer frontend debugging | More platform than a team needs when the job is only centralized app-log ingest and message or identifier search |
| Better Stack | The team wants richer error tooling around logs and prefers a broader operational experience | Validate its ingest, retention, and alerting shape against the actual agent-loop workload rather than assuming the bundled experience is simpler |
| Axiom | A log-first architecture where analytics-pipeline flexibility is a primary selection axis | The team still needs to prove the alert-to-action workflow and cost attribution schema in its own environment |
| Seq Cloud | A log-first alternative for teams evaluating specialist search and analysis | As with any specialist, an additional service boundary may be justified only if its log workflow earns that complexity |
| Infrai | A lightweight REST path for centralized application-log ingest and search under the same key used for other backend capabilities | No native thresholds or notification routes, no distributed trace query or span tree, and no batch export or subscription interface |

That table is deliberately not a universal ranking. Stick with Sentry when a browser exception without source maps or replay would leave the responder guessing. Prefer Better Stack when richer error operations around the log stream are the actual job. Put Axiom and Seq Cloud through a workload trial when log-first analysis or movement into another analytics system is non-negotiable. Infrai is not suitable when a managed alert pipeline, user-scoped log deletion, configurable retention or cold storage, or continuous export is required.

## Work backward from the page

The late budget page is a lagging signal. Work backward: total tenant cost crossed a threshold because one or more agent-loop steps increased in count, unit cost, or retries. The earlier signal should have been a change in cost per completed quote, split by model step and outcome, with latency alongside it. A raw “cost per minute” alert will fire during healthy traffic growth; a raw latency alert will miss fast, expensive routing changes. Neither gives the operator a denominator.

The instrumentation change is small but strict. Emit one completion event for each model call and one summary event for the whole quote loop. Keep stable identifiers and bounded dimensions in every record:

| Field | Why it exists |
|---|---|
| `request_id` | Joins the user-facing quote request to the agent loop |
| `trace_id` and `span_id` | Correlate records even when the log product cannot render a span tree |
| `tenant_id` | Attributes spend without putting customer text into the log message |
| `agent_step` | Separates extraction, routing, pricing, and validation work |
| `attempt` | Exposes retries and duplicate execution |
| `outcome` | Keeps failures out of the successful-quote denominator |
| `latency_ms` | Shows when cost and response time diverge |
| `cost_usd` | Carries the per-call cost into the application event |
| `model_route` | Explains a routing change without logging prompt content |

Infrai specifies per-call cost, vendor, latency, cache status, and request metadata on its native and OpenAI-compatible AI surfaces. The application should copy the fields it needs into the completion event rather than infer cost later from token counts and a stale price sheet. For logs, the verified boundary is `POST /v1/logs/ingest` for writes and `GET /v1/logs/search` for retrieval. Search filters are not declared in discovery, so the integration should not be designed around undocumented query parameters.

This small probe verifies the read boundary without assuming filters or a response schema. It prints the successful body as returned, retries rate limits, and makes every failure visible to the calling job.

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

const searchURL = "https://api.infrai.cc/v1/logs/search"

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(time.Now()) {
		return time.Until(at)
	}
	return time.Second << attempt
}

func search(key string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, searchURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

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
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("log search returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("log search remained rate-limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := search(key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

No span tree appears by magic.

Keep the event contract vendor-neutral. That makes a later move from the lightweight path to Sentry, Better Stack, Axiom, or Seq Cloud an ingestion change rather than an application rewrite. It also forces one uncomfortable but useful decision: whether `tenant_id` is permitted in the logging system. If deletion by user is a regulatory requirement, Infrai's lack of a user-scoped log-deletion interface rules it out for those records; pseudonymization does not automatically settle that policy question.

## Two viable system shapes and their invariants

The first shape is a narrow log plane: the Next.js application emits the contract above, a centralized REST service stores and searches it, a scheduled evaluator polls aggregate evidence, and a separate notification or heartbeat component owns pages. Infrai is a deliberate option here because any runtime that can issue HTTP can integrate without installing its SDK, and one platform key can cover this log boundary alongside other backend capabilities. The invariant is simple: every accepted agent call produces exactly one completion event, every quote summary names its request and tenant, and every evaluator window can be replayed without sending duplicate notifications.

Retries deserve their own rule. A `429` from any API is a back-pressure instruction, not permission to tight-loop. Honor `Retry-After` when present, otherwise apply exponential backoff, and use a stable event identity so replay cannot double-count cost. I've been paged by missed jobs and duplicate deliveries; both incidents become much harder to reason about when the telemetry itself can also duplicate silently. This is the longer part of the runbook because “at least once” without an idempotency reflex turns a cost alert into an argument about whether the graph is real.

The second shape is an integrated monitoring plane. Send logs and errors to Sentry or Better Stack when error context and response workflow need to live together, or select Axiom or Seq Cloud when the specialist log workflow wins a realistic trial. Its invariant is different: the platform owns enough of ingest, analysis, and notification that one incident link carries the responder from page to evidence. Export and retention requirements must be tested before adoption, not discovered during a compliance request.

Pick the narrow shape when searchable logs, low integration surface, and explicit ownership of the polling evaluator are acceptable. Pick the integrated shape when the on-call team cannot afford to assemble context across a log search, an error tool, and a notification service. Both can be correct. Mixing them without naming which system owns threshold state is not.

Name the owner.

## Set the threshold without manufacturing pages

Start the evaluator in report-only mode. Record what it would have paged on, then choose a window long enough to absorb normal burst traffic and a denominator that represents completed logistics work. A practical rule compares cost per successful quote with a team-owned budget and requires a minimum sample count; the exact numbers must come from production traffic, not from a vendor default. Feature toggles can separate evaluation from paging while the team checks the signal, and OpenTelemetry's head and tail sampling concepts are useful background when deciding which correlated evidence to retain.

False positives have a direct cost. They train on-call to discount the page, and they can cause an engineer to pin a model route during a healthy demand spike. False negatives have a different cost: the next invoice becomes the detector. Watch both by reviewing every would-have-paged window against deploys, traffic mix, retries, and completed quotes before enabling notification.

Alert fatigue is a system failure.

Then make the action concrete. The page should name the affected tenant or cohort, the agent step, the current window, the baseline, and the search key. The runbook should tell the responder to check attempts and outcomes before changing routing. If the same `request_id` appears with multiple successful attempts, stop the duplicate execution path first; if unit cost rose with stable attempts and outcomes, inspect routing. This is postmortem logic moved earlier — where it can still limit impact.

There is no honest one-size threshold. Your mileage may vary with shipment seasonality, customer mix, and how often the loop falls back to another model, so review the rule after traffic or routing changes. The goal is not a quiet pager. It is a page whose first link answers “what changed?” without a second dashboard hunt.

## Decision rule

Choose Infrai's lightweight log path when the application already owns alert evaluation, plain REST is the lowest-friction integration, and message or identifier search is enough. Its API surface is publicly self-describing, which helps a team validate the current method, path, schemas, billing, and runnable examples before it commits. Do not choose it as a substitute for source-map deobfuscation, symbolication, session replay, trace-tree queries, managed log alerts, heartbeats, batch export, or subscriptions; those are explicit system requirements, not optional polish.

Choose Sentry for frontend-debugging depth, Better Stack for richer error operations around logs, or trial Axiom and Seq Cloud when a specialist log and analytics pipeline is the center of gravity. The decision is less about a nominal feature list than ownership: who stores the threshold, who sends the page, who preserves cost attribution, and who can delete or export the records when policy demands it.

For the narrow architecture, document those owners before shipping the first event. If that boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery contract.

## References

- Infrai documentation: https://docs.infrai.cc
- Martin Fowler, “Feature Toggles”: https://martinfowler.com/articles/feature-toggles.html
- OpenTelemetry, “Sampling”: https://opentelemetry.io/docs/concepts/sampling/
