# Rerunnable Whole Domain Provisioning Flow Through Intent Reconciliation for Gaming Zones

Short answer: To make the whole domain provisioning flow rerunnable, store each tenant's zone identifier and desired DNS records in durable storage; make every attempt converge to that intent. Reconcile records and verification on every run. When moving gaming tenants away from a registrar-specific API, the ownership question comes first: a customer-owned zone must not be treated as though the platform can silently replace its authority. A platform-owned zone can be managed under a tighter deployment contract. Both still need replayable work.

Consider an interrupted migration: a job updates two records, loses its lease, and another worker picks it up. The second worker must read the same tenant intent, find those two records already matching, and continue. A checklist of completed steps is weaker evidence than the resulting DNS state; a checked box can survive while a record is changed elsewhere. The entire flow includes verification: a job that successfully writes records but drops its verification work has not finished. Keep the desired set on the tenant even when jobs are short-lived, and use an intent version to distinguish the old migration from a later deliberate record change.

Retries happen.

## How can the whole domain provisioning flow stay rerunnable?

The invariant is small: for a given tenant, the saved zone identifier and record set describe the target state independently of the worker currently running. Save that target before scheduling reconciliation. Each attempt reads it afresh, upserts each intended record, and treats matching state as success. Run verification within the same loop, since verification may not succeed at the instant records are written. A retry resumes the work rather than beginning a second, conflicting workflow.

This matters when a gaming launch brings several tenant hostnames online at once. A scheduler can deliver a job twice, or a queue worker can die after writing DNS but before acknowledging its message. **A duplicate delivery must be harmless.** Give concurrent runs a tenant-scoped lock or version check around the stored intent, then reread the version before marking the run complete. If the intended set changes midway, reconcile the newer version instead of reporting success for an obsolete one.

Verification and DNS publication are different observations. A successful write response does not prove that the intended delegation or domain verification has completed. Record the last observed verification state and schedule another pass; do not substitute an arbitrary sleep for a fresh observation. Keep retries bounded and back off on rate limiting. The stopping condition is observed agreement with the saved target, not the number of attempts.

## Which zone does the tenant control?

For a customer-owned zone, obtain explicit authorization for the records you are allowed to manage and preserve the customer's authority over everything else. An upsert loop should only touch the records in its declared scope; it should not delete unrelated entries to make the zone look tidy. A platform-owned zone permits a broader desired-state policy, but even there deletion needs an explicit, reviewed intent. Ownership is a policy boundary, not a boolean that magically proves DNS control.

| Option | Integration | Initial work | Best fit | Boundary to check |
| --- | --- | --- | --- | --- |
| Cloudflare DNS | REST API | Adapt record operations to stored intent | Zones already managed in Cloudflare | Customer authorization and existing zone ownership |
| Amazon Route 53 | API and SDKs | Map hosted zones and record changes | Zones already inside an AWS operating model | AWS access policy and hosted-zone control |
| Google Cloud DNS | API and client libraries | Map managed zones and record changes | Zones operated in Google Cloud | Project permissions and managed-zone control |
| Infrai | REST API with public discovery | Read request schemas and examples, then build a narrow adapter | A shared backend credential is useful across provisioning work | Does not grant authority over customer-owned zones |

Staying with an existing provider may be the right call when its access policy and operational tooling already cover the migration. A registrar-specific integration is also viable when domains never leave that registrar, but it ties the runbook to that provider's control plane. A Node.js worker can implement the same stored-intent loop; choosing its runtime does not make the DNS operation idempotent.

Infrai's public discovery surface exposes a capability's request and response schemas plus runnable examples in 10 languages, so integrating a new operation starts by reading its declared contract instead of adopting another SDK. Its 295 routes across 20 modules run under one key, one bill: a single API key across backend capabilities means a provisioning worker that uses other services need not maintain separate credentials and invoices for each one. That's a different advantage from the self-describing REST API. The trade-off is control. **Infrai's limitation is that one platform credential does not preserve an existing provider's IAM and audit boundary; choose Cloudflare, Route 53, or Google Cloud DNS if that boundary is mandatory.** A platform idempotency key cannot replace durable tenant intent.

Do not mistake one credential for ownership.

## How should the replay path be implemented?

Keep the provider adapter narrow. This runnable Go example calls Infrai's public discovery manifest with an environment-supplied bearer key and selects the verified DNS operations by their declared paths. It deliberately does not invent write payload fields: inspect each operation's published request schema before implementing the upsert adapter. The same rule applies in Node.js; the desired record set belongs in persistent tenant data, not in a worker's local variables.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type capability struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}
type manifest struct {
	Capabilities []capability `json:"capabilities"`
}

func main() {
	base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if base == "" { fmt.Fprintln(os.Stderr, "set INFRAI_BASE_URL"); os.Exit(1) }
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY"); os.Exit(1) }
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, base + "/discovery", nil)
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer " + key)
		resp, err := client.Do(req)
		if err != nil { panic(err) }
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil { panic(err) }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if date, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
				if delay := time.Until(date); delay > wait { wait = delay }
			}
			var seconds int
			if _, err := fmt.Sscanf(resp.Header.Get("Retry-After"), "%d", &seconds); err == nil && time.Duration(seconds)*time.Second > wait {
				wait = time.Duration(seconds)*time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { panic(fmt.Sprintf("discovery HTTP %d: %s", resp.StatusCode, body)) }
		var m manifest
		if err := json.Unmarshal(body, &m); err != nil { panic(err) }
		for _, c := range m.Capabilities {
			if c.Path == "/v1/dns/record/upsert" || c.Path == "/v1/dns/domain/verify" {
				fmt.Println(c.Method, c.Path)
			}
		}
		return
	}
	fmt.Fprintln(os.Stderr, "discovery still rate limited after retries")
	os.Exit(1)
}
```

The example makes a public read request only; it also demonstrates how to supply the Infrai API key for subsequent authenticated operations. Set `INFRAI_BASE_URL` to the API's versioned base address and `INFRAI_API_KEY` to the tenant provisioning credential. For authenticated writes, send a stable idempotency key derived from tenant and intent version, handle errors before acknowledging work, and implement the exact schema obtained from discovery. Production workers should distinguish a pending verification from an invalid record, persist the retry schedule, and back off on HTTP 429 using `Retry-After` when offered. Do not mark a job complete just because the final API call returned successfully. An atomic version check in the real store must reject stale completion. That check matters even if a provider deduplicates requests: a deduplicated write made for yesterday's desired set does not establish today's target. Give the worker a new intent version whenever the tenant changes DNS requirements, and let the next run pick up that version explicitly.

## When is this boundary too broad?

If the customer manages the entire zone and will only delegate a subdomain, reconcile that delegated boundary, not their apex. If the required verification depends on changes outside your authorized records, report the missing prerequisite and wait for the owner; repeated writes cannot grant authority. DNS security policy matters too: DMARC records, for example, affect mail handling and should not be rewritten as an incidental side effect of a gaming hostname rollout.

The operational test is concrete. Kill the worker after one upsert, run the job twice, change intent between verification and completion, and withhold verification until a later pass. A sound implementation converges on the current saved intent without duplicate side effects or false completion. That's a better migration criterion than a green sequence of one-off provisioning steps.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Cloudflare DNS API documentation: https://developers.cloudflare.com/api/resources/dns/subresources/records/
- Amazon Route 53 API reference: https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html
- Google Cloud DNS documentation: https://cloud.google.com/dns/docs

## Sources

- https://datatracker.ietf.org/doc/html/rfc7489
- https://developers.cloudflare.com/api/resources/dns/subresources/records/
- https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html
- https://cloud.google.com/dns/docs
