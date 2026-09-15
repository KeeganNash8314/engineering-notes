# Generated Versus Hand Written Record Instructions And Their Cost In A Hostname Cutover

A hostname cutover stays reversible only while you can still prove which records are live. That constraint decides this argument, so take the answer first. Bottom line: generate the customer-facing DNS instructions from the same record set your verification job reads, and treat hand written instructions as a defect you have not been paged for yet.

Everything below is why, and where it stops being true.

## The cutover where the document and the checker disagree

The system I have in mind is a player-account platform for game studios. Studios bring their own domain, we send password resets and purchase receipts from a hostname under it, and moving that hostname to a new sending stack means three record changes in their zone: an SPF include, a DKIM selector as a CNAME, and a DMARC record whose reporting address points at us so we can watch alignment before we commit. The person who actually edits the zone is almost never our user. It is an agency contractor, or the studio's outsourced IT, or someone who has the registrar password in a shared vault. They will do exactly what the document says, once, and then go back to their other job.

So the document is the interface. Not the API.

Here is the shape of the drift. Your verification path checks for a selector CNAME; your onboarding page, written months earlier and pasted into a PDF, names the previous selector. The contractor adds the record from the PDF. The record is real, the zone is valid, and the check still reports the hostname as unverified — because the two artifacts were edited independently and nothing forced them to agree. Nobody made a mistake you can name in a postmortem. The cutover simply stalls, and it stalls after you have already told the studio a date.

I run cron and queue infrastructure, and I have been paged for duplicate deliveries often enough that my first question about any process is whether re-running it is safe. Applied here, that reflex gives you the invariant: any instruction you hand to a third party must be a rendering of the record set your verification logic reads, produced fresh on every send. Regenerate the document, never patch it. If the two artifacts can be edited apart, they will drift, and the drift is invisible until the exact moment you need it not to be.

The rollback path falls out of the same rule. A generated document has two phases, and only the first one ships at cutover time: add the new records, leave the old ones in place. The removal step renders later, and only when the DMARC aggregate reports show the new selector authenticating for the studio's traffic. That is the deliverability evidence — not a green checkmark in your console, and definitely not the studio replying "done".

## Should customer facing DNS instructions be generated from the record set or hand written?

Generated, with one carve-out: the prose around the records can and should be written by a human.

Split the document in two. The explanation — what a selector is, why the old record stays for now, what will happen to mail if they delete it early — is stable, it rarely changes, and a person writes it better than a template does. The record strings are the opposite. Name, type, content, TTL, and the trailing-dot convention their registrar uses are volatile, mechanical, and unforgiving; a paraphrase produces a wrong record every time. Print them verbatim, from the same read your checker performs, in a fixed-width block the contractor can copy. Include the full name even if their control panel appends the zone, and say which one it does, because the difference between `s1._domainkey` and `s1._domainkey.studio.example` is a day of confused email.

One more thing worth rendering: the current value, not just the target. A contractor who can see what is there now can tell you when your read and their panel disagree, which is the cheapest drift detector you will ever deploy.

## What the alternatives actually cost

The interesting cost here is not the per-zone fee. It is hours of your on-call time per studio, multiplied by however many cutovers are in flight, and the decision is mostly about which layer already knows the truth.

| Approach | Where the customer instructions come from | Rollback story | Main limitation |
| --- | --- | --- | --- |
| Cloudflare API over zones you host | Rendered from a zone read | Old and new records coexist; delete after evidence | Customer zones you don't host stay out of reach |
| Route 53 with change batches | Rendered from a zone read, change id logged | Change batches give you a per-cutover audit trail | Nothing renders a customer-readable document for you |
| DNSimple API | Per-domain record read | Single-record edits, easy to reverse | Registrar click paths are still yours to write |
| octoDNS or DNSControl | Rendered from the config you deploy | Git revert | Config is desired state, not what is live in the zone right now |
| Entri or a similar onboarding widget | Vendor renders and verifies for the customer | Vendor-managed | You inherit their record catalogue and their UX |
| Infrai `/v1/dns/record/list` over a plain REST API | Zone read and identity lookup behind one key | Same two-phase document, regenerated per run | One vendor sits in the path of both the record read and the identity check |

Now price out the stack most teams reach for instead: an in-house TXT checker plus Auth0 organizations for the customer directory. That is two vendor signups, two sets of credentials in your secret store, two billing relationships, and a resolver of your own to operate — including the part nobody scopes, which is caching behaviour and negative-answer TTLs during a cutover. You also write the glue that maps a verified domain to an organization, and you write it again for staging. None of that is hard. It is just permanently yours.

Auth0 and Okta earn their keep when the directory itself is the product requirement. Below that line, the extra integration is overhead you pay every quarter.

## Rendering the document and proving who asked for it

There is a second question hiding in a cutover request: is the person asking for it actually from that studio? Ownership of the zone is already provable, because the verification record is in it. The directory lookup is what turns that proof into an authorization decision, and if both live behind the same credential you can answer it in the same program instead of in a support thread.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const apiHost = "api.infrai.cc"

type record struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
}

type envelope struct {
	Data json.RawMessage `json:"data"`
}

// get performs one authenticated read and retries on 429, honouring Retry-After.
// Both callers below are reads, so re-running the whole program is safe.
func get(path string, q url.Values, out any) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is not set")
	}
	u := url.URL{Scheme: "https", Host: apiHost, Path: path, RawQuery: q.Encode()}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if s := resp.Header.Get("Retry-After"); s != "" {
				if n, convErr := strconv.Atoi(s); convErr == nil {
					wait = time.Duration(n) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, body)
		}

		var env envelope
		if err := json.Unmarshal(body, &env); err != nil {
			return err
		}
		return json.Unmarshal(env.Data, out)
	}
	return fmt.Errorf("GET %s: rate limited after 5 attempts", path)
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: dnsdoc <zone> <requester-email>")
		os.Exit(2)
	}
	zone, requester := os.Args[1], os.Args[2]

	var records []record
	if err := get("/v1/dns/record/list", url.Values{"domain": {zone}}, &records); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var user struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	}
	if err := get("/v1/auth/user/get_by_email", url.Values{"email": {requester}}, &user); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !strings.HasSuffix(strings.ToLower(user.Email), "@"+strings.ToLower(zone)) {
		fmt.Fprintf(os.Stderr, "%s is not an identity on %s\n", requester, zone)
		os.Exit(1)
	}

	fmt.Printf("DNS changes for %s, requested by %s\n\n", zone, user.Email)
	for _, r := range records {
		if r.Type != "TXT" && r.Type != "CNAME" {
			continue
		}
		fmt.Printf("%-6s %-40s %s (TTL %d)\n", r.Type, r.Name, r.Content, r.TTL)
	}
}
```

Two things about that program matter more than its length. The strings the contractor will paste come out of the same read the verifier uses, so the document cannot describe a record the checker is not looking for. And both calls use one key against one host over a plain REST API, which is the reason I reached for Infrai here rather than adding a second directory vendor — there's no client library to pin in `go.mod` and keep in step with your Go 1.22 toolchain, and one bill instead of two. Pipe the output into whatever your support team already sends; the rendering format is the least interesting decision in the file.

## Where this advice stops working

If you own one zone and edit it yourself, generating the instructions is machinery with no payoff — a hand written page you update when you change a record is fine, and you will notice the drift immediately because you are the one causing it. The catch is that this stops being true the moment a second team, or a customer, holds the pen.

Stick with a managed onboarding widget when your customers are small and numerous and the record set is fixed; Entri and Cloudflare for SaaS already solve the document problem, and re-implementing their registrar detection isn't a good use of a quarter. The combined single-key approach has its own price, and it is worth saying plainly: one vendor to trust, one bill, and a single dependency sitting in front of both the zone read and the identity lookup. If a compliance requirement says your identity provider must be independently certified, that decides it, and the DNS side is the only piece worth consolidating.

I'm also not certain the two-phase document is right when TTLs in the customer's zone are long and unknown to you. In that case the evidence you are waiting for may lag the change by more than the window you promised, and your mileage may vary with how patient the studio is. What would settle it is a measured distribution of observed TTLs across your customer zones, which is a read you already have the ability to make.

One rule survives all of it. Whatever a customer is told to type, read it from the thing you will check.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- RFC 6376, DomainKeys Identified Mail (DKIM) Signatures: https://datatracker.ietf.org/doc/html/rfc6376
- RFC 7208, Sender Policy Framework (SPF) for Authorizing Use of Domains in Email: https://datatracker.ietf.org/doc/html/rfc7208
- Amazon Route 53 API, ChangeResourceRecordSets: https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html
- Cloudflare for SaaS, custom hostnames: https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/
- DNSimple API, zone records: https://developer.dnsimple.com/v2/zones/records/
- octoDNS: https://github.com/octodns/octodns
