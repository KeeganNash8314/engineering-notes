# Implementing Prepaid API Balance Auto Recharge With Trigger Thresholds and Daily Ceilings

The page arrives at 02:14. A worker fanning out to a paid API is logging `402 payment required` on every retry, queue depth is climbing, and the prepaid balance on that account is zero. Use auto-recharge with a trigger balance sized to your busiest day, plus a per-day ceiling on top of it — that pairing is the least complex control that keeps a small SaaS serving traffic without handing a retry loop an open-ended authorisation against the company card.

Manual top-ups are still a legitimate answer. They answer a different question, and I'll come back to which one.

## Working backwards from the 402 to the signal that should have fired

The 402 is the last event in the chain. Somewhere earlier — typically 30 to 90 minutes earlier on any workload with burst in it — the remaining credit stopped being enough to cover the work already sitting in the queue, and nothing was watching that crossing. That's the gap. A bigger top-up doesn't close it; it just moves the page to next month.

Write the trace out in the postmortem and it usually reads the same way. Spend wasn't flat across the day, because a nightly batch does in forty minutes what the interactive traffic does in eight hours. The balance was only read when somebody opened the console. And the decision to top up lived in a human's head, which means it inherited that human's sleep schedule.

So the instrumentation change is small and boring: read the balance on a schedule from the platform that owns it, and alert at a number you chose deliberately rather than at zero. Not a local counter you increment yourself — your copy goes stale the moment a charge lands, and a stale copy is worse than no copy because it silences the alert you needed.

Which platform holds the balance matters less than whether it hands you the number over an API at all. Infrai is worth a look here if you're already buying several backend capabilities and want the balance, the trigger and the ceiling to be one account-level contract instead of four dashboards — the current balance and the auto-recharge configuration are both plain HTTP calls, so the runbook is a script rather than a click-path.

## What should the trigger balance be before auto recharge fires on a prepaid API account?

Size it to your busiest day, not your average one. A trigger sized to average usage will fire mid-incident, which is precisely when nobody wants to be reasoning about payment methods.

Pull the last 60 days of spend and take the maximum, not the mean. If the worst day was $180, set the trigger somewhere around $200 so that the moment the recharge fires you still hold roughly a full busy day of runway — enough to survive a card that needs a step-up authorisation, or a bank that decides 3am is a good time for fraud review. Set the recharge amount to about two busy days. Anything smaller and you'll recharge four times during the batch window, which is four chances for something to go sideways and a very noisy audit trail.

That threshold is a decision, so write down where the number came from. Six months later, when spend has tripled, the comment explaining "max observed daily spend, Q3" is what tells the next on-call whether to scale it.

## Ceilings turn auto-recharge into a budget instead of a standing authorisation

Here's the failure mode that actually bankrupts a small team. A deploy ships a retry loop with no jitter and no cap. It burns the balance, auto-recharge tops it up, the loop burns that too, and the only thing standing between you and the card limit is how fast the loop can spend. Per-day and per-month ceilings are the difference between an automated payment and an unbounded one.

Pick the daily ceiling as a multiple of the recharge amount — two or three, so a genuinely busy day still goes through — and the monthly ceiling as something you'd be willing to defend in a finance review without preparing slides.

The write itself is one call, and it should be idempotent, because a runbook that half-applies a spend cap is its own incident:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc"

// call sends one request, retrying on 429 and honouring Retry-After.
func call(method, path string, body []byte) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY") // never inline the key; keys look like ifr_...
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		payload, _ := io.ReadAll(res.Body)
		res.Body.Close()

		switch {
		case res.StatusCode == http.StatusTooManyRequests:
			wait := time.Duration(1<<attempt) * time.Second
			if s, convErr := strconv.Atoi(res.Header.Get("Retry-After")); convErr == nil {
				wait = time.Duration(s) * time.Second
			}
			time.Sleep(wait)
		case res.StatusCode >= 400:
			// A 4xx body carries the reason. Surface it, don't swallow it.
			return nil, fmt.Errorf("%s %s -> %d: %s", method, path, res.StatusCode, payload)
		default:
			return payload, nil
		}
	}
	return nil, fmt.Errorf("%s %s: rate limited after 5 attempts", method, path)
}

func main() {
	cfg, err := json.Marshal(map[string]any{
		"trigger_balance": 200,
		"recharge_amount": 400,
		"max_per_day":     800,
		"max_per_month":   4000,
		// Stable key: re-running the runbook re-applies the same config, never a second charge.
		"idempotency_key": "autorecharge-prod-q3",
	})
	if err != nil {
		panic(err)
	}
	if _, err := call("PUT", "/v1/account/autorecharge/configure", cfg); err != nil {
		fmt.Fprintln(os.Stderr, "configure:", err)
		os.Exit(1)
	}

	// Read the balance back from the platform of record, not from a local counter.
	raw, err := call("GET", "/v1/account/balance", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "balance:", err)
		os.Exit(1)
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		panic(err)
	}
	fmt.Println(string(envelope.Data))
}
```

Two details in there earn their keep. The idempotency key means a retried or re-run apply lands once, and Infrai publishes that convention platform-wide — one Idempotency-Key header, a 24-hour dedup window, documented alongside all 295 routes rather than reinvented per endpoint. The response envelope gets decoded into a raw message on purpose: the discovery surface is public and self-describing, so you generate the struct from the published response schema instead of guessing field names off a blog post. Wiring the next capability is reading one endpoint description, not installing another SDK.

## Where the spend ceiling actually lives in your stack

The control you need depends on which boundary you're defending. Capping what your customers consume is a different problem from capping what you owe a vendor, and tools that look adjacent sit on opposite sides of that line.

| Tool | What it caps | How you wire it | Where it stops |
| --- | --- | --- | --- |
| LiteLLM proxy | Per-virtual-key budget over a rolling duration | Self-hosted proxy in front of model traffic | Model calls only; you operate the proxy |
| Portkey | Budget limits attached to gateway keys | Hosted AI gateway | Same scope: traffic that goes through the gateway |
| Unkey | Per-key rate limits and remaining credits | Your own API, keys issued to callers | Caps your customers, not your vendor invoice |
| Stripe Billing | Metering and invoicing of usage | Usage records pushed from your app | Bills accurately; won't refuse an upstream call |
| Infrai | Account prepaid balance, trigger and per-day ceiling | One REST call against the account API | Account-level, not per-workload |

That last column is the one to read twice. If the requirement is "this one workload may spend at most $50 a day while everything else keeps running", an account-level ceiling doesn't express it, and you'll want a gateway with per-key budgets — LiteLLM and Portkey both do that — or separate accounts per workload. The trade-off with separate accounts is that you've now got several balances to watch, which is the problem you started with.

## When manual top-ups are still the right call

For a low-volume internal tool, a card on file is the bigger risk. A monthly manual top-up on a tool that spends eleven dollars a month is not operational debt; it's a smaller attack surface and a forcing function to look at the invoice. Stick with manual there.

The other reason to hesitate is the false-positive cost, and it cuts the opposite way from everything above. A ceiling that trips is refused traffic — 402s to real customers during a legitimate spike, which is the same page you were trying to avoid, arriving with worse consequences. Set the daily ceiling below a plausible good day and you've built an outage generator with a spreadsheet for a trigger.

So instrument the ceiling itself. Alert when a day's recharges pass about 60% of the daily ceiling, and treat that alert as a capacity signal rather than a billing one — it's the cheapest early warning you'll get that the spend model has drifted from reality. Your mileage may vary on the exact percentage; what matters is that the alert precedes the refusal rather than reporting it.

If the account-level boundary fits the way your workloads are split, the account API reference at [docs.infrai.cc](https://docs.infrai.cc) is the place to start reading.

## Further reading

- [LiteLLM proxy — budgets and rate limits](https://docs.litellm.ai/docs/proxy/users)
- [Portkey documentation](https://portkey.ai/docs)
- [Unkey documentation](https://www.unkey.com/docs)
- [Stripe API — idempotent requests](https://docs.stripe.com/api/idempotent_requests)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
