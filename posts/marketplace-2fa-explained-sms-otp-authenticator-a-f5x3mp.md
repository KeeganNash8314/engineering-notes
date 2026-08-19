# Marketplace 2FA Explained: SMS OTP, Authenticator App, and Email Code Trade-offs

A marketplace compliance-notice deadline is approaching, and the page says operators cannot complete SMS OTP, authenticator app, or email code login challenges. The on-call view should show the affected factor, challenge age, region, attempt count, and whether an alternate factor is enrolled. If it only says "2FA is down," diagnosis has already lost valuable time.

**Short answer:** use an authenticator app as the normal second factor for staff who can enroll it, keep SMS OTP for users whose reach matters more than resistance to carrier-account takeover, and treat an email code as a convenience or recovery path only when the mailbox is independently protected. For a US and EU SaaS, the simplest defensible design is one factor-neutral challenge ledger, explicit regional retention rules, and templates owned by the team that owns the compliance-notice workflow. There is no universally cheapest channel once support, recovery, delivery investigation, and audit evidence enter the bill.

I've been paged for missed jobs and duplicate deliveries. The lesson carries over cleanly: a successful provider handoff isn't the same as a completed user action, and retrying without an idempotency key can turn a delivery problem into an abuse problem.

## How should a marketplace SaaS choose SMS OTP, authenticator app, or email code?

Start with the account and the consequence of a bad login. A marketplace employee who can approve a regulated notice, change payout details, or export seller records deserves a factor that does not travel through the same inbox used for routine work. An authenticator app fits that default because the login service can verify a time-based code locally after enrollment; RFC 6238 defines the time-based one-time password algorithm. The trade-off is enrollment and recovery. Lost devices become a support and identity-proofing problem, so backup codes and factor replacement need the same care as the primary login. SMS OTP has wider practical reach when users already have a phone number on file and don't want another app. Its operational boundary is the telephone account: number reassignment, carrier-account compromise, roaming, filtering, and delayed delivery all sit outside the SaaS team's direct control. Use it when completion and accessibility justify that dependency, but don't describe possession of a phone number as strong proof of identity. A sent message is only a transport event. Email code is easy to explain and keeps the interaction on devices people already use. The catch is factor independence. If the email account can reset the SaaS password and receive its second factor, one compromised mailbox may control both steps. Email can still be reasonable for low-impact accounts, step-up confirmation where another independent signal exists, or a deliberately constrained recovery flow. It is not suitable as the only additional protection for operators who control compliance notices. The US-versus-EU decision belongs in policy, not in separate authentication implementations. Have counsel and privacy owners specify lawful purpose, message content, consent where applicable, retention, deletion, and data-access boundaries for each region. Then make those values configuration attached to an immutable policy version. I'm not sure a generic retention period can be defensible for every marketplace; the applicable notice, sector, and member-state obligations resolve that question, not an engineering blog post. The cost comparison is similarly local. SMS has a visible per-message component, while authenticator verification avoids a delivery on each login but creates enrollment and recovery work. Email uses infrastructure the company may already operate, yet deliverability, reputation, template changes, and mailbox-related support are real costs. Measure cost per completed, legitimate challenge, including support minutes and recovery reviews. "Cheapest per send" answers the wrong question.

Count completion.

## Work backward from the page

The page is the end of a chain. For this marketplace, it might fire because authorized staff are at risk of missing the deadline to send a compliance notice. The immediate dashboard needs to separate three states: challenges created, transport accepted where transport exists, and challenges verified. That distinction prevents an accepted SMS or email from being counted as a successful login. Authenticator apps have no delivery handoff, so their comparable path is challenge created, code submitted, and code verified or rejected.

Transport is not proof.

Now walk backward. A drop in verified challenges is late evidence. Earlier signals include rising challenge age, a widening gap between created and verified events, repeated requests for the same account, recovery starts, and concentration by factor or region. Avoid logging raw codes, shared secrets, full phone numbers, or email bodies. Correlate with opaque challenge and account identifiers, and restrict access to the audit stream.

The ledger should preserve intent as well as mechanics. Record why the challenge existed, which policy version selected the factor, which template version produced the message, the region policy applied, and the terminal result. For the concrete workflow, link the authenticated session to the compliance-notice action without placing notice contents in the authentication event. That gives an auditor a trace while keeping two different data domains apart.

One detail matters more than it looks: template ownership. The team accountable for the compliance-notice workflow should approve user-visible wording and version changes, while the identity platform owns token placement, expiry semantics, secret handling, and rendering constraints. Shared ownership without a final approver tends to produce emergency copy edits during incidents. It also makes an audit question such as "which message did this seller receive?" surprisingly hard to answer.

Noisy pages have a cost. If the alert triggers on every small dip in completion, normal user abandonment will train the on-call to ignore it; if it waits for the deadline to be threatened, there is no room to switch an eligible user to an enrolled alternate factor. Page on sustained risk to the business action, and use lower-severity alerts for channel drift.

## Instrument the decision, not the secret

A compact event model is enough to establish the trace. The following Go example validates the fields that make a challenge auditable, hashes the account reference before it reaches the sink, and uses the challenge ID as the deduplication key. The sink implementation could be an append-only database or a queue consumer, but that choice does not change the contract.

```go
package authaudit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Factor string

const (
	FactorSMS   Factor = "sms_otp"
	FactorTOTP  Factor = "authenticator_app"
	FactorEmail Factor = "email_code"
)

type ChallengeEvent struct {
	ChallengeID    string    `json:"challenge_id"`
	AccountRefHash string    `json:"account_ref_hash"`
	Factor         Factor    `json:"factor"`
	PolicyVersion  string    `json:"policy_version"`
	TemplateVersion string   `json:"template_version,omitempty"`
	RegionPolicy   string    `json:"region_policy"`
	Purpose        string    `json:"purpose"`
	Result         string    `json:"result"`
	OccurredAt     time.Time `json:"occurred_at"`
}

type EventSink interface {
	AppendOnce(ctx context.Context, deduplicationKey string, event ChallengeEvent) error
}

func RecordChallenge(ctx context.Context, sink EventSink, accountRef string, event ChallengeEvent) error {
	if event.ChallengeID == "" || accountRef == "" || event.PolicyVersion == "" {
		return errors.New("challenge ID, account reference, and policy version are required")
	}
	if event.Factor != FactorSMS && event.Factor != FactorTOTP && event.Factor != FactorEmail {
		return errors.New("unsupported factor")
	}

	digest := sha256.Sum256([]byte(accountRef))
	event.AccountRefHash = hex.EncodeToString(digest[:])
	event.OccurredAt = event.OccurredAt.UTC()

	return sink.AppendOnce(ctx, event.ChallengeID, event)
}
```

Hashing an identifier reduces casual exposure; it does not make the event anonymous when the same input always yields the same digest. Access controls, key management for any keyed transformation, retention, and deletion still need explicit design. Also keep code validity and replay defenses in the verifier, not in the audit sink. The event recorder observes a decision. It must never become a second verifier.

For transport factors, add provider handoff identifiers in a restricted delivery table rather than the general authentication event. For authenticator apps, retain enrollment generation and recovery events but never the shared secret. Across all three factors, the useful operational ratio is completed legitimate challenges divided by initiated legitimate challenges; raw send volume can rise during retries while actual access gets worse.

Test the state machine, too. Duplicate callbacks should not create duplicate terminal events. A late delivery should not revive an expired challenge. A newly issued code should invalidate the prior code according to the policy, and concurrent submissions should produce one terminal outcome. These are boring invariants until a compliance deadline is five minutes away.

## Ownership and rollout decide whether the design survives

Keep factor selection behind policy rather than scattering it through login handlers. The policy consumes account risk, role, enrolled factors, recovery state, and regional configuration, then returns an allowed challenge path and a policy version. This makes a staged rollout possible: observe selection decisions first, enable a small cohort, compare completion and recovery rates, and preserve a controlled rollback to the prior policy version.

The identity team should own secret generation, verification, rate limits, replay prevention, and factor lifecycle. The communications team should own sender authentication and transport health for email or SMS. The marketplace compliance team should own the notice language, localization approval, and evidence requirements. One named team must own the cross-service runbook and the alert. Otherwise every component can be healthy while the user remains locked out.

Email needs a few special controls. DKIM signs selected message content and headers so a verifier can associate a message with a signing domain, as specified in RFC 6376; it does not prove that the human recipient read the message. Apple Mail Privacy Protection also prevents senders from learning accurate Mail activity through normal remote-content loading, so an open event should not be treated as delivery proof or authentication completion. Verification at the SaaS endpoint is the meaningful terminal event.

Before launch, rehearse factor loss, delayed transport, duplicate handoffs, expired codes, policy rollback, and regional deletion requests. Keep recovery stricter than ordinary login because an attacker will choose the easiest path offered. Keep an alternate factor only when it was enrolled or verified under an explicit policy; an emergency fallback invented during an incident quietly defeats the original control.

There is no zero-friction answer. Authenticator apps are not suitable when the population cannot reliably enroll or retain a separate app. SMS should remain available when phone reach and accessibility dominate, with its carrier dependency accepted in the threat model. Email codes fit lower-risk or recovery cases where mailbox control is an acceptable dependency. The decision is sound when the limitation is written down, monitored, and tested before the page fires.

## References

- RFC 6238, TOTP: Time-Based One-Time Password Algorithm: https://www.rfc-editor.org/rfc/rfc6238
- NIST SP 800-63B, Authentication and Authenticator Management: https://pages.nist.gov/800-63-4/sp800-63b.html
- RFC 6376, DomainKeys Identified Mail (DKIM) Signatures: https://datatracker.ietf.org/doc/html/rfc6376

## Further reading

- Apple, Use Mail Privacy Protection on iPhone: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
