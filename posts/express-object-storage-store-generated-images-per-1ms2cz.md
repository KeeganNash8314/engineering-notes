# Express Object Storage: Store Generated Images Per User Folder Behind Tenant Authorization

Short answer: store AI-generated images per user folder in private object storage, derive every tenant boundary in Express, and authorize both live images and backup snapshots before issuing a short-lived delivery capability.

For an edtech service, this is the useful default because a school administrator needs simple image delivery while the platform needs a hard answer to a harder question: can a teacher from school A ever name, restore, or delete an object owned by school B? A folder-shaped object key helps operators inspect data, but it cannot answer that question. Authentication establishes the caller; application metadata establishes ownership; storage holds bytes.

That separation also keeps backups honest. A selected snapshot is a set of immutable image generations plus a manifest, not a copied folder that becomes trustworthy merely because a scheduled job finished. The operating rule is blunt: no manifest, no restore; no tenant authorization, no delivery.

## How can Express object storage deliver generated images per user safely?

I've been paged by missed scheduled work and duplicate queue delivery. Those failures become dangerous here when the read path and the backup path disagree about ownership: the live application resolves an image through tenant metadata, while a restore worker trusts a raw object key from a message. Both paths can look healthy. Together, they have opened a route around the tenant check. The runbook should therefore begin by tracing one authenticated image request, one repeated backup message, and one selected restore all the way to the exact object generation they can reach.

A useful failure test changes the tenant in the authenticated principal without changing the image ID. The result must reveal neither the object key nor whether another tenant owns that identifier. Repeat the same exercise against a snapshot ID and against a queued restore operation. This tests the access boundary directly; a successful upload counter or scheduler heartbeat cannot.

Start with an opaque application image ID. After Express authenticates the request, the server looks up that ID inside the authenticated tenant and resolves the current immutable generation. The client never supplies a bucket, tenant prefix, snapshot key, or retention class. This adds one metadata lookup to the read path, but it prevents storage naming from becoming an accidental authorization API.

Use a server-built key such as `tenants/{tenant_id}/images/{image_id}/{generation_id}.png`. Here, “per-user folder” means an operational prefix, not a security primitive. Store the object key, digest, media type, generation, creation time, and retention state in application metadata. Overwriting an existing generation makes retries and backups ambiguous, so a regenerated illustration gets a new generation ID. The logical image record can point to the new one after the upload and metadata commit complete.

There are two reasonable delivery shapes. A server-mediated response gives the application one obvious place to authorize every read, at the cost of putting image bytes through application capacity. A delegated, short-lived delivery capability removes that byte path from the application, but the server must still authorize the tenant and exact object before issuing it. The choice is about operational ownership. It doesn't change the tenant boundary.

| Decision | Simpler delivery | Stronger central control | Operational cost |
| --- | --- | --- | --- |
| Application streams the image | One authenticated application URL | Authorization and revocation stay in one path | Application carries byte traffic |
| Application delegates one exact object | Storage handles the byte transfer | Capability scope and lifetime need review | More credential and expiry states to test |
| Public object URL | Easy to render | No per-request tenant decision | Not suitable for private school content |

Keep secrets out of source, object metadata, image keys, and logs. The OWASP Secrets Management guidance recommends managing secrets through their lifecycle, including creation, rotation, revocation, and expiration. That matters here because a storage credential is an infrastructure capability; it is not a substitute for checking which school owns an image.

Small rule. Big boundary.

## What state machine keeps snapshot recovery and lifecycle deletion safe?

A backup run should first select a stable set of committed image generations for one tenant. Write those entries into an immutable manifest with the expected digest for each object, then mark the snapshot restorable only after every selected object is represented and verified. New generations created after selection belong to a later snapshot. Deleted logical images can remain in a retained snapshot until that snapshot reaches its own retention boundary.

This ordering avoids coupling backup correctness to a listing taken while uploads and deletes are moving underneath it. It also makes “restore the Tuesday snapshot for this school” a precise operation: read one tenant-scoped manifest, copy or expose only its named generations into a fresh restore namespace, verify their digests, and promote the restored view only when the whole set passes. Live objects stay untouched during the drill. Rollback is then a metadata pointer change rather than another bulk copy.

Multipart uploads deserve a separate ledger. The Amazon S3 multipart overview describes initiation, part uploads, completion, and abort as distinct steps; completion creates the object from the uploaded parts, while abort ends an unfinished upload. A snapshot selector should therefore include only committed objects. Reconciliation should track unfinished upload IDs independently so an incomplete transfer never enters a manifest and can be aborted according to policy.

A backup operation needs an idempotency key derived from tenant and snapshot identity, a durable state, and a terminal result that a retry can observe. If the same message arrives twice, both executions must converge on one manifest and one restore decision. They must not create two logical snapshots that happen to contain similar keys. "The cron ran" proves very little.

The following Go code sketches the narrow domain boundary behind a Node.js and Express API. Express can authenticate and enqueue an operation; this worker contract refuses client-selected storage coordinates and makes duplicate execution explicit.

```go
package snapshots

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNotFound = errors.New("image not found")
	ErrConflict = errors.New("generation digest conflict")
)

type Image struct {
	TenantID    string
	ImageID     string
	Generation string
	ObjectKey  string
	Digest     string
}

type Catalog interface {
	FindImage(context.Context, string, string) (Image, error)
	RecordSnapshotEntry(context.Context, string, Image) error
}

func AddToSnapshot(
	ctx context.Context,
	catalog Catalog,
	authenticatedTenant string,
	imageID string,
	snapshotID string,
) error {
	image, err := catalog.FindImage(ctx, authenticatedTenant, imageID)
	if err != nil {
		return ErrNotFound
	}
	if image.TenantID != authenticatedTenant {
		return ErrNotFound
	}
	if image.Digest == "" || image.ObjectKey == "" {
		return fmt.Errorf("generation is not committed")
	}

	// The catalog enforces uniqueness for snapshot, image, and generation.
	return catalog.RecordSnapshotEntry(ctx, snapshotID, image)
}
```

The external contract can return `202` when a restore request has been accepted, `404` when the image or snapshot is not visible inside the authenticated tenant, and `409` when a requested promotion conflicts with a newer live generation. Those are application choices, not storage-provider behavior. In particular, returning the same missing-resource shape for an absent object and another tenant's object avoids confirming cross-tenant names.

Lifecycle deletion is the last step, not the scheduler's first action. Build an eligibility query from application metadata: the generation is older than the tenant's retention boundary, is not current, is not named by a retained snapshot, is not under a hold, and is not participating in a restore. Mark the generation `delete_pending` with an operation ID before sending the storage delete. A retry reads that state and continues the same logical operation.

The storage lifecycle rule is useful as an outer cleanup boundary, especially for abandoned transfers or data that the application has already made unreachable, but application-aware retention still owns snapshot membership and holds. Keep those responsibilities distinct. Otherwise, a convenient time-based rule can race the exact recovery point an administrator selected.

The catch is that manifests and application-mediated authorization add metadata, queue states, and an on-call runbook. This design is not suitable when the images are intentionally public, disposable, and have no tenant-specific recovery requirement; a public asset pipeline with coarse expiry is easier to operate in that case. Stick with server-mediated delivery when immediate centralized access decisions matter more than application bandwidth. Choose delegated delivery when byte volume matters more and the team can test capability scope, expiration, and revocation. I'm not sure one retention window can serve every school, so the policy owner must resolve that from contractual recovery and hold requirements rather than from a convenient cron interval.

Don't treat a successful delete request as the end of the workflow. Reconcile `delete_pending` records against application reachability, record a terminal tombstone, and retain enough non-secret evidence to explain the decision: tenant, image ID, generation, policy, operation ID, and timestamps. Logs should never contain image bytes or credentials.

## Stage the deletion worker rollout and rehearse rollback

Run the drill against one isolated tenant and one explicitly selected snapshot. Attempt a cross-tenant read, duplicate the restore message, restart the worker between object verification and manifest progress, create a newer live generation during the restore, and run retention selection concurrently. Expected outcomes are boring: unauthorized identifiers remain invisible, duplicate delivery converges, the live pointer stays unchanged until verification completes, and retained generations remain ineligible for deletion. Watch the age of the oldest pending backup, restore, and delete operation; the last successful manifest; expected versus verified object counts; digest mismatches; unfinished multipart uploads; and scan-cursor progress. A running process with a frozen cursor is still a missed job. Page on violated recovery objectives and stuck progress, not merely on whether the scheduler process exists. Deployment should begin with deletion disabled. Build manifests, run authorization tests, and execute restores into a fresh namespace first. Then enable marking without physical deletion and compare candidates with retained manifests. Only after that evidence is reviewed should the worker delete objects. If verification fails, stop promotion, preserve the live namespace, and keep the prior pointer.

No clever recovery step is required.

Done means a different operator can choose a tenant snapshot, restore it without seeing another tenant's names, verify every expected digest, promote it atomically, and show why every expired generation was eligible. Delivery simplicity is useful. A recoverable authorization boundary is the requirement.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
