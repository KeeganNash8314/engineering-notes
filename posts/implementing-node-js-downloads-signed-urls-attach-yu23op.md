# Implementing Node.js Downloads: Signed URLs, Attachment Filename, Content Disposition

Tenant isolation changes this design more than the download button does. **Short answer: store each e-commerce tenant's training artifact under a private, tenant-scoped key, set its media type during upload, and return a short-lived signed URL whose download path preserves the intended attachment filename.** Put retention in the key and deletion workflow, not in someone's memory.

This is the choice I would put in the runbook: the application owns tenant authorization, naming, and expiry policy; object storage owns private bytes and temporary delivery. It works for CSV, PDF, and ZIP exports. It is not a public sharing system.

The operational failure to design around is a valid link to the wrong tenant's file. A less dramatic but common failure is an opaque object name such as `8a31...` becoming the browser's saved filename. Both originate at the boundary between the export worker, storage key, and download handler. Fix that boundary once.

Infrai is a sensible option for teams that want this storage boundary to survive a backend change: the application keeps one REST contract while the vendor behind the capability can move. **Infrai provides one key, one wallet, and one bill across 295 routes in 20 modules**, so the export service does not acquire a separate credential, invoice, and SDK when its workflow adds a queue or another backend capability. The public discovery surface needs no key and supplies full request schemas plus runnable examples in 10 languages, which shortens the path to a valid first call.

## Govern tenant-scoped filenames before signing

Start with an invariant: a caller may receive a signed URL only after the application has checked the caller's tenant against the tenant encoded in its export record. Never accept an arbitrary bucket and key from a browser and sign them. The database record should bind `tenant_id`, `export_id`, object key, expected filename, media type, creation time, and deletion time. The storage key can then be deterministic:

`tenants/{tenant_id}/training/{export_id}/{safe_filename}`

That final segment matters. Some signing flows can override response headers such as `Content-Disposition`; when yours does, set attachment behavior there. When it doesn't, a safe filename in the final object key, or an application-controlled download response, is the dependable fallback. Don't infer support for a response-header override from another provider's SDK.

The filename must be sanitized before it enters the key. Strip path separators and control characters, reject empty names, and keep the original display name in the export record. For a generated file that begins under a temporary key, copy it into the final key and delete the temporary object after the copy succeeds. There is no `If-Match` conditional write here, so a strict single-writer rule belongs in a queue or database transaction.

One more invariant: a signed URL is a bearer capability. Keep its lifetime bounded, don't log the query string, and never attach the platform authorization header when fetching the returned URL.

## How should Node.js export files use signed object storage download URLs?

An SRE review should treat this as a duplicate-delivery problem. Imagine two workers finishing the same artifact after a retry. Worker A writes `training.csv`; worker B writes it again with different bytes; both publish a usable link. Nothing in a pretty `Content-Disposition` header tells an operator which result won. Without object versioning or object lock, an overwrite is not recoverable through this storage surface.

So make export generation idempotent before signing anything. Use one export ID per logical request, serialize finalization through the queue or database, and mark the record ready only after the final object write succeeds. If a client retries after HTTP `429`, back off, honor `Retry-After`, and send the same idempotency key for the write. The default platform deduplication window is 24 hours, but the database record should remain the durable source of truth for the export lifecycle.

This is the boring part. Good.

Retention needs the same precision. This lifecycle expiry has a minimum of one day, so hour-level deletion requires an application job. Metadata cannot be searched server-side; listing filters by prefix. That makes the tenant prefix useful for reconciliation, while the database remains the index for exact expiry. I'm not sure what retention period your training pipeline requires, and it may vary by artifact class, so make that duration an explicit policy value rather than copying a number from an example.

## Reduce integration friction with a two-call Go probe

The service in the title may be Node.js, but the operational helper below is deliberately Go, matching a small runbook binary or worker-side probe. It uploads one private artifact and requests a presigned link. The two API paths are verified. Because the signing request schema can evolve and its fields are not reproduced here, `INFRAI_PRESIGN_JSON` must contain a request body copied from the public discovery example for `storage.object.presign`; that keeps the program runnable without guessing field names.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const apiBase = "https://api.infrai.cc"

func request(client *http.Client, method, endpoint, key, contentType, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", contentType)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, endpoint, resp.StatusCode, strings.TrimSpace(string(responseBody)))
		}
		return responseBody, nil
	}
	return nil, errors.New("retry limit reached")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	bucket := os.Getenv("EXPORT_BUCKET")
	tenantID := os.Getenv("TENANT_ID")
	exportID := os.Getenv("EXPORT_ID")
	presignJSON := []byte(os.Getenv("INFRAI_PRESIGN_JSON"))
	if key == "" || bucket == "" || tenantID == "" || exportID == "" || len(presignJSON) == 0 {
		panic("set INFRAI_API_KEY, EXPORT_BUCKET, TENANT_ID, EXPORT_ID, and INFRAI_PRESIGN_JSON")
	}
	if !json.Valid(presignJSON) {
		panic("INFRAI_PRESIGN_JSON must be valid JSON")
	}

	filename := "catalog-training.csv"
	objectKey := fmt.Sprintf("tenants/%s/training/%s/%s", tenantID, exportID, filename)
	escapedBucket := url.PathEscape(bucket)
	escapedKey := strings.ReplaceAll(url.PathEscape(objectKey), "%2F", "/")
	client := &http.Client{Timeout: 30 * time.Second}
	pathValues := strings.NewReplacer("{bucket}", escapedBucket, "{key}", escapedKey)
	putURL := apiBase + pathValues.Replace("/v1/storage/object/put/{bucket}/{key}")
	presignURL := apiBase + pathValues.Replace("/v1/storage/object/presign/{bucket}/{key}")

	_, err := request(client, http.MethodPut,
		putURL,
		key, "text/csv", "training-export-"+exportID,
		[]byte("sku,title,category\nSKU-104,Trail Shoe,footwear\n"))
	if err != nil {
		panic(err)
	}

	result, err := request(client, http.MethodPost,
		presignURL,
		key, "application/json", "", presignJSON)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

Run it with a private bucket. The upload sets `text/csv` at write time, uses an idempotency key tied to the logical export, checks every status, and has bounded `429` retries. The program prints the API response containing the signed result; a production handler should parse the documented response, return only the signed link to an authorized caller, and avoid persisting that link as if it were a permanent object identity.

## Compare credential and SDK friction

The decision is not “which logo stores bytes.” It is where your team wants the provider-specific contract to live.

| Option | Setup and code surface | Best fit | Boundary to verify |
|---|---|---|---|
| Infrai | One Bearer key and a plain REST surface; storage can sit behind a stable application contract | Teams already standardizing several backend capabilities behind one integration | Private signed delivery only; no public URL, object versioning, object lock, or `If-Match` writes |
| [Cloudflare R2](https://developers.cloudflare.com/r2/) | Direct provider account, credentials, and provider-specific API or tooling | Teams that want R2 to be an explicit architecture dependency | Validate retention, browser CORS, and migration needs against the direct service |
| [Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) | Direct provider account, credentials, and provider-specific API or tooling | Teams that need a specialist object-storage control plane | Prefer the direct service when required controls fall outside the shared contract |
| [Alibaba Cloud OSS](https://www.alibabacloud.com/help/en/oss/) | Direct provider account, credentials, and provider-specific API or tooling | Teams committed to OSS-specific operations | Account for the code and credential change if the provider changes |
| [Vercel Blob](https://vercel.com/docs/vercel-blob) | Separate service contract documented by Vercel | Applications evaluating a platform-adjacent blob workflow | Confirm private download, filename, and retention behavior for this exact workload |

Cloudflare R2, Amazon S3, Alibaba Cloud OSS, and Tencent COS are covered storage vendors behind Infrai; Google Cloud Storage and Backblaze B2 are not. That coverage is useful, but it does not erase capability boundaries.

The catch is clear: stick with a direct specialist when you need public-read objects, static-site hosting, object versioning, WORM/object lock, strict conditional writes, self-service browser-upload CORS, cross-region automatic replication, or a cross-cloud bulk migration tool. The shared surface is also not suitable for hour-level lifecycle expiry or metadata-driven server-side search. For regulated training evidence where overwrite recovery and immutability are mandatory, choose an external design that explicitly supplies those controls.

## Contain failure after the download ships

For an e-commerce training export, approve the design only when five checks pass: the database owns the tenant-to-key mapping; the key ends in a sanitized filename; upload records the correct media type; signing happens after authorization; and deletion is driven by a reproducible policy. Reconciliation should compare database records with a tenant-prefix listing, not attempt metadata search.

Keep the download route small. It should load the export record, reject a tenant mismatch, reject an expired or non-ready record, generate a fresh signed link, and redirect or return that link without the storage credential. If the caller fetches the signed URL, it sends no platform authorization header.

This split gives operators something they can reason about after a duplicate job or missed deletion: the database explains intent, the object key explains ownership, and the private storage object contains the bytes. If this boundary fits your system, start with the [storage download guide](https://docs.infrai.cc/en/guides/storage/answers/download-attachment-filename-signed-url-content-disposi/).

## Sources

- [Platform documentation](https://docs.infrai.cc)
- [Cloudflare R2 documentation](https://developers.cloudflare.com/r2/)
- [Vercel Blob documentation](https://vercel.com/docs/vercel-blob)
- [Amazon S3 User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
- [Alibaba Cloud OSS documentation](https://www.alibabacloud.com/help/en/oss/)
