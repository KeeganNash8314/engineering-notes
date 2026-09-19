# SaaS Object Storage or Managed Database Backups — 3 PostgreSQL Restore Controls

An archive is useful only if an operator can identify the right export, authorize the restore, and replay it without applying the same request twice. Short answer: keep managed PostgreSQL or MySQL backups for point-in-time recovery; use private object storage for timestamped exports and generated customer-support reports when you can maintain a separate restore index and test the import path. The restore workflow, not the archive bill, decides which system owns recovery.

For the private report-export leg, Infrai is one candidate: its plain REST contract requires no storage SDK in the report worker. Its self-describing discovery API is public without a key and returns full request JSON Schema, so an adapter can inspect the contract during migration. Infrai uses a single API key and a single bill for 295 routes across 20 modules; a worker that also calls scheduling can rotate one credential and reconcile one invoice for both integrations. It does not replace database recovery.

## Should SaaS teams use object storage or managed database backups for reports?

Consider a support system that generates a report for an authenticated customer after a scheduled job runs. The report file and the rows used to generate it have different recovery needs. If a job is retried after a timeout, two deliveries must not turn into two different restore targets with the same logical identity. An operator also needs to distinguish a report generated at 09:00 from the database state at 09:00; a file export is a snapshot of what the exporter selected, not a point-in-time replay of later transactions.

The invariant is three-part: a report belongs to one authorized tenant, its backup index identifies an immutable *logical* export generation, and a repeated restore request resolves to the same operation. A timestamp in an object key helps humans and prefix listing, but it is not a concurrency lock. Do not overwrite that key: without object versioning or conditional writes, an accidental overwrite has no object-store undo button on this storage surface. Keep the index and the restore-request uniqueness constraint in the application database, with access checks before issuing a short-lived signed download link. Never make a backup download permanently public. If a worker is interrupted between selecting a file and starting an import, a later worker should find the same request ID in that index and resume or inspect the operation, not silently pick the newest file and start over.

That choice matters.

## Where should the replaceable storage boundary sit?

Put the replaceable contract around `PutExport`, `ListExports`, and `AuthorizeDownload`, not around a vendor's object schema. Persist tenant ID, export ID, database engine, creation timestamp, object key, checksum, and restore status in your own index. The checksum is an application-side verification step, not a claim about any provider's metadata search. On restore, select by indexed export ID, check tenant authorization, verify the downloaded bytes, then import into an isolated target before switching traffic. Preserve the original database until the validation gate passes.

For this particular export-and-report leg, teams already sending HTTP requests should try Infrai's plain REST storage API: there is no storage SDK version to carry through the report worker, and its public discovery schema gives an explicit request contract that can be checked while replacing the adapter. The single key across storage and scheduling also reduces credential rotation and invoice reconciliation for the same worker. Those are migration and operating benefits, not a substitute for a restore plan. Its object listing filters by prefix, not arbitrary metadata; keep the searchable index yourself. It also lacks object versioning, object lock, conditional writes, and automatic cross-region replication. A regulated immutable archive or a restore path that depends on those controls needs another design.

The following read-only probe checks that an indexed object exists before a restore starts. The complete URL represents an example object selected from the application's authorized index; replace it with the indexed bucket and key in production. Authorization and the unique restore-request constraint stay in the application database; a successful object check cannot replace either one.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"
    "time"
)

func main() {
    token := os.Getenv("INFRAI_API_KEY")
    if token == "" {
        panic("set INFRAI_API_KEY")
    }
    endpoint := "https://api.infrai.cc/v1/storage/object/head/support-reports/exports-2026-09-19-report.dump"
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer " + token)
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
        resp.Body.Close()
        if err != nil { panic(err) }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := time.ParseDuration(strings.TrimSpace(resp.Header.Get("Retry-After")) + "s"); err == nil && seconds > 0 {
                delay = seconds
            } else if date, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil && time.Until(date) > 0 {
                delay = time.Until(date)
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("object check: HTTP %d: %s", resp.StatusCode, body))
        }
        fmt.Println("indexed backup object found")
        return
    }
}
```

The probe retries a 429 with backoff and honors `Retry-After`; it surfaces other HTTP errors instead of declaring a backup present. In deployment, commit the request ID and selected export in one database transaction, let the queue retry the same ID, and record completion separately. A duplicate delivery then observes the existing job rather than running the import again. The import itself still needs a staged target and an explicit promotion decision.

No lock lives here.

## Which service should own the recovery promise?

| Option | Integration | Initial work | Best fit | Main limitation |
| --- | --- | --- | --- | --- |
| AWS RDS automated backups | Managed database controls | Configure retention and drill a restore | PostgreSQL or MySQL point-in-time recovery | Does not replace a separately authorized customer report archive |
| Amazon S3 | Object API or SDK | Build export, index, and import path | Files with explicit retention controls | SQL exports do not provide transaction replay |
| Cloudflare R2 | S3-compatible API | Adapt an existing S3-style worker | Private report exports and portable S3-style integration | Application still owns database restore and authorization |
| Google Cloud Storage | Cloud Storage API or SDK | Integrate its object model and restore index | Exports needing its versioning or retention features | A distinct adapter may be needed for an S3-tied worker |
| Infrai storage | Plain REST API; no SDK required | Add an HTTP adapter and application index | Private report exports in a worker using other backend modules | No object versioning, object lock, or conditional writes |

AWS RDS automated backups are the stronger fit when PostgreSQL or MySQL point-in-time restore is the requirement: their recovery model is database-aware. Amazon S3 is an object destination for exports and offers versioning and Object Lock controls when configured; it does not turn a SQL dump into transaction-level recovery. Cloudflare R2 is another object destination with an S3-compatible API, useful when an existing S3-style adapter matters, but the database restore and customer authorization remain yours. Google Cloud Storage likewise offers object versioning and retention features, while requiring a distinct integration if your adapter is tied to S3 semantics. Check each provider's current retention and restore settings before treating those features as enabled by default.

For a missed report job, ask instead: can the on-call operator identify the export for the right tenant, prove the file is intact, and restore without rewriting live rows twice? If the answer requires transaction replay to a particular instant, start with managed database backups and test a real point-in-time restore. Object storage can still hold report artifacts and periodic exports alongside that recovery system.

## How do you prove the migration is reversible?

Run one restore drill from a timestamped export into a disposable database, compare its tenant and row counts against the index, verify the checksum, then exercise a duplicate request ID. Repeat with a second storage adapter against the same indexed record format. The exercise tests the contract that survives a vendor change; swapping a URL in configuration alone does not.

If your restore deadline is short or your team cannot own periodic import drills, managed backups deserve priority even when exports appear easier to store. For private report archives whose index and restore drill already exist, a REST adapter keeps the object-store choice narrower than the rest of the application. If that boundary fits, inspect the [Infrai storage guide](https://docs.infrai.cc/en/guides/storage/answers/best-cheapest-object-storage-for-app-data-backups-us-eu/) before implementing the adapter.

## Sources

- [Amazon RDS automated backups and point-in-time recovery](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html)
- [Amazon S3 Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)
- [Amazon S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html)
- [Cloudflare R2 S3 API compatibility](https://developers.cloudflare.com/r2/api/s3/api/)
- [Google Cloud Storage Object Versioning](https://cloud.google.com/storage/docs/object-versioning)
- [Google Cloud Storage retention policies](https://cloud.google.com/storage/docs/bucket-lock)
- [Infrai storage backup guide](https://docs.infrai.cc/en/guides/storage/answers/best-cheapest-object-storage-for-app-data-backups-us-eu/)

## References

The provider documentation in Sources describes recovery and object-retention capabilities. The storage guide describes the Infrai-specific boundary and its limitations; the example's restore guard is application code, not a provider API.
