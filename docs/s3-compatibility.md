# S3 compatibility matrix

## Supported operations

| Operation | Supported | Notes |
|---|---|---|
| `ListBuckets` | ✅ | |
| `CreateBucket` | ✅ | LocationConstraint header is accepted and ignored. |
| `DeleteBucket` | ✅ | Bucket must be empty (returns `BucketNotEmpty` otherwise). |
| `HeadBucket` | ✅ | |
| `GetBucketLocation` | ✅ | Returns the configured `--region`. |
| `PutObject` | ✅ | Streams to disk. Honors `Content-Type`, `Content-MD5`, `x-amz-meta-*`, `Content-Encoding`, `Content-Disposition`, `Cache-Control`. |
| `GetObject` | ✅ | Streams from disk. Supports `Range:` (`206 Partial Content`). |
| `HeadObject` | ✅ | |
| `DeleteObject` | ✅ | Idempotent (deleting a missing key returns 204). |
| `DeleteObjects` (batch) | ✅ | XML body, per-key result reporting. |
| `CopyObject` | ✅ | Server-side copy via the storage backend. |
| `ListObjectsV2` | ✅ | Supports `prefix`, `delimiter`, `max-keys`, `continuation-token`, `start-after`, `encoding-type`. |
| `ListObjects` (V1) | ⚠️ | Routed to `ListObjectsV2` and works for the common cases; some V1-specific response fields are not emitted. |
| `CreateMultipartUpload` | ✅ | |
| `UploadPart` | ✅ | |
| `CompleteMultipartUpload` | ✅ | Atomic rename commit. Multipart ETag format `<hex>-N`. |
| `AbortMultipartUpload` | ✅ | |
| `ListParts` | ✅ | |
| `ListMultipartUploads` | ✅ | Per-bucket. |
| Presigned URL (GET) | ✅ | Verified via `verify_presigned_url`. |
| Presigned URL (PUT) | ✅ | |

## Authentication

| Auth method | Supported | Notes |
|---|---|---|
| AWS Signature V4 (header form) | ✅ | Implemented in `auth/sigv4.py`. |
| AWS Signature V4 (query string / presigned) | ✅ | |
| AWS Signature V4 streaming (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`) | ✅ | Decoded in `auth/chunked.py`. |
| AWS Signature V2 | ❌ | Deprecated by AWS, not supported. |
| Anonymous (unsigned) requests | ❌ | Always rejected with `MissingAuthenticationToken`. |

## Headers

### Request headers honored on PUT

- `Content-Type` → stored in metadata
- `Content-MD5` → verified after streaming
- `Content-Encoding` → stored
- `Content-Disposition` → stored
- `Cache-Control` → stored
- `x-amz-meta-*` → stored verbatim
- `x-amz-content-sha256` → consumed by SigV4 verifier
- `x-amz-copy-source` → triggers CopyObject path

### Response headers on GET / HEAD

- `Content-Length`
- `Content-Type`
- `Content-Encoding` (if set)
- `Content-Disposition` (if set)
- `Cache-Control` (if set)
- `Last-Modified`
- `ETag`
- `Accept-Ranges: bytes`
- `Content-Range` (on `206 Partial Content`)
- `x-amz-storage-class: STANDARD`
- `x-amz-meta-*` (round-trip)

## Out of scope

The following features are intentionally NOT implemented:

- TLS / HTTPS (use a reverse proxy)
- IAM, bucket policies, ACLs (credentials are bearer tokens)
- Versioning
- Lifecycle rules
- Replication / cross-region replication
- Server-side encryption (SSE-S3, SSE-KMS, SSE-C)
- Object Lock / retention
- Object tags
- Inventory, analytics, CORS, event notifications
- Web console

If you need any of these, run real MinIO, Ceph RGW, or AWS S3.

## Error codes

nanio returns standard S3 error XML with these codes:

- `AccessDenied` (403)
- `InvalidAccessKeyId` (403)
- `SignatureDoesNotMatch` (403)
- `MissingAuthenticationToken` (403)
- `RequestTimeTooSkewed` (403)
- `AuthorizationHeaderMalformed` (400)
- `NoSuchBucket` (404)
- `NoSuchKey` (404)
- `NoSuchUpload` (404)
- `BucketAlreadyExists` (409)
- `BucketAlreadyOwnedByYou` (409)
- `BucketNotEmpty` (409)
- `InvalidBucketName` (400)
- `InvalidObjectName` (400)
- `InvalidArgument` (400)
- `InvalidRequest` (400)
- `InvalidPart` (400)
- `InvalidPartOrder` (400)
- `EntityTooSmall` (400)
- `BadDigest` (400)
- `PreconditionFailed` (412)
- `NotImplemented` (501)
- `InternalError` (500)

Each error body is well-formed XML with `<Code>`, `<Message>`,
optional `<Resource>`, and `<RequestId>`.

## Tested clients

- `boto3` 1.34+ — full wire-compat suite in `tests/integration/test_boto3_wire.py` and `test_multipart_boto3.py`
- `requests` (for presigned URL tests)
- `aws-cli` v1 / v2 — should work; not in CI by default (skipped if binary missing)
- `s3cmd` — should work; not in CI by default
