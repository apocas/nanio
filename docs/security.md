# nanio security model

This document describes nanio's threat model, the in-process defenses,
and the operator responsibilities you cannot offload to nanio.

## Threat model

nanio's threat model is the same as a small S3 endpoint:

- **Untrusted clients** speak HTTP to nanio over a network. They control
  every byte of every request: headers, query string, body. They may
  hold a valid `(access_key, secret_key)` pair (legitimate user) or
  not (anonymous attacker).
- **Operator** owns the host, the data dir, the credentials, and the
  reverse proxy. The operator is trusted.
- **Other software on the host** (cron jobs, sysadmins, container
  sidecars) is trusted not to write into nanio's `--data-dir` out of
  band — but as a defense-in-depth measure nanio still refuses to
  follow symlinks (see "Symlink safety" below).

Nanio is **not** designed to defend against:

- A compromised operator account.
- A compromised host (root access).
- Cryptanalysis of HMAC-SHA256 / SHA-256 themselves.
- Side-channel timing attacks below microsecond resolution.

## In-process defenses

### SigV4 verification

- Signature comparisons use `hmac.compare_digest` everywhere.
- Signing key derivation matches the AWS spec exactly (date → region →
  service → `aws4_request`). Verified by round-trip tests against
  boto3's own signer in `tests/unit/test_sigv4.py`.
- The `host` header is **always required to be in `SignedHeaders`** for
  both header-form and presigned-URL requests. Without it, an attacker
  who captured a signed request could replay it against any nanio
  instance regardless of host. (Security audit M1.)
- Unknown access keys produce a generic `InvalidAccessKeyId` error
  identical to a wrong-signature response, so attackers cannot
  enumerate valid access keys. (Security audit M2.)
- Clock skew is enforced symmetrically at 15 minutes per the AWS spec.

### Streaming chunked decoder (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`)

- Each chunk's signature chains off the previous chunk's signature
  (or the seed signature for the first chunk).
- The frame-header reader is bounded at 4096 bytes per line.
- The chunk-size field is parsed but the actual data read is bounded
  by the underlying network read; no pre-allocation occurs.
- The trailing CRLF after each chunk is strictly verified.
- A single-chunk truncation, mid-frame EOF, missing chunk-signature,
  or bad signature all raise `InvalidRequest` / `SignatureDoesNotMatch`.

### XML body parsing

- Both `CompleteMultipartUpload` and `DeleteObjects` parse their XML
  bodies through `defusedxml.ElementTree`, which rejects entity
  expansion (billion-laughs / quadratic blowup) and external entities
  (XXE). (Security audit H2.)
- Request bodies are read through `read_bounded_body` with a 1 MiB
  hard cap. An oversized body raises `EntityTooLarge` before any XML
  parsing happens. (Security audit H3.)
- After parsing, batch sizes are capped at the AWS limits:
  - `DeleteObjects`: 1000 keys per request
  - `CompleteMultipartUpload`: 10 000 parts per request
  Exceeding either raises `MalformedXML`. (Security audit H4.)
- All `x-amz-meta-*` user metadata is capped at 2 KiB total per object
  (the AWS limit). Exceeding raises `MetadataTooLarge`. (Security audit M5.)

### Path safety

- Object key validation rejects empty segments, `.` and `..` segments,
  control characters, leading slashes, and the reserved `.nanio*`
  prefix. Validated in `nanio.keys.validate_object_key`.
- All on-disk path arithmetic goes through `safe_join`, which refuses
  any `..` segment in the relative parts and verifies the result stays
  inside the base directory.
- All `os.open` calls in the storage layer pass `O_NOFOLLOW`, refusing
  to follow leaf symlinks. Combined with `assert_inside_data_dir` (a
  `realpath`-based check on parent directories), this defends against
  both leaf and intermediate symlink attacks. (Security audit H5.)

### Concurrency safety

- Multipart upload `Complete` writes to a per-call scratch file
  (`assembled.<uuid>.tmp`) so two concurrent `Complete` requests on the
  same `uploadId` cannot truncate each other's writes. The atomic
  `os.replace` to the final path remains the commit point. (Security
  audit H1.)
- Object PUT writes to a per-call temp file with `O_EXCL` and renames
  atomically into place.
- Storage operations hold no in-process state, so any worker process
  can handle any subsequent request.

### Error handling

- A catch-all exception handler in `app.py` translates any unhandled
  Python exception into a generic `InternalError` XML body, logging
  the traceback server-side only. Clients never see Python exception
  class names or messages. (Security audit M3.)
- All client-facing errors are `nanio.errors.S3Error` subclasses
  serialized via `to_xml()` with `xml.sax.saxutils.escape`, so error
  bodies cannot contain unescaped XML metacharacters from
  user-controlled fields like bucket name or object key.

### Logging

- nanio's own log lines do **not** contain Authorization headers,
  SigV4 signatures, signing keys, or chunk signatures.
- uvicorn's access log (enabled by default) does include the request
  path and query string. **For presigned URLs, this means
  `X-Amz-Signature=...` will land in the access log.** If your
  operational pipeline ingests these logs to a less-trusted system,
  pass `--no-access-log` and rely on the reverse proxy to log
  requests with the signature redacted.

## Operator responsibilities

These are the things nanio cannot fix for you. Read each one before
deploying.

### TLS termination

nanio binds plaintext HTTP. **Always put a TLS-terminating reverse
proxy in front** (nginx, caddy, traefik, AWS NLB+ALB, etc.). Without
this, all SigV4 secrets are sent over the wire as plaintext (the SigV4
signature itself doesn't carry the secret, but the request body and
metadata are unprotected, and any other auth headers — none in nanio's
case but possible in cloned setups — would be exposed).

### Slowloris / connection-level DoS

uvicorn has a configurable `timeout_keep_alive` (default 5 s) but no
fine-grained body-read timeout. An attacker who sends 1 byte per
minute can tie up a worker for as long as the operating system allows.
**Configure your reverse proxy with `proxy_read_timeout` and
`client_body_timeout`** (nginx) or equivalent to bound this.

Recommended nginx values:

```nginx
client_body_timeout 60s;
client_header_timeout 30s;
proxy_read_timeout 600s;     # large GETs need headroom
proxy_send_timeout 600s;     # large PUTs need headroom
client_max_body_size 0;      # let nanio handle huge uploads
```

### Filesystem hygiene

- The `--data-dir` should be owned exclusively by the user nanio runs
  as. Do not let other processes write into it.
- On NFS, mount with `actimeo=1` or lower to minimize cross-client
  visibility lag.
- Do not let untrusted users place symlinks under `--data-dir` (the
  in-process O_NOFOLLOW + realpath defenses are belt-and-braces, but
  the cleanest fix is to never let them in).
- Run `nanio serve --gc-abandoned-uploads` periodically (cron, systemd
  timer) on the host that owns the data dir, to delete multipart
  upload state older than 7 days. Without this, an attacker can burn
  inodes by repeatedly calling `CreateMultipartUpload` and never
  finishing.

### Clock synchronization

SigV4 enforces a 15-minute clock-skew window between client and
server. **All nanio nodes (and clients) must run NTP/chrony.** Without
synchronized clocks, legitimate requests will be rejected as
`RequestTimeTooSkewed`.

### Disk space and quotas

nanio places no upper bound on object size or total disk usage. An
attacker (or accidental misconfiguration) can fill the disk. Use
filesystem quotas, LVM, or per-volume limits to bound this.

### Credentials at rest

The default credential resolver reads `NANIO_ACCESS_KEY` and
`NANIO_SECRET_KEY` from the process environment. These show up in
`/proc/<pid>/environ`, in the output of `ps eww`, and in any crash
dump. Prefer the `--credentials-file` mode (TOML) for production, with
the file owned by the nanio user and mode `0600`.

## Reporting a vulnerability

If you find a security issue in nanio, please open a GitHub issue
labeled `security` at `https://github.com/apocas/nanio/issues`. For
sensitive findings, contact the maintainer privately first.

## Audit history

| Date | Audit |
|---|---|
| 2026-04-08 | Initial security audit covering auth, filesystem, and input parsing. 6 high-severity findings (H1–H6), 6 medium (M1–M6), 3 low (L1–L3). All HIGH and MEDIUM findings except H5 multi-tenant intermediate symlinks (defense in depth) and M6 (suffix Range header parsing) have remediation in `0.1.2`. |
