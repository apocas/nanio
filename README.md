# nanio

**A minimal, stateless, S3-compatible object storage server.**

Buckets are folders, objects are files, the entire backing store is a flat
POSIX filesystem. Install with `pipx`, run with one command, point any
official S3 client at it.

```bash
pipx install nanio
export NANIO_ACCESS_KEY=minioadmin NANIO_SECRET_KEY=minioadmin
nanio serve --data-dir ./nanio-data
```

In another shell, point the AWS CLI at it. The `aws` CLI only reads its
own `AWS_*` env vars — it doesn't know about `NANIO_*` — so you have to
export the same credentials under the names the AWS SDK expects:

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp README.md s3://test/
aws --endpoint-url http://localhost:9000 s3 ls s3://test/
```

That's it. The official `aws-cli`, `boto3`, and `s3cmd` Just Work against
nanio with no special-casing.

## Why nanio

Early MinIO was beautifully simple: a single binary, a single command, an
S3-compatible HTTP server backed by the filesystem. Modern MinIO has grown
into a feature-rich product with erasure coding, IAM, lifecycle policies,
replication, and a console UI. That's the right call for them — and the
wrong call for the dozens of small use cases that just need an S3 endpoint
in front of a directory: local development, CI fixtures, edge caches,
short-lived test environments, simple backup targets.

nanio is the small thing. It does the S3 wire protocol over a flat
filesystem, and nothing else. The whole codebase is a few thousand lines
of Python. There is no console, no IAM, no versioning, no lifecycle, no
replication, no encryption-at-rest. There is one CLI command (`nanio
serve`) and two environment variables.

## Features

- ✅ S3 wire compatibility for the operations the official clients actually use:
  - `ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`, `GetBucketLocation`
  - `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects` (batch)
  - `ListObjectsV2` with prefix, delimiter, pagination, encoding-type
  - `CopyObject`
  - Multipart upload: `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`
  - Presigned URLs for GET and PUT
  - Range requests (`206 Partial Content`)
- ✅ AWS Signature V4 verification (header form + presigned URLs + streaming `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`)
- ✅ Streaming uploads and downloads — server memory does not scale with object size
- ✅ Stateless — point N processes at a shared filesystem and put any TCP load balancer in front
- ✅ Single-user (env vars) or multi-user (TOML credentials file)

## Non-features

These are deliberately out of scope. nanio is intentionally small.

- ❌ No TLS — terminate HTTPS upstream (nginx, caddy, traefik)
- ❌ No IAM, no policies, no ACLs — credentials are bearer tokens
- ❌ No versioning, lifecycle, replication, or event notifications
- ❌ No web console, no admin API, no metrics endpoint (stick a reverse proxy with metrics in front)
- ❌ No encryption at rest — use a filesystem that does it (LUKS, dm-crypt, ZFS native)
- ❌ No erasure coding — use a filesystem that does it (ZFS, btrfs RAID, mdraid)
- ❌ Not supported on Windows (POSIX `rename` semantics, `os.pread`)

If you need any of those, run real MinIO, Ceph RGW, or AWS S3.

## Installation

```bash
pipx install nanio
```

Or via `uv`:

```bash
uv tool install nanio
```

Both options put a `nanio` binary on your `$PATH`.

## Configuration

`nanio serve` is the only subcommand.

```
nanio serve [OPTIONS]

Options:
  --data-dir PATH            Root directory for buckets   [env: NANIO_DATA_DIR]   default: ./nanio-data
  --host TEXT                Bind host                    [env: NANIO_HOST]       default: 0.0.0.0
  --port INTEGER             Bind port                    [env: NANIO_PORT]       default: 9000
  --workers INTEGER          uvicorn workers              [env: NANIO_WORKERS]    default: 1
  --region TEXT              S3 region to report          [env: NANIO_REGION]     default: us-east-1
  --credentials-file PATH    TOML multi-user file         [env: NANIO_CREDENTIALS_FILE]
  --log-level [debug|info|warning|error]                  default: info
  --no-access-log            Disable per-request logs
  --version
  --help
```

### Single-user (env vars)

```bash
export NANIO_ACCESS_KEY=minioadmin
export NANIO_SECRET_KEY=minioadmin
nanio serve --data-dir ./data
```

### Multi-user (TOML file)

```toml
# nanio-credentials.toml
[[users]]
access_key = "alice"
secret_key = "alice-very-long-secret"

[[users]]
access_key = "bob"
secret_key = "bob-very-long-secret"
```

```bash
nanio serve --data-dir ./data --credentials-file nanio-credentials.toml
```

If neither env vars nor a credentials file are configured, nanio refuses
to start. There is no anonymous mode.

## Scaling out

nanio holds zero in-process state. You scale it horizontally by:

1. Putting all `--data-dir`s on a shared filesystem (NFSv4, cephfs, or any
   POSIX-compliant network mount with atomic `rename`).
2. Running `nanio serve` on N machines pointing at that mount.
3. Putting any TCP load balancer in front (nginx, HAProxy, AWS NLB).

```nginx
upstream nanio {
    server node1:9000;
    server node2:9000;
    server node3:9000;
}

server {
    listen 443 ssl http2;
    server_name s3.example.com;

    ssl_certificate /etc/ssl/example.com.pem;
    ssl_certificate_key /etc/ssl/example.com.key;

    client_max_body_size 0;          # let nanio handle huge uploads
    proxy_request_buffering off;     # stream the request body
    proxy_buffering off;             # stream the response body

    location / {
        proxy_pass http://nanio;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

All nanio nodes must run NTP/chrony — SigV4 enforces a 15-minute
clock-skew window between client and server, and we do not widen it.

## Storage layout

Everything under `--data-dir` is browsable with normal Unix tools:

```
nanio-data/
├── widgets/                          # bucket
│   ├── photos/2026/cat.jpg           # object
│   ├── data.bin                      # object
│   └── .nanio-meta/                  # sidecar metadata (one .json per object)
│       ├── photos/2026/cat.jpg.json
│       └── data.bin.json
└── .nanio/
    └── multipart/                    # in-progress multipart uploads
        └── <upload-id>/
            ├── init.json
            └── parts/000001.bin
```

You can `cat`, `cp`, `rsync`, and `tar` your data directly. Backups are
just filesystem backups.

## Performance notes

nanio uses `os.scandir` for listing and `os.pread`/`os.sendfile` for
streaming I/O. It has been validated with [locust](https://locust.io)
load tests at hundreds of requests per second per worker. See
[`tests/load/README.md`](tests/load/README.md) for the scenarios and how
to run them.

A single bucket with millions of objects in a single directory is the
filesystem's problem, not nanio's. ext4 and XFS handle millions of
entries with htree, but performance degrades past a few million entries
in one directory. The standard fix is the same as on AWS S3: use
prefixed keys (`logs/2026/04/08/...` instead of one flat directory).

## Status

`0.1.0` — alpha. The wire surface is intended to remain stable, but the
admin/CLI surface may change before a `1.0`. Contributions and bug
reports welcome.

## License

Apache 2.0. See `LICENSE`.
