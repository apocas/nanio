# Stateless scaling

nanio is designed to be horizontally scaled by launching more processes,
each of them stateless. There is no leader, no coordinator, no shared
cache to invalidate, no cluster membership protocol. The scaling
contract is small: a shared filesystem and a TCP load balancer.

## The contract

Every nanio process must see the same `--data-dir` (or one with
identical layout). All workers/processes are interchangeable.

Filesystem requirements:

| Requirement | Why |
|---|---|
| Atomic POSIX `rename` | Object PUT and multipart Complete commit by renaming a temp file into place. |
| Honored `fsync` | Metadata sidecars are durable across crashes only if `fsync` is honored. |
| Close-to-open consistency (or stronger) | A PUT on node A must be visible to a GET on node B. |
| Reasonable `scandir` performance | Listing operations walk the bucket tree directly. |

Filesystems that satisfy this:
- Local disks (ext4, xfs, btrfs, ZFS) for single-node deployments
- NFSv4 with `actimeo=1` or lower
- CephFS
- Cloud block storage with single-writer mounts (AWS EBS, GCP PD)
- AWS EFS (NFSv4-based)

Filesystems that **do not** satisfy this:
- s3fs-fuse / goofys (eventually consistent and slow)
- SMB without strict locking
- Any FUSE backend without atomic rename

## Recipe: nginx + N nanio processes

### 1. Mount shared storage on every node

```bash
mount -t nfs4 nfs.example.com:/nanio-data /mnt/nanio
```

### 2. Run nanio on each node

Each instance has its own port but the same `--data-dir`:

```bash
NANIO_ACCESS_KEY=$NANIO_ACCESS_KEY \
NANIO_SECRET_KEY=$NANIO_SECRET_KEY \
nanio serve \
  --data-dir /mnt/nanio \
  --host 127.0.0.1 \
  --port 9000 \
  --workers 4
```

`--workers 4` runs four uvicorn worker processes on a single host (one
per core is a good starting point).

### 3. nginx in front

```nginx
upstream nanio {
    server node1:9000;
    server node2:9000;
    server node3:9000;
}

server {
    listen 443 ssl http2;
    server_name s3.example.com;

    ssl_certificate     /etc/ssl/example.com.pem;
    ssl_certificate_key /etc/ssl/example.com.key;

    # Streaming uploads / downloads
    client_max_body_size 0;
    proxy_request_buffering off;
    proxy_buffering off;

    location / {
        proxy_pass http://nanio;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 600s;
        proxy_send_timeout 600s;
    }
}
```

### 4. NTP / chrony on every node

SigV4 enforces a 15-minute clock-skew window. nanio does not widen it.
All nodes (and clients) must run NTP/chrony.

## Failure modes

| Failure | Behavior |
|---|---|
| One nanio process crashes | The load balancer drops it from the pool. In-flight requests on other processes complete normally. |
| Shared storage briefly unavailable | Requests fail with 5xx until the mount returns. nanio does not retry. |
| Two clients race to PUT the same key | Last-writer-wins (atomic `rename` semantics). |
| Two clients race to Complete the same multipart upload | Last-writer-wins. If the parts lists were identical, the result is byte-identical. |
| A multipart upload is abandoned | The temp directory under `<data-dir>/.nanio/multipart/<upload-id>/` lingers. nanio logs a warning at startup if any are older than 7 days. Clean them up manually. |

## What you cannot do

- You cannot run nanio over s3fs-fuse or any backend without atomic
  rename. The PUT path will silently corrupt data.
- You cannot rely on across-process listing consistency stronger than
  what the filesystem itself provides. NFS close-to-open is fine in
  practice but not strict.
- You cannot point nanio at a directory that another process is also
  writing to. nanio assumes it owns its `--data-dir`.

## Sanity-check the deployment

After standing things up, validate end-to-end with the live server
fixture from your laptop:

```bash
uv run python - <<'PY'
import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3",
    endpoint_url="https://s3.example.com",
    aws_access_key_id="...",
    aws_secret_access_key="...",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path", "payload_signing_enabled": False}),
)
s3.create_bucket(Bucket="smoke")
s3.put_object(Bucket="smoke", Key="hello", Body=b"world")
print(s3.get_object(Bucket="smoke", Key="hello")["Body"].read())
s3.delete_object(Bucket="smoke", Key="hello")
s3.delete_bucket(Bucket="smoke")
PY
```

If that round-trip works, your deployment is good.
