# nanio load tests

These are [locust](https://locust.io) scenarios that drive boto3 against a
running nanio process. They are excluded from the default `pytest` run via
`--ignore=tests/load` in `pyproject.toml`.

## Setup

```bash
# 1. Install the loadtest extra (one-time)
uv sync --extra dev --extra loadtest

# 2. Start nanio in another terminal
NANIO_ACCESS_KEY=test NANIO_SECRET_KEY=test \
    uv run nanio serve --data-dir /tmp/nanio-loadtest --port 9000

# 3. Run a scenario
uv run --extra loadtest locust \
    -f tests/load/locustfile_small.py \
    --host http://127.0.0.1:9000 \
    --headless -u 100 -r 20 -t 60s
```

## Scenarios

| File | Workload |
|---|---|
| `locustfile_small.py` | Many concurrent users putting/getting/deleting 1 KB objects. The "millions of small files" workload. |
| `locustfile_large.py` | A few users uploading and downloading 50 MB objects via boto3's multipart `TransferConfig`. |
| `locustfile_mixed.py` | Realistic 60% GET / 20% PUT / 10% LIST / 10% HEAD on a populated bucket. |

## Reading the output

Locust prints per-request percentile latencies (p50/p95/p99) and a
requests-per-second number. Both should be stable across the run; spikes or
growing tail latency suggest something in nanio is allocating proportionally
to the request stream.

The streaming-property test (`tests/integration/test_streaming.py`) is the
authoritative check that nanio does not buffer big bodies in memory. Locust
is for throughput, not for memory profiling.

## Sharing the data dir across multiple nanio processes

To validate the stateless / horizontally-scaled scenario, point a shared
filesystem (NFS, cephfs, or just a local dir) at multiple nanio processes
and put a TCP load balancer in front:

```bash
# All processes share the same --data-dir.
nanio serve --data-dir /mnt/shared --port 9001 &
nanio serve --data-dir /mnt/shared --port 9002 &
nanio serve --data-dir /mnt/shared --port 9003 &

# nginx upstream { server 127.0.0.1:9001; server 127.0.0.1:9002; server 127.0.0.1:9003; }

uv run --extra loadtest locust -f tests/load/locustfile_mixed.py \
    --host http://127.0.0.1:8080 --headless -u 200 -r 50 -t 120s
```
