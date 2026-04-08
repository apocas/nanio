# CLAUDE.md — instructions for future Claude sessions in this repo

This file is the operating manual for any Claude Code session that lands
in `/home/pedrodias/nanio`. Read it before making changes.

## What is nanio

A minimal, stateless, S3-compatible object storage server. The pitch is
"early MinIO": one CLI command, one binary, a flat filesystem backend,
no admin UI. See `README.md` for the user-facing pitch.

The non-feature list in the README is **load-bearing**. Don't propose
adding TLS, IAM, versioning, lifecycle, replication, encryption-at-rest,
or web console features. They're explicitly out of scope.

## Repo map

```
src/nanio/
├── cli.py            # argparse → Settings → uvicorn.run
├── app.py            # build_app(settings) factory
├── app_state.py      # request.app.state accessors
├── config.py         # Settings dataclass
├── logging.py        # setup_logging
├── errors.py         # S3Error hierarchy + .to_xml()
├── xml.py            # pure XML response builders
├── etag.py           # md5 + multipart etag
├── keys.py           # bucket/object name validation, safe_join
├── auth/
│   ├── credentials.py    # CredentialResolver protocol + env/file impls
│   ├── sigv4.py          # SigV4 header + presigned-URL verification (we wrote this; awssig was unsuitable)
│   ├── chunked.py        # STREAMING-AWS4-HMAC-SHA256-PAYLOAD decoder
│   └── middleware.py     # Pure-ASGI middleware that wraps receive
├── storage/
│   ├── backend.py        # Storage protocol
│   ├── filesystem.py     # The only v1 backend
│   ├── paths.py          # SOLE owner of on-disk path math — don't bypass
│   ├── metadata.py       # JSON sidecar atomic read/write
│   └── multipart.py      # MultipartManager — stateless, FS-backed upload state
└── handlers/
    ├── routes.py         # Starlette route table
    ├── service.py        # GET / → ListBuckets
    ├── bucket.py         # bucket CRUD + ListObjectsV2 + DeleteObjects batch + GetBucketLocation
    ├── object.py         # PUT/GET/HEAD/DELETE/CopyObject + Range
    └── multipart.py      # multipart endpoints

tests/
├── conftest.py            # data_dir, settings, asgi_client, live_server, boto3_client fixtures
├── unit/                  # pure tests, no I/O outside tmp_path
├── integration/           # in-process httpx + real-uvicorn boto3 wire tests
└── load/                  # locust scenarios — excluded from default pytest run
```

## Golden rules

1. **Never buffer a whole request body or response body in memory.**
   - PUT path: stream `request.stream()` to a temp file via
     `asyncio.to_thread(os.write, fd, chunk)`, hashing as you go.
   - GET path: return a `StreamingResponse` whose generator does
     `os.pread` chunk-by-chunk. Implement Range yourself.
   - The streaming memory test (`tests/integration/test_streaming.py`,
     marked `slow`) is the contract. If you change the I/O hot path,
     run it.

2. **No in-process caches.** No bucket-existence cache, no metadata cache,
   no listing cache, no credential cache beyond the immutable resolver
   loaded once at startup. Stateless means stateless.

3. **All on-disk path arithmetic lives in `storage/paths.py`.** Don't
   reach into the filesystem from anywhere else. If you need a new path
   shape, add a function there.

4. **All client-visible errors raise `nanio.errors.S3Error` subclasses.**
   The exception handler in `app.py` serializes them to the standard
   `<Error><Code>...</Code>...</Error>` XML body. Never hand-roll an
   error response.

5. **Auth-disabled mode is a test-only escape hatch.** `Settings.auth_disabled
   = True` is for unit/integration tests that don't need to exercise
   SigV4. Never expose it via CLI flag.

6. **Multipart uploads carry zero in-process state.** The `uploadId` is
   pure entropy (`secrets.token_urlsafe`); state lives in the temp
   directory under `<data-dir>/.nanio/multipart/<uploadId>/`. Any worker
   on the shared FS can handle any subsequent request.

7. **Don't add background tasks.** They conflict with the multi-worker
   story (which worker would run them?). The "abandoned multipart
   uploads" check runs once at startup and emits a warning.

8. **Respect the SigV4 spec quirks.**
   - S3 SigV4 does NOT re-encode query string values (we matched boto3
     bit-for-bit on this). Don't normalize them.
   - The body hash is taken from the `x-amz-content-sha256` header. We
     trust the client; if they lie the signature won't match.
   - For `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, the chunked decoder
     verifies each chunk's signature in `auth/chunked.py`.

## Running tests

```bash
# Default suite (unit + integration, ~250 tests, runs in <10 s)
uv run pytest

# Run all pre-commit hooks against every file (ruff + hygiene)
uv run pre-commit run --all-files

# Just unit tests
uv run pytest tests/unit -q

# Just one file
uv run pytest tests/integration/test_boto3_wire.py -v

# The slow streaming memory test (multi-GB upload, RSS bounded)
uv run pytest -m slow

# Adjust the streaming test size (default 1024 MB)
NANIO_STREAMING_TEST_MB=128 uv run pytest -m slow

# Load tests (manual; excluded by default)
uv run --extra loadtest locust -f tests/load/locustfile_small.py \
    --host http://127.0.0.1:9000 --headless -u 50 -r 10 -t 60s
```

## Running nanio locally for ad-hoc testing

```bash
NANIO_ACCESS_KEY=test NANIO_SECRET_KEY=test \
    uv run nanio serve --data-dir /tmp/nanio-dev --port 9000
```

Then in another shell:

```bash
uv run python - <<'PY'
import boto3
from botocore.config import Config
s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path", "payload_signing_enabled": False}),
)
s3.create_bucket(Bucket="dev")
s3.put_object(Bucket="dev", Key="hello.txt", Body=b"hello")
print(s3.get_object(Bucket="dev", Key="hello.txt")["Body"].read())
PY
```

## Adding new S3 operations

The work is mechanical but easy to get wrong:

1. Add (or reuse) the storage-layer method on `Storage` /
   `FilesystemStorage` and unit-test it against `tmp_path`.
2. Add the handler in `handlers/object.py` or `handlers/bucket.py`. Wire
   it into the right dispatcher based on method + query params.
3. Add the XML response builder in `xml.py` if a new shape is needed.
4. Add an integration test in `tests/integration/test_boto3_wire.py`
   (or an op-specific file) that uses real boto3 against the
   `live_server` fixture.
5. Run `uv run pytest`. The boto3 wire test is the contract.

Wire-format compatibility is the contract. Anything that breaks an
existing boto3 round trip needs an explicit decision and a new test.

## Code style

- `ruff` enforced (config in `pyproject.toml`)
- `ruff format` enforced — CI runs `ruff format --check src tests`
- `mypy --strict` on `src/nanio` (excludes `_vendor` if any)
- Imports sorted via ruff's I rule
- Line length 100, but ruff's E501 is ignored — let the formatter wrap
- All public functions and classes have a one-line docstring at minimum

## Pre-commit hooks

The repo ships a `.pre-commit-config.yaml` that runs ruff (lint + format) and
basic file hygiene checks before each commit. **Install once after cloning:**

```bash
uv sync                       # installs dev tools (pytest, ruff, pre-commit, ...)
uv run pre-commit install     # wires the git hook
```

Dev tools live under `[dependency-groups] dev` (PEP 735), so a plain
`uv sync` always gives you a complete dev environment. Don't move them
back into `[project.optional-dependencies]` — `pip install nanio[dev]`
isn't the install path we care about, and the optional-extras path
silently breaks the pre-commit hook on a plain `uv sync`.

After that, every `git commit` will run the hooks. If a hook auto-fixes
something, the commit aborts — re-stage the fixes and commit again. Run
manually against the whole repo with `uv run pre-commit run --all-files`.

The ruff version pinned in `.pre-commit-config.yaml` MUST match the one in
`uv.lock`. Bump them together.

## Things NOT to break

- The `Storage` protocol shape — handlers depend on it.
- The on-disk layout (path conventions in `storage/paths.py`) — anyone
  with an existing `--data-dir` should be able to upgrade in place.
- Wire format compatibility for any operation listed in the README's
  feature matrix. The boto3 wire-test suite is the gate.
- The streaming memory ceiling. The slow test enforces it.

## Recent context

The project was bootstrapped on 2026-04-08. The original brief was: "be
like early MinIO, super simple, S3-compatible, file-system backed,
stateless, scales horizontally by launching more processes." Every
design decision in this repo flows from that brief. If a change moves
away from that pitch, push back.
