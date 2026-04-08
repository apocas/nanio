# nanio architecture

## Layers

```
                          ┌─────────────────┐
                          │      CLI        │  cli.py
                          └─────────────────┘
                                  │
                                  ▼
                          ┌─────────────────┐
                          │  Settings       │  config.py
                          └─────────────────┘
                                  │
                                  ▼
                          ┌─────────────────┐
                          │  build_app()    │  app.py
                          └─────────────────┘
                                  │
                                  ▼
       ┌─────────────────────┐ ┌───────────────────────┐
       │  SigV4 middleware   │ │  S3Error handler      │
       │  auth/middleware.py │ │  app._s3_error_handler│
       └─────────────────────┘ └───────────────────────┘
                  │
                  ▼  (ASGI scope, possibly with chunked-decoder receive)
       ┌─────────────────────┐
       │  Route table        │  handlers/routes.py
       └─────────────────────┘
                  │
                  ▼
       ┌─────────────────────┐
       │  Per-resource       │  handlers/{service,bucket,object,multipart}.py
       │  handlers           │
       └─────────────────────┘
                  │
                  ▼
       ┌─────────────────────┐
       │  Storage protocol   │  storage/backend.py
       └─────────────────────┘
                  │
                  ▼
       ┌─────────────────────┐
       │  FilesystemStorage  │  storage/filesystem.py
       │  + paths/metadata/  │  storage/{paths,metadata,multipart}.py
       │    multipart        │
       └─────────────────────┘
                  │
                  ▼
              POSIX FS
```

## Request lifecycle: PutObject

1. **Uvicorn** accepts the connection and hands the ASGI scope to the
   Starlette app.
2. **`SigV4Middleware`** reads the `Authorization` header, looks up the
   secret via `Settings.credentials.resolve(access_key)`, and verifies
   the signature against the canonical request. If the
   `x-amz-content-sha256` header is `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`,
   the middleware wraps the ASGI `receive` callable with the chunked
   decoder so the handler sees plain decoded bytes downstream.
3. **`handlers.routes`** dispatches `PUT /<bucket>/<key>` to
   `handlers.object.dispatch_object`, which routes to `put_object`.
4. **`put_object`** reads custom headers (Content-Type,
   `x-amz-meta-*`, Content-MD5), then calls `storage.put_object` with
   `request.stream()` as the body iterator.
5. **`FilesystemStorage.put_object`** opens a temp file with
   `os.open(path, O_WRONLY | O_CREAT | O_EXCL)` and streams the body
   into it via `asyncio.to_thread(os.write, fd, chunk)`, updating an
   incremental MD5 as it goes.
6. On EOF, the storage layer compares the computed MD5 against
   `Content-MD5` (if provided), atomically renames the temp file to the
   final path, and writes the JSON sidecar metadata via
   `metadata.write_atomic`.
7. The handler returns a 200 with an `ETag` header.

The hot path holds at most one chunk (default 1 MiB) in memory at a
time.

## Request lifecycle: GetObject

1. SigV4 middleware verifies (no chunked decoder for GET).
2. `handlers.object.get_object` reads the sidecar metadata for content
   type, ETag, size, custom user metadata.
3. It parses any `Range:` header.
4. It calls `storage.get_object`, which returns a `GetObjectResult`
   whose `body` is an async generator that does
   `await asyncio.to_thread(os.pread, fd, chunk_size, offset)` in a
   loop until the requested length is satisfied.
5. The handler wraps that generator in a Starlette `StreamingResponse`
   with the right status code (200 or 206), `Content-Length`, and
   `Content-Range` (for partial responses).

## Request lifecycle: Multipart upload

1. **`POST /<bucket>/<key>?uploads`** → `multipart.create`. Generates
   `uploadId = secrets.token_urlsafe(24)` and creates the upload
   directory `<data-dir>/.nanio/multipart/<uploadId>/` with an
   `init.json` file describing the target.
2. **`PUT /<bucket>/<key>?partNumber=N&uploadId=u`** →
   `multipart.upload_part`. Streams the body to
   `<upload-dir>/parts/000NNN.bin` and writes `000NNN.md5` alongside.
   Any worker can handle any part — there is no in-process state.
3. **`POST /<bucket>/<key>?uploadId=u`** → `multipart.complete`. Reads
   the parts list from the request body XML, verifies each part's
   on-disk MD5 matches the client-supplied ETag, concatenates parts
   via `os.sendfile` into `<upload-dir>/assembled.tmp`, computes the
   multipart ETag (`md5(concat(bytes.fromhex(part_md5))).hexdigest() +
   "-N"`), atomically renames the assembled file to the final object
   path, writes metadata, and removes the upload directory.
4. **`DELETE /<bucket>/<key>?uploadId=u`** → `multipart.abort`. Removes
   the upload directory.

The atomic-rename at step 3 is the commit point. Two racing Completes
each build their own scratch file inside their own upload dir; the
second `rename` wins. If the parts lists are identical, the result is
byte-identical (S3 semantics: last-writer-wins).

## SigV4 verification

We verify SigV4 ourselves rather than depending on the `awssig` PyPI
package, which depends on the dead `six` library and forces the entire
request body into memory.

The verifier lives in `auth/sigv4.py`:

- `parse_authorization_header` parses the `Authorization` header.
- `canonical_uri`, `canonical_query_string`, `canonical_headers` build
  the canonical-request blocks per the spec.
- `derive_signing_key` does the four-step HMAC chain
  (date → region → service → "aws4_request").
- `verify_header_auth` orchestrates the above and compares signatures
  in constant time via `hmac.compare_digest`.
- `verify_presigned_url` does the same for query-string presigned URLs.

The chunked decoder (`auth/chunked.py`) handles the `aws-chunked` body
framing. Each chunk's signature chains off the previous chunk's
signature (or the seed signature for the first chunk), per the AWS
streaming-payload spec.

The unit tests in `tests/unit/test_sigv4.py` use boto3's own signer to
produce ground-truth signed requests and verify them with our
verifier. If our verifier ever disagrees with boto3, our verifier is
wrong.

## Stateless multi-process operation

There is no in-process state in nanio:

- No bucket-existence cache
- No object metadata cache
- No listing cache
- No credentials cache beyond the immutable resolver loaded at startup
- No background tasks (they would conflict with the multi-worker model)

Every request derives everything it needs from the filesystem. The
contract for scaling is: point N nanio processes at the same data
directory (over a shared filesystem with atomic rename semantics) and
put any TCP load balancer in front.

See `docs/stateless-scaling.md` for the deployment recipe.
