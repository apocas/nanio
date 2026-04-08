"""Route table for nanio.

S3 routes are quirky because the *same path* serves different operations
depending on query string parameters and HTTP method. We use a small
dispatcher per path shape rather than registering 30+ Starlette routes.
"""

from __future__ import annotations

from starlette.routing import Route

from nanio.handlers.bucket import dispatch_bucket
from nanio.handlers.object import dispatch_object
from nanio.handlers.service import list_buckets


def build_routes() -> list[Route]:
    return [
        Route("/", list_buckets, methods=["GET", "HEAD"]),
        Route("/{bucket}", dispatch_bucket, methods=["GET", "HEAD", "PUT", "DELETE", "POST"]),
        Route("/{bucket}/", dispatch_bucket, methods=["GET", "HEAD", "PUT", "DELETE", "POST"]),
        Route(
            "/{bucket}/{key:path}",
            dispatch_object,
            methods=["GET", "HEAD", "PUT", "DELETE", "POST"],
        ),
    ]
