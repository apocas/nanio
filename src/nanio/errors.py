"""S3 error hierarchy.

Every public error nanio returns to a client is an `S3Error` subclass.
The framework's exception handler calls `to_xml()` and uses `http_status`.

The error codes and HTTP statuses match the AWS S3 spec:
https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
"""

from __future__ import annotations

from xml.sax.saxutils import escape

_REQUEST_ID = "0000000000000000"  # placeholder; populated per-request later


class S3Error(Exception):
    """Base class for every S3 protocol error."""

    code: str = "InternalError"
    message: str = "We encountered an internal error. Please try again."
    http_status: int = 500

    def __init__(
        self,
        message: str | None = None,
        *,
        resource: str | None = None,
        request_id: str | None = None,
    ) -> None:
        self.message_text = message or self.message
        self.resource = resource or ""
        self.request_id = request_id or _REQUEST_ID
        super().__init__(f"{self.code}: {self.message_text}")

    def to_xml(self) -> bytes:
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<Error>",
            f"<Code>{escape(self.code)}</Code>",
            f"<Message>{escape(self.message_text)}</Message>",
        ]
        if self.resource:
            parts.append(f"<Resource>{escape(self.resource)}</Resource>")
        parts.append(f"<RequestId>{escape(self.request_id)}</RequestId>")
        parts.append("</Error>")
        return "".join(parts).encode("utf-8")


# ---- 4xx: client errors ----------------------------------------------------


class AccessDenied(S3Error):
    code = "AccessDenied"
    message = "Access Denied"
    http_status = 403


class InvalidAccessKeyId(S3Error):
    code = "InvalidAccessKeyId"
    message = "The AWS Access Key Id you provided does not exist in our records."
    http_status = 403


class SignatureDoesNotMatch(S3Error):
    code = "SignatureDoesNotMatch"
    message = "The request signature we calculated does not match the signature you provided."
    http_status = 403


class AuthorizationHeaderMalformed(S3Error):
    code = "AuthorizationHeaderMalformed"
    message = "The authorization header you provided is invalid."
    http_status = 400


class MissingAuthenticationToken(S3Error):
    code = "MissingAuthenticationToken"
    message = "Request is missing Authentication Token"
    http_status = 403


class RequestTimeTooSkewed(S3Error):
    code = "RequestTimeTooSkewed"
    message = "The difference between the request time and the server's time is too large."
    http_status = 403


class NoSuchBucket(S3Error):
    code = "NoSuchBucket"
    message = "The specified bucket does not exist"
    http_status = 404


class NoSuchKey(S3Error):
    code = "NoSuchKey"
    message = "The specified key does not exist."
    http_status = 404


class NoSuchUpload(S3Error):
    code = "NoSuchUpload"
    message = "The specified multipart upload does not exist."
    http_status = 404


class BucketAlreadyExists(S3Error):
    code = "BucketAlreadyExists"
    message = "The requested bucket name is not available."
    http_status = 409


class BucketAlreadyOwnedByYou(S3Error):
    code = "BucketAlreadyOwnedByYou"
    message = "Your previous request to create the named bucket succeeded and you already own it."
    http_status = 409


class BucketNotEmpty(S3Error):
    code = "BucketNotEmpty"
    message = "The bucket you tried to delete is not empty."
    http_status = 409


class InvalidBucketName(S3Error):
    code = "InvalidBucketName"
    message = "The specified bucket is not valid."
    http_status = 400


class InvalidObjectName(S3Error):
    code = "InvalidObjectName"
    message = "The specified object name is not valid."
    http_status = 400


class InvalidArgument(S3Error):
    code = "InvalidArgument"
    message = "Invalid Argument"
    http_status = 400


class InvalidRequest(S3Error):
    code = "InvalidRequest"
    message = "Invalid Request"
    http_status = 400


class InvalidPart(S3Error):
    code = "InvalidPart"
    message = (
        "One or more of the specified parts could not be found. "
        "The part might not have been uploaded, or the specified entity tag might "
        "not have matched the part's entity tag."
    )
    http_status = 400


class InvalidPartOrder(S3Error):
    code = "InvalidPartOrder"
    message = "The list of parts was not in ascending order. Parts must be ordered by part number."
    http_status = 400


class EntityTooSmall(S3Error):
    code = "EntityTooSmall"
    message = "Your proposed upload is smaller than the minimum allowed object size."
    http_status = 400


class BadDigest(S3Error):
    code = "BadDigest"
    message = "The Content-MD5 you specified did not match what we received."
    http_status = 400


class PreconditionFailed(S3Error):
    code = "PreconditionFailed"
    message = "At least one of the preconditions you specified did not hold."
    http_status = 412


class NotImplemented_(S3Error):
    code = "NotImplemented"
    message = "A header you provided implies functionality that is not implemented."
    http_status = 501


# ---- 5xx: server errors ----------------------------------------------------


class InternalError(S3Error):
    code = "InternalError"
    message = "We encountered an internal error. Please try again."
    http_status = 500
