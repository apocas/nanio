"""nanio — a minimal, stateless, S3-compatible object storage server."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("nanio")
except PackageNotFoundError:  # editable install before metadata is built
    __version__ = "0.0.0+local"

__all__ = ["__version__"]
