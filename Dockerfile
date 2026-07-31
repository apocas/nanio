# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage: install the locked dependency set + the nanio package into a
# venv with uv. The uv image is python:3.12-slim-bookworm plus a uv binary,
# so the venv's interpreter symlinks resolve identically in the runtime
# stage below (both stages must stay on the same Debian release).
# ---------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Dependency layer — only invalidated when the lockfile changes.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Project layer. README.md and LICENSE are build inputs (`readme` and
# `license-files` in pyproject.toml). --no-editable installs nanio as a
# real wheel into .venv so importlib.metadata reports the true version
# and the venv is self-contained (no reference back to /app/src).
COPY README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# ---------------------------------------------------------------------------
# Runtime stage: plain slim Python, non-root, nothing but the venv.
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="nanio" \
      org.opencontainers.image.description="A minimal, stateless, S3-compatible object storage server." \
      org.opencontainers.image.source="https://github.com/apocas/nanio" \
      org.opencontainers.image.licenses="Apache-2.0"

# Fixed uid/gid 1000 so bind-mount ownership is predictable and documentable.
RUN groupadd --gid 1000 nanio \
    && useradd --uid 1000 --gid nanio --shell /usr/sbin/nologin \
       --home-dir /data --no-create-home nanio \
    && mkdir /data \
    && chown nanio:nanio /data

# The venv stays root-owned (read-only to the nanio user) on purpose.
COPY --from=build /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    NANIO_DATA_DIR=/data

WORKDIR /data
USER nanio
EXPOSE 9000

# Exec-form: nanio (uvicorn) is PID 1 and receives SIGTERM directly.
# `docker run <image> serve --workers 4` and `docker run <image> --version`
# both behave like the bare CLI.
ENTRYPOINT ["nanio"]
CMD ["serve"]
