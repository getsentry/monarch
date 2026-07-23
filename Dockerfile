# syntax=docker/dockerfile:1
# Monarch app image: the dashboard and the per-store workers run from this one image;
# k8s sets command/args per role (["dashboard"] or ["worker", "--store", "<name>"]).
FROM python:3.14-slim-bookworm

# uv drives the install (uv.lock + `uv sync --frozen` = reproducible). The ghcr.io uv image
# bundles it, but ghcr.io isn't reachable from the build network, so pull uv from PyPI instead.
# only-system: use this base's Python 3.14, don't let uv download a second managed interpreter.
RUN pip install --no-cache-dir uv
ENV UV_PYTHON_PREFERENCE=only-system

WORKDIR /app

# Resolve deps first (without the project) so this layer caches across app-only changes.
# uv's wheel cache persists across rebuilds, so a lock change reuses unchanged deps.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Then just the package source, and install the project itself. Only monarch/ is needed to
# install; config (fleet.yaml, manifest*.yaml) is mounted from a ConfigMap at runtime, never
# baked in, so one image serves any cell.
COPY monarch/ ./monarch/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev
ENV PATH="/app/.venv/bin:$PATH"

# Cell independent schema manifest
COPY manifest.generated.yaml ./

# Dashboard UI runs on 8008, workers don't expose anything.
EXPOSE 8008

ENTRYPOINT ["monarch"]
