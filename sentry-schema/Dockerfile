# syntax=docker/dockerfile:1
# Reproducible build of the real Sentry schema: clone getsentry/sentry at a pinned
# revision, install it in an isolated Python 3.13 env, and (via the entrypoint) run its
# real Django migrations against the sandbox Postgres. Nothing here depends on a local
# Sentry checkout -- the pinned revision is the only input.
FROM python:3.13-slim-bookworm

# git for the pinned clone; the -dev libs cover the few Sentry deps that still build from
# source. Database work (create/clone/prune) is done in Python via psycopg2.
# cache mounts: apt's downloads live on the build host and survive rebuilds, so a cache-busted
# layer reinstalls in seconds instead of re-fetching. /var/lib/apt is a mount now, not baked
# into the layer, so no rm cleanup is needed to keep the image small.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        git \
        build-essential \
        pkg-config \
        libxml2-dev \
        libxmlsec1-dev \
        libxslt1-dev \
        libffi-dev \
        libpq-dev

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Bump this to move to a newer Sentry; it is the sole reproducibility anchor.
ARG SENTRY_REF=c310f09830044dae87d3e456785eaee9186bbc81
ENV SENTRY_SRC=/sentry

# Shallow, pinned fetch: the revision anchors reproducibility, --depth 1 keeps it small.
RUN git init "$SENTRY_SRC" \
    && cd "$SENTRY_SRC" \
    && git remote add origin https://github.com/getsentry/sentry.git \
    && git fetch --depth 1 origin "$SENTRY_REF" \
    && git checkout FETCH_HEAD

WORKDIR /sentry

# uv reads .python-version (3.13) and uv.lock; --frozen fails loudly if the lock drifted.
# --no-install-project installs only the locked dependencies (Sentry ships no build
# backend); fast_editable then installs sentry itself, including its console scripts.
# uv's wheel cache persists across rebuilds, so the deps that compile from source (libxmlsec,
# etc.) are reused instead of rebuilt -- the bulk of this layer's cost when it does re-run.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project \
    && uv run python -m tools.fast_editable --path .

# Put the venv's `sentry` console script on PATH for the entrypoint.
ENV PATH="/sentry/.venv/bin:$PATH"
ENV SENTRY_CONF=/etc/sentry

COPY sentry.conf.py /etc/sentry/
COPY apply_schema.py /apply_schema.py

# fleet.yaml + manifest.generated.yaml are mounted at /monarch by the compose service.
ENTRYPOINT ["python", "/apply_schema.py"]
