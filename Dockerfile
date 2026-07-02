# Postgres 14 (mirrors Sentry's major version) plus the wal2json logical-decoding
# output plugin, so the stream can consume WAL changes as JSON.
FROM postgres:14
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-14-wal2json \
    && rm -rf /var/lib/apt/lists/*
