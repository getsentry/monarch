# Django settings for the sandbox migrate run: Sentry's own defaults, with DATABASES
# pointed at the monarch Postgres container. Env vars are set by the compose service.
from sentry.conf.server import *  # noqa: F401,F403

import os

DATABASES = {
    "default": {
        "ENGINE": "sentry.db.postgres",
        "NAME": os.environ["SENTRY_DB_NAME"],
        "USER": os.environ["SENTRY_DB_USER"],
        "PASSWORD": os.environ["SENTRY_DB_PASSWORD"],
        "HOST": os.environ["SENTRY_DB_HOST"],
        "PORT": os.environ["SENTRY_DB_PORT"],
    }
}

# A throwaway sandbox key -- migrations sign nothing; this just keeps startup quiet.
SENTRY_OPTIONS["system.secret-key"] = "monarch-sandbox-not-secret"

# `sentry django migrate` boots the whole app, and importing the models wires up service
# backends at import time (the eventattachment model pulls in the attachment cache). Sentry
# refuses to start unless these are configured. Point them at their Django/DB-backed variants:
# a migrate only needs a schema, so nothing here has to talk to Redis or any external service.
SENTRY_CACHE = "sentry.cache.django.DjangoCache"
SENTRY_NODESTORE = "sentry.nodestore.django.DjangoNodeStorage"

# Beyond import time, `sentry django migrate` also validates every service backend at boot
# (ratelimits, quotas, buffer, tsdb, ...), most of which demand Redis. A schema migration needs
# only the database, so trigger Sentry's own skip path: setup_services(validate=False) still
# runs each service's setup() but skips validate(). No CLI flag or env var exposes this, and
# initialize_app calls setup_services as a module global, so wire it on here.
import sentry.runner.initializer as _initializer

_setup_services = _initializer.setup_services
_initializer.setup_services = lambda validate=True: _setup_services(validate=False)
