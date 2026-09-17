"""Environment-driven configuration for deploy processes.

Every setting is an environment variable so one image serves the worker,
control, and migrate roles without code changes. Secret values never come
from the environment directly — ``RUNTIME_DB_PASSWORD_SECRET`` names a
Secret Manager version fetched by the process identity.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import overload

from distributed_runtime.application import RuntimeApplication
from distributed_runtime.registry import RuntimeRegistry


@overload
def _env(name: str) -> str | None: ...
@overload
def _env(name: str, default: str) -> str: ...


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def _env_required(name: str) -> str:
    value = _env(name)
    if value is None:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def load_registry(spec: str | None) -> RuntimeRegistry | None:
    """Import ``module:callable`` and return the application's registry."""
    if spec is None:
        return None
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute:
        raise RuntimeError(f"invalid app spec {spec!r} — expected 'module:callable'")
    factory: Callable[[], RuntimeApplication] = getattr(
        importlib.import_module(module_name), attribute
    )
    return factory().registry


@dataclass(frozen=True, slots=True)
class DeployConfig:
    """Everything the worker/control/migrate roles need."""

    dsn: str
    project: str
    pool_revision: str
    instance_id: str | None
    pubsub_subscription: str | None
    dispatch_topic: str
    events_topic: str
    lease_seconds: int
    stale_worker_seconds: int
    tick_seconds: float
    heartbeat_seconds: float
    poll_seconds: float
    # "loop": reconciler runs inside the control service.
    # "off": an external trigger (Cloud Scheduler job) drives reconcile.
    reconciler_mode: str = "loop"

    @classmethod
    def from_env(cls) -> DeployConfig:
        dsn = _env("RUNTIME_DSN")
        if dsn is None:
            host = _env_required("RUNTIME_DB_HOST")
            port = _env("RUNTIME_DB_PORT", "5432")
            name = _env("RUNTIME_DB_NAME", "runtime")
            user = _env("RUNTIME_DB_USER", "runtime")
            password = _env("RUNTIME_DB_PASSWORD")
            if password is None:
                secret = _env("RUNTIME_DB_PASSWORD_SECRET")
                if secret is None:
                    raise RuntimeError("set RUNTIME_DB_PASSWORD or RUNTIME_DB_PASSWORD_SECRET")
                password = fetch_secret(secret)
            dsn = f"postgresql://{user}:{password}@{host}:{port}/{name}"
        return cls(
            dsn=dsn,
            project=_env("RUNTIME_PROJECT", ""),
            pool_revision=_env("RUNTIME_POOL_REVISION", "local"),
            instance_id=_env("RUNTIME_INSTANCE_ID"),
            pubsub_subscription=_env("RUNTIME_PUBSUB_SUBSCRIPTION"),
            dispatch_topic=_env("RUNTIME_DISPATCH_TOPIC", "runtime-unit-dispatch"),
            events_topic=_env("RUNTIME_EVENTS_TOPIC", "runtime-events"),
            lease_seconds=int(_env("RUNTIME_LEASE_SECONDS", "60") or "60"),
            stale_worker_seconds=int(_env("RUNTIME_STALE_WORKER_SECONDS", "120") or "120"),
            tick_seconds=float(_env("RUNTIME_TICK_SECONDS", "5") or "5"),
            heartbeat_seconds=float(_env("RUNTIME_HEARTBEAT_SECONDS", "15") or "15"),
            poll_seconds=float(_env("RUNTIME_POLL_SECONDS", "5") or "5"),
            reconciler_mode=_env("RUNTIME_CONTROL_RECONCILER", "loop") or "loop",
        )


def fetch_secret(resource_version: str) -> str:
    """Read a Secret Manager version payload using ambient credentials.

    Imported lazily so non-GCP deployments never require google-auth.
    """
    import base64

    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(credentials)  # type: ignore[no-untyped-call]
    response = session.get(f"https://secretmanager.googleapis.com/v1/{resource_version}:access")
    if response.status_code != 200:
        raise RuntimeError(f"secret access denied ({response.status_code}): {resource_version}")
    return base64.b64decode(response.json()["payload"]["data"]).decode()


def resolve_instance_id(configured: str | None) -> str:
    """Instance identity: env override → GCE metadata → hostname."""
    import socket
    import urllib.request
    import uuid

    if configured:
        return configured
    try:
        request = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/name",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(request, timeout=1.5) as response:
            return str(response.read().decode().strip())
    except Exception:
        pass
    hostname = socket.gethostname()
    return hostname if hostname else f"worker:{uuid.uuid4().hex[:12]}"
