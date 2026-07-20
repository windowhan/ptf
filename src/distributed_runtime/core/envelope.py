"""Canonical V1 runtime envelopes."""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import cast

from distributed_runtime.core.artifacts import ArtifactReference
from distributed_runtime.core.errors import UnsupportedVersionError

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type FrozenJsonValue = JsonScalar | tuple[FrozenJsonValue, ...] | Mapping[str, FrozenJsonValue]

INLINE_PAYLOAD_LIMIT_BYTES = 256 * 1024
_KIND = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_SUPPORTED = frozenset({1})


def _freeze(value: JsonValue | FrozenJsonValue) -> FrozenJsonValue:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("payload keys must be strings")
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, tuple | list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, float) and not (-float("inf") < value < float("inf")):
        raise ValueError("payload does not support non-finite numbers")
    if value is None or isinstance(value, str | int | float | bool):
        return value
    raise TypeError(f"unsupported payload value: {type(value).__name__}")


def _thaw(value: JsonValue | FrozenJsonValue) -> JsonValue:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def freeze_json_mapping(payload: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    """Return a deeply immutable JSON mapping for internal contracts."""
    frozen = _freeze(dict(payload))
    return cast(Mapping[str, JsonValue], frozen)


def _required_string(name: str, value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")
    return value


@dataclass(frozen=True, slots=True)
class VersionedEnvelope:
    """Immutable V1 message with canonical JSON encoding."""

    schema_version: int
    message_kind: str
    run_id: str
    execution_id: str
    idempotency_key: str
    application: str
    workload: str
    workload_version: str
    handler: str
    attempt_generation: int
    execution_class: str
    runtime_pool_revision: str
    published_at: str
    payload: Mapping[str, JsonValue] | None = None
    payload_ref: ArtifactReference | None = None
    trace_context: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("schema_version must equal 1")
        if _KIND.fullmatch(self.message_kind) is None:
            raise ValueError("message_kind must be a lowercase dotted identifier")
        for name in (
            "run_id",
            "execution_id",
            "idempotency_key",
            "application",
            "workload",
            "workload_version",
            "handler",
            "execution_class",
            "runtime_pool_revision",
        ):
            _required_string(name, getattr(self, name))
        if type(self.attempt_generation) is not int or self.attempt_generation < 1:
            raise ValueError("attempt_generation must be a positive integer")
        try:
            published = datetime.fromisoformat(self.published_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("published_at must be an RFC 3339 timestamp") from error
        if published.tzinfo is None:
            raise ValueError("published_at must include an offset")
        if (self.payload is None) == (self.payload_ref is None):
            raise ValueError("exactly one of payload or payload_ref is required")
        if self.payload_ref is not None and self.payload_ref.application != self.application:
            raise ValueError("payload_ref application must match envelope application")
        if any(
            not isinstance(key, str) or not key or not isinstance(value, str)
            for key, value in self.trace_context.items()
        ):
            raise ValueError("trace_context must be a string mapping")
        if self.payload is not None:
            frozen = freeze_json_mapping(self.payload)
            encoded = _canonical({key: _thaw(item) for key, item in frozen.items()})
            if len(encoded) > INLINE_PAYLOAD_LIMIT_BYTES:
                raise ValueError("inline payload exceeds 256 KiB; use payload_ref")
            object.__setattr__(self, "payload", frozen)
        object.__setattr__(self, "trace_context", MappingProxyType(dict(self.trace_context)))

    def to_dict(self) -> dict[str, JsonValue]:
        """Return detached JSON data for transport adapters."""
        value: dict[str, JsonValue] = {
            "schema_version": self.schema_version,
            "message_kind": self.message_kind,
            "run_id": self.run_id,
            "execution_id": self.execution_id,
            "idempotency_key": self.idempotency_key,
            "application": self.application,
            "workload": self.workload,
            "workload_version": self.workload_version,
            "handler": self.handler,
            "attempt_generation": self.attempt_generation,
            "execution_class": self.execution_class,
            "runtime_pool_revision": self.runtime_pool_revision,
            "trace_context": dict(self.trace_context),
            "published_at": self.published_at,
        }
        if self.payload is not None:
            value["payload"] = {key: _thaw(item) for key, item in self.payload.items()}
        else:
            payload_ref = self.payload_ref
            if payload_ref is None:
                raise RuntimeError("validated envelope is missing payload data")
            value["payload_ref"] = cast(JsonValue, payload_ref.to_payload())
        return value

    def to_json(self) -> bytes:
        """Encode deterministic UTF-8 JSON for hashing and transport."""
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, JsonValue],
        *,
        supported_versions: Collection[int] = _SUPPORTED,
    ) -> VersionedEnvelope:
        """Decode known V1 fields and ignore unknown optional fields."""
        if any(not isinstance(key, str) for key in value):
            raise TypeError("envelope keys must be strings")
        version = value.get("schema_version")
        if not isinstance(version, int) or isinstance(version, bool):
            raise ValueError("schema_version must be an integer")
        supported = sorted(set(supported_versions))
        if version not in supported:
            raise UnsupportedVersionError(
                f"unsupported envelope schema version: {version}",
                details={"schema_version": version, "supported_versions": supported},
            )

        payload = value.get("payload")
        if payload is not None and not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        payload_ref_data = value.get("payload_ref")
        if payload_ref_data is not None and not isinstance(payload_ref_data, dict):
            raise ValueError("payload_ref must be an object")
        payload_ref = (
            ArtifactReference.from_payload(payload_ref_data)
            if payload_ref_data is not None
            else None
        )
        trace_context = _string_mapping(value.get("trace_context", {}), "trace_context")

        return cls(
            schema_version=version,
            message_kind=_mapping_string(value, "message_kind"),
            run_id=_mapping_string(value, "run_id"),
            execution_id=_mapping_string(value, "execution_id"),
            idempotency_key=_mapping_string(value, "idempotency_key"),
            application=_mapping_string(value, "application"),
            workload=_mapping_string(value, "workload"),
            workload_version=_mapping_string(value, "workload_version"),
            handler=_mapping_string(value, "handler"),
            attempt_generation=_mapping_integer(value, "attempt_generation"),
            execution_class=_mapping_string(value, "execution_class"),
            runtime_pool_revision=_mapping_string(value, "runtime_pool_revision"),
            published_at=_mapping_string(value, "published_at"),
            payload=payload,
            payload_ref=payload_ref,
            trace_context=trace_context,
        )

    @classmethod
    def from_json(
        cls,
        encoded: bytes | str,
        *,
        supported_versions: Collection[int] = _SUPPORTED,
    ) -> VersionedEnvelope:
        """Decode a UTF-8 JSON object through the same version gate."""
        decoded: object = json.loads(encoded)
        if not isinstance(decoded, dict):
            raise ValueError("envelope JSON must contain an object")
        value = cast(Mapping[str, JsonValue], decoded)
        return cls.from_dict(value, supported_versions=supported_versions)


def _mapping_string(value: Mapping[str, JsonValue], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise ValueError(f"{name} must be a string")
    return item


def _mapping_integer(value: Mapping[str, JsonValue], name: str) -> int:
    item = value.get(name)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ValueError(f"{name} must be an integer")
    return item


def _string_mapping(value: JsonValue, name: str) -> dict[str, str]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
    ):
        raise ValueError(f"{name} must be a string mapping")
    return cast(dict[str, str], value)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
