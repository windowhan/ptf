"""CT-ENV: canonical envelope and immutable artifact contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from typing import Any, cast

import pytest

from distributed_runtime.core import (
    ArtifactReference,
    InvariantViolationError,
    JsonValue,
    UnsupportedVersionError,
    VersionedEnvelope,
)
from distributed_runtime.core.envelope import INLINE_PAYLOAD_LIMIT_BYTES


def artifact(application: str = "example") -> ArtifactReference:
    return ArtifactReference.from_bytes(
        application=application,
        bucket="runtime-artifacts",
        object_name="runs/run-1/result.json",
        generation=7,
        content=b'{"ok":true}',
    )


class _DefaultPayload:
    pass


_DEFAULT_PAYLOAD = _DefaultPayload()


def envelope(
    *,
    schema_version: int = 1,
    payload: dict[str, JsonValue] | None | _DefaultPayload = _DEFAULT_PAYLOAD,
    payload_ref: ArtifactReference | None = None,
) -> VersionedEnvelope:
    actual_payload: dict[str, JsonValue] | None = (
        {"z": [1, True, None], "a": {"value": "ok"}}
        if isinstance(payload, _DefaultPayload)
        else payload
    )
    return VersionedEnvelope(
        schema_version=schema_version,
        message_kind="finite.execution",
        run_id="run-1",
        execution_id="execution-1",
        idempotency_key="execution-1",
        application="example",
        workload="snapshot",
        workload_version="1.2.3",
        handler="snapshot",
        attempt_generation=2,
        execution_class="default",
        runtime_pool_revision="finite-r7",
        payload=actual_payload,
        payload_ref=payload_ref,
        trace_context={"traceparent": "00-trace-parent"},
        published_at="2026-07-20T04:00:00Z",
    )


def test_v1_envelope_is_canonical_complete_and_tolerates_optional_fields() -> None:
    first = envelope()
    second = envelope(payload={"a": {"value": "ok"}, "z": [1, True, None]})
    encoded = first.to_json()

    assert encoded == second.to_json()
    assert VersionedEnvelope.from_json(encoded) == first
    value = first.to_dict()
    assert set(value) == {
        "schema_version",
        "message_kind",
        "run_id",
        "execution_id",
        "idempotency_key",
        "application",
        "workload",
        "workload_version",
        "handler",
        "attempt_generation",
        "execution_class",
        "runtime_pool_revision",
        "payload",
        "trace_context",
        "published_at",
    }
    value["future_optional"] = {"accepted": True}
    assert VersionedEnvelope.from_dict(value) == first


def test_supported_version_diagnostics_are_sorted_and_fail_closed() -> None:
    value = envelope().to_dict()
    value["schema_version"] = 2
    with pytest.raises(UnsupportedVersionError) as raised:
        VersionedEnvelope.from_dict(value, supported_versions={3, 1})
    assert raised.value.details == {"schema_version": 2, "supported_versions": (1, 3)}
    with pytest.raises(ValueError, match="schema_version must equal 1"):
        envelope(schema_version=True)


@pytest.mark.parametrize("attempt_generation", [True, 1.0, 1.5])
def test_direct_envelope_rejects_non_integer_attempt_generation(
    attempt_generation: object,
) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        replace(envelope(), attempt_generation=cast(Any, attempt_generation))


def test_direct_envelope_rejects_non_string_json_mapping_keys() -> None:
    with pytest.raises(TypeError, match="payload keys must be strings"):
        envelope(payload=cast(dict[str, JsonValue], {1: "top-level"}))
    with pytest.raises(TypeError, match="payload keys must be strings"):
        envelope(payload=cast(dict[str, JsonValue], {"nested": {1: "invalid"}}))
    with pytest.raises(ValueError, match="trace_context must be a string mapping"):
        replace(envelope(), trace_context=cast(dict[str, str], {1: "invalid"}))


@pytest.mark.parametrize("encoded", ["[]", '"scalar"', "null"])
def test_envelope_json_requires_an_object(encoded: str) -> None:
    with pytest.raises(ValueError, match="must contain an object"):
        VersionedEnvelope.from_json(encoded)


def test_envelope_dict_rejects_non_string_root_keys_before_field_access() -> None:
    value = cast(dict[object, JsonValue], envelope().to_dict())
    value[1] = "invalid"

    with pytest.raises(TypeError, match="envelope keys must be strings"):
        VersionedEnvelope.from_dict(cast(Any, value))


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"schema_version": "1"}, "schema_version must be an integer"),
        ({"payload": "not-an-object"}, "payload must be an object"),
        (
            {"payload": None, "payload_ref": "not-an-object"},
            "payload_ref must be an object",
        ),
        ({"message_kind": 1}, "message_kind must be a string"),
        ({"attempt_generation": "1"}, "attempt_generation must be an integer"),
        ({"trace_context": {"traceparent": 1}}, "trace_context must be a string mapping"),
    ],
)
def test_envelope_decoder_validates_untrusted_field_shapes(
    changes: dict[str, object],
    message: str,
) -> None:
    value = envelope().to_dict()
    value.update(cast(Any, changes))

    with pytest.raises(ValueError, match=message):
        VersionedEnvelope.from_dict(value)


def test_inline_payload_is_frozen_and_enforces_256_kib_threshold() -> None:
    source_items: list[JsonValue] = [{"attempt": 1}]
    source: dict[str, JsonValue] = {"items": source_items}
    message = envelope(payload=source)
    source_items.append("late mutation")
    assert message.to_dict()["payload"] == {"items": [{"attempt": 1}]}
    with pytest.raises(TypeError):
        message.payload["other"] = "value"  # type: ignore[index]
    envelope(payload={"value": "x" * (INLINE_PAYLOAD_LIMIT_BYTES - 249)})
    with pytest.raises(ValueError, match="exceeds 256 KiB"):
        envelope(payload={"value": "x" * INLINE_PAYLOAD_LIMIT_BYTES})


def test_payload_is_exactly_one_inline_or_application_scoped_artifact() -> None:
    reference = artifact()
    message = envelope(payload=None, payload_ref=reference)
    assert message.to_dict()["payload_ref"] == reference.to_payload()
    assert VersionedEnvelope.from_json(message.to_json()) == message
    with pytest.raises(ValueError, match="exactly one"):
        envelope(payload=None)
    with pytest.raises(ValueError, match="exactly one"):
        envelope(payload_ref=reference)
    with pytest.raises(ValueError, match="application must match"):
        envelope(payload=None, payload_ref=artifact("other"))


def test_artifact_reference_round_trips_and_detects_corruption() -> None:
    reference = artifact()
    assert ArtifactReference.from_payload(reference.to_payload()) == reference
    assert reference.uri == ("gs://runtime-artifacts/runs/run-1/result.json?generation=7")
    reference.verify(b'{"ok":true}')
    with pytest.raises(FrozenInstanceError):
        reference.uri = "gs://replacement"  # type: ignore[misc]
    with pytest.raises(InvariantViolationError) as raised:
        reference.verify(b'{"ok":false}')
    assert raised.value.details["application"] == "example"
    assert raised.value.details["actual_sha256"] != reference.sha256


@pytest.mark.parametrize(
    ("payload_change", "message"),
    [
        ({"application": 1}, "application must be a string"),
        ({"generation": "7"}, "generation must be an integer"),
        ({"size_bytes": True}, "size_bytes must be an integer"),
        ({"sha256": None}, "sha256 must be a string"),
        ({"uri": None}, "uri must be a string"),
        ({"object_name": None}, "object_name must be a string"),
        ({"bucket": None}, "bucket must be a string"),
    ],
)
def test_artifact_payload_decoder_validates_field_types(
    payload_change: dict[str, object],
    message: str,
) -> None:
    payload = cast(dict[str, object], artifact().to_payload())
    payload.update(payload_change)

    with pytest.raises(ValueError, match=message):
        ArtifactReference.from_payload(payload)


def test_artifact_payload_decoder_reports_missing_fields() -> None:
    payload = cast(dict[str, object], artifact().to_payload())
    del payload["sha256"]

    with pytest.raises(ValueError, match="artifact fields missing: sha256"):
        ArtifactReference.from_payload(payload)


def test_artifact_payload_decoder_rejects_non_string_keys_directly_and_nested() -> None:
    payload = cast(dict[object, object], artifact().to_payload())
    payload[1] = "invalid"

    with pytest.raises(TypeError, match="artifact keys must be strings"):
        ArtifactReference.from_payload(cast(Any, payload))

    value = envelope(payload=None, payload_ref=artifact()).to_dict()
    value["payload_ref"] = cast(Any, payload)
    with pytest.raises(TypeError, match="artifact keys must be strings"):
        VersionedEnvelope.from_dict(value)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("application", "", "stable name"),
        ("bucket", "bad/bucket", "stable name"),
        ("object_name", "/absolute", "relative object"),
        ("generation", 0, "positive"),
        ("generation", True, "integer"),
        ("uri", "gs://wrong/object?generation=7", "must equal"),
        ("sha256", "ABC", "64 lowercase"),
        ("size_bytes", -1, "non-negative"),
        ("size_bytes", 1.5, "non-negative"),
    ],
)
def test_artifact_reference_validates_immutable_identity(
    field: str, value: object, message: str
) -> None:
    changes = cast(Any, {field: value})
    with pytest.raises(ValueError, match=message):
        replace(artifact(), **changes)
