"""CT-ENV artifact-reference snapshot before envelopes are introduced."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import cast

import pytest

from distributed_runtime.core import ArtifactReference, InvariantViolationError


def artifact(application: str = "example") -> ArtifactReference:
    return ArtifactReference.from_bytes(
        application=application,
        bucket="runtime-artifacts",
        object_name="runs/run-1/result.json",
        generation=7,
        content=b'{"ok":true}',
    )


def test_artifact_reference_round_trips_and_detects_corruption() -> None:
    reference = artifact()
    assert ArtifactReference.from_payload(reference.to_payload()) == reference
    assert reference.uri == "gs://runtime-artifacts/runs/run-1/result.json?generation=7"
    reference.verify(b'{"ok":true}')
    with pytest.raises(FrozenInstanceError):
        reference.uri = "gs://replacement"  # type: ignore[misc]
    with pytest.raises(InvariantViolationError):
        reference.verify(b'{"ok":false}')


def test_artifact_payload_decoder_rejects_missing_and_mistyped_fields() -> None:
    payload = cast(dict[str, object], artifact().to_payload())
    del payload["sha256"]
    with pytest.raises(ValueError, match="artifact fields missing: sha256"):
        ArtifactReference.from_payload(payload)
    payload = cast(dict[str, object], artifact().to_payload())
    payload["generation"] = "7"
    with pytest.raises(ValueError, match="generation must be an integer"):
        ArtifactReference.from_payload(payload)
