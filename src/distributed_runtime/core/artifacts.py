"""Application-scoped immutable artifact references."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self
from urllib.parse import quote

from distributed_runtime.core.errors import InvariantViolationError

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class ArtifactReference:
    """Immutable, generation-scoped pointer to verified artifact content."""

    application: str
    bucket: str
    object_name: str
    generation: int
    uri: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if _NAME.fullmatch(self.application) is None:
            raise ValueError("artifact application must be a stable name")
        if _NAME.fullmatch(self.bucket) is None:
            raise ValueError("artifact bucket must be a stable name")
        if (
            not self.object_name
            or self.object_name.startswith("/")
            or self.object_name.endswith("/")
        ):
            raise ValueError("artifact object_name must be a non-empty relative object name")
        if isinstance(self.generation, bool) or not isinstance(self.generation, int):
            raise ValueError("artifact generation must be an integer")
        if self.generation < 1:
            raise ValueError("artifact generation must be positive")
        expected_uri = self.build_uri(self.bucket, self.object_name, self.generation)
        if self.uri != expected_uri:
            raise ValueError(f"artifact uri must equal {expected_uri}")
        if _SHA256.fullmatch(self.sha256) is None:
            raise ValueError("artifact sha256 must be 64 lowercase hexadecimal characters")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ValueError("artifact size_bytes must be a non-negative integer")

    @staticmethod
    def build_uri(bucket: str, object_name: str, generation: int) -> str:
        """Build the immutable generation-qualified Cloud Storage URI."""
        return f"gs://{bucket}/{quote(object_name, safe='/')}?generation={generation}"

    @classmethod
    def from_bytes(
        cls,
        *,
        application: str,
        bucket: str,
        object_name: str,
        generation: int,
        content: bytes,
    ) -> Self:
        """Create a reference whose digest and length match known content."""
        return cls(
            application=application,
            bucket=bucket,
            object_name=object_name,
            generation=generation,
            uri=cls.build_uri(bucket, object_name, generation),
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )

    def verify(self, content: bytes) -> None:
        """Reject bytes that do not match this immutable reference."""
        actual_sha256 = hashlib.sha256(content).hexdigest()
        if len(content) != self.size_bytes or actual_sha256 != self.sha256:
            raise InvariantViolationError(
                "artifact content does not match its immutable reference",
                details={
                    "application": self.application,
                    "uri": self.uri,
                    "expected_size_bytes": self.size_bytes,
                    "actual_size_bytes": len(content),
                    "expected_sha256": self.sha256,
                    "actual_sha256": actual_sha256,
                },
            )

    def to_payload(self) -> dict[str, str | int]:
        """Return the canonical scalar mapping embedded in envelopes."""
        return {
            "application": self.application,
            "bucket": self.bucket,
            "object_name": self.object_name,
            "generation": self.generation,
            "uri": self.uri,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> Self:
        """Validate and decode a canonical artifact payload."""
        if any(not isinstance(key, str) for key in payload):
            raise TypeError("artifact keys must be strings")
        required = (
            "application",
            "bucket",
            "object_name",
            "generation",
            "uri",
            "sha256",
            "size_bytes",
        )
        missing = [name for name in required if name not in payload]
        if missing:
            raise ValueError(f"artifact fields missing: {', '.join(missing)}")

        return cls(
            application=_string_field(payload, "application"),
            bucket=_string_field(payload, "bucket"),
            object_name=_string_field(payload, "object_name"),
            generation=_integer_field(payload, "generation"),
            uri=_string_field(payload, "uri"),
            sha256=_string_field(payload, "sha256"),
            size_bytes=_integer_field(payload, "size_bytes"),
        )


def _string_field(payload: Mapping[str, object], name: str) -> str:
    value = payload[name]
    if not isinstance(value, str):
        raise ValueError(f"artifact {name} must be a string")
    return value


def _integer_field(payload: Mapping[str, object], name: str) -> int:
    value = payload[name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"artifact {name} must be an integer")
    return value
