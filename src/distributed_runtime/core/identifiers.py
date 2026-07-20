"""Immutable, strongly typed runtime identifiers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar, Self

from distributed_runtime.core.errors import InvalidIdentifierError

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")


@dataclass(frozen=True, slots=True, order=True)
class Identifier:
    """Validated base value for IDs that cross runtime boundaries."""

    value: str

    kind: ClassVar[str] = "identifier"
    max_length: ClassVar[int] = 255

    def __post_init__(self) -> None:
        if not self.value:
            self._raise_invalid("must not be empty")
        if self.value != self.value.strip():
            self._raise_invalid("must not contain leading or trailing whitespace")
        if len(self.value) > self.max_length:
            self._raise_invalid(f"must be at most {self.max_length} characters")
        if _IDENTIFIER_PATTERN.fullmatch(self.value) is None:
            self._raise_invalid("contains unsupported characters")

    def _raise_invalid(self, reason: str) -> None:
        raise InvalidIdentifierError(
            f"invalid {self.kind}: {reason}",
            details={"identifier_kind": self.kind, "value": self.value},
        )

    @classmethod
    def parse(cls, value: str) -> Self:
        """Construct the concrete identifier from a boundary string."""
        return cls(value=value)

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True, order=True)
class WorkloadId(Identifier):
    """Stable logical workload registration identifier."""

    kind: ClassVar[str] = "workload_id"


@dataclass(frozen=True, slots=True, order=True)
class RevisionId(Identifier):
    """Immutable planner or execution revision identifier."""

    kind: ClassVar[str] = "revision_id"


@dataclass(frozen=True, slots=True, order=True)
class RunId(Identifier):
    """Stable finite workload submission identifier."""

    kind: ClassVar[str] = "run_id"


@dataclass(frozen=True, slots=True, order=True)
class ExecutionId(Identifier):
    """Stable identifier for one finite execution unit."""

    kind: ClassVar[str] = "execution_id"


@dataclass(frozen=True, slots=True, order=True)
class DeploymentId(Identifier):
    """Stable continuous workload deployment identifier."""

    kind: ClassVar[str] = "deployment_id"


@dataclass(frozen=True, slots=True, order=True)
class PartitionId(Identifier):
    """Stable partition identifier within a continuous deployment."""

    kind: ClassVar[str] = "partition_id"


@dataclass(frozen=True, slots=True, order=True)
class RuntimeInstanceId(Identifier):
    """Identifier for a worker runtime process or VM instance."""

    kind: ClassVar[str] = "runtime_instance_id"


@dataclass(frozen=True, slots=True, order=True)
class ArtifactId(Identifier):
    """Stable immutable artifact identifier."""

    kind: ClassVar[str] = "artifact_id"
