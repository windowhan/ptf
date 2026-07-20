"""Deliberately small application facade for workload registration."""

from __future__ import annotations

from dataclasses import dataclass, field

from distributed_runtime.core.identifiers import WorkloadId
from distributed_runtime.registry import RuntimeRegistry


@dataclass(frozen=True, slots=True)
class RuntimeApplication:
    """Root object owned by one product integration."""

    name: str
    registry: RuntimeRegistry = field(init=False)

    def __post_init__(self) -> None:
        WorkloadId(self.name)
        object.__setattr__(self, "registry", RuntimeRegistry(application=self.name))
