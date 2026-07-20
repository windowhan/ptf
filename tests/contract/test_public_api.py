"""CT-API: registry behavior and deliberately limited root facade."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import cast

import pytest

import distributed_runtime
from distributed_runtime import RuntimeApplication
from distributed_runtime.continuous import (
    ContinuousWorkload,
    LeaseHandle,
    Partition,
    PartitionContext,
    SinkGuarantee,
)
from distributed_runtime.core import (
    InvalidIdentifierError,
    JsonValue,
    VersionedEnvelope,
    WorkloadMode,
)
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    ExecutionUnit,
    FiniteHandler,
    FinitePlanner,
    WorkloadRequest,
)
from distributed_runtime.registry import (
    ContinuousRegistration,
    FiniteRegistration,
    RegistrationError,
)


class Planner:
    mode = WorkloadMode.FINITE

    def __init__(self, name: str = "snapshot", version: str = "1.0.0") -> None:
        self.name = name
        self.version = version

    async def __call__(self, _: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        yield ExecutionUnit(
            unit_key="one",
            handler="handler",
            payload={},
            execution_class="default",
        )


async def handler(
    _: ExecutionContext,
    payload: Mapping[str, JsonValue],
) -> ExecutionResult:
    return dict(payload)


class Stream:
    mode = WorkloadMode.CONTINUOUS

    def __init__(self, name: str = "stream", version: str = "1.0.0") -> None:
        self.name = name
        self.version = version

    async def discover_partitions(self) -> Sequence[Partition]:
        return []

    async def run_partition(
        self,
        context: PartitionContext,
        partition: Partition,
    ) -> None:
        del context, partition


class Sink:
    guarantee = SinkGuarantee.RUNTIME_FENCED

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        del event, stable_id, lease


def test_root_facade_exports_only_application_and_version() -> None:
    assert distributed_runtime.__all__ == ["RuntimeApplication", "__version__"]
    assert RuntimeApplication is distributed_runtime.RuntimeApplication
    assert not hasattr(distributed_runtime, "RuntimeRegistry")
    assert not hasattr(distributed_runtime, "ExecutionUnit")


def test_registry_supports_both_workload_modes() -> None:
    app = RuntimeApplication("example-product")
    finite = app.registry.register_finite(
        name="snapshot",
        semantic_version="1.0.0",
        execution_class="default",
        planner=cast(FinitePlanner, Planner()),
        handler=cast(FiniteHandler, handler),
    )
    continuous = app.registry.register_continuous(
        name="stream",
        semantic_version="1.0.0",
        execution_class="stateful",
        workload=Stream(),
    )

    assert isinstance(finite, FiniteRegistration)
    assert isinstance(continuous, ContinuousRegistration)
    assert len(app.registry.workloads()) == 2
    assert app.registry.workloads() == (finite, continuous)
    assert app.registry.workload("snapshot", "1.0.0", WorkloadMode.FINITE) is finite
    with pytest.raises(RegistrationError, match="unknown workload"):
        app.registry.workload("missing", "1.0.0", WorkloadMode.FINITE)


def test_runtime_protocols_include_registry_declarations() -> None:
    assert isinstance(Planner(), FinitePlanner)
    assert isinstance(Stream(), ContinuousWorkload)

    class CallOnlyPlanner:
        def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
            return Planner().__call__(request)

    assert not isinstance(CallOnlyPlanner(), FinitePlanner)


def test_registry_identity_allows_versions_and_modes_but_rejects_exact_duplicates() -> None:
    app = RuntimeApplication("example")
    finite = app.registry.register_finite(
        name="same",
        semantic_version="1.0.0",
        execution_class="default",
        planner=cast(FinitePlanner, Planner("same")),
        handler=cast(FiniteHandler, handler),
    )
    continuous = app.registry.register_continuous(
        name="same",
        semantic_version="1.0.0",
        execution_class="stateful",
        workload=Stream("same"),
    )
    finite_v2 = app.registry.register_finite(
        name="same",
        semantic_version="2.0.0",
        execution_class="default",
        planner=cast(FinitePlanner, Planner("same", "2.0.0")),
        handler=cast(FiniteHandler, handler),
    )
    assert finite.mode is WorkloadMode.FINITE
    assert continuous.mode is WorkloadMode.CONTINUOUS
    assert app.registry.workload("same", "2.0.0", WorkloadMode.FINITE) is finite_v2

    with pytest.raises(RegistrationError, match="already registered"):
        app.registry.register_finite(
            name="same",
            semantic_version="1.0.0",
            execution_class="default",
            planner=cast(FinitePlanner, Planner("same")),
            handler=cast(FiniteHandler, handler),
        )


def test_sink_registration_and_lookup_are_explicit() -> None:
    app = RuntimeApplication("example")
    sink = Sink()
    app.registry.register_sink("events", sink)

    assert app.registry.sink("events") is sink
    with pytest.raises(RegistrationError, match="already registered"):
        app.registry.register_sink("events", sink)
    with pytest.raises(RegistrationError, match="unknown sink"):
        app.registry.sink("missing")


def test_application_and_registry_validate_names() -> None:
    with pytest.raises(InvalidIdentifierError):
        RuntimeApplication("contains space")
    app = RuntimeApplication("valid")
    with pytest.raises(RegistrationError, match="version"):
        app.registry.register_finite(
            name="finite",
            semantic_version="",
            execution_class="default",
            planner=cast(FinitePlanner, Planner()),
            handler=cast(FiniteHandler, handler),
        )
