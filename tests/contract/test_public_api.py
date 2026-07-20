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


@pytest.mark.parametrize(
    "version",
    [
        "01.0.0",
        "1.01.0",
        "1.0.01",
        "1.0.0-01",
        "1.0.0-001",
        "1.0.0-alpha..1",
        "1.0.0-alpha_1",
        "1\u0661.0.0",
        "1.2\u0662.0",
        "1.2.3\u0663",
        "1.2.3-1\u0661",
        "1.2.3-alpha.1\u0661",
    ],
)
def test_registry_rejects_invalid_semantic_versions(version: str) -> None:
    app = RuntimeApplication("example")
    with pytest.raises(RegistrationError, match="semantic_version"):
        app.registry.register_finite(
            name="finite",
            semantic_version=version,
            execution_class="default",
            planner=cast(FinitePlanner, Planner("finite", version)),
            handler=cast(FiniteHandler, handler),
        )


@pytest.mark.parametrize(
    "version",
    [
        "0.0.0",
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-1a",
        "1.0.0-0alpha",
        "1.0.0-01a",
        "1.0.0+build.1",
        "1.0.0-alpha+build.1",
    ],
)
def test_registry_accepts_semver_two_versions(version: str) -> None:
    app = RuntimeApplication("example")
    registration = app.registry.register_finite(
        name="finite",
        semantic_version=version,
        execution_class="default",
        planner=cast(FinitePlanner, Planner("finite", version)),
        handler=cast(FiniteHandler, handler),
    )

    assert registration.semantic_version == version


def test_registration_must_match_workload_declaration() -> None:
    app = RuntimeApplication("example")
    with pytest.raises(RegistrationError, match="does not match"):
        app.registry.register_continuous(
            name="different",
            semantic_version="1.0.0",
            execution_class="stateful",
            workload=Stream(),
        )
