"""Typed registry for workload and sink contracts."""

from __future__ import annotations

import re
from dataclasses import dataclass

from distributed_runtime.continuous import ContinuousWorkload, EventSink
from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.identifiers import WorkloadId
from distributed_runtime.finite import FiniteHandler, FinitePlanner


class RegistrationError(ValueError):
    """Raised when registry identity or type invariants are violated."""


_SEMANTIC_VERSION = re.compile(
    r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    r"(?:-(?:0|[1-9][0-9]*|(?=[0-9A-Za-z-]*[A-Za-z-])[0-9A-Za-z-]+)"
    r"(?:\.(?:0|[1-9][0-9]*|(?=[0-9A-Za-z-]*[A-Za-z-])[0-9A-Za-z-]+))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)


@dataclass(frozen=True, slots=True, order=True)
class WorkloadIdentity:
    application: WorkloadId
    workload_name: WorkloadId
    semantic_version: str
    mode: WorkloadMode


@dataclass(frozen=True, slots=True)
class FiniteRegistration:
    application: WorkloadId
    name: WorkloadId
    semantic_version: str
    execution_class: str
    planner: FinitePlanner
    handler: FiniteHandler

    @property
    def mode(self) -> WorkloadMode:
        return WorkloadMode.FINITE


@dataclass(frozen=True, slots=True)
class ContinuousRegistration:
    application: WorkloadId
    name: WorkloadId
    semantic_version: str
    execution_class: str
    workload: ContinuousWorkload

    @property
    def mode(self) -> WorkloadMode:
        return WorkloadMode.CONTINUOUS


type WorkloadRegistration = FiniteRegistration | ContinuousRegistration


class RuntimeRegistry:
    """Mutable composition root that rejects ambiguous registrations."""

    def __init__(self, *, application: str) -> None:
        self.application = WorkloadId(application)
        self._workloads: dict[WorkloadIdentity, WorkloadRegistration] = {}
        self._sinks: dict[str, EventSink] = {}

    def register_finite(
        self,
        *,
        name: str,
        semantic_version: str,
        execution_class: str,
        planner: FinitePlanner,
        handler: FiniteHandler,
    ) -> FiniteRegistration:
        registration = FiniteRegistration(
            application=self.application,
            name=WorkloadId(name),
            semantic_version=_semantic_version(semantic_version),
            execution_class=_required("execution_class", execution_class),
            planner=planner,
            handler=handler,
        )
        _validate_declaration(
            planner, registration.name, registration.semantic_version, registration.mode
        )
        self._add_workload(registration)
        return registration

    def register_continuous(
        self,
        *,
        name: str,
        semantic_version: str,
        execution_class: str,
        workload: ContinuousWorkload,
    ) -> ContinuousRegistration:
        registration = ContinuousRegistration(
            application=self.application,
            name=WorkloadId(name),
            semantic_version=_semantic_version(semantic_version),
            execution_class=_required("execution_class", execution_class),
            workload=workload,
        )
        _validate_declaration(
            workload,
            registration.name,
            registration.semantic_version,
            registration.mode,
        )
        self._add_workload(registration)
        return registration

    def register_sink(self, name: str, sink: EventSink) -> None:
        sink_name = _required("sink name", name)
        if sink_name in self._sinks:
            raise RegistrationError(f"sink already registered: {sink_name}")
        self._sinks[sink_name] = sink

    def workload(
        self,
        name: str,
        semantic_version: str,
        mode: WorkloadMode,
    ) -> WorkloadRegistration:
        identity = WorkloadIdentity(
            self.application,
            WorkloadId(name),
            _semantic_version(semantic_version),
            mode,
        )
        try:
            return self._workloads[identity]
        except KeyError as error:
            raise RegistrationError(f"unknown workload: {identity}") from error

    def sink(self, name: str) -> EventSink:
        try:
            return self._sinks[name]
        except KeyError as error:
            raise RegistrationError(f"unknown sink: {name}") from error

    def workloads(self) -> tuple[WorkloadRegistration, ...]:
        return tuple(self._workloads[key] for key in sorted(self._workloads))

    def _add_workload(self, registration: WorkloadRegistration) -> None:
        identity = WorkloadIdentity(
            registration.application,
            registration.name,
            registration.semantic_version,
            registration.mode,
        )
        if identity in self._workloads:
            raise RegistrationError(f"workload already registered: {identity}")
        self._workloads[identity] = registration


def _required(name: str, value: str) -> str:
    if not value or value != value.strip():
        raise RegistrationError(f"{name} must be a non-empty trimmed string")
    return value


def _semantic_version(value: str) -> str:
    if _SEMANTIC_VERSION.fullmatch(value) is None:
        raise RegistrationError("semantic_version must use major.minor.patch form")
    return value


def _validate_declaration(
    workload: object,
    name: WorkloadId,
    semantic_version: str,
    mode: WorkloadMode,
) -> None:
    declared = (
        getattr(workload, "name", None),
        getattr(workload, "version", None),
        getattr(workload, "mode", None),
    )
    expected = (str(name), semantic_version, mode)
    if declared != expected:
        raise RegistrationError(
            f"workload declaration {declared!r} does not match registration {expected!r}"
        )
