"""Immutable structured log fields with task-local activation."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from math import isfinite
from types import MappingProxyType
from typing import Self

type LogValue = str | int | float | bool | None

_ACTIVE_FIELDS: ContextVar[Mapping[str, LogValue]] = ContextVar(
    "distributed_runtime_log_fields",
    default=MappingProxyType({}),
)


@dataclass(frozen=True, slots=True)
class LogContext:
    """Reusable fields that never mutate parent or sibling bindings."""

    fields: Mapping[str, LogValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_fields(self.fields)
        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))

    def bind(self, **fields: LogValue) -> Self:
        _validate_fields(fields)
        return type(self)({**self.fields, **fields})

    @contextmanager
    def activate(self) -> Iterator[None]:
        token = _ACTIVE_FIELDS.set(MappingProxyType({**_ACTIVE_FIELDS.get(), **self.fields}))
        try:
            yield
        finally:
            _ACTIVE_FIELDS.reset(token)

    def as_dict(self, event: str, **fields: LogValue) -> dict[str, LogValue]:
        if not event:
            raise ValueError("log event must not be empty")
        _validate_fields(fields)
        return {
            **_ACTIVE_FIELDS.get(),
            **self.fields,
            **fields,
            "event": event,
        }


def _validate_fields(fields: Mapping[str, LogValue]) -> None:
    for key, value in fields.items():
        if not isinstance(key, str) or not key or key == "event":
            raise ValueError("log field names must be strings, non-empty, and not 'event'")
        if value is not None and type(value) not in (str, int, float, bool):
            raise ValueError("log field values must be exact JSON scalars")
        if isinstance(value, float) and not isfinite(value):
            raise ValueError("log field values must be finite JSON numbers")
