"""Structured failures required by identifier boundary validation."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import ClassVar

from distributed_runtime.core.enums import ErrorKind


class RuntimeContractError(Exception):
    """Base class for failures a runtime boundary may classify."""

    code: ClassVar[str] = "runtime_contract_error"
    kind: ClassVar[ErrorKind] = ErrorKind.INVARIANT_VIOLATION
    retryable: ClassVar[bool] = False

    def __init__(self, message: str, *, details: Mapping[str, object] | None = None) -> None:
        if not message:
            raise ValueError("error message must not be empty")
        super().__init__(message)
        self.message = message
        self.details: Mapping[str, object] = MappingProxyType(
            dict({} if details is None else details)
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "kind": self.kind.value,
            "message": self.message,
            "retryable": self.retryable,
            "details": dict(self.details),
        }


class InvalidIdentifierError(RuntimeContractError):
    """Raised when a strongly typed identifier rejects boundary input."""

    code = "invalid_identifier"
    kind = ErrorKind.INVALID_IDENTIFIER
