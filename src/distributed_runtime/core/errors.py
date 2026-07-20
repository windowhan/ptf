"""Structured, transport-neutral runtime error taxonomy."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from math import isfinite
from types import MappingProxyType
from typing import ClassVar, cast

from distributed_runtime.core.enums import ErrorKind


def _freeze_detail(value: object) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("error detail keys must be strings")
        return MappingProxyType({key: _freeze_detail(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_detail(item) for item in value)
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError("error details require finite JSON numbers")
        return value
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"unsupported error detail value: {type(value).__name__}")


def _thaw_detail(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_detail(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_detail(item) for item in value]
    return value


class RuntimeContractError(Exception):
    """Base class for failures a runtime boundary may classify."""

    code: ClassVar[str] = "runtime_contract_error"
    kind: ClassVar[ErrorKind] = ErrorKind.INVARIANT_VIOLATION
    retryable: ClassVar[bool] = False

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
    ) -> None:
        if not message:
            raise ValueError("error message must not be empty")
        super().__init__(message)
        self.message = message
        self.details: Mapping[str, object] = cast(
            Mapping[str, object],
            _freeze_detail({} if details is None else details),
        )

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-compatible diagnostic without exception internals."""
        return {
            "code": self.code,
            "kind": self.kind.value,
            "message": self.message,
            "retryable": self.retryable,
            "details": _thaw_detail(self.details),
        }


class InvalidIdentifierError(RuntimeContractError):
    """Raised when an identifier violates the shared lexical contract."""

    code = "invalid_identifier"
    kind = ErrorKind.INVALID_IDENTIFIER


class InvariantViolationError(RuntimeContractError):
    """Raised when runtime state contradicts an internal contract."""

    code = "invariant_violation"
    kind = ErrorKind.INVARIANT_VIOLATION


class PlanningDriftError(RuntimeContractError):
    """Raised when retry planning differs from the durable planned prefix."""

    code = "planning_drift"
    kind = ErrorKind.PLANNING_DRIFT


class CancelledExecutionError(RuntimeContractError):
    code = "cancelled_execution"
    kind = ErrorKind.CANCELLED


class RetryableExecutionError(RuntimeContractError):
    """A product execution failure that may be attempted again."""

    code = "retryable_execution"
    kind = ErrorKind.RETRYABLE_EXECUTION
    retryable = True


class PermanentExecutionError(RuntimeContractError):
    """A product execution failure that must not be retried automatically."""

    code = "permanent_execution"
    kind = ErrorKind.PERMANENT_EXECUTION


class RateLimitedExecutionError(RetryableExecutionError):
    """A retryable execution failure with a minimum retry delay."""

    code = "rate_limited"
    kind = ErrorKind.RATE_LIMITED

    def __init__(
        self,
        message: str,
        *,
        retry_after: timedelta,
        details: Mapping[str, object] | None = None,
    ) -> None:
        if retry_after <= timedelta(0):
            raise ValueError("retry_after must be positive")
        self.retry_after = retry_after
        rate_limit_details = dict({} if details is None else details)
        rate_limit_details["retry_after_seconds"] = retry_after.total_seconds()
        super().__init__(message, details=rate_limit_details)


class UnsupportedVersionError(RuntimeContractError):
    """Raised when a versioned contract cannot be decoded safely."""

    code = "unsupported_version"
    kind = ErrorKind.UNSUPPORTED_VERSION
