"""Retry classification and backoff for finite unit attempts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from distributed_runtime.core.config import FinitePolicy
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.errors import (
    CancelledExecutionError,
    PermanentExecutionError,
    RateLimitedExecutionError,
    RetryableExecutionError,
    RuntimeContractError,
)


@dataclass(frozen=True, slots=True)
class AttemptOutcome:
    """How one finished attempt should be recorded."""

    outcome: str  # matches finite.complete_attempt outcome strings
    error: JsonValue
    retry_delay: timedelta | None


def backoff_delay(attempt: int, policy: FinitePolicy) -> timedelta:
    """Exponential backoff capped at retry_cap_seconds.

    attempt is 1-based: first failure waits retry_base_seconds.
    """
    exponent = max(0, attempt - 1)
    seconds = min(policy.retry_base_seconds * (2**exponent), policy.retry_cap_seconds)
    return timedelta(seconds=seconds)


def classify_error(exc: BaseException, attempt: int, policy: FinitePolicy) -> AttemptOutcome:
    """Map a handler exception to an outcome, error document, and delay.

    Unknown exceptions are retryable — a transient bug must not silently
    kill a unit, and max_attempts still bounds the loop.
    """
    if isinstance(exc, CancelledExecutionError):
        return AttemptOutcome(
            outcome="cancelled",
            error={"kind": exc.code, "message": str(exc)},
            retry_delay=None,
        )
    if isinstance(exc, RateLimitedExecutionError):
        floor = exc.retry_after
        return AttemptOutcome(
            outcome="rate_limited",
            error=cast(JsonValue, exc.as_dict()),
            retry_delay=max(backoff_delay(attempt, policy), floor),
        )
    if isinstance(exc, PermanentExecutionError):
        return AttemptOutcome(
            outcome="permanent",
            error=cast(JsonValue, exc.as_dict()),
            retry_delay=None,
        )
    if isinstance(exc, RetryableExecutionError):
        return AttemptOutcome(
            outcome="retryable",
            error=cast(JsonValue, exc.as_dict()),
            retry_delay=backoff_delay(attempt, policy),
        )
    if isinstance(exc, RuntimeContractError):
        # contract violations (invalid identifier, drift, ...) are permanent:
        # retrying executes the same wrong code again
        return AttemptOutcome(
            outcome="permanent",
            error=cast(JsonValue, exc.as_dict()),
            retry_delay=None,
        )
    return AttemptOutcome(
        outcome="retryable",
        error={"kind": "unexpected", "message": f"{type(exc).__name__}: {exc}"},
        retry_delay=backoff_delay(attempt, policy),
    )


def retries_exhausted(unit_max_attempts: int, attempt: int) -> bool:
    """True when this attempt was the unit's last allowed try."""
    return attempt >= unit_max_attempts
