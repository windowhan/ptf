"""Unit tests for retry classification and backoff (IT-RETRY-01 helpers)."""

from __future__ import annotations

from datetime import timedelta

from distributed_runtime.core.config import FinitePolicy
from distributed_runtime.core.errors import (
    CancelledExecutionError,
    InvariantViolationError,
    PermanentExecutionError,
    RateLimitedExecutionError,
    RetryableExecutionError,
)
from distributed_runtime.worker.retry import (
    backoff_delay,
    classify_error,
    retries_exhausted,
)

POLICY = FinitePolicy(retry_base_seconds=5, retry_cap_seconds=60)


def test_backoff_grows_exponentially_and_caps() -> None:
    assert backoff_delay(1, POLICY) == timedelta(seconds=5)
    assert backoff_delay(2, POLICY) == timedelta(seconds=10)
    assert backoff_delay(3, POLICY) == timedelta(seconds=20)
    assert backoff_delay(10, POLICY) == timedelta(seconds=60)  # capped


def test_classify_retryable() -> None:
    outcome = classify_error(RetryableExecutionError("flaky"), 1, POLICY)
    assert outcome.outcome == "retryable"
    assert outcome.retry_delay == timedelta(seconds=5)


def test_classify_permanent_and_contract_errors() -> None:
    assert classify_error(PermanentExecutionError("bad"), 1, POLICY).outcome == "permanent"
    # contract violations are permanent — retrying runs the same wrong code
    assert (
        classify_error(InvariantViolationError("x", details={}), 1, POLICY).outcome == "permanent"
    )


def test_classify_rate_limited_respects_retry_after_floor() -> None:
    exc = RateLimitedExecutionError("slow", retry_after=timedelta(seconds=120))
    outcome = classify_error(exc, 1, POLICY)
    assert outcome.outcome == "rate_limited"
    assert outcome.retry_delay == timedelta(seconds=120)  # floor wins over backoff


def test_classify_cancelled_and_unknown() -> None:
    assert classify_error(CancelledExecutionError("stop"), 1, POLICY).outcome == "cancelled"
    unknown = classify_error(RuntimeError("boom"), 1, POLICY)
    assert unknown.outcome == "retryable"  # transient-safe default


def test_retries_exhausted() -> None:
    assert not retries_exhausted(3, 1)
    assert retries_exhausted(3, 3)
