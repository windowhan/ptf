"""UT-LIFE: deterministic lifecycle, deadline, and log-context behavior."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

import pytest

from distributed_runtime.core import (
    CancellationError,
    CancellationSource,
    Deadline,
    FakeClock,
    GracefulShutdown,
    LogContext,
    LogValue,
    ShutdownPhase,
)


class FalseyClock(FakeClock):
    def __bool__(self) -> bool:
        return False


def test_fake_clock_advances_wall_and_monotonic_time_together() -> None:
    start = datetime(2026, 7, 20, 3, 0, tzinfo=UTC)
    clock = FakeClock(now=start, monotonic=10.0)

    clock.advance(2.5)

    assert clock.now() == datetime(2026, 7, 20, 3, 0, 2, 500000, tzinfo=UTC)
    assert clock.monotonic() == 12.5
    with pytest.raises(ValueError, match="non-negative"):
        clock.advance(-1)
    with pytest.raises(ValueError, match="timezone-aware"):
        FakeClock(now=datetime(2026, 1, 1))


def test_lifecycle_preserves_explicit_falsey_clock_instances() -> None:
    clock = FalseyClock()
    source = CancellationSource(clock=clock)
    shutdown = GracefulShutdown(clock)

    assert source._clock is clock
    assert shutdown.clock is clock
    assert shutdown.cancellation._clock is clock


def test_deadline_uses_monotonic_boundaries_and_caps_children() -> None:
    clock = FakeClock(monotonic=4.0)
    parent = Deadline.after(clock, 10)
    child = Deadline.after(clock, 30, parent=parent)

    assert parent.expires_at == 14.0
    assert child.expires_at == parent.expires_at
    assert child.remaining_seconds() == 10.0
    assert not bool(child.expired)

    clock.advance(10)

    assert child.expired
    assert child.remaining_seconds() == 0.0
    with pytest.raises(ValueError, match="non-negative"):
        Deadline.after(clock, -0.1)


@pytest.mark.parametrize("duration", [float("nan"), float("inf"), -float("inf")])
def test_lifecycle_rejects_non_finite_durations(duration: float) -> None:
    clock = FakeClock()

    with pytest.raises(ValueError, match="finite"):
        clock.advance(duration)
    with pytest.raises(ValueError, match="finite"):
        Deadline.after(clock, duration)
    with pytest.raises(ValueError, match="finite"):
        GracefulShutdown(clock).begin(duration)


def test_deadlines_reject_mixed_clock_domains() -> None:
    first_clock = FakeClock(monotonic=10)
    second_clock = FakeClock(monotonic=10)
    parent = Deadline.after(first_clock, 30)

    with pytest.raises(ValueError, match="clock"):
        Deadline.after(second_clock, 10, parent=parent)
    with pytest.raises(ValueError, match="clock"):
        CancellationSource(clock=second_clock, deadline=parent)


def test_cancellation_is_idempotent_and_first_reason_wins() -> None:
    source = CancellationSource()

    assert not bool(source.token.is_cancelled)
    assert source.cancel("operator requested shutdown")
    assert not source.cancel("later reason")
    assert source.token.is_cancelled
    assert source.token.reason == "operator requested shutdown"
    with pytest.raises(CancellationError, match="operator requested shutdown"):
        source.token.raise_if_cancelled()


def test_parent_cancellation_propagates_but_child_is_isolated() -> None:
    parent = CancellationSource()
    first = parent.child()
    sibling = parent.child()
    descendant = first.child()

    first.cancel("first stopped")

    assert first.token.is_cancelled
    assert descendant.token.is_cancelled
    assert not parent.token.is_cancelled
    assert not sibling.token.is_cancelled

    parent.cancel("runtime stopped")

    assert sibling.token.reason == "runtime stopped"
    assert parent.child().token.reason == "runtime stopped"
    assert first.token.reason == "first stopped"
    assert not sibling.cancel("too late")
    assert sibling.token.reason == "runtime stopped"


def test_deadline_cancels_token_at_exact_boundary() -> None:
    clock = FakeClock()
    deadline = Deadline.after(clock, 5)
    source = CancellationSource(clock=clock, deadline=deadline)

    clock.advance(4.999)
    assert not bool(source.token.is_cancelled)

    clock.advance(0.001)
    assert source.token.is_cancelled
    assert source.token.reason == "deadline exceeded"


def test_graceful_shutdown_fixes_first_deadline_then_forces_stop() -> None:
    clock = FakeClock()
    shutdown = GracefulShutdown(clock)

    first_deadline = shutdown.begin(120)
    second_deadline = shutdown.begin(5)

    assert shutdown.phase is ShutdownPhase.DRAINING
    assert second_deadline is first_deadline
    assert not shutdown.cancellation.token.is_cancelled

    clock.advance(120)

    assert shutdown.poll() is ShutdownPhase.STOPPED
    assert shutdown.cancellation.token.reason == "graceful shutdown deadline exceeded"


def test_graceful_shutdown_can_finish_before_deadline() -> None:
    shutdown = GracefulShutdown(FakeClock())
    shutdown.begin(120)

    shutdown.stop("work completed")

    assert shutdown.phase is ShutdownPhase.STOPPED
    assert shutdown.cancellation.token.reason == "work completed"
    with pytest.raises(RuntimeError, match="cannot begin"):
        shutdown.begin(1)


def test_log_bindings_are_immutable_nested_and_exception_safe() -> None:
    root = LogContext({"service": "worker"})
    run = root.bind(run_id="run-1")
    sibling = root.bind(run_id="run-2")

    with run.activate():
        assert LogContext().as_dict("started") == {
            "service": "worker",
            "run_id": "run-1",
            "event": "started",
        }
        with (
            pytest.raises(RuntimeError),
            LogContext({"execution_id": "execution-1"}).activate(),
        ):
            raise RuntimeError("handler failed")
        assert LogContext().as_dict("resumed")["run_id"] == "run-1"

    assert root.fields == {"service": "worker"}
    assert sibling.fields["run_id"] == "run-2"
    assert LogContext().as_dict("idle") == {"event": "idle"}


def test_contextvar_log_bindings_are_isolated_between_async_tasks() -> None:
    async def record(run_id: str) -> Mapping[str, object]:
        with LogContext({"run_id": run_id}).activate():
            await asyncio.sleep(0)
            return LogContext().as_dict("handled")

    async def gather() -> list[Mapping[str, object]]:
        return list(await asyncio.gather(record("run-1"), record("run-2")))

    assert asyncio.run(gather()) == [
        {"run_id": "run-1", "event": "handled"},
        {"run_id": "run-2", "event": "handled"},
    ]


def test_log_fields_reject_reserved_or_empty_names() -> None:
    with pytest.raises(ValueError, match="not 'event'"):
        LogContext({"event": "collision"})
    with pytest.raises(ValueError, match="non-empty"):
        LogContext({"": "missing"})
    with pytest.raises(ValueError, match="must be strings"):
        LogContext(cast(Mapping[str, LogValue], {1: "invalid"}))
    with pytest.raises(ValueError, match="exact JSON scalars"):
        LogContext(cast(Mapping[str, LogValue], {"items": []}))
    with pytest.raises(ValueError, match="finite JSON numbers"):
        LogContext({"value": float("nan")})
