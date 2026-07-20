"""Deterministic process lifecycle and cooperative cancellation primitives."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from math import isfinite
from typing import Protocol


class Clock(Protocol):
    """Wall and monotonic time used by local process coordination."""

    def now(self) -> datetime: ...

    def monotonic(self) -> float: ...


class SystemClock:
    """Production clock backed by the host process."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()


class FakeClock:
    """Explicitly advanced clock for deterministic tests and local kits."""

    def __init__(
        self,
        *,
        now: datetime | None = None,
        monotonic: float = 0.0,
    ) -> None:
        self._now = now or datetime(2000, 1, 1, tzinfo=UTC)
        if self._now.tzinfo is None or self._now.utcoffset() != timedelta(0):
            raise ValueError("clock wall time must be timezone-aware UTC")
        if isinstance(monotonic, bool) or not isfinite(monotonic) or monotonic < 0:
            raise ValueError("monotonic time must be finite and non-negative")
        self._monotonic = float(monotonic)

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        if isinstance(seconds, bool) or not isfinite(seconds) or seconds < 0:
            raise ValueError("clock advance must be finite and non-negative")
        self._now += timedelta(seconds=seconds)
        self._monotonic += seconds


@dataclass(frozen=True, slots=True)
class Deadline:
    """A process-local deadline measured only with monotonic time."""

    clock: Clock
    expires_at: float

    def __post_init__(self) -> None:
        if isinstance(self.expires_at, bool) or not isfinite(self.expires_at):
            raise ValueError("deadline expiry must be finite")

    @classmethod
    def after(
        cls,
        clock: Clock,
        timeout_seconds: float,
        *,
        parent: Deadline | None = None,
    ) -> Deadline:
        if (
            isinstance(timeout_seconds, bool)
            or not isfinite(timeout_seconds)
            or timeout_seconds < 0
        ):
            raise ValueError("deadline timeout must be finite and non-negative")
        expires_at = clock.monotonic() + timeout_seconds
        if parent is not None:
            if parent.clock is not clock:
                raise ValueError("parent deadline must use the same clock")
            expires_at = min(expires_at, parent.expires_at)
        return cls(clock=clock, expires_at=expires_at)

    def remaining_seconds(self) -> float:
        return max(0.0, self.expires_at - self.clock.monotonic())

    @property
    def expired(self) -> bool:
        return self.clock.monotonic() >= self.expires_at


class CancellationError(RuntimeError):
    """Raised at a cooperative cancellation checkpoint."""


class CancellationToken:
    """Read-only cancellation state supplied to product code."""

    def __init__(self, source: CancellationSource) -> None:
        self._source = source

    @property
    def is_cancelled(self) -> bool:
        return self._source.is_cancelled

    @property
    def reason(self) -> str | None:
        return self._source.reason

    def raise_if_cancelled(self) -> None:
        if self.is_cancelled:
            raise CancellationError(self.reason or "cancelled")


class CancellationSource:
    """Runtime-owned authority that can create isolated child scopes."""

    def __init__(
        self,
        *,
        clock: Clock | None = None,
        deadline: Deadline | None = None,
        parent: CancellationToken | None = None,
    ) -> None:
        self._clock = SystemClock() if clock is None else clock
        if deadline is not None and deadline.clock is not self._clock:
            raise ValueError("deadline must use the cancellation source clock")
        self._deadline = deadline
        self._parent = parent
        self._reason: str | None = None
        self._token = CancellationToken(self)

    @property
    def token(self) -> CancellationToken:
        return self._token

    @property
    def is_cancelled(self) -> bool:
        return (
            self._reason is not None
            or (self._parent is not None and self._parent.is_cancelled)
            or (self._deadline is not None and self._deadline.expired)
        )

    @property
    def reason(self) -> str | None:
        if self._reason is not None:
            return self._reason
        if self._parent is not None and self._parent.is_cancelled:
            return self._parent.reason
        if self._deadline is not None and self._deadline.expired:
            return "deadline exceeded"
        return None

    def cancel(self, reason: str = "cancelled") -> bool:
        if not reason:
            raise ValueError("cancellation reason must not be empty")
        if self.is_cancelled:
            return False
        self._reason = reason
        return True

    def child(self, *, deadline: Deadline | None = None) -> CancellationSource:
        if deadline is not None and deadline.clock is not self._clock:
            raise ValueError("child deadline must use the parent clock")
        child_deadline = deadline
        if self._deadline is not None:
            if child_deadline is None:
                child_deadline = self._deadline
            else:
                child_deadline = Deadline(
                    clock=child_deadline.clock,
                    expires_at=min(child_deadline.expires_at, self._deadline.expires_at),
                )
        return CancellationSource(
            clock=self._clock,
            deadline=child_deadline,
            parent=self.token,
        )


class ShutdownPhase(StrEnum):
    RUNNING = "running"
    DRAINING = "draining"
    STOPPED = "stopped"


class GracefulShutdown:
    """Tracks one fixed grace window before forced cancellation."""

    def __init__(self, clock: Clock | None = None) -> None:
        self.clock = SystemClock() if clock is None else clock
        self.cancellation = CancellationSource(clock=self.clock)
        self.phase = ShutdownPhase.RUNNING
        self.deadline: Deadline | None = None

    def begin(self, grace_seconds: float) -> Deadline:
        if self.phase is ShutdownPhase.STOPPED:
            raise RuntimeError("stopped shutdown cannot begin draining")
        if self.phase is ShutdownPhase.RUNNING:
            self.deadline = Deadline.after(self.clock, grace_seconds)
            self.phase = ShutdownPhase.DRAINING
        assert self.deadline is not None
        return self.deadline

    def poll(self) -> ShutdownPhase:
        if (
            self.phase is ShutdownPhase.DRAINING
            and self.deadline is not None
            and self.deadline.expired
        ):
            self.cancellation.cancel("graceful shutdown deadline exceeded")
            self.phase = ShutdownPhase.STOPPED
        return self.phase

    def stop(self, reason: str = "shutdown complete") -> None:
        self.cancellation.cancel(reason)
        self.phase = ShutdownPhase.STOPPED
