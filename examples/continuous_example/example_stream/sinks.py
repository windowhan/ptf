"""Product-configured event sinks.

The runtime wraps every emit in lease-fencing checks; the product only
decides where accepted events go. ``ListSink`` is the minimal example —
a real product would publish to Pub/Sub or another durable store here.
"""

from __future__ import annotations

from distributed_runtime.continuous import EventSink, LeaseHandle, SinkGuarantee
from distributed_runtime.core.envelope import VersionedEnvelope


class ListSink(EventSink):
    """Append fenced events to an in-memory list."""

    guarantee = SinkGuarantee.RUNTIME_FENCED

    def __init__(self) -> None:
        self.events: list[VersionedEnvelope] = []

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        self.events.append(event)
