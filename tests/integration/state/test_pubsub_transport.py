"""IT-PS-01: Pub/Sub transport adapter against the local emulator.

Set RUNTIME_TEST_PUBSUB=host:port (default localhost:58085). Skips when
the emulator is unreachable.
"""

from __future__ import annotations

import asyncio
import os
import socket

import pytest
from google.cloud import pubsub_v1

os.environ.setdefault("PUBSUB_EMULATOR_HOST", "localhost:58085")

from distributed_runtime.core import VersionedEnvelope
from distributed_runtime.gcp.pubsub import PubSubConfig, PubSubTransport


def _emulator_reachable() -> bool:
    host, _, port = os.environ["PUBSUB_EMULATOR_HOST"].partition(":")
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
    except OSError:
        return False
    return True


requires_pubsub = pytest.mark.skipif(
    not _emulator_reachable(), reason="Pub/Sub emulator not reachable"
)

CONFIG = PubSubConfig(project="ptf-local", topic="runtime-units", subscription="w-0")


def _envelope(seq: int) -> VersionedEnvelope:
    return VersionedEnvelope(
        schema_version=1,
        message_kind="unit.dispatch",
        run_id="run:x",
        execution_id=f"execution:{seq}",
        idempotency_key=f"u:{seq}",
        application="test",
        workload="w",
        workload_version="1.0.0",
        handler="h",
        attempt_generation=1,
        execution_class="lightweight",
        runtime_pool_revision="local",
        published_at="2026-01-01T00:00:00Z",
        payload={"n": seq},
    )


@requires_pubsub
def test_publish_and_receive_roundtrip() -> None:
    async def exercise() -> VersionedEnvelope:
        transport = PubSubTransport(CONFIG)
        transport.ensure_topology()
        transport.publish(_envelope(0))
        transport.publish(_envelope(1))
        received: list[VersionedEnvelope] = []
        async for envelope, message in transport.stream():
            received.append(envelope)
            message.ack()
            if len(received) == 2:
                return received[1]
        raise AssertionError("stream ended")

    result = asyncio.run(asyncio.wait_for(exercise(), timeout=30))
    assert result.idempotency_key == "u:1"
    assert result.payload == {"n": 1}


@requires_pubsub
def test_envelope_survives_transport_intact() -> None:
    async def exercise() -> VersionedEnvelope:
        transport = PubSubTransport(CONFIG)
        transport.ensure_topology()
        original = _envelope(7)
        transport.publish(original)
        async for envelope, message in transport.stream():
            if envelope.execution_id == "execution:7":
                message.ack()
                return envelope
            message.nack()
        raise AssertionError("stream ended")

    result = asyncio.run(asyncio.wait_for(exercise(), timeout=30))
    assert result.to_dict() == _envelope(7).to_dict()


@requires_pubsub
def test_poison_message_is_nacked_stream_survives() -> None:
    """A malformed payload nacks (→ dead-letter in GCP) instead of
    killing the consume stream; valid deliveries keep flowing."""

    async def exercise() -> VersionedEnvelope:
        transport = PubSubTransport(CONFIG)
        transport.ensure_topology()
        pubsub_v1.PublisherClient().publish(
            CONFIG.topic_path, b'{"definitely": not-envelope'
        ).result(timeout=10)
        transport.publish(_envelope(9))
        async for envelope, message in transport.stream():
            message.ack()
            if envelope.execution_id == "execution:9":
                return envelope
        raise AssertionError("stream ended")

    result = asyncio.run(asyncio.wait_for(exercise(), timeout=30))
    assert result.execution_id == "execution:9"
