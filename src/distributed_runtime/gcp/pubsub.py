"""Pub/Sub transport adapter for VersionedEnvelope messages.

All Google client usage is isolated to this module — core, finite, and
continuous never import it. The adapter is transport-only: it publishes
envelope bytes and yields received messages; claim/ack semantics live in
the worker and state layers.
"""

from __future__ import annotations

import asyncio
import contextlib
import queue
from collections.abc import AsyncIterator
from dataclasses import dataclass

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message

from distributed_runtime.core.envelope import VersionedEnvelope


@dataclass(frozen=True, slots=True)
class PubSubConfig:
    """Connection settings. PUBSUB_EMULATOR_HOST is honored by the clients."""

    project: str
    topic: str
    subscription: str

    @property
    def topic_path(self) -> str:
        return f"projects/{self.project}/topics/{self.topic}"

    @property
    def subscription_path(self) -> str:
        return f"projects/{self.project}/subscriptions/{self.subscription}"


def ensure_topic(publisher: pubsub_v1.PublisherClient, config: PubSubConfig) -> str:
    """Create the topic if absent; return its path."""
    with contextlib.suppress(Exception):
        publisher.create_topic(request={"name": config.topic_path})
    return config.topic_path


def ensure_subscription(subscriber: pubsub_v1.SubscriberClient, config: PubSubConfig) -> str:
    """Create a pull subscription bound to the topic if absent."""
    with contextlib.suppress(Exception):
        subscriber.create_subscription(
            request={
                "name": config.subscription_path,
                "topic": config.topic_path,
            }
        )
    return config.subscription_path


class PubSubTransport:
    """Publish envelopes and stream received deliveries."""

    def __init__(self, config: PubSubConfig) -> None:
        self.config = config
        self._publisher = pubsub_v1.PublisherClient()
        self._subscriber = pubsub_v1.SubscriberClient()

    def ensure_topology(self) -> None:
        ensure_topic(self._publisher, self.config)
        ensure_subscription(self._subscriber, self.config)

    def publish(self, envelope: VersionedEnvelope) -> str:
        """Publish one envelope; returns the Pub/Sub message id."""
        future = self._publisher.publish(
            self.config.topic_path,
            envelope.to_json(),
            ordering_key="",
            message_kind=envelope.message_kind,
            workload=envelope.workload,
            idempotency_key=envelope.idempotency_key,
            runtime_pool_revision=envelope.runtime_pool_revision,
        )
        return str(future.result(timeout=30))

    async def stream(self) -> AsyncIterator[tuple[VersionedEnvelope, Message]]:
        """Yield (envelope, raw message) deliveries forever.

        The caller decides ack/nack on the raw message — ack only after
        durable state commit. Pull is explicit so tests control pacing.
        """
        deliveries: queue.Queue[Message] = queue.Queue()
        streaming = self._subscriber.subscribe(
            self.config.subscription_path, callback=deliveries.put
        )
        try:
            while True:
                try:
                    message = deliveries.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.05)
                    continue
                try:
                    envelope = VersionedEnvelope.from_json(message.data)
                except Exception:
                    # Poison pill: nack so the dead-letter policy takes it
                    # instead of crashing the stream and holding the lease.
                    message.nack()
                    continue
                yield envelope, message
        finally:
            streaming.cancel()
