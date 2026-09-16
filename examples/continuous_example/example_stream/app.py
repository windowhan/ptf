"""Composition root for the example product's runtime application."""

from distributed_runtime import RuntimeApplication
from example_stream.sinks import ListSink
from example_stream.workloads.shard_pulse import ShardPulseWorkload


def build_application(sink: ListSink | None = None) -> RuntimeApplication:
    """Register every workload and sink this product serves."""
    app = RuntimeApplication("example-stream")
    app.registry.register_continuous(
        name="shard.pulse",
        semantic_version="1.0.0",
        execution_class="stateful-stream",
        workload=ShardPulseWorkload(),
    )
    app.registry.register_sink("pulses", sink or ListSink())
    return app
