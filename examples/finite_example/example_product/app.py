"""Composition root for the example product's runtime application."""

from distributed_runtime import RuntimeApplication
from example_product.workloads.range_sum import RangeSumPlanner, sum_partial


def build_application() -> RuntimeApplication:
    """Register every workload this product serves."""
    app = RuntimeApplication("example-product")
    app.registry.register_finite(
        name="range.sum",
        semantic_version="1.0.0",
        execution_class="lightweight",
        planner=RangeSumPlanner(),
        handler=sum_partial,
    )
    return app
