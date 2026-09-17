"""Composition root for the crawler product's runtime application."""

from crawler_product.workloads.crawl import CrawlPlanner, fetch_page
from distributed_runtime import RuntimeApplication


def build_application() -> RuntimeApplication:
    """Register every workload this product serves."""
    app = RuntimeApplication("crawler-product")
    app.registry.register_finite(
        name="crawl.fetch",
        semantic_version="1.0.0",
        execution_class="lightweight",
        planner=CrawlPlanner(),
        handler=fetch_page,
    )
    return app
