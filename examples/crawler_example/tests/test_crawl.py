"""Local-kit verification for the crawl.fetch finite workload."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx
import pytest
from crawler_product.app import build_application
from crawler_product.workloads import crawl
from crawler_product.workloads.crawl import next_frontier

from distributed_runtime.core.enums import ExecutionStatus, RunStatus
from distributed_runtime.testing import FiniteRuntimeTestKit

HTML = """<html><head><title>Example Domain</title></head>
<body>
<a href="/about">about</a>
<a href="https://other.example/x#frag">other</a>
<a href="mailto:a@b.c">mail</a>
<a href="/about">dup</a>
</body></html>"""


def _mock_client(responder: Callable[[httpx.Request], httpx.Response]) -> None:
    """Point the workload's client factory at an httpx MockTransport."""
    transport = httpx.MockTransport(responder)
    crawl._client_factory = lambda: httpx.AsyncClient(transport=transport)


def _scripted(*responses: httpx.Response) -> Callable[[httpx.Request], httpx.Response]:
    queue = list(responses)

    def respond(request: httpx.Request) -> httpx.Response:
        return queue.pop(0) if len(queue) > 1 else queue[0]

    return respond


@pytest.fixture(autouse=True)
def _restore_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(crawl, "_client_factory", crawl._client_factory)


def test_fetch_success_and_next_frontier() -> None:
    async def scenario() -> None:
        _mock_client(
            _scripted(httpx.Response(200, text=HTML, headers={"content-type": "text/html"}))
        )
        kit = FiniteRuntimeTestKit(registry=build_application().registry)
        run_id = await kit.submit(
            workload="crawl.fetch",
            version="1.0.0",
            payload={"urls": ["https://example.com/"]},
        )
        attempts = await kit.run_pending(run_id)
        assert [a.status for a in attempts] == [ExecutionStatus.SUCCEEDED]
        (result,) = kit.results(run_id)
        assert isinstance(result, dict)
        assert result["status"] == 200
        assert result["title"] == "Example Domain"
        assert result["links"] == [
            "https://example.com/about",
            "https://other.example/x",
        ]
        assert next_frontier(kit.results(run_id)) == [
            "https://example.com/about",
            "https://other.example/x",
        ]

    asyncio.run(scenario())


def test_server_error_retries_then_succeeds() -> None:
    async def scenario() -> None:
        _mock_client(
            _scripted(
                httpx.Response(500),
                httpx.Response(200, text=HTML, headers={"content-type": "text/html"}),
            )
        )
        kit = FiniteRuntimeTestKit(registry=build_application().registry)
        run_id = await kit.submit(
            workload="crawl.fetch",
            version="1.0.0",
            payload={"urls": ["https://example.com/"]},
        )
        first = await kit.run_pending(run_id)
        assert [a.status for a in first] == [ExecutionStatus.RETRY_SCHEDULED]
        kit.clock.advance(120)
        second = await kit.run_pending(run_id)
        assert [a.status for a in second] == [ExecutionStatus.SUCCEEDED]
        assert second[0].attempt == 2
        assert kit.run(run_id).status is RunStatus.SUCCEEDED

    asyncio.run(scenario())


def test_client_error_is_permanent_not_retried() -> None:
    async def scenario() -> None:
        _mock_client(_scripted(httpx.Response(404)))
        kit = FiniteRuntimeTestKit(registry=build_application().registry)
        run_id = await kit.submit(
            workload="crawl.fetch",
            version="1.0.0",
            payload={"urls": ["https://example.com/missing"]},
        )
        attempts = await kit.run_pending(run_id)
        assert [a.status for a in attempts] == [ExecutionStatus.FAILED]
        assert kit.run(run_id).status is RunStatus.FAILED

    asyncio.run(scenario())


def test_rate_limit_honours_retry_after() -> None:
    async def scenario() -> None:
        _mock_client(
            _scripted(
                httpx.Response(429, headers={"retry-after": "30"}),
                httpx.Response(200, text=HTML, headers={"content-type": "text/html"}),
            )
        )
        kit = FiniteRuntimeTestKit(registry=build_application().registry)
        run_id = await kit.submit(
            workload="crawl.fetch",
            version="1.0.0",
            payload={"urls": ["https://example.com/"]},
        )
        first = await kit.run_pending(run_id)
        assert [a.status for a in first] == [ExecutionStatus.RETRY_SCHEDULED]
        kit.clock.advance(10)  # before retry-after: still not due
        assert await kit.run_pending(run_id) == ()
        kit.clock.advance(30)
        second = await kit.run_pending(run_id)
        assert [a.status for a in second] == [ExecutionStatus.SUCCEEDED]

    asyncio.run(scenario())


def test_planner_normalizes_and_dedups() -> None:
    async def scenario() -> None:
        kit = FiniteRuntimeTestKit(registry=build_application().registry)
        run_id = await kit.submit(
            workload="crawl.fetch",
            version="1.0.0",
            payload={
                "urls": [
                    " https://example.com/#a ",
                    "https://example.com/#b",
                    "ftp://nope.example/",
                    "notaurl",
                    "https://other.example/",
                ]
            },
        )
        units = await kit.replan(run_id)
        assert [u.payload["url"] for u in units] == [
            "https://example.com/",
            "https://other.example/",
        ]

    asyncio.run(scenario())
