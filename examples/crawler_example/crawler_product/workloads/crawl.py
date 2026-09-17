"""Single-depth page-fetch finite workload.

The planner turns ``input.urls`` into one unit per normalized URL; the
handler GETs the page and maps HTTP outcomes onto the runtime error
contract (retryable 5xx/network, rate-limited 429, permanent 4xx).

Planners must be deterministic, so link-following is breadth-first
across runs: each result carries ``links`` and the product submits the
next run with :func:`next_frontier`.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import timedelta
from html.parser import HTMLParser
from typing import cast
from urllib.parse import urldefrag, urljoin, urlparse

import httpx

from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.errors import (
    PermanentExecutionError,
    RateLimitedExecutionError,
    RetryableExecutionError,
)
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    ExecutionUnit,
    WorkloadRequest,
)

USER_AGENT = "crawler-example/1.0 (+distributed-runtime)"
MAX_LINKS_PER_PAGE = 100
_PERMANENT_STATUSES = frozenset({400, 401, 403, 404, 410})


class CrawlPlanner:
    """Emit one fetch unit per normalized, deduplicated input URL."""

    name = "crawl.fetch"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    async def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        urls = cast(list[str], (request.payload or {})["urls"])
        for url in normalize_urls(urls):
            yield ExecutionUnit(
                unit_key=unit_key_for(url),
                handler="crawl.fetch.page",
                payload={"url": url},
                execution_class="lightweight",
                timeout_seconds=60,
                max_attempts=4,
            )


async def fetch_page(
    context: ExecutionContext, payload: Mapping[str, JsonValue]
) -> ExecutionResult:
    """Fetch one URL and return small JSON metadata plus discovered links."""
    context.cancellation.raise_if_cancelled()
    url = str(payload["url"])
    try:
        async with _client_factory() as client:
            response = await client.get(url)
    except httpx.HTTPError as exc:
        raise RetryableExecutionError(f"fetch failed: {url}") from exc

    status = response.status_code
    if status == 429:
        raise RateLimitedExecutionError(
            f"rate limited: {url}",
            retry_after=timedelta(seconds=_retry_after_seconds(response)),
        )
    if status >= 500:
        raise RetryableExecutionError(f"server error {status}: {url}")
    if status in _PERMANENT_STATUSES:
        raise PermanentExecutionError(f"client error {status}: {url}")

    result: dict[str, JsonValue] = {
        "url": url,
        "status": status,
        "bytes": len(response.content),
        "title": None,
        "links": [],
    }
    if status == 200 and "text/html" in response.headers.get("content-type", ""):
        title, links = parse_page(response.text, url)
        result["title"] = title
        result["links"] = cast(list[JsonValue], links)
    return result


def normalize_urls(urls: Sequence[str]) -> list[str]:
    """Strip fragments, require http(s), and dedup while preserving order."""
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in urls:
        url = urldefrag(raw.strip()).url
        if urlparse(url).scheme not in ("http", "https") or url in seen:
            continue
        seen.add(url)
        normalized.append(url)
    return normalized


def unit_key_for(url: str) -> str:
    """Derive the contract-safe unit key (and default idempotency key)."""
    return f"crawl:{hashlib.sha256(url.encode()).hexdigest()[:16]}"


def parse_page(html: str, base_url: str) -> tuple[str | None, list[str]]:
    """Extract the title and deduplicated absolute http(s) links."""
    parser = _PageParser()
    parser.feed(html)
    title = " ".join(part.strip() for part in parser.title_parts if part.strip()) or None
    seen: set[str] = set()
    links: list[str] = []
    for href in parser.hrefs:
        absolute = urldefrag(urljoin(base_url, href)).url
        if urlparse(absolute).scheme not in ("http", "https") or absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
        if len(links) >= MAX_LINKS_PER_PAGE:
            break
    return title, links


def next_frontier(results: Sequence[ExecutionResult]) -> list[str]:
    """Collect deduplicated discovered links for the next breadth-first run."""
    frontier: list[str] = []
    seen: set[str] = set()
    for result in results:
        if not isinstance(result, Mapping):
            continue
        links = result.get("links")
        if not isinstance(links, list):
            continue
        for link in links:
            if isinstance(link, str) and link not in seen:
                seen.add(link)
                frontier.append(link)
    return frontier


def _retry_after_seconds(response: httpx.Response) -> float:
    raw = response.headers.get("retry-after")
    if raw is None:
        return 10.0
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 10.0


def _httpx_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers={"user-agent": USER_AGENT},
    )


_client_factory = _httpx_client  # tests substitute a MockTransport client


class _PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._in_title = False
        self.title_parts: list[str] = []
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "title":
            self._in_title = True
        elif tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.hrefs.append(href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
