"""UT-GCS: Cloud Storage artifact adapter against a fake HTTP session."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import pytest

from distributed_runtime.core.artifacts import ArtifactReference
from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.gcp.storage import ArtifactStoreError, GcsArtifactStore


class FakeResponse:
    def __init__(self, status_code: int, body: dict[str, Any] | None = None, content: bytes = b""):
        self.status_code = status_code
        self._body = body or {}
        self.content = content

    def json(self) -> dict[str, Any]:
        return self._body


class FakeSession:
    """Records calls and serves canned responses per method."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.post_body: dict[str, Any] = {}
        self.get_content = b""
        self.status = 200

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(("GET", url, kwargs))
        return FakeResponse(self.status, content=self.get_content)

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(("POST", url, kwargs))
        return FakeResponse(self.status, self.post_body)

    def delete(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(("DELETE", url, kwargs))
        return FakeResponse(self.status)


def _store(session: FakeSession) -> GcsArtifactStore:
    return GcsArtifactStore(bucket="runtime-artifacts", application="app", _session=session)


def test_put_pins_generation_and_digest() -> None:
    session = FakeSession()
    session.post_body = {"name": "requests/run-1.json", "generation": "42"}
    content = b'{"tenant":"acme"}'
    reference = asyncio.run(_store(session).put("requests/run-1.json", content))
    assert reference.generation == 42
    assert reference.sha256 == hashlib.sha256(content).hexdigest()
    assert reference.size_bytes == len(content)
    assert reference.uri.endswith("?generation=42")
    method, _url, kwargs = session.calls[0]
    assert method == "POST" and kwargs["params"]["name"] == "requests/run-1.json"


def test_get_downloads_pinned_generation_and_verifies() -> None:
    session = FakeSession()
    content = b"payload-bytes"
    session.get_content = content
    reference = ArtifactReference.from_bytes(
        application="app",
        bucket="runtime-artifacts",
        object_name="a/b.bin",
        generation=7,
        content=content,
    )
    assert asyncio.run(_store(session).get(reference)) == content
    _, _, kwargs = session.calls[0]
    assert kwargs["params"]["generation"] == "7"
    assert kwargs["params"]["alt"] == "media"


def test_get_rejects_corrupted_content() -> None:
    session = FakeSession()
    session.get_content = b"tampered"
    reference = ArtifactReference.from_bytes(
        application="app",
        bucket="runtime-artifacts",
        object_name="a.bin",
        generation=3,
        content=b"original",
    )
    with pytest.raises(InvariantViolationError):
        asyncio.run(_store(session).get(reference))


def test_cross_application_reference_rejected() -> None:
    session = FakeSession()
    reference = ArtifactReference.from_bytes(
        application="other-app",
        bucket="runtime-artifacts",
        object_name="a.bin",
        generation=1,
        content=b"x",
    )
    with pytest.raises(ArtifactStoreError):
        asyncio.run(_store(session).get(reference))
    assert not session.calls


def test_http_errors_raise() -> None:
    session = FakeSession()
    session.status = 403
    with pytest.raises(ArtifactStoreError):
        asyncio.run(_store(session).put("x.bin", b"data"))


def test_delete_tolerates_missing_object() -> None:
    session = FakeSession()
    session.status = 404
    reference = ArtifactReference.from_bytes(
        application="app",
        bucket="runtime-artifacts",
        object_name="gone.bin",
        generation=9,
        content=b"x",
    )
    asyncio.run(_store(session).delete(reference))
    assert session.calls[0][0] == "DELETE"
