"""Cloud Storage adapter for immutable artifact content.

Writes bytes and reads them back through generation-pinned URIs so a
reference can never silently drift to different content. Transport is the
same AuthorizedSession REST pattern as ``deploy.config.fetch_secret`` —
no google-cloud-storage dependency. Sessions are injectable so tests never
touch the network.
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

from distributed_runtime.core.artifacts import ArtifactReference


class ArtifactStoreError(RuntimeError):
    """A Cloud Storage call returned a non-success status."""


class _Session(Protocol):
    """Minimal requests-style surface the adapter needs."""

    def get(self, url: str, **kwargs: Any) -> Any: ...
    def post(self, url: str, **kwargs: Any) -> Any: ...
    def delete(self, url: str, **kwargs: Any) -> Any: ...


def _authorized_session() -> _Session:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_write"]
    )
    return AuthorizedSession(credentials)  # type: ignore[no-untyped-call,return-value]


def _check(response: Any, action: str, uri: str, *, allowed: tuple[int, ...] = (200,)) -> Any:
    if response.status_code not in allowed:
        raise ArtifactStoreError(f"{action} failed ({response.status_code}): {uri}")
    return response


@dataclass(frozen=True, slots=True)
class GcsArtifactStore:
    """Upload and fetch generation-pinned artifact content."""

    bucket: str
    application: str
    _session: _Session

    @classmethod
    def connect(cls, *, bucket: str, application: str) -> GcsArtifactStore:
        """Build a store on ambient GCP credentials."""
        return cls(bucket=bucket, application=application, _session=_authorized_session())

    async def put(self, object_name: str, content: bytes) -> ArtifactReference:
        """Upload content; the returned reference pins the real generation."""

        def _upload() -> ArtifactReference:
            response = self._session.post(
                f"https://storage.googleapis.com/upload/storage/v1/b/{self.bucket}/o",
                params={"uploadType": "media", "name": object_name},
                headers={"Content-Type": "application/octet-stream"},
                data=content,
            )
            _check(response, "artifact upload", f"gs://{self.bucket}/{object_name}")
            body = response.json()
            return ArtifactReference(
                application=self.application,
                bucket=self.bucket,
                object_name=str(body["name"]),
                generation=int(body["generation"]),
                uri=ArtifactReference.build_uri(
                    self.bucket, str(body["name"]), int(body["generation"])
                ),
                sha256=hashlib.sha256(content).hexdigest(),
                size_bytes=len(content),
            )

        return await asyncio.to_thread(_upload)

    async def get(self, reference: ArtifactReference) -> bytes:
        """Download the exact generation and verify it against the reference."""
        self._check_owner(reference)

        def _download() -> bytes:
            encoded = quote(reference.object_name, safe="")
            response = self._session.get(
                f"https://storage.googleapis.com/storage/v1/b/{reference.bucket}/o/{encoded}",
                params={"alt": "media", "generation": str(reference.generation)},
            )
            _check(response, "artifact download", reference.uri)
            content = bytes(response.content)
            reference.verify(content)
            return content

        return await asyncio.to_thread(_download)

    async def delete(self, reference: ArtifactReference) -> None:
        """Delete the pinned generation; missing objects are already gone."""
        self._check_owner(reference)

        def _remove() -> None:
            encoded = quote(reference.object_name, safe="")
            response = self._session.delete(
                f"https://storage.googleapis.com/storage/v1/b/{reference.bucket}/o/{encoded}",
                params={"generation": str(reference.generation)},
            )
            _check(response, "artifact delete", reference.uri, allowed=(200, 204, 404))

        await asyncio.to_thread(_remove)

    def _check_owner(self, reference: ArtifactReference) -> None:
        if reference.application != self.application:
            raise ArtifactStoreError(
                f"artifact belongs to {reference.application}, not {self.application}"
            )
