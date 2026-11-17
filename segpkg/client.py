"""HTTP client used by the consumer: resumable, digest-verified fetches."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

from .util import SegpkgError, sha256_bytes


class ReleaseClientError(SegpkgError):
    pass


class ReleaseClient:
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str) -> tuple[bytes, dict[str, str]]:
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read(), dict(resp.headers)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            try:
                message = json.loads(body).get("error", body)
            except json.JSONDecodeError:
                message = body
            raise ReleaseClientError(f"{exc.code} from {path}: {message}") from exc
        except urllib.error.URLError as exc:
            raise ReleaseClientError(f"cannot reach {url}: {exc.reason}") from exc

    def _get_json(self, path: str) -> Any:
        raw, headers = self._get(path)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ReleaseClientError(f"invalid JSON from {path}: {exc}") from exc

    def health(self) -> dict[str, Any]:
        return self._get_json("/api/health")

    def datasets(self) -> list[dict[str, Any]]:
        return self._get_json("/api/datasets")["datasets"]

    def dataset(self, dataset: str) -> dict[str, Any]:
        return self._get_json(f"/api/datasets/{quote(dataset)}")

    def manifest_bytes(self, dataset: str, version: str) -> bytes:
        raw, headers = self._get(
            f"/api/datasets/{quote(dataset)}/versions/{quote(version)}/manifest"
        )
        digest = headers.get("X-Manifest-Digest") or headers.get("x-manifest-digest")
        if digest and sha256_bytes(raw) != digest:
            raise ReleaseClientError("manifest bytes do not match server digest header")
        return raw

    def diff_bytes(self, dataset: str, version: str, base: str) -> bytes:
        raw, headers = self._get(
            f"/api/datasets/{quote(dataset)}/versions/{quote(version)}/diffs/{quote(base)}"
        )
        digest = headers.get("X-Diff-Digest") or headers.get("x-diff-digest")
        if digest and sha256_bytes(raw) != digest:
            raise ReleaseClientError("diff bytes do not match server digest header")
        return raw

    def blob(
        self,
        digest: str,
        progress: "ProgressSink | None" = None,
        max_bytes: int | None = None,
    ) -> bytes:
        """Fetch one blob, verifying the digest before returning it."""
        raw, _ = self._get(f"/blobs/{quote(digest)}")
        actual = sha256_bytes(raw)
        if actual != digest:
            raise ReleaseClientError(
                f"blob {digest[:12]} failed verification (got {actual[:12]})"
            )
        if max_bytes is not None:
            raw = raw[:max_bytes]
        if progress is not None:
            progress.emit(
                {"phase": "blob", "digest": digest, "bytes": len(raw)}
            )
        return raw


class ProgressSink:
    """Collects transfer events for the consuming app/CLI."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event: dict[str, Any]) -> None:
        self.events.append(event)
