"""Release service: a small read-only HTTP API over a release store.

Endpoints::

    GET /api/health
    GET /api/datasets
    GET /api/datasets/<dataset>
    GET /api/datasets/<dataset>/versions/<version>/manifest
    GET /api/datasets/<dataset>/versions/<version>/diffs/<base>
    GET /blobs/<digest>
    GET /api/events                          (SSE: publish/download notifications)

All responses are immutable and addressable by digest, so the service can be
cached freely. The service never mutates the release root.
"""

from __future__ import annotations

import json
import os
import queue
import re
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .release_store import ReleaseStore
from .util import SegpkgError, ValidationError, is_hex64

_API_VERSION = "segpkg-release/1"
_DIGEST_RE = re.compile(r"^/blobs/([0-9a-f]{64})$")
_DATASET_RE = re.compile(r"^/api/datasets/([A-Za-z0-9][A-Za-z0-9._-]{0,127})$")
_MANIFEST_RE = re.compile(
    r"^/api/datasets/([A-Za-z0-9][A-Za-z0-9._-]{0,127})/versions/"
    r"([A-Za-z0-9][A-Za-z0-9._-]{0,127})/manifest$"
)
_DIFF_RE = re.compile(
    r"^/api/datasets/([A-Za-z0-9][A-Za-z0-9._-]{0,127})/versions/"
    r"([A-Za-z0-9][A-Za-z0-9._-]{0,127})/diffs/([A-Za-z0-9][A-Za-z0-9._-]{0,127})$"
)


class EventBus:
    """Tiny fan-out for server-sent events (used by the web console)."""

    def __init__(self) -> None:
        self._subscribers: list[queue.Queue[dict[str, Any]]] = []
        self._lock = threading.Lock()

    def publish(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._subscribers = [q for q in self._subscribers if not q.full()]
            for q in self._subscribers:
                q.put(event)

    def subscribe(self) -> queue.Queue[dict[str, Any]]:
        q: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=200)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue[dict[str, Any]]) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)


class ReleaseHandler(BaseHTTPRequestHandler):
    server_version = "segpkg-release/0.1"
    protocol_version = "HTTP/1.1"

    # set by the server
    store: ReleaseStore
    bus: EventBus
    access_log: list[dict[str, Any]] | None = None

    def log_message(self, fmt: str, *args: Any) -> None:  # quieter default
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    # --- helpers -------------------------------------------------------
    @property
    def store(self) -> ReleaseStore:
        return self.server.store  # type: ignore[attr-defined]

    @property
    def bus(self) -> EventBus:
        return self.server.bus  # type: ignore[attr-defined]

    @property
    def access_log(self) -> list[dict[str, Any]] | None:
        return self.server.access_log  # type: ignore[attr-defined]

    def _json(self, status: int, obj: Any, extra_headers: dict[str, str] | None = None) -> None:
        body = json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _bytes(
        self,
        status: int,
        body: bytes,
        content_type: str,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, message: str) -> None:
        self._json(status, {"error": message})

    def _record(self, status: int, path: str) -> None:
        if self.access_log is not None:
            self.access_log.append(
                {"at": time.time(), "path": path, "status": status, "ip": self.client_address[0]}
            )
            del self.access_log[:-500]

    # --- routes --------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if path == "/api/health":
                now = time.time()
                self._json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "api": _API_VERSION,
                        "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
                        "datasets": self.store.list_datasets(),
                    },
                )
                return
            if path == "/api/datasets":
                self._json(
                    HTTPStatus.OK,
                    {"datasets": [self._dataset_summary(d) for d in self.store.list_datasets()]},
                )
                return
            if path == "/api/events":
                self._sse()
                return
            m = _DATASET_RE.match(path)
            if m:
                dataset = m.group(1)
                self._json(HTTPStatus.OK, self._dataset_detail(dataset))
                return
            m = _MANIFEST_RE.match(path)
            if m:
                dataset, version = m.group(1), m.group(2)
                manifest, raw = self.store.manifest(dataset, version)
                self._bytes(
                    HTTPStatus.OK,
                    raw,
                    "application/json",
                    {
                        "ETag": f'"sha256:{manifest.digest}"',
                        "X-Manifest-Digest": manifest.digest,
                        "X-Content-Digest": manifest.content_digest,
                        "Cache-Control": "public, max-age=31536000, immutable",
                    },
                )
                return
            m = _DIFF_RE.match(path)
            if m:
                dataset, version, base = m.group(1), m.group(2), m.group(3)
                diff, raw = self.store.diff(dataset, version, base)
                self._bytes(
                    HTTPStatus.OK,
                    raw,
                    "application/json",
                    {
                        "ETag": f'"sha256:{diff.digest}"',
                        "X-Diff-Digest": diff.digest,
                        "Cache-Control": "public, max-age=31536000, immutable",
                    },
                )
                return
            m = _DIGEST_RE.match(path)
            if m:
                self._blob(m.group(1))
                return
            self._error(HTTPStatus.NOT_FOUND, f"no such route: {path}")
            self._record(404, path)
        except ValidationError as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
            self._record(400, path)
        except SegpkgError as exc:
            self._error(HTTPStatus.NOT_FOUND, str(exc))
            self._record(404, path)
        except BrokenPipeError:
            pass
        except ConnectionResetError:
            pass
        except Exception as exc:  # pragma: no cover - defensive
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, f"{type(exc).__name__}: {exc}")
            self._record(500, path)

    def do_HEAD(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        m = _DIGEST_RE.match(parsed.path)
        if m:
            digest = m.group(1)
            path = self.store.blob_path(digest)
            if not path.is_file():
                self._error(HTTPStatus.NOT_FOUND, f"blob {digest} not found")
                return
            size = path.stat().st_size
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(size))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("X-Blob-Digest", digest)
            self.end_headers()
            return
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _blob(self, digest: str) -> None:
        if not is_hex64(digest):
            self._error(HTTPStatus.BAD_REQUEST, "invalid digest")
            return
        path = self.store.blob_path(digest)
        if not path.is_file():
            self._error(HTTPStatus.NOT_FOUND, f"blob {digest} not found")
            self._record(404, self.path)
            return
        data = path.read_bytes()
        import hashlib

        if hashlib.sha256(data).hexdigest() != digest:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "release blob is corrupt")
            self._record(500, self.path)
            return
        self._bytes(
            HTTPStatus.OK,
            data,
            "application/octet-stream",
            {
                "ETag": f'"sha256:{digest}"',
                "X-Blob-Digest": digest,
                "Cache-Control": "public, max-age=31536000, immutable",
            },
        )
        if self.access_log is not None:
            self.access_log.append(
                {
                    "at": time.time(),
                    "path": self.path,
                    "status": 200,
                    "bytes": len(data),
                    "digest": digest,
                    "ip": self.client_address[0],
                }
            )
            del self.access_log[:-500]

    def _sse(self) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        q = self.bus.subscribe()
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    event = q.get(timeout=15)
                    payload = json.dumps(event, sort_keys=True)
                    self.wfile.write(f"event: update\ndata: {payload}\n\n".encode())
                    self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.bus.unsubscribe(q)

    # --- projections ---------------------------------------------------
    def _dataset_summary(self, dataset: str) -> dict[str, Any]:
        versions = self.store.versions(dataset)
        latest = versions[-1] if versions else None
        return {
            "dataset": dataset,
            "versions": len(versions),
            "latest": latest.version if latest else None,
            "latest_seq": latest.seq if latest else None,
            "latest_bytes": latest.total_bytes if latest else 0,
        }

    def _dataset_detail(self, dataset: str) -> dict[str, Any]:
        versions = self.store.versions(dataset)
        if not versions:
            raise SegpkgError(f"dataset {dataset!r} is not published")
        out_versions = []
        for v in versions:
            manifest, _ = self.store.manifest(dataset, v.version)
            out_versions.append(
                {
                    "version": v.version,
                    "seq": v.seq,
                    "manifest_digest": v.manifest_digest,
                    "content_digest": v.content_digest,
                    "total_bytes": v.total_bytes,
                    "total_records": v.total_records,
                    "created_at": v.created_at,
                    "segments": len(manifest.segments),
                    "diffs": v.diffs,
                }
            )
        return {"dataset": dataset, "versions": out_versions}


class ReleaseServer:
    def __init__(self, store: ReleaseStore, host: str = "127.0.0.1", port: int = 0,
                 verbose: bool = False):
        self.store = store
        self.bus = EventBus()
        self.httpd = ThreadingHTTPServer((host, port), ReleaseHandler)
        self.httpd.daemon_threads = True
        self.httpd.verbose = verbose  # type: ignore[attr-defined]
        self.httpd.store = store  # type: ignore[attr-defined]
        self.httpd.bus = self.bus  # type: ignore[attr-defined]
        self.httpd.access_log = []  # type: ignore[attr-defined]
        self.host, self.port = self.httpd.server_address[:2]

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def serve_forever(self) -> None:
        self.httpd.serve_forever()

    def shutdown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    def start_background(self) -> "ReleaseServer":
        t = threading.Thread(target=self.serve_forever, daemon=True, name="release-http")
        t.start()
        self._thread = t
        return self


def create_server(release_root: str, host: str = "127.0.0.1", port: int = 0,
                  verbose: bool = False) -> ReleaseServer:
    return ReleaseServer(ReleaseStore(release_root), host=host, port=port, verbose=verbose)
