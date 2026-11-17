"""Web console for the segmented delivery system.

A single process that owns a demo workspace and exposes:

* a read-only release API (same routes as the standalone release service),
* a small console API for publishing versions, simulating consumer mistakes,
  planning/upgrading, querying, verifying and running retention,
* static UI served from ``web/``.

The console is a *view* over real directories: every button calls the same
builder/release/consumer code the CLI uses. Nothing is faked in memory.
"""

from __future__ import annotations

import json
import mimetypes
import shutil
import threading
import traceback
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import builder
from .client import ProgressSink, ReleaseClient
from .consumer import Consumer
from .fixtures import DatasetSpec, DatasetVariant
from .release_server import EventBus, ReleaseHandler
from .release_store import ReleaseStore
from .segments import make_diff
from .upgrade import Plan, Strategy, apply_plan, plan_upgrade, upgrade
from .util import (
    SegpkgError,
    ValidationError,
    atomic_write,
    ensure_dir,
    human_bytes,
    now_iso,
    read_json,
    sha256_bytes,
    validate_dataset,
    validate_version,
    version_seq,
)

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


class Workspace:
    """A demo workspace: a release root, source data, and named consumers."""

    def __init__(self, root: Path, dataset: str = "catalog"):
        self.root = root
        self.dataset = dataset
        self.release = root / "release"
        self.sources = root / "sources"
        self.consumers_dir = root / "consumers"
        self.state_path = root / "workspace.json"
        self.lock = threading.RLock()
        ensure_dir(self.release)
        ensure_dir(self.sources)
        ensure_dir(self.consumers_dir)
        self.store = ReleaseStore(self.release)
        self.bus = EventBus()

    # --- state ---------------------------------------------------------
    def state(self) -> dict[str, Any]:
        if self.state_path.is_file():
            return read_json(self.state_path)
        return {
            "dataset": self.dataset,
            "consumers": ["c1", "c2", "c3"],
            "scenario": [],
            "next_version": 1,
        }

    def save_state(self, state: dict[str, Any]) -> None:
        atomic_write(self.state_path, json.dumps(state, indent=2, sort_keys=True).encode())

    def consumer(self, name: str) -> Consumer:
        if not name or "/" in name or ".." in name:
            raise ValidationError("invalid consumer name")
        return Consumer(self.consumers_dir / name)

    # --- release views --------------------------------------------------
    def release_view(self) -> dict[str, Any]:
        datasets = []
        for ds in self.store.list_datasets():
            versions = self.store.versions(ds)
            out = []
            for v in versions:
                manifest, _ = self.store.manifest(ds, v.version)
                out.append(
                    {
                        "version": v.version,
                        "seq": v.seq,
                        "manifest_digest": v.manifest_digest,
                        "content_digest": v.content_digest,
                        "total_bytes": v.total_bytes,
                        "total_human": human_bytes(v.total_bytes),
                        "total_records": v.total_records,
                        "segments": len(manifest.segments),
                        "created_at": v.created_at,
                        "diffs": v.diffs,
                    }
                )
            datasets.append({"dataset": ds, "versions": out})
        return {"datasets": datasets}

    def consumers_view(self) -> list[dict[str, Any]]:
        out = []
        for name in self.state().get("consumers", []):
            consumer = self.consumer(name)
            installed = []
            for v in consumer.installed():
                try:
                    check = consumer.verify(v.dataset, v.version)
                except SegpkgError as exc:
                    check = {"ok": False, "error": str(exc)}
                installed.append(
                    {
                        "dataset": v.dataset,
                        "version": v.version,
                        "manifest_digest": v.manifest_digest,
                        "content_digest": v.content_digest,
                        "source": v.source,
                        "installed_at": v.installed_at,
                        "bytes": v.total_bytes,
                        "healthy": check.get("ok", False),
                        "missing": check.get("missing", []),
                        "corrupt": check.get("corrupt", []),
                    }
                )
            out.append({"name": name, "installed": installed})
        return out

    # --- actions --------------------------------------------------------
    def publish(
        self,
        count: int = 1,
        edits: int = 1,
        skip_version: bool = False,
        diff_mode: str = "all",
    ) -> dict[str, Any]:
        with self.lock:
            state = self.state()
            nxt = int(state.get("next_version", 1))
            existing = self.store.versions(self.dataset)
            if existing:
                nxt = max(nxt, max(v.seq for v in existing) + 1)
            published = []
            if diff_mode not in ("all", "previous"):
                raise ValidationError("diff_mode must be 'all' or 'previous'")
            for index in range(count):
                version = f"v{nxt}"
                spec = DatasetSpec(self.dataset, version)
                variant: DatasetVariant
                if existing:
                    latest = max(existing, key=lambda v: v.seq)
                    src_latest = self.sources / "raw" / f"{self.dataset}-{latest.version}.jsonl"
                    if src_latest.is_file():
                        variant = DatasetVariant.__new__(DatasetVariant)
                        variant.spec = spec
                        variant.records = _read_jsonl(src_latest)
                    else:
                        variant = DatasetVariant(spec)
                else:
                    variant = DatasetVariant(spec)
                variant.spec = spec
                for i in range(edits):
                    variant.touch((nxt * 7 + i * 13) % len(variant.records), f"publish-{nxt}-{i}")
                if nxt > 1:
                    variant.append(1, f"publish-{nxt}")
                raw_path = ensure_dir(self.sources / "raw") / f"{self.dataset}-{version}.jsonl"
                variant.write(raw_path)
                build_dir = self.sources / "builds" / version
                manifest = builder.build_package(
                    variant.records, self.dataset, version, build_dir,
                    created_at=now_iso(),
                )
                if diff_mode == "previous" and existing:
                    earlier = [
                        self.store.manifest(
                            self.dataset, max(existing, key=lambda v: v.seq).version
                        )[0]
                    ]
                else:
                    earlier = [
                        self.store.manifest(self.dataset, v.version)[0]
                        for v in existing
                    ]
                info = self.store.publish(
                    manifest,
                    builder.read_build(build_dir)[1],
                    make_diffs_from=earlier,
                )
                published.append(info.to_dict())
                existing = self.store.versions(self.dataset)
                nxt = manifest.seq + 1
                # The gap is created by the *next* version published: this call
                # publishes vN, the following one publishes vN+2.
                if skip_version and index == count - 1:
                    nxt += 1
            state["next_version"] = nxt
            state.setdefault("scenario", []).append(
                {"at": now_iso(), "event": "publish", "versions": [p["version"] for p in published]}
            )
            self.save_state(state)
            self.bus.publish({"type": "publish", "versions": published})
            return {"published": published}

    def install(self, consumer_name: str, version: str) -> dict[str, Any]:
        consumer = self.consumer(consumer_name)
        manifest, _ = self.store.manifest(self.dataset, version)
        # Serve from the in-process store, not HTTP: same code path, but the
        # console does not need a listening socket.
        client = _LocalClient(self.store)
        strategy = Strategy(
            kind="full",
            download_segments=len(manifest.segments),
            download_bytes=manifest.total_bytes,
            reasons=["initial install"],
        )
        plan = Plan(self.dataset, version, manifest, [strategy], strategy,
                    manifest.content_digest)
        result = apply_plan(consumer, client, plan)
        self._log({"event": "install", "consumer": consumer_name, "version": version,
                   "bytes": result.get("downloaded_bytes")})
        return result

    def plan(self, consumer_name: str, version: str, prefer: str = "auto") -> dict[str, Any]:
        consumer = self.consumer(consumer_name)
        plan = plan_upgrade(consumer, _LocalClient(self.store), self.dataset, version,
                            prefer=prefer)
        return plan.to_dict()

    def upgrade(
        self,
        consumer_name: str,
        version: str,
        prefer: str = "auto",
        damage: str | None = None,
        max_blobs: int | None = None,
    ) -> dict[str, Any]:
        consumer = self.consumer(consumer_name)
        damaged = None
        if damage:
            damaged = _damage_local(
                consumer, self.store, self.dataset, damage, target_version=version
            )
        result = upgrade(
            consumer,
            _LocalClient(self.store),
            self.dataset,
            version,
            prefer=prefer,
            max_blobs=max_blobs,
        )
        result["damage"] = damaged
        self._log(
            {
                "event": "upgrade",
                "consumer": consumer_name,
                "version": version,
                "strategy": result.get("strategy_used"),
                "bytes": result.get("downloaded_bytes"),
                "fell_back": result.get("fell_back"),
                "damage": damaged,
            }
        )
        return result

    def query(self, consumer_name: str, **filters: Any) -> dict[str, Any]:
        consumer = self.consumer(consumer_name)
        records = consumer.query(
            self.dataset,
            version=filters.get("version"),
            category=filters.get("category"),
            min_score=filters.get("min_score"),
            text=filters.get("text"),
            limit=filters.get("limit", 20),
        )
        return {"records": records, "count": len(records)}

    def verify(self, consumer_name: str, version: str) -> dict[str, Any]:
        return self.consumer(consumer_name).verify(self.dataset, version)

    def gc(self, keep_last: int, dry_run: bool = True) -> dict[str, Any]:
        with self.lock:
            result = self.store.gc(self.dataset, keep_last=keep_last, dry_run=dry_run)
            result["release_verify"] = self.store.verify(self.dataset)
            self._log({"event": "gc", "keep_last": keep_last, "dry_run": dry_run,
                       "removed": result.get("removed_versions")})
            return result

    def scenario_reset(self) -> dict[str, Any]:
        with self.lock:
            for path in (self.release, self.sources, self.consumers_dir):
                shutil.rmtree(path, ignore_errors=True)
            if self.state_path.is_file():
                self.state_path.unlink()
            ensure_dir(self.release)
            ensure_dir(self.sources)
            ensure_dir(self.consumers_dir)
            self.store = ReleaseStore(self.release)
            self.save_state(
                {"dataset": self.dataset, "consumers": ["c1", "c2", "c3"],
                 "scenario": [], "next_version": 1}
            )
            return {"reset": True}

    def _log(self, event: dict[str, Any]) -> None:
        state = self.state()
        state.setdefault("scenario", []).append({"at": now_iso(), **event})
        del state["scenario"][:-100]
        self.save_state(state)
        self.bus.publish(event)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    from .util import read_jsonl

    return read_jsonl(path)


def _damage_local(
    consumer: Consumer,
    store: ReleaseStore,
    dataset: str,
    mode: str,
    target_version: str | None = None,
) -> dict[str, Any]:
    """Simulate a consumer-side problem with the installed base.

    Modes:
      ``corrupt-needed``  flip a byte in a segment the next diff reuses
      ``corrupt-any``     flip a byte in the first installed segment
      ``delete-needed``   delete a blob the next diff reuses
      ``delete-any``      delete the first installed segment's blob
    """
    installed = [v for v in consumer.installed() if v.dataset == dataset]
    if not installed:
        raise SegpkgError("consumer has no installed version to damage")
    base_version = installed[-1].version
    manifest = consumer.require(dataset, base_version)
    names = manifest.segment_names()
    needed: list[str] = []
    if mode.endswith("needed") and target_version:
        if base_version in store.get_version(dataset, target_version).diffs:
            diff, _ = store.diff(dataset, target_version, base_version)
            needed = [e["name"] for e in diff.segments if e["op"] == "unchanged"]
    if needed:
        names = [n for n in names if n in set(needed)] or names
    name = names[0]
    digest = manifest.segments[name].digest
    path = consumer.blob_path(digest)
    if mode.startswith("delete"):
        if path.exists():
            path.unlink()
        action = "deleted"
    else:
        data = bytearray(path.read_bytes()) if path.exists() else bytearray(b"x")
        if data:
            data[len(data) // 2] ^= 0xFF
        path.write_bytes(bytes(data))
        action = "corrupted"
    return {
        "mode": mode,
        "consumer": consumer.root.name,
        "version": base_version,
        "segment": name,
        "digest": digest,
        "action": action,
    }


class _LocalClient(ReleaseClient):
    """ReleaseClient that reads a ReleaseStore directly (no HTTP socket)."""

    def __init__(self, store: ReleaseStore):
        self.store = store
        self.base_url = "local://release"

    def dataset(self, dataset: str) -> dict[str, Any]:
        versions = []
        for v in self.store.versions(dataset):
            manifest, _ = self.store.manifest(dataset, v.version)
            versions.append(
                {
                    "version": v.version,
                    "seq": v.seq,
                    "manifest_digest": v.manifest_digest,
                    "content_digest": v.content_digest,
                    "total_bytes": v.total_bytes,
                    "diffs": v.diffs,
                    "segments": len(manifest.segments),
                }
            )
        if not versions:
            raise SegpkgError(f"dataset {dataset!r} is not published")
        return {"dataset": dataset, "versions": versions}

    def manifest_bytes(self, dataset: str, version: str) -> bytes:
        return self.store.manifest(dataset, version)[1]

    def diff_bytes(self, dataset: str, version: str, base: str) -> bytes:
        return self.store.diff(dataset, version, base)[1]

    def blob(self, digest: str, progress: ProgressSink | None = None,
             max_bytes: int | None = None) -> bytes:
        path = self.store.blob_path(digest)
        if not path.is_file():
            raise SegpkgError(f"blob {digest} not found")
        raw = path.read_bytes()
        if sha256_bytes(raw) != digest:
            raise SegpkgError(f"release blob {digest} is corrupt")
        if progress is not None:
            progress.emit({"phase": "blob", "digest": digest, "bytes": len(raw)})
        return raw


class ConsoleHandler(ReleaseHandler):
    workspace: Workspace

    @property
    def store(self) -> ReleaseStore:  # type: ignore[override]
        return self.server.workspace.store  # type: ignore[attr-defined]

    @property
    def bus(self) -> EventBus:  # type: ignore[override]
        return self.server.workspace.bus  # type: ignore[attr-defined]

    @property
    def access_log(self) -> list[dict[str, Any]] | None:  # type: ignore[override]
        return None

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/console/"):
            self._console_get(parsed)
            return
        if parsed.path in ("/", "/index.html"):
            self._serve_static("index.html")
            return
        if parsed.path.startswith("/static/"):
            self._serve_static(parsed.path[len("/static/"):])
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/console/api/"):
            self._error(HTTPStatus.NOT_FOUND, "no such route")
            return
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError as exc:
            self._error(HTTPStatus.BAD_REQUEST, f"invalid JSON: {exc}")
            return
        try:
            result = self._console_action(parsed.path, body)
            self._json(HTTPStatus.OK, result)
        except SegpkgError as exc:
            self._json(HTTPStatus.CONFLICT, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - surfaced to the UI
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()},
            )

    def _console_get(self, parsed: Any) -> None:
        path = parsed.path
        query = parse_qs(parsed.query)
        try:
            if path == "/console/api/state":
                ws: Workspace = self.server.workspace  # type: ignore[attr-defined]
                state = ws.state()
                self._json(
                    HTTPStatus.OK,
                    {
                        "release": ws.release_view(),
                        "consumers": ws.consumers_view(),
                        "scenario": state.get("scenario", []),
                        "dataset": ws.dataset,
                    },
                )
                return
            if path == "/console/api/diff":
                ws = self.server.workspace  # type: ignore[attr-defined]
                target = query["target"][0]
                base = query["base"][0]
                diff, raw = ws.store.diff(ws.dataset, target, base)
                self._json(HTTPStatus.OK, json.loads(raw))
                return
            if path == "/console/api/manifest":
                ws = self.server.workspace  # type: ignore[attr-defined]
                version = query["version"][0]
                manifest, raw = ws.store.manifest(ws.dataset, version)
                self._json(HTTPStatus.OK, manifest.to_dict())
                return
            self._error(HTTPStatus.NOT_FOUND, "no such console route")
        except SegpkgError as exc:
            self._json(HTTPStatus.CONFLICT, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover
            self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"{type(exc).__name__}: {exc}"})

    def _console_action(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        ws: Workspace = self.server.workspace  # type: ignore[attr-defined]
        if path == "/console/api/reset":
            return ws.scenario_reset()
        if path == "/console/api/publish":
            return ws.publish(
                count=int(body.get("count", 1)),
                edits=int(body.get("edits", 1)),
                skip_version=bool(body.get("skip_version", False)),
                diff_mode=body.get("diff_mode", "all"),
            )
        if path == "/console/api/install":
            return ws.install(body.get("consumer", "c1"), body["version"])
        if path == "/console/api/plan":
            return ws.plan(
                body.get("consumer", "c1"),
                body["version"],
                prefer=body.get("prefer", "auto"),
            )
        if path == "/console/api/upgrade":
            return ws.upgrade(
                body.get("consumer", "c1"),
                body["version"],
                prefer=body.get("prefer", "auto"),
                damage=body.get("damage"),
                max_blobs=body.get("max_blobs"),
            )
        if path == "/console/api/query":
            return ws.query(body.get("consumer", "c1"), **body)
        if path == "/console/api/verify":
            return ws.verify(body.get("consumer", "c1"), body["version"])
        if path == "/console/api/gc":
            return ws.gc(int(body.get("keep_last", 2)), dry_run=bool(body.get("dry_run", True)))
        raise ValidationError(f"unknown console action {path}")

    def _serve_static(self, rel: str) -> None:
        rel = rel.split("?")[0].lstrip("/")
        if not rel or ".." in Path(rel).parts:
            self._error(HTTPStatus.NOT_FOUND, "bad path")
            return
        path = WEB_DIR / rel
        if not path.is_file():
            self._error(HTTPStatus.NOT_FOUND, f"no such file: rel")
            return
        ctype = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        raw = path.read_bytes()
        self._bytes(HTTPStatus.OK, raw, ctype, {"Cache-Control": "no-store"})


class ConsoleServer:
    def __init__(self, workspace: Workspace, host: str = "127.0.0.1", port: int = 0):
        self.workspace = workspace
        self.httpd = ThreadingHTTPServer((host, port), ConsoleHandler)
        self.httpd.daemon_threads = True
        self.httpd.workspace = workspace  # type: ignore[attr-defined]
        self.host, self.port = self.httpd.server_address[:2]

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def serve_forever(self) -> None:
        self.httpd.serve_forever()

    def start_background(self) -> "ConsoleServer":
        threading.Thread(target=self.serve_forever, daemon=True, name="console").start()
        return self

    def shutdown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
