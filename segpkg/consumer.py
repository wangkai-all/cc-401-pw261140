"""Consumer-side local state.

Layout of a consumer root::

    <root>/
      versions/<version>/manifest.json     installed versions (complete only)
      blobs/sha256/<aa>/<digest>.blob      verified segment bytes
      staging/<op-id>/                     in-flight, resumable downloads
      state.json                           last installed / selections

Downloads never write into ``blobs/`` directly. Blobs are moved in only after
their SHA-256 matches the manifest, so an interrupted download can be resumed
from staging but can never leave a half-written blob that looks valid.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .segments import Manifest
from .util import (
    SegpkgError,
    ValidationError,
    atomic_write,
    atomic_write_json,
    blob_abspath,
    blob_relpath,
    ensure_dir,
    fsync_dir,
    now_iso,
    read_json,
    sha256_file,
    validate_dataset,
    validate_version,
)

STATE_FORMAT = "segpkg-state/1"


class MissingBaseError(SegpkgError):
    """Raised when a diff is requested but the exact base is not installed."""


class CorruptLocalError(SegpkgError):
    """Raised when local bytes fail verification."""


@dataclass
class InstalledVersion:
    dataset: str
    version: str
    manifest_digest: str
    content_digest: str
    installed_at: str
    source: str  # "full" | "diff:<base>"
    total_bytes: int


class Consumer:
    def __init__(self, root: os.PathLike[str] | str):
        self.root = Path(root)
        ensure_dir(self.root)

    # --- paths ---------------------------------------------------------
    def version_dir(self, dataset: str, version: str) -> Path:
        validate_dataset(dataset)
        validate_version(version)
        return self.root / "versions" / dataset / version

    def manifest_path(self, dataset: str, version: str) -> Path:
        return self.version_dir(dataset, version) / "manifest.json"

    def blob_path(self, digest: str) -> Path:
        return blob_abspath(self.root, digest)

    def staging_dir(self, op_id: str) -> Path:
        if not op_id or "/" in op_id or "\\" in op_id or ".." in op_id:
            raise ValidationError(f"invalid staging id: {op_id!r}")
        return self.root / "staging" / op_id

    def state_path(self) -> Path:
        return self.root / "state.json"

    # --- querying ------------------------------------------------------
    def has_blob(self, digest: str) -> bool:
        return self.blob_path(digest).is_file()

    def blob_ok(self, digest: str) -> bool:
        path = self.blob_path(digest)
        if not path.is_file():
            return False
        try:
            return sha256_file(path) == digest
        except OSError:
            return False

    def installed(self) -> list[InstalledVersion]:
        base = self.root / "versions"
        out: list[InstalledVersion] = []
        if not base.is_dir():
            return out
        for ds_dir in sorted(base.iterdir()):
            if not ds_dir.is_dir():
                continue
            for v_dir in sorted(ds_dir.iterdir()):
                mpath = v_dir / "manifest.json"
                if not mpath.is_file():
                    continue
                manifest = Manifest.from_bytes(mpath.read_bytes())
                meta_path = v_dir / "installed.json"
                meta = read_json(meta_path) if meta_path.is_file() else {}
                out.append(
                    InstalledVersion(
                        dataset=manifest.dataset,
                        version=manifest.version,
                        manifest_digest=manifest.digest,
                        content_digest=manifest.content_digest,
                        installed_at=meta.get("installed_at", ""),
                        source=meta.get("source", "unknown"),
                        total_bytes=manifest.total_bytes,
                    )
                )
        return out

    def get(self, dataset: str, version: str) -> Manifest | None:
        path = self.manifest_path(dataset, version)
        if not path.is_file():
            return None
        return Manifest.from_bytes(path.read_bytes())

    def require(self, dataset: str, version: str) -> Manifest:
        manifest = self.get(dataset, version)
        if manifest is None:
            raise MissingBaseError(f"{dataset}/{version} is not installed")
        return manifest

    def latest(self, dataset: str) -> Manifest | None:
        versions = [v for v in self.installed() if v.dataset == dataset]
        if not versions:
            return None
        best = max(versions, key=lambda v: (self.require(dataset, v.version).seq, v.version))
        return self.require(dataset, best.version)

    # --- installation --------------------------------------------------
    def stage_payload(self, op_id: str, digest: str, payload: bytes) -> Path:
        """Verify and persist a downloaded segment in the staging area.

        Re-downloading an already staged byte-identical segment is a no-op, so
        resumption is safe.
        """
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValidationError(f"downloaded bytes for {digest} do not match digest")
        path = self.staging_dir(op_id) / blob_relpath(digest)
        atomic_write(path, payload)
        return path

    def staged_ok(self, op_id: str, digest: str) -> bool:
        path = self.staging_dir(op_id) / blob_relpath(digest)
        if not path.is_file():
            return False
        try:
            return sha256_file(path) == digest
        except OSError:
            return False

    def promote_staged(self, op_id: str, digest: str) -> None:
        """Move a verified staged blob into the durable blob store."""
        src = self.staging_dir(op_id) / blob_relpath(digest)
        if not src.is_file() or sha256_file(src) != digest:
            raise CorruptLocalError(f"staged blob {digest} is missing or corrupt")
        dst = self.blob_path(digest)
        # If the durable copy exists but is corrupt, it must not shadow the
        # verified staged copy: replace it.
        if dst.is_file() and sha256_file(dst) != digest:
            dst.unlink()
        ensure_dir(dst.parent)
        os.replace(src, dst)
        fsync_dir(dst.parent)

    def install_manifest(
        self,
        manifest: Manifest,
        op_id: str,
        source: str,
    ) -> None:
        """Record an installed version after all its blobs are present.

        This is the commit point. It verifies every segment blob, writes the
        manifest atomically, and only then marks the version visible. A version
        is invisible to queries until this returns.
        """
        for seg in manifest.segments.values():
            if not self.has_blob(seg.digest) or not self.blob_ok(seg.digest):
                raise CorruptLocalError(
                    f"cannot install {manifest.dataset}/{manifest.version}: "
                    f"blob {seg.name}/{seg.digest[:12]} is missing or corrupt"
                )
        vdir = self.version_dir(manifest.dataset, manifest.version)
        ensure_dir(vdir)
        # installed.json is written first; if we crash before manifest.json the
        # version is invisible, which is the safe direction.
        atomic_write_json(
            vdir / "installed.json",
            {
                "dataset": manifest.dataset,
                "version": manifest.version,
                "installed_at": now_iso(),
                "source": source,
                "manifest_digest": manifest.digest,
                "content_digest": manifest.content_digest,
            },
        )
        atomic_write(vdir / "manifest.json", manifest.to_bytes())
        self._update_state(
            dataset=manifest.dataset,
            version=manifest.version,
            manifest_digest=manifest.digest,
        )

    def _update_state(self, dataset: str, version: str, manifest_digest: str) -> None:
        state = self.state()
        state["last_installed"] = {
            "dataset": dataset,
            "version": version,
            "manifest_digest": manifest_digest,
            "at": now_iso(),
        }
        atomic_write_json(self.state_path(), state)

    def state(self) -> dict[str, Any]:
        path = self.state_path()
        if not path.is_file():
            return {"format": STATE_FORMAT, "last_installed": None, "events": []}
        raw = read_json(path)
        if not isinstance(raw, dict) or raw.get("format") != STATE_FORMAT:
            raise ValidationError("consumer state has unsupported format")
        return raw

    def log_event(self, event: dict[str, Any]) -> None:
        state = self.state()
        events = state.setdefault("events", [])
        events.append({"at": now_iso(), **event})
        del events[:-200]
        atomic_write_json(self.state_path(), state)

    def cleanup_staging(self, op_id: str) -> None:
        shutil.rmtree(self.staging_dir(op_id), ignore_errors=True)

    # --- querying installed content ------------------------------------
    def query(
        self,
        dataset: str,
        version: str | None = None,
        category: str | None = None,
        min_score: float | None = None,
        text: str | None = None,
        limit: int | None = None,
        all_versions: bool = False,
    ) -> list[dict[str, Any]]:
        """Read records through the installed manifest, never from raw files."""
        if all_versions:
            manifests = [
                self.require(dataset, v.version)
                for v in self.installed()
                if v.dataset == dataset
            ]
            manifests.sort(key=lambda m: (m.seq, m.version))
        elif version is not None:
            manifests = [self.require(dataset, version)]
        else:
            latest = self.latest(dataset)
            if latest is None:
                raise SegpkgError(f"dataset {dataset!r} is not installed")
            manifests = [latest]
        results: list[dict[str, Any]] = []
        for manifest in manifests:
            for name in manifest.segment_names():
                seg = manifest.segments[name]
                path = self.blob_path(seg.digest)
                if not path.is_file():
                    raise CorruptLocalError(
                        f"installed version {manifest.version} is missing segment {name}"
                    )
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != seg.digest:
                    raise CorruptLocalError(
                        f"installed segment {manifest.version}/{name} is corrupt"
                    )
                for rec in _parse(raw):
                    if all_versions:
                        rec = {**rec, "_version": manifest.version}
                    if category is not None and rec.get("category") != category:
                        continue
                    if min_score is not None:
                        try:
                            score = float(rec.get("score", 0))
                        except (TypeError, ValueError):
                            continue
                        if score < min_score:
                            continue
                    if text:
                        haystack = f"{rec.get('title','')} {rec.get('body','')}".lower()
                        if text.lower() not in haystack:
                            continue
                    results.append(rec)
                    if not all_versions and limit is not None and len(results) >= limit:
                        return results
        if limit is not None:
            return results[:limit]
        return results

    # --- integrity -----------------------------------------------------
    def verify(self, dataset: str, version: str) -> dict[str, Any]:
        manifest = self.require(dataset, version)
        bad: list[str] = []
        missing: list[str] = []
        for name in manifest.segment_names():
            seg = manifest.segments[name]
            path = self.blob_path(seg.digest)
            if not path.is_file():
                missing.append(name)
            elif sha256_file(path) != seg.digest:
                bad.append(name)
        return {
            "ok": not bad and not missing,
            "dataset": dataset,
            "version": version,
            "manifest_digest": manifest.digest,
            "content_digest": manifest.content_digest,
            "missing": missing,
            "corrupt": bad,
            "segments": len(manifest.segments),
        }

    def remove_version(self, dataset: str, version: str) -> None:
        shutil.rmtree(self.version_dir(dataset, version), ignore_errors=True)


def _parse(raw: bytes) -> Iterable[dict[str, Any]]:
    from .util import parse_jsonl_bytes

    return parse_jsonl_bytes(raw)
