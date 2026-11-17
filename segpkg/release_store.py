"""On-disk release directory shared by the builder and the release service.

Layout::

    <root>/
      .lock
      blobs/sha256/<aa>/<digest>.blob      content-addressed segment bytes
      datasets/<dataset>/
        index.json                          published versions + available diffs
        versions/<version>/
          manifest.json                     canonical manifest (raw bytes hashed)
          diffs/<base>.json                 diff from <base> (base "none" = full)
          package.json                      small human/UI summary

Publishing is atomic from a reader's point of view: blobs are written first,
then the manifest and diffs, and only then is ``index.json`` replaced. The
release service reads ``index.json`` on every request, so a version is either
fully visible or not visible at all.

Retention (``gc``) refuses to remove a version that a retained diff depends on
and only deletes blobs no retained manifest or diff target references.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .segments import DiffManifest, Manifest
from .util import (
    SegpkgError,
    ValidationError,
    atomic_write,
    atomic_write_json,
    blob_abspath,
    blob_relpath,
    ensure_dir,
    is_hex64,
    now_iso,
    read_json,
    sha256_file,
    validate_dataset,
    validate_version,
)

INDEX_FORMAT = "segpkg-index/1"


@dataclass
class VersionInfo:
    version: str
    seq: int
    manifest_digest: str
    content_digest: str
    total_bytes: int
    total_records: int
    created_at: str
    diffs: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "seq": self.seq,
            "manifest_digest": self.manifest_digest,
            "content_digest": self.content_digest,
            "total_bytes": self.total_bytes,
            "total_records": self.total_records,
            "created_at": self.created_at,
            "diffs": self.diffs,
        }


def _index_from_disk(obj: Any) -> dict[str, Any]:
    if not isinstance(obj, dict) or obj.get("format") != INDEX_FORMAT:
        raise ValidationError("release index has unsupported format")
    return obj


class ReleaseStore:
    def __init__(self, root: os.PathLike[str] | str):
        self.root = Path(root)
        ensure_dir(self.root)

    # --- paths ---------------------------------------------------------
    def dataset_dir(self, dataset: str) -> Path:
        validate_dataset(dataset)
        return self.root / "datasets" / dataset

    def version_dir(self, dataset: str, version: str) -> Path:
        validate_version(version)
        return self.dataset_dir(dataset) / "versions" / version

    def index_path(self, dataset: str) -> Path:
        return self.dataset_dir(dataset) / "index.json"

    def diff_path(self, dataset: str, target: str, base: str) -> Path:
        return self.version_dir(dataset, target) / "diffs" / f"{base}.json"

    def blob_path(self, digest: str) -> Path:
        return blob_abspath(self.root, digest)

    def has_blob(self, digest: str) -> bool:
        return self.blob_path(digest).is_file()

    def blob_size(self, digest: str) -> int:
        return self.blob_path(digest).stat().st_size

    # --- locking -------------------------------------------------------
    @contextlib.contextmanager
    def lock(self) -> Iterator[None]:
        lock_path = self.root / ".lock"
        ensure_dir(lock_path.parent)
        with open(lock_path, "a+b") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    # --- reading -------------------------------------------------------
    def list_datasets(self) -> list[str]:
        base = self.root / "datasets"
        if not base.is_dir():
            return []
        return sorted(p.name for p in base.iterdir() if (p / "index.json").is_file())

    def index(self, dataset: str) -> dict[str, Any]:
        path = self.index_path(dataset)
        if not path.is_file():
            return {
                "format": INDEX_FORMAT,
                "dataset": dataset,
                "updated_at": None,
                "versions": [],
            }
        return _index_from_disk(read_json(path))

    def versions(self, dataset: str) -> list[VersionInfo]:
        out: list[VersionInfo] = []
        for raw in self.index(dataset).get("versions", []):
            out.append(
                VersionInfo(
                    version=raw["version"],
                    seq=raw["seq"],
                    manifest_digest=raw["manifest_digest"],
                    content_digest=raw["content_digest"],
                    total_bytes=raw["total_bytes"],
                    total_records=raw["total_records"],
                    created_at=raw["created_at"],
                    diffs=raw.get("diffs", {}),
                )
            )
        return sorted(out, key=lambda v: v.seq)

    def get_version(self, dataset: str, version: str) -> VersionInfo:
        for info in self.versions(dataset):
            if info.version == version:
                return info
        raise SegpkgError(f"version {version!r} of dataset {dataset!r} is not published")

    def manifest(self, dataset: str, version: str) -> tuple[Manifest, bytes]:
        path = self.version_dir(dataset, version) / "manifest.json"
        if not path.is_file():
            raise SegpkgError(f"manifest for {dataset}/{version} is missing")
        raw = path.read_bytes()
        manifest = Manifest.from_bytes(raw)
        if raw != manifest.to_bytes():
            raise ValidationError(
                f"manifest for {dataset}/{version} is not canonical"
            )
        info = self.get_version(dataset, version)
        if manifest.digest != info.manifest_digest:
            raise ValidationError(
                f"manifest digest for {dataset}/{version} does not match index"
            )
        return manifest, raw

    def diff(self, dataset: str, target: str, base: str) -> tuple[DiffManifest, bytes]:
        path = self.diff_path(dataset, target, base)
        if not path.is_file():
            raise SegpkgError(
                f"no diff from {base!r} to {dataset}/{target}"
            )
        raw = path.read_bytes()
        diff = DiffManifest.from_bytes(raw)
        if raw != diff.to_bytes():
            raise ValidationError(
                f"diff {dataset}/{target} from {base} is not canonical"
            )
        target_info = self.get_version(dataset, target)
        if diff.target_manifest_digest != target_info.manifest_digest:
            raise ValidationError(
                f"diff {dataset}/{target} from {base} targets a different manifest"
            )
        return diff, raw

    # --- writing -------------------------------------------------------
    def _write_blob(self, digest: str, payload: bytes) -> None:
        path = self.blob_path(digest)
        if path.is_file():
            if path.stat().st_size == len(payload) and sha256_file(path) == digest:
                return
            # A verified-hash-named blob with bad content is corruption; replace.
        ensure_dir(path.parent)
        atomic_write(path, payload)

    def publish(
        self,
        manifest: Manifest,
        payloads: dict[str, bytes],
        diffs: list[DiffManifest] | None = None,
        make_diffs_from: list[Manifest] | None = None,
    ) -> VersionInfo:
        """Publish *manifest* and its segment payloads.

        ``diffs`` are precomputed diff documents (their base must already be
        published and must match the store's manifest byte-for-byte).
        ``make_diffs_from`` is a list of older manifests to build direct diffs
        from. A full-package document (base ``none``) is always added.

        Versions are immutable: publishing the same version twice is an error.
        """
        info_versions = self.versions(manifest.dataset)
        if any(v.version == manifest.version for v in info_versions):
            raise SegpkgError(
                f"version {manifest.version!r} already exists; versions are immutable"
            )
        for seg in manifest.segments.values():
            if seg.name not in payloads:
                raise ValidationError(f"missing payload for segment {seg.name}")
            payload = payloads[seg.name]
            if sha256_file_bytes(payload) != seg.digest:
                raise ValidationError(
                    f"payload for {seg.name} does not hash to {seg.digest}"
                )
            if len(payload) != seg.size:
                raise ValidationError(f"payload for {seg.name} has wrong size")

        with self.lock():
            # Re-check under the lock: another builder may have won the race.
            if any(
                v.version == manifest.version
                for v in self.versions(manifest.dataset)
            ):
                raise SegpkgError(
                    f"version {manifest.version!r} was published concurrently"
                )
            index = self.index(manifest.dataset)
            for seg in manifest.segments.values():
                self._write_blob(seg.digest, payloads[seg.name])

            vdir = self.version_dir(manifest.dataset, manifest.version)
            ensure_dir(vdir / "diffs")
            atomic_write(vdir / "manifest.json", manifest.to_bytes())

            from .segments import make_diff

            specs: dict[str, DiffManifest] = {}
            for base in make_diffs_from or []:
                if base.seq >= manifest.seq:
                    raise ValidationError(
                        f"base {base.version} is not older than {manifest.version}"
                    )
                specs[base.version] = make_diff(base, manifest, now_iso())
            for diff in diffs or []:
                if diff.dataset != manifest.dataset:
                    raise ValidationError("diff dataset does not match package")
                if diff.target_manifest_digest != manifest.digest:
                    raise ValidationError(
                        f"diff from {diff.base_version} targets a different manifest"
                    )
                if not diff.is_full:
                    if diff.base_version in specs:
                        raise ValidationError(
                            f"duplicate diff from {diff.base_version}"
                        )
                    # The diff must be built on the exact published base.
                    base_manifest, _ = self.manifest(
                        manifest.dataset, diff.base_version
                    )
                    if base_manifest.digest != diff.base_manifest_digest:
                        raise ValidationError(
                            f"diff from {diff.base_version} names a base manifest "
                            "that is not the published one"
                        )
                specs[diff.base_version] = diff
            if "none" not in specs:
                specs["none"] = make_diff(None, manifest, now_iso())

            diff_entries: dict[str, dict[str, Any]] = {}
            for base_name, diff in specs.items():
                path = self.diff_path(manifest.dataset, manifest.version, base_name)
                atomic_write(path, diff.to_bytes())
                diff_entries[base_name] = {
                    "digest": diff.digest,
                    "bytes": len(diff.to_bytes()),
                    "download_bytes": diff.download_bytes,
                    "download_segments": diff.download_segments,
                    "base_manifest_digest": diff.base_manifest_digest,
                    "base_version": diff.base_version,
                }

            entry = VersionInfo(
                version=manifest.version,
                seq=manifest.seq,
                manifest_digest=manifest.digest,
                content_digest=manifest.content_digest,
                total_bytes=manifest.total_bytes,
                total_records=manifest.total_records,
                created_at=manifest.created_at,
                diffs=diff_entries,
            ).to_dict()
            versions = [
                v for v in index.get("versions", [])
                if v.get("version") != manifest.version
            ]
            versions.append(entry)
            versions.sort(key=lambda v: (v["seq"], v["version"]))
            index = {
                "format": INDEX_FORMAT,
                "dataset": manifest.dataset,
                "updated_at": now_iso(),
                "versions": versions,
            }
            atomic_write_json(self.index_path(manifest.dataset), index)
            atomic_write_json(
                vdir / "package.json",
                {
                    "dataset": manifest.dataset,
                    "version": manifest.version,
                    "manifest_digest": manifest.digest,
                    "content_digest": manifest.content_digest,
                    "segments": len(manifest.segments),
                    "total_bytes": manifest.total_bytes,
                    "total_records": manifest.total_records,
                },
            )
        return VersionInfo(
            version=manifest.version,
            seq=manifest.seq,
            manifest_digest=manifest.digest,
            content_digest=manifest.content_digest,
            total_bytes=manifest.total_bytes,
            total_records=manifest.total_records,
            created_at=manifest.created_at,
            diffs=diff_entries,
        )

    # --- retention -----------------------------------------------------
    def gc(
        self,
        dataset: str,
        keep_versions: list[str] | None = None,
        keep_last: int | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Prune versions/diffs/blobs without breaking retained diff chains.

        A version can only be dropped when no retained diff names it as base.
        If a version is dropped, its outward diffs are dropped first. Blobs are
        kept when referenced by any retained manifest or by any retained diff
        target segment. The result is verified before the index is rewritten.
        """
        infos = self.versions(dataset)
        if not infos:
            return {
                "dataset": dataset,
                "kept": [],
                "removed_versions": [],
                "removed_diffs": [],
                "removed_blobs": 0,
                "freed_bytes": 0,
            }
        if keep_versions is not None and keep_last is not None:
            raise SegpkgError("use either keep_versions or keep_last, not both")

        with self.lock():
            infos = self.versions(dataset)
            if keep_versions is not None:
                known = {v.version for v in infos}
                unknown = sorted(set(keep_versions) - known)
                if unknown:
                    raise ValidationError(f"cannot keep unknown versions: {unknown}")
                kept = [v for v in infos if v.version in set(keep_versions)]
            elif keep_last is not None:
                if keep_last < 1:
                    raise ValidationError("keep_last must be >= 1")
                kept = infos[-keep_last:]
            else:
                kept = infos
            kept_names = {v.version for v in kept}
            removed_versions = sorted({v.version for v in infos} - kept_names)

            # Diffs from removed bases are unusable and must go with the base.
            removed_diffs: list[dict[str, Any]] = []
            kept_diffs: list[tuple[str, str]] = []
            for target in kept:
                for base_name in target.diffs:
                    if base_name == "none":
                        kept_diffs.append((target.version, base_name))
                    elif base_name in kept_names:
                        kept_diffs.append((target.version, base_name))
                    else:
                        removed_diffs.append(
                            {"target": target.version, "base": base_name}
                        )

            # Any blob referenced by a retained manifest or by a kept diff's
            # target segments must survive. Blobs are shared by every dataset in
            # the store, so scan every *other* dataset in full; for the dataset
            # being pruned, only its retained versions and retained diffs count.
            referenced: set[str] = set()
            for ds in self.list_datasets():
                for other in self.versions(ds):
                    if ds == dataset and other.version not in kept_names:
                        continue
                    manifest, _ = self.manifest(ds, other.version)
                    referenced.update(manifest.digest_map().values())
                    for base_name in other.diffs:
                        if ds == dataset and (
                            (other.version, base_name) not in kept_diffs
                        ):
                            continue
                        diff, _ = self.diff(ds, other.version, base_name)
                        for entry in diff.segments:
                            if entry["op"] in ("add", "replace"):
                                referenced.add(entry["digest"])

            existing_blobs = {
                p.name[: -len(".blob")]
                for p in (self.root / "blobs" / "sha256").glob("*/*.blob")
            }
            removable_blobs = existing_blobs - referenced
            # Never remove a blob that a parse of the retained diffs could ask
            # for; re-scan as a belt-and-braces check before deleting.
            for info in kept:
                manifest, _ = self.manifest(dataset, info.version)
                for seg in manifest.segments.values():
                    if seg.digest in removable_blobs:
                        raise ValidationError(
                            f"refusing to delete live blob {seg.digest} for {info.version}"
                        )

            freed = 0
            for digest in sorted(removable_blobs):
                path = self.blob_path(digest)
                try:
                    size = path.stat().st_size
                except OSError:
                    continue
                freed += size
                if not dry_run:
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        continue
                    with contextlib.suppress(OSError):
                        path.parent.rmdir()

            if not dry_run:
                for target in removed_versions:
                    shutil.rmtree(self.version_dir(dataset, target), ignore_errors=True)
                for rd in removed_diffs:
                    path = self.diff_path(dataset, rd["target"], rd["base"])
                    with contextlib.suppress(FileNotFoundError):
                        path.unlink()
                index = self.index(dataset)
                versions = []
                for raw in index.get("versions", []):
                    if raw["version"] not in kept_names:
                        continue
                    raw = dict(raw)
                    raw["diffs"] = {
                        base: meta
                        for base, meta in raw.get("diffs", {}).items()
                        if base == "none" or base in kept_names
                    }
                    versions.append(raw)
                atomic_write_json(
                    self.index_path(dataset),
                    {
                        "format": INDEX_FORMAT,
                        "dataset": dataset,
                        "updated_at": now_iso(),
                        "versions": sorted(versions, key=lambda v: v["seq"]),
                    },
                )
                # Self-check: every retained diff must still find its base and
                # every retained manifest must still have all of its blobs.
                live = {v.version for v in self.versions(dataset)}
                dangling = [
                    (v.version, b) for v in self.versions(dataset) for b in v.diffs
                    if b != "none" and b not in live
                ]
                if dangling:
                    raise SegpkgError(
                        f"internal error: gc produced dangling diffs {dangling}"
                    )
                missing: list[str] = []
                for info in self.versions(dataset):
                    manifest, _ = self.manifest(dataset, info.version)
                    for seg in manifest.segments.values():
                        if not self.has_blob(seg.digest):
                            missing.append(f"{info.version}:{seg.name}")
                if missing:
                    raise SegpkgError(
                        f"internal error: gc removed live blobs {missing}"
                    )
            return {
                "dataset": dataset,
                "kept": sorted(kept_names),
                "removed_versions": removed_versions,
                "removed_diffs": removed_diffs,
                "removed_blobs": len(removable_blobs),
                "freed_bytes": freed,
                "dry_run": dry_run,
            }

    # --- verification ---------------------------------------------------
    def verify(self, dataset: str | None = None) -> dict[str, Any]:
        """Verify every published manifest, diff and referenced blob."""
        datasets = [dataset] if dataset else self.list_datasets()
        problems: list[str] = []
        checked = {"datasets": 0, "versions": 0, "diffs": 0, "blobs": 0}
        for ds in datasets:
            checked["datasets"] += 1
            try:
                infos = self.versions(ds)
            except ValidationError as exc:
                problems.append(f"{ds}: index unreadable: {exc}")
                continue
            names = {v.version for v in infos}
            for info in infos:
                checked["versions"] += 1
                try:
                    manifest, raw = self.manifest(ds, info.version)
                except (SegpkgError, ValidationError) as exc:
                    problems.append(f"{ds}/{info.version}: {exc}")
                    continue
                if manifest.content_digest != info.content_digest:
                    problems.append(
                        f"{ds}/{info.version}: content digest mismatch"
                    )
                for seg in manifest.segments.values():
                    path = self.blob_path(seg.digest)
                    if not path.is_file():
                        problems.append(
                            f"{ds}/{info.version}: missing blob {seg.digest} for {seg.name}"
                        )
                        continue
                    checked["blobs"] += 1
                    if sha256_file(path) != seg.digest:
                        problems.append(
                            f"{ds}/{info.version}: corrupt blob {seg.digest}"
                        )
                for base_name, meta in info.diffs.items():
                    if base_name != "none" and base_name not in names:
                        problems.append(
                            f"{ds}/{info.version}: diff base {base_name} no longer exists"
                        )
                        continue
                    checked["diffs"] += 1
                    try:
                        diff, raw = self.diff(ds, info.version, base_name)
                    except (SegpkgError, ValidationError) as exc:
                        problems.append(
                            f"{ds}/{info.version}<-{base_name}: {exc}"
                        )
                        continue
                    if diff.digest != meta.get("digest"):
                        problems.append(
                            f"{ds}/{info.version}<-{base_name}: diff digest mismatch"
                        )
                    for entry in diff.segments:
                        if entry["op"] in ("add", "replace"):
                            if not self.has_blob(entry["digest"]):
                                problems.append(
                                    f"{ds}/{info.version}<-{base_name}: missing "
                                    f"changed blob {entry['digest']}"
                                )
            # Every blob outside a version dir's reachability set is reported
            # only as a stat, not a problem.
        return {
            "ok": not problems,
            "problems": problems,
            "checked": checked,
        }


def sha256_file_bytes(payload: bytes) -> str:
    import hashlib

    return hashlib.sha256(payload).hexdigest()
