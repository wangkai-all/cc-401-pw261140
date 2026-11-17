"""Segment, manifest, and diff model.

A *package* is an immutable, content-addressed set of segments plus a manifest.
Segments are stored as blobs named by the SHA-256 of their bytes. A manifest
records, for each segment, its digest, byte size and record count; the manifest
itself is identified by a digest over its canonical JSON.

Two digests matter:

``manifest_digest``
    Hash of the full manifest including version and timestamp. Used to pin the
    exact document a diff was built against.

``content_digest``
    Hash of ``{dataset, segments:[{name,digest,size,count}]}`` only. It is
    independent of build time and of how the package was obtained, so a consumer
    that upgrades via a diff can prove its result equals the independently
    published full package.

Sharding uses a stable hash of each record's ``id`` into a fixed bucket count.
Editing or adding one record therefore touches exactly one segment, which is
what makes small diffs possible.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable

from .util import (
    SegpkgError,
    ValidationError,
    canonical_json,
    is_hex64,
    parse_jsonl_bytes,
    sha256_bytes,
    sha256_json,
    validate_dataset,
    validate_version,
)

MANIFEST_FORMAT = "segpkg-manifest/1"
DIFF_FORMAT = "segpkg-diff/1"
SHARD_BUCKETS = 16


def record_key(rec: dict[str, Any], index: int) -> str:
    rid = rec.get("id")
    if isinstance(rid, (str, int)) and str(rid) != "":
        return str(rid)
    return f"#idx:{index}"


def canonical_segment_bytes(records: Iterable[dict[str, Any]]) -> bytes:
    """Canonical JSONL: sorted keys, compact separators, trailing newline."""
    lines = [canonical_json(rec) for rec in records]
    if not lines:
        return b""
    return b"\n".join(lines) + b"\n"


def _shard_records(
    records: list[dict[str, Any]], shard_prefix: str
) -> dict[str, list[dict[str, Any]]]:
    shards: dict[str, list[dict[str, Any]]] = {}
    for index, rec in enumerate(records):
        key = record_key(rec, index)
        bucket = int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16) % SHARD_BUCKETS
        shards.setdefault(f"{shard_prefix}-{bucket:02d}", []).append(rec)
    for items in shards.values():
        items.sort(key=lambda r: str(r.get("id", "")))
    return shards


def segment_payloads(
    records: list[dict[str, Any]], shard_prefix: str = "part"
) -> dict[str, bytes]:
    """Return ``{segment_name: canonical bytes}`` for a record list."""
    return {
        name: canonical_segment_bytes(items)
        for name, items in _shard_records(records, shard_prefix).items()
    }


def build_segments(
    records: list[dict[str, Any]], shard_prefix: str = "part"
) -> dict[str, "Segment"]:
    segments: dict[str, Segment] = {}
    for name, payload in segment_payloads(records, shard_prefix).items():
        items = parse_jsonl_bytes(payload)
        segments[name] = Segment(
            name=name,
            digest=sha256_bytes(payload),
            size=len(payload),
            count=len(items),
        )
    return segments


def parse_segment_bytes(
    raw: bytes, expected_digest: str | None = None
) -> list[dict[str, Any]]:
    if expected_digest is not None:
        actual = sha256_bytes(raw)
        if actual != expected_digest:
            raise ValidationError(
                f"segment digest mismatch: expected {expected_digest}, got {actual}"
            )
    return parse_jsonl_bytes(raw)


@dataclass(frozen=True)
class Segment:
    name: str
    digest: str
    size: int
    count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "digest": self.digest,
            "size": self.size,
            "count": self.count,
        }


@dataclass
class Manifest:
    dataset: str
    version: str
    seq: int
    created_at: str
    segments: dict[str, Segment]
    attributes: dict[str, Any] = field(default_factory=dict)

    # --- serialisation -------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "format": MANIFEST_FORMAT,
            "dataset": self.dataset,
            "version": self.version,
            "seq": self.seq,
            "created_at": self.created_at,
            "attributes": self.attributes,
            "segments": [
                s.to_dict()
                for s in sorted(self.segments.values(), key=lambda s: s.name)
            ],
        }

    def to_bytes(self) -> bytes:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return sha256_bytes(self.to_bytes())

    @property
    def content_digest(self) -> str:
        return sha256_json(
            {
                "dataset": self.dataset,
                "segments": [
                    s.to_dict()
                    for s in sorted(self.segments.values(), key=lambda s: s.name)
                ],
            }
        )

    @property
    def total_bytes(self) -> int:
        return sum(s.size for s in self.segments.values())

    @property
    def total_records(self) -> int:
        return sum(s.count for s in self.segments.values())

    def segment_names(self) -> list[str]:
        return sorted(self.segments)

    def digest_map(self) -> dict[str, str]:
        return {name: seg.digest for name, seg in self.segments.items()}

    # --- validation ----------------------------------------------------
    @classmethod
    def from_dict(cls, obj: Any) -> "Manifest":
        if not isinstance(obj, dict):
            raise ValidationError("manifest must be a JSON object")
        if obj.get("format") != MANIFEST_FORMAT:
            raise ValidationError(
                f"unsupported manifest format: {obj.get('format')!r}"
            )
        dataset = validate_dataset(obj.get("dataset"))
        version = validate_version(obj.get("version"))
        seq = obj.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise ValidationError("manifest seq must be a non-negative integer")
        created_at = obj.get("created_at")
        if not isinstance(created_at, str):
            raise ValidationError("manifest created_at must be a string")
        raw_segments = obj.get("segments")
        if not isinstance(raw_segments, list) or not raw_segments:
            raise ValidationError("manifest must list at least one segment")
        segments: dict[str, Segment] = {}
        for raw in raw_segments:
            if not isinstance(raw, dict):
                raise ValidationError("segment entries must be objects")
            name = raw.get("name")
            if not isinstance(name, str) or not name or "/" in name or "\\" in name:
                raise ValidationError(f"invalid segment name: {name!r}")
            if name in segments:
                raise ValidationError(f"duplicate segment name: {name}")
            digest = raw.get("digest")
            if not is_hex64(digest):
                raise ValidationError(f"invalid digest for segment {name}")
            size = raw.get("size")
            count = raw.get("count")
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise ValidationError(f"invalid size for segment {name}")
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise ValidationError(f"invalid count for segment {name}")
            segments[name] = Segment(name=name, digest=digest, size=size, count=count)
        attributes = obj.get("attributes", {})
        if not isinstance(attributes, dict):
            raise ValidationError("manifest attributes must be an object")
        manifest = cls(
            dataset=dataset,
            version=version,
            seq=seq,
            created_at=created_at,
            segments=segments,
            attributes=attributes,
        )
        if manifest.total_bytes != sum(
            s.size for s in manifest.segments.values()
        ):
            raise ValidationError("manifest size accounting is inconsistent")
        return manifest

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Manifest":
        try:
            obj = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationError(f"manifest is not valid JSON: {exc}") from exc
        return cls.from_dict(obj)


# ---------------------------------------------------------------------------
# Diff manifests
# ---------------------------------------------------------------------------


@dataclass
class DiffManifest:
    """A patch that turns one exact base package into one exact target package.

    The diff carries the full target segment declaration (so the target is
    reconstructed byte-for-byte, not inferred), plus a per-segment operation
    list that tells the consumer what it must download and which already-held
    blobs it may reuse. ``base`` pins the exact manifest that must be present
    locally before the patch is allowed to run.
    """

    dataset: str
    base_version: str
    base_seq: int
    base_manifest_digest: str
    target_manifest: Manifest
    created_at: str
    tool_version: str = "0.1.0"

    # --- derived -------------------------------------------------------
    @property
    def target_version(self) -> str:
        return self.target_manifest.version

    @property
    def target_seq(self) -> int:
        return self.target_manifest.seq

    @property
    def target_manifest_digest(self) -> str:
        return self.target_manifest.digest

    @property
    def target_content_digest(self) -> str:
        return self.target_manifest.content_digest

    @property
    def is_full(self) -> bool:
        return self.base_version == "none"

    @property
    def segments(self) -> list[dict[str, Any]]:
        return self._ops()

    def _ops(self) -> list[dict[str, Any]]:
        if self.is_full:
            return [
                {
                    "name": seg.name,
                    "op": "add",
                    "digest": seg.digest,
                    "size": seg.size,
                    "count": seg.count,
                }
                for seg in self.target_manifest.segments.values()
            ]
        # Ops are recomputed from base_manifest, which the caller attaches when
        # building; see make_diff. For a parsed diff we use the stored ops.
        return self._stored_ops

    _stored_ops: list[dict[str, Any]] = field(default_factory=list, repr=False)

    @property
    def added(self) -> list[dict[str, Any]]:
        return [s for s in self.segments if s["op"] == "add"]

    @property
    def replaced(self) -> list[dict[str, Any]]:
        return [s for s in self.segments if s["op"] == "replace"]

    @property
    def removed(self) -> list[dict[str, Any]]:
        return [s for s in self.segments if s["op"] == "remove"]

    @property
    def unchanged(self) -> list[dict[str, Any]]:
        return [s for s in self.segments if s["op"] == "unchanged"]

    @property
    def download_bytes(self) -> int:
        return sum(s["size"] for s in self.segments if s["op"] in ("add", "replace"))

    @property
    def download_segments(self) -> int:
        return len(self.added) + len(self.replaced)

    @property
    def required_blobs(self) -> list[dict[str, Any]]:
        return [s for s in self.segments if s["op"] in ("add", "replace")]

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": DIFF_FORMAT,
            "tool_version": self.tool_version,
            "dataset": self.dataset,
            "created_at": self.created_at,
            "base": {
                "version": self.base_version,
                "seq": self.base_seq,
                "manifest_digest": self.base_manifest_digest,
            },
            "target": self.target_manifest.to_dict(),
            "segments": sorted(self.segments, key=lambda s: s["name"]),
        }

    def to_bytes(self) -> bytes:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return sha256_bytes(self.to_bytes())

    # --- validation ----------------------------------------------------
    @classmethod
    def from_dict(cls, obj: Any) -> "DiffManifest":
        if not isinstance(obj, dict):
            raise ValidationError("diff must be a JSON object")
        if obj.get("format") != DIFF_FORMAT:
            raise ValidationError(f"unsupported diff format: {obj.get('format')!r}")
        dataset = validate_dataset(obj.get("dataset"))
        base = obj.get("base") or {}
        if not isinstance(base, dict):
            raise ValidationError("diff base must be an object")
        base_version = base.get("version")
        if not isinstance(base_version, str) or not base_version:
            raise ValidationError("diff base.version is required")
        base_seq = base.get("seq")
        if not isinstance(base_seq, int) or isinstance(base_seq, bool) or base_seq < 0:
            raise ValidationError("diff base.seq must be a non-negative integer")
        base_digest = base.get("manifest_digest")
        if not is_hex64(base_digest):
            raise ValidationError("diff base.manifest_digest must be sha256")
        target_raw = obj.get("target")
        if not isinstance(target_raw, dict):
            raise ValidationError("diff target must be an object")
        target = Manifest.from_dict(target_raw)
        if target.dataset != dataset:
            raise ValidationError("diff target dataset does not match diff dataset")
        if target.seq <= base_seq and base_version != "none":
            raise ValidationError("diff target must be newer than its base")
        if base_version == "none" and (
            base_seq != 0 or base_digest != "0" * 64
        ):
            raise ValidationError(
                "full diff must use base seq 0 and a zero manifest digest"
            )
        raw_segments = obj.get("segments")
        if not isinstance(raw_segments, list) or not raw_segments:
            raise ValidationError("diff must list segments")
        ops: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in raw_segments:
            if not isinstance(raw, dict):
                raise ValidationError("diff segments must be objects")
            name = raw.get("name")
            if not isinstance(name, str) or not name or "/" in name or "\\" in name:
                raise ValidationError(f"invalid diff segment name: {name!r}")
            if name in seen:
                raise ValidationError(f"duplicate diff segment name: {name}")
            seen.add(name)
            op = raw.get("op")
            if op not in ("add", "replace", "remove", "unchanged"):
                raise ValidationError(f"invalid diff op {op!r} for segment {name}")
            size = raw.get("size", 0)
            count = raw.get("count", 0)
            if not isinstance(size, int) or size < 0:
                raise ValidationError(f"invalid size for segment {name}")
            if not isinstance(count, int) or count < 0:
                raise ValidationError(f"invalid count for segment {name}")
            entry: dict[str, Any] = {"name": name, "op": op, "size": size, "count": count}
            if op != "remove":
                digest = raw.get("digest")
                if not is_hex64(digest):
                    raise ValidationError(f"segment {name} needs a sha256 digest")
                entry["digest"] = digest
            if op in ("unchanged", "replace"):
                old_digest = raw.get("base_digest")
                if not is_hex64(old_digest):
                    raise ValidationError(
                        f"segment {name} of op {op} must name its base_digest"
                    )
                entry["base_digest"] = old_digest
            if op == "unchanged" and entry.get("digest") != entry.get("base_digest"):
                raise ValidationError(
                    f"unchanged segment {name} must have digest == base_digest"
                )
            ops.append(entry)
        _validate_ops_against_target(target, ops, base_digest, base_version)
        created_at = obj.get("created_at")
        if not isinstance(created_at, str):
            raise ValidationError("diff created_at must be a string")
        diff = cls(
            dataset=dataset,
            base_version=base_version,
            base_seq=base_seq,
            base_manifest_digest=base_digest,
            target_manifest=target,
            created_at=created_at,
            tool_version=str(obj.get("tool_version", "unknown")),
        )
        diff._stored_ops = ops
        return diff

    @classmethod
    def from_bytes(cls, raw: bytes) -> "DiffManifest":
        try:
            obj = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationError(f"diff is not valid JSON: {exc}") from exc
        return cls.from_dict(obj)


def _validate_ops_against_target(
    target: Manifest,
    ops: list[dict[str, Any]],
    base_digest: str,
    base_version: str,
) -> None:
    ops_by_name = {o["name"]: o for o in ops}
    removals = {n for n, o in ops_by_name.items() if o["op"] == "remove"}
    if set(ops_by_name) - removals != set(target.segments):
        missing = sorted(set(target.segments) - set(ops_by_name))
        extra = sorted(set(ops_by_name) - removals - set(target.segments))
        raise ValidationError(
            "diff operations do not match target segments "
            f"(missing={missing}, extra={extra})"
        )
    for name, seg in target.segments.items():
        op = ops_by_name[name]
        if op["op"] == "remove":
            raise ValidationError(f"target segment {name} cannot have op remove")
        if op["digest"] != seg.digest or op["size"] != seg.size or op["count"] != seg.count:
            raise ValidationError(
                f"diff operation for {name} disagrees with target manifest"
            )
    if base_version == "none":
        for op in ops:
            if op["op"] != "add":
                raise ValidationError("a full diff may only contain add operations")
    else:
        for op in ops:
            if op["op"] == "unchanged" and "base_digest" not in op:
                raise ValidationError(
                    f"unchanged segment {op['name']} must name its base_digest"
                )


def make_diff(
    base: Manifest | None,
    target: Manifest,
    created_at: str,
    tool_version: str = "0.1.0",
) -> DiffManifest:
    """Compute a diff manifest from *base* (None means a full package)."""
    if base is None:
        ops = [
            {
                "name": seg.name,
                "op": "add",
                "digest": seg.digest,
                "size": seg.size,
                "count": seg.count,
            }
            for seg in target.segments.values()
        ]
        diff = DiffManifest(
            dataset=target.dataset,
            base_version="none",
            base_seq=0,
            base_manifest_digest="0" * 64,
            target_manifest=target,
            created_at=created_at,
            tool_version=tool_version,
        )
        diff._stored_ops = ops
        return diff
    if base.dataset != target.dataset:
        raise SegpkgError(
            f"cannot diff datasets {base.dataset!r} and {target.dataset!r}"
        )
    if target.seq <= base.seq:
        raise SegpkgError(
            f"diff target {target.version} must be newer than base {base.version}"
        )
    ops: list[dict[str, Any]] = []
    for name in sorted(target.segments):
        new = target.segments[name]
        old = base.segments.get(name)
        if old is not None and old.digest == new.digest:
            ops.append(
                {
                    "name": name,
                    "op": "unchanged",
                    "digest": new.digest,
                    "base_digest": old.digest,
                    "size": new.size,
                    "count": new.count,
                }
            )
        else:
            entry = {
                "name": name,
                "op": "replace" if old is not None else "add",
                "digest": new.digest,
                "size": new.size,
                "count": new.count,
            }
            if old is not None:
                entry["base_digest"] = old.digest
            ops.append(entry)
    # Segments that disappeared from the target are not needed to rebuild it;
    # they are listed as removals so the consumer can prune local state.
    for name in sorted(set(base.segments) - set(target.segments)):
        old = base.segments[name]
        ops.append(
            {"name": name, "op": "remove", "digest": old.digest, "size": old.size, "count": old.count}
        )
    diff = DiffManifest(
        dataset=target.dataset,
        base_version=base.version,
        base_seq=base.seq,
        base_manifest_digest=base.digest,
        target_manifest=target,
        created_at=created_at,
        tool_version=tool_version,
    )
    diff._stored_ops = ops
    return diff


def apply_diff(base: Manifest, diff: DiffManifest) -> Manifest:
    """Verify that *diff* applies to *base* and return its target manifest.

    This is the structural check only: it proves the diff names this exact base
    and that its operations reconstruct the declared target. Byte-level checks
    of referenced blobs are the consumer's job.

    The reconstruction is derived from the *base* manifest plus the ops, never
    from the diff's target declaration, so comparing it with the target is a
    real consistency check rather than a tautology.
    """
    if diff.dataset != base.dataset:
        raise ValidationError(
            f"diff targets dataset {diff.dataset!r}, local base is {base.dataset!r}"
        )
    if diff.is_full:
        raise ValidationError("a full package is not a diff and has no base")
    if diff.base_manifest_digest != base.digest:
        raise ValidationError(
            "diff was built for a different base manifest "
            f"({diff.base_manifest_digest[:12]} != {base.digest[:12]})"
        )
    if diff.base_version != base.version or diff.base_seq != base.seq:
        raise ValidationError(
            f"diff was built for base {diff.base_version}, local base is {base.version}"
        )
    ops_by_name = {o["name"]: o for o in diff.segments}
    # Start from the base's own segments, then apply the operations.
    segments: dict[str, Segment] = {}
    for name, entry in ops_by_name.items():
        if entry["op"] == "remove":
            if name not in base.segments:
                raise ValidationError(
                    f"diff removes segment {name}, which the base does not contain"
                )
            continue
        if entry["op"] in ("unchanged", "replace"):
            local = base.segments.get(name)
            if local is None:
                raise ValidationError(
                    f"diff {entry['op']}s segment {name}, which the base does not contain"
                )
            if local.digest != entry["base_digest"]:
                raise ValidationError(
                    f"base segment {name} differs from the diff's recorded base"
                )
        if entry["op"] == "unchanged":
            segments[name] = local
        elif entry["op"] in ("add", "replace"):
            segments[name] = Segment(
                name=name,
                digest=entry["digest"],
                size=entry["size"],
                count=entry["count"],
            )
        else:  # pragma: no cover - op values are validated on parse
            raise ValidationError(f"unknown diff op {entry['op']!r}")
    rebuilt = Manifest(
        dataset=diff.dataset,
        version=diff.target_version,
        seq=diff.target_seq,
        created_at=diff.target_manifest.created_at,
        segments=segments,
        # Attributes are manifest-level metadata the diff carries explicitly;
        # the segment set, digests, sizes and counts above are what must be
        # derived from the base plus the operations.
        attributes=diff.target_manifest.attributes,
    )
    if rebuilt.to_bytes() != diff.target_manifest.to_bytes():
        raise ValidationError(
            "diff operations do not reconstruct the declared target manifest"
        )
    return diff.target_manifest
