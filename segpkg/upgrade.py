"""Consumer upgrade planner and applier.

Given what the consumer has installed and what the release offers, the planner
chooses among:

``diff``
    Fetch the diff from the exact installed base, verify every referenced base
    blob, download only changed segments, and prove the reconstructed package
    equals the target's content digest.

``chain``
    No single diff leads from an installed version to the target (for example
    the consumer skipped a release). Apply a sequence of published diffs through
    intermediate versions, verifying each step and the final content digest.

``refetch-base``
    The release still offers the base and the local copy is incomplete or
    damaged. Re-download the damaged base segments, then diff from it. Only
    chosen when cheaper than a full download and the base is still available.

``full``
    Download the whole target package. Always valid; more bytes. Also the
    fallback when a diff cannot be applied or verified.

Every successful path ends with ``_verify_result``: the installed manifest's
content digest must equal the independently published target's, and every
segment blob is re-hashed. A failed diff therefore degrades to a full download,
never to a mixed-version install.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .client import ProgressSink, ReleaseClient, ReleaseClientError
from .consumer import Consumer
from .segments import DiffManifest, Manifest, apply_diff
from .util import SegpkgError, ValidationError, blob_relpath, human_bytes, sha256_bytes


class ApplyError(SegpkgError):
    pass


class DownloadAborted(SegpkgError):
    """Raised by tests to simulate an interrupted transfer."""


@dataclass
class Strategy:
    kind: str  # "diff" | "chain" | "refetch-base" | "full"
    download_segments: int
    download_bytes: int
    base_version: str | None = None
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    steps: list[DiffManifest] = field(default_factory=list)
    missing_base_segments: list[str] = field(default_factory=list)
    corrupt_base_segments: list[str] = field(default_factory=list)

    @property
    def is_diff_like(self) -> bool:
        return self.kind in ("diff", "chain", "refetch-base")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "base_version": self.base_version,
            "download_segments": self.download_segments,
            "download_bytes": self.download_bytes,
            "reasons": self.reasons,
            "warnings": self.warnings,
            "missing_base_segments": self.missing_base_segments,
            "corrupt_base_segments": self.corrupt_base_segments,
            "steps": [
                {
                    "base": s.base_version,
                    "target": s.target_version,
                    "download_segments": s.download_segments,
                    "download_bytes": s.download_bytes,
                }
                for s in self.steps
            ],
        }


@dataclass
class Plan:
    dataset: str
    target_version: str
    target_manifest: Manifest
    options: list[Strategy]
    selected: Strategy
    target_content_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "target_version": self.target_version,
            "target_content_digest": self.target_content_digest,
            "target_bytes": self.target_manifest.total_bytes,
            "target_segments": len(self.target_manifest.segments),
            "selected": self.selected.to_dict(),
            "options": [o.to_dict() for o in self.options],
        }


def _scan_blobs(consumer: Consumer, manifest: Manifest) -> tuple[list[str], list[str]]:
    """Return (missing segment names, corrupt segment names) for *manifest*."""
    missing: list[str] = []
    corrupt: list[str] = []
    for name in manifest.segment_names():
        digest = manifest.segments[name].digest
        path = consumer.blob_path(digest)
        if not path.is_file():
            missing.append(name)
            continue
        h = hashlib.sha256()
        try:
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
        except OSError:
            missing.append(name)
            continue
        if h.hexdigest() != digest:
            corrupt.append(name)
    return missing, corrupt


class _DiffCache:
    def __init__(self, client: ReleaseClient, dataset: str):
        self.client = client
        self.dataset = dataset
        self._cache: dict[tuple[str, str], DiffManifest | None] = {}

    def get(self, target_version: str, base_version: str) -> DiffManifest | None:
        key = (target_version, base_version)
        if key not in self._cache:
            try:
                raw = self.client.diff_bytes(self.dataset, target_version, base_version)
                diff = DiffManifest.from_bytes(raw)
            except (SegpkgError, ValidationError):
                # A missing or unreadable diff just means this edge does not
                # exist; the planner will consider other paths.
                diff = None
            if diff is not None and diff.is_full:
                diff = None
            self._cache[key] = diff
        return self._cache[key]


def plan_upgrade(
    consumer: Consumer,
    client: ReleaseClient,
    dataset: str,
    target_version: str,
    prefer: str = "auto",
    allow_refetch_base: bool = True,
) -> Plan:
    """Build a plan. ``prefer`` is ``auto``, ``diff`` (no diff -> error), or ``full``."""
    if prefer not in ("auto", "diff", "full"):
        raise ValidationError("prefer must be auto, diff, or full")
    detail = client.dataset(dataset)
    versions = {v["version"]: v for v in detail["versions"]}
    if target_version not in versions:
        raise SegpkgError(f"{dataset}/{target_version} is not published")
    target_info = versions[target_version]
    target = Manifest.from_bytes(client.manifest_bytes(dataset, target_version))
    if target.digest != target_info["manifest_digest"]:
        raise ReleaseClientError("target manifest does not match release index")
    if target.content_digest != target_info["content_digest"]:
        raise ReleaseClientError("target manifest content digest mismatch")

    full = Strategy(
        kind="full",
        download_segments=len(target.segments),
        download_bytes=target.total_bytes,
        reasons=["download the complete target package"],
    )
    if prefer == "full":
        return Plan(dataset, target_version, target, [full], full,
                    target.content_digest)

    local = {v.version: v for v in consumer.installed() if v.dataset == dataset}
    local_manifests: dict[str, Manifest] = {}
    for version in local:
        manifest = consumer.get(dataset, version)
        if manifest is not None:
            local_manifests[version] = manifest

    cache = _DiffCache(client, dataset)
    published = sorted(
        (v for v in versions.values() if v["seq"] < target.seq),
        key=lambda v: (v["seq"], v["version"]),
    )

    options: list[Strategy] = []

    # --- direct diffs from installed bases -----------------------------
    for version, manifest in sorted(local_manifests.items()):
        if version not in versions or versions[version]["seq"] >= target.seq:
            continue
        if version not in target_info.get("diffs", {}):
            continue
        diff = cache.get(target_version, version)
        if diff is None:
            continue
        if diff.target_manifest_digest != target.digest:
            continue
        if diff.base_manifest_digest != manifest.digest:
            # Local bytes for this version label came from a different build;
            # never improvise a patch against an unverified base.
            options.append(
                Strategy(
                    kind="full",
                    download_segments=len(target.segments),
                    download_bytes=target.total_bytes,
                    base_version=version,
                    reasons=[
                        f"local {version} is a different build than the release's "
                        f"{version}; its manifest digest does not match"
                    ],
                    warnings=[
                        "re-install the package to get a patchable base"
                    ],
                )
            )
            continue
        options.extend(
            _diff_options(
                consumer,
                manifest,
                diff,
                target,
                version,
                allow_refetch_base and version in versions,
            )
        )

    # --- chained diffs through intermediate published versions ---------
    chain = _find_chain(
        consumer, cache, local_manifests, versions, published, target, allow_refetch_base
    )
    if chain is not None:
        options.append(chain)

    # --- choose ---------------------------------------------------------
    if prefer == "diff":
        diff_like = [o for o in options if o.is_diff_like]
        if not diff_like:
            raise SegpkgError(
                "no applicable diff leads to "
                f"{dataset}/{target_version} from installed versions "
                f"{sorted(local_manifests)}"
            )
        winner = min(diff_like, key=lambda o: o.download_bytes)
        ordered = [winner] + [o for o in options if o is not winner]
        return Plan(dataset, target_version, target, ordered, winner,
                    target.content_digest)

    diff_like = [o for o in options if o.is_diff_like]
    options.append(full)
    if diff_like:
        winner = min(diff_like, key=lambda o: (o.download_bytes, o.kind != "diff"))
        if winner.download_bytes >= target.total_bytes:
            reasons = winner.reasons + [
                f"diff path would transfer {human_bytes(winner.download_bytes)}, "
                f"no better than the full package ({human_bytes(target.total_bytes)})"
            ]
            winner = Strategy(
                kind="full",
                download_segments=len(target.segments),
                download_bytes=target.total_bytes,
                base_version=winner.base_version,
                reasons=reasons,
                warnings=winner.warnings,
            )
            options.append(winner)
    else:
        winner = full
        if local_manifests:
            winner.reasons.append(
                "no published diff reaches the target from the installed versions"
            )
    return Plan(dataset, target_version, target, options, winner,
                target.content_digest)


def _diff_options(
    consumer: Consumer,
    base: Manifest,
    diff: DiffManifest,
    target: Manifest,
    base_version: str,
    allow_refetch_base: bool,
) -> list[Strategy]:
    """Strategies for one base->target diff, depending on local base health."""
    missing, corrupt = _scan_blobs(consumer, base)
    # Only ``unchanged`` segments reuse local base bytes. A ``replace`` brings
    # new content, so a damaged old copy of it does not block the patch.
    required_names = {e["name"] for e in diff.segments if e["op"] == "unchanged"}
    needed_missing = sorted(n for n in missing if n in required_names)
    needed_corrupt = sorted(n for n in corrupt if n in required_names)
    repair_bytes = sum(
        base.segments[n].size for n in set(needed_missing) | set(needed_corrupt)
    )
    healthy = not needed_missing and not needed_corrupt
    reasons = [
        f"installed {base_version} matches the diff's pinned base "
        f"({diff.base_manifest_digest[:12]}); "
        f"{diff.download_segments} of {len(target.segments)} segments change"
    ]
    warnings: list[str] = []
    if corrupt:
        warnings.append(
            "local base has corrupt segments: " + ", ".join(sorted(corrupt))
        )
    if healthy:
        return [
            Strategy(
                kind="diff",
                base_version=base_version,
                download_segments=diff.download_segments,
                download_bytes=diff.download_bytes,
                reasons=reasons,
                warnings=warnings,
                steps=[diff],
            )
        ]
    # Base is missing or damaged: we may repair only what the patch needs.
    if allow_refetch_base:
        total = diff.download_bytes + repair_bytes
        repair_note = (
            f"base {base_version} needs {len(needed_missing)} missing and "
            f"{len(needed_corrupt)} corrupt segment(s) re-fetched "
            f"({human_bytes(repair_bytes)})"
        )
        if total < target.total_bytes:
            return [
                Strategy(
                    kind="refetch-base",
                    base_version=base_version,
                    download_segments=diff.download_segments + len(needed_missing) + len(needed_corrupt),
                    download_bytes=total,
                    reasons=[repair_note] + reasons,
                    warnings=warnings,
                    steps=[diff],
                    missing_base_segments=needed_missing,
                    corrupt_base_segments=needed_corrupt,
                )
            ]
        return [
            Strategy(
                kind="full",
                base_version=base_version,
                download_segments=len(target.segments),
                download_bytes=target.total_bytes,
                reasons=[
                    f"repairing base {base_version} would cost {human_bytes(total)}, "
                    f"not less than the full package ({human_bytes(target.total_bytes)})"
                ],
                warnings=warnings,
                missing_base_segments=needed_missing,
                corrupt_base_segments=needed_corrupt,
            )
        ]
    return [
        Strategy(
            kind="full",
            base_version=base_version,
            download_segments=len(target.segments),
            download_bytes=target.total_bytes,
            reasons=[
                f"base {base_version} is damaged and base re-fetch is disabled"
            ],
            warnings=warnings,
            missing_base_segments=needed_missing,
            corrupt_base_segments=needed_corrupt,
        )
    ]


def _find_chain(
    consumer: Consumer,
    cache: _DiffCache,
    local_manifests: dict[str, Manifest],
    versions: dict[str, dict[str, Any]],
    published: list[dict[str, Any]],
    target: Manifest,
    allow_refetch_base: bool,
) -> Strategy | None:
    """Cheapest multi-hop diff path from an installed version to the target.

    Dijkstra over published versions; an edge exists when the newer version
    publishes a diff naming the older one. Cost is downloaded bytes. The path
    must be at least two hops to be interesting (single hops are handled as
    direct diffs), and every hop's diff must name the manifest digest of the
    version it starts from, which we verify against the published manifest.
    """
    if not local_manifests:
        return None
    nodes = {v["version"]: v for v in published}
    nodes[target.version] = {
        "version": target.version,
        "seq": target.seq,
        "manifest_digest": target.digest,
        "content_digest": target.content_digest,
        "total_bytes": target.total_bytes,
    }
    by_seq = sorted(nodes.values(), key=lambda v: (v["seq"], v["version"]))
    published_manifests: dict[str, Manifest] = {target.version: target}
    for info in by_seq:
        version = info["version"]
        if version in published_manifests:
            continue
        try:
            from .segments import Manifest as _M

            manifest = _M.from_bytes(
                cache.client.manifest_bytes(cache.dataset, version)
            )
        except (ReleaseClientError, ValidationError):
            continue
        published_manifests[version] = manifest

    # Best known cost to reach each version, and the path taken.
    best: dict[str, tuple[int, list[DiffManifest]]] = {}
    repairs: dict[str, int] = {}
    for version, manifest in local_manifests.items():
        if (
            version in published_manifests
            and published_manifests[version].digest == manifest.digest
        ):
            # A damaged local base can still start the chain: the applier
            # re-fetches any segment a step actually needs. Price the repair in
            # so the comparison with a full download stays honest.
            missing, corrupt = _scan_blobs(consumer, manifest)
            repair_cost = 0
            if (missing or corrupt) and not allow_refetch_base:
                continue
            if missing or corrupt:
                repair_cost = sum(
                    manifest.segments[n].size for n in set(missing) | set(corrupt)
                )
            best[version] = (repair_cost, [])
            repairs[version] = repair_cost

    if not best:
        return None

    import heapq

    queue: list[tuple[int, str]] = [(0, v) for v in best]
    heapq.heapify(queue)
    final_path: list[DiffManifest] | None = None
    while queue:
        cost, version = heapq.heappop(queue)
        if cost > best.get(version, (1 << 62, []))[0]:
            continue
        if version == target.version and len(best[version][1]) >= 2:
            final_path = best[version][1]
            break
        for nxt in by_seq:
            nversion = nxt["version"]
            if nxt["seq"] <= nodes[version]["seq"]:
                continue
            if version not in published_manifests or nversion not in published_manifests:
                continue
            diff = cache.get(nversion, version)
            if diff is None:
                continue
            if diff.base_manifest_digest != published_manifests[version].digest:
                continue
            if diff.target_manifest_digest != published_manifests[nversion].digest:
                continue
            ncost = cost + diff.download_bytes
            if ncost < best.get(nversion, (1 << 62, []))[0]:
                best[nversion] = (ncost, best[version][1] + [diff])
                heapq.heappush(queue, (ncost, nversion))
    if final_path is None or len(final_path) < 2:
        return None
    start = final_path[0].base_version
    reasons = [
        "no single published diff reaches the target; chaining "
        + " -> ".join([start] + [d.target_version for d in final_path])
    ]
    warnings: list[str] = []
    if repairs.get(start):
        warnings.append(
            f"chain starts from a damaged {start}; needed segments are re-fetched"
        )
    return Strategy(
        kind="chain",
        base_version=start,
        download_segments=sum(d.download_segments for d in final_path),
        download_bytes=sum(d.download_bytes for d in final_path) + repairs.get(start, 0),
        reasons=reasons,
        warnings=warnings,
        steps=final_path,
    )


# ---------------------------------------------------------------------------
# Applying
# ---------------------------------------------------------------------------


class _Transfer:
    def __init__(
        self,
        consumer: Consumer,
        client: ReleaseClient,
        op_id: str,
        progress: ProgressSink,
        max_blobs: int | None,
    ):
        self.consumer = consumer
        self.client = client
        self.op_id = op_id
        self.progress = progress
        self.max_blobs = max_blobs
        self.blob_downloads = 0
        self.bytes_downloaded = 0
        self.reused = 0

    def fetch(self, digest: str) -> bytes:
        """Return verified bytes for *digest*, downloading only if needed."""
        staged = blob_relpath(digest)
        staged_path = self.consumer.staging_dir(self.op_id) / staged
        if staged_path.is_file() and self.consumer.staged_ok(self.op_id, digest):
            self.progress.emit({"phase": "reuse-staged", "digest": digest})
            self.reused += 1
            return staged_path.read_bytes()
        if self.consumer.blob_ok(digest):
            self.progress.emit({"phase": "reuse-local", "digest": digest})
            self.reused += 1
            return self.consumer.blob_path(digest).read_bytes()
        if self.max_blobs is not None and self.blob_downloads >= self.max_blobs:
            raise DownloadAborted(
                f"simulated interruption after {self.blob_downloads} blob downloads"
            )
        payload = self.client.blob(digest, progress=self.progress)
        self.consumer.stage_payload(self.op_id, digest, payload)
        self.blob_downloads += 1
        self.bytes_downloaded += len(payload)
        return payload

    def commit(self, digest: str) -> None:
        if self.consumer.blob_ok(digest):
            # Already durable (reused locally); nothing to promote.
            return
        self.consumer.promote_staged(self.op_id, digest)


def apply_plan(
    consumer: Consumer,
    client: ReleaseClient,
    plan: Plan,
    *,
    progress: ProgressSink | None = None,
    max_blobs: int | None = None,
) -> dict[str, Any]:
    """Execute *plan*, verifying the installed result equals the full package.

    ``max_blobs`` aborts after N network downloads to simulate an interrupted
    transfer. Fetched-but-uncommitted blobs stay in staging and are reused on
    the next attempt; installed versions and durable blobs are untouched.
    """
    strategy = plan.selected
    progress = progress or ProgressSink()
    op_id = f"{plan.dataset}-{plan.target_version}"
    transfer = _Transfer(consumer, client, op_id, progress, max_blobs)

    def done(result: dict[str, Any], used: str, fallback: str | None = None) -> dict[str, Any]:
        consumer.log_event(
            {
                "type": "upgrade",
                "dataset": plan.dataset,
                "target": plan.target_version,
                "strategy": used,
                "fell_back": fallback is not None,
            }
        )
        result.update(
            {
                "plan": plan.to_dict(),
                "strategy_used": used,
                "fell_back": fallback is not None,
            }
        )
        if fallback is not None:
            result["fallback_reason"] = fallback
        return result

    if strategy.is_diff_like and strategy.steps:
        try:
            result = _apply_diff_path(consumer, plan, strategy, transfer)
        except (DownloadAborted, KeyboardInterrupt):
            consumer.log_event(
                {
                    "type": "interrupted",
                    "dataset": plan.dataset,
                    "target": plan.target_version,
                    "base": strategy.base_version,
                }
            )
            raise
        except Exception as exc:  # noqa: BLE001 - verified fallback is intended
            consumer.log_event(
                {
                    "type": "diff-failed",
                    "dataset": plan.dataset,
                    "target": plan.target_version,
                    "base": strategy.base_version,
                    "error": str(exc),
                }
            )
            progress.emit({"phase": "fallback", "reason": str(exc)})
            result = _apply_full(consumer, plan, transfer)
            result["diff_failure"] = str(exc)
            return done(result, "full", fallback=str(exc))
        return done(result, strategy.kind)

    result = _apply_full(consumer, plan, transfer)
    return done(result, "full")


def _apply_diff_path(
    consumer: Consumer,
    plan: Plan,
    strategy: Strategy,
    transfer: _Transfer,
) -> dict[str, Any]:
    dataset = plan.dataset
    first = strategy.steps[0]
    current = consumer.require(dataset, first.base_version)
    if current.digest != first.base_manifest_digest:
        raise ApplyError(
            f"local base {first.base_version} does not match the diff's pinned base"
        )
    for step in strategy.steps:
        if step.base_manifest_digest != current.digest:
            raise ApplyError(
                f"diff {step.base_version}->{step.target_version} does not chain "
                "onto the package built by the previous step"
            )
        # Structural check: the diff names this exact base and its operations
        # reconstruct the declared target manifest byte-for-byte.
        reconstructed = apply_diff(current, step)
        if reconstructed.to_bytes() != step.target_manifest.to_bytes():
            raise ApplyError(
                f"diff {step.base_version}->{step.target_version} does not "
                "reconstruct the declared target manifest"
            )
        for entry in step.segments:
            if entry["op"] != "unchanged":
                continue
            local = current.segments.get(entry["name"])
            if local is None or local.digest != entry["base_digest"]:
                raise ApplyError(
                    f"diff step needs base segment {entry['name']} that local "
                    f"{step.base_version} does not have in the required form"
                )
            payload = transfer.fetch(local.digest)
            if sha256_bytes(payload) != local.digest:
                raise ApplyError(
                    f"segment {entry['name']} of base {step.base_version} failed verification"
                )
            # If this blob had to be re-fetched, make the repair durable now.
            transfer.commit(local.digest)
        for entry in step.segments:
            if entry["op"] not in ("add", "replace"):
                continue
            payload = transfer.fetch(entry["digest"])
            if sha256_bytes(payload) != entry["digest"]:
                raise ApplyError(
                    f"downloaded segment {entry['name']} failed verification"
                )
        for entry in step.segments:
            if entry["op"] != "remove":
                transfer.commit(entry["digest"])
        # Advance in memory. Intermediate manifests are not installed as
        # versions: only the final target becomes visible to queries.
        current = step.target_manifest
    consumer.install_manifest(
        current, transfer.op_id, f"diff:{first.base_version}"
    )
    _verify_result(consumer, plan)
    consumer.cleanup_staging(transfer.op_id)
    transfer.progress.emit(
        {"phase": "installed", "version": plan.target_version, "strategy": strategy.kind}
    )
    return {
        "dataset": dataset,
        "version": plan.target_version,
        "base_version": strategy.base_version,
        "strategy": strategy.kind,
        "downloaded_segments": transfer.blob_downloads,
        "plan_segments": strategy.download_segments,
        "downloaded_bytes": transfer.bytes_downloaded,
        "reused_segments": transfer.reused,
        "hops": [
            {
                "base": s.base_version,
                "target": s.target_version,
                "download_bytes": s.download_bytes,
            }
            for s in strategy.steps
        ],
        "content_digest": plan.target_content_digest,
        "verified": True,
    }


def _apply_full(
    consumer: Consumer, plan: Plan, transfer: _Transfer
) -> dict[str, Any]:
    target = plan.target_manifest
    for name in target.segment_names():
        seg = target.segments[name]
        payload = transfer.fetch(seg.digest)
        if sha256_bytes(payload) != seg.digest:
            raise ApplyError(f"downloaded segment {name} failed verification")
        transfer.commit(seg.digest)
    consumer.install_manifest(target, transfer.op_id, "full")
    _verify_result(consumer, plan)
    consumer.cleanup_staging(transfer.op_id)
    transfer.progress.emit(
        {"phase": "installed", "version": target.version, "strategy": "full"}
    )
    return {
        "dataset": plan.dataset,
        "version": target.version,
        "strategy": "full",
        "downloaded_segments": transfer.blob_downloads,
        "plan_segments": len(target.segments),
        "downloaded_bytes": transfer.bytes_downloaded,
        "reused_segments": transfer.reused,
        "content_digest": plan.target_content_digest,
        "verified": True,
    }


def _verify_result(consumer: Consumer, plan: Plan) -> None:
    """Prove the installed package equals the independent full build."""
    manifest = consumer.require(plan.dataset, plan.target_version)
    if manifest.content_digest != plan.target_content_digest:
        raise ApplyError(
            "installed content digest does not match the published target"
        )
    missing, corrupt = _scan_blobs(consumer, manifest)
    if missing or corrupt:
        raise ApplyError(
            f"installed package failed verification (missing={missing}, corrupt={corrupt})"
        )


def upgrade(
    consumer: Consumer,
    client: ReleaseClient,
    dataset: str,
    target_version: str,
    prefer: str = "auto",
    progress: ProgressSink | None = None,
    max_blobs: int | None = None,
) -> dict[str, Any]:
    plan = plan_upgrade(consumer, client, dataset, target_version, prefer=prefer)
    result = apply_plan(consumer, client, plan, progress=progress, max_blobs=max_blobs)
    return result
