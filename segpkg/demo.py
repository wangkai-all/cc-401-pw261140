"""One-shot demo/orchestrator: builds versions, publishes, serves, and shows
consumer recovery paths. Used by the web console and by integration tests.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from .client import ReleaseClient
from .consumer import Consumer
from .fixtures import DatasetSpec, DatasetVariant
from .release_server import ReleaseServer
from .release_store import ReleaseStore
from .segments import Manifest, make_diff
from .util import human_bytes, now_iso
from . import builder


def make_version_chain(
    source_dir: Path,
    dataset: str = "catalog",
    base: str = "v1",
    count: int = 3,
    record_count: int = 60,
) -> list[Manifest]:
    """Generate and build *count* versions, each editing a few records."""
    source_dir.mkdir(parents=True, exist_ok=True)
    base_seq = int(base.lstrip("v")) if base.lstrip("v").isdigit() else 1
    manifests: list[Manifest] = []
    variant: DatasetVariant | None = None
    for step in range(count):
        version = f"v{base_seq + step}"
        spec = DatasetSpec(dataset, version, record_count=record_count)
        if variant is None:
            variant = DatasetVariant(spec)
        else:
            variant.spec = spec
            if step == 1:
                variant.touch(3, "edit-one")
            else:
                variant.touch(11 + step, f"edit-{step}")
                variant.append(2, f"append-{step}")
        src = source_dir / f"{dataset}-{version}.jsonl"
        variant.write(src)
        out = source_dir / "builds" / version
        manifest = builder.build_package(
            variant.records, dataset, version, out, created_at=now_iso()
        )
        manifests.append(manifest)
    return manifests


def publish_chain(
    release_root: Path, source_dir: Path, dataset: str, versions: list[str]
) -> list[dict[str, Any]]:
    """Publish each version, with a direct diff from every earlier version.

    Publishing all pairs is what lets a consumer that skipped several releases
    take one hop instead of replaying every intermediate diff.
    """
    store = ReleaseStore(release_root)
    out = []
    earlier: list[Manifest] = []
    for version in versions:
        build_dir = source_dir / "builds" / version
        manifest, payloads = builder.read_build(build_dir)
        info = store.publish(manifest, payloads, make_diffs_from=earlier)
        out.append(info.to_dict())
        earlier.append(manifest)
    return out


def build_and_publish(
    release_root: Path,
    source_dir: Path,
    dataset: str = "catalog",
    base: str = "v1",
    count: int = 3,
) -> list[dict[str, Any]]:
    manifests = make_version_chain(source_dir, dataset, base, count)
    return publish_chain(release_root, source_dir, dataset, [m.version for m in manifests])


def demo(
    workdir: Path,
    port: int = 0,
    consumer_count: int = 2,
) -> dict[str, Any]:
    """Run the full scenario and return a JSON summary (web console backend)."""
    release = workdir / "release"
    sources = workdir / "sources"
    shutil.rmtree(release, ignore_errors=True)
    shutil.rmtree(sources, ignore_errors=True)
    for i in range(consumer_count + 2):
        shutil.rmtree(workdir / f"c{i + 1}", ignore_errors=True)
    published = build_and_publish(release, sources)
    store = ReleaseStore(release)
    server = ReleaseServer(store, port=port).start_background()
    try:
        client = ReleaseClient(server.base_url)
        consumers = {}
        for i in range(consumer_count):
            consumers[f"c{i + 1}"] = Consumer(workdir / f"c{i + 1}")
        steps: list[dict[str, Any]] = []

        def show(label: str, result: dict[str, Any]) -> None:
            steps.append({"label": label, **result})

        # 1. install v1 everywhere
        for name, consumer in consumers.items():
            from .upgrade import Plan, Strategy, apply_plan, upgrade

            target = Manifest.from_bytes(client.manifest_bytes("catalog", "v1"))
            full = Strategy("full", len(target.segments), target.total_bytes,
                            reasons=["initial install"])
            plan = Plan("catalog", "v1", target, [full], full, target.content_digest)
            show(f"{name}: install v1", _summarise(apply_plan(consumer, client, plan)))
        # 2. plain diff upgrade to v2
        c1 = consumers["c1"]
        show("c1: upgrade v1->v2 (diff)", _summarise(
            upgrade(
                c1, client, "catalog", "v2"
            )
        ))
        # 3. break one local base segment the v1->v3 diff needs, then upgrade.
        c2 = consumers["c2"]
        v1 = consumers["c1"].get("catalog", "v1")  # c1 just upgraded; same v1
        from .segments import DiffManifest

        diff13 = DiffManifest.from_bytes(client.diff_bytes("catalog", "v3", "v1"))
        needed = [e["name"] for e in diff13.segments if e["op"] == "unchanged"]
        broken = _corrupt_random_base_segment(c2, "catalog", "v1", needed)
        show("c2: local v1 segment damaged", {"corrupt_segment": broken})
        show("c2: upgrade v1->v3 with damaged base", _summarise(
            upgrade(
                c2, client, "catalog", "v3"
            )
        ))
        # 4. skip a version: fresh consumer installs v1 then jumps to v3
        c3 = Consumer(workdir / "c3")
        from .upgrade import Plan, Strategy, apply_plan, upgrade

        target = Manifest.from_bytes(client.manifest_bytes("catalog", "v1"))
        full = Strategy("full", len(target.segments), target.total_bytes)
        plan = Plan("catalog", "v1", target, [full], full, target.content_digest)
        apply_plan(c3, client, plan)
        show("c3: install v1 then upgrade to v3", _summarise(
            upgrade(
                c3, client, "catalog", "v3"
            )
        ))
        show("c3: query latest", {"records": len(c3.query("catalog", limit=1000))})
        return {
            "release": str(release),
            "published": published,
            "steps": steps,
            "url": server.base_url,
        }
    finally:
        server.shutdown()


def _summarise(result: dict[str, Any]) -> dict[str, Any]:
    plan = result.get("plan", {})
    selected = plan.get("selected", {})
    return {
        "version": result.get("version"),
        "strategy_used": result.get("strategy_used"),
        "fell_back": result.get("fell_back", False),
        "fallback_reason": result.get("fallback_reason"),
        "downloaded_segments": result.get("downloaded_segments"),
        "downloaded_bytes": result.get("downloaded_bytes"),
        "downloaded_human": human_bytes(result.get("downloaded_bytes") or 0),
        "reused_segments": result.get("reused_segments"),
        "verified": result.get("verified"),
        "content_digest": result.get("content_digest"),
        "plan_kind": selected.get("kind"),
        "plan_reasons": selected.get("reasons"),
        "plan_warnings": selected.get("warnings"),
        "missing_base_segments": selected.get("missing_base_segments"),
        "corrupt_base_segments": selected.get("corrupt_base_segments"),
        "hops": result.get("hops"),
    }


def _corrupt_random_base_segment(
    consumer: Consumer,
    dataset: str,
    version: str,
    required_names: list[str] | None = None,
) -> str:
    """Corrupt one installed segment, preferring one a later diff will need."""
    manifest = consumer.require(dataset, version)
    names = manifest.segment_names()
    if required_names:
        names = [n for n in names if n in set(required_names)] or names
    name = names[0]
    digest = manifest.segments[name].digest
    path = consumer.blob_path(digest)
    data = bytearray(path.read_bytes())
    if data:
        data[len(data) // 2] ^= 0xFF
    else:  # pragma: no cover
        data = b"corrupt"
    path.write_bytes(bytes(data))
    return name


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="segpkg.demo")
    parser.add_argument("--workdir", default="runtime/demo")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    workdir = Path(args.workdir)
    result = demo(workdir, port=args.port)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
