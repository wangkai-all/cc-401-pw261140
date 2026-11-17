"""Package builder: turn a JSONL source into an immutable, sharded package.

The builder is deliberately a separate process from the release service. It
writes a *build directory* containing ``manifest.json``, ``segments/`` and the
diff documents it can compute from earlier versions, then (optionally) copies
the result into a release root in one atomic publish.

Usage::

    python -m segpkg.builder build  --dataset catalog --version v2 \
        --source data/catalog-v2.jsonl --out build/v2 \
        --base-version v1 --base-manifest build/v1/manifest.json
    python -m segpkg.builder publish --build build/v2 --release releases/main
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from .segments import (
    Manifest,
    build_segments,
    canonical_segment_bytes,
    make_diff,
    record_key,
    segment_payloads,
)
from .util import (
    SegpkgError,
    ValidationError,
    atomic_write,
    atomic_write_json,
    ensure_dir,
    now_iso,
    sha256_bytes,
    validate_dataset,
    validate_version,
    version_seq,
)
from .release_store import ReleaseStore


def build_package(
    records: list[dict[str, Any]],
    dataset: str,
    version: str,
    out_dir: Path,
    attributes: dict[str, Any] | None = None,
    created_at: str | None = None,
) -> Manifest:
    validate_dataset(dataset)
    validate_version(version)
    payloads = segment_payloads(records)
    segments = build_segments(records)
    if not segments:
        raise ValidationError("source produced no segments")
    manifest = Manifest(
        dataset=dataset,
        version=version,
        seq=version_seq(version),
        created_at=created_at or now_iso(),
        segments=segments,
        attributes=attributes or {},
    )
    ensure_dir(out_dir / "segments")
    for name, payload in payloads.items():
        atomic_write(out_dir / "segments" / f"{name}.jsonl", payload)
    atomic_write(out_dir / "manifest.json", manifest.to_bytes())
    atomic_write_json(
        out_dir / "build.json",
        {
            "dataset": dataset,
            "version": version,
            "manifest_digest": manifest.digest,
            "content_digest": manifest.content_digest,
            "segments": len(segments),
            "total_bytes": manifest.total_bytes,
            "total_records": manifest.total_records,
            "records": len(records),
        },
    )
    return manifest


def read_build(out_dir: Path) -> tuple[Manifest, dict[str, bytes]]:
    manifest_path = out_dir / "manifest.json"
    if not manifest_path.is_file():
        raise SegpkgError(f"no manifest.json in build directory {out_dir}")
    manifest = Manifest.from_bytes(manifest_path.read_bytes())
    payloads: dict[str, bytes] = {}
    for seg in manifest.segments.values():
        path = out_dir / "segments" / f"{seg.name}.jsonl"
        if not path.is_file():
            raise SegpkgError(f"build is missing segment file {path}")
        payload = path.read_bytes()
        if sha256_bytes(payload) != seg.digest:
            raise ValidationError(
                f"build segment {seg.name} hash does not match manifest"
            )
        payloads[seg.name] = payload
    return manifest, payloads


def write_diff(build_dir: Path, diff, base_tag: str) -> Path:
    path = build_dir / "diffs" / f"{base_tag}.json"
    atomic_write(path, diff.to_bytes())
    return path


def cmd_build(args: argparse.Namespace) -> int:
    from .util import read_jsonl

    records = read_jsonl(args.source)
    if not records:
        raise ValidationError(f"{args.source} contains no records")
    ids = [record_key(r, i) for i, r in enumerate(records)]
    if len(set(ids)) != len(ids):
        raise ValidationError("source has duplicate record ids")
    out = Path(args.out)
    attributes = {}
    if args.attributes:
        import json

        attributes = json.loads(args.attributes)
    manifest = build_package(records, args.dataset, args.version, out, attributes)
    message = {
        "built": f"{args.dataset}/{args.version}",
        "manifest_digest": manifest.digest,
        "content_digest": manifest.content_digest,
        "segments": len(manifest.segments),
        "bytes": manifest.total_bytes,
        "records": manifest.total_records,
        "out": str(out),
    }
    if args.base_manifest:
        base = Manifest.from_bytes(Path(args.base_manifest).read_bytes())
        diff = make_diff(base, manifest, now_iso())
        path = write_diff(out, diff, base.version)
        message["diff"] = {
            "base": base.version,
            "path": str(path),
            "download_segments": diff.download_segments,
            "download_bytes": diff.download_bytes,
            "removed_segments": len(diff.removed),
            "file_bytes": len(diff.to_bytes()),
        }
    print_json(message)
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    build_dir = Path(args.build)
    manifest, payloads = read_build(build_dir)
    store = ReleaseStore(args.release)

    bases: list[Manifest] = []
    for base_version in args.base_version or []:
        base_manifest, _ = store.manifest(manifest.dataset, base_version)
        if base_manifest.seq >= manifest.seq:
            raise ValidationError(
                f"base {base_version} is not older than {manifest.version}"
            )
        bases.append(base_manifest)
    # Also pick up diffs written next to the build, so a publish can be done
    # from a machine that never held the release root.
    diff_paths = sorted((build_dir / "diffs").glob("*.json")) if (build_dir / "diffs").is_dir() else []
    diffs = []
    for path in diff_paths:
        from .segments import DiffManifest

        diff = DiffManifest.from_bytes(path.read_bytes())
        if diff.is_full:
            continue
        if diff.target_manifest_digest != manifest.digest:
            raise ValidationError(
                f"{path} is a diff for a different target manifest"
            )
        if path.stem != diff.base_version:
            raise ValidationError(
                f"{path} is named for base {path.stem}, diff says {diff.base_version}"
            )
        diffs.append(diff)
    info = store.publish(
        manifest,
        payloads,
        diffs=diffs,
        make_diffs_from=bases,
    )
    print_json(
        {
            "published": f"{manifest.dataset}/{manifest.version}",
            "manifest_digest": info.manifest_digest,
            "content_digest": info.content_digest,
            "diffs": sorted(info.diffs),
        }
    )
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    store = ReleaseStore(args.release)
    result = store.verify(args.dataset)
    print_json(result)
    return 0 if result["ok"] else 1


def cmd_gc(args: argparse.Namespace) -> int:
    store = ReleaseStore(args.release)
    keep = None
    if args.keep_versions:
        keep = [v.strip() for v in args.keep_versions.split(",") if v.strip()]
    result = store.gc(
        args.dataset,
        keep_versions=keep,
        keep_last=args.keep_last,
        dry_run=args.dry_run,
    )
    print_json(result)
    return 0


def print_json(obj: Any) -> None:
    import json

    print(json.dumps(obj, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="segpkg.builder")
    sub = parser.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build", help="build a package from a JSONL source")
    b.add_argument("--dataset", required=True)
    b.add_argument("--version", required=True)
    b.add_argument("--source", required=True)
    b.add_argument("--out", required=True)
    b.add_argument("--attributes", default=None, help="JSON object of metadata")
    b.add_argument("--base-manifest", default=None, help="older manifest.json")
    b.set_defaults(func=cmd_build)

    p = sub.add_parser("publish", help="publish a build to a release directory")
    p.add_argument("--build", required=True)
    p.add_argument("--release", required=True)
    p.add_argument("--base-version", action="append", default=[],
                   help="version in the release to build a direct diff from")
    p.set_defaults(func=cmd_publish)

    v = sub.add_parser("verify", help="verify a release directory")
    v.add_argument("--release", required=True)
    v.add_argument("--dataset", default=None)
    v.set_defaults(func=cmd_verify)

    g = sub.add_parser("gc", help="prune old versions without breaking diffs")
    g.add_argument("--release", required=True)
    g.add_argument("--dataset", required=True)
    g.add_argument("--keep-versions", default=None,
                   help="comma separated versions to keep")
    g.add_argument("--keep-last", type=int, default=None)
    g.add_argument("--dry-run", action="store_true")
    g.set_defaults(func=cmd_gc)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (SegpkgError, ValidationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
