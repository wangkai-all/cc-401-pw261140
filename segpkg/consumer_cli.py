"""Consumer CLI: install, plan, upgrade, verify, query.

Examples::

    python -m segpkg.consumer_cli install  --root c1 --release http://127.0.0.1:8000 \
        --dataset catalog --version v1
    python -m segpkg.consumer_cli plan     --root c1 --release http://127.0.0.1:8000 \
        --dataset catalog --version v3
    python -m segpkg.consumer_cli upgrade  --root c1 --release http://127.0.0.1:8000 \
        --dataset catalog --version v3 --prefer auto
    python -m segpkg.consumer_cli query    --root c1 --dataset catalog --category alpha
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .client import ReleaseClient
from .consumer import Consumer, CorruptLocalError
from .segments import Manifest
from .upgrade import apply_plan, plan_upgrade, upgrade
from .util import SegpkgError


def cmd_install(args: argparse.Namespace) -> int:
    consumer = Consumer(args.root)
    client = ReleaseClient(args.release)
    manifest = Manifest.from_bytes(client.manifest_bytes(args.dataset, args.version))
    info = client.dataset(args.dataset)
    published = {v["version"]: v for v in info["versions"]}
    if args.version not in published:
        raise SegpkgError(f"{args.dataset}/{args.version} not published")
    if published[args.version]["manifest_digest"] != manifest.digest:
        raise SegpkgError("release index disagrees with served manifest")
    # Install as a "full" plan from scratch.
    from .upgrade import Plan, Strategy

    strategy = Strategy(
        kind="full",
        download_segments=len(manifest.segments),
        download_bytes=manifest.total_bytes,
        reasons=["initial install"],
    )
    plan = Plan(args.dataset, args.version, manifest, [strategy], strategy,
                manifest.content_digest)
    result = apply_plan(consumer, client, plan)
    print_json(result)
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    consumer = Consumer(args.root)
    client = ReleaseClient(args.release)
    plan = plan_upgrade(
        consumer, client, args.dataset, args.version, prefer=args.prefer
    )
    print_json(plan.to_dict())
    return 0


def cmd_upgrade(args: argparse.Namespace) -> int:
    consumer = Consumer(args.root)
    client = ReleaseClient(args.release)
    result = upgrade(
        consumer, client, args.dataset, args.version,
        prefer=args.prefer, max_blobs=args.max_blobs,
    )
    print_json(result)
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    consumer = Consumer(args.root)
    result = consumer.verify(args.dataset, args.version)
    print_json(result)
    return 0 if result["ok"] else 1


def cmd_query(args: argparse.Namespace) -> int:
    consumer = Consumer(args.root)
    records = consumer.query(
        args.dataset,
        version=args.version,
        category=args.category,
        min_score=args.min_score,
        text=args.text,
        limit=args.limit,
        all_versions=args.all_versions,
    )
    for rec in records:
        print(json.dumps(rec, ensure_ascii=False, sort_keys=True))
    print(f"# {len(records)} record(s)", file=sys.stderr)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    consumer = Consumer(args.root)
    print_json(
        [
            {
                "dataset": v.dataset,
                "version": v.version,
                "manifest_digest": v.manifest_digest,
                "content_digest": v.content_digest,
                "source": v.source,
                "installed_at": v.installed_at,
                "bytes": v.total_bytes,
            }
            for v in consumer.installed()
        ]
    )
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    print_json(ReleaseClient(args.release).health())
    return 0


def print_json(obj: Any) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="segpkg.consumer")
    parser.add_argument("--root", default="consumer", help="consumer state directory")
    parser.add_argument("--release", default="http://127.0.0.1:8000",
                        help="release service base URL")
    sub = parser.add_subparsers(dest="command", required=True)

    common_dataset = argparse.ArgumentParser(add_help=False)
    common_dataset.add_argument("--dataset", required=True)

    i = sub.add_parser("install", parents=[common_dataset])
    i.add_argument("--version", required=True)
    i.set_defaults(func=cmd_install)

    p = sub.add_parser("plan", parents=[common_dataset])
    p.add_argument("--version", required=True)
    p.add_argument("--prefer", choices=["auto", "diff", "full"], default="auto")
    p.set_defaults(func=cmd_plan)

    u = sub.add_parser("upgrade", parents=[common_dataset])
    u.add_argument("--version", required=True)
    u.add_argument("--prefer", choices=["auto", "diff", "full"], default="auto")
    u.add_argument("--max-blobs", type=int, default=None,
                   help="testing hook: abort after N network downloads")
    u.set_defaults(func=cmd_upgrade)

    v = sub.add_parser("verify", parents=[common_dataset])
    v.add_argument("--version", required=True)
    v.set_defaults(func=cmd_verify)

    q = sub.add_parser("query", parents=[common_dataset])
    q.add_argument("--version", default=None)
    q.add_argument("--category", default=None)
    q.add_argument("--min-score", type=float, default=None)
    q.add_argument("--text", default=None)
    q.add_argument("--limit", type=int, default=None)
    q.add_argument("--all-versions", action="store_true")
    q.set_defaults(func=cmd_query)

    l = sub.add_parser("list")
    l.set_defaults(func=cmd_list)

    h = sub.add_parser("health")
    h.set_defaults(func=cmd_health)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (SegpkgError, CorruptLocalError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
