"""Entry point for the web console and the standalone release service.

    python -m segpkg.server console   --workdir runtime/console --port 8080
    python -m segpkg.server release   --release runtime/console/release --port 8090
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .console import ConsoleServer, Workspace
from .release_server import ReleaseServer
from .release_store import ReleaseStore
from .util import SegpkgError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="segpkg.server")
    sub = parser.add_subparsers(dest="command", required=True)

    c = sub.add_parser("console", help="web console with an interactive demo workspace")
    c.add_argument("--workdir", default="runtime/console")
    c.add_argument("--host", default="127.0.0.1")
    c.add_argument("--port", type=int, default=8080)
    c.add_argument("--dataset", default="catalog")
    c.add_argument("--seed", action="store_true", help="publish v1-v3 immediately")

    r = sub.add_parser("release", help="standalone read-only release service")
    r.add_argument("--release", required=True)
    r.add_argument("--host", default="127.0.0.1")
    r.add_argument("--port", type=int, default=8090)
    r.add_argument("--verbose", action="store_true")

    args = parser.parse_args(argv)
    try:
        if args.command == "console":
            workspace = Workspace(Path(args.workdir), dataset=args.dataset)
            if args.seed and not workspace.store.list_datasets():
                workspace.publish(count=3, edits=1)
            server = ConsoleServer(workspace, host=args.host, port=args.port)
            print(f"console: {server.base_url}/", flush=True)
            print(f"release API: {server.base_url}/api/health", flush=True)
            server.serve_forever()
            return 0
        store = ReleaseStore(args.release)
        server = ReleaseServer(store, host=args.host, port=args.port, verbose=args.verbose)
        print(f"release service: {server.base_url}", flush=True)
        server.serve_forever()
        return 0
    except KeyboardInterrupt:
        return 0
    except SegpkgError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
