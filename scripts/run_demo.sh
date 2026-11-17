#!/usr/bin/env bash
# Full three-process demonstration using the CLIs and real HTTP.
#
#   ./scripts/run_demo.sh [workdir]
#
# Build process  -> writes build dirs and publishes into the release directory
# Release process-> serves the release directory over HTTP (separate process)
# Consumer process-> installs, damages, upgrades, resumes, queries
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${1:-$ROOT/runtime/cli-demo}"
export PYTHONPATH="$ROOT"

PORT="${PORT:-8812}"
RELEASE="$WORK/release"
SRC="$WORK/sources"

rm -rf "$WORK"
mkdir -p "$SRC" "$RELEASE"

step() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
field() { python3 -c "import json,sys; d=json.load(sys.stdin); print('$1', d$2)"; }

step "1. builder: generate and build v1..v3 (separate from the release service)"
python3 - "$SRC" <<'PY'
import json, sys
from pathlib import Path
from segpkg.fixtures import DatasetSpec, DatasetVariant
from segpkg import builder

src = Path(sys.argv[1])
variant = None
for n in (1, 2, 3):
    spec = DatasetSpec("catalog", f"v{n}", record_count=60)
    if variant is None:
        variant = DatasetVariant(spec)
    else:
        variant.spec = spec
        variant.touch(3 if n == 2 else 17, f"cli-edit-{n}")
        if n >= 3:
            variant.append(1, f"cli-append-{n}")
    path = src / f"catalog-v{n}.jsonl"
    variant.write(path)
    builder.main(["build", "--dataset", "catalog", "--version", f"v{n}",
                  "--source", str(path), "--out", str(src / "builds" / f"v{n}")])
PY

step "2. publisher: publish v1, then v2 and v3 with diffs from all earlier versions"
python3 -m segpkg.builder publish --build "$SRC/builds/v1" --release "$RELEASE"
for v in v2 v3; do
  python3 -m segpkg.builder publish --build "$SRC/builds/$v" --release "$RELEASE" \
    --base-version v1 $( [ "$v" = v3 ] && echo --base-version v2 )
done

step "3. release service: separate process on :$PORT"
python3 -m segpkg.server release --release "$RELEASE" --port "$PORT" > "$WORK/release.log" 2>&1 &
REL_PID=$!
trap 'kill $REL_PID 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$PORT/api/health" >/dev/null && break; sleep 0.1; done
curl -s "http://127.0.0.1:$PORT/api/datasets" | python3 -m json.tool

REL="http://127.0.0.1:$PORT"
C="$WORK/consumer"
CC="python3 -m segpkg.consumer_cli --root $C --release $REL"

step "4. consumer: install v1 in full"
$CC install --dataset catalog --version v1 | field "downloaded_bytes:" "['downloaded_bytes']"

step "5. consumer: damage one segment the v1->v3 diff reuses"
python3 - "$C" <<'PY'
import sys
from pathlib import Path
from segpkg.consumer import Consumer
c = Consumer(Path(sys.argv[1]))
m = c.require("catalog", "v1")
name = m.segment_names()[0]
d = m.segments[name].digest
p = c.blob_path(d)
b = bytearray(p.read_bytes()); b[0] ^= 0xFF; p.write_bytes(bytes(b))
print(f"corrupted {name} ({d[:12]})")
PY

step "6. consumer: plan v1 -> v3"
$CC plan --dataset catalog --version v3 | python3 -c "
import json,sys
p=json.load(sys.stdin); s=p['selected']
print('selected:', s['kind'], '| bytes:', s['download_bytes'], '| segments:', s['download_segments'])
for r in s['reasons']: print('  -', r)
for w in s['warnings']: print('  !', w)
for o in p['options']:
    print('  option:', o['kind'], o['download_bytes'], 'base', o['base_version'])
"

step "7. consumer: upgrade (repairs base, patches, verifies final content)"
$CC upgrade --dataset catalog --version v3 | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('strategy:', d['strategy_used'], '| bytes:', d['downloaded_bytes'],
      '| plan segments:', d['plan_segments'], '| verified:', d['verified'])
print('content digest:', d['content_digest'])
"
$CC verify --dataset catalog --version v3

step "8. consumer: interrupt a transfer mid-way, then resume"
$CC install --dataset catalog --version v1 --root "$WORK/consumer2" 2>/dev/null || \
  python3 -m segpkg.consumer_cli --root "$WORK/consumer2" --release "$REL" \
    install --dataset catalog --version v1 >/dev/null
set +e
python3 -m segpkg.consumer_cli --root "$WORK/consumer2" --release "$REL" \
  upgrade --dataset catalog --version v3 --max-blobs 1 > "$WORK/interrupt.json" 2>&1
echo "interrupted (expected): $(tail -1 "$WORK/interrupt.json")"
set -e
echo "staged blobs kept: $(find "$WORK/consumer2/staging" -name '*.blob' | wc -l)"
echo "installed versions: $(python3 -m segpkg.consumer_cli --root "$WORK/consumer2" --release "$REL" list | python3 -c "import json,sys;print([v['version'] for v in json.load(sys.stdin)])")"
python3 -m segpkg.consumer_cli --root "$WORK/consumer2" --release "$REL" \
  upgrade --dataset catalog --version v3 | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('resumed:', d['strategy_used'], '| downloaded now:', d['downloaded_bytes'],
      '| reused segments:', d['reused_segments'], '| verified:', d['verified'])
"

step "9. consumer: query through the installed segments"
$CC query --dataset catalog --category alpha --limit 3

step "10. verify the release directory and prune, keeping diff chains intact"
python3 -m segpkg.builder verify --release "$RELEASE"
python3 -m segpkg.builder gc --release "$RELEASE" --dataset catalog --keep-last 1 --dry-run
python3 -m segpkg.builder verify --release "$RELEASE"

step "done"
echo "release dir: $RELEASE"
