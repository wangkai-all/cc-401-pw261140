# 分段数据包发布 (Segmented Delivery)

A complete, dependency-free implementation of **segmented data package
publishing**: a builder, a release directory, a read-only release service, and a
consumer agent that downloads only changed segments, verifies everything against
an independently built full package, and recovers safely from a missing or
damaged base version.

Everything is real files and real HTTP. There is no in-memory simulation in the
delivery path.

```
┌──────────────┐   build/    ┌─────────────────┐   publish   ┌──────────────────┐
│  builder     │ ─────────▶  │  build dir      │ ─────────▶  │  release dir     │
│  (process 1) │             │  manifest.json  │             │  blobs/ + index  │
└──────────────┘             │  segments/      │             └────────┬─────────┘
                             └─────────────────┘                      │ read-only
                                                                      ▼
┌──────────────┐    HTTP     ┌─────────────────┐             ┌──────────────────┐
│  consumer    │ ◀─────────  │ release service │ ◀────────── │  (same directory)│
│  (process 3) │             │  (process 2)    │             └──────────────────┘
└──────┬───────┘             └─────────────────┘
       │ local blobs/, versions/, staging/
       ▼
  query through installed manifests
```

## Quick start

```bash
# 1. Web console (publishes a demo v1-v3 on first run)
python3 -m segpkg.server console --workdir runtime/console --port 8080 --seed
# open http://127.0.0.1:8080/

# 2. Full CLI demonstration (builder, release service, consumer, resume, GC)
./scripts/run_demo.sh runtime/cli-demo

# 3. Tests
python3 -m unittest discover -s tests -v
```

No third-party packages are required; Python 3.10+ standard library only.

## What the requirements map to

| Requirement | Where |
| --- | --- |
| Data product published as segment files; a new version changes few segments | `segpkg/segments.py` — stable hash sharding; one edited record touches one segment |
| A diff package must declare the baseline it depends on | `DiffManifest.base` (version, seq, **manifest digest**); `apply_diff` refuses a mismatched base |
| Consumer must not apply a patch when the old segment is missing | `upgrade._apply_diff_path` verifies every reused base blob byte-for-byte before committing; missing base → `refetch-base` or `full` |
| Full-package fallback and missing-segment refetch have different costs | `upgrade.plan_upgrade` prices both and picks the cheaper, unless `prefer=full/diff` |
| The selected path must prove the final content equals an independent full build | `Manifest.content_digest` (independent of build time) + `_verify_result` re-hashes every installed segment |
| Web UI: publish, skip a version, damage an old segment, choose a recovery path | `web/` + `segpkg/console.py` |
| Interrupted download resumes without mixing two versions | `consumer.staging/` per target; blobs move to `blobs/` only after hash verification; `install_manifest` is the single commit point |
| Truly empty directory → actual file generation, transfer, consumer query | `builder.build_package`, `ReleaseStore.publish`, `ReleaseServer`, `Consumer.query` |
| Old-version cleanup must not break content still referenced by diff chains | `ReleaseStore.gc` — removes diffs whose base is dropped, keeps every blob referenced by a retained manifest or retained diff, self-checks the result |

## File formats

**Manifest** (`versions/<v>/manifest.json`, canonical JSON):

```json
{
  "format": "segpkg-manifest/1",
  "dataset": "catalog", "version": "v3", "seq": 3,
  "created_at": "2026-11-17T02:04:43Z",
  "segments": [
    {"name": "part-00", "digest": "<sha256>", "size": 2519, "count": 9}
  ]
}
```

* `manifest_digest` — SHA-256 of the whole document (pins a build).
* `content_digest` — SHA-256 of `{dataset, segments}` only. A package obtained by
  applying a diff has the same `content_digest` as the independently published
  full package; this is the equality the consumer verifies.

**Diff** (`versions/<target>/diffs/<base>.json`):

```json
{
  "format": "segpkg-diff/1",
  "base":   {"version": "v1", "seq": 1, "manifest_digest": "<sha256>"},
  "target": { ...full target manifest... },
  "segments": [
    {"name": "part-00", "op": "unchanged", "digest": "<d>", "base_digest": "<d>", "size": 2519, "count": 9},
    {"name": "part-02", "op": "replace",   "digest": "<new>", "base_digest": "<old>", "size": 2600, "count": 9},
    {"name": "part-07", "op": "add",       "digest": "<new>", "size": 300, "count": 1},
    {"name": "part-09", "op": "remove",    "digest": "<old>", "size": 2500, "count": 9}
  ]
}
```

The diff carries the entire target manifest rather than asking the consumer to
guess it, so reconstruction is an equality check, not a heuristic. `unchanged`
segments are *not* included in the package bytes; the consumer reuses its local
blob only after re-hashing it.

## Recovery paths and their costs

| Situation | Plan | Transfer |
| --- | --- | --- |
| exact base installed, healthy | `diff` | only changed segments |
| base has a damaged/missing segment the diff reuses | `refetch-base` | changed segments + repair of only the reused ones (chosen only if cheaper than full) |
| no direct diff, but a chain of published diffs exists | `chain` | sum of the hops |
| no usable base, repair is impossible/withdrawn | `full` | whole package |
| diff fails verification at apply time | `full` | whole package; the failure is reported, never papered over |

`prefer=diff` turns "no applicable diff" into an error instead of a silent full
download. `prefer=auto` chooses the cheapest option that can be verified.

## Interruption and resumption

* Downloads land in `staging/<dataset>-<target>/blobs/...` and are verified
  before being accepted.
* `blobs/` only ever receives fully verified blobs (`os.replace`, fsync).
* `versions/<v>/manifest.json` is written last: a version is visible to queries
  only after every one of its blobs is present and correct.
* A resumed upgrade re-reads staging, reuses already-verified blobs, and re-runs
  the same final `_verify_result`. Two versions can never be mixed because the
  commit is a single manifest write and every segment is content-addressed.

Simulate with `--max-blobs N` (CLI) or the *中断模拟* selector in the web console.

## Retention

`builder gc --dataset catalog --keep-last 2 [--dry-run]`

* Versions to be removed are dropped along with any diff that names them as a
  base (those diffs are unusable once the base is gone).
* A blob is deleted only if no retained manifest and no retained diff target
  references it — across **all** datasets in the release root, since blobs are
  content-addressed and shared.
* After pruning, GC re-checks that no retained diff has a dangling base and no
  retained manifest has a missing blob; it raises rather than leave the release
  inconsistent.

## Layout

```
segpkg/
  segments.py       manifest, diff, sharding, apply_diff
  release_store.py  on-disk release, atomic publish, gc, verify
  release_server.py read-only HTTP API (+ SSE)
  client.py         digest-verifying HTTP fetcher
  consumer.py       local state, staging, commit, query
  upgrade.py        planning, recovery choice, apply, final verification
  builder.py        build/publish/verify/gc CLI
  consumer_cli.py   install/plan/upgrade/verify/query CLI
  console.py        web console backend (same code paths as the CLIs)
  demo.py           scripted end-to-end scenario
  server.py         entry point for console / standalone release service
web/                console UI (vanilla HTML/CSS/JS)
tests/              integration tests (real files + real HTTP)
scripts/run_demo.sh three-process CLI walkthrough
```

## Test coverage

`tests/test_delivery.py` covers manifest/diff invariants, tampered artifacts,
the HTTP surface, all recovery paths, interruption/resume, retention safety
(including blobs shared between datasets), and the console actions. Tests build
real directories under a temp root and assert the installed bytes hash to the
independent full build.
