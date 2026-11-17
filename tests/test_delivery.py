"""End-to-end tests for the segmented delivery system.

These tests use real files, a real HTTP server, and real consumer directories.
Run with::

    python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from segpkg import builder
from segpkg.client import ReleaseClient, ReleaseClientError
from segpkg.consumer import Consumer, CorruptLocalError
from segpkg.demo import build_and_publish, make_version_chain
from segpkg.fixtures import DatasetSpec, DatasetVariant
from segpkg.release_server import ReleaseServer
from segpkg.release_store import ReleaseStore
from segpkg.segments import (
    DiffManifest,
    Manifest,
    apply_diff,
    build_segments,
    make_diff,
)
from segpkg.upgrade import (
    DownloadAborted,
    apply_plan,
    plan_upgrade,
    upgrade,
)
from segpkg.util import SegpkgError, now_iso, sha256_file


class TempDirTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="segpkg-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def build(self, release: Path, sources: Path, count: int = 3, dataset: str = "catalog"):
        return build_and_publish(release, sources, dataset=dataset, count=count)


class ManifestTests(TempDirTest):
    def test_manifest_roundtrip_is_canonical(self):
        variant = DatasetVariant(DatasetSpec("catalog", "v1"))
        manifest = Manifest(
            "catalog", "v1", 1, now_iso(), build_segments(variant.records)
        )
        raw = manifest.to_bytes()
        parsed = Manifest.from_bytes(raw)
        self.assertEqual(parsed.to_bytes(), raw)
        self.assertEqual(parsed.digest, manifest.digest)
        self.assertEqual(parsed.content_digest, manifest.content_digest)

    def test_content_digest_ignores_build_time(self):
        variant = DatasetVariant(DatasetSpec("catalog", "v1"))
        segments = build_segments(variant.records)
        a = Manifest("catalog", "v1", 1, "2024-01-01T00:00:00Z", segments)
        b = Manifest("catalog", "v1", 1, "2026-11-17T09:00:00Z", segments)
        self.assertNotEqual(a.digest, b.digest)
        self.assertEqual(a.content_digest, b.content_digest)

    def test_manifest_rejects_bad_digest(self):
        variant = DatasetVariant(DatasetSpec("catalog", "v1"))
        manifest = Manifest(
            "catalog", "v1", 1, now_iso(), build_segments(variant.records)
        )
        obj = manifest.to_dict()
        obj["segments"][0]["digest"] = "not-a-digest"
        with self.assertRaises(SegpkgError):
            Manifest.from_dict(obj)

    def test_sharding_is_stable_and_local(self):
        variant = DatasetVariant(DatasetSpec("catalog", "v1"))
        before = build_segments(variant.records)
        variant2 = DatasetVariant(DatasetSpec("catalog", "v1"))
        variant2.records = copy.deepcopy(variant.records)
        variant2.touch(5, "one-change")
        after = build_segments(variant2.records)
        changed = [
            name for name in before if before[name].digest != after[name].digest
        ]
        self.assertEqual(len(changed), 1, f"expected exactly one changed segment, got {changed}")


class DiffTests(TempDirTest):
    def setUp(self) -> None:
        super().setUp()
        self.v1 = DatasetVariant(DatasetSpec("catalog", "v1"))
        self.m1 = Manifest("catalog", "v1", 1, now_iso(), build_segments(self.v1.records))
        self.v2 = DatasetVariant(DatasetSpec("catalog", "v2"))
        self.v2.records = copy.deepcopy(self.v1.records)
        self.v2.touch(2, "small-edit")
        self.m2 = Manifest("catalog", "v2", 2, now_iso(), build_segments(self.v2.records))

    def test_diff_is_small_and_reconstructs_target(self):
        diff = make_diff(self.m1, self.m2, now_iso())
        self.assertEqual(diff.download_segments, 1)
        self.assertLess(diff.download_bytes, self.m2.total_bytes // 5)
        rebuilt = apply_diff(self.m1, DiffManifest.from_bytes(diff.to_bytes()))
        self.assertEqual(rebuilt.to_bytes(), self.m2.to_bytes())
        self.assertEqual(rebuilt.content_digest, self.m2.content_digest)

    def test_diff_refuses_wrong_base_manifest(self):
        diff = make_diff(self.m1, self.m2, now_iso())
        other = DatasetVariant(DatasetSpec("catalog", "v1"))
        other.records = copy.deepcopy(self.v1.records)
        other.touch(0, "different")
        wrong = Manifest("catalog", "v1", 1, now_iso(), build_segments(other.records))
        with self.assertRaisesRegex(SegpkgError, "different base"):
            apply_diff(wrong, diff)

    def test_diff_refuses_missing_unchanged_segment(self):
        diff = make_diff(self.m1, self.m2, now_iso())
        unchanged_name = diff.unchanged[0]["name"]
        partial = Manifest(
            "catalog", "v1", 1, self.m1.created_at,
            {k: v for k, v in self.m1.segments.items() if k != unchanged_name},
        )
        with self.assertRaises(SegpkgError):
            apply_diff(partial, diff)

    def test_diff_that_does_not_reconstruct_target_is_rejected(self):
        diff = make_diff(self.m1, self.m2, now_iso())
        # Build a diff that keeps a valid op list but claims a different (valid)
        # target: reconstruction from base + ops must not match it.
        other = DatasetVariant(DatasetSpec("catalog", "v2"))
        other.records = copy.deepcopy(self.v1.records)
        other.touch(9, "other")
        forged_target = Manifest(
            "catalog", "v2", 2, diff.target_manifest.created_at,
            build_segments(other.records),
        )
        forged = DiffManifest(
            dataset="catalog",
            base_version=diff.base_version,
            base_seq=diff.base_seq,
            base_manifest_digest=diff.base_manifest_digest,
            target_manifest=forged_target,
            created_at=diff.created_at,
        )
        forged._stored_ops = diff.segments
        with self.assertRaises(SegpkgError):
            apply_diff(self.m1, forged)

    def test_tampered_diff_is_rejected_on_parse(self):
        diff = make_diff(self.m1, self.m2, now_iso())
        obj = diff.to_dict()
        # Flip an operation to a different digest while keeping the target.
        for entry in obj["segments"]:
            if entry["op"] == "replace":
                entry["digest"] = "0" * 64
                break
        with self.assertRaisesRegex(SegpkgError, "disagrees with target"):
            DiffManifest.from_dict(obj)


class PublishTests(TempDirTest):
    def test_publish_creates_real_files_and_index(self):
        release = self.tmp / "release"
        sources = self.tmp / "sources"
        published = self.build(release, sources)
        self.assertEqual([p["version"] for p in published], ["v1", "v2", "v3"])
        index = json.loads((release / "datasets/catalog/index.json").read_text())
        self.assertEqual([v["version"] for v in index["versions"]], ["v1", "v2", "v3"])
        blobs = list((release / "blobs/sha256").glob("*/*.blob"))
        self.assertGreater(len(blobs), 10)
        for blob in blobs:
            self.assertIn(blob.stem, sha256_file(blob))

    def test_versions_are_immutable(self):
        release = self.tmp / "release"
        sources = self.tmp / "sources"
        self.build(release, sources, count=1)
        store = ReleaseStore(release)
        manifest, payloads = builder.read_build(sources / "builds" / "v1")
        with self.assertRaisesRegex(SegpkgError, "immutable"):
            store.publish(manifest, payloads)

    def test_publish_rejects_mismatched_payload(self):
        release = self.tmp / "release"
        sources = self.tmp / "sources"
        self.build(release, sources, count=1)
        store = ReleaseStore(release)
        variant = DatasetVariant(DatasetSpec("catalog", "v9"))
        manifest = Manifest("catalog", "v9", 9, now_iso(), build_segments(variant.records))
        payloads = {name: b"wrong" for name in manifest.segments}
        with self.assertRaises(SegpkgError):
            store.publish(manifest, payloads)

    def test_verify_detects_missing_blob(self):
        release = self.tmp / "release"
        sources = self.tmp / "sources"
        self.build(release, sources, count=1)
        store = ReleaseStore(release)
        self.assertTrue(store.verify("catalog")["ok"])
        manifest, _ = store.manifest("catalog", "v1")
        victim = store.blob_path(next(iter(manifest.segments.values())).digest)
        victim.unlink()
        result = store.verify("catalog")
        self.assertFalse(result["ok"])
        self.assertTrue(any("missing blob" in p for p in result["problems"]))

    def test_verify_detects_corrupt_blob(self):
        release = self.tmp / "release"
        sources = self.tmp / "sources"
        self.build(release, sources, count=1)
        store = ReleaseStore(release)
        manifest, _ = store.manifest("catalog", "v1")
        victim = store.blob_path(next(iter(manifest.segments.values())).digest)
        data = bytearray(victim.read_bytes())
        data[0] ^= 0xFF
        victim.write_bytes(bytes(data))
        result = store.verify("catalog")
        self.assertFalse(result["ok"])
        self.assertTrue(any("corrupt blob" in p for p in result["problems"]))


class HttpTests(TempDirTest):
    def setUp(self) -> None:
        super().setUp()
        self.release = self.tmp / "release"
        self.sources = self.tmp / "sources"
        self.build(self.release, self.sources)
        self.store = ReleaseStore(self.release)
        self.server = ReleaseServer(self.store).start_background()
        self.addCleanup(self.server.shutdown)
        self.client = ReleaseClient(self.server.base_url)

    def test_health_and_dataset_index(self):
        self.assertTrue(self.client.health()["ok"])
        detail = self.client.dataset("catalog")
        self.assertEqual([v["version"] for v in detail["versions"]], ["v1", "v2", "v3"])
        self.assertIn("v1", detail["versions"][1]["diffs"])

    def test_manifest_and_blob_roundtrip(self):
        raw = self.client.manifest_bytes("catalog", "v2")
        manifest = Manifest.from_bytes(raw)
        for seg in manifest.segments.values():
            data = self.client.blob(seg.digest)
            self.assertEqual(len(data), seg.size)

    def test_unknown_routes_are_404(self):
        with self.assertRaises(ReleaseClientError):
            self.client.dataset("nope")
        with self.assertRaises(ReleaseClientError):
            self.client.manifest_bytes("catalog", "v99")

    def test_corrupt_release_blob_is_rejected(self):
        manifest = Manifest.from_bytes(self.client.manifest_bytes("catalog", "v1"))
        seg = next(iter(manifest.segments.values()))
        path = self.store.blob_path(seg.digest)
        data = bytearray(path.read_bytes())
        data[0] ^= 0xFF
        path.write_bytes(bytes(data))
        with self.assertRaises(ReleaseClientError):
            self.client.blob(seg.digest)


class ConsumerFlowTests(TempDirTest):
    def setUp(self) -> None:
        super().setUp()
        self.release = self.tmp / "release"
        self.sources = self.tmp / "sources"
        self.build(self.release, self.sources)
        self.store = ReleaseStore(self.release)
        self.server = ReleaseServer(self.store).start_background()
        self.addCleanup(self.server.shutdown)
        self.client = ReleaseClient(self.server.base_url)
        self.consumer = Consumer(self.tmp / "c1")

    def install(self, version: str) -> dict:
        from segpkg.upgrade import Plan, Strategy

        manifest = Manifest.from_bytes(self.client.manifest_bytes("catalog", version))
        strategy = Strategy("full", len(manifest.segments), manifest.total_bytes)
        plan = Plan("catalog", version, manifest, [strategy], strategy,
                    manifest.content_digest)
        return apply_plan(self.consumer, self.client, plan)

    def test_full_install_then_diff_upgrade(self):
        first = self.install("v1")
        self.assertEqual(first["downloaded_bytes"], first["plan"]["target_bytes"])
        result = upgrade(self.consumer, self.client, "catalog", "v2")
        self.assertEqual(result["strategy_used"], "diff")
        self.assertLess(result["downloaded_bytes"], result["plan"]["target_bytes"] // 4)
        self.assertTrue(result["verified"])
        self.assertEqual(
            self.consumer.require("catalog", "v2").content_digest,
            Manifest.from_bytes(self.client.manifest_bytes("catalog", "v2")).content_digest,
        )

    def test_skip_version_uses_chain(self):
        self.install("v1")
        result = upgrade(self.consumer, self.client, "catalog", "v3")
        self.assertIn(result["strategy_used"], ("diff", "chain"))
        self.assertTrue(result["verified"])
        self.assertEqual(len(result["hops"]), 1 if result["strategy_used"] == "diff" else 2)

    def test_chain_when_no_direct_diff_is_published(self):
        # Re-publish a release where only the previous version has a diff.
        release = self.tmp / "chain-release"
        sources = self.tmp / "chain-sources"
        manifests = make_version_chain(sources, count=4)
        store = ReleaseStore(release)
        previous = None
        for manifest in manifests:
            payloads = builder.read_build(sources / "builds" / manifest.version)[1]
            store.publish(
                manifest, payloads,
                make_diffs_from=[previous] if previous else [],
            )
            previous = manifest
        server = ReleaseServer(store).start_background()
        self.addCleanup(server.shutdown)
        client = ReleaseClient(server.base_url)
        consumer = Consumer(self.tmp / "chain-consumer")
        from segpkg.upgrade import Plan, Strategy

        v1 = Manifest.from_bytes(client.manifest_bytes("catalog", "v1"))
        strategy = Strategy("full", len(v1.segments), v1.total_bytes)
        apply_plan(consumer, client, Plan("catalog", "v1", v1, [strategy], strategy,
                                         v1.content_digest))
        plan = plan_upgrade(consumer, client, "catalog", "v4")
        self.assertEqual(plan.selected.kind, "chain")
        self.assertEqual(
            [(s.base_version, s.target_version) for s in plan.selected.steps],
            [("v1", "v2"), ("v2", "v3"), ("v3", "v4")],
        )
        result = upgrade(consumer, client, "catalog", "v4")
        self.assertTrue(result["verified"])
        self.assertLess(result["downloaded_bytes"], result["plan"]["target_bytes"])
        installed = consumer.require("catalog", "v4")
        self.assertEqual(installed.content_digest, manifests[-1].content_digest)

    def test_chain_can_start_from_a_damaged_base(self):
        release = self.tmp / "chain2-release"
        sources = self.tmp / "chain2-sources"
        manifests = make_version_chain(sources, count=4)
        store = ReleaseStore(release)
        previous = None
        for manifest in manifests:
            payloads = builder.read_build(sources / "builds" / manifest.version)[1]
            store.publish(manifest, payloads,
                          make_diffs_from=[previous] if previous else [])
            previous = manifest
        server = ReleaseServer(store).start_background()
        self.addCleanup(server.shutdown)
        client = ReleaseClient(server.base_url)
        consumer = Consumer(self.tmp / "chain2-consumer")
        from segpkg.upgrade import Plan, Strategy

        v1 = Manifest.from_bytes(client.manifest_bytes("catalog", "v1"))
        strategy = Strategy("full", len(v1.segments), v1.total_bytes)
        apply_plan(consumer, client, Plan("catalog", "v1", v1, [strategy], strategy,
                                         v1.content_digest))
        # Damage one blob in the only installed base.
        digest = next(iter(v1.segments.values())).digest
        path = consumer.blob_path(digest)
        data = bytearray(path.read_bytes())
        data[0] ^= 0xFF
        path.write_bytes(bytes(data))
        plan = plan_upgrade(consumer, client, "catalog", "v4")
        self.assertEqual(plan.selected.kind, "chain")
        self.assertTrue(plan.selected.warnings)
        result = upgrade(consumer, client, "catalog", "v4")
        self.assertTrue(result["verified"])
        self.assertEqual(
            consumer.require("catalog", "v4").content_digest,
            manifests[-1].content_digest,
        )

    def test_removed_installed_version_falls_back_to_full(self):
        self.install("v1")
        # Remove the installed version record but leave its blobs behind: the
        # consumer no longer has a base it can prove anything about.
        shutil.rmtree(self.consumer.version_dir("catalog", "v1"))
        self.assertIsNone(self.consumer.get("catalog", "v1"))
        plan = plan_upgrade(self.consumer, self.client, "catalog", "v2")
        self.assertEqual(plan.selected.kind, "full")
        result = upgrade(self.consumer, self.client, "catalog", "v2")
        self.assertTrue(result["verified"])

    def test_diff_preference_fails_without_base(self):
        with self.assertRaises(SegpkgError):
            plan_upgrade(self.consumer, self.client, "catalog", "v2", prefer="diff")

    def test_corrupt_base_segment_is_repaired_or_refused(self):
        self.install("v1")
        manifest = self.consumer.require("catalog", "v1")
        diff = DiffManifest.from_bytes(self.client.diff_bytes("catalog", "v3", "v1"))
        unchanged = next(e for e in diff.segments if e["op"] == "unchanged")
        digest = manifest.segments[unchanged["name"]].digest
        path = self.consumer.blob_path(digest)
        data = bytearray(path.read_bytes())
        data[0] ^= 0xFF
        path.write_bytes(bytes(data))
        plan = plan_upgrade(self.consumer, self.client, "catalog", "v3")
        self.assertEqual(plan.selected.kind, "refetch-base")
        result = upgrade(self.consumer, self.client, "catalog", "v3")
        self.assertTrue(result["verified"])
        self.assertTrue(self.consumer.blob_ok(digest))

    def test_deleted_base_segment_is_refetched(self):
        self.install("v1")
        manifest = self.consumer.require("catalog", "v1")
        diff = DiffManifest.from_bytes(self.client.diff_bytes("catalog", "v3", "v1"))
        unchanged = next(e for e in diff.segments if e["op"] == "unchanged")
        digest = manifest.segments[unchanged["name"]].digest
        self.consumer.blob_path(digest).unlink()
        result = upgrade(self.consumer, self.client, "catalog", "v3")
        self.assertEqual(result["strategy_used"], "refetch-base")
        self.assertTrue(result["verified"])

    def test_refetch_disallowed_falls_back_to_full(self):
        self.install("v1")
        manifest = self.consumer.require("catalog", "v1")
        diff = DiffManifest.from_bytes(self.client.diff_bytes("catalog", "v3", "v1"))
        unchanged = next(e for e in diff.segments if e["op"] == "unchanged")
        digest = manifest.segments[unchanged["name"]].digest
        self.consumer.blob_path(digest).unlink()
        plan = plan_upgrade(
            self.consumer, self.client, "catalog", "v3", allow_refetch_base=False
        )
        self.assertEqual(plan.selected.kind, "full")

    def test_planned_diff_that_fails_at_apply_time_falls_back_to_full(self):
        self.install("v1")
        target = Manifest.from_bytes(self.client.manifest_bytes("catalog", "v2"))
        plan = plan_upgrade(self.consumer, self.client, "catalog", "v2")
        self.assertEqual(plan.selected.kind, "diff")
        # The base disappears between planning and applying: the applier must
        # not force the patch; it degrades to a verified full install.
        shutil.rmtree(self.consumer.version_dir("catalog", "v1"))
        result = apply_plan(self.consumer, self.client, plan)
        self.assertEqual(result["strategy_used"], "full")
        self.assertTrue(result["fell_back"])
        self.assertIn("catalog/v1 is not installed", result["fallback_reason"])
        self.assertEqual(result["diff_failure"], result["fallback_reason"])
        self.assertTrue(result["verified"])
        installed = self.consumer.require("catalog", "v2")
        self.assertEqual(installed.content_digest, target.content_digest)
        self.assertEqual(len(self.consumer.query("catalog")), target.total_records)
        events = [e for e in self.consumer.state()["events"] if e["type"] == "diff-failed"]
        self.assertTrue(events, "the failed diff attempt must be recorded")

    def test_quit_during_download_resumes_without_mixing(self):
        self.install("v1")
        v1_digests = {
            s.digest for s in self.consumer.require("catalog", "v1").segments.values()
        }
        with self.assertRaises(DownloadAborted):
            upgrade(self.consumer, self.client, "catalog", "v3", max_blobs=2)
        # Nothing new became visible; v3 is not installed.
        self.assertIsNone(self.consumer.get("catalog", "v3"))
        self.assertIsNotNone(self.consumer.get("catalog", "v1"))
        staged = list((self.consumer.staging_dir("catalog-v3")).glob("**/*.blob"))
        self.assertGreaterEqual(len(staged), 1)
        # Resume: already-staged bytes are reused and the result is verified.
        result = upgrade(self.consumer, self.client, "catalog", "v3")
        self.assertTrue(result["verified"])
        self.assertGreater(result["reused_segments"], 0)
        installed = self.consumer.require("catalog", "v3")
        self.assertEqual(
            installed.content_digest,
            Manifest.from_bytes(self.client.manifest_bytes("catalog", "v3")).content_digest,
        )

    def test_interrupted_download_never_installs_partial(self):
        self.install("v1")
        with self.assertRaises(DownloadAborted):
            upgrade(self.consumer, self.client, "catalog", "v3", max_blobs=0) if False else None
            upgrade(self.consumer, self.client, "catalog", "v3", max_blobs=0)
        self.assertEqual(
            [v.version for v in self.consumer.installed()], ["v1"]
        )

    def test_local_tamper_is_detected_by_query(self):
        self.install("v1")
        manifest = self.consumer.require("catalog", "v1")
        digest = next(iter(manifest.segments.values())).digest
        path = self.consumer.blob_path(digest)
        data = bytearray(path.read_bytes())
        data[0] ^= 0xFF
        path.write_bytes(bytes(data))
        with self.assertRaises(CorruptLocalError):
            self.consumer.query("catalog")

    def test_query_filters(self):
        self.install("v2")
        rows = self.consumer.query("catalog", category="alpha", limit=100)
        self.assertTrue(rows)
        self.assertTrue(all(r["category"] == "alpha" for r in rows))
        limited = self.consumer.query("catalog", limit=3)
        self.assertEqual(len(limited), 3)


class RetentionTests(TempDirTest):
    def setUp(self) -> None:
        super().setUp()
        self.release = self.tmp / "release"
        self.sources = self.tmp / "sources"
        self.build(self.release, self.sources, count=4)
        self.store = ReleaseStore(self.release)

    def test_gc_keeps_referenced_blobs_and_valid_diffs(self):
        before = set(self.store.versions("catalog")[0].__dict__)  # touch attribute
        result = self.store.gc("catalog", keep_last=2)
        self.assertEqual(result["removed_versions"], ["v1", "v2"])
        verify = self.store.verify("catalog")
        self.assertTrue(verify["ok"], verify["problems"])
        # Retained versions still have their diffs from "none" and between each
        # other if both bases survive.
        for info in self.store.versions("catalog"):
            for base in info.diffs:
                self.store.diff("catalog", info.version, base)
        # No dangling diff base remains.
        live = {v.version for v in self.store.versions("catalog")}
        for info in self.store.versions("catalog"):
            for base in info.diffs:
                self.assertTrue(base == "none" or base in live)

    def test_gc_refuses_to_break_a_retained_chain(self):
        # Keep v1 and v4 but drop v2/v3: the direct v1->v4 diff must survive
        # and the now-unusable v2/v3-based diffs must be removed.
        result = self.store.gc("catalog", keep_versions=["v1", "v4"])
        self.assertEqual(result["removed_versions"], ["v2", "v3"])
        self.assertTrue(self.store.verify("catalog")["ok"])
        kept = self.store.versions("catalog")
        for info in kept:
            self.assertEqual(
                set(info.diffs) - {"none", "v1"}, set(),
                f"{info.version} still lists a diff from a removed base",
            )
        # The v1->v4 diff still applies to the real v1.
        manifest1, _ = self.store.manifest("catalog", "v1")
        manifest4, _ = self.store.manifest("catalog", "v4")
        diff, _ = self.store.diff("catalog", "v4", "v1")
        self.assertEqual(apply_diff(manifest1, diff).to_bytes(), manifest4.to_bytes())

    def test_gc_does_not_delete_blobs_shared_with_another_dataset(self):
        # Two datasets with identical record content share every blob. Pruning
        # one dataset must never remove content the other still serves.
        release = self.tmp / "shared-release"
        sources = self.tmp / "shared-sources"
        build_and_publish(release, sources / "a", dataset="alpha", count=3)
        store = ReleaseStore(release)
        # Build the second dataset from the same records as alpha/v3.
        from segpkg.fixtures import DatasetSpec, DatasetVariant

        variant = DatasetVariant(DatasetSpec("alpha", "v3"))
        build_dir = sources / "b" / "builds" / "v3"
        manifest = builder.build_package(variant.records, "beta", "v3", build_dir)
        store.publish(manifest, builder.read_build(build_dir)[1])
        before = {p.stem for p in (release / "blobs").glob("**/*.blob")}
        store.gc("alpha", keep_last=1)
        self.assertTrue(store.verify("beta")["ok"])
        beta_manifest, _ = store.manifest("beta", "v3")
        for seg in beta_manifest.segments.values():
            self.assertTrue(store.has_blob(seg.digest))
        after = {p.stem for p in (release / "blobs").glob("**/*.blob")}
        self.assertTrue(before <= after or after <= before)

    def test_gc_dry_run_changes_nothing(self):
        before = {p for p in (self.release / "blobs").glob("**/*.blob")}
        result = self.store.gc("catalog", keep_last=1, dry_run=True)
        after = {p for p in (self.release / "blobs").glob("**/*.blob")}
        self.assertEqual(before, after)
        self.assertGreater(result["freed_bytes"], 0)
        self.assertEqual(len(self.store.versions("catalog")), 4)

    def test_consumer_with_removed_base_cannot_patch(self):
        server = ReleaseServer(self.store).start_background()
        self.addCleanup(server.shutdown)
        client = ReleaseClient(server.base_url)
        consumer = Consumer(self.tmp / "c1")
        from segpkg.upgrade import Plan, Strategy

        manifest = Manifest.from_bytes(client.manifest_bytes("catalog", "v3"))
        strategy = Strategy("full", len(manifest.segments), manifest.total_bytes)
        plan = Plan("catalog", "v3", manifest, [strategy], strategy, manifest.content_digest)
        apply_plan(consumer, client, plan)
        self.store.gc("catalog", keep_last=1)  # only v4 survives; v3's base is gone
        # The consumer's base version no longer exists upstream: must full fetch.
        plan = plan_upgrade(consumer, client, "catalog", "v4")
        self.assertEqual(plan.selected.kind, "full")
        result = upgrade(consumer, client, "catalog", "v4")
        self.assertTrue(result["verified"])


class RecoveryScenarioTests(TempDirTest):
    """The scenarios the web console exposes, exercised end to end."""

    def setUp(self) -> None:
        super().setUp()
        self.release = self.tmp / "release"
        self.sources = self.tmp / "sources"
        self.build(self.release, self.sources)
        self.store = ReleaseStore(self.release)
        self.server = ReleaseServer(self.store).start_background()
        self.addCleanup(self.server.shutdown)
        self.client = ReleaseClient(self.server.base_url)

    def install(self, consumer: Consumer, version: str) -> None:
        from segpkg.upgrade import Plan, Strategy

        manifest = Manifest.from_bytes(self.client.manifest_bytes("catalog", version))
        strategy = Strategy("full", len(manifest.segments), manifest.total_bytes)
        plan = Plan("catalog", version, manifest, [strategy], strategy,
                    manifest.content_digest)
        apply_plan(consumer, self.client, plan)

    def assert_matches_full_build(self, consumer: Consumer, version: str) -> None:
        manifest, _ = self.store.manifest("catalog", version)
        installed = consumer.require("catalog", version)
        self.assertEqual(installed.content_digest, manifest.content_digest)
        for name, seg in manifest.segments.items():
            self.assertEqual(
                sha256_file(consumer.blob_path(seg.digest)), seg.digest,
                f"segment {name} differs from the independent build",
            )

    def test_scenario_skip_one_release(self):
        consumer = Consumer(self.tmp / "skip")
        self.install(consumer, "v1")
        result = upgrade(consumer, self.client, "catalog", "v3")
        self.assertTrue(result["verified"])
        self.assert_matches_full_build(consumer, "v3")
        self.assertLess(result["downloaded_bytes"], result["plan"]["target_bytes"])

    def test_scenario_damaged_old_segment(self):
        consumer = Consumer(self.tmp / "damaged")
        self.install(consumer, "v1")
        manifest = consumer.require("catalog", "v1")
        diff = DiffManifest.from_bytes(self.client.diff_bytes("catalog", "v2", "v1"))
        unchanged = next(e for e in diff.segments if e["op"] == "unchanged")
        digest = manifest.segments[unchanged["name"]].digest
        path = consumer.blob_path(digest)
        data = bytearray(path.read_bytes())
        data[len(data) // 2] ^= 0xFF
        path.write_bytes(bytes(data))
        result = upgrade(consumer, self.client, "catalog", "v2")
        self.assertTrue(result["verified"])
        self.assertEqual(result["strategy_used"], "refetch-base")
        self.assert_matches_full_build(consumer, "v2")

    def test_scenario_missing_old_segment(self):
        consumer = Consumer(self.tmp / "missing")
        self.install(consumer, "v1")
        manifest = consumer.require("catalog", "v1")
        diff = DiffManifest.from_bytes(self.client.diff_bytes("catalog", "v2", "v1"))
        unchanged = next(e for e in diff.segments if e["op"] == "unchanged")
        digest = manifest.segments[unchanged["name"]].digest
        consumer.blob_path(digest).unlink()
        result = upgrade(consumer, self.client, "catalog", "v2")
        self.assertTrue(result["verified"])
        self.assert_matches_full_build(consumer, "v2")

    def test_scenario_full_fallback_when_diff_impossible(self):
        consumer = Consumer(self.tmp / "fallback")
        self.install(consumer, "v1")
        # Corrupt every unchanged segment needed by the v1->v3 diff and make the
        # release withdraw v1 so repair is impossible.
        manifest = consumer.require("catalog", "v1")
        for seg in manifest.segments.values():
            consumer.blob_path(seg.digest).unlink()
        self.store.gc("catalog", keep_last=2)  # drops v1
        result = upgrade(consumer, self.client, "catalog", "v3")
        self.assertTrue(result["verified"])
        self.assert_matches_full_build(consumer, "v3")

    def test_scenario_interrupted_then_resumed(self):
        consumer = Consumer(self.tmp / "resume")
        self.install(consumer, "v1")
        with self.assertRaises(DownloadAborted):
            upgrade(consumer, self.client, "catalog", "v3", max_blobs=1)
        self.assertEqual([v.version for v in consumer.installed()], ["v1"])
        result = upgrade(consumer, self.client, "catalog", "v3")
        self.assertTrue(result["verified"])
        self.assert_matches_full_build(consumer, "v3")

    def test_direct_full_and_diff_agree(self):
        direct = Consumer(self.tmp / "direct")
        patched = Consumer(self.tmp / "patched")
        self.install(direct, "v3")
        self.install(patched, "v1")
        upgrade(patched, self.client, "catalog", "v3")
        self.assertEqual(
            direct.require("catalog", "v3").content_digest,
            patched.require("catalog", "v3").content_digest,
        )
        for a, b in zip(
            sorted(direct.require("catalog", "v3").segments.items()),
            sorted(patched.require("catalog", "v3").segments.items()),
        ):
            self.assertEqual(a[1].digest, b[1].digest)


class BuilderCliTests(TempDirTest):
    def test_build_publish_verify_cli(self):
        source = self.tmp / "catalog-v1.jsonl"
        variant = DatasetVariant(DatasetSpec("catalog", "v1"))
        variant.write(source)
        build = self.tmp / "build-v1"
        release = self.tmp / "release"
        rc = builder.main([
            "build", "--dataset", "catalog", "--version", "v1",
            "--source", str(source), "--out", str(build),
        ])
        self.assertEqual(rc, 0)
        self.assertTrue((build / "manifest.json").is_file())
        self.assertTrue((build / "segments").is_dir())
        rc = builder.main([
            "publish", "--build", str(build), "--release", str(release),
        ])
        self.assertEqual(rc, 0)
        store = ReleaseStore(release)
        self.assertEqual([v.version for v in store.versions("catalog")], ["v1"])
        self.assertTrue(store.verify("catalog")["ok"])


class ConsoleApiTests(TempDirTest):
    def test_console_scenario_end_to_end(self):
        from segpkg.console import ConsoleServer, Workspace

        workspace = Workspace(self.tmp / "ws")
        server = ConsoleServer(workspace).start_background()
        self.addCleanup(server.shutdown)
        result = workspace.publish(count=3, edits=1)
        self.assertEqual(len(result["published"]), 3)
        workspace.install("c1", "v1")
        upgrade_result = workspace.upgrade("c1", "v3")
        self.assertTrue(upgrade_result["verified"])
        workspace.install("c2", "v1")
        damaged = workspace.upgrade("c2", "v3", damage="corrupt-needed")
        self.assertTrue(damaged["verified"])
        self.assertIn("damage", damaged)
        q = workspace.query("c1", category="alpha", limit=5)
        self.assertTrue(all(r["category"] == "alpha" for r in q["records"]))
        gc = workspace.gc(keep_last=2, dry_run=False)
        self.assertTrue(gc["release_verify"]["ok"])


if __name__ == "__main__":
    unittest.main()
