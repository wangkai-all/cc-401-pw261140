"""Deterministic synthetic datasets used by the demo and integration tests.

A dataset is a sequence of JSONL records. Records are sharded into segments by
``id`` so that editing one record changes exactly one segment. The generator is
seeded so every process can regenerate the same baseline if needed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .util import SegpkgError, atomic_write, version_seq

WORDS = [
    "harbor", "quartz", "meadow", "signal", "cobalt", "lantern", "orchard",
    "ripple", "summit", "vellum", "garnet", "willow", "copper", "tundra",
    "beacon", "cinder", "fathom", "glacier", "mosaic", "nectar", "onyx",
    "prism", "raven", "saffron", "thistle", "umber", "violet", "zenith",
]


@dataclass(frozen=True)
class DatasetSpec:
    dataset: str
    version: str
    record_count: int = 60
    segment_records: int = 10
    seed: int | None = None
    profile: str = "default"

    @property
    def seed_value(self) -> int:
        if self.seed is not None:
            return self.seed
        return version_seq(self.version)


def _text(rng: random.Random, words: int = 6) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(words))


def record_for(spec: DatasetSpec, index: int, salt: str = "") -> dict[str, Any]:
    rng = random.Random(f"{spec.dataset}:{spec.seed_value}:{salt}:{index}")
    return {
        "id": f"rec-{index:05d}",
        "title": _text(rng, 4).title(),
        "category": rng.choice(["alpha", "beta", "gamma", "delta"]),
        "score": round(rng.random() * 100, 4),
        "body": _text(rng, 16),
        "nested": {
            "tag": rng.choice(WORDS),
            "flags": [rng.random() < 0.25 for _ in range(4)],
            "count": rng.randrange(0, 1000),
        },
    }


class DatasetVariant:
    """A small mutable view over a dataset that can be spilled to JSONL.

    Mutations are expressed as ``records[i] = ...`` replacements so the demo can
    point at an existing segment file in the UI.
    """

    def __init__(self, spec: DatasetSpec):
        self.spec = spec
        self.records: list[dict[str, Any]] = [
            record_for(spec, i) for i in range(spec.record_count)
        ]

    def touch(self, index: int, salt: str, words: int = 12) -> None:
        """Replace one record with deterministic content keyed by *salt*."""
        if not 0 <= index < len(self.records):
            raise SegpkgError(f"record index {index} out of range")
        rng = random.Random(f"variant:{self.spec.dataset}:{salt}:{index}")
        rec = self.records[index]
        rec["title"] = _text(rng, 3).title()
        rec["body"] = _text(rng, words)
        rec["nested"] = {
            "tag": rng.choice(WORDS),
            "flags": [rng.random() < 0.5 for _ in range(4)],
            "count": rng.randrange(0, 1000),
        }
        rec["score"] = round(rng.random() * 100, 4)

    def append(self, count: int, salt: str) -> None:
        for _ in range(count):
            index = len(self.records)
            rec = record_for(self.spec, index, salt=salt)
            self.records.append(rec)

    def write(self, path: Path) -> Path:
        import json

        lines = [
            json.dumps(r, ensure_ascii=False, sort_keys=True) for r in self.records
        ]
        atomic_write(path, ("\n".join(lines) + "\n").encode("utf-8"))
        return path


def variant_from_jsonl(spec: DatasetSpec, src: Path) -> DatasetVariant:
    """Load a JSONL file into a variant so the demo can mutate a given version."""
    from .util import read_jsonl

    variant = DatasetVariant.__new__(DatasetVariant)
    variant.spec = spec
    variant.records = read_jsonl(src)
    return variant


