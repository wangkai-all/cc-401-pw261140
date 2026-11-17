"""Small, dependency-free helpers shared by every process.

Artifacts are serialised with canonical JSON so that a digest taken by the
builder, the publisher, the release service and the consumer is always the
digest of the same bytes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any


class SegpkgError(Exception):
    """Base class for user-facing errors."""


class ValidationError(SegpkgError):
    pass


def canonical_json(obj: Any) -> bytes:
    """Canonical JSON bytes (sorted keys, compact separators, UTF-8)."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(obj: Any) -> str:
    return sha256_bytes(canonical_json(obj))


def sha256_file(path: os.PathLike[str] | str, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: os.PathLike[str] | str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def fsync_dir(path: os.PathLike[str] | str) -> None:
    try:
        fd = os.open(os.fspath(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write(path: os.PathLike[str] | str, data: bytes, mode: int = 0o644) -> None:
    """Write *data* to *path* atomically (same-directory temp + rename)."""
    target = Path(path)
    ensure_dir(target.parent)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=os.fspath(target.parent)
    )
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, target)
        fsync_dir(target.parent)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: os.PathLike[str] | str, obj: Any) -> None:
    atomic_write(path, canonical_json(obj))


def read_json(path: os.PathLike[str] | str) -> Any:
    with open(path, "rb") as fh:
        return json.load(fh)


def read_json_bytes(path: os.PathLike[str] | str) -> tuple[Any, bytes]:
    """Return (parsed, raw bytes) so callers can hash exactly what was read."""
    with open(path, "rb") as fh:
        raw = fh.read()
    return json.loads(raw.decode("utf-8")), raw


def parse_jsonl_bytes(raw: bytes) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for lineno, line in enumerate(raw.decode("utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"invalid JSON on line {lineno}: {exc}") from exc
        if not isinstance(obj, dict):
            raise ValidationError(f"line {lineno} is not a JSON object")
        records.append(obj)
    return records


def read_jsonl(path: os.PathLike[str] | str) -> list[dict[str, Any]]:
    with open(path, "rb") as fh:
        return parse_jsonl_bytes(fh.read())


def version_seq(version: str) -> int:
    """Extract the ordering integer from a version label such as ``v12``."""
    if not isinstance(version, str) or not version:
        raise ValidationError("version must be a non-empty string")
    groups = re.findall(r"\d+", version)
    if not groups:
        raise ValidationError(
            f"version {version!r} must contain a number, e.g. v1 or 2024.11"
        )
    seq = int(groups[-1])
    if seq < 0:
        raise ValidationError(f"version {version!r} must not be negative")
    return seq


_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def validate_dataset(name: str) -> str:
    if not _SAFE_NAME.match(name or ""):
        raise ValidationError(f"invalid dataset name: {name!r}")
    return name


def validate_version(version: str) -> str:
    if not _SAFE_NAME.match(version or ""):
        raise ValidationError(f"invalid version name: {version!r}")
    version_seq(version)
    return version


def validate_digest(digest: str) -> str:
    if not isinstance(digest, str) or not _HEX64.match(digest):
        raise ValidationError(f"invalid sha256 digest: {digest!r}")
    return digest


def is_hex64(value: str) -> bool:
    return isinstance(value, str) and bool(_HEX64.match(value))


def human_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(n)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{n} B"


def blob_relpath(digest: str) -> Path:
    """Content-addressed blob path, sharded by the first two hex digits."""
    validate_digest(digest)
    return Path("blobs") / "sha256" / digest[:2] / f"{digest}.blob"


def blob_abspath(root: os.PathLike[str] | str, digest: str) -> Path:
    return Path(root) / blob_relpath(digest)
