# tests/test_schema_drift.py
import os
from pathlib import Path

from src.schema_loader import load_schema, serialize_schema_for_prompt

SNAPSHOT_DIR = Path("tests/schema_snapshots")
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

def _snapshot_path(name: str) -> Path:
    return SNAPSHOT_DIR / f"{name}.txt"

def _hash_path(name: str) -> Path:
    return SNAPSHOT_DIR / f"{name}.sha256"

def test_schema_serialization_is_deterministic(sqlite_db_path):
    schema1 = load_schema(sqlite_db_path, include_stats=False)
    schema2 = load_schema(sqlite_db_path, include_stats=False)

    blob1 = serialize_schema_for_prompt(schema1)
    blob2 = serialize_schema_for_prompt(schema2)

    assert blob1 == blob2
    assert schema1.schema_version == schema2.schema_version

def test_schema_drift_snapshot(sqlite_db_path):
    """
    Fails if schema changes.
    Update snapshot intentionally by deleting snapshot files and re-running,
    or by adding a dedicated "update snapshot" script/command in your Makefile.
    """
    schema = load_schema(sqlite_db_path, include_stats=False)
    blob = serialize_schema_for_prompt(schema)

    name = "primary"
    snap = _snapshot_path(name)
    hfile = _hash_path(name)

    if not snap.exists() or not hfile.exists():
        snap.write_text(blob, encoding="utf-8")
        hfile.write_text(schema.schema_version, encoding="utf-8")
        # First run creates snapshot, but we still fail to force intentional approval.
        raise AssertionError(
            "Schema snapshot was created. Re-run tests to confirm, and commit the snapshot files."
        )

    expected_blob = snap.read_text(encoding="utf-8")
    expected_hash = hfile.read_text(encoding="utf-8").strip()

    assert blob == expected_blob, "Schema prompt snapshot changed. This is schema drift."
    assert schema.schema_version == expected_hash, "Schema hash changed. This is schema drift."
