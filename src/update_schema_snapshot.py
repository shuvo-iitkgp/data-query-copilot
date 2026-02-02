# scripts/update_schema_snapshot.py
import os
from pathlib import Path

from src.schema_loader import load_schema, serialize_schema_for_prompt

def main():
    db_path = os.environ.get("TEST_DB_PATH")
    if not db_path:
        raise RuntimeError("TEST_DB_PATH env var is required.")

    schema = load_schema(db_path, include_stats=False)
    blob = serialize_schema_for_prompt(schema)

    out_dir = Path("tests/schema_snapshots")
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "primary.txt").write_text(blob, encoding="utf-8")
    (out_dir / "primary.sha256").write_text(schema.schema_version, encoding="utf-8")

    print("Updated schema snapshots.")

if __name__ == "__main__":
    main()
