# src/cli.py
from __future__ import annotations

import argparse
import os
import sys

from src.schema_service import SchemaService
from src.prompts.schema_prompt import SCHEMA_CONTEXT_TEMPLATE


def build_prompt(question: str, schema_blob: str) -> str:
    # Phase 1 dry-run prompt: schema grounding + the user question.
    # No policy, no validator, no execution.
    return (
        SCHEMA_CONTEXT_TEMPLATE.format(schema_blob=schema_blob).rstrip()
        + "\n\nUSER_QUESTION:\n"
        + question.strip()
        + "\n\nSQL:"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Data-Query-CoPilot dry run (Phase 1)")
    parser.add_argument(
        "--db",
        dest="db_path",
        default=os.environ.get("DB_PATH") or os.environ.get("TEST_DB_PATH"),
        help="Path to SQLite DB. Defaults to env DB_PATH or TEST_DB_PATH.",
    )
    parser.add_argument(
        "-q",
        "--question",
        required=True,
        help="Natural language analytics question.",
    )
    parser.add_argument(
        "--max-schema-chars",
        type=int,
        default=6000,
        help="Max characters for schema blob in the prompt.",
    )
    parser.add_argument(
        "--print-schema",
        action="store_true",
        help="Print schema blob (can be long).",
    )
    parser.add_argument(
        "--print-prompt",
        action="store_true",
        help="Print final prompt that would go to the LLM.",
    )

    args = parser.parse_args()

    if not args.db_path:
        print("ERROR: Provide --db or set DB_PATH/TEST_DB_PATH.", file=sys.stderr)
        return 2

    svc = SchemaService(args.db_path)
    schema = svc.schema()
    schema_blob = svc.schema_blob()
    if len(schema_blob) > args.max_schema_chars:
        # SchemaService currently uses default max in serialize; this is a second guard.
        schema_blob = schema_blob[: args.max_schema_chars]

    print("=== DRY RUN ===")
    print(f"DB: {args.db_path}")
    print(f"SCHEMA_VERSION: {schema.schema_version}")

    if args.print_schema:
        print("\n=== SCHEMA_BLOB ===")
        print(schema_blob)

    prompt = build_prompt(args.question, schema_blob)

    if args.print_prompt:
        print("\n=== FINAL_PROMPT_TO_LLM ===")
        print(prompt)

    # Always show a small preview so user knows it’s working.
    print("\n=== PROMPT_PREVIEW (first 600 chars) ===")
    print(prompt[:600])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
