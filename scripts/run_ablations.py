# scripts/run_ablations.py
from __future__ import annotations

import json
import time
import sqlite3
from pathlib import Path
from typing import List, Dict, Any

from src.schema_service import SchemaService
from src.sql_generator import SQLGenerator, GenerationConfig, build_sql_prompt
from src.sql_validator import validate_sql
from src.sql_rewriter import rewrite_sql
from src.sql_policy import SQLPolicy


# -------------------------
# Config
# -------------------------

DB_PATH = "tests/fixtures/nrel_sample.sqlite"
OUT_DIR = Path("reports/phase2_ablations")
OUT_DIR.mkdir(parents=True, exist_ok=True)

QUESTIONS = [
    "How many stations are there by state?",
    "Top 5 states with the most fuel stations",
    "Number of EV stations per state",
    "How many stations are in California?",
    "Count stations by fuel type",
    "Which states have more than 100 stations?",
    "How many stations were updated in the last year?",
    "Top 10 cities by station count",
    "Number of stations per country",
    "How many stations have restricted access?",
    "Count stations by owner type",
    "States with the highest number of EV chargers",
    "Average latitude and longitude per state",
    "How many stations are currently operational?",
    "Count stations grouped by facility type",
    "Number of stations by status code",
    "How many stations are in New York?",
    "States with the fewest stations",
    "Count of stations by fuel_type_code",
    "How many stations are open to the public?",
]

POLICY = SQLPolicy()

GEN_CFG = GenerationConfig(
    max_new_tokens=256,
    do_sample=False,
)

# -------------------------
# Prompt variants
# -------------------------

def build_schema_only_prompt(schema_blob: str, question: str) -> str:
    return f"""
You generate one SQLite SELECT query.

Schema:
{schema_blob}

Question:
{question}

SQL:
""".strip()


def build_schema_plus_rules_prompt(schema_blob: str, question: str) -> str:
    return build_sql_prompt(
        schema_blob=schema_blob,
        question=question,
        policy=POLICY,
    )


PROMPT_VARIANTS = {
    "schema_only": build_schema_only_prompt,
    "schema_plus_rules": build_schema_plus_rules_prompt,
}


# -------------------------
# Execution helper
# -------------------------

def try_execute(sql: str) -> bool:
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchmany(5)
        conn.close()
        return True
    except Exception:
        return False


# -------------------------
# Main
# -------------------------

def main():
    schema_svc = SchemaService(DB_PATH)
    gen = SQLGenerator(schema_svc, GEN_CFG)

    results: List[Dict[str, Any]] = []

    for variant_name, prompt_builder in PROMPT_VARIANTS.items():
        print(f"\n=== Running variant: {variant_name} ===")

        for q in QUESTIONS:
            schema_blob = schema_svc.schema_blob()
            prompt = prompt_builder(schema_blob, q)

            t0 = time.time()
            gen_out = gen.generate_sql(q, policy=POLICY)
            latency_ms = int((time.time() - t0) * 1000)

            validation = validate_sql(gen_out.sql_clean, POLICY)

            executed = False
            final_sql = None

            if validation.ok:
                rewritten = rewrite_sql(gen_out.sql_clean, POLICY)
                final_sql = rewritten.sql
                executed = try_execute(final_sql)

            record = {
                "variant": variant_name,
                "question": q,
                "sql_raw": gen_out.sql_raw,
                "sql_clean": gen_out.sql_clean,
                "final_sql": final_sql,
                "validator_ok": validation.ok,
                "validator_reasons": validation.reasons,
                "executed_ok": executed,
                "latency_ms": latency_ms,
            }

            results.append(record)

            print(
                f"[{variant_name}] "
                f"ok={validation.ok} exec={executed} "
                f"latency={latency_ms}ms | {q}"
            )

    # -------------------------
    # Save JSONL
    # -------------------------

    jsonl_path = OUT_DIR / "ablations.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    # -------------------------
    # Aggregate + Markdown
    # -------------------------

    summary: Dict[str, Dict[str, float]] = {}

    for v in PROMPT_VARIANTS:
        rows = [r for r in results if r["variant"] == v]
        n = len(rows)
        val_ok = sum(r["validator_ok"] for r in rows)
        exec_ok = sum(r["executed_ok"] for r in rows)
        avg_lat = sum(r["latency_ms"] for r in rows) / n

        summary[v] = {
            "validator_pass_rate": val_ok / n,
            "execution_success_rate": exec_ok / n,
            "avg_latency_ms": avg_lat,
        }

    md = [
        "# Phase 2 Prompt Ablations",
        "",
        "| Variant | Validator Pass Rate | Execution Success Rate | Avg Latency (ms) |",
        "|--------|---------------------|------------------------|------------------|",
    ]

    for v, s in summary.items():
        md.append(
            f"| {v} | "
            f"{s['validator_pass_rate']:.2f} | "
            f"{s['execution_success_rate']:.2f} | "
            f"{int(s['avg_latency_ms'])} |"
        )

    md_path = OUT_DIR / "summary.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    print("\n=== Ablation summary ===")
    print("\n".join(md))
    print(f"\nSaved results to {jsonl_path}")
    print(f"Saved summary to {md_path}")


if __name__ == "__main__":
    main()
