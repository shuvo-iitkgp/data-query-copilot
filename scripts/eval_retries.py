# scripts/eval_retries.py
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Any

from src.schema_service import SchemaService
from src.sql_generator import SQLGenerator, GenerationConfig
from src.query_executor import QueryExecutor
from src.retry_logic import RetryRunner

DB = "tests/fixtures/nrel_sample.sqlite"

OUT_DIR = Path("reports/phase4_retries")
OUT_DIR.mkdir(parents=True, exist_ok=True)

QUESTIONS = [
    "How many stations are there by state?",
    "Top 10 cities by station count",
    "How many stations have restricted access?",
    "Count stations by fuel_type_code",
    # include harder ones that can trigger errors
    "How many stations were updated in the last year?",
    "Show 50 stations in California with station_name and street_address",
]
def attempt_to_dict(a):
    d = a.__dict__.copy()
    if d.get("error_feedback") is not None:
        d["error_feedback"] = {
            "category": d["error_feedback"].category,
            "message": d["error_feedback"].message,
            "details": d["error_feedback"].details,
        }
    return d
def main():
    svc = SchemaService(DB)
    gen = SQLGenerator(svc, GenerationConfig(max_new_tokens=256, do_sample=False))
    executor = QueryExecutor(DB, timeout_ms=2000, max_rows=1000)

    runner_no_retry = RetryRunner(gen, executor, max_attempts=1, stop_on_repeat_sql=True)
    runner_retry = RetryRunner(gen, executor, max_attempts=3, stop_on_repeat_sql=True)

    rows: List[Dict[str, Any]] = []

    for q in QUESTIONS:
        r1 = runner_no_retry.run(q)
        r3 = runner_retry.run(q)

        rows.append({
            "question": q,
            "no_retry_ok": r1.ok,
            "no_retry_stop_reason": r1.stop_reason,
            "attempt_count_no_retry": len(r1.attempts),

            "retry_ok": r3.ok,
            "retry_stop_reason": r3.stop_reason,
            "attempt_count_retry": len(r3.attempts),
            "retry_converged": bool(r3.ok),
            "retry_oscillated": (r3.stop_reason == "oscillation"),

            "no_retry_attempts": [attempt_to_dict(a) for a in r1.attempts],
            "retry_attempts": [attempt_to_dict(a) for a in r3.attempts],
        })


        print(f"Q: {q}")
        print(f"  no-retry: ok={r1.ok} stop={r1.stop_reason} attempts={len(r1.attempts)}")
        print(f"  retry   : ok={r3.ok} stop={r3.stop_reason} attempts={len(r3.attempts)}")

    # Save logs
    jsonl_path = OUT_DIR / "retry_eval.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Metrics
    n = len(rows)
    no_retry_success = sum(1 for r in rows if r["no_retry_ok"])
    retry_success = sum(1 for r in rows if r["retry_ok"])

    summary = {
        "n_questions": n,
        "no_retry_success_rate": no_retry_success / n,
        "retry_success_rate": retry_success / n,
        "absolute_gain": (retry_success - no_retry_success) / n,
        "logs_path": str(jsonl_path),
    }

    summary_path = OUT_DIR / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("\n=== Phase 4 Metrics ===")
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
