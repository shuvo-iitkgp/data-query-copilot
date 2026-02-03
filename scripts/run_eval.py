# scripts/run_eval.py
from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.schema_service import SchemaService
from src.sql_generator import SQLGenerator, GenerationConfig
from src.sql_policy import SQLPolicy
from src.sql_validator import validate_sql
from src.sql_rewriter import rewrite_sql
from src.query_executor import QueryExecutor
from src.retry_logic import RetryRunner, RetryResult


# ----------------------------
# Helpers
# ----------------------------

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def fingerprint_result(columns: Tuple[str, ...], rows: List[Tuple[Any, ...]]) -> str:
    payload = {"columns": list(columns), "rows": [list(r) for r in rows]}
    txt = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256_text(txt)

def norm_sql(sql: str) -> str:
    return " ".join((sql or "").split()).strip().upper()

def load_cases(path: str) -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            cases.append(json.loads(line))
    return cases

def check_sql_expectations(sql: str, expect: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    s = (sql or "")
    u = s.upper()

    ok = True

    for frag in expect.get("sql_contains", []):
        if frag.upper() not in u:
            ok = False
            reasons.append(f"sql_missing:{frag}")

    sql_regex = expect.get("sql_regex")
    if sql_regex:
        import re
        if not re.search(sql_regex, s, flags=re.IGNORECASE | re.DOTALL):
            ok = False
            reasons.append("sql_regex_no_match")

    sql_not_contains = expect.get("sql_not_contains", [])
    for frag in sql_not_contains:
        if frag.upper() in u:
            ok = False
            reasons.append(f"sql_forbidden_contains:{frag}")

    return ok, reasons

def check_result_properties(columns: Tuple[str, ...], rows: List[Tuple[Any, ...]], props: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ok = True

    cols = [c.lower() for c in columns]

    cols_need = [c.lower() for c in props.get("columns_contains", [])]
    for c in cols_need:
        if c not in cols:
            ok = False
            reasons.append(f"missing_column:{c}")

    n = len(rows)
    if "row_count_equals" in props and n != int(props["row_count_equals"]):
        ok = False
        reasons.append(f"row_count_not_equal:{n}")

    if "min_rows" in props and n < int(props["min_rows"]):
        ok = False
        reasons.append(f"min_rows_failed:{n}")

    if "max_rows" in props and n > int(props["max_rows"]):
        ok = False
        reasons.append(f"max_rows_failed:{n}")

    return ok, reasons


# ----------------------------
# Core pipeline (Phase 5)
# ----------------------------

@dataclass(frozen=True)
class RunOutcome:
    ok: bool
    stop_reason: str
    sql: Optional[str]
    sql_hash: Optional[str]
    result_fingerprint: Optional[str]
    columns: Optional[Tuple[str, ...]]
    rows: Optional[List[Tuple[Any, ...]]]
    attempts_used: int
    validation_ok: Optional[bool]


def run_once_no_retry(
    *,
    question: str,
    gen: SQLGenerator,
    executor: QueryExecutor,
    policy: SQLPolicy,
) -> RunOutcome:
    gen_res = gen.generate_sql(question, policy=policy)
    dec = validate_sql(gen_res.sql_clean, policy=policy)
    if not dec.ok:
        return RunOutcome(
            ok=False,
            stop_reason="validation_failed",
            sql=gen_res.sql_clean,
            sql_hash=sha256_text(norm_sql(gen_res.sql_clean)),
            result_fingerprint=None,
            columns=None,
            rows=None,
            attempts_used=1,
            validation_ok=False,
        )

    rewritten = rewrite_sql(gen_res.sql_clean, policy=policy).sql
    try:
        exec_res = executor.execute(rewritten)
        fp = fingerprint_result(exec_res.columns, exec_res.rows)
        return RunOutcome(
            ok=True,
            stop_reason="success",
            sql=rewritten,
            sql_hash=sha256_text(norm_sql(rewritten)),
            result_fingerprint=fp,
            columns=exec_res.columns,
            rows=exec_res.rows,
            attempts_used=1,
            validation_ok=True,
        )
    except Exception as e:
        return RunOutcome(
            ok=False,
            stop_reason=getattr(e, "code", "execution_failed"),
            sql=rewritten,
            sql_hash=sha256_text(norm_sql(rewritten)),
            result_fingerprint=None,
            columns=None,
            rows=None,
            attempts_used=1,
            validation_ok=True,
        )


def run_once_retry(
    *,
    question: str,
    runner: RetryRunner,
) -> Tuple[RunOutcome, RetryResult]:
    rr = runner.run(question)
    if not rr.ok:
        # best effort: capture last attempted sql
        last_sql = rr.attempts[-1].rewritten_sql or rr.attempts[-1].sql_clean if rr.attempts else None
        return (
            RunOutcome(
                ok=False,
                stop_reason=rr.stop_reason,
                sql=last_sql,
                sql_hash=sha256_text(norm_sql(last_sql or "")) if last_sql else None,
                result_fingerprint=None,
                columns=None,
                rows=None,
                attempts_used=len(rr.attempts),
                validation_ok=None,
            ),
            rr,
        )

    fp = fingerprint_result(rr.columns or (), rr.rows or [])
    return (
        RunOutcome(
            ok=True,
            stop_reason="success",
            sql=rr.final_sql,
            sql_hash=sha256_text(norm_sql(rr.final_sql or "")) if rr.final_sql else None,
            result_fingerprint=fp,
            columns=rr.columns,
            rows=rr.rows,
            attempts_used=len(rr.attempts),
            validation_ok=None,
        ),
        rr,
    )


# ----------------------------
# Eval
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.environ.get("DB_PATH", "tests/fixtures/nrel_sample.sqlite"))
    ap.add_argument("--cases", default="eval/cases_nrel.jsonl")
    ap.add_argument("--outdir", default="reports/phase5_eval")
    ap.add_argument("--mode", choices=["no_retry", "retry", "compare"], default="compare")
    ap.add_argument("--runs", type=int, default=3, help="Repeat runs per query to test consistency.")
    ap.add_argument("--max_attempts", type=int, default=3, help="Retry attempts when mode=retry/compare.")
    ap.add_argument("--timeout_ms", type=int, default=2000)
    ap.add_argument("--max_rows", type=int, default=1000)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    policy = SQLPolicy()
    svc = SchemaService(args.db)
    gen = SQLGenerator(svc, GenerationConfig(max_new_tokens=256, do_sample=False))
    executor = QueryExecutor(args.db, timeout_ms=args.timeout_ms, max_rows=args.max_rows)
    runner = RetryRunner(gen, executor, max_attempts=args.max_attempts, stop_on_repeat_sql=True)

    cases = load_cases(args.cases)

    # records.jsonl: one record per (case, run, mode)
    jsonl_path = outdir / "records.jsonl"
    f = jsonl_path.open("w", encoding="utf-8")

    def eval_mode(mode: str) -> Dict[str, Any]:
        total = 0
        exec_ok = 0
        correct_ok = 0
        consistent_sql = 0
        consistent_result = 0
        allow_fail_count = 0

        for case in cases:
            cid = case.get("id", "")
            q = case["question"]
            expect = case.get("expect", {})
            allow_fail = bool(expect.get("allow_fail", False))
            if allow_fail:
                allow_fail_count += 1

            run_outcomes: List[RunOutcome] = []
            sql_hashes: List[Optional[str]] = []
            res_fps: List[Optional[str]] = []

            for r in range(1, args.runs + 1):
                if mode == "no_retry":
                    out = run_once_no_retry(question=q, gen=gen, executor=executor, policy=policy)
                    rr = None
                else:
                    out, rr = run_once_retry(question=q, runner=runner)

                run_outcomes.append(out)
                sql_hashes.append(out.sql_hash)
                res_fps.append(out.result_fingerprint)

                # correctness checks only if execution ok (otherwise meaningless)
                sql_ok = False
                res_ok = False
                reasons: List[str] = []

                if out.ok and out.sql:
                    sql_ok, r1 = check_sql_expectations(out.sql, expect)
                    reasons += r1

                    props = expect.get("result_props")
                    if props and out.columns is not None and out.rows is not None:
                        res_ok, r2 = check_result_properties(out.columns, out.rows, props)
                        reasons += r2
                    else:
                        # if no props specified, treat result check as pass
                        res_ok = True

                is_correct = out.ok and sql_ok and res_ok

                # scoring
                total += 1
                if out.ok:
                    exec_ok += 1

                # If allow_fail and it failed, do not penalize correctness
                if allow_fail and (not out.ok):
                    pass
                else:
                    if is_correct:
                        correct_ok += 1

                record = {
                    "mode": mode,
                    "case_id": cid,
                    "run": r,
                    "question": q,
                    "ok": out.ok,
                    "stop_reason": out.stop_reason,
                    "attempts_used": out.attempts_used,
                    "sql": out.sql,
                    "sql_hash": out.sql_hash,
                    "result_fingerprint": out.result_fingerprint,
                    "correct": is_correct,
                    "reasons": reasons,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            # consistency across runs (per case)
            # only count if at least one successful run
            good_sql_hashes = [h for h in sql_hashes if h is not None]
            good_fps = [x for x in res_fps if x is not None]

            if len(good_sql_hashes) >= 2 and len(set(good_sql_hashes)) == 1:
                consistent_sql += 1
            if len(good_fps) >= 2 and len(set(good_fps)) == 1:
                consistent_result += 1

        n_cases = len(cases)
        denom_correct = total  # includes all runs; allow_fail failures simply don't increment correct_ok

        return {
            "mode": mode,
            "cases": n_cases,
            "runs_per_case": args.runs,
            "total_trials": total,
            "execution_success_rate": exec_ok / total if total else 0.0,
            "correctness_rate": correct_ok / denom_correct if denom_correct else 0.0,
            "cases_consistent_sql_rate": consistent_sql / n_cases if n_cases else 0.0,
            "cases_consistent_result_rate": consistent_result / n_cases if n_cases else 0.0,
            "allow_fail_cases": allow_fail_count,
        }

    summaries: List[Dict[str, Any]] = []
    if args.mode in ("no_retry", "compare"):
        summaries.append(eval_mode("no_retry"))
    if args.mode in ("retry", "compare"):
        summaries.append(eval_mode("retry"))

    f.close()

    summary_path = outdir / "summary.json"
    summary_path.write_text(json.dumps(summaries, indent=2), encoding="utf-8")

    # markdown dashboard
    md_lines = [
        "# Phase 5 Evaluation Summary",
        "",
        f"- DB: `{args.db}`",
        f"- Cases: `{args.cases}`",
        f"- Runs per case: {args.runs}",
        f"- Records: `{jsonl_path}`",
        "",
        "| Mode | Exec Success | Correctness | Consistent SQL (per case) | Consistent Results (per case) |",
        "|------|--------------|-------------|----------------------------|-------------------------------|",
    ]
    for s in summaries:
        md_lines.append(
            f"| {s['mode']} | "
            f"{s['execution_success_rate']:.3f} | "
            f"{s['correctness_rate']:.3f} | "
            f"{s['cases_consistent_sql_rate']:.3f} | "
            f"{s['cases_consistent_result_rate']:.3f} |"
        )

    md_path = outdir / "summary.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    print("\n".join(md_lines))
    print(f"\nWrote: {summary_path}")
    print(f"Wrote: {md_path}")
    print(f"Wrote: {jsonl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
