# src/report_generator.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.schema_service import SchemaService
from src.sql_generator import SQLGenerator, GenerationConfig
from src.sql_policy import SQLPolicy
from src.query_executor import QueryExecutor
from src.retry_logic import RetryRunner, RetryResult
from src.summarizer import summarize_table, render_markdown_table, render_summary_markdown


@dataclass(frozen=True)
class ReportQuery:
    id: str
    title: str
    question: str


@dataclass(frozen=True)
class ReportConfig:
    report_title: str
    db_path: str
    queries: Tuple[ReportQuery, ...]
    max_attempts: int = 3
    table_preview_rows: int = 15


def load_report_config(path: str) -> ReportConfig:
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    queries = tuple(
        ReportQuery(id=q["id"], title=q["title"], question=q["question"])
        for q in obj["queries"]
    )
    return ReportConfig(
        report_title=obj["report_title"],
        db_path=obj["db_path"],
        queries=queries,
        max_attempts=int(obj.get("max_attempts", 3)),
        table_preview_rows=int(obj.get("table_preview_rows", 15)),
    )
def normalize_question(q: str) -> str:
    # deterministic alias mapping
    aliases = {
        "California": "CA",
        "New York": "NY",
        "Washington": "WA",
    }
    for full, abbr in aliases.items():
        q = q.replace(full, abbr)
    return q
def _json_safe(obj):
    # Converts nested dataclasses / tuples / custom objects into JSON-safe structures.
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, tuple):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, list):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}

    # dataclass-like objects (AttemptRecord, ErrorFeedback) expose __dict__
    if hasattr(obj, "__dict__"):
        d = obj.__dict__.copy()
        return _json_safe(d)

    # last resort
    return str(obj)


def generate_report_markdown(cfg: ReportConfig) -> Tuple[str, List[Dict[str, Any]]]:
    policy = SQLPolicy()
    svc = SchemaService(cfg.db_path)
    gen = SQLGenerator(svc, GenerationConfig(max_new_tokens=256, do_sample=False))
    executor = QueryExecutor(cfg.db_path, timeout_ms=2000, max_rows=1000)
    runner = RetryRunner(gen, executor, max_attempts=cfg.max_attempts, stop_on_repeat_sql=True)

    blocks: List[str] = []
    logs: List[Dict[str, Any]] = []

    blocks.append(f"# {cfg.report_title}")
    blocks.append("")
    blocks.append("## Executive summary")
    blocks.append("- This report was generated automatically from the SQLite analytics database.")
    blocks.append(f"- Retry cap: {cfg.max_attempts}. Deterministic generation: enabled.")
    blocks.append("")

    for rq in cfg.queries:
        rr = runner.run(normalize_question(rq.question))


        blocks.append(f"## {rq.title}")
        blocks.append("")
        blocks.append(f"**Question:** {rq.question}")
        blocks.append("")

        if not rr.ok:
            blocks.append(f"**Status:** FAILED ({rr.stop_reason})")
            blocks.append("")
            if rr.attempts:
                last = rr.attempts[-1]
                blocks.append("**Last attempted SQL (best effort):**")
                blocks.append("```sql")
                blocks.append((last.rewritten_sql or last.sql_clean or "").strip())
                blocks.append("```")
                if last.error_feedback:
                    blocks.append("**Error:**")
                    blocks.append(f"- Category: {last.error_feedback.category}")
                    blocks.append(f"- Message: {last.error_feedback.message}")
            blocks.append("")
            logs.append({
                "id": rq.id,
                "title": rq.title,
                "question": rq.question,
                "ok": False,
                "stop_reason": rr.stop_reason,
                "attempts": [a.__dict__ for a in rr.attempts],
            })
            continue

        sql = rr.final_sql or ""
        cols = rr.columns or ()
        rows = rr.rows or []

        blocks.append("**SQL:**")
        blocks.append("```sql")
        blocks.append(sql.strip())
        blocks.append("```")
        blocks.append("")

        ts = summarize_table(cols, rows, title="Table summary")
        blocks.append(render_summary_markdown(ts))
        blocks.append("")
        blocks.append("**Preview:**")
        blocks.append("")
        blocks.append(render_markdown_table(cols, rows, max_rows=cfg.table_preview_rows))
        blocks.append("")

        logs.append({
            "id": rq.id,
            "title": rq.title,
            "question": rq.question,
            "ok": True,
            "stop_reason": rr.stop_reason,
            "sql": sql,
            "row_count": len(rows),
            "columns": list(cols),
            "attempts_used": len(rr.attempts),
        })

    return "\n".join(blocks), logs


def write_report(cfg_path: str, out_dir: str = "reports/mock_team") -> None:
    cfg = load_report_config(cfg_path)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    md, logs = generate_report_markdown(cfg)

    md_path = out / "report.md"
    md_path.write_text(md, encoding="utf-8")

    log_path = out / "report_run.json"
    log_path.write_text(json.dumps(_json_safe(logs), indent=2), encoding="utf-8")


    print(f"Wrote: {md_path}")
    print(f"Wrote: {log_path}")
