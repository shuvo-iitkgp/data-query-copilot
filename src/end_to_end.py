# src/end_to_end.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.schema_service import SchemaService
from src.sql_policy import SQLPolicy
from src.sql_generator import SQLGenerator, GenerationConfig
from src.query_executor import QueryExecutor
from src.retry_logic import RetryRunner, RetryResult

from src.summarizer import (
    summarize_table,
    render_markdown_table,
    render_summary_markdown,
)


@dataclass(frozen=True)
class ReportItem:
    id: str
    title: str
    question: str


@dataclass(frozen=True)
class RunAndReportConfig:
    db_path: str
    report_title: str = "Analytics Report"
    max_attempts: int = 3
    timeout_ms: int = 2000
    max_rows: int = 1000
    preview_rows: int = 12
    model_name: str = "Qwen/Qwen2.5-Coder-7B-Instruct"
    max_new_tokens: int = 256


def _json_safe(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, tuple):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, list):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if hasattr(obj, "__dict__"):
        return _json_safe(obj.__dict__.copy())
    return str(obj)


def run_and_report(
    items: Union[str, List[str], List[ReportItem]],
    *,
    cfg: RunAndReportConfig,
    out_dir: Optional[str] = "reports/demo",
) -> Dict[str, Any]:
    """
    End-to-end: NL -> SQL -> validate -> rewrite -> safe execute -> summarize -> markdown report + JSON log.

    items:
      - single question str
      - list[str] questions
      - list[ReportItem] with id/title/question

    returns: dict with paths + run metadata
    """
    # Normalize inputs into ReportItem[]
    report_items: List[ReportItem] = []
    if isinstance(items, str):
        report_items = [ReportItem(id="q1", title="Query 1", question=items)]
    elif items and isinstance(items[0], str):
        report_items = [
            ReportItem(id=f"q{i+1}", title=f"Query {i+1}", question=q)  # type: ignore[index]
            for i, q in enumerate(items)  # type: ignore[arg-type]
        ]
    else:
        report_items = list(items)  # type: ignore[arg-type]

    policy = SQLPolicy()
    svc = SchemaService(cfg.db_path)

    gen_cfg = GenerationConfig(
        model_name=cfg.model_name,
        max_new_tokens=cfg.max_new_tokens,
        do_sample=False,
    )

    gen = SQLGenerator(svc, gen_cfg)
    executor = QueryExecutor(cfg.db_path, timeout_ms=cfg.timeout_ms, max_rows=cfg.max_rows)
    runner = RetryRunner(gen, executor, policy=policy, max_attempts=cfg.max_attempts, stop_on_repeat_sql=True)

    md_blocks: List[str] = []
    logs: List[Dict[str, Any]] = []

    md_blocks.append(f"# {cfg.report_title}")
    md_blocks.append("")
    md_blocks.append("## Executive summary")
    md_blocks.append(f"- Database: `{cfg.db_path}`")
    md_blocks.append(f"- Retry cap: {cfg.max_attempts}")
    md_blocks.append(f"- Read-only execution: enabled")
    md_blocks.append("")

    ok_count = 0

    for it in report_items:
        rr: RetryResult = runner.run(it.question)

        md_blocks.append(f"## {it.title}")
        md_blocks.append("")
        md_blocks.append(f"**Question:** {it.question}")
        md_blocks.append("")

        if not rr.ok:
            md_blocks.append(f"**Status:** FAILED ({rr.stop_reason})")
            md_blocks.append("")
            if rr.attempts:
                last = rr.attempts[-1]
                md_blocks.append("**Last attempted SQL (best effort):**")
                md_blocks.append("```sql")
                md_blocks.append(((last.rewritten_sql or last.sql_clean) or "").strip())
                md_blocks.append("```")
                if last.error_feedback:
                    md_blocks.append("**Error:**")
                    md_blocks.append(f"- Category: {last.error_feedback.category}")
                    md_blocks.append(f"- Message: {last.error_feedback.message}")
            md_blocks.append("")

            logs.append({
                "id": it.id,
                "title": it.title,
                "question": it.question,
                "ok": False,
                "stop_reason": rr.stop_reason,
                "attempts_used": len(rr.attempts),
                "attempts": rr.attempts,  # keep rich objects, will be _json_safe()’d at write time
            })
            continue

        ok_count += 1

        sql = rr.final_sql or ""
        cols = rr.columns or ()
        rows = rr.rows or []

        md_blocks.append("**SQL:**")
        md_blocks.append("```sql")
        md_blocks.append(sql.strip())
        md_blocks.append("```")
        md_blocks.append("")

        ts = summarize_table(cols, rows, title="Table summary")
        md_blocks.append(render_summary_markdown(ts))
        md_blocks.append("")
        md_blocks.append("**Preview:**")
        md_blocks.append("")
        md_blocks.append(render_markdown_table(cols, rows, max_rows=cfg.preview_rows))
        md_blocks.append("")

        logs.append({
            "id": it.id,
            "title": it.title,
            "question": it.question,
            "ok": True,
            "stop_reason": rr.stop_reason,
            "attempts_used": len(rr.attempts),
            "sql": sql,
            "columns": list(cols),
            "row_count": len(rows),
        })

    md_blocks.insert(
        5,
        f"- Successful queries: {ok_count}/{len(report_items)}"
    )

    # Write outputs
    out_path = Path(out_dir) if out_dir else None
    md_text = "\n".join(md_blocks)

    md_file = None
    json_file = None

    if out_path:
        out_path.mkdir(parents=True, exist_ok=True)
        md_file = out_path / "report.md"
        json_file = out_path / "run_log.json"

        md_file.write_text(md_text, encoding="utf-8")
        json_file.write_text(json.dumps(_json_safe(logs), indent=2), encoding="utf-8")

    return {
        "ok": ok_count == len(report_items),
        "success_count": ok_count,
        "total": len(report_items),
        "report_path": str(md_file) if md_file else None,
        "run_log_path": str(json_file) if json_file else None,
        "logs": logs if not out_path else None,
    }
