# src/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any, Dict, Tuple

from src.sql_generator import SQLGenerator
from src.sql_validator import validate_sql, ValidationDecision
from src.sql_rewriter import rewrite_sql, RewriteResult
from src.sql_policy import SQLPolicy
from src.query_executor import QueryExecutor, ExecutionResult
from src.query_executor import QueryExecutionError


@dataclass(frozen=True)
class PipelineResult:
    sql_raw: str
    sql_clean: str
    validation: ValidationDecision
    rewritten: Optional[RewriteResult]
    final_sql: Optional[str]
    meta: Dict[str, Any]


def generate_validate_rewrite(
    question: str,
    generator: SQLGenerator,
    policy: Optional[SQLPolicy] = None,
) -> PipelineResult:
    policy = policy or SQLPolicy()

    gen_res = generator.generate_sql(question, policy=policy)
    dec = validate_sql(gen_res.sql_clean, policy=policy)

    rewritten = None
    final_sql = None
    if dec.ok:
        rewritten = rewrite_sql(gen_res.sql_clean, policy=policy)
        final_sql = rewritten.sql

    return PipelineResult(
        sql_raw=gen_res.sql_raw,
        sql_clean=gen_res.sql_clean,
        validation=dec,
        rewritten=rewritten,
        final_sql=final_sql,
        meta={
            "latency_ms": gen_res.latency_ms,
            "model_name": gen_res.model_name,
            **gen_res.meta,
        },
    )
def generate_validate_execute(
    question: str,
    generator: SQLGenerator,
    executor: QueryExecutor,
    policy: Optional[SQLPolicy] = None,
):
    policy = policy or SQLPolicy()

    gen_res = generator.generate_sql(question, policy=policy)
    dec = validate_sql(gen_res.sql_clean, policy=policy)

    if not dec.ok:
        raise ValidationFailed(dec.reasons)

    rewritten = rewrite_sql(gen_res.sql_clean, policy=policy)
    result = executor.execute(rewritten.sql)

    return {
        "sql": rewritten.sql,
        "columns": result.columns,
        "rows": result.rows,
        "row_count": result.row_count,
        "execution_time_ms": result.execution_time_ms,
    }
