# src/retry_logic.py
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.sql_policy import SQLPolicy
from src.sql_validator import validate_sql, ValidationDecision
from src.sql_rewriter import rewrite_sql, RewriteResult
from src.query_executor import (
    QueryExecutor,
    QueryExecutionError,
    SQLiteExecutionError,
    TimeoutExceeded,
    RowLimitExceeded,
)
from src.sql_generator import SQLGenerator, GenerationResult


# ----------------------------
# Error feedback taxonomy
# ----------------------------

@dataclass(frozen=True)
class ErrorFeedback:
    category: str  # "validation" | "sqlite" | "timeout" | "row_limit" | "unknown"
    message: str
    details: Dict[str, Any]


def _feedback_from_validation(dec: ValidationDecision) -> ErrorFeedback:
    return ErrorFeedback(
        category="validation",
        message="SQL failed validation rules.",
        details={"reasons": list(dec.reasons)},
    )


def _feedback_from_exception(e: Exception) -> ErrorFeedback:
    if isinstance(e, TimeoutExceeded):
        return ErrorFeedback(category="timeout", message=str(e), details={})
    if isinstance(e, RowLimitExceeded):
        return ErrorFeedback(category="row_limit", message=str(e), details={})
    if isinstance(e, SQLiteExecutionError):
        return ErrorFeedback(category="sqlite", message=str(e), details={})
    if isinstance(e, QueryExecutionError):
        return ErrorFeedback(category=getattr(e, "code", "execution_error"), message=str(e), details={})
    return ErrorFeedback(category="unknown", message=str(e), details={})


# ----------------------------
# Attempt log (for convergence proofs)
# ----------------------------

@dataclass(frozen=True)
class AttemptRecord:
    attempt: int
    sql_raw: str
    sql_clean: str
    validated_ok: bool
    validation_reasons: Tuple[str, ...]
    rewritten_sql: Optional[str]
    executed_ok: bool
    error_feedback: Optional[ErrorFeedback]
    latency_ms: int


@dataclass(frozen=True)
class RetryResult:
    ok: bool
    final_sql: Optional[str]
    columns: Optional[Tuple[str, ...]]
    rows: Optional[List[Tuple[Any, ...]]]
    row_count: Optional[int]
    execution_time_ms: Optional[int]
    attempts: Tuple[AttemptRecord, ...]
    stop_reason: str  # "success" | "max_retries" | "oscillation" | "validation_deadend" | "execution_deadend"


class RetryRunner:
    """
    Phase 4: Automatic retry + self-correction.
    Uses structured feedback injected into the prompt for subsequent attempts.
    """

    def __init__(
        self,
        generator: SQLGenerator,
        executor: QueryExecutor,
        *,
        policy: Optional[SQLPolicy] = None,
        max_attempts: int = 3,
        stop_on_repeat_sql: bool = True,
    ):
        self.generator = generator
        self.executor = executor
        self.policy = policy or SQLPolicy()
        self.max_attempts = max_attempts
        self.stop_on_repeat_sql = stop_on_repeat_sql

    def run(self, question: str) -> RetryResult:
        attempts: List[AttemptRecord] = []
        seen_sql: Dict[str, int] = {}

        last_sql_clean: Optional[str] = None
        last_feedback: Optional[ErrorFeedback] = None

        for i in range(1, self.max_attempts + 1):
            # Inject structured feedback for self-correction
            gen_res: GenerationResult = self.generator.generate_sql(
                question,
                policy=self.policy,
                error_context=self._format_error_context(
                    attempt=i,
                    previous_sql=last_sql_clean,
                    feedback=last_feedback,
                ),
            )

            sql_clean = gen_res.sql_clean

            # Oscillation / repeat detection
            key = " ".join(sql_clean.split()).strip().upper()
            if self.stop_on_repeat_sql:
                if key in seen_sql:
                    attempts.append(
                        AttemptRecord(
                            attempt=i,
                            sql_raw=gen_res.sql_raw,
                            sql_clean=sql_clean,
                            validated_ok=False,
                            validation_reasons=("oscillation_detected",),
                            rewritten_sql=None,
                            executed_ok=False,
                            error_feedback=ErrorFeedback(
                                category="oscillation",
                                message=f"Repeated SQL from attempt {seen_sql[key]}",
                                details={"repeated_attempt": seen_sql[key]},
                            ),
                            latency_ms=gen_res.latency_ms,
                        )
                    )
                    return RetryResult(
                        ok=False,
                        final_sql=None,
                        columns=None,
                        rows=None,
                        row_count=None,
                        execution_time_ms=None,
                        attempts=tuple(attempts),
                        stop_reason="oscillation",
                    )
                seen_sql[key] = i

            # Validate
            dec = validate_sql(sql_clean, policy=self.policy)
            if not dec.ok:
                fb = _feedback_from_validation(dec)
                attempts.append(
                    AttemptRecord(
                        attempt=i,
                        sql_raw=gen_res.sql_raw,
                        sql_clean=sql_clean,
                        validated_ok=False,
                        validation_reasons=dec.reasons,
                        rewritten_sql=None,
                        executed_ok=False,
                        error_feedback=fb,
                        latency_ms=gen_res.latency_ms,
                    )
                )
                last_sql_clean = sql_clean
                last_feedback = fb
                continue

            # Rewrite (LIMIT enforcement, cap)
            rewritten: RewriteResult = rewrite_sql(sql_clean, policy=self.policy)
            final_sql = rewritten.sql

            # Execute safely
            try:
                exec_res = self.executor.execute(final_sql)
                attempts.append(
                    AttemptRecord(
                        attempt=i,
                        sql_raw=gen_res.sql_raw,
                        sql_clean=sql_clean,
                        validated_ok=True,
                        validation_reasons=(),
                        rewritten_sql=final_sql,
                        executed_ok=True,
                        error_feedback=None,
                        latency_ms=gen_res.latency_ms,
                    )
                )
                return RetryResult(
                    ok=True,
                    final_sql=final_sql,
                    columns=exec_res.columns,
                    rows=exec_res.rows,
                    row_count=exec_res.row_count,
                    execution_time_ms=exec_res.execution_time_ms,
                    attempts=tuple(attempts),
                    stop_reason="success",
                )
            except Exception as e:
                fb = _feedback_from_exception(e)
                attempts.append(
                    AttemptRecord(
                        attempt=i,
                        sql_raw=gen_res.sql_raw,
                        sql_clean=sql_clean,
                        validated_ok=True,
                        validation_reasons=(),
                        rewritten_sql=final_sql,
                        executed_ok=False,
                        error_feedback=fb,
                        latency_ms=gen_res.latency_ms,
                    )
                )
                last_sql_clean = final_sql
                last_feedback = fb
                continue

        # Exhausted attempts
        return RetryResult(
            ok=False,
            final_sql=None,
            columns=None,
            rows=None,
            row_count=None,
            execution_time_ms=None,
            attempts=tuple(attempts),
            stop_reason="max_retries",
        )

    def _format_error_context(
        self,
        *,
        attempt: int,
        previous_sql: Optional[str],
        feedback: Optional[ErrorFeedback],
    ) -> str:
        if attempt == 1 or feedback is None:
            return ""

        # Structured feedback block for the model. Keep it short and direct.
        payload = {
            "attempt": attempt,
            "previous_sql": previous_sql,
            "error": {
                "category": feedback.category,
                "message": feedback.message,
                "details": feedback.details,
            },
            "instruction": "Fix the SQL. Use only schema columns. Output SQL only.",
        }
        return json.dumps(payload, ensure_ascii=False)
