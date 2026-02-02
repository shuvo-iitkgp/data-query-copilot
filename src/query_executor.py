# src/query_executor.py
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any, List, Tuple, Optional


# ----------------------------
# Error taxonomy
# ----------------------------

class QueryExecutionError(Exception):
    code: str = "execution_error"

class ValidationFailed(QueryExecutionError):
    code = "validation_failed"

class TimeoutExceeded(QueryExecutionError):
    code = "timeout_exceeded"

class RowLimitExceeded(QueryExecutionError):
    code = "row_limit_exceeded"

class SQLiteExecutionError(QueryExecutionError):
    code = "sqlite_error"


# ----------------------------
# Result object
# ----------------------------

@dataclass(frozen=True)
class ExecutionResult:
    columns: Tuple[str, ...]
    rows: List[Tuple[Any, ...]]
    row_count: int
    execution_time_ms: int


# ----------------------------
# Executor
# ----------------------------

class QueryExecutor:
    """
    Final trust boundary.
    Assumes SQL has already passed validate_sql + rewrite_sql.
    """

    def __init__(
        self,
        db_path: str,
        *,
        timeout_ms: int = 2000,
        max_rows: int = 1000,
    ):
        self.db_path = db_path
        self.timeout_ms = timeout_ms
        self.max_rows = max_rows

    def _connect_readonly(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def execute(self, sql: str) -> ExecutionResult:
        start = time.time()

        try:
            conn = self._connect_readonly()
            cur = conn.cursor()

            # SQLite timeout (best effort)
            conn.execute(f"PRAGMA busy_timeout = {self.timeout_ms}")

            cur.execute(sql)

            rows: List[Tuple[Any, ...]] = []
            for row in cur:
                rows.append(tuple(row))
                if len(rows) > self.max_rows:
                    raise RowLimitExceeded(
                        f"row_limit_exceeded: {len(rows)} > {self.max_rows}"
                    )

            columns = tuple([d[0] for d in cur.description]) if cur.description else ()

            exec_ms = int((time.time() - start) * 1000)

            if exec_ms > self.timeout_ms:
                raise TimeoutExceeded(
                    f"timeout_exceeded: {exec_ms}ms > {self.timeout_ms}ms"
                )

            return ExecutionResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                execution_time_ms=exec_ms,
            )

        except QueryExecutionError:
            raise
        except sqlite3.Error as e:
            raise SQLiteExecutionError(str(e))
        finally:
            try:
                conn.close()
            except Exception:
                pass
