# src/sql_rewriter.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple

from src.sql_policy import SQLPolicy


LIMIT_RE = re.compile(r"\bLIMIT\b", re.IGNORECASE)
LIMIT_VALUE_RE = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)


@dataclass(frozen=True)
class RewriteResult:
    sql: str
    applied: Tuple[str, ...]


def rewrite_sql(validated_sql: str, policy: Optional[SQLPolicy] = None) -> RewriteResult:
    policy = policy or SQLPolicy()
    sql = validated_sql.strip()
    applied = []

    # If LIMIT missing, add it.
    if not LIMIT_RE.search(sql):
        sql = sql.rstrip()
        sql = f"{sql} LIMIT {policy.default_limit}"
        applied.append("added_limit")
        return RewriteResult(sql=sql, applied=tuple(applied))

    # If LIMIT present, cap it.
    m = LIMIT_VALUE_RE.search(sql)
    if m:
        lim = int(m.group(1))
        if lim > policy.max_limit:
            sql = LIMIT_VALUE_RE.sub(f"LIMIT {policy.max_limit}", sql, count=1)
            applied.append("capped_limit")

    return RewriteResult(sql=sql, applied=tuple(applied))
