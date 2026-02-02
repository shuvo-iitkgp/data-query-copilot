# src/sql_validator.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from src.sql_policy import (
    SQLPolicy,
    FORBIDDEN_KEYWORDS,
    COMMENT_PATTERNS,
    SEMICOLON_RE,
    STARTS_WITH_SELECT_RE,
    WITH_RE,
    SELECT_STAR_RE,
)


@dataclass(frozen=True)
class ValidationDecision:
    ok: bool
    reasons: Tuple[str, ...]


def _upper_sql(sql: str) -> str:
    # Normalize only for keyword scanning. Do not rewrite content.
    return sql.upper()


def validate_sql(raw_sql: str, policy: Optional[SQLPolicy] = None) -> ValidationDecision:
    policy = policy or SQLPolicy()
    reasons: List[str] = []

    if raw_sql is None or not raw_sql.strip():
        return ValidationDecision(ok=False, reasons=("empty_sql",))

    sql = raw_sql.strip()

    # 1) Semicolons (multi-statement vector)
    if policy.disallow_semicolons and SEMICOLON_RE.search(sql):
        reasons.append("contains_semicolon")

    # 2) Comments (hide payload / obfuscate)
    if policy.disallow_comments:
        for pat in COMMENT_PATTERNS:
            if pat.search(sql):
                reasons.append("contains_comment_syntax")
                break

    # 3) Must start with SELECT (or optionally WITH if allowed later)
    if policy.allow_only_select:
        if policy.disallow_with:
            if not STARTS_WITH_SELECT_RE.search(sql):
                reasons.append("not_select")
        else:
            # allow SELECT or WITH ... SELECT
            if not (STARTS_WITH_SELECT_RE.search(sql) or WITH_RE.search(sql)):
                reasons.append("not_select_or_with")

    # 4) Forbidden keywords anywhere (cheap but effective)
    if policy.disallow_writes or policy.disallow_pragma_attach:
        upper = _upper_sql(sql)
        for kw in FORBIDDEN_KEYWORDS:
            if kw in upper:
                reasons.append(f"forbidden_keyword:{kw}")

    # 5) Optional: block SELECT *
    if policy.disallow_select_star and SELECT_STAR_RE.search(sql):
        reasons.append("select_star_disallowed")

    ok = len(reasons) == 0
    return ValidationDecision(ok=ok, reasons=tuple(reasons))
