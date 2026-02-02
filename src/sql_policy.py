# src/sql_policy.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class SQLPolicy:
    # Hard guarantees
    allow_only_select: bool = True
    single_statement_only: bool = True
    disallow_semicolons: bool = True

    # Safety blocks
    disallow_comments: bool = True  # -- and /* */
    disallow_pragma_attach: bool = True
    disallow_writes: bool = True

    # Optional tightening
    disallow_with: bool = False  # set True if you want to ban CTEs initially
    disallow_select_star: bool = False  # set True if you want to ban SELECT * initially

    # Default limit policy (enforced by rewriter)
    default_limit: int = 200
    max_limit: int = 1000


FORBIDDEN_KEYWORDS: Tuple[str, ...] = (
    # Writes / schema changes
    "INSERT", "UPDATE", "DELETE", "REPLACE", "UPSERT",
    "CREATE", "ALTER", "DROP", "TRUNCATE",
    # Transaction / multi-statement / admin-ish
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
    "VACUUM", "REINDEX",
    # Attach & pragma are huge escape hatches
    "ATTACH", "DETACH", "PRAGMA",
)

COMMENT_PATTERNS: Tuple[re.Pattern, ...] = (
    re.compile(r"--"),          # line comment
    re.compile(r"/\*"),         # block comment start
    re.compile(r"\*/"),         # block comment end
)

SEMICOLON_RE = re.compile(r";")

STARTS_WITH_SELECT_RE = re.compile(r"^\s*SELECT\b", re.IGNORECASE)
WITH_RE = re.compile(r"^\s*WITH\b", re.IGNORECASE)
SELECT_STAR_RE = re.compile(r"^\s*SELECT\s+\*\b", re.IGNORECASE)
