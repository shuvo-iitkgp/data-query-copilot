# src/summarizer.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter
import math
import datetime as dt
import re


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")  # ISO-ish prefix


@dataclass(frozen=True)
class TableSummary:
    title: str
    n_rows: int
    n_cols: int
    columns: Tuple[str, ...]
    inferred_types: Dict[str, str]
    numeric_stats: Dict[str, Dict[str, Any]]
    categorical_top: Dict[str, List[Tuple[str, int]]]
    date_range: Optional[Dict[str, Any]]
    bullets: List[str]


def _is_null(x: Any) -> bool:
    return x is None or (isinstance(x, str) and x.strip() == "")


def _try_float(x: Any) -> Optional[float]:
    if _is_null(x):
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except Exception:
            return None
    return None


def _try_date(x: Any) -> Optional[dt.date]:
    if _is_null(x):
        return None
    if isinstance(x, (dt.date, dt.datetime)):
        return x.date() if isinstance(x, dt.datetime) else x
    if isinstance(x, str) and _DATE_RE.match(x.strip()):
        s = x.strip()[:10]
        try:
            return dt.date.fromisoformat(s)
        except Exception:
            return None
    return None


def _infer_column_type(values: List[Any]) -> str:
    non_null = [v for v in values if not _is_null(v)]
    if not non_null:
        return "null"

    # date-like
    date_hits = 0
    for v in non_null[:50]:
        if _try_date(v) is not None:
            date_hits += 1
    if date_hits >= max(3, int(0.6 * min(len(non_null), 50))):
        return "date"

    # numeric-like
    num_hits = 0
    for v in non_null[:50]:
        if _try_float(v) is not None:
            num_hits += 1
    sample_n = min(len(non_null), 50)
    # if we have very few rows, be lenient
    threshold = 1 if sample_n < 3 else int(0.8 * sample_n)
    threshold = max(1, threshold)

    if num_hits >= threshold:
        return "numeric"


    return "text"


def _quantiles(sorted_vals: List[float], qs: List[float]) -> Dict[str, float]:
    if not sorted_vals:
        return {}
    n = len(sorted_vals)
    out: Dict[str, float] = {}
    for q in qs:
        idx = int(round((n - 1) * q))
        idx = max(0, min(n - 1, idx))
        out[str(q)] = sorted_vals[idx]
    return out


def summarize_table(
    columns: Tuple[str, ...],
    rows: List[Tuple[Any, ...]],
    *,
    title: str = "Query Result",
    max_categories: int = 5,
) -> TableSummary:
    n_rows = len(rows)
    n_cols = len(columns)

    col_values: Dict[str, List[Any]] = {c: [] for c in columns}
    for r in rows:
        for i, c in enumerate(columns):
            col_values[c].append(r[i] if i < len(r) else None)

    inferred_types: Dict[str, str] = {}
    numeric_stats: Dict[str, Dict[str, Any]] = {}
    categorical_top: Dict[str, List[Tuple[str, int]]] = {}

    date_candidates: List[str] = []

    for c in columns:
        vals = col_values[c]
        t = _infer_column_type(vals)
        inferred_types[c] = t

        if t == "date":
            date_candidates.append(c)

        if t == "numeric":
            nums = [v for v in (_try_float(x) for x in vals) if v is not None]
            nums_sorted = sorted(nums)
            if nums_sorted:
                mean = sum(nums_sorted) / len(nums_sorted)
                # std dev (population)
                var = sum((x - mean) ** 2 for x in nums_sorted) / len(nums_sorted)
                std = math.sqrt(var)
                qs = _quantiles(nums_sorted, [0.0, 0.25, 0.5, 0.75, 1.0])
                numeric_stats[c] = {
                    "count": len(nums_sorted),
                    "min": qs.get("0.0"),
                    "p25": qs.get("0.25"),
                    "median": qs.get("0.5"),
                    "p75": qs.get("0.75"),
                    "max": qs.get("1.0"),
                    "mean": mean,
                    "std": std,
                }

        if t == "text":
            # treat as categorical if low-ish cardinality in sample
            non_null = [str(v).strip() for v in vals if not _is_null(v)]
            if non_null:
                counts = Counter(non_null)
                # only show "top categories" if it is not basically all unique
                if len(counts) <= max(50, int(0.5 * len(non_null))):
                    categorical_top[c] = counts.most_common(max_categories)

    # Date range detection: pick first date-like column
    date_range: Optional[Dict[str, Any]] = None
    if date_candidates:
        c = date_candidates[0]
        ds = [d for d in (_try_date(x) for x in col_values[c]) if d is not None]
        if ds:
            date_range = {"column": c, "min": min(ds).isoformat(), "max": max(ds).isoformat()}

    bullets: List[str] = []
    bullets.append(f"Returned {n_rows} rows and {n_cols} columns.")

    if numeric_stats:
        # pick up to two numeric columns
        num_cols = list(numeric_stats.keys())[:2]
        for nc in num_cols:
            st = numeric_stats[nc]
            bullets.append(
                f"Numeric column `{nc}`: median {st['median']:.3g}, range {st['min']:.3g} to {st['max']:.3g}."
            )

    if categorical_top:
        # pick up to two categorical columns
        cat_cols = list(categorical_top.keys())[:2]
        for cc in cat_cols:
            top = categorical_top[cc]
            if top:
                bullets.append(f"Top `{cc}` values: " + ", ".join([f"{k} ({v})" for k, v in top]) + ".")

    if date_range:
        bullets.append(f"Date coverage in `{date_range['column']}`: {date_range['min']} to {date_range['max']}.")

    return TableSummary(
        title=title,
        n_rows=n_rows,
        n_cols=n_cols,
        columns=columns,
        inferred_types=inferred_types,
        numeric_stats=numeric_stats,
        categorical_top=categorical_top,
        date_range=date_range,
        bullets=bullets,
    )


def render_markdown_table(columns: Tuple[str, ...], rows: List[Tuple[Any, ...]], *, max_rows: int = 15) -> str:
    cols = list(columns)
    show_rows = rows[:max_rows]

    def esc(x: Any) -> str:
        s = "" if x is None else str(x)
        s = s.replace("\n", " ").replace("|", "\\|")
        return s

    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = "\n".join("| " + " | ".join(esc(v) for v in r) + " |" for r in show_rows)
    if not body:
        body = "| " + " | ".join([""] * len(cols)) + " |"

    more = ""
    if len(rows) > max_rows:
        more = f"\n\nShowing first {max_rows} rows of {len(rows)}."

    return "\n".join([header, sep, body]) + more


def render_summary_markdown(ts: TableSummary) -> str:
    lines = [f"### {ts.title}", ""]
    for b in ts.bullets:
        lines.append(f"- {b}")
    return "\n".join(lines)
