# src/schema_loader.py
from __future__ import annotations

import dataclasses
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------
# Data model
# ----------------------------

@dataclass(frozen=True)
class ForeignKey:
    from_column: str
    ref_table: str
    ref_column: str
    on_update: Optional[str] = None
    on_delete: Optional[str] = None

@dataclass(frozen=True)
class Column:
    name: str
    type: str
    not_null: bool
    default: Optional[str]
    is_primary_key: bool

@dataclass(frozen=True)
class Table:
    name: str
    columns: Tuple[Column, ...]
    primary_key: Tuple[str, ...]
    foreign_keys: Tuple[ForeignKey, ...]
    row_count: Optional[int] = None
    column_stats: Optional[Dict[str, Dict[str, Any]]] = None  # only if enabled

@dataclass(frozen=True)
class Schema:
    dialect: str
    tables: Tuple[Table, ...]
    schema_version: str  # stable hash of structure (and optionally stats if you want)


# ----------------------------
# Loader
# ----------------------------

DEFAULT_EXCLUDE_TABLES_PREFIXES = ("sqlite_",)

def _connect_readonly(db_path: str) -> sqlite3.Connection:
    # SQLite read-only URI mode.
    # Works for file paths; if you use :memory: in tests, use normal connect.
    if db_path == ":memory:":
        return sqlite3.connect(db_path)
    uri = f"file:{db_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)

def _fetchall(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()

def _list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = _fetchall(
        conn,
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name ASC
        """
    )
    names = [r[0] for r in rows]
    names = [n for n in names if not any(n.startswith(p) for p in DEFAULT_EXCLUDE_TABLES_PREFIXES)]
    return names

def _table_columns(conn: sqlite3.Connection, table: str) -> List[Column]:
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    rows = _fetchall(conn, f"PRAGMA table_info({table})")
    cols: List[Column] = []
    for (_, name, ctype, notnull, dflt_value, pk) in rows:
        cols.append(
            Column(
                name=str(name),
                type=str(ctype or "").upper(),
                not_null=bool(notnull),
                default=None if dflt_value is None else str(dflt_value),
                is_primary_key=bool(pk),
            )
        )
    # preserve PRAGMA output order, but enforce stable sort anyway (name ASC)
    cols.sort(key=lambda c: c.name)
    return cols

def _table_primary_key(columns: List[Column]) -> List[str]:
    pk = [c.name for c in columns if c.is_primary_key]
    pk.sort()
    return pk

def _table_foreign_keys(conn: sqlite3.Connection, table: str) -> List[ForeignKey]:
    # PRAGMA foreign_key_list: id, seq, table, from, to, on_update, on_delete, match
    rows = _fetchall(conn, f"PRAGMA foreign_key_list({table})")
    fks: List[ForeignKey] = []
    for (_id, _seq, ref_table, from_col, to_col, on_update, on_delete, _match) in rows:
        fks.append(
            ForeignKey(
                from_column=str(from_col),
                ref_table=str(ref_table),
                ref_column=str(to_col),
                on_update=None if on_update is None else str(on_update),
                on_delete=None if on_delete is None else str(on_delete),
            )
        )
    fks.sort(key=lambda fk: (fk.from_column, fk.ref_table, fk.ref_column))
    return fks

def _safe_ident(name: str) -> str:
    # Minimal escaping for identifiers used in generated SQL.
    # Use double quotes, but also refuse weird embedded quotes.
    if '"' in name:
        raise ValueError(f'Unsafe identifier contains double quote: {name}')
    return f'"{name}"'

def _compute_table_stats(conn: sqlite3.Connection, table: str, columns: List[Column]) -> Tuple[int, Dict[str, Dict[str, Any]]]:
    # Keep this deliberately simple and cheap-ish.
    # For each column: null_count, min, max (min/max only computed for numeric-ish and date-ish and text)
    q_table = _safe_ident(table)

    row_count = _fetchall(conn, f"SELECT COUNT(*) FROM {q_table}")[0][0]

    stats: Dict[str, Dict[str, Any]] = {}
    for c in columns:
        q_col = _safe_ident(c.name)
        null_count = _fetchall(conn, f"SELECT SUM(CASE WHEN {q_col} IS NULL THEN 1 ELSE 0 END) FROM {q_table}")[0][0]

        col_stat: Dict[str, Any] = {"null_count": int(null_count)}

        # min/max are generally safe for SQLite for numeric/text/date-like stored as TEXT.
        # If values are mixed types, SQLite still returns something deterministic.
        min_val = _fetchall(conn, f"SELECT MIN({q_col}) FROM {q_table}")[0][0]
        max_val = _fetchall(conn, f"SELECT MAX({q_col}) FROM {q_table}")[0][0]
        col_stat["min"] = min_val
        col_stat["max"] = max_val

        stats[c.name] = col_stat

    return int(row_count), stats

def _schema_structure_dict(tables: List[Table]) -> Dict[str, Any]:
    # Only structural parts by default.
    # If you want stats included in schema_version, add them here behind a flag.
    return {
        "dialect": "sqlite",
        "tables": [
            {
                "name": t.name,
                "columns": [
                    {
                        "name": c.name,
                        "type": c.type,
                        "not_null": c.not_null,
                        "default": c.default,
                        "is_primary_key": c.is_primary_key,
                    }
                    for c in t.columns
                ],
                "primary_key": list(t.primary_key),
                "foreign_keys": [
                    {
                        "from_column": fk.from_column,
                        "ref_table": fk.ref_table,
                        "ref_column": fk.ref_column,
                        "on_update": fk.on_update,
                        "on_delete": fk.on_delete,
                    }
                    for fk in t.foreign_keys
                ],
            }
            for t in tables
        ],
    }

def _stable_hash(obj: Any) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

def load_schema(
    db_path: str,
    *,
    include_stats: bool = False,
    include_row_counts: bool = False,
    include_column_stats: bool = False,
) -> Schema:
    """
    include_stats is a convenience: if True, includes row counts + column stats.
    Otherwise you can toggle include_row_counts / include_column_stats.
    """
    if include_stats:
        include_row_counts = True
        include_column_stats = True

    conn = _connect_readonly(db_path)
    try:
        table_names = _list_tables(conn)
        tables: List[Table] = []

        for tname in table_names:
            cols = _table_columns(conn, tname)
            pk = _table_primary_key(cols)
            fks = _table_foreign_keys(conn, tname)

            row_count: Optional[int] = None
            col_stats: Optional[Dict[str, Dict[str, Any]]] = None

            if include_row_counts or include_column_stats:
                rc, cs = _compute_table_stats(conn, tname, cols)
                if include_row_counts:
                    row_count = rc
                if include_column_stats:
                    col_stats = cs

            tables.append(
                Table(
                    name=tname,
                    columns=tuple(cols),
                    primary_key=tuple(pk),
                    foreign_keys=tuple(fks),
                    row_count=row_count,
                    column_stats=col_stats,
                )
            )

        # stable sort
        tables.sort(key=lambda t: t.name)

        schema_version = _stable_hash(_schema_structure_dict(tables))
        return Schema(dialect="sqlite", tables=tuple(tables), schema_version=schema_version)
    finally:
        conn.close()


# ----------------------------
# Prompt serialization
# ----------------------------

def serialize_schema_for_prompt(schema: Schema, *, max_chars: int = 6000) -> str:
    """
    Minimal, stable, compact text.
    Deterministic ordering.
    Avoids noisy details.
    """
    lines: List[str] = []
    lines.append(f"DIALECT: {schema.dialect}")
    lines.append(f"SCHEMA_VERSION: {schema.schema_version}")
    lines.append("TABLES:")

    for t in schema.tables:
        lines.append(f"- {t.name}")
        if t.primary_key:
            lines.append(f"  PK: {', '.join(t.primary_key)}")
        if t.foreign_keys:
            fk_parts = []
            for fk in t.foreign_keys:
                fk_parts.append(f"{fk.from_column}->{fk.ref_table}.{fk.ref_column}")
            lines.append(f"  FK: {', '.join(fk_parts)}")

        lines.append("  COLUMNS:")
        for c in t.columns:
            flags = []
            if c.not_null:
                flags.append("NOT_NULL")
            if c.is_primary_key:
                flags.append("PK")
            flag_str = f" [{'|'.join(flags)}]" if flags else ""
            dtype = c.type or "UNKNOWN"
            lines.append(f"    - {c.name}: {dtype}{flag_str}")

        # Stats are optional and can bloat prompts. Include only if present.
        if t.row_count is not None:
            lines.append(f"  ROWS: {t.row_count}")
        if t.column_stats is not None:
            lines.append("  COL_STATS:")
            for col_name in sorted(t.column_stats.keys()):
                s = t.column_stats[col_name]
                # keep compact
                lines.append(f"    - {col_name}: nulls={s.get('null_count')}, min={s.get('min')}, max={s.get('max')}")

    text = "\n".join(lines)

    if len(text) <= max_chars:
        return text

    # Truncate defensively. Prefer keeping structure over stats.
    # Remove COL_STATS blocks first.
    pruned_lines: List[str] = []
    skip = False
    for line in lines:
        if line.strip() == "COL_STATS:":
            skip = True
            continue
        if skip:
            if line.startswith("- ") or line.startswith("  ") is False:
                skip = False
            else:
                continue
        if not skip:
            pruned_lines.append(line)

    text2 = "\n".join(pruned_lines)
    if len(text2) <= max_chars:
        return text2

    # Last resort: hard cut
    return text2[:max_chars]
