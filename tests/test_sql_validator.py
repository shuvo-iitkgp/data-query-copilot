# tests/test_sql_validator.py
from src.sql_policy import SQLPolicy
from src.sql_validator import validate_sql
from src.sql_rewriter import rewrite_sql


def test_allows_simple_select():
    dec = validate_sql("SELECT 1")
    assert dec.ok


def test_blocks_semicolon():
    dec = validate_sql("SELECT 1;")
    assert not dec.ok
    assert "contains_semicolon" in dec.reasons


def test_blocks_comments():
    dec = validate_sql("SELECT 1 -- sneaky")
    assert not dec.ok


def test_blocks_writes():
    dec = validate_sql("DELETE FROM fuel_stations")
    assert not dec.ok


def test_adds_limit_if_missing():
    out = rewrite_sql("SELECT state, COUNT(*) c FROM fuel_stations GROUP BY state")
    assert "LIMIT" in out.sql.upper()
    assert "added_limit" in out.applied


def test_caps_limit():
    policy = SQLPolicy(max_limit=1000)
    out = rewrite_sql("SELECT * FROM fuel_stations LIMIT 50000", policy=policy)
    assert "LIMIT 1000" in out.sql.upper()
    assert "capped_limit" in out.applied
