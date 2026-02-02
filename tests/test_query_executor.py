import pytest

from src.query_executor import (
    QueryExecutor,
    RowLimitExceeded,
    SQLiteExecutionError,
)
from src.sql_validator import validate_sql
from src.sql_policy import SQLPolicy
from src.sql_rewriter import rewrite_sql


DB = "tests/fixtures/nrel_sample.sqlite"


def test_read_only_blocks_writes():
    executor = QueryExecutor(DB)

    with pytest.raises(SQLiteExecutionError):
        executor.execute("INSERT INTO fuel_stations VALUES (1)")


def test_row_limit_enforced():
    executor = QueryExecutor(DB, max_rows=5)

    sql = "SELECT * FROM fuel_stations LIMIT 100"
    with pytest.raises(RowLimitExceeded):
        executor.execute(sql)


def test_valid_query_executes():
    executor = QueryExecutor(DB)

    sql = "SELECT state, COUNT(*) c FROM fuel_stations GROUP BY state LIMIT 5"
    res = executor.execute(sql)

    assert res.row_count <= 5
    assert "state" in res.columns
    assert "c" in res.columns
