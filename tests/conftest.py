# tests/conftest.py
import os
import pytest

@pytest.fixture(scope="session")
def sqlite_db_path():
    """
    Set TEST_DB_PATH env var in CI to point at your canonical SQLite db file.
    """
    path = os.environ.get("TEST_DB_PATH")
    if not path:
        raise RuntimeError("TEST_DB_PATH env var is required for schema drift tests.")
    return path
