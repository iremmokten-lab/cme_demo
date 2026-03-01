import os
import tempfile
import pytest

from src.db.session import init_db


@pytest.fixture(scope="session", autouse=True)
def _db_tmp():
    # Use a temporary sqlite db per test session
    fd, path = tempfile.mkstemp(prefix="cme_test_", suffix=".db")
    os.close(fd)
    os.environ["CME_DB_PATH"] = path
    # re-import session to apply env in same process? init_db uses module-level DB_PATH, so for tests we rely on file path already set before import.
    init_db()
    yield
    try:
        os.remove(path)
    except Exception:
        pass
