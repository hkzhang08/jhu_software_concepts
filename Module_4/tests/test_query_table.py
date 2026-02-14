import importlib
import sys
from pathlib import Path

import pytest
import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.db


def test_query_table_top_level_with_fake_db(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/grad_cafe")
    class State:
        def __init__(self):
            self.execute_count = 0
            self.fetchone_calls = 0
            self.fetchall_calls = 0
            self.fetchone_values = [
                (10,),
                (12.34,),
                (3.5, 300.0, 150.0, 4.0),
                (3.7,),
                (45.67,),
                (3.9,),
                (5,),
                (2,),
                (1,),
            ]
            self.fetchall_values = [
                [("Program A", 1)],
                [("Program B", 2)],
            ]

    class FakeCursor:
        def __init__(self, state):
            self.state = state

        def execute(self, _query, _params=None):
            self.state.execute_count += 1

        def fetchone(self):
            self.state.fetchone_calls += 1
            return self.state.fetchone_values.pop(0)

        def fetchall(self):
            self.state.fetchall_calls += 1
            return self.state.fetchall_values.pop(0)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def __init__(self, state):
            self.state = state

        def cursor(self):
            return FakeCursor(self.state)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    state = State()
    monkeypatch.setattr(psycopg, "connect", lambda _dsn: FakeConnection(state))

    if "src.query_table" in sys.modules:
        del sys.modules["src.query_table"]
    import src.query_table  # noqa: F401

    assert state.execute_count == 11
    assert state.fetchone_calls == 9
    assert state.fetchall_calls == 2
    assert state.fetchone_values == []
    assert state.fetchall_values == []
