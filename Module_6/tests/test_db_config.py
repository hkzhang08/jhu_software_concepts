"""Unit tests for db_config helpers."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import db_config

pytestmark = pytest.mark.db


def test_get_db_dsn_prefers_database_url(monkeypatch):
    """DATABASE_URL takes precedence over split DB_* values."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/grad_cafe")
    for key in db_config.DB_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    assert db_config.get_db_dsn() == "postgresql://localhost/grad_cafe"


def test_get_db_dsn_raises_when_db_parts_missing(monkeypatch):
    """Missing DB_* values raise a clear RuntimeError."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    for key in db_config.DB_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(RuntimeError) as exc_info:
        db_config.get_db_dsn()
    message = str(exc_info.value)
    assert "Database configuration missing." in message
    assert "missing: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD" in message


def test_get_db_dsn_builds_and_escapes_conninfo(monkeypatch):
    """DB_* variables are converted to quoted/escaped conninfo pairs."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "grad_cafe")
    monkeypatch.setenv("DB_USER", "user\\name")
    monkeypatch.setenv("DB_PASSWORD", "pa'ss\\word")

    dsn = db_config.get_db_dsn()
    assert dsn == (
        "host='localhost' port='5432' dbname='grad_cafe' "
        "user='user\\\\name' password='pa\\'ss\\\\word'"
    )
