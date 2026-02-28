from __future__ import annotations

from sqlalchemy import text

from src.db.session import engine


def _try(conn, sql: str):
    try:
        conn.execute(text(sql))
    except Exception:
        pass


def run_migrations():
    """SQLite migration-like: eksik kolonları ekle.

    Not: SQLite ALTER TABLE ADD COLUMN destekler (temel).
    """
    with engine.connect() as conn:
        # verification_cases sampling fields
        _try(conn, "ALTER TABLE verification_cases ADD COLUMN sampling_notes TEXT")
        _try(conn, "ALTER TABLE verification_cases ADD COLUMN sampling_size INTEGER")

        # alerts meta json
        _try(conn, "ALTER TABLE alerts ADD COLUMN meta_json TEXT")
        _try(conn, "ALTER TABLE alerts ADD COLUMN resolved_at DATETIME")
        _try(conn, "ALTER TABLE alerts ADD COLUMN resolved_by_user_id INTEGER")
