from __future__ import annotations

from sqlalchemy import Engine, text


def _try(conn, sql: str):
    try:
        conn.execute(text(sql))
    except Exception:
        pass


def run_migrations(engine: Engine):
    """SQLite için migration-like stabilization.

    - Faz 2: verificationcases sampling alanları
    - Faz 2: alerts tablosu varsa yeni kolonlar (future-proof)
    """
    with engine.connect() as conn:
        # verificationcases: sampling
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN sampling_notes TEXT")
        _try(conn, "ALTER TABLE verificationcases ADD COLUMN sampling_size INTEGER")

        # alerts: geleceğe dönük kolonlar (table yoksa ignore)
        _try(conn, "ALTER TABLE alerts ADD COLUMN meta_json TEXT")
        _try(conn, "ALTER TABLE alerts ADD COLUMN resolved_at DATETIME")
        _try(conn, "ALTER TABLE alerts ADD COLUMN resolved_by_user_id INTEGER")
