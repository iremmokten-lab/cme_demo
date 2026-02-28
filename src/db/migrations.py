from __future__ import annotations

"""SQLite için migration-like schema stabilization.

Streamlit Cloud demo ortamında klasik migration aracı (Alembic) yerine
idempotent bir "kolon ekle" yaklaşımı kullanıyoruz.

Kurallar:
- Sadece eksik kolon ekler (ALTER TABLE ... ADD COLUMN)
- Veri kaybı yapmaz
- Var olan kolon tiplerini değiştirmez
"""

from typing import Dict, List, Tuple

from sqlalchemy import Engine, text


def _table_exists(engine: Engine, table: str) -> bool:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    with engine.connect() as c:
        r = c.execute(text(q), {"t": table}).fetchone()
        return r is not None


def _existing_columns(engine: Engine, table: str) -> List[str]:
    with engine.connect() as c:
        rows = c.execute(text(f"PRAGMA table_info('{table}')")).fetchall()
    cols = []
    for r in rows:
        try:
            cols.append(str(r[1]))
        except Exception:
            pass
    return cols


def _add_column(engine: Engine, table: str, col: str, ddl: str) -> None:
    with engine.begin() as c:
        c.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}"))


def ensure_schema(engine: Engine) -> None:
    """Beklenen kritik kolonların DB'de varlığını garanti eder."""

    # tablo -> (kolon, ddl)
    targets: Dict[str, List[Tuple[str, str]]] = {
        # Faz 2: Verification sampling notes
        "verificationcases": [
            ("sampling_notes", "TEXT DEFAULT ''"),
            ("sampling_universe_json", "TEXT DEFAULT '{}'"),
        ],
        # Faz 2: Alerts / tasks
        "alerts": [
            ("id", "INTEGER"),
        ],
    }

    # alerts tablosu yoksa create_all ile oluşacak; burada kolon ekleme yapmayız.
    for table, cols in targets.items():
        if not _table_exists(engine, table):
            continue
        existing = set(_existing_columns(engine, table))
        for col, ddl in cols:
            if col in existing:
                continue
            # alerts tablosu için bu yol kullanılmaz
            if table == "alerts":
                continue
            _add_column(engine, table, col, ddl)
