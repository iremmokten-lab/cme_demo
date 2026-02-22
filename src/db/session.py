from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

DB_PATH = os.getenv("CME_DB_PATH", "/tmp/cme_demo.db")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def db():
    return SessionLocal()


def _sqlite_add_column_if_missing(table: str, col: str, ddl: str):
    """SQLite için çok hafif migration.

    Streamlit Cloud'da Alembic kullanmadan, mevcut DB dosyaları bozulmasın diye
    'eksik kolon varsa ALTER TABLE' yaklaşımı uygulanır.
    """
    try:
        insp = inspect(engine)
        cols = {c["name"] for c in insp.get_columns(table)}
        if col in cols:
            return
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl}"))
    except Exception:
        # Migration başarısızsa uygulamayı düşürmeyelim; yeni DB'lerde zaten create_all yapacak.
        return


def _ensure_sqlite_migrations():
    try:
        if not str(engine.url).startswith("sqlite"):
            return
        insp = inspect(engine)
        tables = set(insp.get_table_names())

        # Var olan tablolar için sonradan eklenen kolonlar
        if "datasetuploads" in tables:
            _sqlite_add_column_if_missing("datasetuploads", "source", "source VARCHAR(200) DEFAULT ''")
            _sqlite_add_column_if_missing("datasetuploads", "document_ref", "document_ref VARCHAR(300) DEFAULT ''")

        if "calculationsnapshots" in tables:
            _sqlite_add_column_if_missing("calculationsnapshots", "methodology_id", "methodology_id INTEGER")
            _sqlite_add_column_if_missing("calculationsnapshots", "created_by_user_id", "created_by_user_id INTEGER")
            _sqlite_add_column_if_missing("calculationsnapshots", "locked", "locked BOOLEAN DEFAULT 0")
            _sqlite_add_column_if_missing("calculationsnapshots", "locked_at", "locked_at DATETIME")
            _sqlite_add_column_if_missing("calculationsnapshots", "locked_by_user_id", "locked_by_user_id INTEGER")
            _sqlite_add_column_if_missing("calculationsnapshots", "shared_with_client", "shared_with_client BOOLEAN DEFAULT 0")
    except Exception:
        return


def _seed_minimum_reference_data():
    """Boş DB'de minimum Methodology / EmissionFactor seed'i.

    Bu seed, demo amaçlıdır; prod ortamda kendi kütüphanenizi UI'dan doldurabilirsiniz.
    """
    try:
        from sqlalchemy import select

        from src.db.models import EmissionFactor, Methodology

        with db() as s:
            any_m = s.execute(select(Methodology).limit(1)).scalars().first()
            if not any_m:
                s.add(
                    Methodology(
                        name="Demo Metodoloji (CBAM+ETS)",
                        description=(
                            "Demo amaçlı metodoloji. Hesaplamalar: enerji bazlı Scope-1/2 tahmini, "
                            "ETS net ve CBAM gömülü emisyon (basit örnek). Resmî raporlama için değildir."
                        ),
                        scope="CBAM+ETS",
                        version="v1",
                    )
                )

            any_f = s.execute(select(EmissionFactor).limit(1)).scalars().first()
            if not any_f:
                s.add_all(
                    [
                        EmissionFactor(
                            factor_type="grid_electricity",
                            value=0.42,
                            unit="kgCO2e/kWh",
                            source="Demo varsayım",
                            year=2025,
                            version="v1",
                            region="TR",
                        ),
                        EmissionFactor(
                            factor_type="fuel_natural_gas",
                            value=2.75,
                            unit="kgCO2e/Nm3",
                            source="Demo varsayım",
                            year=2025,
                            version="v1",
                            region="TR",
                        ),
                    ]
                )
            s.commit()
    except Exception:
        return


def init_db():
    from src.db.models import Base

    Base.metadata.create_all(bind=engine)
    _ensure_sqlite_migrations()
    _seed_minimum_reference_data()
