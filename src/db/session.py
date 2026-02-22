from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

DB_PATH = os.getenv("CME_DB_PATH", "/tmp/cme_demo.db")
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

DATABASE_URL = os.getenv("DATABASE_URL") or f"sqlite:///{DB_PATH}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def db():
    return SessionLocal()


def _sqlite_add_column_if_missing(table: str, col: str, ddl: str):
    try:
        insp = inspect(engine)
        cols = {c["name"] for c in insp.get_columns(table)}
        if col in cols:
            return
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl}"))
    except Exception:
        return


def _ensure_sqlite_migrations():
    """SQLite için minimal migration. Yeni tablolar create_all ile gelir.
    Var olan tablolara eklenen kolonlar ALTER TABLE ile eklenir."""
    try:
        if not str(engine.url).startswith("sqlite"):
            return

        insp = inspect(engine)
        tables = set(insp.get_table_names())

        if "datasetuploads" in tables:
            _sqlite_add_column_if_missing("datasetuploads", "source", "source VARCHAR(200) DEFAULT ''")
            _sqlite_add_column_if_missing("datasetuploads", "document_ref", "document_ref VARCHAR(300) DEFAULT ''")
            _sqlite_add_column_if_missing("datasetuploads", "evidence_document_id", "evidence_document_id INTEGER")
            _sqlite_add_column_if_missing("datasetuploads", "data_quality_score", "data_quality_score INTEGER")
            _sqlite_add_column_if_missing("datasetuploads", "data_quality_report_json", "data_quality_report_json TEXT DEFAULT '{}'")

        if "calculationsnapshots" in tables:
            _sqlite_add_column_if_missing("calculationsnapshots", "methodology_id", "methodology_id INTEGER")
            _sqlite_add_column_if_missing("calculationsnapshots", "created_by_user_id", "created_by_user_id INTEGER")
            _sqlite_add_column_if_missing("calculationsnapshots", "locked", "locked BOOLEAN DEFAULT 0")
            _sqlite_add_column_if_missing("calculationsnapshots", "locked_at", "locked_at DATETIME")
            _sqlite_add_column_if_missing("calculationsnapshots", "locked_by_user_id", "locked_by_user_id INTEGER")
            _sqlite_add_column_if_missing("calculationsnapshots", "shared_with_client", "shared_with_client BOOLEAN DEFAULT 0")
            _sqlite_add_column_if_missing("calculationsnapshots", "previous_snapshot_hash", "previous_snapshot_hash VARCHAR(64)")
    except Exception:
        return


def _seed_minimum_reference_data():
    """Boş DB'de minimum Methodology + EmissionFactor seed."""
    try:
        from sqlalchemy import select

        from src.db.models import EmissionFactor, Methodology

        demo_factors = [
            ("grid:location", 0.42, "kgCO2e/kWh", "Demo varsayım", 2025, "v1", "TR"),
            ("grid:market", 0.10, "kgCO2e/kWh", "Demo varsayım", 2025, "v1", "TR"),
            ("ncv:natural_gas", 0.038, "GJ/Nm3", "Demo varsayım", 2025, "v1", "TR"),
            ("ef:natural_gas", 0.0561, "tCO2/GJ", "Demo varsayım", 2025, "v1", "TR"),
            ("of:natural_gas", 0.995, "-", "Demo varsayım", 2025, "v1", "TR"),
            ("ncv:diesel", 0.036, "GJ/L", "Demo varsayım", 2025, "v1", "TR"),
            ("ef:diesel", 0.0741, "tCO2/GJ", "Demo varsayım", 2025, "v1", "TR"),
            ("of:diesel", 0.995, "-", "Demo varsayım", 2025, "v1", "TR"),
            ("ncv:coal", 0.025, "GJ/kg", "Demo varsayım", 2025, "v1", "TR"),
            ("ef:coal", 0.0946, "tCO2/GJ", "Demo varsayım", 2025, "v1", "TR"),
            ("of:coal", 0.98, "-", "Demo varsayım", 2025, "v1", "TR"),
        ]

        with db() as s:
            any_m = s.execute(select(Methodology).limit(1)).scalars().first()
            if not any_m:
                s.add(
                    Methodology(
                        name="Demo Metodoloji (CBAM+ETS)",
                        description=(
                            "Paket A motoru ile: yakıt bazlı direct, elektrik bazlı indirect, "
                            "materials.csv üzerinden precursor; CBAM kapsamı CN code + cbam_covered ile belirlenir."
                        ),
                        scope="CBAM+ETS",
                        version="v1",
                    )
                )

            any_f = s.execute(select(EmissionFactor).limit(1)).scalars().first()
            if not any_f:
                for ft, val, unit, src, yr, ver, reg in demo_factors:
                    s.add(
                        EmissionFactor(
                            factor_type=ft,
                            value=float(val),
                            unit=unit,
                            source=src,
                            year=int(yr),
                            version=ver,
                            region=reg,
                        )
                    )

            s.commit()
    except Exception:
        return


def init_db():
    from src.db.models import Base

    Base.metadata.create_all(bind=engine)
    _ensure_sqlite_migrations()
    _seed_minimum_reference_data()
