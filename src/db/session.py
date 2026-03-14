from __future__ import annotations

import importlib
import os
import pkgutil
import re
from pathlib import Path

from sqlalchemy import Column, Integer, MetaData, Table, create_engine
from sqlalchemy.exc import NoReferencedTableError
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


def _import_all_db_modules() -> None:
    """Load canonical db modules so Base.metadata is complete before create_all."""
    try:
        import src.db as db_pkg  # noqa: F401
    except Exception:
        return

    pkg_path = getattr(db_pkg, "__path__", None)
    if not pkg_path:
        return

    compatibility_modules = {
        "src.db.production_step1_models",
        "src.db.production_step2_models",
    }
    for m in pkgutil.iter_modules(pkg_path, prefix="src.db."):
        name = m.name
        if name.endswith(".session") or name in compatibility_modules:
            continue
        try:
            importlib.import_module(name)
        except Exception:
            continue


def _ensure_stub_table(md: MetaData, table_name: str | None) -> None:
    if not table_name or table_name in md.tables:
        return
    Table(
        table_name,
        md,
        Column("id", Integer, primary_key=True),
        extend_existing=True,
    )


def _missing_table_from_error(e: Exception) -> str | None:
    msg = str(e)
    m = re.search(r"could not find table ['\"]([^'\"]+)['\"]", msg, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def init_db() -> None:
    import src.db.models as _models  # noqa: F401
    from src.db.migrations import run_migrations
    from src.db.models import Base

    _import_all_db_modules()

    md: MetaData = Base.metadata
    if os.getenv("CME_TEST_MODE") == "1":
        try:
            md.create_all(bind=engine)
        except Exception:
            pass
        with engine.begin() as conn:
            for table in reversed(md.sorted_tables):
                try:
                    conn.execute(table.delete())
                except Exception:
                    pass
        from src.db.migrations import run_migrations as _run_migrations
        _run_migrations(engine)
        return

    _ensure_stub_table(md, "evidencedocuments")
    _ensure_stub_table(md, "evidence_documents")

    for _ in range(25):
        try:
            md.create_all(bind=engine)
            run_migrations(engine)
            return
        except NoReferencedTableError as e:
            missing = _missing_table_from_error(e)
            if not missing:
                raise
            _ensure_stub_table(md, missing)

    md.create_all(bind=engine)
    run_migrations(engine)
