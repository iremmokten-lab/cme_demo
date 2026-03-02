import os
from pathlib import Path

from sqlalchemy import create_engine
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


def init_db():
    # Base tablolar
    from src.db.models import Base

    # IMPORTANT:
    # SQLAlchemy needs all model modules imported before create_all(),
    # otherwise ForeignKey targets might not be registered and
    # "NoReferencedTableError" happens.
    model_modules = [
        "src.db.cbam_registry",
        "src.db.cbam_compliance_models",
        "src.db.ets_compliance_models",
        "src.db.phase_ab_models",
        "src.db.production_step1_models",
        "src.db.production_step2_models",
        "src.db.global_ready_models_step1",
        "src.db.global_ready_models_step2",
        "src.db.job_models",
        "src.db.erp_models",
        "src.db.erp_automation_models",
    ]

    for mod in model_modules:
        try:
            __import__(mod)  # noqa: WPS421
        except Exception:
            # Demo mode: allow missing optional modules.
            pass

    Base.metadata.create_all(bind=engine)

    # Faz 2: migration-like stabilization (SQLite)
    try:
        from src.db.migrations import run_migrations

        run_migrations(engine)
    except Exception:
        pass
