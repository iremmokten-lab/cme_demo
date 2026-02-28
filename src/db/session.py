from __future__ import annotations

import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

DB_URL = os.getenv("DB_URL", "sqlite:///./data/app.db")

# SQLite için check_same_thread=False gerekli olabilir (Streamlit multipage)
engine = create_engine(
    DB_URL,
    connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {},
    echo=False,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


@contextmanager
def db():
    s = SessionLocal()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def init_db():
    # Base import side-effects: models must be imported
    from src.db import models  # noqa: F401

    Base.metadata.create_all(bind=engine)

    # migration-like stabilization
    try:
        from src.db.migrations import run_migrations

        run_migrations()
    except Exception:
        pass
