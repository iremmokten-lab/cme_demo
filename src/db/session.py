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
    from src.db.models import Base
    Base.metadata.create_all(bind=engine)
