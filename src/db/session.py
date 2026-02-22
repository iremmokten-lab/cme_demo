import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Streamlit Cloud için en güvenlisi: /tmp yazılabilir
DB_PATH = os.getenv("CME_DB_PATH", "/tmp/cme_demo.db")

# klasör varsa garanti et
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

DB_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(
    DB_URL,
    connect_args={"check_same_thread": False},
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
)
