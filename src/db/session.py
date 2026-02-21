from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# SQLite file in app working directory
DB_URL = "sqlite:///app.db"

engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
