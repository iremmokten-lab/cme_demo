import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Streamlit Cloud için güvenli default DB yolu
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


def init_db():
    """Tüm model modüllerini yükleyip sonra tabloları oluşturur.

    NoReferencedTableError hatalarının ana nedeni, FK hedef tabloları metadata'ya
    eklenmeden create_all() çalıştırılmasıydı.
    """
    from src.db.models import Base  # Base + core tablolar

    # Uygulamadaki tüm model modüllerini import ederek metadata'ya dahil et
    import src.db.models  # noqa: F401
    try:
        import src.db.production_step1_models  # noqa: F401
    except Exception:
        pass
    try:
        import src.db.phase_ab_models  # noqa: F401
    except Exception:
        pass
    try:
        import src.db.global_ready_models_step1  # noqa: F401
    except Exception:
        pass
    try:
        import src.db.cbam_compliance_models  # noqa: F401
    except Exception:
        pass
    try:
        import src.db.ets_compliance_models  # noqa: F401
    except Exception:
        pass
    try:
        import src.db.erp_automation_models  # noqa: F401
    except Exception:
        pass
    try:
        import src.db.cbam_registry  # noqa: F401
    except Exception:
        pass

    Base.metadata.create_all(bind=engine)

    # SQLite üzerinde alan ekleme vb. için best-effort migration
    try:
        from src.db.migrations import run_migrations

        run_migrations(engine)
    except Exception:
        pass
