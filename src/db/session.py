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


def _import_all_db_modules():
    """src.db paketindeki tüm modülleri yükleyerek tüm Table tanımlarını metadata'ya dahil eder."""
    try:
        import src.db as db_pkg  # noqa: F401
    except Exception:
        return

    pkg_path = getattr(db_pkg, "__path__", None)
    if not pkg_path:
        return

    for m in pkgutil.iter_modules(pkg_path, prefix="src.db."):
        name = m.name
        if name.endswith(".session"):
            continue
        try:
            importlib.import_module(name)
        except Exception:
            # Opsiyonel modüller hata verse de açılışı bloklamasın
            continue


def _ensure_stub_table(md: MetaData, table_name: str):
    """Eksik FK hedef tablolar için minimum stub tablo yaratır."""
    if not table_name:
        return
    if table_name in md.tables:
        return
    Table(
        table_name,
        md,
        Column("id", Integer, primary_key=True),
        extend_existing=True,
    )


def _missing_table_from_error(e: Exception) -> str | None:
    msg = str(e)
    # ör: could not find table 'evidencedocuments'
    m = re.search(r"could not find table ['\"]([^'\"]+)['\"]", msg, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def init_db():
    """Uygulama açılış DB init.

    Streamlit Cloud'da import sırası nedeniyle metadata eksik kalıp
    NoReferencedTableError üretebiliyor. Bu fonksiyon:
      1) src.db.models ve diğer db modüllerini yükler
      2) create_all sırasında eksik FK hedef tablolarını otomatik stub olarak ekler
      3) create_all'ı başarıyla tamamlar
    """
    import src.db.models as _models  # noqa: F401
    from src.db.models import Base

    _import_all_db_modules()

    md: MetaData = Base.metadata

    # Sık görülen compat isimleri
    _ensure_stub_table(md, "evidencedocuments")
    _ensure_stub_table(md, "evidence_documents")

    # create_all: eksik referans varsa otomatik toparla (max döngü)
    for _ in range(25):
        try:
            md.create_all(bind=engine)
            return
        except NoReferencedTableError as e:
            missing = _missing_table_from_error(e)
            if not missing:
                raise
            _ensure_stub_table(md, missing)

    # Çok sayıda eksik hedef tablo varsa burada hala patlıyor olabilir
    md.create_all(bind=engine)
