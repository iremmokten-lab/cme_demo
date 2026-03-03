from __future__ import annotations

import os
import pkgutil
import importlib
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
    """
    src.db paketindeki tüm modülleri yükleyerek (models, registry, vb.)
    tüm SQLAlchemy Table tanımlarının Base.metadata içine girmesini sağlar.

    Streamlit Cloud'da import sırası bazen farklı olabildiği için bu yaklaşım,
    create_all öncesi metadata'nın eksiksiz kurulmasını sağlar.
    """
    try:
        import src.db as db_pkg  # noqa: F401
    except Exception:
        return

    try:
        pkg_path = getattr(db_pkg, "__path__", None)
        if not pkg_path:
            return
        for m in pkgutil.iter_modules(pkg_path, prefix="src.db."):
            name = m.name
            # session modülünü tekrar import etmeyelim (döngü riski)
            if name.endswith(".session"):
                continue
            try:
                importlib.import_module(name)
            except Exception:
                # Bazı opsiyonel modüller hata verebilir; kritik olanlar zaten models üzerinden yüklenir.
                continue
    except Exception:
        return


def _ensure_compat_tables(md: MetaData):
    """
    Bazı tablolarda FK olarak referenced edilen ama metadata'da bulunmayan
    hedef tabloları (özellikle evidencedocuments) için minimum compat tabloları ekler.
    """
    if "evidencedocuments" not in md.tables:
        Table(
            "evidencedocuments",
            md,
            Column("id", Integer, primary_key=True),
            extend_existing=True,
        )
    # Bazı kod dallarında farklı isimle referans edilebiliyor
    if "evidence_documents" not in md.tables:
        Table(
            "evidence_documents",
            md,
            Column("id", Integer, primary_key=True),
            extend_existing=True,
        )


def init_db():
    """
    Uygulama açılışında SQLite şemasını güvenli şekilde ayağa kaldırır.

    Bu fonksiyon şu problemi çözer:
    - create_all sırasında FK hedef tablosu metadata'da yoksa NoReferencedTableError oluşur.
      (Örn: cbam_producer_attestations.document_evidence_id -> evidencedocuments.id)
      Bu hata uygulamanın hiç açılmamasına sebep olur. :contentReference[oaicite:1]{index=1}

    Çözüm:
    - src.db altındaki modülleri yükle
    - compat hedef tabloları garanti et
    - create_all'ı NoReferencedTableError korumasıyla çalıştır
    """

    # 1) Modelleri kesin yükle (Base + tablolar)
    import src.db.models as _models  # noqa: F401
    from src.db.models import Base  # noqa: F401

    # 2) Diğer db modüllerini de yükle (metadata eksik kalmasın)
    _import_all_db_modules()

    md: MetaData = Base.metadata

    # 3) Compat hedef tabloları garanti altına al
    _ensure_compat_tables(md)

    # 4) create_all - iki aşamalı koruma
    try:
        md.create_all(bind=engine)
    except NoReferencedTableError:
        # metadata'da eksik hedef tablo varsa tamamla ve tekrar dene
        _ensure_compat_tables(md)
        md.create_all(bind=engine)
