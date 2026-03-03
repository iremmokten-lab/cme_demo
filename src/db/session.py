import os
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


def init_db():
    """Uygulama açılışında SQLite şemasını güvenli şekilde ayağa kaldırır.

    Streamlit Cloud ortamında ilk açılışta (veya code sync sonrası) bazı
    tabloların import sırası / yarım deploy gibi nedenlerle metadata içinde
    referanslanan FK hedef tablosu bulunamayabilir.

    Bu fonksiyon, kritik çekirdek tabloların metadata'ya dahil olduğundan
    emin olur ve create_all sırasında oluşabilecek NoReferencedTableError
    hatasını otomatik olarak toparlamaya çalışır.
    """

    # 1) Base + çekirdek modeller
    from src.db.models import Base  # noqa: F401

    # 2) Diğer DB modülleri (metadata içine dahil olsun diye import)
    try:
        import src.db.cbam_registry  # noqa: F401
    except Exception:
        pass

    # 3) FK hedef tablosu eksikse (özellikle evidencedocuments), minimal bir
    #    compat tablosu tanımlayıp create_all'ın çalışmasını sağla.
    md: MetaData = Base.metadata

    if "evidencedocuments" not in md.tables:
        Table(
            "evidencedocuments",
            md,
            Column("id", Integer, primary_key=True),
            extend_existing=True,
        )
    if "evidence_documents" not in md.tables:
        Table(
            "evidence_documents",
            md,
            Column("id", Integer, primary_key=True),
            extend_existing=True,
        )

    # 4) Şemayı oluştur (2 aşamalı koruma)
    try:
        md.create_all(bind=engine)
    except NoReferencedTableError:
        # Bazı deploy senaryolarında metadata tam yüklenmeden create_all tetiklenebiliyor.
        # Tekrar denemeden önce compat tabloları garantiye al.
        if "evidencedocuments" not in md.tables:
            Table(
                "evidencedocuments",
                md,
                Column("id", Integer, primary_key=True),
                extend_existing=True,
            )
        if "evidence_documents" not in md.tables:
            Table(
                "evidence_documents",
                md,
                Column("id", Integer, primary_key=True),
                extend_existing=True,
            )
        md.create_all(bind=engine)
