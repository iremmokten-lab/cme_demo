from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    Index,
    UniqueConstraint,
)

from src.db.models import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ERPConnection(Base):
    """ERP entegrasyon bağlantı kaydı.

    Güvenlik notu:
      - Secret değerleri DB'de saklanmaz.
      - secret_ref üzerinden ortam değişkeni (env) ile okunur.
        Örn: ERP_SECRET_<secret_ref>=... (Streamlit Cloud secrets / env).
    """

    __tablename__ = "erp_connections"
    __table_args__ = (
        Index("ix_erp_conn_company_active", "company_id", "is_active"),
        UniqueConstraint("company_id", "name", name="uq_erp_conn_company_name"),
    )

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    vendor = Column(String(50), nullable=False, default="CUSTOM")  # SAP/LOGO/NETSIS/CUSTOM

    mode = Column(String(20), nullable=False, default="csv_upload")  # csv_upload/rest
    base_url = Column(String(500), default="")
    auth_type = Column(String(20), default="none")  # none/api_key/bearer/basic
    secret_ref = Column(String(100), default="")

    description = Column(Text, default="")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)


class ERPMapping(Base):
    """ERP → Platform dataset mapping.

    mapping_json: kaynak kolon adı → hedef kolon adı eşlemesi
    transform_json: birim dönüşümü / sabit değer gibi basit kurallar
    """

    __tablename__ = "erp_mappings"
    __table_args__ = (
        Index("ix_erp_map_conn_dataset", "connection_id", "dataset_type"),
        UniqueConstraint("connection_id", "dataset_type", name="uq_erp_map_conn_dataset"),
    )

    id = Column(Integer, primary_key=True)
    connection_id = Column(Integer, ForeignKey("erp_connections.id"), nullable=False, index=True)

    dataset_type = Column(String(50), nullable=False)  # energy/production/materials/cbam_products/bom_precursors

    mapping_json = Column(Text, default="{}")
    transform_json = Column(Text, default="{}")
    enabled = Column(Boolean, default=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)


class ERPJobRun(Base):
    """Çalıştırma kaydı (audit için).

    Bir sync çalıştırılır → job_run oluşur → sonucunda DatasetUpload kayıtları oluşur.
    """

    __tablename__ = "erp_job_runs"
    __table_args__ = (
        Index("ix_erp_job_company_time", "company_id", "started_at"),
        Index("ix_erp_job_conn_time", "connection_id", "started_at"),
    )

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    connection_id = Column(Integer, ForeignKey("erp_connections.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    status = Column(String(30), default="running")  # running/success/failed
    summary_json = Column(Text, default="{}")
    error_text = Column(Text, default="")

    started_at = Column(DateTime(timezone=True), default=utcnow)
    finished_at = Column(DateTime(timezone=True), nullable=True)
