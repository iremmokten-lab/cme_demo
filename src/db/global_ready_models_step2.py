from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint

from src.db.models import Base, utcnow
from src.db.production_step1_models import AccessAuditLog


class RegulationSpecVersion(Base):
    __tablename__ = "regulation_spec_versions"
    __table_args__ = (
        UniqueConstraint("spec_name", "version_label", name="uq_reg_spec_name_version"),
        {"extend_existing": True},
    )

    id = Column(Integer, primary_key=True)
    spec_name = Column(String(120), nullable=False, index=True)
    version_label = Column(String(80), nullable=False, index=True)
    sha256 = Column(String(80), default="")
    source_url = Column(String(500), default="")
    notes = Column(Text, default="")
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class ERPConnection(Base):
    __tablename__ = "erp_connections"
    __table_args__ = (
        UniqueConstraint("company_id", "name", name="uq_erp_company_name"),
        {"extend_existing": True},
    )

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    name = Column(String(120), nullable=False)
    kind = Column(String(50), default="odata")
    base_url = Column(String(500), default="")
    token_secret = Column(String(500), default="")
    config_json = Column(Text, default="{}")
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)
