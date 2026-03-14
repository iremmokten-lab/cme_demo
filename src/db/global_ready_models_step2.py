from __future__ import annotations

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

from src.db.models import Base, utcnow


class AccessAuditLog(Base):
    __tablename__ = "access_audit_logs"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)

    action = Column(String(120), nullable=False, index=True)
    resource_type = Column(String(80), default="", index=True)
    resource_id = Column(String(80), default="", index=True)

    ip = Column(String(80), default="")
    user_agent = Column(String(250), default="")

    meta_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)


class RegulationSpecVersion(Base):
    __tablename__ = "regulation_spec_versions"
    __table_args__ = (UniqueConstraint("spec_name", "version_label", name="uq_reg_spec_name_version"),)

    id = Column(Integer, primary_key=True)
    spec_name = Column(String(120), nullable=False, index=True)  # CBAM_XSD / ETS_MRR / CBAM_ANNEX
    version_label = Column(String(80), nullable=False, index=True)
    sha256 = Column(String(80), default="")

    source_url = Column(String(500), default="")
    notes = Column(Text, default="")

    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class ERPConnection(Base):
    __tablename__ = "erp_connections"
    __table_args__ = (UniqueConstraint("company_id", "name", name="uq_erp_company_name"),)

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)

    name = Column(String(120), nullable=False)
    kind = Column(String(50), default="odata")  # odata/csv/api
    base_url = Column(String(500), default="")
    token_secret = Column(String(500), default="")

    config_json = Column(Text, default="{}")

    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)
