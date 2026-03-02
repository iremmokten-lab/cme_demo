from __future__ import annotations
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, UniqueConstraint
from src.db.models import Base, utcnow

class IntegrationConnection(Base):
    __tablename__ = "integration_connections"
    __table_args__ = (UniqueConstraint("project_id","name", name="uq_integration_project_name"),)
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    name = Column(String(120), nullable=False, index=True)  # SAP/Logo/Netsis/Generic
    kind = Column(String(80), default="generic", index=True)  # odata/rest/file
    base_url = Column(String(300), default="")
    auth_json = Column(Text, default="{}")
    config_json = Column(Text, default="{}")
    status = Column(String(40), default="active", index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)
    kind = Column(String(80), nullable=False, index=True)  # ingestion/export/refresh/cache
    status = Column(String(40), default="queued", index=True)  # queued/running/success/failed
    payload_json = Column(Text, default="{}")
    result_json = Column(Text, default="{}")
    error = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)

class CacheEntry(Base):
    __tablename__ = "cache_entries"
    __table_args__ = (UniqueConstraint("key", name="uq_cache_key"),)
    id = Column(Integer, primary_key=True)
    key = Column(String(200), nullable=False, index=True)
    value_json = Column(Text, default="{}")
    expires_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class RegulationSpec(Base):
    __tablename__ = "regulation_specs"
    __table_args__ = (UniqueConstraint("code","version", name="uq_reg_spec_identity"),)
    id = Column(Integer, primary_key=True)
    code = Column(String(60), nullable=False, index=True)  # CBAM/ETS/TR_ETS/...
    version = Column(String(60), nullable=False, index=True)
    source_url = Column(String(400), default="")
    sha256 = Column(String(80), default="", index=True)
    notes = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
