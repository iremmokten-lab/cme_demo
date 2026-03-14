from __future__ import annotations

"""Compatibility models for step-2 pages.

Keep declarations unique by importing step-1 models and declaring only the
extra tables needed by step-2 services.
"""

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint

from src.db.models import Base, utcnow
from src.db.production_step1_models import (  # re-export canonical shared tables
    AccessAuditLog,
    CBAMQuarterlySubmission,
    CbamPortalSubmission,
    CorrectiveAction,
    DatasetApproval,
    MonitoringPlanVersion,
    Producer,
    ProducerAttestation,
    VerificationCaseState,
    VerificationFinding,
    VerificationSamplingItem,
)


class CacheEntry(Base):
    __tablename__ = "cache_entries"
    __table_args__ = (Index("ix_cache_entries_expires", "expires_at"),)

    id = Column(Integer, primary_key=True)
    key = Column(String(255), nullable=False, unique=True, index=True)
    value_json = Column(Text, default="{}")
    expires_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (Index("ix_jobs_status", "status"), Index("ix_jobs_kind", "kind"))

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)
    kind = Column(String(80), nullable=False)
    payload_json = Column(Text, default="{}")
    status = Column(String(40), default="queued")
    result_json = Column(Text, default="{}")
    error = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)


class IntegrationConnection(Base):
    __tablename__ = "integration_connections"
    __table_args__ = (UniqueConstraint("project_id", "name", name="uq_integration_project_name"),)

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    name = Column(String(120), nullable=False)
    kind = Column(String(50), default="generic")
    base_url = Column(String(500), default="")
    auth_json = Column(Text, default="{}")
    config_json = Column(Text, default="{}")
    status = Column(String(30), default="active")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)


class RegulationSpec(Base):
    __tablename__ = "regulation_specs"
    __table_args__ = (UniqueConstraint("code", "version", name="uq_regulation_specs_code_version"),)

    id = Column(Integer, primary_key=True)
    code = Column(String(80), nullable=False, index=True)
    version = Column(String(80), nullable=False, index=True)
    source_url = Column(String(500), default="")
    sha256 = Column(String(80), default="")
    notes = Column(Text, default="")
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


__all__ = [name for name in globals() if not name.startswith("_")]
