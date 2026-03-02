from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Boolean, Float, UniqueConstraint, Index
from src.db.models import Base

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class ETSMonitoringPlan(Base):
    __tablename__ = "ets_monitoring_plans"
    __table_args__ = (
        UniqueConstraint("company_id", "year", "version", name="uq_ets_mp_company_year_version"),
        Index("ix_ets_mp_company_year", "company_id", "year"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True)
    year = Column(Integer, nullable=False)
    version = Column(Integer, nullable=False, default=1)
    valid_from = Column(DateTime(timezone=True), nullable=True)
    valid_to = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(40), default="active")  # active/superseded/locked

    plan_json = Column(Text, default="{}")  # source streams, tiers, methods, QAQC
    plan_hash = Column(String(80), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)

class ETSCalibrationCertificate(Base):
    __tablename__ = "ets_calibration_certificates"
    __table_args__ = (Index("ix_ets_cal_company", "company_id"),)
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    device_id = Column(String(120), nullable=False)
    certificate_ref = Column(String(200), default="")
    valid_from = Column(DateTime(timezone=True), nullable=True)
    valid_to = Column(DateTime(timezone=True), nullable=True)
    evidence_doc_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class ETSUncertaintyAssessment(Base):
    __tablename__ = "ets_uncertainty_assessments"
    __table_args__ = (Index("ix_ets_unc_company_year", "company_id", "year"),)
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year = Column(Integer, nullable=False)
    method = Column(String(120), default="sqrt_sum_squares")
    assessment_json = Column(Text, default="{}")
    result_percent = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class ETSTierJustification(Base):
    __tablename__ = "ets_tier_justifications"
    __table_args__ = (Index("ix_ets_tier_company_year", "company_id", "year"),)
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year = Column(Integer, nullable=False)
    justification_json = Column(Text, default="{}")
    created_at = Column(DateTime(timezone=True), default=utcnow)

class ETSQAQCEvidence(Base):
    __tablename__ = "ets_qaqc_evidence"
    __table_args__ = (Index("ix_ets_qaqc_company_year", "company_id", "year"),)
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year = Column(Integer, nullable=False)
    control_name = Column(String(200), nullable=False)
    description = Column(Text, default="")
    evidence_doc_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class ETSFallbackEvent(Base):
    __tablename__ = "ets_fallback_events"
    __table_args__ = (Index("ix_ets_fb_company_year", "company_id", "year"),)
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year = Column(Integer, nullable=False)
    reason = Column(Text, default="")
    method = Column(String(120), default="historical_average")
    value = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), default=utcnow)
