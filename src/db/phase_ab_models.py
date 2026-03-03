from __future__ import annotations

from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, UniqueConstraint
from src.db.models import Base, utcnow

class Producer(Base):
    __tablename__ = "producers"
    __table_args__ = (UniqueConstraint("company_id","name", name="uq_producer_company_name"),)
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    name = Column(String(200), nullable=False, index=True)
    country = Column(String(100), default="TR")
    vat_or_tax_id = Column(String(80), default="")
    contact_email = Column(String(200), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)

class ProducerAttestation(Base):
    __tablename__ = "producer_attestations"
    id = Column(Integer, primary_key=True)
    producer_id = Column(Integer, ForeignKey("producers.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    period_quarter = Column(Integer, nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)  # draft/submitted/locked
    declaration_json = Column(Text, default="{}")
    signed_by = Column(String(200), default="")
    signed_at = Column(DateTime(timezone=True), nullable=True)
    evidence_doc_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class CBAMQuarterlySubmission(Base):
    __tablename__ = "cbam_quarterly_submissions"
    __table_args__ = (UniqueConstraint("project_id","period_year","period_quarter", name="uq_cbam_sub_period"),)
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    period_quarter = Column(Integer, nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)  # draft/submitted/corrected/resubmitted/locked
    cbam_xml_uri = Column(String(500), default="")
    portal_zip_uri = Column(String(500), default="")
    schema_version = Column(String(40), default="")
    xsd_hash = Column(String(80), default="")
    notes = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)

class MonitoringPlanVersion(Base):
    __tablename__ = "monitoring_plan_versions"
    __table_args__ = (UniqueConstraint("project_id","period_year","version", name="uq_mp_project_year_version"),)
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    version = Column(Integer, nullable=False, default=1)
    status = Column(String(30), default="draft", index=True)  # draft/approved/locked
    plan_json = Column(Text, default="{}")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)
