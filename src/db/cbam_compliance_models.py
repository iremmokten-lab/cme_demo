from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Boolean, Float, UniqueConstraint, Index
from sqlalchemy.orm import relationship

from src.db.models import Base

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class CBAMProducer(Base):
    __tablename__ = "cbam_producers"
    __table_args__ = (
        UniqueConstraint("company_id", "producer_code", name="uq_cbam_producer_company_code"),
        Index("ix_cbam_producers_company", "company_id"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    producer_code = Column(String(100), nullable=False)  # internal code or registry id
    legal_name = Column(String(250), nullable=False)
    country = Column(String(80), default="")
    city = Column(String(120), default="")
    address = Column(String(400), default="")
    contact_email = Column(String(200), default="")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class CBAMProducerAttestation(Base):
    __tablename__ = "cbam_producer_attestations"
    __table_args__ = (
        Index("ix_cbam_attest_company", "company_id"),
        Index("ix_cbam_attest_producer", "producer_id"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    producer_id = Column(Integer, ForeignKey("cbam_producers.id"), nullable=False)

    # attestation payload summary (hashable)
    attestation_ref = Column(String(200), default="")  # document ref id / external ref
    statement = Column(Text, default="")  # brief statement
    signed_by = Column(String(200), default="")
    signed_at = Column(DateTime(timezone=True), default=utcnow)
    document_evidence_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)

class CBAMMethodologyEvidence(Base):
    __tablename__ = "cbam_methodology_evidence"
    __table_args__ = (
        Index("ix_cbam_meth_company", "company_id"),
        Index("ix_cbam_meth_snapshot", "snapshot_id"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    snapshot_id = Column(Integer, ForeignKey("calculation_snapshots.id"), nullable=False)

    boundary = Column(Text, default="")        # system boundary
    allocation = Column(Text, default="")      # allocation rationale
    scrap_method = Column(Text, default="")    # scrap treatment
    electricity_method = Column(Text, default="")  # indirect electricity methodology
    electricity_factor_source = Column(String(250), default="")  # source of electricity factor
    notes = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)

class CBAMCarbonPricePaid(Base):
    __tablename__ = "cbam_carbon_price_paid"
    __table_args__ = (
        Index("ix_cbam_cpp_company", "company_id"),
        Index("ix_cbam_cpp_snapshot", "snapshot_id"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    snapshot_id = Column(Integer, ForeignKey("calculation_snapshots.id"), nullable=False)

    country = Column(String(80), default="")
    instrument = Column(String(120), default="")  # tax / ETS / fee
    amount_per_tco2 = Column(Float, default=0.0)
    currency = Column(String(10), default="EUR")
    evidence_doc_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True)
    verified = Column(Boolean, default=False)
    notes = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)

class CBAMQuarterlySubmission(Base):
    __tablename__ = "cbam_quarterly_submissions"
    __table_args__ = (
        UniqueConstraint("company_id", "year", "quarter", name="uq_cbam_company_year_quarter"),
        Index("ix_cbam_q_company", "company_id"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)  # 1..4

    status = Column(String(40), default="draft")  # draft/submitted/corrected/resubmitted/locked
    snapshot_id = Column(Integer, ForeignKey("calculation_snapshots.id"), nullable=True)

    portal_package_uri = Column(String(500), default="")
    portal_package_sha256 = Column(String(80), default="")

    submitted_at = Column(DateTime(timezone=True), nullable=True)
    last_correction_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)

class RegulationSpecVersion(Base):
    __tablename__ = "regulation_spec_versions"
    __table_args__ = (
        UniqueConstraint("spec_name", "spec_version", name="uq_reg_spec_name_version"),
        Index("ix_reg_spec_name", "spec_name"),
    )
    id = Column(Integer, primary_key=True)
    spec_name = Column(String(120), nullable=False)  # CBAM_XSD, ETS_MRR, CBAM_RULES
    spec_version = Column(String(120), nullable=False)
    spec_hash = Column(String(80), default="")
    source = Column(String(400), default="")
    fetched_at = Column(DateTime(timezone=True), default=utcnow)
