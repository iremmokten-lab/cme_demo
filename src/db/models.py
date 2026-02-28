from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ----------------------------
# Core tenant / identity
# ----------------------------
class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), unique=True, nullable=False, index=True)

    facilities = relationship("Facility", back_populates="company", cascade="all, delete-orphan")
    projects = relationship("Project", back_populates="company", cascade="all, delete-orphan")
    users = relationship("User", back_populates="company", cascade="all, delete-orphan")


class Facility(Base):
    __tablename__ = "facilities"

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    name = Column(String(200), nullable=False)

    # Regulatory metadata
    country = Column(String(100), default="TR")
    sector = Column(String(200), default="")

    company = relationship("Company", back_populates="facilities")
    projects = relationship("Project", back_populates="facility")
    verification_cases = relationship("VerificationCase", back_populates="facility")


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True, index=True)

    name = Column(String(200), nullable=False)
    description = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)

    company = relationship("Company", back_populates="projects")
    facility = relationship("Facility", back_populates="projects")

    uploads = relationship("DatasetUpload", back_populates="project", cascade="all, delete-orphan")
    evidence_docs = relationship("EvidenceDocument", back_populates="project", cascade="all, delete-orphan")
    snapshots = relationship("CalculationSnapshot", back_populates="project", cascade="all, delete-orphan")

    methodologies = relationship("Methodology", back_populates="project", cascade="all, delete-orphan")
    monitoring_plans = relationship("MonitoringPlan", back_populates="project", cascade="all, delete-orphan")

    factor_sets = relationship("FactorSet", back_populates="project", cascade="all, delete-orphan")
    emission_factors = relationship("EmissionFactor", back_populates="project", cascade="all, delete-orphan")

    verification_cases = relationship("VerificationCase", back_populates="project", cascade="all, delete-orphan")


class User(Base):
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("email", name="uq_users_email"),)

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)

    email = Column(String(200), nullable=False, index=True)
    password_hash = Column(String(200), nullable=False)

    # consultant_admin / consultant / client / verifier_admin / verifier
    role = Column(String(50), default="consultant_admin", index=True)

    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    # login security
    failed_attempts = Column(Integer, default=0)
    locked_until = Column(DateTime(timezone=True), nullable=True)

    company = relationship("Company", back_populates="users")


# ----------------------------
# Data ingestion & evidence
# ----------------------------
class DatasetUpload(Base):
    __tablename__ = "datasetuploads"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    dataset_type = Column(String(80), nullable=False, index=True)  # energy / production / electricity / products / ...
    schema_version = Column(String(20), default="v1")

    # Legacy fields used across repo
    original_filename = Column(String(255), default="")
    sha256 = Column(String(64), default="", index=True)

    # Newer naming (kept for compatibility)
    content_hash = Column(String(64), default="", index=True)

    # Storage (Streamlit Cloud friendly)
    storage_uri = Column(String(500), default="")

    # Small-file DB storage (legacy + cloud)
    content_bytes = Column(LargeBinary, nullable=True)
    content_b64 = Column(Text, nullable=True)

    meta_json = Column(Text, default="{}")

    # Data Quality fields used by exports/ui
    validated = Column(Boolean, default=False)
    data_quality_score = Column(Integer, default=0)
    data_quality_report_json = Column(Text, default="{}")

    uploaded_at = Column(DateTime(timezone=True), default=utcnow)
    uploaded_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="uploads")


class EvidenceDocument(Base):
    __tablename__ = "evidencedocuments"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    # Legacy fields (UI + exports)
    original_filename = Column(String(255), default="")
    title = Column(String(255), nullable=False, default="Evidence")
    category = Column(String(80), default="documents")  # invoice / meter / lab / contract / statement / ...
    doc_type = Column(String(80), default="generic")
    notes = Column(Text, default="")

    sha256 = Column(String(64), default="", index=True)
    content_hash = Column(String(64), default="", index=True)

    storage_uri = Column(String(500), default="")

    # DB copy (optional)
    content_bytes = Column(LargeBinary, nullable=True)
    content_b64 = Column(Text, nullable=True)
    mime_type = Column(String(120), default="application/octet-stream")

    meta_json = Column(Text, default="{}")

    uploaded_at = Column(DateTime(timezone=True), default=utcnow)
    uploaded_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="evidence_docs")


# ----------------------------
# Methodology / monitoring plan (MRR / MRV)
# ----------------------------
class Methodology(Base):
    __tablename__ = "methodologies"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    regime = Column(String(50), default="ets", index=True)  # cbam / ets / tr_ets / internal

    config_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    is_active = Column(Boolean, default=True)

    project = relationship("Project", back_populates="methodologies")
    snapshots = relationship("CalculationSnapshot", back_populates="methodology")


class MonitoringPlan(Base):
    __tablename__ = "monitoringplans"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    plan_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    is_active = Column(Boolean, default=True)

    project = relationship("Project", back_populates="monitoring_plans")


# ----------------------------
# Factors (versioned, deterministic)
# ----------------------------
class FactorSet(Base):
    __tablename__ = "factorsets"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    region = Column(String(20), default="TR", index=True)
    version = Column(String(50), default="v1")
    year = Column(Integer, nullable=True, index=True)

    meta_json = Column(Text, default="{}")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    locked = Column(Boolean, default=False)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="factor_sets")
    factors = relationship("EmissionFactor", back_populates="factor_set", cascade="all, delete-orphan")


class EmissionFactor(Base):
    __tablename__ = "emissionfactors"
    __table_args__ = (
        UniqueConstraint("factor_set_id", "factor_type", "region", "year", "version", name="uq_factor_identity"),
    )

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    factor_set_id = Column(Integer, ForeignKey("factorsets.id"), nullable=True, index=True)

    factor_type = Column(String(120), nullable=False, index=True)
    region = Column(String(20), default="TR", index=True)
    year = Column(Integer, nullable=True, index=True)
    version = Column(String(50), default="v1")

    value = Column(Float, nullable=False, default=0.0)
    unit = Column(String(50), default="")
    source = Column(String(255), default="")
    reference = Column(Text, default="")
    meta_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="emission_factors")
    factor_set = relationship("FactorSet", back_populates="factors")


# ----------------------------
# Snapshots / reports / audit
# ----------------------------
class CalculationSnapshot(Base):
    __tablename__ = "calculationsnapshots"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    engine_version = Column(String(50), default="engine-0.0.0")

    config_json = Column(Text, default="{}")
    input_hashes_json = Column(Text, default="{}")
    results_json = Column(Text, default="{}")

    methodology_id = Column(Integer, ForeignKey("methodologies.id"), nullable=True, index=True)
    factor_set_id = Column(Integer, ForeignKey("factorsets.id"), nullable=True, index=True)
    monitoring_plan_id = Column(Integer, ForeignKey("monitoringplans.id"), nullable=True, index=True)

    # Audit-ready hashes
    input_hash = Column(String(64), default="", index=True)
    result_hash = Column(String(64), default="", index=True)

    previous_snapshot_hash = Column(String(64), nullable=True, index=True)

    created_by_user_id = Column(Integer, nullable=True)

    locked = Column(Boolean, default=False)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by_user_id = Column(Integer, nullable=True)

    shared_with_client = Column(Boolean, default=False)

    project = relationship("Project", back_populates="snapshots")
    reports = relationship("Report", back_populates="snapshot", cascade="all, delete-orphan")
    methodology = relationship("Methodology", back_populates="snapshots")
    factor_set = relationship("FactorSet")
    monitoring_plan = relationship("MonitoringPlan")


class VerificationCase(Base):
    __tablename__ = "verificationcases"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True, index=True)

    # Period key
    period_year = Column(Integer, nullable=True, index=True)

    snapshot_id = Column(Integer, ForeignKey("calculationsnapshots.id"), nullable=True, index=True)

    # planning / in_review / actions / closed
    status = Column(String(50), default="open", index=True)
    title = Column(String(200), default="Verification Case")
    description = Column(Text, default="")

    verifier_org = Column(String(200), default="")

    sampling_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    closed_at = Column(DateTime(timezone=True), nullable=True)
    closed_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="verification_cases")
    facility = relationship("Facility", back_populates="verification_cases")
    findings = relationship("VerificationFinding", back_populates="case", cascade="all, delete-orphan")


class VerificationFinding(Base):
    __tablename__ = "verificationfindings"

    id = Column(Integer, primary_key=True)
    case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)

    severity = Column(String(50), default="major")  # major/minor/observation
    title = Column(String(200), nullable=False)
    description = Column(Text, default="")

    # Evidence and actions
    evidence_ref = Column(String(200), default="")
    corrective_action = Column(Text, default="")
    action_due_date = Column(String(30), default="")
    status = Column(String(50), default="open")  # open / in_progress / resolved / accepted

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    resolved_at = Column(DateTime(timezone=True), nullable=True)

    case = relationship("VerificationCase", back_populates="findings")


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    kind = Column(String(80), default="info", index=True)  # info/warn/error
    title = Column(String(200), nullable=False)
    message = Column(Text, default="")
    meta_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by_user_id = Column(Integer, nullable=True)


class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    snapshot_id = Column(Integer, ForeignKey("calculationsnapshots.id"), nullable=True, index=True)

    report_type = Column(String(50), nullable=False, index=True)  # cbam / ets / compliance
    file_path = Column(String(300), default="")
    file_hash = Column(String(64), default="")
    meta_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    snapshot = relationship("CalculationSnapshot", back_populates="reports")


class AuditEvent(Base):
    __tablename__ = "auditevents"

    id = Column(Integer, primary_key=True)
    at = Column(DateTime(timezone=True), default=utcnow, index=True)

    action = Column(String(80), nullable=False, index=True)

    user_id = Column(Integer, nullable=True, index=True)
    company_id = Column(Integer, nullable=True, index=True)
    project_id = Column(Integer, nullable=True, index=True)

    entity_type = Column(String(80), default="")
    entity_id = Column(String(80), default="")

    details_json = Column(Text, default="{}")
