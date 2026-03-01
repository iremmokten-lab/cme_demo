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
    content_hash = Column(String(64), default="")
    storage_uri = Column(String(500), default="")

    # Streamlit Cloud storage (optional)
    content_bytes = Column(LargeBinary, nullable=True)
    content_b64 = Column(Text, default="")

    # Validation / DQ
    meta_json = Column(Text, default="{}")
    validated = Column(Boolean, default=False)
    data_quality_score = Column(Integer, default=0)
    data_quality_report_json = Column(Text, default="{}")

    uploaded_at = Column(DateTime(timezone=True), default=utcnow)

    project = relationship("Project", back_populates="uploads")


class EvidenceDocument(Base):
    __tablename__ = "evidencedocuments"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    title = Column(String(200), nullable=False)
    description = Column(Text, default="")

    original_filename = Column(String(255), default="")
    category = Column(String(80), default="documents")
    notes = Column(Text, default="")

    sha256 = Column(String(64), default="", index=True)
    content_hash = Column(String(64), default="")
    storage_uri = Column(String(500), default="")
    content_bytes = Column(LargeBinary, nullable=True)
    content_b64 = Column(Text, default="")
    mime_type = Column(String(120), default="")
    meta_json = Column(Text, default="{}")

    uploaded_at = Column(DateTime(timezone=True), default=utcnow)

    project = relationship("Project", back_populates="evidence_docs")


# ----------------------------
# Monitoring plan / methodology
# ----------------------------
class Methodology(Base):
    __tablename__ = "methodologies"
    __table_args__ = (UniqueConstraint("project_id", "name", "version", name="uq_methodology_identity"),)

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    version = Column(String(50), default="v1")
    scope = Column(String(120), default="")

    description = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="methodologies")
    snapshots = relationship("CalculationSnapshot", back_populates="methodology")


class MonitoringPlan(Base):
    __tablename__ = "monitoringplans"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True, index=True)

    method = Column(String(120), default="calculation")
    tier_level = Column(String(50), default="Tier 2")

    config_json = Column(Text, default="{}")

    updated_at = Column(DateTime(timezone=True), default=utcnow)
    updated_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="monitoring_plans")


# ----------------------------
# Factor governance
# ----------------------------
class FactorSet(Base):
    __tablename__ = "factorsets"
    __table_args__ = (UniqueConstraint("project_id", "name", "region", "year", "version", name="uq_factorset_identity"),)

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    region = Column(String(20), default="TR")
    year = Column(Integer, nullable=True)
    version = Column(String(50), default="v1")

    meta_json = Column(Text, default="{}")

    locked = Column(Boolean, default=False)
    locked_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

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

    # Governance identity
    factor_type = Column(String(120), nullable=False, index=True)
    region = Column(String(20), default="TR", index=True)
    year = Column(Integer, nullable=True, index=True)
    version = Column(String(50), default="v1")

    # Value
    value = Column(Float, nullable=False, default=0.0)
    unit = Column(String(50), default="")
    source = Column(String(255), default="")
    reference = Column(Text, default="")
    methodology = Column(Text, default="")  # methodology / tier note

    # Validity window (audit / replay)
    valid_from = Column(String(30), default="")  # ISO date string for portability (Streamlit Cloud)
    valid_to = Column(String(30), default="")

    # Immutability / drift control
    locked = Column(Boolean, default=False)
    factor_hash = Column(String(64), default="", index=True)

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

    # Engine / orchestration versioning (replayability)
    engine_version = Column(String(50), default="engine-0.0.0")

    # Core payloads (canonical JSON)
    config_json = Column(Text, default="{}")          # monitoring plan / config / methodology params
    input_hashes_json = Column(Text, default="{}")    # dataset refs (sha256/uri/ids)
    results_json = Column(Text, default="{}")         # calculation output + reports payloads

    # Explicit governance refs
    methodology_id = Column(Integer, ForeignKey("methodologies.id"), nullable=True, index=True)
    factor_set_id = Column(Integer, ForeignKey("factorsets.id"), nullable=True, index=True)
    monitoring_plan_id = Column(Integer, ForeignKey("monitoringplans.id"), nullable=True, index=True)

    # Deterministic hash set (audit-ready)
    dataset_hashes_json = Column(Text, default="{}")  # per dataset hash map (energy/production/materials)
    factor_set_hash = Column(String(64), default="", index=True)
    methodology_hash = Column(String(64), default="", index=True)

    # Input/Result hashes (single source of truth)
    input_hash = Column(String(64), default="", index=True)
    result_hash = Column(String(64), default="", index=True)

    # Scenario lineage
    previous_snapshot_hash = Column(String(64), nullable=True, index=True)
    base_snapshot_id = Column(Integer, nullable=True, index=True)  # scenario base snapshot if any
    scenario_meta_json = Column(Text, default="{}")

    # External evidence frozen for replay (e.g., carbon price)
    price_evidence_json = Column(Text, default="[]")

    created_by_user_id = Column(Integer, nullable=True)

    # Immutability
    locked = Column(Boolean, default=False, index=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by_user_id = Column(Integer, nullable=True)

    # Sharing (client/verifier read-only)
    shared_with_client = Column(Boolean, default=False)

    project = relationship("Project", back_populates="snapshots")
    reports = relationship("Report", back_populates="snapshot", cascade="all, delete-orphan")
    methodology = relationship("Methodology", back_populates="snapshots")
    factor_set = relationship("FactorSet")
    monitoring_plan = relationship("MonitoringPlan")


class SnapshotDatasetLink(Base):
    """Snapshot ↔ DatasetUpload bağları (DB-level immutability & audit).

    Amaç:
      - Snapshot oluşturulduğu anda hangi dataset upload'larının kullanıldığını kayıt etmek
      - Kilitli snapshot'lar için datasetupload silme/güncelleme işlemlerini DB katmanında engellemek (SQLite trigger ile)
    """
    __tablename__ = "snapshot_dataset_links"
    __table_args__ = (UniqueConstraint("snapshot_id", "datasetupload_id", name="uq_snapshot_dataset_link"),)

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("calculationsnapshots.id"), nullable=False, index=True)
    datasetupload_id = Column(Integer, ForeignKey("datasetuploads.id"), nullable=False, index=True)

    dataset_type = Column(String(50), default="", index=True)
    sha256 = Column(String(64), default="", index=True)
    storage_uri = Column(String(500), default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)


class SnapshotFactorLink(Base):
    """Snapshot ↔ EmissionFactor bağları (factor governance / used_in_snapshots)."""
    __tablename__ = "snapshot_factor_links"
    __table_args__ = (UniqueConstraint("snapshot_id", "factor_id", name="uq_snapshot_factor_link"),)

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("calculationsnapshots.id"), nullable=False, index=True)
    factor_id = Column(Integer, ForeignKey("emissionfactors.id"), nullable=False, index=True)

    factor_type = Column(String(120), default="", index=True)
    region = Column(String(20), default="TR", index=True)
    year = Column(Integer, nullable=True, index=True)
    version = Column(String(50), default="v1")

    factor_hash = Column(String(64), default="", index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)


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

    # Evidence and action
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
    at = Column(DateTime(timezone=True), default=utcnow)

    action = Column(String(120), nullable=False, index=True)
    meta_json = Column(Text, default="{}")

    user_id = Column(Integer, nullable=True)
    company_id = Column(Integer, nullable=True)

    entity_type = Column(String(120), default="")
    entity_id = Column(String(120), default="")
