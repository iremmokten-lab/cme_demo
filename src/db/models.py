from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def utcnow():
    return datetime.now(timezone.utc)


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

    verification_cases = relationship("VerificationCase", back_populates="project", cascade="all, delete-orphan")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)

    email = Column(String(200), unique=True, nullable=False, index=True)
    password_hash = Column(String(200), nullable=False)

    role = Column(String(50), default="client")  # consultantadmin/consultant/client/verifier
    is_active = Column(Boolean, default=True)

    # login security
    failed_login_attempts = Column(Integer, default=0)
    locked_until = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)

    company = relationship("Company", back_populates="users")


class DatasetUpload(Base):
    __tablename__ = "datasetuploads"

    id = Column(Integer, primary_key=True)

    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    uploaded_at = Column(DateTime(timezone=True), default=utcnow)

    dataset_type = Column(String(50), nullable=False, index=True)  # energy/production/materials
    original_filename = Column(String(300), default="")
    storage_uri = Column(String(500), nullable=False)
    sha256 = Column(String(64), nullable=False, index=True)

    schema_version = Column(String(50), default="v1")

    data_quality_score = Column(Float, nullable=True)
    data_quality_report_json = Column(Text, default="{}")

    project = relationship("Project", back_populates="uploads")


class EvidenceDocument(Base):
    __tablename__ = "evidencedocuments"

    id = Column(Integer, primary_key=True)

    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    uploaded_at = Column(DateTime(timezone=True), default=utcnow)

    category = Column(String(80), default="general", index=True)
    original_filename = Column(String(300), default="")
    storage_uri = Column(String(500), nullable=False)
    sha256 = Column(String(64), nullable=False, index=True)

    notes = Column(Text, default="")

    project = relationship("Project", back_populates="evidence_docs")


class Methodology(Base):
    __tablename__ = "methodologies"

    id = Column(Integer, primary_key=True)

    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    name = Column(String(200), default="Default Methodology")
    version = Column(String(50), default="v1")

    config_json = Column(Text, default="{}")

    project = relationship("Project", back_populates="methodologies")
    snapshots = relationship("CalculationSnapshot", back_populates="methodology")


class MonitoringPlan(Base):
    __tablename__ = "monitoringplans"

    id = Column(Integer, primary_key=True)

    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    name = Column(String(200), default="Default Monitoring Plan")
    version = Column(String(50), default="v1")

    config_json = Column(Text, default="{}")

    project = relationship("Project", back_populates="monitoring_plans")


class CalculationSnapshot(Base):
    __tablename__ = "calculationsnapshots"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    engine_version = Column(String(50), default="engine-0.1.0")

    config_json = Column(Text, default="{}")
    input_hashes_json = Column(Text, default="{}")
    results_json = Column(Text, default="{}")

    methodology_id = Column(Integer, ForeignKey("methodologies.id"), nullable=True, index=True)

    result_hash = Column(String(64), nullable=False, index=True)

    previous_snapshot_hash = Column(String(64), nullable=True, index=True)

    created_by_user_id = Column(Integer, nullable=True)
    locked = Column(Boolean, default=False)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by_user_id = Column(Integer, nullable=True)
    shared_with_client = Column(Boolean, default=False)

    project = relationship("Project", back_populates="snapshots")
    reports = relationship("Report", back_populates="snapshot", cascade="all, delete-orphan")
    methodology = relationship("Methodology", back_populates="snapshots")


class VerificationCase(Base):
    """Verification Workflow (MVP) — 2018/2067 readiness.

    Case facility + period üzerinde açılır.
    Snapshot ile birebir bağ zorunlu değil; evidence pack içinde snapshot’ın
    project + period’ına göre dahil edilebilir.
    """

    __tablename__ = "verificationcases"

    id = Column(Integer, primary_key=True)

    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True, index=True)

    period_year = Column(Integer, nullable=False, default=2025, index=True)
    verifier_org = Column(String(200), default="")

    status = Column(String(50), default="planning", index=True)  # planning/fieldwork/findings/closed

    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)

    closed_at = Column(DateTime(timezone=True), nullable=True)

    # Faz 2: verifier sampling notları
    sampling_notes = Column(Text, default="")
    sampling_size = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="verification_cases")
    facility = relationship("Facility", back_populates="verification_cases")
    findings = relationship("VerificationFinding", back_populates="case", cascade="all, delete-orphan")


class VerificationFinding(Base):
    """Verification bulgusu (finding)."""

    __tablename__ = "verificationfindings"

    id = Column(Integer, primary_key=True)
    case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)

    severity = Column(String(20), default="minor", index=True)  # minor/major/critical
    description = Column(Text, default="")
    corrective_action = Column(Text, default="")

    due_date = Column(String(30), default="")  # MVP: string (YYYY-MM-DD)
    status = Column(String(30), default="open", index=True)  # open/in_progress/closed

    created_at = Column(DateTime(timezone=True), default=utcnow)
    closed_at = Column(DateTime(timezone=True), nullable=True)

    case = relationship("VerificationCase", back_populates="findings")


class Alert(Base):
    """Faz 2: Alerts / Tasks (basit).

    Uyum FAIL/WARN, eksik evidence, data quality anomalileri gibi olaylar için.
    """

    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)
    snapshot_id = Column(Integer, ForeignKey("calculationsnapshots.id"), nullable=True, index=True)

    alert_type = Column(String(120), default="generic", index=True)
    severity = Column(String(20), default="warn", index=True)  # info/warn/critical
    title = Column(String(300), default="")
    message = Column(Text, default="")

    status = Column(String(30), default="open", index=True)  # open/resolved

    meta_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow, index=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_by_user_id = Column(Integer, nullable=True)


class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("calculationsnapshots.id"), nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    report_type = Column(String(50), default="pdf")

    storage_uri = Column(String(500), nullable=False)
    sha256 = Column(String(64), nullable=False, index=True)

    snapshot = relationship("CalculationSnapshot", back_populates="reports")


class AuditEvent(Base):
    __tablename__ = "auditevents"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), default=utcnow, index=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)

    event_type = Column(String(120), nullable=False, index=True)
    entity_type = Column(String(120), default="", index=True)  # snapshot/report/evidence/upload
    entity_id = Column(Integer, nullable=True, index=True)

    details_json = Column(Text, default="{}")
