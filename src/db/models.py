from __future__ import annotations

import datetime
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
from sqlalchemy.orm import relationship

from src.db.session import Base


# ----------------------------
# Core auth / tenancy
# ----------------------------
class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, unique=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    users = relationship("User", back_populates="company")
    facilities = relationship("Facility", back_populates="company")
    projects = relationship("Project", back_populates="company")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String(200), nullable=False, unique=True)
    password_hash = Column(String(500), nullable=False)
    role = Column(String(50), nullable=False, default="client")  # consultant / client / verifier
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    company = relationship("Company", back_populates="users")


# ----------------------------
# Facilities / Projects
# ----------------------------
class Facility(Base):
    __tablename__ = "facilities"

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    name = Column(String(200), nullable=False)
    country_code = Column(String(10), nullable=True, default="TR")
    country = Column(String(100), nullable=True, default="TR")  # backward compat
    sector = Column(String(200), nullable=True, default="")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    company = relationship("Company", back_populates="facilities")
    projects = relationship("Project", back_populates="facility")
    monitoring_plans = relationship("MonitoringPlan", back_populates="facility")


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True)

    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    year = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    company = relationship("Company", back_populates="projects")
    facility = relationship("Facility", back_populates="projects")
    uploads = relationship("DatasetUpload", back_populates="project")
    snapshots = relationship("CalculationSnapshot", back_populates="project")


# ----------------------------
# Uploads & datasets
# ----------------------------
class DatasetUpload(Base):
    __tablename__ = "dataset_uploads"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)

    dataset_type = Column(String(50), nullable=False)  # energy / production / materials
    original_filename = Column(String(300), nullable=True)
    storage_uri = Column(Text, nullable=False)

    sha256 = Column(String(64), nullable=True)
    schema_version = Column(String(50), nullable=True, default="v1")

    data_quality_score = Column(Float, nullable=True)
    data_quality_report_json = Column(Text, nullable=True)

    uploaded_at = Column(DateTime, default=datetime.datetime.utcnow)

    project = relationship("Project", back_populates="uploads")


# ----------------------------
# MRV / factors / methodology
# ----------------------------
class EmissionFactor(Base):
    __tablename__ = "emission_factors"

    id = Column(Integer, primary_key=True)
    factor_type = Column(String(100), nullable=False)  # e.g., fuel_co2, grid_factor
    value = Column(Float, nullable=False, default=0.0)
    unit = Column(String(100), nullable=True)
    source = Column(String(500), nullable=True)

    year = Column(Integer, nullable=True)
    version = Column(String(50), nullable=True, default="v1")
    region = Column(String(50), nullable=True, default="TR")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Methodology(Base):
    __tablename__ = "methodologies"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, default="Default Methodology")
    description = Column(Text, nullable=True)
    scope = Column(String(200), nullable=True, default="CBAM/ETS")
    version = Column(String(50), nullable=True, default="v1")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class MonitoringPlan(Base):
    __tablename__ = "monitoring_plans"

    id = Column(Integer, primary_key=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=False)

    method = Column(String(100), nullable=True, default="")
    tier_level = Column(String(50), nullable=True, default="")

    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

    facility = relationship("Facility", back_populates="monitoring_plans")


# ----------------------------
# Snapshots / reports / evidence
# ----------------------------
class CalculationSnapshot(Base):
    __tablename__ = "calculation_snapshots"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)

    engine_version = Column(String(100), nullable=True, default="")
    result_hash = Column(String(128), nullable=True)

    config_json = Column(Text, nullable=True)
    input_hashes_json = Column(Text, nullable=True)
    results_json = Column(Text, nullable=True)

    methodology_id = Column(Integer, ForeignKey("methodologies.id"), nullable=True)

    previous_snapshot_hash = Column(String(128), nullable=True)

    locked = Column(Boolean, default=False)
    shared_with_client = Column(Boolean, default=False)

    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    project = relationship("Project", back_populates="snapshots")


class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("calculation_snapshots.id"), nullable=False)
    storage_uri = Column(Text, nullable=False)
    sha256 = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class EvidenceDocument(Base):
    __tablename__ = "evidence_documents"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    category = Column(String(50), nullable=True, default="documents")

    original_filename = Column(String(300), nullable=True)
    storage_uri = Column(Text, nullable=False)
    sha256 = Column(String(64), nullable=True)

    uploaded_at = Column(DateTime, default=datetime.datetime.utcnow)


# ----------------------------
# Verification workflow
# ----------------------------
class VerificationCase(Base):
    __tablename__ = "verification_cases"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True)
    period_year = Column(Integer, nullable=False, default=2025)

    verifier_org = Column(String(200), nullable=True)
    status = Column(String(50), nullable=True, default="open")

    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

    # Faz 2 - sampling notes / size (migration-like eklenir)
    sampling_notes = Column(Text, nullable=True)
    sampling_size = Column(Integer, nullable=True)

    findings = relationship("VerificationFinding", back_populates="case")


class VerificationFinding(Base):
    __tablename__ = "verification_findings"

    id = Column(Integer, primary_key=True)
    case_id = Column(Integer, ForeignKey("verification_cases.id"), nullable=False)

    severity = Column(String(50), nullable=True, default="minor")
    description = Column(Text, nullable=True)
    corrective_action = Column(Text, nullable=True)
    due_date = Column(String(50), nullable=True)
    status = Column(String(50), nullable=True, default="open")

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

    case = relationship("VerificationCase", back_populates="findings")


# ----------------------------
# Alerts (Faz 2)
# ----------------------------
class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True)
    snapshot_id = Column(Integer, ForeignKey("calculation_snapshots.id"), nullable=True)

    alert_type = Column(String(100), nullable=False, default="generic")
    severity = Column(String(20), nullable=False, default="warn")  # info/warn/critical
    title = Column(String(300), nullable=True)
    message = Column(Text, nullable=True)

    status = Column(String(20), nullable=False, default="open")  # open/resolved
    meta_json = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)
    resolved_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
