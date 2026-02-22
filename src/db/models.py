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
    monitoring_plans = relationship("MonitoringPlan", back_populates="facility", cascade="all, delete-orphan")


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True, index=True)
    name = Column(String(200), nullable=False)
    year = Column(Integer, default=2025)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    company = relationship("Company", back_populates="projects")
    facility = relationship("Facility", back_populates="projects")
    uploads = relationship("DatasetUpload", back_populates="project", cascade="all, delete-orphan")
    snapshots = relationship("CalculationSnapshot", back_populates="project", cascade="all, delete-orphan")


class DatasetUpload(Base):
    __tablename__ = "datasetuploads"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    dataset_type = Column(String(50), nullable=False)  # energy / production / materials
    uploaded_at = Column(DateTime(timezone=True), default=utcnow)
    original_filename = Column(String(300), nullable=False)

    sha256 = Column(String(64), nullable=False, index=True)
    schema_version = Column(String(50), default="v1")
    storage_uri = Column(String(500), nullable=False)

    uploaded_by_user_id = Column(Integer, nullable=True)

    # Evidence / lineage meta
    source = Column(String(200), default="")
    document_ref = Column(String(300), default="")

    project = relationship("Project", back_populates="uploads")


class Methodology(Base):
    __tablename__ = "methodologies"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, index=True)
    description = Column(Text, default="")
    scope = Column(String(200), default="CBAM+ETS")
    version = Column(String(50), default="v1")
    created_at = Column(DateTime(timezone=True), default=utcnow)

    snapshots = relationship("CalculationSnapshot", back_populates="methodology")


class EmissionFactor(Base):
    __tablename__ = "emissionfactors"

    id = Column(Integer, primary_key=True)

    # Önerilen factor_type convention (Paket A):
    # - ncv:<fuel_type>                 value: GJ / fuel_unit
    # - ef:<fuel_type>                  value: tCO2 / GJ
    # - of:<fuel_type>                  value: oxidation factor (0-1)
    # - grid:location                   value: kgCO2e / kWh
    # - grid:market                     value: kgCO2e / kWh
    factor_type = Column(String(120), nullable=False, index=True)
    value = Column(Float, nullable=False)
    unit = Column(String(80), nullable=False, default="")
    source = Column(String(300), default="")
    year = Column(Integer, nullable=True)
    version = Column(String(50), default="v1")
    region = Column(String(120), default="TR")

    created_at = Column(DateTime(timezone=True), default=utcnow)


class MonitoringPlan(Base):
    __tablename__ = "monitoringplans"

    id = Column(Integer, primary_key=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=False, index=True)

    method = Column(String(120), default="standard")
    tier_level = Column(String(50), default="Tier 2")
    data_source = Column(String(200), default="")
    qa_procedure = Column(Text, default="")
    responsible_person = Column(String(200), default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)

    facility = relationship("Facility", back_populates="monitoring_plans")


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

    # Hash chain (Paket A)
    previous_snapshot_hash = Column(String(64), nullable=True, index=True)

    # MRV / governance
    created_by_user_id = Column(Integer, nullable=True)
    locked = Column(Boolean, default=False)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by_user_id = Column(Integer, nullable=True)
    shared_with_client = Column(Boolean, default=False)

    project = relationship("Project", back_populates="snapshots")
    reports = relationship("Report", back_populates="snapshot", cascade="all, delete-orphan")
    methodology = relationship("Methodology", back_populates="snapshots")


class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("calculationsnapshots.id"), nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    report_type = Column(String(50), default="pdf")

    storage_uri = Column(String(500), nullable=False)
    sha256 = Column(String(64), nullable=False, index=True)

    snapshot = relationship("CalculationSnapshot", back_populates="reports")


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String(300), unique=True, nullable=False, index=True)
    password_hash = Column(String(200), nullable=False)
    role = Column(String(50), default="clientviewer")

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)
    company = relationship("Company", back_populates="users")
