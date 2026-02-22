from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
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

    dataset_type = Column(String(50), nullable=False)  # energy / production
    uploaded_at = Column(DateTime(timezone=True), default=utcnow)

    original_filename = Column(String(300), nullable=False)
    sha256 = Column(String(64), nullable=False, index=True)
    schema_version = Column(String(50), default="v1")

    storage_uri = Column(String(500), nullable=False)
    uploaded_by_user_id = Column(Integer, nullable=True)

    project = relationship("Project", back_populates="uploads")


class CalculationSnapshot(Base):
    __tablename__ = "calculationsnapshots"

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    created_at = Column(DateTime(timezone=True), default=utcnow)
    engine_version = Column(String(50), default="engine-0.1.0")

    config_json = Column(Text, default="{}")
    input_hashes_json = Column(Text, default="{}")
    results_json = Column(Text, default="{}")
    result_hash = Column(String(64), nullable=False, index=True)

    project = relationship("Project", back_populates="snapshots")
    reports = relationship("Report", back_populates="snapshot", cascade="all, delete-orphan")


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
