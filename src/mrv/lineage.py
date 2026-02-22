from datetime import datetime, timezone
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, DateTime, ForeignKey, Text

def utcnow():
    return datetime.now(timezone.utc)

class Base(DeclarativeBase):
    pass

class Company(Base):
    __tablename__ = "companies"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)

    facilities: Mapped[list["Facility"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    projects: Mapped[list["Project"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    users: Mapped[list["User"]] = relationship(back_populates="company", cascade="all, delete-orphan")

class Facility(Base):
    __tablename__ = "facilities"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    country: Mapped[str] = mapped_column(String(100), default="TR")
    sector: Mapped[str] = mapped_column(String(200), default="")

    company: Mapped["Company"] = relationship(back_populates="facilities")
    projects: Mapped[list["Project"]] = relationship(back_populates="facility")

class Project(Base):
    __tablename__ = "projects"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), nullable=False)
    facility_id: Mapped[int | None] = mapped_column(ForeignKey("facilities.id"), nullable=True)

    name: Mapped[str] = mapped_column(String(200), nullable=False)
    year: Mapped[int] = mapped_column(Integer, default=2025)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    company: Mapped["Company"] = relationship(back_populates="projects")
    facility: Mapped["Facility"] = relationship(back_populates="projects")
    uploads: Mapped[list["DatasetUpload"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    snapshots: Mapped[list["CalculationSnapshot"]] = relationship(back_populates="project", cascade="all, delete-orphan")

class DatasetUpload(Base):
    __tablename__ = "datasetuploads"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"), nullable=False)

    dataset_type: Mapped[str] = mapped_column(String(50), nullable=False)  # energy, production
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    original_filename: Mapped[str] = mapped_column(String(300), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(50), default="v1")
    storage_uri: Mapped[str] = mapped_column(String(500), nullable=False)  # local path

    uploaded_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    project: Mapped["Project"] = relationship(back_populates="uploads")

class CalculationSnapshot(Base):
    __tablename__ = "calculationsnapshots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"), nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    engine_version: Mapped[str] = mapped_column(String(50), default="engine-0.1.0")

    config_json: Mapped[str] = mapped_column(Text, default="{}")
    input_hashes_json: Mapped[str] = mapped_column(Text, default="{}")
    results_json: Mapped[str] = mapped_column(Text, default="{}")
    result_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    project: Mapped["Project"] = relationship(back_populates="snapshots")
    reports: Mapped[list["Report"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")

class Report(Base):
    __tablename__ = "reports"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("calculationsnapshots.id"), nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    report_type: Mapped[str] = mapped_column(String(50), default="pdf")
    storage_uri: Mapped[str] = mapped_column(String(500), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)

    snapshot: Mapped["CalculationSnapshot"] = relationship(back_populates="reports")

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    email: Mapped[str] = mapped_column(String(300), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(200), nullable=False)

    role: Mapped[str] = mapped_column(String(50), default="clientviewer")
    company_id: Mapped[int | None] = mapped_column(ForeignKey("companies.id"), nullable=True)

    company: Mapped["Company"] = relationship(back_populates="users")
