from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, LargeBinary, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)

    projects: Mapped[list["Project"]] = relationship(
        back_populates="company",
        cascade="all, delete-orphan",
    )


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    name: Mapped[str] = mapped_column(String(200), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    company: Mapped["Company"] = relationship(back_populates="projects")

    uploads: Mapped[list["DatasetUpload"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )
    snapshots: Mapped[list["CalculationSnapshot"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )


class DatasetUpload(Base):
    __tablename__ = "dataset_uploads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"), index=True)

    dataset_type: Mapped[str] = mapped_column(String(50))  # energy / production
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    original_filename: Mapped[str] = mapped_column(String(255))
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    schema_version: Mapped[str] = mapped_column(String(50), default="v1")

    # MVP sprint1: bytes DB'de tutuluyor (sonra storage_uri'ye geçersin)
    content_bytes: Mapped[bytes] = mapped_column(LargeBinary)

    project: Mapped["Project"] = relationship(back_populates="uploads")


class CalculationSnapshot(Base):
    __tablename__ = "calculation_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    engine_version: Mapped[str] = mapped_column(String(50))

    config_json: Mapped[str] = mapped_column(Text)
    input_hashes_json: Mapped[str] = mapped_column(Text)
    results_json: Mapped[str] = mapped_column(Text)
    result_hash: Mapped[str] = mapped_column(String(64), index=True)

    project: Mapped["Project"] = relationship(back_populates="snapshots")
