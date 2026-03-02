from __future__ import annotations

"""ERP Automation DB models.

Several UI pages and services expect these models to exist under
`src.db.erp_automation_models`.

The repository already contains `src.db.erp_models`, but it was a different/older
schema. To avoid breaking the app, we provide the expected models here.

Notes:
- SQLite is used in the demo; keep schemas simple and migration-friendly.
- Models are imported during `init_db()` so that `Base.metadata.create_all()`
  sees all tables and ForeignKeys.
"""

from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, Index, UniqueConstraint

from src.db.models import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ERPConnection(Base):
    __tablename__ = "erp_connections"
    __table_args__ = (
        Index("ix_erp_conn_project_status", "project_id", "status"),
        UniqueConstraint("project_id", "name", name="uq_erp_conn_project_name"),
    )

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    name = Column(String(200), nullable=False)
    kind = Column(String(30), default="rest")  # rest/odata/file
    base_url = Column(String(500), default="")

    auth_json = Column(Text, default="{}")
    config_json = Column(Text, default="{}")

    status = Column(String(20), default="active")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)


class ERPMapping(Base):
    __tablename__ = "erp_mappings"
    __table_args__ = (
        Index("ix_erp_map_project_dataset", "project_id", "dataset_type"),
        UniqueConstraint("project_id", "dataset_type", "version", name="uq_erp_map_project_dataset_ver"),
    )

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)

    dataset_type = Column(String(50), nullable=False)
    version = Column(Integer, default=1)
    status = Column(String(20), default="draft")  # draft/approved/locked

    mapping_json = Column(Text, default="{}")
    notes = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)


class ERPIngestionRun(Base):
    __tablename__ = "erp_ingestion_runs"
    __table_args__ = (
        Index("ix_erp_run_project_time", "project_id", "started_at"),
        Index("ix_erp_run_conn_time", "connection_id", "started_at"),
    )

    id = Column(Integer, primary_key=True)

    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    connection_id = Column(Integer, ForeignKey("erp_connections.id"), nullable=False, index=True)

    dataset_type = Column(String(50), nullable=False, index=True)
    mapping_version = Column(Integer, default=1)

    status = Column(String(20), default="running")  # running/success/failed
    started_at = Column(DateTime(timezone=True), default=utcnow)
    finished_at = Column(DateTime(timezone=True), nullable=True)

    raw_count = Column(Integer, default=0)
    normalized_count = Column(Integer, default=0)
    raw_sha256 = Column(String(64), default="")
    normalized_sha256 = Column(String(64), default="")

    output_upload_id = Column(Integer, ForeignKey("datasetuploads.id"), nullable=True, index=True)
    error_text = Column(Text, default="")


class ERPDeadLetter(Base):
    __tablename__ = "erp_dead_letter"
    __table_args__ = (Index("ix_erp_dlq_run", "run_id"),)

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("erp_ingestion_runs.id"), nullable=False, index=True)

    dataset_type = Column(String(50), default="", index=True)
    reason = Column(String(120), default="")
    record_json = Column(Text, default="{}")

    created_at = Column(DateTime(timezone=True), default=utcnow)


class ERPJob(Base):
    """Very small background-job table used by the demo queue."""

    __tablename__ = "erp_jobs"
    __table_args__ = (Index("ix_erp_jobs_status", "status"),)

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)

    kind = Column(String(80), nullable=False)
    status = Column(String(20), default="queued")  # queued/running/success/failed

    payload_json = Column(Text, default="{}")
    result_json = Column(Text, default="{}")
    error = Column(Text, default="")

    created_at = Column(DateTime(timezone=True), default=utcnow)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)

    is_locked = Column(Boolean, default=False)
