from __future__ import annotations

from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, DateTime, Text, Index
from src.db.models import Base

def utcnow():
    return datetime.now(timezone.utc)

class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (Index("ix_jobs_status", "status"), Index("ix_jobs_type", "job_type"),)
    id = Column(Integer, primary_key=True)
    job_type = Column(String(80), nullable=False)
    payload_json = Column(Text, default="{}")
    status = Column(String(40), default="queued")  # queued/running/succeeded/failed
    result_json = Column(Text, default="{}")
    error = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)
