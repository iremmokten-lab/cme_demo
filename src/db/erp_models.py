from src.db.erp_automation_models import ERPConnection, ERPMapping  # noqa: F401

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text
from src.db.models import Base, utcnow


class ERPJobRun(Base):
    __tablename__ = "erp_job_runs"

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    connection_id = Column(Integer, ForeignKey("erp_connections.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    status = Column(String(30), default="running")
    summary_json = Column(Text, default="{}")
    error_text = Column(Text, default="")
    started_at = Column(DateTime(timezone=True), default=utcnow)
    finished_at = Column(DateTime(timezone=True), nullable=True)
