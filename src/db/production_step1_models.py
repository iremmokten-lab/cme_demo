from __future__ import annotations

from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, UniqueConstraint, Boolean
from src.db.models import Base, utcnow

class AccessAuditLog(Base):
    __tablename__ = "access_audit_logs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True, index=True)
    action = Column(String(120), nullable=False, index=True)
    resource = Column(String(200), default="")
    ip = Column(String(80), default="")
    user_agent = Column(String(300), default="")
    meta_json = Column(Text, default="{}")
    created_at = Column(DateTime(timezone=True), default=utcnow)

class DatasetApproval(Base):
    __tablename__ = "dataset_approvals"
    __table_args__ = (UniqueConstraint("upload_id", name="uq_dataset_approval_upload"),)
    id = Column(Integer, primary_key=True)
    upload_id = Column(Integer, ForeignKey("datasetuploads.id"), nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)  # draft/submitted/approved/rejected
    notes = Column(Text, default="")
    submitted_by_user_id = Column(Integer, nullable=True)
    reviewed_by_user_id = Column(Integer, nullable=True)
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)

class CbamPortalSubmission(Base):
    __tablename__ = "cbam_portal_submissions"
    __table_args__ = (UniqueConstraint("project_id","period_year","period_quarter", name="uq_cbam_portal_sub"),)
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    period_quarter = Column(Integer, nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)  # draft/ready/submitted/accepted/rejected/corrected
    portal_reference = Column(String(120), default="", index=True)
    request_meta_json = Column(Text, default="{}")
    response_meta_json = Column(Text, default="{}")
    portal_zip_uri = Column(String(500), default="")
    cbam_xml_uri = Column(String(500), default="")
    schema_version = Column(String(80), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)

class VerificationCaseState(Base):
    __tablename__ = "verification_case_state"
    __table_args__ = (UniqueConstraint("verification_case_id", name="uq_ver_case_state"),)
    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    status = Column(String(40), default="open", index=True)  # open/in_review/closed
    approval_chain_json = Column(Text, default="[]")
    created_at = Column(DateTime(timezone=True), default=utcnow)

class VerificationSamplingItem(Base):
    __tablename__ = "verification_sampling_items"
    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    record_ref = Column(String(200), nullable=False, index=True)
    reason = Column(String(200), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)

class VerificationFinding(Base):
    __tablename__ = "verification_findings"
    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    code = Column(String(80), nullable=False, index=True)
    severity = Column(String(20), default="medium", index=True)  # low/medium/high
    description = Column(Text, default="")
    status = Column(String(30), default="open", index=True)  # open/closed
    created_at = Column(DateTime(timezone=True), default=utcnow)

class CorrectiveAction(Base):
    __tablename__ = "verification_corrective_actions"
    id = Column(Integer, primary_key=True)
    finding_id = Column(Integer, ForeignKey("verification_findings.id"), nullable=False, index=True)
    owner = Column(String(120), default="")
    action = Column(Text, default="")
    due_date = Column(String(40), default="")
    status = Column(String(30), default="open", index=True)  # open/done
    evidence_doc_id = Column(Integer, ForeignKey("evidencedocuments.id"), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)
