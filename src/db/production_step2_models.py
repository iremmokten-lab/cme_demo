from __future__ import annotations

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint

from src.db.models import Base, utcnow
from src.db.job_models import Job  # compatibility re-export
from src.db.global_ready_models_step2 import AccessAuditLog  # single source of truth


class Step2DatasetApproval(Base):
    __tablename__ = "dataset_approvals"
    __table_args__ = (
        UniqueConstraint("upload_id", name="uq_dataset_approval_upload"),
    )

    id = Column(Integer, primary_key=True)
    upload_id = Column(Integer, ForeignKey("datasetuploads.id"), nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)
    notes = Column(Text, default="")
    submitted_by_user_id = Column(Integer, nullable=True)
    reviewed_by_user_id = Column(Integer, nullable=True)
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class Step2CbamPortalSubmission(Base):
    __tablename__ = "cbam_portal_submissions"
    __table_args__ = (
        UniqueConstraint("project_id", "period_year", "period_quarter", name="uq_cbam_portal_sub"),
    )

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    period_quarter = Column(Integer, nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)
    portal_reference = Column(String(120), default="", index=True)
    request_meta_json = Column(Text, default="{}")
    response_meta_json = Column(Text, default="{}")
    portal_zip_uri = Column(String(500), default="")
    cbam_xml_uri = Column(String(500), default="")
    schema_version = Column(String(80), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)


class Step2VerificationCaseState(Base):
    __tablename__ = "verification_case_state"
    __table_args__ = (
        UniqueConstraint("verification_case_id", name="uq_ver_case_state"),
    )

    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    status = Column(String(40), default="open", index=True)
    approval_chain_json = Column(Text, default="[]")
    created_at = Column(DateTime(timezone=True), default=utcnow)


class Step2VerificationSamplingItem(Base):
    __tablename__ = "verification_sampling_items"

    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    record_ref = Column(String(200), nullable=False, index=True)
    reason = Column(String(200), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)


class Step2VerificationFinding(Base):
    __tablename__ = "verification_findings"

    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    code = Column(String(80), nullable=False, index=True)
    severity = Column(String(20), default="medium", index=True)
    description = Column(Text, default="")
    status = Column(String(30), default="open", index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class Step2CorrectiveAction(Base):
    __tablename__ = "verification_corrective_actions"

    id = Column(Integer, primary_key=True)
    finding_id = Column(Integer, ForeignKey("verification_findings.id"), nullable=False, index=True)
    owner = Column(String(120), default="")
    action = Column(Text, default="")
    due_date = Column(String(40), default="")
    status = Column(String(30), default="open", index=True)
    evidence_doc_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class Step2CacheEntry(Base):
    __tablename__ = "cache_entries"

    id = Column(Integer, primary_key=True)
    key = Column(String(255), nullable=False, unique=True, index=True)
    value_json = Column(Text, default="{}")
    expires_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


__all__ = [
    "AccessAuditLog",
    "CacheEntry",
    "CbamPortalSubmission",
    "CorrectiveAction",
    "DatasetApproval",
    "Job",
    "VerificationCaseState",
    "VerificationFinding",
    "VerificationSamplingItem",
]


DatasetApproval = Step2DatasetApproval
CbamPortalSubmission = Step2CbamPortalSubmission
VerificationCaseState = Step2VerificationCaseState
VerificationSamplingItem = Step2VerificationSamplingItem
VerificationFinding = Step2VerificationFinding
CorrectiveAction = Step2CorrectiveAction
CacheEntry = Step2CacheEntry
