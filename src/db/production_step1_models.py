from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint

from src.db.models import Base, utcnow


class AccessAuditLogStep1(Base):
    __tablename__ = "access_audit_logs"
    __table_args__ = {"extend_existing": True}

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


class DatasetApprovalStep1(Base):
    __tablename__ = "dataset_approvals"
    __table_args__ = (UniqueConstraint("upload_id", name="uq_dataset_approval_upload"), {"extend_existing": True})

    id = Column(Integer, primary_key=True)
    upload_id = Column(Integer, ForeignKey("datasetuploads.id"), nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)
    notes = Column(Text, default="")
    submitted_by_user_id = Column(Integer, nullable=True)
    reviewed_by_user_id = Column(Integer, nullable=True)
    reviewed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class CbamPortalSubmissionStep1(Base):
    __tablename__ = "cbam_portal_submissions"
    __table_args__ = (UniqueConstraint("project_id", "period_year", "period_quarter", name="uq_cbam_portal_sub"), {"extend_existing": True})

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


class VerificationCaseStateStep1(Base):
    __tablename__ = "verification_case_state"
    __table_args__ = (UniqueConstraint("verification_case_id", name="uq_ver_case_state"), {"extend_existing": True})

    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    status = Column(String(40), default="open", index=True)
    approval_chain_json = Column(Text, default="[]")
    created_at = Column(DateTime(timezone=True), default=utcnow)


class VerificationSamplingItemStep1(Base):
    __tablename__ = "verification_sampling_items"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    record_ref = Column(String(200), nullable=False, index=True)
    reason = Column(String(200), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)


class VerificationFindingStep1(Base):
    __tablename__ = "verification_findings"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    verification_case_id = Column(Integer, ForeignKey("verificationcases.id"), nullable=False, index=True)
    code = Column(String(80), nullable=False, index=True)
    severity = Column(String(20), default="medium", index=True)
    description = Column(Text, default="")
    status = Column(String(30), default="open", index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class CorrectiveActionStep1(Base):
    __tablename__ = "verification_corrective_actions"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    finding_id = Column(Integer, ForeignKey("verification_findings.id"), nullable=False, index=True)
    owner = Column(String(120), default="")
    action = Column(Text, default="")
    due_date = Column(String(40), default="")
    status = Column(String(30), default="open", index=True)
    evidence_doc_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class ProducerStep1(Base):
    __tablename__ = "producers"
    __table_args__ = (UniqueConstraint("company_id", "name", name="uq_producer_company_name"), {"extend_existing": True})

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    name = Column(String(200), nullable=False, index=True)
    country = Column(String(100), default="TR")
    vat_or_tax_id = Column(String(80), default="")
    contact_email = Column(String(200), default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)


class ProducerAttestationStep1(Base):
    __tablename__ = "producer_attestations"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    producer_id = Column(Integer, ForeignKey("producers.id"), nullable=False, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    period_quarter = Column(Integer, nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)
    declaration_json = Column(Text, default="{}")
    signed_by = Column(String(200), default="")
    signed_at = Column(DateTime(timezone=True), nullable=True)
    evidence_doc_id = Column(Integer, ForeignKey("evidence_documents.id"), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=utcnow)


class CBAMQuarterlySubmissionStep1(Base):
    __tablename__ = "cbam_quarterly_submissions"
    __table_args__ = (UniqueConstraint("project_id", "period_year", "period_quarter", name="uq_cbam_sub_period"), {"extend_existing": True})

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    period_quarter = Column(Integer, nullable=False, index=True)
    status = Column(String(40), default="draft", index=True)
    cbam_xml_uri = Column(String(500), default="")
    portal_zip_uri = Column(String(500), default="")
    schema_version = Column(String(40), default="")
    xsd_hash = Column(String(80), default="")
    notes = Column(Text, default="")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    updated_at = Column(DateTime(timezone=True), default=utcnow)


class MonitoringPlanVersionStep1(Base):
    __tablename__ = "monitoring_plan_versions"
    __table_args__ = (UniqueConstraint("project_id", "period_year", "version", name="uq_mp_project_year_version"), {"extend_existing": True})

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False, index=True)
    facility_id = Column(Integer, ForeignKey("facilities.id"), nullable=True, index=True)
    period_year = Column(Integer, nullable=False, index=True)
    version = Column(Integer, nullable=False, default=1)
    status = Column(String(30), default="draft", index=True)
    plan_json = Column(Text, default="{}")
    created_at = Column(DateTime(timezone=True), default=utcnow)
    created_by_user_id = Column(Integer, nullable=True)


AccessAuditLog = AccessAuditLogStep1
DatasetApproval = DatasetApprovalStep1
CbamPortalSubmission = CbamPortalSubmissionStep1
VerificationCaseState = VerificationCaseStateStep1
VerificationSamplingItem = VerificationSamplingItemStep1
VerificationFinding = VerificationFindingStep1
CorrectiveAction = CorrectiveActionStep1
Producer = ProducerStep1
ProducerAttestation = ProducerAttestationStep1
CBAMQuarterlySubmission = CBAMQuarterlySubmissionStep1
MonitoringPlanVersion = MonitoringPlanVersionStep1
