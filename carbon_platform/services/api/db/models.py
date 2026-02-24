import uuid
from datetime import datetime
from sqlalchemy import String, DateTime, Boolean, ForeignKey, Numeric, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from services.api.db.base import Base

def gen_uuid() -> uuid.UUID:
    return uuid.uuid4()

class Tenant(Base):
    __tablename__ = "tenant"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    country: Mapped[str | None] = mapped_column(String(80))
    currency: Mapped[str | None] = mapped_column(String(10))
    timezone: Mapped[str | None] = mapped_column(String(64))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

class User(Base):
    __tablename__ = "app_user"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    mfa_enabled: Mapped[bool] = mapped_column(Boolean, default=False)

    tenant = relationship("Tenant")

    __table_args__ = (Index("ux_user_tenant_email", "tenant_id", "email", unique=True),)

class Role(Base):
    __tablename__ = "role"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    name: Mapped[str] = mapped_column(String(80), nullable=False, unique=True)

class UserRole(Base):
    __tablename__ = "user_role"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("app_user.id"), nullable=False, index=True)
    role_name: Mapped[str] = mapped_column(String(80), nullable=False)

    __table_args__ = (Index("ux_user_role", "tenant_id", "user_id", "role_name", unique=True),)

class Facility(Base):
    __tablename__ = "facility"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    country: Mapped[str | None] = mapped_column(String(80))
    ets_in_scope: Mapped[bool] = mapped_column(Boolean, default=False)
    cbam_in_scope: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (Index("ix_facility_tenant_name", "tenant_id", "name"),)

# -----------------------------
# Methodology Registry (Tier/Method governance)
# -----------------------------
class Methodology(Base):
    """
    Regulation-grade registry: hesap metodolojisi + tier + sistem sınırı + reg referansı.
    """
    __tablename__ = "methodology"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)

    code: Mapped[str] = mapped_column(String(80), nullable=False)   # örn: ETS_FUEL_COMBUSTION_TIER2
    name: Mapped[str] = mapped_column(String(200), nullable=False)  # Türkçe isim
    scope: Mapped[str] = mapped_column(String(20), nullable=False)  # ETS/CBAM/MRV
    tier_level: Mapped[str | None] = mapped_column(String(20))      # Tier 1/2/3 vb.
    reg_reference: Mapped[str] = mapped_column(String(200), nullable=False)  # (EU) 2018/2066 vb.
    description_tr: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), default="active")  # active/retired

    __table_args__ = (Index("ux_methodology_tenant_code", "tenant_id", "code", unique=True),)

# -----------------------------
# Monitoring Plan (MRV Layer)
# -----------------------------
class MonitoringPlan(Base):
    __tablename__ = "monitoring_plan"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    facility_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("facility.id"), nullable=False, index=True)

    version: Mapped[int] = mapped_column(nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(20), default="draft")  # draft/approved/retired
    effective_from: Mapped[str | None] = mapped_column(String(10))    # YYYY-MM-DD
    effective_to: Mapped[str | None] = mapped_column(String(10))

    overall_notes_tr: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ux_monitoring_plan_version", "tenant_id", "facility_id", "version", unique=True),
        Index("ix_monitoring_plan_status", "tenant_id", "facility_id", "status"),
    )

class MonitoringMethod(Base):
    __tablename__ = "monitoring_method"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    monitoring_plan_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("monitoring_plan.id"), nullable=False, index=True)

    emission_source: Mapped[str] = mapped_column(String(40), nullable=False)  # fuel/electricity/process/material
    method_type: Mapped[str] = mapped_column(String(60), nullable=False)      # örn: "Calculation", "Measurement"
    tier_level: Mapped[str | None] = mapped_column(String(20))               # Tier 1/2/3
    uncertainty_class: Mapped[str | None] = mapped_column(String(40))
    methodology_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("methodology.id"))

    reference_standard: Mapped[str | None] = mapped_column(String(120))      # ISO/GHG/IPCC referansı (opsiyonel)

    __table_args__ = (Index("ix_monitoring_method_plan_source", "tenant_id", "monitoring_plan_id", "emission_source"),)

class MeteringAsset(Base):
    __tablename__ = "metering_asset"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    facility_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("facility.id"), nullable=False, index=True)

    asset_type: Mapped[str] = mapped_column(String(40), nullable=False)  # gas_meter, electricity_meter, flow_meter
    serial_no: Mapped[str | None] = mapped_column(String(120))
    calibration_schedule: Mapped[str | None] = mapped_column(String(80))  # örn: yearly
    last_calibration_doc_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

    __table_args__ = (Index("ix_metering_asset_facility_type", "tenant_id", "facility_id", "asset_type"),)

class QAQCControl(Base):
    __tablename__ = "qa_qc_control"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    monitoring_plan_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("monitoring_plan.id"), nullable=False, index=True)

    control_type: Mapped[str] = mapped_column(String(80), nullable=False)      # completeness_check, plausibility_check...
    frequency: Mapped[str | None] = mapped_column(String(40))                 # daily/monthly/quarterly
    acceptance_criteria_tr: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (Index("ix_qaqc_plan_type", "tenant_id", "monitoring_plan_id", "control_type"),)

# -----------------------------
# Factors / Activity / Evidence / Jobs (Önceki çekirdek aynı)
# -----------------------------
class FactorSource(Base):
    __tablename__ = "factor_source"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    publisher: Mapped[str] = mapped_column(String(200), nullable=False)
    document_url: Mapped[str | None] = mapped_column(String(1000))
    publication_date: Mapped[str | None] = mapped_column(String(32))
    jurisdiction: Mapped[str | None] = mapped_column(String(80))

class EmissionFactor(Base):
    __tablename__ = "emission_factor"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    factor_type: Mapped[str] = mapped_column(String(40), nullable=False)  # NCV, EF, grid, process, oxidation
    value: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    unit: Mapped[str] = mapped_column(String(40), nullable=False)
    gas: Mapped[str | None] = mapped_column(String(20))  # CO2, CH4, N2O, CO2e
    source_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("factor_source.id"))
    version: Mapped[int] = mapped_column(default=1)
    valid_from: Mapped[str | None] = mapped_column(String(32))
    valid_to: Mapped[str | None] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(20), default="proposed")  # proposed/approved/retired

    __table_args__ = (Index("ix_factor_tenant_type_status", "tenant_id", "factor_type", "status"),)

class FactorApproval(Base):
    __tablename__ = "factor_approval"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    factor_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("emission_factor.id"), nullable=False, index=True)
    submitted_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("app_user.id"), nullable=False)
    reviewed_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("app_user.id"))
    decision: Mapped[str | None] = mapped_column(String(20))  # approved/rejected
    review_notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

class ActivityRecord(Base):
    __tablename__ = "activity_record"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    facility_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("facility.id"), nullable=False, index=True)
    period_start: Mapped[str] = mapped_column(String(10), nullable=False)  # YYYY-MM-DD
    period_end: Mapped[str] = mapped_column(String(10), nullable=False)
    source_system: Mapped[str | None] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(20), default="raw")  # raw/validated/locked

    __table_args__ = (Index("ix_activity_facility_period", "tenant_id", "facility_id", "period_start", "period_end"),)

class FuelActivity(Base):
    __tablename__ = "fuel_activity"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    activity_record_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("activity_record.id"), nullable=False, index=True)
    fuel_type: Mapped[str] = mapped_column(String(80), nullable=False)
    quantity: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)
    ncv_factor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("emission_factor.id"))
    ef_factor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("emission_factor.id"))
    oxidation_factor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("emission_factor.id"))
    doc_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

class ElectricityActivity(Base):
    __tablename__ = "electricity_activity"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    activity_record_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("activity_record.id"), nullable=False, index=True)
    kwh: Mapped[float] = mapped_column(Numeric(18, 3), nullable=False)
    grid_factor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("emission_factor.id"))
    market_based_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    doc_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

class ProcessActivity(Base):
    __tablename__ = "process_activity"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    activity_record_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("activity_record.id"), nullable=False, index=True)
    process_type: Mapped[str] = mapped_column(String(80), nullable=False)
    production_qty: Mapped[float] = mapped_column(Numeric(18, 3), nullable=False)
    unit: Mapped[str] = mapped_column(String(20), nullable=False)
    process_factor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("emission_factor.id"))
    doc_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))

class Document(Base):
    __tablename__ = "document"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    s3_key: Mapped[str] = mapped_column(String(500), nullable=False)
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    doc_type: Mapped[str] = mapped_column(String(80), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    uploaded_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("app_user.id"), nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

class CalculationRun(Base):
    __tablename__ = "calculation_run"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    facility_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("facility.id"), nullable=False, index=True)
    activity_record_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("activity_record.id"), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), default="completed")
    result_json: Mapped[str] = mapped_column(Text, nullable=False)
    result_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

class EvidencePack(Base):
    __tablename__ = "evidence_pack"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    period_start: Mapped[str] = mapped_column(String(10), nullable=False)
    period_end: Mapped[str] = mapped_column(String(10), nullable=False)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)  # CBAM/ETS/MRV
    status: Mapped[str] = mapped_column(String(20), default="built")  # building/built/failed
    manifest_s3_key: Mapped[str] = mapped_column(String(500), nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("app_user.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

class EvidenceItem(Base):
    __tablename__ = "evidence_item"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    evidence_pack_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("evidence_pack.id"), nullable=False, index=True)
    item_type: Mapped[str] = mapped_column(String(40), nullable=False)
    s3_key: Mapped[str] = mapped_column(String(500), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False)

class Snapshot(Base):
    __tablename__ = "snapshot"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(80), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(80), nullable=False)
    version: Mapped[int] = mapped_column(nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (Index("ux_snapshot_entity_version", "tenant_id", "entity_type", "entity_id", "version", unique=True),)

class AuditLog(Base):
    __tablename__ = "audit_log"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    actor_user_id: Mapped[str] = mapped_column(String(80), nullable=False)
    action: Mapped[str] = mapped_column(String(80), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(80), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(80), nullable=False)
    before_hash: Mapped[str | None] = mapped_column(String(64))
    after_hash: Mapped[str | None] = mapped_column(String(64))
    ip: Mapped[str | None] = mapped_column(String(80))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

class Job(Base):
    __tablename__ = "job"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=gen_uuid)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenant.id"), nullable=False, index=True)
    job_type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)  # queued/running/succeeded/failed
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    result: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (Index("ix_job_tenant_status", "tenant_id", "status"),)
