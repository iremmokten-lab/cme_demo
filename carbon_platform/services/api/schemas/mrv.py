from pydantic import BaseModel

class MethodologyCreate(BaseModel):
    code: str
    name: str
    scope: str  # ETS/CBAM/MRV
    tier_level: str | None = None
    reg_reference: str
    description_tr: str | None = None

class MethodologyOut(BaseModel):
    id: str
    code: str
    name: str
    scope: str
    tier_level: str | None
    reg_reference: str
    description_tr: str | None
    status: str

class MonitoringPlanCreate(BaseModel):
    facility_id: str
    version: int = 1
    effective_from: str | None = None
    effective_to: str | None = None
    overall_notes_tr: str | None = None

class MonitoringPlanOut(BaseModel):
    id: str
    facility_id: str
    version: int
    status: str
    effective_from: str | None
    effective_to: str | None
    overall_notes_tr: str | None

class MonitoringMethodCreate(BaseModel):
    monitoring_plan_id: str
    emission_source: str  # fuel/electricity/process/material
    method_type: str      # Calculation/Measurement
    tier_level: str | None = None
    uncertainty_class: str | None = None
    methodology_id: str | None = None
    reference_standard: str | None = None

class MonitoringMethodOut(BaseModel):
    id: str
    monitoring_plan_id: str
    emission_source: str
    method_type: str
    tier_level: str | None
    uncertainty_class: str | None
    methodology_id: str | None
    reference_standard: str | None

class MeteringAssetCreate(BaseModel):
    facility_id: str
    asset_type: str
    serial_no: str | None = None
    calibration_schedule: str | None = None
    last_calibration_doc_id: str | None = None

class MeteringAssetOut(BaseModel):
    id: str
    facility_id: str
    asset_type: str
    serial_no: str | None
    calibration_schedule: str | None
    last_calibration_doc_id: str | None

class QAQCControlCreate(BaseModel):
    monitoring_plan_id: str
    control_type: str
    frequency: str | None = None
    acceptance_criteria_tr: str | None = None

class QAQCControlOut(BaseModel):
    id: str
    monitoring_plan_id: str
    control_type: str
    frequency: str | None
    acceptance_criteria_tr: str | None
