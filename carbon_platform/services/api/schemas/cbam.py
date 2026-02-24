from pydantic import BaseModel

class ProductionRecordCreate(BaseModel):
    activity_record_id: str
    product_id: str
    quantity: float
    unit: str = "ton"
    doc_id: str | None = None

class MaterialInputCreate(BaseModel):
    activity_record_id: str
    product_id: str
    material_id: str
    quantity: float
    unit: str = "ton"
    embedded_factor_id: str | None = None
    doc_id: str | None = None

class ExportRecordCreate(BaseModel):
    facility_id: str
    product_id: str
    period_start: str
    period_end: str
    export_qty: float
    unit: str = "ton"
    destination: str | None = None
    customs_doc_id: str | None = None

class CBAMRunCreate(BaseModel):
    facility_id: str
    activity_record_id: str
    period_start: str
    period_end: str
    ets_price_eur_per_tco2: float = 75.0

    # CBAM-IR transitional header fields (audit ready)
    declarant_name: str | None = None
    installation_name: str | None = None
    installation_country: str | None = None
    methodology_note_tr: str | None = None

class CBAMRunOut(BaseModel):
    report_id: str
    report_hash: str
    report: dict
    checks: list[dict]
