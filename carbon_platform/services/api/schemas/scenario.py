from pydantic import BaseModel

class ScenarioCreate(BaseModel):
    facility_id: str
    name: str
    base_activity_record_id: str | None = None
    period_start: str | None = None
    period_end: str | None = None
    notes_tr: str | None = None

class ScenarioOut(BaseModel):
    id: str
    facility_id: str
    name: str
    status: str
    base_activity_record_id: str | None
    period_start: str | None
    period_end: str | None
    notes_tr: str | None

class AssumptionUpsert(BaseModel):
    scenario_id: str
    key: str
    value: str
    unit: str | None = None
    notes_tr: str | None = None

class AssumptionOut(BaseModel):
    id: str
    scenario_id: str
    key: str
    value: str
    unit: str | None
    notes_tr: str | None

class ScenarioRunRequest(BaseModel):
    scenario_id: str

class ScenarioRunOut(BaseModel):
    run_id: str
    status: str
    job_id: str | None
    result: dict | None
