
import io
import pandas as pd

from src.db.session import init_db, db
from src.db.models import Company, Project, Facility
from src.services.excel_ingestion_service import ingest_excel_to_datasetupload


def _make_project():
    init_db()
    with db() as s:
        c = Company(name="TestCo")
        s.add(c)
        s.commit()
        s.refresh(c)
        f = Facility(company_id=c.id, name="Test Facility")
        s.add(f)
        s.commit()
        s.refresh(f)
        p = Project(company_id=c.id, facility_id=f.id, name="Test Project")
        s.add(p)
        s.commit()
        s.refresh(p)
        return p.id


def test_excel_ingestion_creates_datasetupload(tmp_path):
    project_id = _make_project()

    df = pd.DataFrame([
        {"month": "2025-01", "facility_id": "F001", "fuel_type": "natural_gas", "fuel_quantity": 10.0, "fuel_unit": "Nm3"},
        {"month": "2025-02", "facility_id": "F001", "fuel_type": "natural_gas", "fuel_quantity": 12.0, "fuel_unit": "Nm3"},
    ])

    xlsx = tmp_path / "energy.xlsx"
    df.to_excel(xlsx, index=False)

    res = ingest_excel_to_datasetupload(
        project_id=project_id,
        dataset_type="energy",
        xlsx_bytes=xlsx.read_bytes(),
        original_filename="energy.xlsx",
        uploaded_by_user_id=None,
    )

    assert res["dataset_upload_id"] > 0
    assert res["dataset_type"] == "energy"
    assert res["sha256"]
    assert res["content_hash"]
