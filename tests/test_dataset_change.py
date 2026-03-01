from pathlib import Path
from src.db.session import db, init_db
from src.db.models import Company, Facility, Project, DatasetUpload
from src.mrv.lineage import sha256_bytes
from src.services.workflow import run_full


def _add_upload(project_id: int, dataset_type: str, uri: str) -> int:
    bts = Path(uri).read_bytes()
    h = sha256_bytes(bts)
    with db() as s:
        up = DatasetUpload(project_id=int(project_id), dataset_type=str(dataset_type), storage_uri=str(uri), sha256=str(h), original_filename=Path(uri).name)
        s.add(up)
        s.commit()
        s.refresh(up)
        return int(up.id)


def test_dataset_change_changes_hash(tmp_path: Path):
    init_db()
    with db() as s:
        c = Company(name="TenantD")
        s.add(c); s.commit(); s.refresh(c)
        f = Facility(company_id=c.id, name="Tesis D")
        s.add(f); s.commit(); s.refresh(f)
        p = Project(company_id=c.id, facility_id=f.id, name="Proje D")
        s.add(p); s.commit(); s.refresh(p)
        pid = int(p.id)

    e1 = tmp_path / "energy.csv"
    e1.write_text("energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit\nnatural_gas,1,1000,2.00\n", encoding="utf-8")
    pr = tmp_path / "production.csv"
    pr.write_text("sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit,cbam_covered\nSKU,1,1,1.0,1\n", encoding="utf-8")

    _add_upload(pid, "energy", str(e1))
    _add_upload(pid, "production", str(pr))

    cfg = {"period": {"year": 2025}}
    s1 = run_full(project_id=pid, config=cfg, scenario={}, methodology_id=None)

    # modify energy dataset => new upload
    e2 = tmp_path / "energy2.csv"
    e2.write_text("energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit\nnatural_gas,1,2000,2.00\n", encoding="utf-8")
    _add_upload(pid, "energy", str(e2))

    s2 = run_full(project_id=pid, config=cfg, scenario={}, methodology_id=None)

    assert s1.input_hash != s2.input_hash
