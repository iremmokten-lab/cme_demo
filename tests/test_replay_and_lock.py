import json
from pathlib import Path

import pandas as pd

from src.db.session import db, init_db
from src.db.models import Company, Facility, Project, DatasetUpload
from src.mrv.lineage import sha256_bytes
from src.services.workflow import run_full
from src.mrv.replay import replay
from src.services.snapshots import lock_snapshot


def _write(path: Path, text: str) -> str:
    path.write_text(text, encoding="utf-8")
    return str(path)


def _add_upload(project_id: int, dataset_type: str, uri: str) -> int:
    bts = Path(uri).read_bytes()
    h = sha256_bytes(bts)
    with db() as s:
        up = DatasetUpload(project_id=int(project_id), dataset_type=str(dataset_type), storage_uri=str(uri), sha256=str(h), original_filename=Path(uri).name)
        s.add(up)
        s.commit()
        s.refresh(up)
        return int(up.id)


def test_replay_and_lock_policy(tmp_path: Path):
    init_db()
    with db() as s:
        c = Company(name="TenantA")
        s.add(c)
        s.commit()
        s.refresh(c)
        f = Facility(company_id=c.id, name="Tesis A", country="TR", sector="Cement")
        s.add(f)
        s.commit()
        s.refresh(f)
        p = Project(company_id=c.id, facility_id=f.id, name="Proje A", description="")
        s.add(p)
        s.commit()
        s.refresh(p)
        project_id = int(p.id)

    # create input csvs
    energy_csv = """energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit
natural_gas,1,1000,2.00
diesel,1,200,2.68
electricity,2,5000,0.40
"""
    prod_csv = """sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit,cbam_covered,cn_code,product_name
SKU-A,1000,200,1.20,1,7201,Steel
SKU-B,500,50,0.80,1,7202,Steel2
"""

    e_uri = _write(tmp_path / "energy.csv", energy_csv)
    p_uri = _write(tmp_path / "production.csv", prod_csv)

    _add_upload(project_id, "energy", e_uri)
    _add_upload(project_id, "production", p_uri)

    cfg = {
        "period": {"year": 2025},
        "eua_price_eur_per_t": 70.0,
        "fx_tl_per_eur": 35.0,
        "ai": {"optimizer_constraints": {"max_capex_eur": 1000000}},
    }

    snap = run_full(project_id=project_id, config=cfg, scenario={}, methodology_id=None, created_by_user_id=None)
    assert snap.input_hash
    assert snap.result_hash

    # lock then replay
    lock_snapshot(int(snap.id))
    rep = replay(int(snap.id))
    assert rep["input_hash_match"] is True
    assert rep["result_hash_match"] is True

    # lock policy: update should fail at DB trigger
    failed = False
    try:
        with db() as s:
            sn = s.get(type(snap), int(snap.id))
            sn.engine_version = "tamper"
            s.commit()
    except Exception:
        failed = True
    assert failed is True
