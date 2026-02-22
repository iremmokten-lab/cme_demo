import json
from sqlalchemy.orm import Session

from src.db.models import DatasetUpload, CalculationSnapshot
from src.mrv.lineage import sha256_bytes, sha256_json


def save_upload(
    db: Session,
    project_id: int,
    dataset_type: str,
    original_filename: str,
    content_bytes: bytes,
    schema_version: str = "v1",
) -> DatasetUpload:
    h = sha256_bytes(content_bytes)
    u = DatasetUpload(
        project_id=project_id,
        dataset_type=dataset_type,
        original_filename=original_filename or f"{dataset_type}.csv",
        sha256=h,
        schema_version=schema_version,
        content_bytes=content_bytes,
    )
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


def save_snapshot(
    db: Session,
    project_id: int,
    engine_version: str,
    config: dict,
    input_hashes: dict,
    results: dict,
) -> CalculationSnapshot:
    result_hash = sha256_json({"config": config, "inputs": input_hashes, "results": results})
    s = CalculationSnapshot(
        project_id=project_id,
        engine_version=engine_version,
        config_json=json.dumps(config, ensure_ascii=False),
        input_hashes_json=json.dumps(input_hashes, ensure_ascii=False),
        results_json=json.dumps(results, ensure_ascii=False),
        result_hash=result_hash,
    )
    db.add(s)
    db.commit()
    db.refresh(s)
    return s
