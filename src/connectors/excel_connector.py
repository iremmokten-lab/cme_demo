import pandas as pd
import hashlib
import json
from .excel_schema import SCHEMAS


def canonical_json(data):
    return json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":")
    )


def compute_hash(df):
    data = df.to_dict(orient="records")
    canonical = canonical_json(data)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_schema(df, schema):

    missing = []

    for col in schema:
        if col not in df.columns:
            missing.append(col)

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return True


def load_excel(file, dataset_type):

    schema = SCHEMAS[dataset_type]

    df = pd.read_excel(file)

    validate_schema(df, schema)

    dataset_hash = compute_hash(df)

    return {
        "dataframe": df,
        "hash": dataset_hash
    }
