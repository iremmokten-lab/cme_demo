import pandas as pd

REQUIRED_COLUMNS = {
    "energy": ["energy_carrier", "scope", "activity_amount", "emission_factor_kgco2_per_unit"],
    "production": ["sku", "quantity", "export_to_eu_quantity", "input_emission_factor_kg_per_unit"],
}

def validate_csv(dataset_type: str, df: pd.DataFrame) -> list[str]:
    req = REQUIRED_COLUMNS.get(dataset_type, [])
    missing = [c for c in req if c not in df.columns]
    errs = []
    if missing:
        errs.append(f"Eksik kolonlar: {missing}")
    return errs
