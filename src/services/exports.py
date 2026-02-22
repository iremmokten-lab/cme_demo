import io
import json
import zipfile
import pandas as pd
from pathlib import Path

from src.services.storage import EXPORT_DIR

def build_xlsx_from_results(results_json: str) -> bytes:
    results = json.loads(results_json)
    kpis = results.get("kpis", {})
    table = results.get("cbam_table", [])

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        pd.DataFrame([kpis]).to_excel(writer, index=False, sheet_name="KPIs")
        pd.DataFrame(table).to_excel(writer, index=False, sheet_name="CBAM_Table")
    return out.getvalue()

def build_zip(snapshot_id: int, results_json: str) -> bytes:
    xlsx_bytes = build_xlsx_from_results(results_json)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"snapshot_{snapshot_id}_results.json", results_json)
        z.writestr(f"snapshot_{snapshot_id}_export.xlsx", xlsx_bytes)
    return out.getvalue()
