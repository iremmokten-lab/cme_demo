from __future__ import annotations

"""XLSX ingestion templates (Faz 2).

Amaç:
- CSV yanında Excel template ile ingestion
- Tek bir "MRV Template" dosyası: energy / production / materials sheet'leri
"""

from io import BytesIO

from openpyxl import Workbook


def build_mrv_template_xlsx() -> bytes:
    wb = Workbook()

    ws = wb.active
    ws.title = "README"
    ws.append(["CME Demo — MRV Excel Template"])
    ws.append([""])
    ws.append(["Bu dosya Streamlit demo ingestion için hazırlanmıştır."])
    ws.append(["Sheet isimleri: energy, production, materials"])
    ws.append(["Kolon adlarını mümkünse değiştirmeyin. Tarih formatı: YYYY-MM"])

    ws_e = wb.create_sheet("energy")
    ws_e.append(["month", "facility_id", "fuel_type", "fuel_quantity", "fuel_unit"])
    ws_e.append(["2025-01", "1", "natural_gas", "1000", "Nm3"])
    ws_e.append(["2025-01", "1", "electricity", "50000", "kWh"])

    ws_p = wb.create_sheet("production")
    ws_p.append(["month", "facility_id", "sku", "cn_code", "quantity", "unit", "export_to_eu_quantity"])
    ws_p.append(["2025-01", "1", "SKU-A", "7207", "1000", "kg", "200"])

    ws_m = wb.create_sheet("materials")
    ws_m.append(["sku", "material_name", "material_quantity", "material_unit", "emission_factor"])
    ws_m.append(["SKU-A", "precursor_x", "10", "kg", "2.5"])

    for w in [ws_e, ws_p, ws_m]:
        w.freeze_panes = "A2"

    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()
