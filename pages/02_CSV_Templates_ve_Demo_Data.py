from __future__ import annotations

import zipfile
from io import BytesIO

import pandas as pd
import streamlit as st

st.title("CSV Templates ve Demo Data")
st.caption("Tek tıkla şablon veya demo dataset indirip **Veri Yükleme** sayfasından sisteme yükleyebilirsiniz.")

st.write(
    """
Bu sayfadaki dosyalar, platformun beklediği minimum şemaya göre hazırlanmıştır.

**Hedef:** Engine + Compliance + Verification (MVP) akışını kullanıcı elinde dosya olmadan test edebilmek.

- ✅ Şablonlar: doğru kolon isimleri ve örnek satırlar içerir
- ✅ Demo dataset: enerji + üretim + (opsiyonel) materials + (opsiyonel) monitoring plan + verification örnekleri
"""
)

# ----------------------------
# Templates
# ----------------------------
st.subheader("1) CSV Şablonları")

energy_template = pd.DataFrame(
    [
        {"month": "2025-01", "facility_id": 1, "fuel_type": "natural_gas", "fuel_quantity": 12000, "fuel_unit": "Nm3"},
        {"month": "2025-01", "facility_id": 1, "fuel_type": "electricity", "fuel_quantity": 350000, "fuel_unit": "kWh"},
    ]
)

production_template = pd.DataFrame(
    [
        {
            "month": "2025-01",
            "facility_id": 1,
            "cn_code": "7207",
            "product_name": "Yarı mamul çelik",
            "product_code": "STEEL_SEMI",
            "quantity": 1000,
            "unit": "t",
            "cbam_covered": True,
        }
    ]
)

materials_template = pd.DataFrame(
    [
        {
            "month": "2025-01",
            "facility_id": 1,
            "material_name": "Lime",
            "material_code": "LIME",
            "quantity": 50,
            "unit": "t",
            "embedded_factor_tco2_per_t": 0.75,
            "is_actual": False,
            "note": "Demo precursor/material satırı",
        }
    ]
)

monitoring_plan_template = pd.DataFrame(
    [
        {
            "facility_id": 1,
            "method": "standard",
            "tier_level": "Tier 2",
            "data_source": "ERP + Sayaç",
            "qa_procedure": "Aylık mutabakat, örnekleme kontrolü, kalibrasyon takibi",
            "responsible_person": "Tesis Enerji Yöneticisi",
        }
    ]
)

verification_case_template = pd.DataFrame(
    [
        {
            "period_year": 2025,
            "facility_id": 1,
            "verifier_org": "Demo Verifier Ltd.",
            "status": "planning",
        }
    ]
)

verification_finding_template = pd.DataFrame(
    [
        {
            "case_ref": "CASE_DEMO_2025_1",
            "severity": "major",
            "description": "Sayaç kalibrasyon sertifikası eksik.",
            "corrective_action": "Kalibrasyon sertifikası temin edilip evidence sekmesine yüklenmeli.",
            "due_date": "2026-03-15",
            "status": "open",
        }
    ]
)


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


c1, c2, c3 = st.columns(3)
with c1:
    st.download_button(
        "energy.csv şablonu indir",
        data=_df_to_csv_bytes(energy_template),
        file_name="energy_template.csv",
        mime="text/csv",
        use_container_width=True,
    )
with c2:
    st.download_button(
        "production.csv şablonu indir",
        data=_df_to_csv_bytes(production_template),
        file_name="production_template.csv",
        mime="text/csv",
        use_container_width=True,
    )
with c3:
    st.download_button(
        "materials.csv şablonu indir (opsiyonel)",
        data=_df_to_csv_bytes(materials_template),
        file_name="materials_template.csv",
        mime="text/csv",
        use_container_width=True,
    )

c4, c5, c6 = st.columns(3)
with c4:
    st.download_button(
        "monitoring_plan.csv şablonu indir (opsiyonel)",
        data=_df_to_csv_bytes(monitoring_plan_template),
        file_name="monitoring_plan_template.csv",
        mime="text/csv",
        use_container_width=True,
    )
with c5:
    st.download_button(
        "verification_case.csv şablonu indir (opsiyonel)",
        data=_df_to_csv_bytes(verification_case_template),
        file_name="verification_case_template.csv",
        mime="text/csv",
        use_container_width=True,
    )
with c6:
    st.download_button(
        "verification_finding.csv şablonu indir (opsiyonel)",
        data=_df_to_csv_bytes(verification_finding_template),
        file_name="verification_finding_template.csv",
        mime="text/csv",
        use_container_width=True,
    )

st.info(
    """\
**Notlar**
- `energy.csv` ve `production.csv` engine çalışması için zorunludur.
- `materials.csv` CBAM precursor/material demonstrasyonu içindir (opsiyonel).
- `monitoring_plan.csv` ve verification şablonları MVP akışını uçtan uca test etmek içindir (opsiyonel).
"""
)

st.divider()

# ----------------------------
# Demo dataset zip (one click)
# ----------------------------
st.subheader("2) Tek Tıkla Demo Dataset ZIP")

demo_energy = pd.DataFrame(
    [
        {"month": "2025-01", "facility_id": 1, "fuel_type": "natural_gas", "fuel_quantity": 12000, "fuel_unit": "Nm3"},
        {"month": "2025-01", "facility_id": 1, "fuel_type": "electricity", "fuel_quantity": 350000, "fuel_unit": "kWh"},
        {"month": "2025-02", "facility_id": 1, "fuel_type": "natural_gas", "fuel_quantity": 11000, "fuel_unit": "Nm3"},
        {"month": "2025-02", "facility_id": 1, "fuel_type": "electricity", "fuel_quantity": 330000, "fuel_unit": "kWh"},
    ]
)

demo_production = pd.DataFrame(
    [
        {
            "month": "2025-01",
            "facility_id": 1,
            "cn_code": "7207",
            "product_name": "Yarı mamul çelik",
            "product_code": "STEEL_SEMI",
            "quantity": 1000,
            "unit": "t",
            "cbam_covered": True,
        },
        {
            "month": "2025-02",
            "facility_id": 1,
            "cn_code": "7207",
            "product_name": "Yarı mamul çelik",
            "product_code": "STEEL_SEMI",
            "quantity": 950,
            "unit": "t",
            "cbam_covered": True,
        },
    ]
)

demo_materials = pd.DataFrame(
    [
        {
            "month": "2025-01",
            "facility_id": 1,
            "material_name": "Lime",
            "material_code": "LIME",
            "quantity": 50,
            "unit": "t",
            "embedded_factor_tco2_per_t": 0.75,
            "is_actual": False,
            "note": "Demo precursor/material satırı",
        }
    ]
)

demo_monitoring_plan = pd.DataFrame(
    [
        {
            "facility_id": 1,
            "method": "standard",
            "tier_level": "Tier 2",
            "data_source": "ERP + Sayaç",
            "qa_procedure": "Aylık mutabakat, örnekleme kontrolü, kalibrasyon takibi",
            "responsible_person": "Tesis Enerji Yöneticisi",
        }
    ]
)

demo_verification_case = pd.DataFrame(
    [
        {"case_ref": "CASE_DEMO_2025_1", "period_year": 2025, "facility_id": 1, "verifier_org": "Demo Verifier Ltd.", "status": "planning"}
    ]
)

demo_verification_findings = pd.DataFrame(
    [
        {
            "case_ref": "CASE_DEMO_2025_1",
            "severity": "major",
            "description": "Sayaç kalibrasyon sertifikası eksik.",
            "corrective_action": "Kalibrasyon sertifikası temin edilip evidence sekmesine yüklenmeli.",
            "due_date": "2026-03-15",
            "status": "open",
        },
        {
            "case_ref": "CASE_DEMO_2025_1",
            "severity": "minor",
            "description": "Elektrik tedarik sözleşmesi (market-based) eklenmemiş.",
            "corrective_action": "Sözleşme ve garanti belgesi eklenmeli; market factor override açıklanmalı.",
            "due_date": "2026-03-20",
            "status": "open",
        },
    ]
)

readme_txt = """\
DEMO DATASET — CME Demo (CBAM + EU ETS MRV)

İÇERİK
- input/energy.csv
- input/production.csv
- input/materials.csv (opsiyonel)
- reference/monitoring_plan.csv (opsiyonel)
- verification/verification_case.csv (opsiyonel, UI’dan case oluşturmak için referans)
- verification/verification_findings.csv (opsiyonel, UI’dan bulgu oluşturmak için referans)

KULLANIM
1) Uygulamada Danışman Paneli -> Veri Yükleme:
   - energy.csv ve production.csv’yi yükleyin
   - materials.csv opsiyonel
2) Hesaplama (snapshot) oluşturun.
3) Uyum Kontrolleri (Checklist) sayfasında compliance_checks sonuçlarını inceleyin.
4) Verification Workflow sayfasında case oluşturup bulgular ekleyin.
5) Evidence Pack export alıp manifest + signature doğrulamasını kontrol edin.

NOT
- Compliance çıktısı hesap sonrası snapshot.results_json içine yazılır.
- Verification case JSON’u evidence pack içine snapshot’ın dönem yılına göre dahil edilir.
"""


def _build_demo_zip_bytes() -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.txt", readme_txt.encode("utf-8"))
        z.writestr("input/energy.csv", _df_to_csv_bytes(demo_energy))
        z.writestr("input/production.csv", _df_to_csv_bytes(demo_production))
        z.writestr("input/materials.csv", _df_to_csv_bytes(demo_materials))
        z.writestr("reference/monitoring_plan.csv", _df_to_csv_bytes(demo_monitoring_plan))
        z.writestr("verification/verification_case.csv", _df_to_csv_bytes(demo_verification_case))
        z.writestr("verification/verification_findings.csv", _df_to_csv_bytes(demo_verification_findings))
        # ayrıca şablonlar
        z.writestr("templates/energy_template.csv", _df_to_csv_bytes(energy_template))
        z.writestr("templates/production_template.csv", _df_to_csv_bytes(production_template))
        z.writestr("templates/materials_template.csv", _df_to_csv_bytes(materials_template))
        z.writestr("templates/monitoring_plan_template.csv", _df_to_csv_bytes(monitoring_plan_template))
        z.writestr("templates/verification_case_template.csv", _df_to_csv_bytes(verification_case_template))
        z.writestr("templates/verification_finding_template.csv", _df_to_csv_bytes(verification_finding_template))
    return buf.getvalue()


st.download_button(
    "📦 Demo dataset ZIP indir (tek tık)",
    data=_build_demo_zip_bytes(),
    file_name="cme_demo_dataset.zip",
    mime="application/zip",
    use_container_width=True,
)

with st.expander("Demo dataset içeriğini önizle", expanded=False):
    st.write("**energy.csv (demo)**")
    st.dataframe(demo_energy, use_container_width=True, hide_index=True)
    st.write("**production.csv (demo)**")
    st.dataframe(demo_production, use_container_width=True, hide_index=True)
    st.write("**materials.csv (demo)**")
    st.dataframe(demo_materials, use_container_width=True, hide_index=True)
    st.write("**monitoring_plan.csv (demo)**")
    st.dataframe(demo_monitoring_plan, use_container_width=True, hide_index=True)
    st.write("**verification findings (demo)**")
    st.dataframe(demo_verification_findings, use_container_width=True, hide_index=True)

st.caption("Bu sayfa sadece indirme sağlar; yükleme ve hesaplama adımları Danışman Paneli üzerinden yürütülür.")
