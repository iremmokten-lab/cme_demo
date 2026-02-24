from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from src.db.models import EvidenceDocument, Project
from src.db.session import db
from src.mrv.bundles import ComplianceCheck, InputBundle, QAFlag, ResultBundle


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _mk(
    *,
    rule_id: str,
    reg_reference: str,
    severity: str,
    status: str,
    message_tr: str,
    remediation_tr: str,
    evidence_requirements: Optional[List[str]] = None,
    details: Optional[Dict[str, Any]] = None,
) -> ComplianceCheck:
    return ComplianceCheck(
        rule_id=rule_id,
        reg_reference=reg_reference,
        severity=severity,
        status=status,
        message_tr=message_tr,
        remediation_tr=remediation_tr,
        evidence_requirements=list(evidence_requirements or []),
        details=dict(details or {}),
    )


def _list_evidence_docs(project_id: int) -> List[EvidenceDocument]:
    with db() as s:
        return (
            s.execute(
                select(EvidenceDocument)
                .where(EvidenceDocument.project_id == int(project_id))
                .order_by(EvidenceDocument.uploaded_at.desc())
            )
            .scalars()
            .all()
        )


def _evidence_presence_heuristics(docs: List[EvidenceDocument]) -> Dict[str, Any]:
    """
    Verification Regulation (2018/2067) için MVP “minimum readiness” heuristics.
    DB’de kategori listesi sınırlı olduğu için:
      - meter_readings: sayaç okuması / kalibrasyon evidences
      - invoices: fatura/mutabakat
      - documents: lab raporu / prosedür / belgelendirme
      - contracts: tedarik sözleşmeleri
    Ek olarak filename içinde anahtar kelimeler ile “customs” / “lab” yakalanır.
    """
    cats = {"documents": 0, "meter_readings": 0, "invoices": 0, "contracts": 0}
    kw = {"calibration": 0, "lab": 0, "customs": 0}

    for d in docs or []:
        c = _norm(getattr(d, "category", "documents"))
        if c in cats:
            cats[c] += 1
        fn = _norm(getattr(d, "original_filename", ""))
        if any(k in fn for k in ["kalibr", "calibrat", "calibration", "certificate"]):
            kw["calibration"] += 1
        if any(k in fn for k in ["lab", "laboratuvar", "analysis", "analiz", "test_report"]):
            kw["lab"] += 1
        if any(k in fn for k in ["customs", "gümrük", "gumruk", "export", "ithalat", "ihracat"]):
            kw["customs"] += 1

    return {"categories": cats, "keywords": kw, "total": len(docs or [])}


def evaluate_compliance(
    *,
    input_bundle: InputBundle,
    result_bundle: ResultBundle,
    legacy_results: Dict[str, Any],
) -> Tuple[List[ComplianceCheck], List[QAFlag]]:
    """
    A3: Compliance Rule Engine (regulation-grade / audit-ready output format)

    Kapsam (MVP):
      - MRR 2018/2066: Monitoring plan tier/method uyumu (plan var mı? tier/method set mi?)
      - CBAM 2023/1773: raporlama alan doluluğu + actual/default flag heuristics
      - Verification 2018/2067: minimum doküman/evidence readiness

    NOT: Bu repo MVP olduğundan “tam mevzuat ekleri” yerine audit-ready JSON üretir
         ve kullanıcıyı eksikleri tamamlamaya yönlendirir.
    """
    checks: List[ComplianceCheck] = []
    qa_flags: List[QAFlag] = []

    # -------------------------
    # 1) MRR 2018/2066 - Monitoring plan
    # -------------------------
    mp = input_bundle.monitoring_plan_ref.to_dict() if input_bundle.monitoring_plan_ref else None
    if not mp:
        checks.append(
            _mk(
                rule_id="MRR_MP_REQUIRED",
                reg_reference="2018/2066",
                severity="fail",
                status="fail",
                message_tr="ETS Monitoring Plan bulunamadı. Tesis için Monitoring Plan kaydı zorunludur.",
                remediation_tr="Danışman panelinde ilgili tesis için Monitoring Plan oluşturun/güncelleyin (yöntem + tier + veri kaynağı + QA prosedürü).",
                evidence_requirements=["monitoring_plan_record", "qa_qc_procedure_document"],
                details={"facility": input_bundle.facility, "project_id": input_bundle.project_id},
            )
        )
    else:
        method = str(mp.get("method") or "").strip()
        tier = str(mp.get("tier_level") or "").strip()
        ok = bool(method) and bool(tier)
        checks.append(
            _mk(
                rule_id="MRR_MP_MIN_FIELDS",
                reg_reference="2018/2066",
                severity="warn" if not ok else "info",
                status="warn" if not ok else "pass",
                message_tr=("Monitoring Plan alanları eksik görünüyor (method/tier)."
                            if not ok else "Monitoring Plan minimum alanları mevcut."),
                remediation_tr=("Monitoring Plan’da method ve tier alanlarını doldurun. "
                                "Verification için QA prosedürü ve veri kaynağı da önerilir."
                                if not ok else "Planı period değişikliklerinde güncel tutun."),
                evidence_requirements=["monitoring_plan_record"],
                details={"monitoring_plan_ref": mp},
            )
        )

    # -------------------------
    # 2) CBAM 2023/1773 - Transitional reporting completeness
    # -------------------------
    cbam_rows = legacy_results.get("cbam_table", []) or []
    required_fields = ["cn_code", "embedded", "embedded_tco2", "direct_tco2", "indirect_tco2"]
    # cbam engine farklı kolon isimleri üretebilir: best-effort normalize check
    def _has_any(row: Dict[str, Any], keys: List[str]) -> bool:
        return any(k in row for k in keys)

    missing_count = 0
    actual_default_missing = 0

    for r in cbam_rows:
        if not isinstance(r, dict):
            continue
        # CN
        if not _has_any(r, ["cn_code", "cn", "cncode"]):
            missing_count += 1
        # Embedded emissions
        if not _has_any(r, ["embedded_tco2", "embedded_tco2e", "embedded_emissions_tco2", "embedded_emissions"]):
            missing_count += 1

        # Actual/default flag heuristics:
        # Eğer satırda "is_actual" / "actual" / "default_used" gibi alan yoksa warn.
        if not _has_any(r, ["is_actual", "actual", "default_used", "method_flag", "data_type_flag"]):
            actual_default_missing += 1

    if len(cbam_rows) == 0:
        checks.append(
            _mk(
                rule_id="CBAM_ROWS_REQUIRED",
                reg_reference="2023/1773",
                severity="fail",
                status="fail",
                message_tr="CBAM raporlama tablosu boş görünüyor. production.csv / materials.csv içeriğini kontrol edin.",
                remediation_tr="production.csv’de ürün/CN bilgisi ve miktar alanlarını; varsa materials.csv’de precursor verisini sağlayın.",
                evidence_requirements=["production_activity_data", "customs_declarations"],
                details={"cbam_rows": 0},
            )
        )
    else:
        if missing_count > 0:
            checks.append(
                _mk(
                    rule_id="CBAM_FIELD_COMPLETENESS",
                    reg_reference="2023/1773",
                    severity="warn",
                    status="warn",
                    message_tr="CBAM satırlarında bazı zorunlu alanlar eksik/uyumsuz görünüyor (CN veya embedded emissions).",
                    remediation_tr="production.csv alan adlarını şablonla uyumlu hale getirin ve CBAM hesap çıktısında CN + embedded emissions alanlarının dolu olduğundan emin olun.",
                    evidence_requirements=["production_activity_data", "calculation_workings"],
                    details={"rows": len(cbam_rows), "missing_signals": missing_count},
                )
            )
        else:
            checks.append(
                _mk(
                    rule_id="CBAM_FIELD_COMPLETENESS",
                    reg_reference="2023/1773",
                    severity="info",
                    status="pass",
                    message_tr="CBAM temel alan doluluğu kontrolü geçti (MVP).",
                    remediation_tr="Yine de transitional reporting için ürün bazında actual/default metodolojisini açıkça belgelendirin.",
                    evidence_requirements=["calculation_workings", "methodology_statement"],
                    details={"rows": len(cbam_rows)},
                )
            )

        if actual_default_missing > 0:
            checks.append(
                _mk(
                    rule_id="CBAM_ACTUAL_DEFAULT_FLAG",
                    reg_reference="2023/1773",
                    severity="warn",
                    status="warn",
                    message_tr="CBAM satırlarında actual/default metod bayrağı bulunamadı (MVP heuristics).",
                    remediation_tr="Ürün bazında kullanılan verinin actual mı default mu olduğunu raporlayacak alanları (flag) ekleyin veya metodoloji notunda açıklayın.",
                    evidence_requirements=["methodology_statement"],
                    details={"rows_without_flag": actual_default_missing, "rows_total": len(cbam_rows)},
                )
            )
        else:
            checks.append(
                _mk(
                    rule_id="CBAM_ACTUAL_DEFAULT_FLAG",
                    reg_reference="2023/1773",
                    severity="info",
                    status="pass",
                    message_tr="CBAM actual/default bayrak kontrolü (best-effort) geçti.",
                    remediation_tr="Bayrakların audit trail’de değiştirilemez şekilde saklandığından emin olun.",
                    evidence_requirements=["methodology_statement"],
                    details={"rows_total": len(cbam_rows)},
                )
            )

    # -------------------------
    # 3) Verification Regulation 2018/2067 - Minimum evidence readiness
    # -------------------------
    docs = _list_evidence_docs(int(input_bundle.project_id))
    ev = _evidence_presence_heuristics(docs)

    cats = ev.get("categories", {})
    kw = ev.get("keywords", {})

    # Minimum set: meter_readings + invoices + at least one documents
    missing = []
    if int(cats.get("meter_readings", 0)) <= 0:
        missing.append("meter_readings (sayaç okuması/kalibrasyon)")
    if int(cats.get("invoices", 0)) <= 0:
        missing.append("invoices (fatura/mutabakat)")
    if int(cats.get("documents", 0)) <= 0:
        missing.append("documents (prosedür/lab/ekler)")

    if missing:
        checks.append(
            _mk(
                rule_id="VR_MIN_EVIDENCE_SET",
                reg_reference="2018/2067",
                severity="fail",
                status="fail",
                message_tr="Verification için minimum evidence seti eksik görünüyor.",
                remediation_tr="Evidence sekmesinden sayaç okuması/kalibrasyon, fatura ve ilgili dokümanları yükleyin. (Kategori doğru seçilmeli.)",
                evidence_requirements=["meter_readings", "invoices", "calibration_certificates", "qa_qc_documents"],
                details={"missing": missing, "evidence_summary": ev},
            )
        )
    else:
        checks.append(
            _mk(
                rule_id="VR_MIN_EVIDENCE_SET",
                reg_reference="2018/2067",
                severity="info",
                status="pass",
                message_tr="Minimum evidence seti (kategori bazlı) mevcut görünüyor.",
                remediation_tr="Verification için sampling plan, belirsizlik hesapları ve değişiklik yönetimi eklerini de hazırlayın.",
                evidence_requirements=["sampling_plan", "uncertainty_assessment", "change_log"],
                details={"evidence_summary": ev},
            )
        )

    # Keyword heuristics (customs/lab/calibration)
    if int(kw.get("calibration", 0)) <= 0:
        checks.append(
            _mk(
                rule_id="VR_CALIBRATION_EVIDENCE",
                reg_reference="2018/2067",
                severity="warn",
                status="warn",
                message_tr="Kalibrasyon/sertifika evidencesi filename heuristics ile bulunamadı.",
                remediation_tr="Kalibrasyon sertifikalarını (PDF) yükleyin ve dosya adında kalibrasyon/certificate ibaresi olmasına dikkat edin.",
                evidence_requirements=["calibration_certificates"],
                details={"evidence_summary": ev},
            )
        )

    if int(kw.get("lab", 0)) <= 0:
        checks.append(
            _mk(
                rule_id="VR_LAB_REPORT_EVIDENCE",
                reg_reference="2018/2067",
                severity="warn",
                status="warn",
                message_tr="Lab raporu evidencesi filename heuristics ile bulunamadı.",
                remediation_tr="Varsa lab/analiz raporlarını yükleyin (özellikle precursor/kimyasal analizleri).",
                evidence_requirements=["lab_reports"],
                details={"evidence_summary": ev},
            )
        )

    if int(kw.get("customs", 0)) <= 0:
        checks.append(
            _mk(
                rule_id="CBAM_CUSTOMS_EVIDENCE",
                reg_reference="2023/1773",
                severity="warn",
                status="warn",
                message_tr="Gümrük/ihracat evidencesi filename heuristics ile bulunamadı.",
                remediation_tr="CBAM transitional reporting için gümrük beyannamesi/ihracat kayıtlarını doküman olarak ekleyin.",
                evidence_requirements=["customs_declarations"],
                details={"evidence_summary": ev},
            )
        )

    # QA flags to supplement compliance
    if mp is None:
        qa_flags.append(
            QAFlag(
                flag_id="QA_NO_MONITORING_PLAN",
                severity="fail",
                message_tr="Monitoring Plan yok: ETS verification payload eksik/riski yüksek.",
                context={"reg_reference": "2018/2066"},
            )
        )

    return checks, qa_flags
