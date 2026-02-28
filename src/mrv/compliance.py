from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from src.db.models import EvidenceDocument
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
    """Verification Regulation (2018/2067) için MVP “minimum readiness” heuristics."""
    cats = {"documents": 0, "meter_readings": 0, "invoices": 0, "contracts": 0}
    kw = {"calibration": 0, "lab": 0, "customs": 0}

    for d in docs or []:
        c = _norm(getattr(d, "category", "documents"))
        if c in cats:
            cats[c] += 1
        fn = _norm(getattr(d, "original_filename", ""))
        if any(k in fn for k in ["kalibr", "calibrat", "calibration", "certificate"]):
            kw["calibration"] += 1
        if any(k in fn for k in ["lab", "laboratuvar", "analysis", "analiz", "test_report", "rapor"]):
            kw["lab"] += 1
        if any(k in fn for k in ["customs", "gümrük", "gumruk", "export", "ithalat", "ihracat", "invoice"]):
            kw["customs"] += 1

    return {"categories": cats, "keywords": kw, "total": len(docs or [])}


def _cbam_completeness(cbam_reporting: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cbam_reporting, dict):
        return {"ok": False, "errors": ["cbam_reporting_missing_or_invalid"]}

    errs: List[str] = []
    period = cbam_reporting.get("period") or {}
    dec = cbam_reporting.get("declarant") or {}
    inst = cbam_reporting.get("installation") or {}
    goods = cbam_reporting.get("goods") or []

    if not period.get("year"):
        errs.append("missing_period_year")
    if not _norm(dec.get("company_name")):
        errs.append("missing_declarant_company_name")
    if not _norm(inst.get("facility_name")):
        errs.append("missing_installation_facility_name")

    # goods list: en az 1 satır beklenir (export varsa kritik)
    if not isinstance(goods, list):
        errs.append("goods_not_list")
        goods = []
    if len(goods) == 0:
        errs.append("goods_empty")

    # satır bazlı min alanlar
    row_errs = 0
    for g in goods[:2000]:
        if not isinstance(g, dict):
            row_errs += 1
            continue
        if not _norm(g.get("cn_code")):
            row_errs += 1
        if g.get("eu_import_quantity") is None:
            row_errs += 1
        if _norm(g.get("data_type_flag")) not in ("actual", "default"):
            row_errs += 1

    if row_errs > 0:
        errs.append(f"goods_row_errors:{row_errs}")

    return {"ok": len(errs) == 0, "errors": errs}


def evaluate_compliance(
    *,
    input_bundle: InputBundle,
    result_bundle: ResultBundle,
    legacy_results: Dict[str, Any],
) -> Tuple[List[ComplianceCheck], List[QAFlag]]:
    """A3: Compliance Rule Engine (FAZ 1.4 reg-grade genişletme).

    Kapsam:
      - CBAM 2023/1773: actual/default flag + completeness (cbam_reporting)
      - MRR 2018/2066: monitoring plan tier/method kontrolleri (MVP)
      - Verification 2018/2067: evidence completeness + sampling universe (MVP)
    """
    checks: List[ComplianceCheck] = []
    qa_flags: List[QAFlag] = []

    # -------------------------
    # 1) MRR 2018/2066 - Monitoring plan minimum
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
                remediation_tr="Danışman panelinde tesis için Monitoring Plan oluşturun/güncelleyin (yöntem + tier).",
                evidence_requirements=["monitoring_plan_document", "method_statement"],
                details={"facility_id": input_bundle.facility.get("id")},
            )
        )
    else:
        method = _norm(mp.get("method"))
        tier = _norm(mp.get("tier_level"))
        if not method or not tier:
            checks.append(
                _mk(
                    rule_id="MRR_MP_INCOMPLETE",
                    reg_reference="2018/2066",
                    severity="warn",
                    status="warn",
                    message_tr="Monitoring Plan mevcut ancak yöntem/tier bilgisi eksik.",
                    remediation_tr="Monitoring Plan kaydında yöntem ve tier alanlarını doldurun.",
                    evidence_requirements=["monitoring_plan_document"],
                    details={"monitoring_plan": mp},
                )
            )
        else:
            checks.append(
                _mk(
                    rule_id="MRR_MP_PRESENT",
                    reg_reference="2018/2066",
                    severity="info",
                    status="pass",
                    message_tr="Monitoring Plan kayıtlı (MVP kontrol).",
                    remediation_tr="",
                    details={"monitoring_plan": mp},
                )
            )

    # -------------------------
    # 2) CBAM 2023/1773 - reporting completeness + actual/default
    # -------------------------
    cbam_reporting = (legacy_results or {}).get("cbam_reporting", {}) or {}
    comp = _cbam_completeness(cbam_reporting)
    if not comp["ok"]:
        checks.append(
            _mk(
                rule_id="CBAM_REPORT_INCOMPLETE",
                reg_reference="2023/1773",
                severity="fail",
                status="fail",
                message_tr="CBAM rapor yapısı eksik veya hatalı (cbam_reporting).",
                remediation_tr="Production dosyasında CN kodları ve EU import miktarlarını kontrol edin; config içinde CBAM EORI ve iletişim e-postası girin; snapshot'ı tekrar üretin.",
                evidence_requirements=["production_records", "customs_records", "emissions_calculation_workbook"],
                details={"errors": comp["errors"]},
            )
        )
    else:
        checks.append(
            _mk(
                rule_id="CBAM_REPORT_READY",
                reg_reference="2023/1773",
                severity="info",
                status="pass",
                message_tr="CBAM rapor yapısı (XML-ready) üretildi.",
                remediation_tr="Evidence pack export içinde cbam_report.json ve cbam_report.xml dosyalarını doğrulayın.",
                details={
                    "generated_at_utc": cbam_reporting.get("generated_at_utc"),
                    "goods_count": len(cbam_reporting.get("goods") or []),
                },
            )
        )

    # actual/default flag heuristics
    goods = cbam_reporting.get("goods") if isinstance(cbam_reporting, dict) else []
    actual_count = 0
    default_count = 0
    if isinstance(goods, list):
        for g in goods:
            if not isinstance(g, dict):
                continue
            flag = _norm(g.get("data_type_flag"))
            if flag == "default":
                default_count += 1
            elif flag == "actual":
                actual_count += 1

    if (actual_count + default_count) > 0:
        if default_count > 0:
            checks.append(
                _mk(
                    rule_id="CBAM_DATA_DEFAULT_USED",
                    reg_reference="2023/1773",
                    severity="warn",
                    status="warn",
                    message_tr="CBAM raporunda 'default' veri bayrağı kullanılan kalemler var.",
                    remediation_tr="Mümkünse 'actual' veri ile güncelleyin ve supporting evidence ekleyin (ölçüm/lab raporu, enerji faturaları, sayaç okuma).",
                    evidence_requirements=["meter_readings", "lab_reports", "invoices"],
                    details={"actual_count": actual_count, "default_count": default_count},
                )
            )
        else:
            checks.append(
                _mk(
                    rule_id="CBAM_DATA_ACTUAL",
                    reg_reference="2023/1773",
                    severity="info",
                    status="pass",
                    message_tr="CBAM raporunda tüm kalemler 'actual' olarak işaretli (MVP).",
                    remediation_tr="",
                    details={"actual_count": actual_count, "default_count": default_count},
                )
            )

    # Allocation determinism check (FAZ 1.3)
    alloc = (legacy_results or {}).get("allocation", {}) or {}
    alloc_hash = alloc.get("allocation_hash") if isinstance(alloc, dict) else None
    if alloc_hash:
        checks.append(
            _mk(
                rule_id="ALLOCATION_DETERMINISTIC_HASH",
                reg_reference="internal",
                severity="info",
                status="pass",
                message_tr="Ürün bazlı allocation deterministik hash ile kilitlendi.",
                remediation_tr="",
                details={"allocation_hash": alloc_hash, "allocation_method": alloc.get("allocation_method")},
            )
        )
    else:
        checks.append(
            _mk(
                rule_id="ALLOCATION_HASH_MISSING",
                reg_reference="internal",
                severity="warn",
                status="warn",
                message_tr="Allocation hash bulunamadı. (Deterministik allocation beklenir.)",
                remediation_tr="Production dosyasında sku ve quantity alanlarının dolu olduğundan emin olun; snapshot'ı tekrar üretin.",
                details={},
            )
        )

    # -------------------------
    # 3) Verification 2018/2067 - evidence completeness + sampling universe
    # -------------------------
    docs = _list_evidence_docs(int(input_bundle.project_id))
    ev = _evidence_presence_heuristics(docs)

    # sampling universe (MVP): input satır sayıları + evidence sayısı
    universe = {
        "energy_rows": int((input_bundle.activity_snapshot_ref or {}).get("energy_rows", 0) or 0),
        "production_rows": int((input_bundle.activity_snapshot_ref or {}).get("production_rows", 0) or 0),
        "materials_rows": int((input_bundle.activity_snapshot_ref or {}).get("materials_rows", 0) or 0),
        "evidence_total": int(ev.get("total") or 0),
        "categories": ev.get("categories"),
        "keywords": ev.get("keywords"),
    }

    # minimum evidence readiness
    has_meter = (ev.get("categories") or {}).get("meter_readings", 0) > 0
    has_inv = (ev.get("categories") or {}).get("invoices", 0) > 0
    has_doc = (ev.get("categories") or {}).get("documents", 0) > 0

    if not (has_meter and has_inv and has_doc):
        checks.append(
            _mk(
                rule_id="VERIF_EVIDENCE_MINIMUM",
                reg_reference="2018/2067",
                severity="warn",
                status="warn",
                message_tr="Verification için minimum evidence seti eksik görünüyor (MVP).",
                remediation_tr="Sayaç okuma/kalibrasyon, enerji faturaları ve prosedür/lab raporu gibi belgeleri Evidence bölümüne yükleyin.",
                evidence_requirements=["meter_readings", "invoices", "documents"],
                details={"universe": universe},
            )
        )
    else:
        checks.append(
            _mk(
                rule_id="VERIF_EVIDENCE_PRESENT",
                reg_reference="2018/2067",
                severity="info",
                status="pass",
                message_tr="Verification için minimum evidence seti mevcut (MVP).",
                remediation_tr="Sampling plan için evidence pack manifestini ve cbam_report dosyalarını kullanın.",
                details={"universe": universe},
            )
        )

    # QA Flags bağlama
    for q in (result_bundle.qa_flags or []):
        if _norm(getattr(q, "severity", "")) == "fail":
            checks.append(
                _mk(
                    rule_id=f"QA_{_norm(getattr(q, 'flag_id', ''))}".upper(),
                    reg_reference="internal",
                    severity="fail",
                    status="fail",
                    message_tr=str(getattr(q, "message_tr", "") or "Kalite kontrol hatası."),
                    remediation_tr="İlgili dataset'i düzeltin ve tekrar yükleyin.",
                    details={"context": getattr(q, "context", {})},
                )
            )

    return checks, qa_flags
