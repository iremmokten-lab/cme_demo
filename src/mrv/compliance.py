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
    kw = {"calibration": 0, "lab": 0, "customs": 0, "sampling": 0}

    for d in docs or []:
        c = _norm(getattr(d, "category", "documents"))
        if c in cats:
            cats[c] += 1
        fn = _norm(getattr(d, "original_filename", ""))
        if any(k in fn for k in ["kalibr", "calibrat", "calibration", "certificate", "sertifika"]):
            kw["calibration"] += 1
        if any(k in fn for k in ["lab", "laboratuvar", "analysis", "analiz", "test_report", "rapor"]):
            kw["lab"] += 1
        if any(k in fn for k in ["customs", "gümrük", "gumruk", "export", "ithalat", "ihracat", "eori"]):
            kw["customs"] += 1
        if any(k in fn for k in ["sampling", "örnek", "ornek", "sample", "universe"]):
            kw["sampling"] += 1

    return {"categories": cats, "keywords": kw, "total": len(docs or [])}


def _has_any(row: Dict[str, Any], keys: List[str]) -> bool:
    return any((k in row) and (row.get(k) is not None) and (str(row.get(k)).strip() != "") for k in keys)


def evaluate_compliance(
    *,
    input_bundle: InputBundle,
    result_bundle: ResultBundle,
    legacy_results: Dict[str, Any],
) -> Tuple[List[ComplianceCheck], List[QAFlag]]:
    """A3: Compliance Rule Engine (FAZ 1 genişletilmiş).

    Kapsam:
      - CBAM IR 2023/1773: XML-ready rapor alan doluluğu + actual/default flag + allocation trace
      - ETS MRR 2018/2066: activity data + source streams + uncertainty/tier evidence
      - Verification 2018/2067: evidence completeness + sampling universe readiness
    """
    checks: List[ComplianceCheck] = []
    qa_flags: List[QAFlag] = []

    project_id = int(input_bundle.project_id)

    # ------------------------------------------------------------
    # 1) ETS (MRR 2018/2066) — Monitoring plan, tiers, source streams
    # ------------------------------------------------------------
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
                details={"facility": input_bundle.facility, "project_id": project_id},
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
                message_tr=("Monitoring Plan alanları eksik görünüyor (method/tier)." if not ok else "Monitoring Plan minimum alanları mevcut."),
                remediation_tr=("Monitoring Plan’da method ve tier alanlarını doldurun. Verification için QA prosedürü ve veri kaynağı da önerilir." if not ok else "Planı period değişikliklerinde güncel tutun."),
                evidence_requirements=["monitoring_plan_record"],
                details={"monitoring_plan_ref": mp},
            )
        )

    ets_reporting = legacy_results.get("ets_reporting") or {}
    if isinstance(ets_reporting, dict):
        act = (ets_reporting.get("activity_data") or {})
        fuel_rows = act.get("fuel_rows") or []
        elec_rows = act.get("electricity_rows") or []

        if not fuel_rows and not elec_rows:
            checks.append(
                _mk(
                    rule_id="MRR_ACTIVITY_DATA_REQUIRED",
                    reg_reference="2018/2066",
                    severity="fail",
                    status="fail",
                    message_tr="ETS activity data boş görünüyor (yakıt/elektrik satırı yok).",
                    remediation_tr="energy.csv şablonuna uygun veri yükleyin (yakıt miktarları ve/veya elektrik tüketimi).",
                    evidence_requirements=["energy_activity_data", "meter_readings", "invoices"],
                    details={"fuel_rows": 0, "electricity_rows": 0},
                )
            )
        else:
            # source stream kontrolü: yakıt satırlarında temel alanlar olmalı
            missing_fields = 0
            for r in fuel_rows:
                if not isinstance(r, dict):
                    continue
                if not _has_any(r, ["fuel_type", "fuel"]):
                    missing_fields += 1
                if not _has_any(r, ["quantity", "amount"]):
                    missing_fields += 1
                if not _has_any(r, ["unit", "quantity_unit"]):
                    missing_fields += 1
                # deterministik factor lock işareti: factor_meta veya factor_lock
                if not _has_any(r, ["factor_meta", "factor_lock", "factor_set_lock"]):
                    missing_fields += 1

            sev = "warn" if missing_fields > 0 else "info"
            status = "warn" if missing_fields > 0 else "pass"
            checks.append(
                _mk(
                    rule_id="MRR_SOURCE_STREAM_CONTROLS",
                    reg_reference="2018/2066",
                    severity=sev,
                    status=status,
                    message_tr=("ETS source stream alanlarında eksikler var (yakıt türü/miktar/birim/factor lock)." if missing_fields > 0 else "ETS source stream kontrolleri (MVP) geçti."),
                    remediation_tr=("energy.csv kolonlarını şablona uyumlu hale getirin ve factor_set_ref kilitlenmesini sağlayın." if missing_fields > 0 else "Belirsizlik (uncertainty) ve tier kanıtlarını dokümante edin."),
                    evidence_requirements=["energy_activity_data", "meter_readings", "calibration_certificates", "factor_library_lock"],
                    details={"missing_signals": missing_fields, "fuel_rows": len(fuel_rows), "electricity_rows": len(elec_rows)},
                )
            )

            # uncertainty/tier evidence
            un = ets_reporting.get("uncertainty_and_tiers") or {}
            un_note = str(un.get("uncertainty_notes") or "").strip()
            if not un_note:
                checks.append(
                    _mk(
                        rule_id="MRR_UNCERTAINTY_NOTE",
                        reg_reference="2018/2066",
                        severity="warn",
                        status="warn",
                        message_tr="ETS belirsizlik/uncertainty notu boş görünüyor.",
                        remediation_tr="Config alanında uncertainty_notes ekleyin ve ölçüm belirsizliği + hesaplama yaklaşımını açıklayın.",
                        evidence_requirements=["uncertainty_assessment", "qa_qc_documents"],
                        details={"tier_level": un.get("tier_level"), "method": un.get("method")},
                    )
                )
            else:
                checks.append(
                    _mk(
                        rule_id="MRR_UNCERTAINTY_NOTE",
                        reg_reference="2018/2066",
                        severity="info",
                        status="pass",
                        message_tr="ETS belirsizlik/uncertainty notu mevcut.",
                        remediation_tr="Notu period değişikliklerinde güncel tutun ve referans dokümanlarla destekleyin.",
                        evidence_requirements=["uncertainty_assessment"],
                        details={"length": len(un_note)},
                    )
                )

    # ------------------------------------------------------------
    # 2) CBAM (2023/1773) — XML-ready reporting completeness + actual/default + allocation trace
    # ------------------------------------------------------------
    cbam_reporting = legacy_results.get("cbam_reporting") or {}
    cbam_rows = legacy_results.get("cbam_table", []) or []

    if not cbam_rows:
        checks.append(
            _mk(
                rule_id="CBAM_ROWS_REQUIRED",
                reg_reference="2023/1773",
                severity="fail",
                status="fail",
                message_tr="CBAM raporlama tablosu boş görünüyor. production.csv / materials.csv içeriğini kontrol edin.",
                remediation_tr="production.csv’de sku + CN + quantity alanlarını; varsa materials.csv’de precursor verisini sağlayın.",
                evidence_requirements=["production_activity_data", "customs_declarations"],
                details={"cbam_rows": 0},
            )
        )
    else:
        # alan doluluğu
        missing_count = 0
        flag_missing = 0
        alloc_missing = 0

        for r in cbam_rows:
            if not isinstance(r, dict):
                continue
            if not _has_any(r, ["cn_code", "cn", "cncode"]):
                missing_count += 1
            if not _has_any(r, ["embedded_tco2", "embedded_emissions_tco2", "embedded_emissions"]):
                missing_count += 1
            if not _has_any(r, ["direct_alloc_tco2", "direct_tco2"]):
                missing_count += 1
            if not _has_any(r, ["indirect_alloc_tco2", "indirect_tco2"]):
                missing_count += 1

            if not _has_any(r, ["data_type_flag", "method_flag", "is_actual", "default_used"]):
                flag_missing += 1
            if not _has_any(r, ["allocation_hash", "allocation_method"]):
                alloc_missing += 1

        checks.append(
            _mk(
                rule_id="CBAM_FIELD_COMPLETENESS",
                reg_reference="2023/1773",
                severity="warn" if missing_count > 0 else "info",
                status="warn" if missing_count > 0 else "pass",
                message_tr=("CBAM satırlarında bazı zorunlu alanlar eksik görünüyor (CN/embedded/direct/indirect)." if missing_count > 0 else "CBAM temel alan doluluğu kontrolü geçti (MVP)."),
                remediation_tr=("production.csv alan adlarını şablonla uyumlu hale getirin ve CBAM hesap çıktısında CN + emisyon split alanlarını doldurun." if missing_count > 0 else "Ürün bazında actual/default metodolojisini raporda netleştirin."),
                evidence_requirements=["production_activity_data", "calculation_workings"],
                details={"rows": len(cbam_rows), "missing_signals": missing_count},
            )
        )

        checks.append(
            _mk(
                rule_id="CBAM_ACTUAL_DEFAULT_FLAG",
                reg_reference="2023/1773",
                severity="warn" if flag_missing > 0 else "info",
                status="warn" if flag_missing > 0 else "pass",
                message_tr=("CBAM satırlarında actual/default bayrağı eksik görünüyor." if flag_missing > 0 else "CBAM actual/default bayrak kontrolü geçti (best-effort)."),
                remediation_tr=("cbam_reporting.goods[].data_type_flag alanını actual/default olacak şekilde set edin veya metodoloji notunda açıklayın." if flag_missing > 0 else "Bayrakların audit trail’de değiştirilemez şekilde saklandığından emin olun."),
                evidence_requirements=["methodology_statement"],
                details={"rows_without_flag": flag_missing, "rows_total": len(cbam_rows)},
            )
        )

        checks.append(
            _mk(
                rule_id="CBAM_ALLOCATION_TRACE",
                reg_reference="2023/1773",
                severity="warn" if alloc_missing > 0 else "info",
                status="warn" if alloc_missing > 0 else "pass",
                message_tr=("CBAM satırlarında allocation trace (method/hash) eksik görünüyor." if alloc_missing > 0 else "Allocation trace (method/hash) mevcut."),
                remediation_tr=("Allocation engine çıktısını results_json.allocation altında saklayın ve CBAM table satırlarına allocation_hash yazın." if alloc_missing > 0 else "Allocation hash değişikliklerinde snapshot chain'i koruyun."),
                evidence_requirements=["allocation_workings", "calculation_workings"],
                details={"rows_without_alloc_trace": alloc_missing, "rows_total": len(cbam_rows)},
            )
        )

    # XML-ready structure kontrolü (cbam_reporting)
    if not isinstance(cbam_reporting, dict) or not cbam_reporting.get("goods"):
        checks.append(
            _mk(
                rule_id="CBAM_XML_READY_STRUCTURE",
                reg_reference="2023/1773",
                severity="warn",
                status="warn",
                message_tr="CBAM XML-ready rapor yapısı (cbam_reporting) boş veya bulunamadı.",
                remediation_tr="Orchestrator cbam_reporting oluşturmalı. Snapshot üretip tekrar deneyin.",
                evidence_requirements=["cbam_report_mapping"],
                details={"has_cbam_reporting": isinstance(cbam_reporting, dict)},
            )
        )
    else:
        checks.append(
            _mk(
                rule_id="CBAM_XML_READY_STRUCTURE",
                reg_reference="2023/1773",
                severity="info",
                status="pass",
                message_tr="CBAM XML-ready rapor yapısı üretildi.",
                remediation_tr="Resmi CBAM XML formatına mapping için alan adlarını sabitleyin ve EORI/period bilgilerini tamamlayın.",
                evidence_requirements=["cbam_report_mapping", "customs_declarations"],
                details={"goods_count": len(cbam_reporting.get("goods") or [])},
            )
        )

    # ------------------------------------------------------------
    # 3) Verification Regulation (2018/2067) — minimum evidence + sampling universe
    # ------------------------------------------------------------
    docs = _list_evidence_docs(project_id)
    ev = _evidence_presence_heuristics(docs)

    cats = ev.get("categories", {})
    kw = ev.get("keywords", {})

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

    # Sampling universe readiness (FAZ 1)
    # Universe: fuel_rows + electricity_rows + cbam goods count
    fu = 0
    el = 0
    gd = 0
    if isinstance(ets_reporting, dict):
        act = (ets_reporting.get("activity_data") or {})
        fu = len(act.get("fuel_rows") or []) if isinstance(act.get("fuel_rows"), list) else 0
        el = len(act.get("electricity_rows") or []) if isinstance(act.get("electricity_rows"), list) else 0
    if isinstance(cbam_reporting, dict):
        gd = len(cbam_reporting.get("goods") or []) if isinstance(cbam_reporting.get("goods"), list) else 0

    universe = {"fuel_rows": fu, "electricity_rows": el, "cbam_goods": gd, "total": int(fu + el + gd)}

    if int(kw.get("sampling", 0)) <= 0:
        checks.append(
            _mk(
                rule_id="VR_SAMPLING_UNIVERSE",
                reg_reference="2018/2067",
                severity="warn",
                status="warn",
                message_tr="Sampling plan / sampling universe evidencesi bulunamadı (dosya adı heuristics).",
                remediation_tr="Verifier için sampling plan dokümanı yükleyin (örn: sampling_plan.pdf) ve örnekleme evrenini (universe) tanımlayın.",
                evidence_requirements=["sampling_plan", "sampling_universe_definition"],
                details={"sampling_universe": universe, "keyword_hits": kw.get("sampling", 0)},
            )
        )
    else:
        checks.append(
            _mk(
                rule_id="VR_SAMPLING_UNIVERSE",
                reg_reference="2018/2067",
                severity="info",
                status="pass",
                message_tr="Sampling plan / sampling universe evidencesi bulundu (heuristics).",
                remediation_tr="Sampling plan'ın period ve veri setleri ile uyumlu olduğundan emin olun.",
                evidence_requirements=["sampling_plan"],
                details={"sampling_universe": universe},
            )
        )

    # Calibration evidence check
    if int(kw.get("calibration", 0)) <= 0:
        checks.append(
            _mk(
                rule_id="VR_CALIBRATION_EVIDENCE",
                reg_reference="2018/2067",
                severity="warn",
                status="warn",
                message_tr="Kalibrasyon evidencesi (dosya adı heuristics) bulunamadı.",
                remediation_tr="Sayaç kalibrasyon sertifikalarını meter_readings kategorisinde yükleyin.",
                evidence_requirements=["calibration_certificates"],
                details={"keyword_hits": kw.get("calibration", 0), "evidence_summary": ev},
            )
        )
    else:
        checks.append(
            _mk(
                rule_id="VR_CALIBRATION_EVIDENCE",
                reg_reference="2018/2067",
                severity="info",
                status="pass",
                message_tr="Kalibrasyon evidencesi bulundu (heuristics).",
                remediation_tr="Sertifikaların geçerlilik tarihlerini period ile eşleştirin.",
                evidence_requirements=["calibration_certificates"],
                details={"keyword_hits": kw.get("calibration", 0)},
            )
        )

    return checks, qa_flags
