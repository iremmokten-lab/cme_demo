from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from src.db.models import FactorSet, MonitoringPlan, Project
from src.db.session import db
from src.services.regulatory_specs import MappingCheck, assess_project_against_spec, get_spec


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _get_in(obj: Any, path: str) -> Any:
    """
    Safe dotted-path getter.
    Example: obj={"a":{"b":1}}, path="a.b" -> 1
    """
    cur = obj
    for part in (path or "").split("."):
        part = part.strip()
        if not part:
            continue
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


@dataclass
class StrictCheck:
    rule_id: str
    spec_id: str
    field_key: str
    label_tr: str
    reg_reference: str
    required: str  # MUST / CONDITIONAL / DEFINITIVE_ONLY / SHOULD
    status: str  # PASS / FAIL / WARN
    message_tr: str
    remediation_tr: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "spec_id": self.spec_id,
            "field_key": self.field_key,
            "label_tr": self.label_tr,
            "reg_reference": self.reg_reference,
            "required": self.required,
            "status": self.status,
            "message_tr": self.message_tr,
            "remediation_tr": self.remediation_tr,
            "details": self.details,
        }


def _spec_reg_ref(spec: dict) -> str:
    refs = spec.get("regulation_refs") or []
    if isinstance(refs, list) and refs:
        name = (refs[0] or {}).get("name")
        return str(name or spec.get("spec_id") or "regulatory_spec")
    return str(spec.get("spec_id") or "regulatory_spec")


def _is_missing_value(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    if isinstance(v, (list, tuple, set, dict)) and len(v) == 0:
        return True
    return False


def _must_fields(spec: dict) -> List[dict]:
    fields = spec.get("fields") or []
    out = []
    for f in fields:
        req = str((f or {}).get("required") or "").strip().upper()
        if req == "MUST":
            out.append(f)
    return out


def validate_strict(
    *,
    project_id: int,
    config: Dict[str, Any],
    results_json: Dict[str, Any],
    spec_id: str,
    tr_ets_mode: bool = False,
) -> Tuple[str, List[StrictCheck]]:
    """
    HARD FAIL validator.

    - MUST alan eksikse: FAIL
    - CONDITIONAL: config'a bağlı (bu adımda konservatif yaklaşım: eksikse FAIL, ama ayrıntı conditions Adım-3/5'te)
    - DEFINITIVE_ONLY: 2026+ CBAM definitive için (bu adımda sadece bilgi amaçlı WARN)
    - SHOULD: WARN (rapor üretimini engellemez)

    Çıktı:
      overall_status: PASS / FAIL
      checks[]: denetçi okunabilir, Türkçe remediation içerir.
    """
    spec = get_spec(spec_id)
    reg_ref = _spec_reg_ref(spec)

    # Step-1'deki structural kontroller + dataset kolonları
    structural: List[MappingCheck] = []
    with db() as s:
        structural = assess_project_against_spec(s, int(project_id), spec_id)

        # Extra regulatory hard requirements not visible in dataset columns:
        # - Monitoring plan must exist for ETS/TR ETS
        # - Factor set must exist and be locked for deterministic reporting
        proj = s.get(Project, int(project_id))
        mp = None
        if proj:
            # prefer latest monitoring plan
            mp = (
                s.execute(
                    select(MonitoringPlan)
                    .where(MonitoringPlan.project_id == int(project_id))
                    .order_by(MonitoringPlan.id.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )

        factor_set_locked = None
        factor_set_id = None
        try:
            ib = (results_json or {}).get("input_bundle") or {}
            fs_ref = (ib.get("factor_set_ref") or {})
            factor_set_id = fs_ref.get("factor_set_id") if isinstance(fs_ref, dict) else None
        except Exception:
            factor_set_id = None
        if factor_set_id is not None:
            fs = s.get(FactorSet, int(factor_set_id))
            if fs:
                factor_set_locked = bool(getattr(fs, "locked", False))
            else:
                factor_set_locked = None

    checks: List[StrictCheck] = []

    # Helper: add check
    def add(rule_id: str, field_key: str, label_tr: str, required: str, status: str, msg: str, fix: str, details: Dict[str, Any]):
        checks.append(
            StrictCheck(
                rule_id=rule_id,
                spec_id=spec_id,
                field_key=field_key,
                label_tr=label_tr,
                reg_reference=reg_ref,
                required=required,
                status=status,
                message_tr=msg,
                remediation_tr=fix,
                details=details or {},
            )
        )

    # 1) Mapping structural results -> strict
    for c in structural:
        req = str(c.required or "").strip().upper()
        status = "PASS"
        if c.status == "MISSING":
            if req == "MUST":
                status = "FAIL"
            elif req == "CONDITIONAL":
                # conservative: FAIL (Adım-3 ile koşulları netleştireceğiz)
                status = "FAIL"
            elif req == "DEFINTIVE_ONLY" or req == "DEFINITIVE_ONLY":
                status = "WARN"
            else:
                status = "WARN"
        elif c.status == "UNKNOWN":
            # If MUST and unknown, treat as FAIL because we cannot prove completeness
            if req == "MUST":
                status = "FAIL"
            elif req == "CONDITIONAL":
                status = "FAIL"
            else:
                status = "WARN"

        if status != "PASS":
            msg = f"Zorunlu alan kontrolü başarısız: {c.label_tr}"
            if status == "WARN":
                msg = f"Alan doğrulanamadı / önerilir: {c.label_tr}"

            fix = c.reason_tr or "İlgili alanı doldurun ve/veya gerekli dataset kolonlarını ekleyin."
            add(
                rule_id=f"{spec_id}_FIELD_{_norm(c.field_key).upper()}",
                field_key=c.field_key,
                label_tr=c.label_tr,
                required=req or "SHOULD",
                status=status,
                msg=msg,
                fix=fix,
                details={
                    "source": c.source,
                    "internal_path": c.internal_path,
                    "struct_status": c.status,
                    "reason_tr": c.reason_tr,
                },
            )

    # 2) Config MUST keys check via internal_path (config.*)
    for f in _must_fields(spec):
        src = str((f or {}).get("source") or "")
        internal = str((f or {}).get("internal_path") or "")
        key = str((f or {}).get("key") or "")
        label = str((f or {}).get("label_tr") or key)
        if src == "config" and internal.startswith("config."):
            v = _get_in({"config": config or {}}, internal)
            if _is_missing_value(v):
                add(
                    rule_id=f"{spec_id}_CONFIG_{_norm(key).upper()}",
                    field_key=key,
                    label_tr=label,
                    required="MUST",
                    status="FAIL",
                    msg=f"Zorunlu konfigürasyon alanı eksik: {label}",
                    fix="Ayarlar bölümünde raporlama dönemi/yıl gibi zorunlu alanları doldurun.",
                    details={"internal_path": internal},
                )

    # 3) ETS/TR ETS: monitoring plan must exist
    if spec_id.startswith("ETS_") or tr_ets_mode:
        # Monitoring plan ref can be in results_json.input_bundle.monitoring_plan_ref or DB.
        mp_ok = False
        try:
            ib = (results_json or {}).get("input_bundle") or {}
            mp_ref = ib.get("monitoring_plan_ref")
            if isinstance(mp_ref, dict) and mp_ref.get("monitoring_plan_id"):
                mp_ok = True
        except Exception:
            mp_ok = False
        if not mp_ok:
            add(
                rule_id=f"{spec_id}_MONITORING_PLAN_REQUIRED",
                field_key="monitoring_plan",
                label_tr="Monitoring Plan (İzleme Planı)",
                required="MUST",
                status="FAIL",
                msg="Monitoring plan kaydı yok veya snapshot'a bağlanmamış.",
                fix="Monitoring Plan sayfasından izleme planı oluşturun ve snapshot üretimini tekrar çalıştırın.",
                details={},
            )

    # 4) Factor set must be locked for regulatory-grade reporting
    # (Determinism + audit requirement)
    ib = (results_json or {}).get("input_bundle") or {}
    fs_ref = ib.get("factor_set_ref") or {}
    fs_id = fs_ref.get("factor_set_id") if isinstance(fs_ref, dict) else None
    if fs_id is None:
        add(
            rule_id=f"{spec_id}_FACTOR_SET_REQUIRED",
            field_key="factor_set",
            label_tr="Faktör Seti",
            required="MUST",
            status="FAIL",
            msg="Faktör seti seçili değil (factor_set_id yok).",
            fix="Faktör Kataloğu'ndan bir faktör seti seçin ve kilitleyin (locked).",
            details={},
        )
    else:
        # verify locked flag if possible
        locked = None
        try:
            with db() as s:
                fs = s.get(FactorSet, int(fs_id))
                if fs:
                    locked = bool(getattr(fs, "locked", False))
        except Exception:
            locked = None
        if locked is False:
            add(
                rule_id=f"{spec_id}_FACTOR_SET_NOT_LOCKED",
                field_key="factor_set.locked",
                label_tr="Faktör Seti Kilidi",
                required="MUST",
                status="FAIL",
                msg="Faktör seti kilitli değil. Denetimde drift riski var.",
                fix="Faktör setini kilitleyin (locked=true). Kilitlenmeyen faktör setiyle regülasyon raporu üretilmez.",
                details={"factor_set_id": fs_id},
            )

    # 5) CBAM table row completeness (hard fail for required fields)
    if spec_id.startswith("CBAM_"):
        table = (results_json or {}).get("cbam_table") or (results_json or {}).get("cbam", {}).get("table") or []
        if not isinstance(table, list) or len(table) == 0:
            add(
                rule_id=f"{spec_id}_GOODS_REQUIRED",
                field_key="cbam.goods",
                label_tr="CBAM Ürün Satırları",
                required="MUST",
                status="FAIL",
                msg="CBAM ürün tablosu boş. CN kodlu en az 1 ürün satırı gerekir.",
                fix="CBAM ürünlerini/üretim miktarlarını yükleyin veya production.csv içinde ürün satırlarını tanımlayın.",
                details={},
            )
        else:
            missing_rows = 0
            for r in table[:5000]:
                if not isinstance(r, dict):
                    missing_rows += 1
                    continue
                # required fields
                if _is_missing_value(r.get("cn_code")):
                    missing_rows += 1
                    continue
                if r.get("eu_import_quantity") is None and r.get("quantity") is None:
                    missing_rows += 1
                    continue
                # emissions/intensity
                if r.get("embedded_emissions") is None and r.get("embedded_emissions_tco2e") is None:
                    missing_rows += 1
                    continue
                if r.get("intensity") is None and r.get("intensity_tco2e_per_ton") is None:
                    missing_rows += 1
                    continue
                flag = _norm(r.get("data_type_flag") or r.get("actual_default_flag"))
                if flag not in ("actual", "default"):
                    missing_rows += 1
                    continue
            if missing_rows > 0:
                add(
                    rule_id=f"{spec_id}_GOODS_ROWS_INCOMPLETE",
                    field_key="cbam.goods.rows",
                    label_tr="CBAM Ürün Satırı Zorunlu Alanları",
                    required="MUST",
                    status="FAIL",
                    msg=f"CBAM ürün satırlarında eksik/uygunsuz alan var: {missing_rows} satır hatalı.",
                    fix="Her satır için CN kodu, miktar, embedded emissions ve intensity alanlarını ve actual/default bayrağını doldurun.",
                    details={"bad_rows": missing_rows},
                )

    overall = "PASS"
    if any(c.status == "FAIL" and str(c.required).upper() in ("MUST", "CONDITIONAL") for c in checks):
        overall = "FAIL"

    return overall, checks


def build_compliance_checks_json(
    *,
    project_id: int,
    snapshot_id: Optional[int],
    config: Dict[str, Any],
    results_json: Dict[str, Any],
    tr_ets_mode: bool = False,
) -> Dict[str, Any]:
    """
    Standard JSON output for compliance_checks.json
    """
    statuses: Dict[str, Any] = {}
    checks_out: List[Dict[str, Any]] = []

    # ETS spec
    ets_overall, ets_checks = validate_strict(
        project_id=int(project_id),
        config=config or {},
        results_json=results_json or {},
        spec_id="ETS_MRR_2018_2066",
        tr_ets_mode=bool(tr_ets_mode),
    )
    statuses["ETS_MRR_2018_2066"] = ets_overall
    checks_out.extend([c.to_dict() for c in ets_checks])

    # CBAM spec
    cbam_overall, cbam_checks = validate_strict(
        project_id=int(project_id),
        config=config or {},
        results_json=results_json or {},
        spec_id="CBAM_2023_956_2023_1773",
        tr_ets_mode=bool(tr_ets_mode),
    )
    statuses["CBAM_2023_956_2023_1773"] = cbam_overall
    checks_out.extend([c.to_dict() for c in cbam_checks])

    overall = "PASS" if all(v == "PASS" for v in statuses.values()) else "FAIL"

    return {
        "schema": "compliance_checks.v1",
        "generated_at_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "project_id": int(project_id),
        "snapshot_id": int(snapshot_id) if snapshot_id is not None else None,
        "overall_status": overall,
        "by_spec": statuses,
        "checks": checks_out,
    }
