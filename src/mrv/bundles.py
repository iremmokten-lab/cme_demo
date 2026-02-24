from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from src.mrv.lineage import canonical_json, sha256_json


def _as_str(x: Any, default: str = "") -> str:
    try:
        s = str(x)
        return s
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _sorted_list_of_dicts(rows: List[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
    def kf(d: Dict[str, Any]) -> Tuple:
        return tuple(_as_str(d.get(k, "")) for k in keys)

    return sorted((r or {}) for r in (rows or []), key=kf)


@dataclass(frozen=True)
class FactorRef:
    """
    Factor set version ref: deterministik kilit için kullanılır.
    """
    id: Optional[int]
    factor_type: str
    region: str
    year: Optional[int]
    version: str
    value: float
    unit: str
    source: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "factor_type": self.factor_type,
            "region": self.region,
            "year": self.year,
            "version": self.version,
            "value": self.value,
            "unit": self.unit,
            "source": self.source,
        }


@dataclass(frozen=True)
class MonitoringPlanRef:
    id: Optional[int]
    facility_id: Optional[int]
    method: str
    tier_level: str
    updated_at: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "facility_id": self.facility_id,
            "method": self.method,
            "tier_level": self.tier_level,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class PriceRef:
    """
    Fiyat referansı: ETS/CBAM maliyet çıktılarında deterministik kilit.
    """
    eua_price_eur_per_t: float
    fx_tl_per_eur: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "eua_price_eur_per_t": self.eua_price_eur_per_t,
            "fx_tl_per_eur": self.fx_tl_per_eur,
        }


@dataclass(frozen=True)
class InputBundle:
    """
    Core veri sözleşmesi (deterministik):
      - period
      - facility
      - product mapping
      - activity data snapshot ref
      - monitoring plan version ref
      - factor set version ref
      - price ref
      - config hash
    """
    engine_version: str
    project_id: int
    period: Dict[str, Any]
    facility: Dict[str, Any]
    product_mapping: List[Dict[str, Any]]
    activity_snapshot_ref: Dict[str, Any]      # uploads hash refs (energy/production/materials)
    monitoring_plan_ref: Optional[MonitoringPlanRef]
    factor_set_ref: List[FactorRef]
    price_ref: PriceRef
    config: Dict[str, Any]
    config_hash: str
    methodology_ref: Optional[Dict[str, Any]] = None
    scenario: Dict[str, Any] = field(default_factory=dict)

    def to_canonical_dict(self) -> Dict[str, Any]:
        # Tam deterministik sıralama: factor_set_ref, product_mapping sabit order
        factors = _sorted_list_of_dicts([f.to_dict() for f in (self.factor_set_ref or [])], keys=["factor_type", "region", "version", "id"])
        products = _sorted_list_of_dicts(self.product_mapping or [], keys=["cn_code", "product_name", "product_code"])

        return {
            "engine_version": self.engine_version,
            "project_id": self.project_id,
            "period": self.period or {},
            "facility": self.facility or {},
            "product_mapping": products,
            "activity_snapshot_ref": self.activity_snapshot_ref or {},
            "monitoring_plan_ref": (self.monitoring_plan_ref.to_dict() if self.monitoring_plan_ref else None),
            "factor_set_ref": factors,
            "price_ref": self.price_ref.to_dict(),
            "config": self.config or {},
            "config_hash": self.config_hash,
            "methodology_ref": self.methodology_ref,
            "scenario": self.scenario or {},
        }

    def input_bundle_hash(self) -> str:
        return sha256_json(self.to_canonical_dict())


@dataclass(frozen=True)
class QAFlag:
    """
    QA flags: evidence pack içine girebilecek net, deterministik JSON.
    """
    flag_id: str
    severity: str   # info/warn/fail
    message_tr: str
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "flag_id": self.flag_id,
            "severity": self.severity,
            "message_tr": self.message_tr,
            "context": self.context or {},
        }


@dataclass(frozen=True)
class ComplianceCheck:
    rule_id: str
    reg_reference: str          # 2018/2066, 2018/2067, 2023/1773
    severity: str               # info/warn/fail
    status: str                 # pass/warn/fail
    message_tr: str
    remediation_tr: str
    evidence_requirements: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "reg_reference": self.reg_reference,
            "severity": self.severity,
            "status": self.status,
            "message_tr": self.message_tr,
            "remediation_tr": self.remediation_tr,
            "evidence_requirements": list(self.evidence_requirements or []),
            "details": self.details or {},
        }


@dataclass(frozen=True)
class ResultBundle:
    """
    ResultBundle:
      - totals (tCO2e)
      - breakdown tree (fuel/process/product)
      - unit dönüşümleri
      - kaynak referansları
      - QA flags
      - compliance checks
      - cost outputs (CBAM/ETS)
    """
    engine_version: str
    input_bundle_hash: str
    result_hash: str
    totals: Dict[str, Any]
    breakdown: Dict[str, Any]
    unit_conversions: Dict[str, Any]
    source_references: Dict[str, Any]
    qa_flags: List[QAFlag] = field(default_factory=list)
    compliance_checks: List[ComplianceCheck] = field(default_factory=list)
    cost_outputs: Dict[str, Any] = field(default_factory=dict)

    def to_canonical_dict(self) -> Dict[str, Any]:
        qa = _sorted_list_of_dicts([q.to_dict() for q in (self.qa_flags or [])], keys=["severity", "flag_id"])
        cc = _sorted_list_of_dicts([c.to_dict() for c in (self.compliance_checks or [])], keys=["reg_reference", "severity", "rule_id"])
        return {
            "engine_version": self.engine_version,
            "input_bundle_hash": self.input_bundle_hash,
            "result_hash": self.result_hash,
            "totals": self.totals or {},
            "breakdown": self.breakdown or {},
            "unit_conversions": self.unit_conversions or {},
            "source_references": self.source_references or {},
            "qa_flags": qa,
            "compliance_checks": cc,
            "cost_outputs": self.cost_outputs or {},
        }

    def canonical_json(self) -> str:
        return canonical_json(self.to_canonical_dict())

    def verify_hash(self) -> bool:
        # result_hash doğrulama: ResultBundle canonical JSON üzerinden
        return self.result_hash == sha256_json({"result": self.to_canonical_dict()})
