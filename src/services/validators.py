from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ValidationIssue:
    rule_id: str
    reg_reference: str
    severity: str  # info/warn/fail
    message_tr: str
    remediation_tr: str
    details: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "reg_reference": self.reg_reference,
            "severity": self.severity,
            "message_tr": self.message_tr,
            "remediation_tr": self.remediation_tr,
            "details": self.details or {},
        }


def _get(d: Dict[str, Any], path: str, default=None):
    cur: Any = d
    for p in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


def validate_cbam_report(cbam_report: Dict[str, Any]) -> List[ValidationIssue]:
    """CBAM transitional report dataset validation (best-effort checklist).

    References:
      - Regulation (EU) 2023/956 (CBAM)
      - Implementing Regulation (EU) 2023/1773 (transitional reporting)
    """
    issues: List[ValidationIssue] = []

    if not isinstance(cbam_report, dict) or not cbam_report:
        issues.append(
            ValidationIssue(
                rule_id="CBAM.STRUCT.000",
                reg_reference="EU 2023/1773 Annex I",
                severity="fail",
                message_tr="CBAM raporu boş veya okunamıyor.",
                remediation_tr="CBAM hesaplamasını çalıştırın ve cbam_report.json üretildiğinden emin olun.",
            )
        )
        return issues

    header = _get(cbam_report, "header", {}) or {}
    goods = _get(cbam_report, "goods", []) or _get(cbam_report, "cbam_goods", []) or []

    # Header checks
    for field, rid in [
        ("reporting_period", "CBAM.HEAD.010"),
        ("declarant", "CBAM.HEAD.020"),
    ]:
        if not header.get(field):
            issues.append(
                ValidationIssue(
                    rule_id=rid,
                    reg_reference="EU 2023/1773 Annex I",
                    severity="fail",
                    message_tr=f"CBAM header alanı eksik: {field}",
                    remediation_tr="Header bilgilerini (dönem, beyan eden) doldurun.",
                )
            )

    if not isinstance(goods, list) or len(goods) == 0:
        issues.append(
            ValidationIssue(
                rule_id="CBAM.GOODS.010",
                reg_reference="EU 2023/1773 Annex I",
                severity="fail",
                message_tr="CBAM goods listesi boş.",
                remediation_tr="En az 1 ürün için quantity + embedded emissions hesaplanmalı.",
            )
        )
        return issues

    # Goods-level checks
    for i, g in enumerate(goods, start=1):
        if not isinstance(g, dict):
            continue
        cn = g.get("cn_code") or g.get("goods_code") or ""
        if not cn:
            issues.append(
                ValidationIssue(
                    rule_id="CBAM.GOODS.020",
                    reg_reference="EU 2023/1773 Annex I",
                    severity="fail",
                    message_tr=f"Goods #{i}: CN/HS code eksik.",
                    remediation_tr="Ürün satırında CN kodunu doldurun.",
                    details={"index": i},
                )
            )
        qty = g.get("quantity") or g.get("mass") or g.get("net_mass") or 0
        try:
            qty_f = float(qty)
        except Exception:
            qty_f = 0.0
        if qty_f <= 0:
            issues.append(
                ValidationIssue(
                    rule_id="CBAM.GOODS.030",
                    reg_reference="EU 2023/1773 Annex I",
                    severity="fail",
                    message_tr=f"Goods #{i}: quantity sıfır/negatif.",
                    remediation_tr="CBAM goods quantity pozitif olmalı.",
                    details={"index": i, "cn_code": cn},
                )
            )

        # emissions
        direct = g.get("direct_emissions_tco2") or g.get("direct_tco2") or 0
        indirect = g.get("indirect_emissions_tco2") or g.get("indirect_tco2") or 0
        embedded = g.get("embedded_emissions_tco2") or g.get("embedded_tco2")
        if embedded is None:
            # allow direct+indirect composition
            try:
                embedded = float(direct) + float(indirect)
            except Exception:
                embedded = None

        if embedded is None:
            issues.append(
                ValidationIssue(
                    rule_id="CBAM.EM.010",
                    reg_reference="EU 2023/956 Art. 7; EU 2023/1773 Annex I",
                    severity="fail",
                    message_tr=f"Goods #{i}: embedded emissions eksik.",
                    remediation_tr="Direct + indirect emisyonlar hesaplanmalı ve embedded türetilmeli.",
                    details={"index": i, "cn_code": cn},
                )
            )

        # actual/default flag
        flag = g.get("data_type_flag") or g.get("actual_vs_default") or ""
        if not flag:
            issues.append(
                ValidationIssue(
                    rule_id="CBAM.FLAG.010",
                    reg_reference="EU 2023/1773 Annex III (methods) - transparency",
                    severity="warn",
                    message_tr=f"Goods #{i}: actual/default işareti eksik.",
                    remediation_tr="Faktörler veya metodoloji default ise 'default', aksi halde 'actual' işaretleyin.",
                    details={"index": i, "cn_code": cn},
                )
            )

        # carbon price paid
        cpp = g.get("carbon_price_paid_amount_eur")
        if cpp is None:
            # optional in transitional but recommended
            issues.append(
                ValidationIssue(
                    rule_id="CBAM.CPP.010",
                    reg_reference="EU 2023/956 Art. 9; EU 2023/1773 Annex I",
                    severity="info",
                    message_tr=f"Goods #{i}: carbon price paid alanı boş.",
                    remediation_tr="Eğer üretim ülkesinde karbon fiyatı ödendiyse miktarı girin.",
                    details={"index": i, "cn_code": cn},
                )
            )

    return issues


def validate_ets_reporting(ets: Dict[str, Any]) -> List[ValidationIssue]:
    """EU ETS MRR (2018/2066) — best-effort mandatory fields checklist.

    Bu validator, "MRR'a uygunluğu" garanti etmez; ama denetim için minimum zorunlu alanlar
    ve QA/QC kaydı açısından eksikleri yakalamayı hedefler.
    """
    issues: List[ValidationIssue] = []

    if not isinstance(ets, dict) or not ets:
        issues.append(
            ValidationIssue(
                rule_id="ETS.STRUCT.000",
                reg_reference="EU 2018/2066 (MRR)",
                severity="fail",
                message_tr="ETS raporlama dataseti boş.",
                remediation_tr="ETS hesaplamasını çalıştırın ve ets_reporting.json üretildiğinden emin olun.",
            )
        )
        return issues

    # required blocks
    for key, rid in [
        ("installation", "ETS.INST.010"),
        ("reporting_period", "ETS.PER.010"),
        ("source_streams", "ETS.SS.010"),
        ("totals", "ETS.TOT.010"),
        ("qa_qc", "ETS.QA.010"),
    ]:
        if key not in ets:
            issues.append(
                ValidationIssue(
                    rule_id=rid,
                    reg_reference="EU 2018/2066 (MRR)",
                    severity="fail",
                    message_tr=f"ETS dataset alanı eksik: {key}",
                    remediation_tr="ETS reporting dataset şemasını tamamlayın (installation/period/source_streams/totals/qa_qc).",
                )
            )

    # streams checks
    ss = ets.get("source_streams") or []
    if isinstance(ss, list) and len(ss) == 0:
        issues.append(
            ValidationIssue(
                rule_id="ETS.SS.020",
                reg_reference="EU 2018/2066 Art. 12, 20, 28 (boundaries, sources, streams)",
                severity="fail",
                message_tr="ETS source_streams boş.",
                remediation_tr="Yakıt/akış bazında en az 1 source stream olmalı.",
            )
        )

    for i, s in enumerate(ss or [], start=1):
        if not isinstance(s, dict):
            continue
        if not s.get("stream_id") and not s.get("fuel_type"):
            issues.append(
                ValidationIssue(
                    rule_id="ETS.SS.030",
                    reg_reference="EU 2018/2066 Art. 12",
                    severity="fail",
                    message_tr=f"Source stream #{i}: stream_id/fuel_type eksik.",
                    remediation_tr="Her stream için benzersiz id ve fuel/type tanımı girin.",
                    details={"index": i},
                )
            )
        if s.get("activity_data") is None:
            issues.append(
                ValidationIssue(
                    rule_id="ETS.AD.010",
                    reg_reference="EU 2018/2066 Art. 44-48 (activity data)",
                    severity="fail",
                    message_tr=f"Source stream #{i}: activity_data eksik.",
                    remediation_tr="Her stream için activity data (quantity + unit + period) raporlanmalı.",
                    details={"index": i},
                )
            )
        if s.get("emission_factors") is None:
            issues.append(
                ValidationIssue(
                    rule_id="ETS.EF.010",
                    reg_reference="EU 2018/2066 Art. 32-39 (calculation factors)",
                    severity="fail",
                    message_tr=f"Source stream #{i}: emission_factors eksik.",
                    remediation_tr="Her stream için EF/NCV/OF gibi calculation factor referansı olmalı.",
                    details={"index": i},
                )
            )
        if not s.get("tier"):
            issues.append(
                ValidationIssue(
                    rule_id="ETS.TIER.010",
                    reg_reference="EU 2018/2066 Art. 26-29 (tiers)",
                    severity="warn",
                    message_tr=f"Source stream #{i}: tier bilgisi yok.",
                    remediation_tr="Tier seçimi ve gerekçesi ekleyin (uncertainty + data sources).",
                    details={"index": i},
                )
            )

    # totals
    totals = ets.get("totals") or {}
    if isinstance(totals, dict):
        if totals.get("total_emissions_tco2") is None:
            issues.append(
                ValidationIssue(
                    rule_id="ETS.TOT.020",
                    reg_reference="EU 2018/2066 Art. 67-72 (reporting)",
                    severity="fail",
                    message_tr="ETS totals.total_emissions_tco2 eksik.",
                    remediation_tr="Toplam emisyon tCO2 raporlanmalı.",
                )
            )

    # QA/QC
    qa = ets.get("qa_qc") or {}
    if isinstance(qa, dict):
        if not qa.get("procedures"):
            issues.append(
                ValidationIssue(
                    rule_id="ETS.QA.020",
                    reg_reference="EU 2018/2066 Art. 58-61 (QA/QC)",
                    severity="warn",
                    message_tr="ETS QA/QC prosedür kaydı eksik.",
                    remediation_tr="En azından veri kontrol adımları + sorumlular + sıklık ekleyin.",
                )
            )

    return issues
