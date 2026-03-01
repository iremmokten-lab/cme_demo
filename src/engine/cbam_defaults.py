from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from src.mrv.lineage import sha256_json


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _clean_cn(x: Any) -> str:
    s = str(x or "").strip().replace(".", "").replace(" ", "")
    return s


def _to_float(x: Any) -> float:
    try:
        if pd.isna(x):
            return 0.0
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return 0.0


@dataclass(frozen=True)
class DefaultValueEvidence:
    """
    Default value evidence for CBAM Transitional period.
    We pin the chosen default row to ensure determinism in snapshots.
    """
    default_key: str               # cn:<code> or good:<key>
    direct_intensity_tco2_per_unit: float
    indirect_intensity_tco2_per_unit: float
    unit: str
    source: str
    version: str
    valid_from: str
    valid_to: str
    row_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "default_key": self.default_key,
            "direct_intensity_tco2_per_unit": self.direct_intensity_tco2_per_unit,
            "indirect_intensity_tco2_per_unit": self.indirect_intensity_tco2_per_unit,
            "unit": self.unit,
            "source": self.source,
            "version": self.version,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "row_hash": self.row_hash,
        }


def resolve_default_intensities(
    *,
    cn_code: str,
    cbam_good_key: str,
    quantity_unit: str,
    reporting_year: int,
    defaults_df: Optional[pd.DataFrame],
) -> Tuple[Optional[DefaultValueEvidence], float, float]:
    """
    Resolve default direct+indirect intensities for a product row.

    Expected defaults_df columns (flexible):
      - cn_code (optional)
      - cbam_good_key (optional)
      - direct_intensity_tco2_per_unit
      - indirect_intensity_tco2_per_unit
      - unit (default 't')
      - source (document / url / citation)
      - version
      - valid_from (YYYY-MM-DD or year)
      - valid_to (YYYY-MM-DD or year)
      - priority (higher wins)

    Matching rules (deterministic):
      1) exact cn_code match (after cleaning)
      2) cbam_good_key match
    Within hits:
      - prefer rows whose unit matches quantity_unit
      - then higher priority
      - then latest valid_from
    """
    if defaults_df is None or len(defaults_df) == 0:
        return None, 0.0, 0.0

    df = defaults_df.copy()
    df.columns = [_norm(c) for c in df.columns]

    if "direct_intensity_tco2_per_unit" not in df.columns:
        # tolerate alternative naming
        for alt in ("direct_intensity", "default_direct_intensity", "direct_tco2_per_unit"):
            if alt in df.columns:
                df["direct_intensity_tco2_per_unit"] = df[alt]
                break
    if "indirect_intensity_tco2_per_unit" not in df.columns:
        for alt in ("indirect_intensity", "default_indirect_intensity", "indirect_tco2_per_unit"):
            if alt in df.columns:
                df["indirect_intensity_tco2_per_unit"] = df[alt]
                break

    if "direct_intensity_tco2_per_unit" not in df.columns:
        df["direct_intensity_tco2_per_unit"] = 0.0
    if "indirect_intensity_tco2_per_unit" not in df.columns:
        df["indirect_intensity_tco2_per_unit"] = 0.0

    df["direct_intensity_tco2_per_unit"] = df["direct_intensity_tco2_per_unit"].apply(_to_float)
    df["indirect_intensity_tco2_per_unit"] = df["indirect_intensity_tco2_per_unit"].apply(_to_float)

    if "cn_code" not in df.columns:
        df["cn_code"] = ""
    if "cbam_good_key" not in df.columns:
        df["cbam_good_key"] = ""
    if "unit" not in df.columns:
        df["unit"] = "t"
    if "source" not in df.columns:
        df["source"] = ""
    if "version" not in df.columns:
        df["version"] = ""
    if "valid_from" not in df.columns:
        df["valid_from"] = ""
    if "valid_to" not in df.columns:
        df["valid_to"] = ""
    if "priority" not in df.columns:
        df["priority"] = 0

    df["cn_code_clean"] = df["cn_code"].apply(_clean_cn)
    cn_clean = _clean_cn(cn_code)
    good_key = _norm(cbam_good_key)
    unit = _norm(quantity_unit) or "t"

    # candidates
    exact = df[df["cn_code_clean"] == cn_clean] if cn_clean else df.iloc[0:0]
    good = df[df["cbam_good_key"].apply(_norm) == good_key] if good_key else df.iloc[0:0]
    candidates = exact if len(exact) > 0 else good
    if len(candidates) == 0:
        return None, 0.0, 0.0

    # unit match preference
    candidates = candidates.copy()
    candidates["unit_norm"] = candidates["unit"].apply(_norm)
    candidates["unit_match"] = (candidates["unit_norm"] == unit).astype(int)

    # valid_from: parse year-ish
    def _vf_score(v: Any) -> int:
        s = str(v or "").strip()
        if not s:
            return 0
        # try yyyy-mm-dd
        try:
            return int(s.split("-")[0])
        except Exception:
            pass
        try:
            return int(float(s))
        except Exception:
            return 0

    candidates["vf_year"] = candidates["valid_from"].apply(_vf_score)
    candidates["priority_i"] = candidates["priority"].apply(lambda x: int(_to_float(x)))

    # deterministic sort: unit match desc, priority desc, vf_year desc, cn_code length desc
    candidates["cn_len"] = candidates["cn_code_clean"].apply(lambda x: len(str(x or "")))
    candidates = candidates.sort_values(
        by=["unit_match", "priority_i", "vf_year", "cn_len"],
        ascending=[False, False, False, False],
        kind="mergesort",
    )

    chosen = candidates.iloc[0].to_dict()

    evidence = DefaultValueEvidence(
        default_key=("cn:" + cn_clean) if (cn_clean and len(exact) > 0) else ("good:" + good_key),
        direct_intensity_tco2_per_unit=float(chosen.get("direct_intensity_tco2_per_unit") or 0.0),
        indirect_intensity_tco2_per_unit=float(chosen.get("indirect_intensity_tco2_per_unit") or 0.0),
        unit=str(chosen.get("unit") or "t"),
        source=str(chosen.get("source") or ""),
        version=str(chosen.get("version") or ""),
        valid_from=str(chosen.get("valid_from") or ""),
        valid_to=str(chosen.get("valid_to") or ""),
        row_hash=sha256_json(
            {
                "cn_code": str(chosen.get("cn_code") or ""),
                "cbam_good_key": str(chosen.get("cbam_good_key") or ""),
                "direct_intensity_tco2_per_unit": float(chosen.get("direct_intensity_tco2_per_unit") or 0.0),
                "indirect_intensity_tco2_per_unit": float(chosen.get("indirect_intensity_tco2_per_unit") or 0.0),
                "unit": str(chosen.get("unit") or "t"),
                "source": str(chosen.get("source") or ""),
                "version": str(chosen.get("version") or ""),
                "valid_from": str(chosen.get("valid_from") or ""),
                "valid_to": str(chosen.get("valid_to") or ""),
                "priority": int(_to_float(chosen.get("priority") or 0)),
            }
        ),
    )

    return evidence, evidence.direct_intensity_tco2_per_unit, evidence.indirect_intensity_tco2_per_unit
