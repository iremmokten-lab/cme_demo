from __future__ import annotations

import os
from datetime import date

import streamlit as st


def _get_secret(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default


def _get_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def get_tr_ets_mode() -> bool:
    """Türkiye ETS modu.

    Öncelik:
      1) Streamlit secrets: TR_ETS_MODE
      2) ENV: TR_ETS_MODE
      3) default False
    """
    v = _get_secret("TR_ETS_MODE", None)
    if v is None:
        v = os.getenv("TR_ETS_MODE", None)
    return _get_bool(v, False)


def get_evidence_pack_hmac_key() -> str:
    """Evidence pack imzası için HMAC key.

    Streamlit secrets: EVIDENCE_PACK_HMAC_KEY
    ENV: EVIDENCE_PACK_HMAC_KEY
    """
    v = _get_secret("EVIDENCE_PACK_HMAC_KEY", None)
    if v is None:
        v = os.getenv("EVIDENCE_PACK_HMAC_KEY", "")
    return str(v or "")


def get_cbam_reporting_year() -> int:
    """CBAM definitive regime starts 2026. Defaults to current year (UTC) but min 2023."""
    v = _get_secret("CBAM_REPORTING_YEAR", None)
    if v is None:
        v = os.getenv("CBAM_REPORTING_YEAR", None)
    if v is None or str(v).strip() == "":
        y = date.today().year
        return int(max(2023, y))
    try:
        return int(v)
    except Exception:
        return date.today().year


def get_eu_ets_reference_price_eur_per_t() -> float:
    """EU ETS reference price (€/tCO2) used for CBAM certificate pricing when no market feed is provided.

    Streamlit secrets: EU_ETS_PRICE_EUR_PER_T
    ENV: EU_ETS_PRICE_EUR_PER_T
    Default: 0.0 (user must provide in UI/config for payable amount)
    """
    v = _get_secret("EU_ETS_PRICE_EUR_PER_T", None)
    if v is None:
        v = os.getenv("EU_ETS_PRICE_EUR_PER_T", None)
    try:
        return float(v)
    except Exception:
        return 0.0


def get_tr_ets_pilot_years() -> tuple[int, int]:
    """TR ETS pilot years (default 2026-2027) based on published draft summary."""
    start = _get_secret("TR_ETS_PILOT_START_YEAR", None) or os.getenv("TR_ETS_PILOT_START_YEAR", None)
    end = _get_secret("TR_ETS_PILOT_END_YEAR", None) or os.getenv("TR_ETS_PILOT_END_YEAR", None)
    try:
        s = int(start) if start else 2026
    except Exception:
        s = 2026
    try:
        e = int(end) if end else 2027
    except Exception:
        e = 2027
    return s, e


def get_tr_ets_threshold_tco2() -> float:
    """TR ETS scope threshold (default 50,000 tCO2 capacity/yr in draft summary)."""
    v = _get_secret("TR_ETS_THRESHOLD_TCO2", None)
    if v is None:
        v = os.getenv("TR_ETS_THRESHOLD_TCO2", None)
    try:
        return float(v)
    except Exception:
        return 50000.0
