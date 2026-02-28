from __future__ import annotations

import os

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
