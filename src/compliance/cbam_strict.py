from __future__ import annotations

from typing import Dict, Any, List

def cbam_strict_checks(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    cbam = (results or {}).get("cbam") if isinstance(results, dict) else {}
    table = (cbam or {}).get("table") or []
    xml = (cbam or {}).get("cbam_reporting") or ""
    if not xml:
        checks.append({"code":"CBAM_XML_REQUIRED","status":"FAIL","message_tr":"CBAM XML zorunlu (portal submission)."})
    if not table:
        checks.append({"code":"CBAM_TABLE_REQUIRED","status":"FAIL","message_tr":"CBAM ürün satırları boş olamaz."})
    # minimal: require each row has cn_code and embedded emissions
    for i, row in enumerate(table):
        cn = str((row or {}).get("cn_code") or "").strip()
        emb = (row or {}).get("embedded_emissions", None)
        if not cn:
            checks.append({"code":"CBAM_CN_REQUIRED","status":"FAIL","message_tr":f"Satır {i+1}: CN kodu zorunlu."})
        if emb is None:
            checks.append({"code":"CBAM_EMBEDDED_REQUIRED","status":"FAIL","message_tr":f"Satır {i+1}: embedded emissions zorunlu."})
    return checks
