from __future__ import annotations

from typing import Any, Dict, List


def require_company_id(user: Dict[str, Any]) -> int:
    """
    Kullanıcının company_id'sini döndürür.
    Eğer yoksa demo company oluşturur.
    """

    if not user:
        return 1

    cid = user.get("company_id")

    if cid:
        return cid

    # fallback (demo / consultant mode)
    return 1


def list_companies_for_user(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Consultant panel için şirket listesi.
    """

    cid = require_company_id(user)

    companies = [
        {
            "id": cid,
            "name": "Demo Manufacturing Group",
            "country": "TR",
            "sector": "Steel"
        }
    ]

    return companies
