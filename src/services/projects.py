from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class Company:
    id: int
    name: str
    country: str
    sector: str


def require_company_id(user: Dict[str, Any]) -> int:

    if not user:
        return 1

    cid = user.get("company_id")

    if cid:
        return cid

    return 1


def list_companies_for_user(user: Dict[str, Any]) -> List[Company]:

    cid = require_company_id(user)

    companies = [
        Company(
            id=cid,
            name="Demo Manufacturing Group",
            country="TR",
            sector="Steel"
        )
    ]

    return companies
