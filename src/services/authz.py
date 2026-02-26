from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DemoUser:
    id: int
    email: str
    role: str
    company_id: int | None = None


def get_or_create_demo_user() -> DemoUser:

    return DemoUser(
        id=1,
        email="consultant@demo.com",
        role="consultant",
        company_id=None
    )
