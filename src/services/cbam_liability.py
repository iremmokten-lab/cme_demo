from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

# EU ETS free allocation phase-out alignment for CBAM certificates (CBAM factor = remaining free allocation share)
_CBAM_FACTOR = {
    2026: 0.975,
    2027: 0.95,
    2028: 0.90,
    2029: 0.775,
    2030: 0.515,
    2031: 0.39,
    2032: 0.265,
    2033: 0.14,
    2034: 0.0,
}


def cbam_payable_share(year: int) -> float:
    """
    Payable share of embedded emissions (0..1).
    Transitional period (<=2025) => reporting only => 0
    2026+ => increases based on phase-in.
    """
    y = int(year)
    if y <= 2025:
        return 0.0
    if y in _CBAM_FACTOR:
        return max(0.0, min(1.0, 1.0 - float(_CBAM_FACTOR[y])))
    if y > 2034:
        return 1.0
    return 1.0


@dataclass
class CBAMLiability:
    year: int
    embedded_emissions_tco2: float
    payable_share: float
    payable_emissions_tco2: float
    eu_ets_price_eur_per_t: float
    carbon_price_paid_eur_per_t: float
    effective_paid_ratio: float
    certificates_required: float
    estimated_payable_amount_eur: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "year": self.year,
            "embedded_emissions_tco2": self.embedded_emissions_tco2,
            "payable_share": self.payable_share,
            "payable_emissions_tco2": self.payable_emissions_tco2,
            "eu_ets_price_eur_per_t": self.eu_ets_price_eur_per_t,
            "carbon_price_paid_eur_per_t": self.carbon_price_paid_eur_per_t,
            "effective_paid_ratio": self.effective_paid_ratio,
            "certificates_required": self.certificates_required,
            "estimated_payable_amount_eur": self.estimated_payable_amount_eur,
        }


def compute_cbam_liability(
    *,
    year: int,
    embedded_emissions_tco2: float,
    eu_ets_price_eur_per_t: float,
    carbon_price_paid_eur_per_t: float = 0.0,
) -> CBAMLiability:
    """
    CBAM liability estimate:
      payable_emissions = embedded_emissions * payable_share
      effective_paid_ratio = min(carbon_price_paid / eu_ets_price, 1)
      certificates_required = payable_emissions * (1 - effective_paid_ratio)
      payable_amount = certificates_required * eu_ets_price
    """
    ee = max(0.0, float(embedded_emissions_tco2 or 0.0))
    price = max(0.0, float(eu_ets_price_eur_per_t or 0.0))
    paid = max(0.0, float(carbon_price_paid_eur_per_t or 0.0))

    share = cbam_payable_share(int(year))
    payable_em = ee * share

    ratio = 0.0
    if price > 0:
        ratio = min(1.0, paid / price)

    certs = max(0.0, payable_em * (1.0 - ratio))
    amount = certs * price

    return CBAMLiability(
        year=int(year),
        embedded_emissions_tco2=ee,
        payable_share=share,
        payable_emissions_tco2=payable_em,
        eu_ets_price_eur_per_t=price,
        carbon_price_paid_eur_per_t=paid,
        effective_paid_ratio=ratio,
        certificates_required=certs,
        estimated_payable_amount_eur=amount,
    )
