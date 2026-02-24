from dataclasses import dataclass
from decimal import Decimal

@dataclass(frozen=True)
class FuelInput:
    fuel_type: str
    quantity: Decimal
    ncv: Decimal
    emission_factor: Decimal
    oxidation_factor: Decimal

@dataclass(frozen=True)
class ElectricityInput:
    kwh: Decimal
    grid_factor: Decimal  # tCO2e / kWh

@dataclass(frozen=True)
class ProcessInput:
    process_type: str
    production_qty: Decimal
    factor: Decimal  # tCO2e / unit

@dataclass(frozen=True)
class CostInputs:
    ets_price: Decimal  # EUR / tCO2
    allowances: Decimal  # tCO2

# -------------------------
# CBAM product-level inputs
# -------------------------
@dataclass(frozen=True)
class ProductionInput:
    product_id: str
    quantity: Decimal

@dataclass(frozen=True)
class PrecursorInput:
    product_id: str
    material_id: str
    quantity: Decimal
    embedded_factor: Decimal  # tCO2e / unit
