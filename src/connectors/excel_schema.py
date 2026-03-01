ENERGY_SCHEMA = {
    "facility_id": str,
    "month": str,
    "fuel_type": str,
    "quantity": float,
    "unit": str
}

PRODUCTION_SCHEMA = {
    "facility_id": str,
    "month": str,
    "product": str,
    "quantity": float,
    "unit": str
}

FACILITY_SCHEMA = {
    "facility_id": str,
    "facility_name": str,
    "country": str,
    "sector": str
}

SCHEMAS = {
    "energy": ENERGY_SCHEMA,
    "production": PRODUCTION_SCHEMA,
    "facility": FACILITY_SCHEMA
}
