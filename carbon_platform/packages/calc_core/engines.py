from decimal import Decimal
from packages.calc_core.models import (
    FuelInput, ElectricityInput, ProcessInput, CostInputs,
    ProductionInput, PrecursorInput
)
from packages.calc_core.utils import d, canonical_json, sha256_text

class ETS_CBAM_Engine:
    """
    Facility-level deterministik çekirdek.
    """

    def run(
        self,
        fuels: list[FuelInput],
        electricity: list[ElectricityInput],
        processes: list[ProcessInput],
        cost: CostInputs,
        production_total: Decimal | None = None
    ) -> dict:
        fuel_em = Decimal("0")
        fuel_breakdown = []
        for f in fuels:
            em = d(f.quantity) * d(f.ncv) * d(f.emission_factor) * d(f.oxidation_factor)
            fuel_em += em
            fuel_breakdown.append({
                "fuel_type": f.fuel_type,
                "quantity": str(f.quantity),
                "ncv": str(f.ncv),
                "emission_factor": str(f.emission_factor),
                "oxidation_factor": str(f.oxidation_factor),
                "tco2e": str(em),
            })

        elec_em = Decimal("0")
        elec_breakdown = []
        for e in electricity:
            em = d(e.kwh) * d(e.grid_factor)
            elec_em += em
            elec_breakdown.append({
                "kwh": str(e.kwh),
                "grid_factor": str(e.grid_factor),
                "tco2e": str(em),
            })

        proc_em = Decimal("0")
        proc_breakdown = []
        for p in processes:
            em = d(p.production_qty) * d(p.factor)
            proc_em += em
            proc_breakdown.append({
                "process_type": p.process_type,
                "production_qty": str(p.production_qty),
                "factor": str(p.factor),
                "tco2e": str(em),
            })

        facility_emissions = fuel_em + elec_em + proc_em

        intensity = None
        if production_total is not None and d(production_total) != 0:
            intensity = facility_emissions / d(production_total)

        ets_payable = facility_emissions - d(cost.allowances)
        if ets_payable < 0:
            ets_payable = Decimal("0")
        ets_cost = ets_payable * d(cost.ets_price)

        embedded_emissions = facility_emissions
        cbam_cost = embedded_emissions * d(cost.ets_price)

        result = {
            "totals": {
                "fuel_tco2e": str(fuel_em),
                "electricity_tco2e": str(elec_em),
                "process_tco2e": str(proc_em),
                "facility_tco2e": str(facility_emissions),
                "embedded_tco2e": str(embedded_emissions),
                "intensity_tco2e_per_unit": str(intensity) if intensity is not None else None,
            },
            "breakdown": {
                "fuel": fuel_breakdown,
                "electricity": elec_breakdown,
                "process": proc_breakdown,
            },
            "costs": {
                "ets_allowances_tco2": str(cost.allowances),
                "ets_payable_tco2": str(ets_payable),
                "ets_price_eur_per_tco2": str(cost.ets_price),
                "ets_cost_eur": str(ets_cost),
                "cbam_certificates_tco2": str(embedded_emissions),
                "cbam_cost_eur": str(cbam_cost),
            },
            "meta": {
                "engine": "ETS_CBAM_Engine",
                "deterministic": True,
            }
        }
        result_hash = sha256_text(canonical_json(result))
        result["meta"]["result_hash"] = result_hash
        return result


class CBAM_Product_Engine:
    """
    CBAM ürün bazlı embedded emissions:

    Embedded(product) = allocated_facility_emissions(product) + precursor_embedded(product)

    Allocation default: production share (mass-based), ürün üretim miktarına göre facility toplamını dağıtır.

    Export embedded = embedded_intensity(product) * export_qty
    CBAM certificates = export embedded (tCO2e)
    """

    def run(
        self,
        facility_totals: dict,
        productions: list[ProductionInput],
        precursors: list[PrecursorInput],
        exports: list[dict],
        ets_price_eur_per_tco2: Decimal
    ) -> dict:
        facility_tco2e = d(facility_totals.get("facility_tco2e", "0"))
        if facility_tco2e < 0:
            facility_tco2e = Decimal("0")

        prod_total = sum([d(p.quantity) for p in productions], Decimal("0"))
        if prod_total <= 0:
            raise ValueError("Ürün bazlı üretim (production_record) toplamı 0. Allocation yapılamaz.")

        # allocation by production share
        allocated = {}
        for p in productions:
            share = d(p.quantity) / prod_total
            allocated[p.product_id] = facility_tco2e * share

        # precursor totals per product
        precursor_by_product = {}
        precursor_breakdown = []
        for x in precursors:
            em = d(x.quantity) * d(x.embedded_factor)
            precursor_by_product[x.product_id] = precursor_by_product.get(x.product_id, Decimal("0")) + em
            precursor_breakdown.append({
                "product_id": x.product_id,
                "material_id": x.material_id,
                "quantity": str(x.quantity),
                "embedded_factor": str(x.embedded_factor),
                "tco2e": str(em)
            })

        # embedded totals and intensity per product
        product_rows = []
        product_embedded_total = {}
        product_intensity = {}
        for p in productions:
            pid = p.product_id
            direct_elec_proc_alloc = allocated.get(pid, Decimal("0"))
            precursor_em = precursor_by_product.get(pid, Decimal("0"))
            embedded = direct_elec_proc_alloc + precursor_em
            intensity = embedded / d(p.quantity) if d(p.quantity) != 0 else None
            product_embedded_total[pid] = embedded
            product_intensity[pid] = intensity
            product_rows.append({
                "product_id": pid,
                "production_qty": str(p.quantity),
                "allocated_facility_tco2e": str(direct_elec_proc_alloc),
                "precursor_tco2e": str(precursor_em),
                "embedded_tco2e_total": str(embedded),
                "embedded_intensity_tco2e_per_unit": str(intensity) if intensity is not None else None,
            })

        # exports
        export_rows = []
        total_export_embedded = Decimal("0")
        for ex in exports:
            pid = ex["product_id"]
            qty = d(ex["export_qty"])
            intensity = product_intensity.get(pid)
            if intensity is None:
                raise ValueError(f"Ürün intensity hesaplanamadı: product_id={pid}")
            emb = d(intensity) * qty
            total_export_embedded += emb
            export_rows.append({
                "export_id": ex.get("export_id"),
                "product_id": pid,
                "export_qty": str(qty),
                "embedded_intensity_tco2e_per_unit": str(intensity),
                "export_embedded_tco2e": str(emb),
                "destination": ex.get("destination"),
            })

        cbam_certificates = total_export_embedded
        cbam_cost = cbam_certificates * d(ets_price_eur_per_tco2)

        result = {
            "facility": {
                "facility_tco2e": str(facility_tco2e),
                "allocation_basis": "production_share",
                "production_total": str(prod_total),
            },
            "products": product_rows,
            "precursors": precursor_breakdown,
            "exports": export_rows,
            "totals": {
                "export_embedded_tco2e": str(total_export_embedded),
                "cbam_certificates_tco2e": str(cbam_certificates),
                "ets_price_eur_per_tco2": str(ets_price_eur_per_tco2),
                "cbam_cost_eur": str(cbam_cost),
            },
            "meta": {
                "engine": "CBAM_Product_Engine",
                "deterministic": True,
            }
        }
        result_hash = sha256_text(canonical_json(result))
        result["meta"]["result_hash"] = result_hash
        return result
