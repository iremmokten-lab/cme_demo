from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from src.mrv.lineage import sha256_json


def _norm(s: Any) -> str:
    return str(s or "").strip().lower().replace(" ", "_")


def _to_float(x: Any) -> float:
    try:
        if pd.isna(x):
            return 0.0
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return 0.0


@dataclass(frozen=True)
class PrecursorEdge:
    """
    Precursor relation: parent product (sku) uses precursor (precursor_sku) in given quantity.
    Emissions of precursor can be:
      - provided directly (precursor_embedded_tco2)
      - or derived from precursor embedded intensity and precursor quantity
      - or derived from emission_factor and quantity
    """
    sku: str
    precursor_sku: str
    precursor_quantity: float
    precursor_quantity_unit: str
    precursor_embedded_tco2: float
    source_row_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sku": self.sku,
            "precursor_sku": self.precursor_sku,
            "precursor_quantity": self.precursor_quantity,
            "precursor_quantity_unit": self.precursor_quantity_unit,
            "precursor_embedded_tco2": self.precursor_embedded_tco2,
            "source_row_hash": self.source_row_hash,
        }


def parse_precursor_edges(materials_df: Optional[pd.DataFrame]) -> List[PrecursorEdge]:
    """
    Parse materials_df into precursor edges if it contains precursor fields.
    Expected flexible columns:
      - sku (product)
      - precursor_sku (or input_sku, material_sku)
      - precursor_quantity (or quantity, material_quantity)
      - precursor_quantity_unit (or unit)
      - precursor_embedded_tco2 (optional)
      - precursor_embedded_intensity_tco2_per_unit (optional)
      - emission_factor_kgco2e_per_unit (optional, multiplied by quantity and /1000)
      - month/facility_id ignored in aggregation
    """
    if materials_df is None or len(materials_df) == 0:
        return []

    df = materials_df.copy()
    df.columns = [_norm(c) for c in df.columns]
    cols = set(df.columns)

    def pick(*names: str) -> str:
        for n in names:
            if n in cols:
                return n
        return ""

    sku_c = pick("sku", "product_sku", "product_code")
    prec_c = pick("precursor_sku", "input_sku", "material_sku", "precursor", "material")
    qty_c = pick("precursor_quantity", "quantity", "material_quantity", "qty")
    unit_c = pick("precursor_quantity_unit", "unit", "uom")
    emb_c = pick("precursor_embedded_tco2", "embedded_tco2", "precursor_tco2")
    emb_i_c = pick("precursor_embedded_intensity_tco2_per_unit", "embedded_intensity_tco2_per_unit", "embedded_intensity")
    ef_c = pick("emission_factor_kgco2e_per_unit", "emission_factor", "ef_kgco2e_per_unit")

    if not sku_c or not prec_c or not qty_c:
        # not a precursor sheet
        return []

    if not unit_c:
        df["__unit"] = "t"
        unit_c = "__unit"

    edges: List[PrecursorEdge] = []
    for _, r in df.iterrows():
        sku = str(r.get(sku_c) or "").strip()
        psku = str(r.get(prec_c) or "").strip()
        if not sku or not psku:
            continue
        qty = _to_float(r.get(qty_c))
        unit = str(r.get(unit_c) or "t").strip()

        embedded_tco2 = 0.0
        if emb_c:
            embedded_tco2 = _to_float(r.get(emb_c))
        elif emb_i_c:
            embedded_tco2 = _to_float(r.get(emb_i_c)) * max(0.0, qty)
        elif ef_c:
            embedded_tco2 = (_to_float(r.get(ef_c)) * max(0.0, qty)) / 1000.0

        row_hash = sha256_json(
            {
                "sku": sku,
                "precursor_sku": psku,
                "precursor_quantity": float(qty),
                "precursor_quantity_unit": unit,
                "precursor_embedded_tco2": float(embedded_tco2),
            }
        )

        edges.append(
            PrecursorEdge(
                sku=sku,
                precursor_sku=psku,
                precursor_quantity=float(qty),
                precursor_quantity_unit=unit,
                precursor_embedded_tco2=float(embedded_tco2),
                source_row_hash=row_hash,
            )
        )

    # deterministic ordering
    edges.sort(key=lambda e: (e.sku, e.precursor_sku, e.source_row_hash))
    return edges


def compute_precursor_tco2_by_sku(
    *,
    production_df: pd.DataFrame,
    materials_df: Optional[pd.DataFrame],
    embedded_tco2_by_sku: Optional[Dict[str, float]] = None,
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    Compute precursor emissions per sku.

    Two modes:
      1) Explicit mode: materials rows provide precursor_embedded_tco2 directly (no chain needed).
      2) Chain mode: if embedded_tco2_by_sku provided and precursor_embedded_tco2 is 0,
         then use embedded_tco2_by_sku[precursor_sku] scaled by precursor_quantity / produced_quantity_of_precursor
         (requires production_df quantity for precursor_sku). This is a simplified deterministic chain approach.

    Cycle detection is applied in chain mode using SKU graph.

    Returns:
      precursor_map: sku -> precursor_tco2
      meta: edges + cycle info + method
    """
    edges = parse_precursor_edges(materials_df)
    if not edges:
        return {}, {"precursor_method": "none", "edges": []}

    # quantities for precursor SKUs
    prod = production_df.copy()
    prod.columns = [_norm(c) for c in prod.columns]
    if "sku" not in prod.columns:
        # tolerate product_code
        if "product_code" in prod.columns:
            prod["sku"] = prod["product_code"].astype(str)
        else:
            prod["sku"] = ""
    if "quantity" not in prod.columns:
        prod["quantity"] = 0.0
    prod["sku"] = prod["sku"].astype(str).fillna("").apply(lambda x: str(x).strip())
    prod["quantity"] = prod["quantity"].apply(_to_float)

    qty_by_sku = prod.groupby("sku", dropna=False)["quantity"].sum().to_dict()

    # Build graph
    graph: Dict[str, List[str]] = {}
    for e in edges:
        graph.setdefault(e.sku, []).append(e.precursor_sku)

    def topo_sort(nodes: List[str]) -> Tuple[List[str], Optional[List[str]]]:
        # deterministic Kahn
        indeg: Dict[str, int] = {n: 0 for n in nodes}
        for a, outs in graph.items():
            for b in outs:
                if b in indeg:
                    indeg[b] += 1
        q = sorted([n for n, d in indeg.items() if d == 0])
        out: List[str] = []
        while q:
            n = q.pop(0)
            out.append(n)
            for b in sorted(graph.get(n, [])):
                if b in indeg:
                    indeg[b] -= 1
                    if indeg[b] == 0:
                        q.append(b)
                        q.sort()
        if len(out) != len(nodes):
            # cycle: return remaining
            cyc = sorted([n for n in nodes if n not in out])
            return out, cyc
        return out, None

    nodes = sorted(set([e.sku for e in edges] + [e.precursor_sku for e in edges]))
    order, cycle_nodes = topo_sort(nodes)

    precursor_map: Dict[str, float] = {k: 0.0 for k in nodes}

    chain_mode = embedded_tco2_by_sku is not None

    # If explicit precursor_embedded_tco2 is present, we always add it.
    for e in edges:
        precursor_map[e.sku] = precursor_map.get(e.sku, 0.0) + float(e.precursor_embedded_tco2 or 0.0)

    if chain_mode:
        # compute additional from precursor sku emissions when row doesn't specify embedded
        # We only add for edges where embedded was 0.0.
        for sku in order:
            # for each edge sku<-precursor
            for e in [x for x in edges if x.sku == sku]:
                if float(e.precursor_embedded_tco2 or 0.0) > 0.0:
                    continue
                psku = e.precursor_sku
                p_emb = float((embedded_tco2_by_sku or {}).get(psku, 0.0) or 0.0)
                p_qty = float(qty_by_sku.get(psku, 0.0) or 0.0)
                if p_qty <= 0.0:
                    continue
                share = max(0.0, float(e.precursor_quantity or 0.0)) / p_qty
                precursor_map[sku] = precursor_map.get(sku, 0.0) + p_emb * share

    meta = {
        "precursor_method": "explicit+chain" if chain_mode else "explicit",
        "edges": [e.to_dict() for e in edges],
        "cycle_nodes": cycle_nodes or [],
    }
    return precursor_map, meta
