from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


# ---------------------------------------------------------------------
# Deterministic canonical JSON + SHA256 hashing (audit-grade)
#
# Goals:
# - stable across Python versions / environments
# - stable floats (no 0.30000000004 drift)
# - sorted keys
# - utf-8
# - no whitespace variance
#
# NOTE:
# This module is the single source of truth for hashing across the app.
# ---------------------------------------------------------------------


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _quantize_decimal(d: Decimal, *, digits: int = 12) -> Decimal:
    """Quantize Decimal deterministically.

    - ROUND_HALF_UP (audit-friendly)
    - fixed scale = `digits`
    """
    q = Decimal(10) ** Decimal(-digits)
    return d.quantize(q, rounding=ROUND_HALF_UP)


def _normalize(obj: Any) -> Any:
    """Recursively normalize objects for deterministic JSON.

    Rules:
    - dict keys are coerced to str
    - lists/tuples are preserved in order (ordering determinism must be handled upstream)
    - floats are converted to a normalized Decimal string with fixed precision
    - Decimals are quantized and converted to string
    - bytes are hashed (sha256) to avoid binary variance
    - dataclasses/objects: best-effort __dict__ fallback
    """
    if obj is None:
        return None

    # Primitive stable types
    if isinstance(obj, (bool, int, str)):
        return obj

    # Bytes: do not embed raw binary
    if isinstance(obj, (bytes, bytearray)):
        return {"__bytes_sha256": sha256_bytes(bytes(obj))}

    # Decimal
    if isinstance(obj, Decimal):
        try:
            q = _quantize_decimal(obj)
            # string (not float) to avoid JSON float variance
            return format(q, "f")
        except Exception:
            return "0"

    # Float (including numpy floats if they behave like float)
    if isinstance(obj, float):
        try:
            if obj != obj:  # NaN
                return "NaN"
            if obj == float("inf"):
                return "Infinity"
            if obj == float("-inf"):
                return "-Infinity"
            d = Decimal(str(obj))
            q = _quantize_decimal(d)
            return format(q, "f")
        except (InvalidOperation, Exception):
            return "0"

    # Lists / tuples
    if isinstance(obj, (list, tuple)):
        return [_normalize(x) for x in obj]

    # Dict
    if isinstance(obj, dict):
        # Normalize keys to str for stability
        out = {}
        for k, v in obj.items():
            ks = str(k)
            out[ks] = _normalize(v)
        return out

    # Pandas / numpy scalars and other numeric-like
    try:
        # Convert numeric-like to Python primitives
        if hasattr(obj, "item") and callable(getattr(obj, "item")):
            return _normalize(obj.item())
    except Exception:
        pass

    # Dataclasses / objects
    try:
        if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
            return _normalize(obj.to_dict())
        if hasattr(obj, "__dict__"):
            return _normalize(dict(obj.__dict__))
    except Exception:
        pass

    # Fallback: string representation
    return str(obj)


def canonical_json(obj: Any) -> str:
    """Deterministic JSON encoding with stable floats."""
    normalized = _normalize(obj)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_json(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def build_lineage_graph(
    *,
    snapshot_id: int,
    project_id: int,
    input_hash: str,
    result_hash: str,
    datasets: list[dict] | None = None,
    evidence_docs: list[dict] | None = None,
    factor_refs: list[dict] | None = None,
    compliance: dict | None = None,
    reports: list[dict] | None = None,
) -> dict:
    """Deterministic lineage graph export (JSON).

    Graph:
      Evidence → Dataset → InputBundle → FactorSet → Calculation → Compliance → Report
    """
    datasets = datasets or []
    evidence_docs = evidence_docs or []
    factor_refs = factor_refs or []
    reports = reports or []
    compliance = compliance or {}

    nodes: list[dict] = []
    edges: list[dict] = []

    def node(nid: str, ntype: str, meta: dict):
        nodes.append({"id": nid, "type": ntype, "meta": meta})

    def edge(src: str, dst: str, rel: str):
        edges.append({"from": src, "to": dst, "rel": rel})

    for e in evidence_docs:
        eid = f"evidence:{e.get('id')}"
        node(eid, "Evidence", e)

    for d in datasets:
        did = f"dataset:{d.get('dataset_type')}:{d.get('sha256')}"
        node(did, "Dataset", d)
        for e in evidence_docs:
            edge(f"evidence:{e.get('id')}", did, "supports")

    ib_meta = {"project_id": int(project_id), "snapshot_id": int(snapshot_id), "input_hash": str(input_hash)}
    ib_id = f"inputbundle:{input_hash}"
    node(ib_id, "InputBundle", ib_meta)

    for d in datasets:
        did = f"dataset:{d.get('dataset_type')}:{d.get('sha256')}"
        edge(did, ib_id, "feeds")

    fs_hash = sha256_json(factor_refs)
    fs_id = f"factorset:{fs_hash}"
    node(fs_id, "FactorSet", {"factor_refs": factor_refs, "factor_set_hash": fs_hash})
    edge(ib_id, fs_id, "uses")

    calc_id = f"calculation:{result_hash}"
    node(calc_id, "Calculation", {"snapshot_id": int(snapshot_id), "result_hash": str(result_hash)})
    edge(fs_id, calc_id, "enables")
    edge(ib_id, calc_id, "computes")

    comp_hash = sha256_json(compliance or {})
    comp_id = f"compliance:{comp_hash}"
    node(comp_id, "Compliance", {"hash": comp_hash, "payload": compliance or {}})
    edge(calc_id, comp_id, "validated_by")

    for r in reports:
        rid = f"report:{r.get('report_type')}:{r.get('sha256', '')}"
        node(rid, "Report", r)
        edge(comp_id, rid, "produces")

    nodes.sort(key=lambda x: (x["type"], x["id"]))
    edges.sort(key=lambda x: (x["from"], x["to"], x["rel"]))

    graph = {"nodes": nodes, "edges": edges}
    graph["graph_hash"] = sha256_json(graph)
    return graph
