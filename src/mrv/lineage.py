from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def canonical_json(obj: Any) -> str:
    """
    Deterministic JSON encoding:
      - sort_keys=True
      - separators without spaces
      - ensure_ascii=False
    """
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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
    """
    Evidence → Dataset → InputBundle → FactorSet → Calculation → Compliance → Report

    Bu fonksiyon export/evidence pack içine koymak için deterministik lineage.json üretir.
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

    # Evidence nodes
    for e in evidence_docs:
        eid = f"evidence:{e.get('id')}"
        node(eid, "Evidence", e)

    # Dataset nodes
    for d in datasets:
        did = f"dataset:{d.get('dataset_type')}:{d.get('sha256')}"
        node(did, "Dataset", d)
        # Evidence → Dataset relation if evidence references exist
        # (best-effort)
        for e in evidence_docs:
            edge(f"evidence:{e.get('id')}", did, "supports")

    # InputBundle node
    ib_meta = {
        "project_id": int(project_id),
        "snapshot_id": int(snapshot_id),
        "input_hash": str(input_hash),
    }
    ib_id = f"inputbundle:{input_hash}"
    node(ib_id, "InputBundle", ib_meta)

    for d in datasets:
        did = f"dataset:{d.get('dataset_type')}:{d.get('sha256')}"
        edge(did, ib_id, "feeds")

    # FactorSet node (as a locked factor reference list)
    fs_id = f"factorset:{sha256_json(factor_refs)}"
    node(fs_id, "FactorSet", {"factor_refs": factor_refs, "factor_set_hash": sha256_json(factor_refs)})
    edge(ib_id, fs_id, "uses")

    # Calculation node
    calc_id = f"calculation:{result_hash}"
    node(
        calc_id,
        "Calculation",
        {"snapshot_id": int(snapshot_id), "result_hash": str(result_hash)},
    )
    edge(fs_id, calc_id, "enables")
    edge(ib_id, calc_id, "computes")

    # Compliance node
    comp_hash = sha256_json(compliance or {})
    comp_id = f"compliance:{comp_hash}"
    node(comp_id, "Compliance", {"hash": comp_hash, "payload": compliance or {}})
    edge(calc_id, comp_id, "validated_by")

    # Report nodes
    for r in reports:
        rid = f"report:{r.get('report_type')}:{r.get('sha256', '')}"
        node(rid, "Report", r)
        edge(comp_id, rid, "produces")

    # Deterministic sort
    nodes.sort(key=lambda x: (x["type"], x["id"]))
    edges.sort(key=lambda x: (x["from"], x["to"], x["rel"]))

    graph = {"nodes": nodes, "edges": edges}
    graph["graph_hash"] = sha256_json(graph)
    return graph
