
import json

def build_lineage(snapshot_id, datasets, reports):
    graph = {
        "snapshot_id": snapshot_id,
        "flow": []
    }

    for d in datasets:
        graph["flow"].append({
            "type": "dataset",
            "name": d
        })

    graph["flow"].append({"type":"calculation"})

    for r in reports:
        graph["flow"].append({
            "type": "report",
            "name": r
        })

    return graph

def save_lineage(graph, path):
    with open(path,"w",encoding="utf-8") as f:
        json.dump(graph,f,indent=2)

