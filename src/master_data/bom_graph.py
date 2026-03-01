from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Set, Tuple


@dataclass(frozen=True)
class Edge:
    parent_id: int
    child_id: int


class BOMGraph:
    """Deterministik BOM graph yardımcı sınıfı.

    - cycle detection
    - deterministic traversal (sorted)
    """

    def __init__(self, edges: Iterable[Edge]):
        g: Dict[int, List[int]] = defaultdict(list)
        for e in edges:
            g[e.parent_id].append(e.child_id)

        # deterministik: çocuk listelerini sırala
        self.graph: Dict[int, List[int]] = {k: sorted(v) for k, v in g.items()}

    def has_cycle(self) -> bool:
        visited: Set[int] = set()
        stack: Set[int] = set()

        def visit(n: int) -> bool:
            if n in stack:
                return True
            if n in visited:
                return False
            visited.add(n)
            stack.add(n)
            for c in self.graph.get(n, []):
                if visit(c):
                    return True
            stack.remove(n)
            return False

        for node in sorted(self.graph.keys()):
            if visit(node):
                return True
        return False

    def topo_paths(self, root: int, depth_limit: int = 25) -> List[List[int]]:
        """Root'tan başlayan tüm yolları deterministik döndürür."""
        paths: List[List[int]] = []

        def dfs(node: int, path: List[int], depth: int) -> None:
            if depth > depth_limit:
                return
            children = self.graph.get(node, [])
            if not children:
                paths.append(path[:])
                return
            for c in children:
                path.append(c)
                dfs(c, path, depth + 1)
                path.pop()

        dfs(root, [root], 0)
        return paths
