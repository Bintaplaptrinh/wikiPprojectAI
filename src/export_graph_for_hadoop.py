"""Convert the wiki influence graph into JSON Lines for Hadoop streaming.

Each line in the output file contains a single JSON object with the structure
``{"source": "Country", "targets": ["Neighbor", ...]}``. The Hadoop Q4 job
expects this format in order to compute in/out degree counts efficiently.

Usage
-----
Run the script after generating or updating ``graph.json``:

```
python -m src.export_graph_for_hadoop
```

Environment variables
~~~~~~~~~~~~~~~~~~~~~~
``GRAPH_SOURCE_PATH``
    Optional path to the source graph JSON (defaults to ``graph.json``).
``GRAPH_EXPORT_PATH``
    Optional path for the JSONL output (defaults to ``data/graph_edges.jsonl``).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GRAPH_PATH = Path(__file__).resolve().with_name("graph.json")
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "graph_edges.jsonl"

GRAPH_SOURCE = Path(
    os.getenv("GRAPH_SOURCE_PATH", str(DEFAULT_GRAPH_PATH))
)
OUTPUT_PATH = Path(
    os.getenv("GRAPH_EXPORT_PATH", str(DEFAULT_OUTPUT_PATH))
)


def _clean_targets(raw_targets: Iterable[object]) -> List[str]:
    """Return non-empty target names as a list of strings."""

    cleaned: List[str] = []
    for item in raw_targets:
        if isinstance(item, str):
            candidate = item.strip()
            if candidate:
                cleaned.append(candidate)
    return cleaned


def _load_graph(path: Path) -> Dict[str, List[str]]:
    """Load the adjacency dictionary, ensuring keys and values are strings."""

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError("Graph JSON must be an object mapping source -> targets")

    graph: Dict[str, List[str]] = {}
    for source, raw_targets in payload.items():
        if not isinstance(source, str):
            continue
        name = source.strip()
        if not name:
            continue

        if isinstance(raw_targets, dict):
            raw_iterable: Iterable[object] = raw_targets.values()
        elif isinstance(raw_targets, Iterable):
            raw_iterable = raw_targets
        else:
            raw_iterable = []

        graph[name] = _clean_targets(raw_iterable)

    return graph


def export_edges(
    graph_source: Path = GRAPH_SOURCE, output_path: Path = OUTPUT_PATH
) -> None:
    """Write graph edges as JSON Lines to ``output_path`` for Hadoop streaming."""

    graph_path = Path(graph_source)
    if not graph_path.exists():
        raise FileNotFoundError(f"Graph file not found: {graph_path}")

    adjacency = _load_graph(graph_path)

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as handle:
        for source, targets in adjacency.items():
            payload = {"source": source, "targets": targets}
            handle.write(json.dumps(payload, ensure_ascii=False))
            handle.write("\n")

    print(f"Exported {len(adjacency)} records to {out_path.resolve()}")


if __name__ == "__main__":
    export_edges()
