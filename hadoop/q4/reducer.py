#!/usr/bin/env python3
"""Q4 Reducer: Compute top countries by total degree centrality.

This reducer expects to run with a single reducer task so that it can compute a
final top ten ranking. Configure Hadoop streaming with
``-D mapreduce.job.reduces=1`` when launching this job.
"""

import json
import sys
from collections import defaultdict

def main() -> None:
    in_degree = defaultdict(int)
    out_degree = defaultdict(int)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            country, payload = line.split("\t", 1)
        except ValueError:
            continue

        if payload.startswith("OUT:"):
            try:
                out_degree[country] += int(payload.split(":", 1)[1])
            except ValueError:
                continue
        elif payload.startswith("IN:"):
            out_degree[country]  # touch key to keep order consistent
            try:
                in_degree[country] += int(payload.split(":", 1)[1])
            except ValueError:
                continue

    rankings = []
    seen = set(in_degree.keys()) | set(out_degree.keys())
    for country in seen:
        in_count = in_degree[country]
        out_count = out_degree[country]
        total = in_count + out_count
        rankings.append({
            "country": country,
            "in_degree": in_count,
            "out_degree": out_count,
            "total_degree": total,
        })

    rankings.sort(key=lambda item: item["total_degree"], reverse=True)

    for record in rankings[:10]:
        sys.stdout.write(json.dumps(record, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    main()
