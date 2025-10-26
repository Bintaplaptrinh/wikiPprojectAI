#!/usr/bin/env python3
"""Q4 Mapper: Degree contributions per country.

Input: JSON Lines with fields ``source`` and ``targets``.
Output:
    country\tOUT:<count>
    country\tIN:1
The reducer aggregates these values to compute total degree centrality.
"""

import json
import sys

def emit(key: str, value: str) -> None:
    sys.stdout.write(f"{key}\t{value}\n")

def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        source = record.get("source")
        targets = record.get("targets", []) or []

        if not source:
            continue

        emit(source, f"OUT:{len(targets)}")

        for target in targets:
            if target:
                emit(target, "IN:1")

if __name__ == "__main__":
    main()
