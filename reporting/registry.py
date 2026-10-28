"""Prepare a flat run registry from verified local T3 manifests; never upload."""
from __future__ import annotations

import argparse
from pathlib import Path

from detection.exports import read_export
from pipeline.local import snapshot_bytes


def write_registry(exports, destination):
    rows = []
    seen = set()
    for path in exports:
        bundle = read_export(path)
        meta = bundle["metadata"]
        if meta["detection_run_id"] in seen:
            raise ValueError("duplicate run ID in registry input")
        seen.add(meta["detection_run_id"])
        rows.append(dict(meta, status="complete"))
    data = b"".join(snapshot_bytes(row) + b"\n" for row in sorted(rows, key=lambda row: row["detection_run_id"]))
    with Path(destination).open("xb") as handle:
        handle.write(data)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("exports", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    write_registry(args.exports, args.output)
