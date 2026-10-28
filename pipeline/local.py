"""Local JSON snapshots for fixtures, with optional Parquet using installed Spark."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from decimal import Decimal
from pathlib import Path

from pipeline.transform import transform_events, write_parquet


def read_events(paths):
    for path in paths:
        with Path(path).open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def snapshot_bytes(tables):
    def encode(value):
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError("unsupported JSON type")
    return json.dumps(tables, default=encode,
                      sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def write_snapshot(tables, output: Path):
    output.mkdir(parents=True, exist_ok=True)
    data = snapshot_bytes(tables)
    digest = hashlib.sha256(data).hexdigest()
    # One atomic file is the local authoritative snapshot; never append facts.
    with tempfile.NamedTemporaryFile(dir=output, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(data)
    try:
        os.replace(temporary, output / "tables.json")
    finally:
        temporary.unlink(missing_ok=True)
    return digest


def run_local(inputs, output: Path, *, spark=None):
    tables = transform_events(read_events(inputs))
    write_snapshot(tables, output)
    if spark is not None:
        write_parquet(spark, tables, str(output / "parquet"))
    return tables


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--parquet", action="store_true", help="Requires local PySpark and a compatible JVM")
    args = parser.parse_args()
    spark = None
    try:
        if args.parquet:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.master("local[2]").appName("paysim-local").getOrCreate()
        tables = run_local(args.inputs, args.output, spark=spark)
        print("fact_count=", len(tables["fact_transactions"]))
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
