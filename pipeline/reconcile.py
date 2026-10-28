"""Compare counts and exact amount sums by PaySim step and type."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from decimal import Decimal, localcontext
from pathlib import Path


def aggregates(rows):
    result = defaultdict(lambda: {"count": 0, "amount_sum": Decimal(0)})
    seen = set()
    with localcontext() as ctx:
        ctx.prec = 50
        for row in rows:
            if row["txn_id"] in seen:
                raise ValueError("duplicate txn_id in reconciliation input")
            seen.add(row["txn_id"])
            group = result[(int(row["step"]), str(row["type"]))]
            amount = Decimal(str(row["amount"]))
            if not amount.is_finite() or amount < 0:
                raise ValueError("invalid reconciliation amount")
            group["count"] += 1
            group["amount_sum"] += amount
    return dict(result)


def compare(expected, actual):
    left, right = aggregates(expected), aggregates(actual)
    zero = {"count": 0, "amount_sum": Decimal(0)}
    return [dict(step=key[0], type=key[1], expected=left.get(key, zero), actual=right.get(key, zero))
            for key in sorted(left.keys() | right.keys()) if left.get(key, zero) != right.get(key, zero)]


def read_facts(path: Path):
    if path.suffix == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    if path.suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))["fact_transactions"]
    # Supports the future T0.5 Parquet baseline without touching Neo4j.
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[2]").appName("paysim-reconcile").getOrCreate()
    try:
        return [row.asDict() for row in spark.read.parquet(str(path)).select("txn_id", "step", "type", "amount").toLocalIterator()]
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("expected", type=Path)
    parser.add_argument("actual", type=Path)
    args = parser.parse_args()
    differences = compare(read_facts(args.expected), read_facts(args.actual))
    print(json.dumps({"matches": not differences, "differences": differences}, default=str, sort_keys=True))
    raise SystemExit(1 if differences else 0)


if __name__ == "__main__":
    main()
