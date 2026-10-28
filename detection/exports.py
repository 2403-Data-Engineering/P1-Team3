"""Local, immutable graph-signal snapshots and explicit-run curated joins.

Consumes a completed T0.5 graph export; it never runs detection or connects to a
database/cloud. All IDs/timestamps describe detection completion, not export time.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from batching_python_driver import ACCOUNT_COLUMNS, normalize_account, transaction_to_fact
from pipeline.local import snapshot_bytes
from pipeline.transform import money

SIGNAL_FIELDS = tuple(name for name in ACCOUNT_COLUMNS if name != "account_id")
TXN_FIELDS = ("transfer_cashout_flag",)
SIGNAL_TABLES = ("accounts", "transactions", "rings")
RUN_FIELDS = ("detection_run_id", "detected_at", "window_start_step", "window_end_step", "window_kind")


def run_metadata(run_id, detected_at, start_step, end_step):
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", run_id):
        raise ValueError("run ID must contain 1–80 safe letters, digits, underscores or hyphens")
    stamp = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("detected_at must include a timezone")
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in (start_step, end_step)) or end_step < start_step:
        raise ValueError("window must be inclusive positive steps with start <= end")
    return dict(detection_run_id=run_id,
                detected_at=stamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                window_start_step=start_step, window_end_step=end_step,
                window_kind="full_graph_snapshot")


def _unique(rows, key):
    result = {}
    for row in rows:
        value = row.get(key)
        if value is None or value == "" or value in result:
            raise ValueError(f"missing or duplicate {key}")
        result[value] = row
    return result


def build_export(tables, *, detection_run_id, detected_at, window_start_step, window_end_step):
    """Export precomputed signals for one complete, quiescent detection snapshot.

    No filtering of an already scored graph is allowed: all supplied facts must
    be within the declared detection scope. The caller attests that detection
    ran on exactly this snapshot; this function cannot reconstruct that history.
    """
    metadata = run_metadata(detection_run_id, detected_at, window_start_step, window_end_step)
    accounts = _unique([normalize_account(row) for row in tables["dim_account"]], "account_id")
    facts = _unique([transaction_to_fact(row) for row in tables["fact_transactions"]], "txn_id")
    if any(row["risk_score"] is None for row in accounts.values()):
        raise ValueError("completed detection must supply risk_score for every account")
    for row in facts.values():
        if not window_start_step <= row["step"] <= window_end_step:
            raise ValueError("fact lies outside the declared detection window")
        if row["nameOrig"] not in accounts or row["nameDest"] not in accounts:
            raise ValueError("graph export fact references an unknown account")
    account_rows = [dict(row, **metadata) for _, row in sorted(accounts.items())]
    txn_rows = [dict(txn_id=key, **{field: row[field] for field in TXN_FIELDS}, **metadata)
                for key, row in sorted(facts.items())]
    communities = {}
    for account in accounts.values():
        community = account["community_id"]
        if community is not None:
            communities.setdefault(community, []).append(account)
    from decimal import Decimal, localcontext
    totals = {community: [0, Decimal(0), Decimal(0)] for community in communities}
    with localcontext() as context:
        context.prec = 50
        for row in facts.values():
            community = accounts[row["nameOrig"]]["community_id"]
            if community is not None and community == accounts[row["nameDest"]]["community_id"]:
                total = totals[community]
                total[0] += 1
                total[1] += row["amount"]
                if row["transfer_cashout_flag"] == 1:
                    total[2] += row["amount"]
    ring_rows = []
    for community, members in sorted(communities.items()):
        ids = {row["account_id"] for row in members}
        count, amount, flagged_amount = totals[community]
        ring_rows.append(dict(ring_id=str(community), community_id=community,
                              account_ids=sorted(ids), account_count=len(ids),
                              transaction_count=count, internal_amount=amount,
                              transfer_cashout_amount=flagged_amount,
                              dense_community_flag=1 if any(row["dense_community_flag"] == 1 for row in members) else
                              0 if all(row["dense_community_flag"] == 0 for row in members) else None,
                              **metadata))
    return dict(metadata=metadata, accounts=account_rows, transactions=txn_rows, rings=ring_rows)


def validate_export(bundle):
    meta = bundle["metadata"]
    canonical = run_metadata(meta["detection_run_id"], meta["detected_at"], meta["window_start_step"], meta["window_end_step"])
    if meta != canonical:
        raise ValueError("noncanonical detection run metadata")
    for table, key in (("accounts", "account_id"), ("transactions", "txn_id"), ("rings", "ring_id")):
        _unique(bundle[table], key)
        for row in bundle[table]:
            if any(row.get(field) != meta[field] for field in RUN_FIELDS):
                raise ValueError("mixed detection run or window in signal rows")
    return meta


def write_export(bundle, root):
    """Write S3-ready JSONL locally; exclusive per-run directory, no uploader."""
    meta = validate_export(bundle)
    contents = {table: b"".join(snapshot_bytes(row) + b"\n" for row in bundle[table]) for table in SIGNAL_TABLES}
    destination = Path(root) / "graph_signals" / f"detection_run_id={meta['detection_run_id']}"
    destination.mkdir(parents=True, exist_ok=False)
    for table, data in contents.items():
        folder = destination / table
        folder.mkdir()
        (folder / "part-00000.jsonl").write_bytes(data)
    manifest = dict(schema_version="graph-signals-v1", metadata=meta,
                    files={table: dict(path=f"{table}/part-00000.jsonl", rows=len(bundle[table]),
                                       sha256=hashlib.sha256(contents[table]).hexdigest()) for table in SIGNAL_TABLES})
    # The manifest is the commit marker; consumers reject partial exports.
    (destination / "manifest.json").write_bytes(snapshot_bytes(manifest))
    return destination


def read_export(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_bytes())
    if manifest["schema_version"] != "graph-signals-v1":
        raise ValueError("unsupported graph export schema")
    bundle = {"metadata": manifest["metadata"]}
    for table in SIGNAL_TABLES:
        entry = manifest["files"][table]
        if entry["path"] != f"{table}/part-00000.jsonl":
            raise ValueError("invalid graph export file path")
        data = (directory / entry["path"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise ValueError("graph export checksum mismatch")
        rows = [json.loads(line) for line in data.splitlines() if line.strip()]
        if len(rows) != entry["rows"]:
            raise ValueError("graph export row count mismatch")
        for row in rows:
            if table == "accounts":
                row["community_total_amount"] = money(row.get("community_total_amount"))
            if table == "rings":
                # Aggregates can exceed a single transaction's decimal(38,10) range.
                from decimal import Decimal
                for field in ("internal_amount", "transfer_cashout_amount"):
                    row[field] = Decimal(row[field])
        bundle[table] = rows
    validate_export(bundle)
    return bundle


def join_curated(curated, exports, *, detection_run_id):
    """Left joins for ONE explicitly selected run; never a many-to-many history join."""
    by_run = {}
    for bundle in exports:
        meta = validate_export(bundle)
        key = meta["detection_run_id"]
        if key in by_run:
            raise ValueError("duplicate detection run bundle")
        by_run[key] = bundle
    if detection_run_id not in by_run:
        raise ValueError("selected detection run is unavailable")
    bundle = by_run[detection_run_id]
    accounts = _unique(bundle["accounts"], "account_id")
    transactions = _unique(bundle["transactions"], "txn_id")
    _unique(curated["dim_account"], "account_id")
    _unique(curated["fact_transactions"], "txn_id")
    result = copy.deepcopy(curated)
    for table, key, signals, fields in (
        ("dim_account", "account_id", accounts, SIGNAL_FIELDS),
        ("fact_transactions", "txn_id", transactions, TXN_FIELDS),
    ):
        for row in result[table]:
            match = signals.get(row[key])
            for field in (*fields, *RUN_FIELDS):
                row[field] = match[field] if match else None
            row["graph_signal_present"] = match is not None
    result["graph_rings"] = copy.deepcopy(bundle["rings"])
    result["graph_run"] = copy.deepcopy(bundle["metadata"])
    return result


def tables_from_parquet(spark, directory):
    """Read a completed LOCAL T0.5 export using an injected Spark reader."""
    directory = Path(directory)
    if (directory / "_EXPORT_INCOMPLETE").exists():
        raise ValueError("T0.5 export is incomplete")
    expected = json.loads((directory / "_EXPORT_COMPLETE.json").read_bytes())
    tables = {}
    for name in ("dim_account", "fact_transactions"):
        tables[name] = [row.asDict() for row in spark.read.parquet(str(directory / name)).toLocalIterator()]
        if len(tables[name]) != expected[name]:
            raise ValueError("T0.5 export row count mismatch")
    return tables


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("export", help="Export a local T0.5-style tables JSON snapshot")
    create.add_argument("source", type=Path)
    create.add_argument("--parquet", action="store_true", help="Read a completed local T0.5 Parquet directory; requires Spark")
    create.add_argument("--output", required=True, type=Path)
    create.add_argument("--detection-run-id", required=True)
    create.add_argument("--detected-at", required=True)
    create.add_argument("--window-start-step", required=True, type=int)
    create.add_argument("--window-end-step", required=True, type=int)
    join = commands.add_parser("join", help="Enrich a local curated tables JSON snapshot")
    join.add_argument("curated", type=Path)
    join.add_argument("exports", nargs="+", type=Path)
    join.add_argument("--detection-run-id", required=True)
    join.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "export":
        spark = None
        try:
            if args.parquet:
                from batching_python_driver import create_spark
                spark = create_spark()
                tables = tables_from_parquet(spark, args.source)
            else:
                tables = json.loads(args.source.read_bytes())
        finally:
            if spark is not None:
                spark.stop()
        bundle = build_export(tables, detection_run_id=args.detection_run_id,
                              detected_at=args.detected_at, window_start_step=args.window_start_step,
                              window_end_step=args.window_end_step)
        write_export(bundle, args.output)
    else:
        result = join_curated(json.loads(args.curated.read_bytes()), [read_export(path) for path in args.exports],
                              detection_run_id=args.detection_run_id)
        data = snapshot_bytes(result)
        with args.output.open("xb") as handle:
            handle.write(data)


if __name__ == "__main__":
    main()
