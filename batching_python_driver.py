"""Read-only, batched local Neo4j export of the complete Parquet star schema.

Imports are side-effect free. Runtime dependencies and environment credentials
are loaded only by main(); fixtures inject a reader and a Spark-compatible sink.
See docs/STAR_SCHEMA.md for types, output safety and verification boundaries.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from contracts.transaction_event import (
    GRAPH_SCORE_PROPERTY, PAYSIM_FIELD_ORDER, TRANSACTION_TYPE_KEYS,
    build_transaction_event,
)
from pipeline.transform import date_dimension, event_to_fact, money, spark_schema as shared_schema

OUTPUT_DIR = Path("Output_Parquet")
BATCH_SIZE = 25_000

# Pure descriptors can be tested without importing PySpark or starting a JVM.
ACCOUNT_FIELDS = (
    ("account_id", "string", False),
    ("community_id", "bigint", True),
    ("community_total_amount", "decimal(38,10)", True),
    ("community_transaction_count", "bigint", True),
    ("outDegree", "double", True),
    ("inDegree", "double", True),
    ("fan_out_flag", "bigint", True),
    ("fan_in_flag", "bigint", True),
    ("unusualFanIn", "bigint", True),
    ("drain_behavior_flag", "bigint", True),
    ("dense_community_flag", "bigint", True),
    ("transfer_cashout_flag", "bigint", True),
    ("in_cycle_flag", "bigint", True),
    ("guilt_by_association_flag", "bigint", True),
    (GRAPH_SCORE_PROPERTY, "bigint", True),
)
ACCOUNT_COLUMNS = tuple(name for name, _, _ in ACCOUNT_FIELDS)
ACCOUNTS_QUERY = """
MATCH (a:Account)
RETURN
""" + ",\n".join(f"  a.{('id' if name == 'account_id' else name)} AS {name}"
                  for name in ACCOUNT_COLUMNS) + "\nORDER BY a.id"

# Business-key keyset pagination. No Neo4j internal ID is exported or regenerated.
TRANSACTIONS_QUERY = """
MATCH (orig:Account)-[t:TRANSACTION]->(dest:Account)
WHERE t.txn_id > $last_txn_id
RETURN t.txn_id AS txn_id, t.source_file AS source_file,
       t.row_number AS row_number, orig.id AS nameOrig, dest.id AS nameDest,
       t.step AS step, t.type AS type, t.amount AS amount,
       t.oldbalanceOrg AS oldbalanceOrg, t.newbalanceOrig AS newbalanceOrig,
       t.oldbalanceDest AS oldbalanceDest, t.newbalanceDest AS newbalanceDest,
       t.transfer_cashout_flag AS transfer_cashout_flag
ORDER BY t.txn_id
LIMIT $batch_size
"""
COUNTS_QUERY = """
MATCH (a:Account)
WITH count(a) AS accounts, count(DISTINCT a.id) AS account_ids
OPTIONAL MATCH ()-[t:TRANSACTION]->()
RETURN accounts, account_ids, count(t) AS transactions,
       count(DISTINCT t.txn_id) AS txn_ids,
       count(CASE WHEN t IS NOT NULL AND t.txn_id IS NULL THEN 1 END) AS missing_txn_ids
"""


def graph_counts(driver):
    with driver.session(default_access_mode="READ") as session:
        record = session.run(COUNTS_QUERY).single()
        if record is None:
            raise ValueError("graph count query returned no record")
        counts = {name: int(record[name]) for name in
                  ("accounts", "account_ids", "transactions", "txn_ids", "missing_txn_ids")}
    if counts["missing_txn_ids"] or counts["transactions"] != counts["txn_ids"]:
        raise ValueError("every TRANSACTION must have a unique txn_id before export")
    if counts["accounts"] != counts["account_ids"]:
        raise ValueError("every Account must have a unique non-null id before export")
    return counts


def _integer(value, name, *, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _flag(value, name):
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if not isinstance(value, int) or value not in (0, 1):
        raise ValueError(f"{name} must be null, 0 or 1")
    return value


def normalize_account(record):
    account_id = record.get("account_id")
    if not isinstance(account_id, str) or not account_id.strip():
        raise ValueError("account_id must be a nonempty string")
    row = {"account_id": account_id}
    for name, kind, _ in ACCOUNT_FIELDS[1:]:
        value = record.get(name)
        if value is None:
            row[name] = None
        elif name.endswith("_flag") or name == "unusualFanIn":
            row[name] = _flag(value, name)
        elif kind == "decimal(38,10)":
            row[name] = money(value)
        elif kind == "double":
            import math
            number = float(value)
            if not math.isfinite(number) or number < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
            row[name] = number
        else:
            row[name] = _integer(value, name)
    return row


def fetch_accounts(driver):
    with driver.session(default_access_mode="READ") as session:
        rows = [normalize_account(dict(record)) for record in session.run(ACCOUNTS_QUERY)]
    if len({row["account_id"] for row in rows}) != len(rows):
        raise ValueError("duplicate account ID in export")
    return rows


def transaction_to_fact(record):
    txn_id = record.get("txn_id")
    if not isinstance(txn_id, str) or re.fullmatch("[a-f0-9]{64}", txn_id) is None:
        raise ValueError("TRANSACTION needs a contract txn_id")
    if not isinstance(record.get("source_file"), str) or not record["source_file"]:
        raise ValueError("TRANSACTION needs source_file audit metadata")
    row_number = _integer(record.get("row_number"), "row_number", minimum=1)
    event = build_transaction_event(
        {name: record.get(name) for name in PAYSIM_FIELD_ORDER},
        source_file=record["source_file"], row_number=row_number,
        ingested_at=datetime(1970, 1, 1, tzinfo=timezone.utc),  # Excluded from facts/identity.
    )
    if event["txn_id"] != txn_id:
        raise ValueError("stored txn_id disagrees with contract payload/audit coordinates")
    fact = event_to_fact(event)
    fact["transfer_cashout_flag"] = _flag(record.get("transfer_cashout_flag"), "transfer_cashout_flag")
    fact["date"] = date.fromisoformat(date_dimension([fact["step"]])[0]["date"])
    return fact


def transaction_batches(driver, batch_size=BATCH_SIZE):
    _integer(batch_size, "batch_size", minimum=1)
    last_txn_id = ""
    while True:
        with driver.session(default_access_mode="READ") as session:
            records = [dict(record) for record in session.run(
                TRANSACTIONS_QUERY, last_txn_id=last_txn_id, batch_size=batch_size)]
        if not records:
            return
        batch = []
        for record in records:
            row = transaction_to_fact(record)
            if row["txn_id"] <= last_txn_id:
                raise ValueError("transaction cursor did not advance in unique txn_id order")
            last_txn_id = row["txn_id"]
            batch.append(row)
        yield batch


def schema_for(table):
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DecimalType, DateType
    if table == "dim_account":
        types = {"string": StringType(), "bigint": LongType(),
                 "double": DoubleType(), "decimal(38,10)": DecimalType(38, 10)}
        return StructType([StructField(name, types[kind], nullable) for name, kind, nullable in ACCOUNT_FIELDS])
    if table == "fact_transactions":
        return StructType([*shared_schema(table).fields,
                           StructField("transfer_cashout_flag", LongType(), True),
                           StructField("date", DateType(), False)])
    return shared_schema(table)


def write_table(spark, table, rows, output, *, mode="errorifexists"):
    if table == "dim_date":
        rows = [dict(row, date=date.fromisoformat(row["date"])) for row in rows]
    writer = spark.createDataFrame(rows, schema=schema_for(table)).write.mode(mode).option("compression", "snappy")
    if table == "fact_transactions":
        writer = writer.partitionBy("date")
    writer.parquet(str(output / table))


def export_star_schema(driver, spark, output=OUTPUT_DIR, *, batch_size=BATCH_SIZE):
    """Write once to a NEW local directory; counts and completion marker guard use."""
    output = Path(output)
    if output.exists():
        raise FileExistsError("output already exists; choose a new export directory")
    expected = graph_counts(driver)
    accounts = fetch_accounts(driver)
    if len(accounts) != expected["accounts"]:
        raise ValueError("account count changed before export")
    account_ids = {row["account_id"] for row in accounts}
    output.mkdir(parents=True, exist_ok=False)
    incomplete = output / "_EXPORT_INCOMPLETE"
    incomplete.touch()
    write_table(spark, "dim_account", accounts, output)
    total, steps = 0, set()
    for batch in transaction_batches(driver, batch_size):
        if any(row[key] not in account_ids for row in batch for key in ("nameOrig", "nameDest")):
            raise ValueError("fact references an account missing from dim_account")
        write_table(spark, "fact_transactions", batch, output,
                    mode="errorifexists" if total == 0 else "append")
        total += len(batch)
        steps.update(row["step"] for row in batch)
    if total == 0:
        write_table(spark, "fact_transactions", [], output)
    types = [{"type_key": key, "type_name": name} for name, key in TRANSACTION_TYPE_KEYS.items()]
    dates = date_dimension(steps)
    write_table(spark, "dim_transaction_type", types, output)
    write_table(spark, "dim_date", dates, output)
    if total != expected["transactions"] or graph_counts(driver) != expected:
        raise ValueError("graph counts changed or transaction export is incomplete")
    counts = {"dim_account": len(accounts), "fact_transactions": total,
              "dim_transaction_type": len(types), "dim_date": len(dates)}
    for table, count in counts.items():
        if spark.read.parquet(str(output / table)).count() != count:
            raise ValueError(f"written row count disagrees for {table}")
    (output / "_EXPORT_COMPLETE.json").write_text(json.dumps(counts, sort_keys=True), encoding="utf-8")
    incomplete.unlink()
    return counts


def create_driver():
    from dotenv import load_dotenv
    from neo4j import GraphDatabase
    from database_creation_script import neo4j_connect_args
    load_dotenv()
    uri, user, password = neo4j_connect_args()
    if urlsplit(uri).hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("export requires a local loopback Neo4j endpoint")
    return GraphDatabase.driver(uri, auth=(user, password))


def create_spark():
    from pyspark.sql import SparkSession
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return SparkSession.builder.master("local[2]").appName("neo4j-star-schema").getOrCreate()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise FileExistsError("output already exists; choose a new export directory")
    _integer(args.batch_size, "batch_size", minimum=1)
    driver = spark = None
    try:
        driver = create_driver()
        spark = create_spark()
        counts = export_star_schema(driver, spark, args.output, batch_size=args.batch_size)
        print(json.dumps(counts, sort_keys=True))  # No connection details or credentials.
        return counts
    finally:
        try:
            if driver is not None:
                driver.close()
        finally:
            if spark is not None:
                spark.stop()


if __name__ == "__main__":
    main()
