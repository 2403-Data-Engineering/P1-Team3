"""
Extract data from Neo4j and write as partitioned Parquet star schema.

SCALABLE VERSION — for full PaySim or anything large.

Key difference from the demo extract_to_parquet.py:
  - Reads transactions in batches using KEYSET PAGINATION (not SKIP/LIMIT)
  - Writes each batch to Parquet incrementally (append mode)
  - Memory stays bounded at ~one batch worth of data, regardless of total size

Why not SKIP/LIMIT?
  Cypher executes `SKIP n` by scanning n rows and discarding them. As the
  offset grows, each batch gets slower. For millions of rows, the last
  batches take forever. Keyset pagination tracks the last ID seen and
  asks for "the next batch where id > last_id" — constant cost per batch.

Why elementId() and not id()?
  id() is deprecated in Neo4j 5+. elementId() is the current API. Returns
  a string instead of an integer; ordering is lexicographic but stable
  and unique, which is all we need for cursor-based pagination.

What students need to customize for their project:
  - Add their derived node properties (fraud_score, fan_in, etc.) to the
    accounts query RETURN clause and to ACCOUNT_SCHEMA
  - Add their derived edge properties to the transactions query RETURN
    clause and to TRANSACTION_BATCH_SCHEMA
  - That's it. Everything else stays the same.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Spark needs to know which Python to use on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-memory 4g "
    "--conf spark.sql.shuffle.partitions=50 "
    "pyspark-shell"
)

from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, DateType,
)

# -------------------------------------------------------------
# Config
# -------------------------------------------------------------
NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "12345678"

# Reference date that step=1 corresponds to (PaySim step is hours offset)
START_DATE = datetime(2026, 3, 1)

OUTPUT_DIR = Path("./Output_Parquet")

# Batch size for transaction reads. Tune based on memory:
#   - Larger = fewer round trips to Neo4j, more memory per batch
#   - Smaller = more round trips, less memory pressure
# 10k-50k is reasonable for typical hardware.
BATCH_SIZE = 25000


# -------------------------------------------------------------
# Schemas — keep these in sync with the RETURN clauses below.
# Define explicitly because Spark can't infer types when we append
# DataFrames batch by batch (each batch must match the same schema).
# -------------------------------------------------------------
ACCOUNT_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    # ADD STUDENTS' DERIVED NODE PROPERTIES HERE, e.g.:
    
    StructField("community_id", IntegerType(), True), # whether a field can be null
    # StructField("inDegree", IntegerType(), True),
    StructField("community_total_amount", DoubleType(), True),
    StructField("community_transaction_count", IntegerType(), True),
    StructField("drain_behavior_flag", IntegerType(), True),
    StructField("dense_community_flag", IntegerType(), True),
    StructField("unusualFanIn", IntegerType(), True),
    StructField("transfer_cashout_flag", IntegerType(), True), # inlcudes the recipient, not sender
    StructField("guilt_by_association_flag", IntegerType(), True),

    StructField("fraud_score", IntegerType(), True),
    # StructField("fan_in",      IntegerType(), True),

])

# Transactions come back from Neo4j with a string cursor (elementId)
# and the raw graph properties. We include the cursor in the schema
# during the read, then drop it before writing Parquet.
TRANSACTION_BATCH_SCHEMA = StructType([
    StructField("cursor",         StringType(), False),
    StructField("nameOrig",       StringType(), False),
    StructField("nameDest",       StringType(), False),
    StructField("step",           IntegerType(), False),
    StructField("type",           StringType(), False),
    StructField("amount",         DoubleType(),  False),
    StructField("oldbalanceOrg",  DoubleType(),  False),
    StructField("newbalanceOrig", DoubleType(),  False),
    StructField("oldbalanceDest", DoubleType(),  False),
    StructField("newbalanceDest", DoubleType(),  False),
    # ADD STUDENTS' DERIVED EDGE PROPERTIES HERE
])


# -------------------------------------------------------------
# Cypher queries
# -------------------------------------------------------------

# Accounts are typically a manageable size even at full scale (millions,
# not billions). We pull them in one query. If account count is huge,
# this could also be paginated with the same keyset pattern.
ACCOUNTS_QUERY = """
MATCH (a:Account)
RETURN
  a.id AS account_id,
  // ADD DERIVED PROPERTIES HERE, e.g.:
  a.community_id AS community_id,
  a.community_total_amount AS community_total_amount,
  a.community_transaction_count AS community_transaction_count,
  a.drain_behavior_flag AS drain_behavior_flag,
  a.dense_community_flag AS dense_community_flag,
  a.unusualFanIn AS unusualFanIn,
  a.transfer_cashout_flag AS transfer_cashout_flag,
  a.guilt_by_association_flag AS guilt_by_association_flag,
  a.fraud_score AS fraud_score
ORDER BY a.id
"""

# Keyset pagination: each call returns the next BATCH_SIZE transactions
# with elementId greater than the last cursor we saw.
# First call passes an empty string as the cursor (everything sorts after "").
TRANSACTIONS_QUERY = """
MATCH (orig:Account)-[t:TRANSACTION]->(dest:Account)
WHERE elementId(t) > $last_cursor
RETURN
  elementId(t)     AS cursor,
  orig.id          AS nameOrig,
  dest.id          AS nameDest,
  t.step           AS step,
  t.type           AS type,
  t.amount         AS amount,
  t.oldbalanceOrg  AS oldbalanceOrg,
  t.newbalanceOrig AS newbalanceOrig,
  t.oldbalanceDest AS oldbalanceDest,
  t.newbalanceDest AS newbalanceDest
  // ADD DERIVED EDGE PROPERTIES HERE
ORDER BY elementId(t)
LIMIT $batch_size
"""


# -------------------------------------------------------------
# Step 1: Pull accounts (single query, build dim_account)
# -------------------------------------------------------------
def fetch_accounts(driver):
    print("Fetching accounts...")
    with driver.session() as session:
        result = session.run(ACCOUNTS_QUERY)
        rows = [tuple(r.values()) for r in result]
    print(f"  Got {len(rows)} accounts")
    return rows


# -------------------------------------------------------------
# Step 2: Stream transactions in batches, write each to Parquet.
#
# Returns the unique transaction types seen and the max step value,
# both needed downstream to build dim_transaction_type and dim_date.
# -------------------------------------------------------------
def stream_transactions_to_parquet(driver, spark, output_path):
    """
    Read transactions in keyset-paginated batches.
    Convert each batch to a Spark DataFrame and append to the Parquet output.
    Track unique transaction types and max step seen for downstream dim tables.
    """
    last_cursor = ""  # empty string sorts before any real elementId
    batch_num = 0
    total_rows = 0
    seen_types = set()
    max_step = 0

    print(f"Streaming transactions in batches of {BATCH_SIZE}...")

    while True:
        with driver.session() as session:
            result = session.run(
                TRANSACTIONS_QUERY,
                last_cursor=last_cursor,
                batch_size=BATCH_SIZE,
            )
            # Materialize this batch immediately. Driver's lazy result
            # ties up the session connection; we want it freed.
            batch = [
                (
                    r["cursor"],
                    r["nameOrig"],
                    r["nameDest"],
                    int(r["step"]),
                    r["type"],
                    float(r["amount"]),
                    float(r["oldbalanceOrg"]),
                    float(r["newbalanceOrig"]),
                    float(r["oldbalanceDest"]),
                    float(r["newbalanceDest"])
                )
                for r in result
            ]

        if not batch:
            break  # no more data, we're done

        # Track values needed for downstream dim tables
        for row in batch:
            seen_types.add(row[4])      # type column
            if row[3] > max_step:       # step column
                max_step = row[3]

        # Build a Spark DataFrame for this batch and append to Parquet.
        # The first batch creates the directory; subsequent batches append.
        df = spark.createDataFrame(batch, schema=TRANSACTION_BATCH_SCHEMA)
        # Drop the cursor — it was only needed for pagination, not output
        df = df.drop("cursor")

        write_mode = "overwrite" if batch_num == 0 else "append"
        (df.write
            .mode(write_mode)
            .option("compression", "snappy")
            .parquet(str(output_path)))

        # Advance the cursor to the last elementId we saw.
        # Next iteration picks up from there.
        last_cursor = batch[-1][0]
        batch_num += 1
        total_rows += len(batch)
        print(f"  batch {batch_num}: {len(batch)} rows (total: {total_rows})")

    print(f"Done streaming. {total_rows} transactions written across {batch_num} batches.")
    return seen_types, max_step


# -------------------------------------------------------------
# Step 3: Build the small dim tables (account, type, date) and write them.
# These are small enough to handle in one shot regardless of fact size.
# -------------------------------------------------------------
def write_dim_account(spark, account_rows, output_path):
    print("Writing dim_account...")
    df = spark.createDataFrame(account_rows, schema=ACCOUNT_SCHEMA)
    (df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path)))


def write_dim_transaction_type(spark, seen_types, output_path):
    print("Writing dim_transaction_type...")
    rows = [(i + 1, name) for i, name in enumerate(sorted(seen_types))]
    schema = StructType([
        StructField("type_key",  IntegerType(), False),
        StructField("type_name", StringType(),  False),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    (df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path)))


def write_dim_date(spark, max_step, output_path):
    print(f"Writing dim_date for {max_step} steps...")
    rows = []
    for step in range(1, max_step + 1):
        dt = START_DATE + timedelta(hours=step - 1)
        rows.append((
            step,
            dt.strftime("%Y-%m-%d %H:00:00"),
            dt.date(),
            dt.hour,
            dt.strftime("%A"),
        ))
    schema = StructType([
        StructField("date_key",    IntegerType(), False),
        StructField("datetime",    StringType(),  False),
        StructField("date",        DateType(),    False),
        StructField("hour",        IntegerType(), False),
        StructField("day_of_week", StringType(),  False),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    (df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path)))


# -------------------------------------------------------------
# Step 4: Re-read the streamed fact table, partition it by date, rewrite.
#
# Why this extra step?
#   When streaming, we appended unpartitioned Parquet for simplicity.
#   To get partitioned-by-date layout (for Power BI partition pruning),
#   we re-read the data, join in dim_date for the `date` column, and
#   write again with partitionBy("date").
#
#   Alternative: partition during streaming by writing each batch directly
#   into date=YYYY-MM-DD/ folders. More efficient (no second pass) but
#   more code complexity. For students learning the pattern, the two-pass
#   version is clearer.
# -------------------------------------------------------------
def repartition_fact_by_date(spark, raw_path, dim_date_path, final_path):
    print("Repartitioning fact table by date...")
    fact = spark.read.parquet(str(raw_path))
    dim_date = spark.read.parquet(str(dim_date_path))

    # Map step -> date via dim_date (PaySim's step IS our date_key)
    fact_with_date = fact.join(
        dim_date.select(dim_date.date_key.alias("step"), "date"),
        on="step",
        how="inner",
    )

    (fact_with_date
        .write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("date")
        .parquet(str(final_path)))


# -------------------------------------------------------------
# Main
# -------------------------------------------------------------
# def main():
#     OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    

#     print(f"Connecting to {NEO4J_URI}...")
#     driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

#     print("Starting Spark session...")
#     spark = (
#         SparkSession.builder
#         .appName("neo4j-to-parquet-batched")
#         # .config("spark.sql.shuffle.partitions", "50") #spark tuning
#         # .config("spark.driver.memory", "4g") #spark tuning
#         .getOrCreate()
#     )

#     # Output paths
#     dim_account_path = OUTPUT_DIR / "dim_account"
#     dim_type_path    = OUTPUT_DIR / "dim_transaction_type"
#     dim_date_path    = OUTPUT_DIR / "dim_date"
#     fact_raw_path    = OUTPUT_DIR / "_fact_transactions_raw"  # intermediate
#     fact_final_path  = OUTPUT_DIR / "fact_transactions"       # partitioned

#     # 1. Accounts — single read
#     accounts = fetch_accounts(driver)
#     write_dim_account(spark, accounts, dim_account_path)

#     # 2. Transactions — streamed in batches, written incrementally
#     seen_types, max_step = stream_transactions_to_parquet(
#         driver, spark, fact_raw_path
#     )

#     driver.close()

#     # 3. Dim tables that depend on what we saw during streaming
#     write_dim_transaction_type(spark, seen_types, dim_type_path)
#     write_dim_date(spark, max_step, dim_date_path)

#     # 4. Repartition the fact table by date (joins with dim_date)
#     repartition_fact_by_date(spark, fact_raw_path, dim_date_path, fact_final_path)

#     # Cleanup intermediate
#     print("Cleaning up intermediate files...")
#     import shutil
#     shutil.rmtree(fact_raw_path, ignore_errors=True)

#     print(f"\nDone. Output: {OUTPUT_DIR.resolve()}")
#     spark.stop()


# if __name__ == "__main__":
#     main()

# Only write the dim_account again
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    print("Starting Spark session...")
    spark = (
        SparkSession.builder
        .appName("neo4j-to-parquet-dim-account-only")
        .getOrCreate()
    )

    # Output path (ONLY this one matters now)
    dim_account_path = OUTPUT_DIR / "dim_account"

    # 🔹 Rebuild ONLY dim_account
    print("Rebuilding dim_account with new columns...")
    accounts = fetch_accounts(driver)
    write_dim_account(spark, accounts, dim_account_path)

    driver.close()

    print(f"\nDone. Updated: {dim_account_path.resolve()}")
    spark.stop()


if __name__ == "__main__":
    main()