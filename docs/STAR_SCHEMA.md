# T0.5 local star schema

`batching_python_driver.py` exports all four tables to a **new local directory**.
It performs read-only queries using `db_uri`, `db_user`, and `db_password`, reusing
T0.3's environment configuration helper. Its runtime guard accepts loopback
endpoints only. Imports do not load credentials, connect, or start Spark.

| Table / grain | Columns and types |
|---|---|
| `dim_account` / Account.id | Non-null string `account_id`; nullable bigint `community_id`, `community_transaction_count`, `risk_score`, and flags `fan_out_flag`, `fan_in_flag`, `unusualFanIn`, `drain_behavior_flag`, `dense_community_flag`, `transfer_cashout_flag`, `in_cycle_flag`, `guilt_by_association_flag`; nullable double `outDegree`, `inDegree`; nullable decimal(38,10) `community_total_amount` |
| `dim_transaction_type` / fixed type | Non-null bigint `type_key`, string `type_name`. Reuses `contracts.transaction_event.TRANSACTION_TYPE_KEYS`: CASH_IN=1, CASH_OUT=2, DEBIT=3, PAYMENT=4, TRANSFER=5. All five rows are written, regardless of which types were read. |
| `dim_date` / observed PaySim step | Non-null bigint `date_key`, `hour`; string ISO `datetime`, `day_of_week`; date `date`. Reuses T2's synthetic calendar: step 1 = 2026-03-01 00:00, then step − 1 hours. This is simulated time, not ingestion time. |
| `fact_transactions` / stored txn_id | Non-null string `txn_id`, `source_file`, `nameOrig`, `nameDest`, `type`; bigint `row_number`, `source_step`, `step`, `date_key`, `type_key`; decimal(38,10) `amount`, `oldbalanceOrg`, `newbalanceOrig`; nullable decimal(38,10) `oldbalanceDest`, `newbalanceDest`; nullable bigint relationship `transfer_cashout_flag`; non-null date `date` (partition column) |

Facts join to accounts twice (`nameOrig`/`nameDest` → `account_id`), to types on
`type_key`, and to dates on `date_key`. Monetary casts reuse T2's Decimal policy:
do not round silently; reject values outside decimal(38,10). This cannot recover
precision already lost to the existing v1 JSON/Neo4j double representation.
Merchant destination balances remain null. Optional graph results stay null
when detection has not run; missing flags/scores are not invented as zero.
Boolean flags are normalized to integer 0/1. Evaluation labels are never queried
or exported. Detection version metadata is deferred to T3.

The reader retains batched keyset pagination, now ordered on unique `txn_id`;
it uses no internal graph ID as a fact key. Each relationship must already have
the T0.3 ID and `source_file`/`row_number` audit metadata. The exporter checks its
stored ID against the shared contract; legacy or inconsistent rows fail instead
of receiving replacement IDs. Batch writes append only inside a newly reserved
export directory. Facts are partitioned directly by synthetic date, so the old
intermediate fact rewrite is unnecessary. Empty graphs still write four tables
with explicit schemas (the type dimension still has five rows).

Account/transaction counts and identity coverage are checked before export;
counts are rechecked after reads and compared with each written Parquet table.
Keep ingestion and detection paused for an export: these reads do **not** form
a database-wide snapshot. Count checks cannot detect every equal-count mutation.
Memory holds one fact batch plus the account dimension and observed steps.

Runtime prerequisites: project Python requirements plus a compatible PySpark
installation and JVM. For a concrete supported pairing, PySpark 4.0.1 documents
Python 3.9+ and Java 17+ with `JAVA_HOME` set; see the
[Apache Spark installation documentation](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html).
Use the same Python for driver/workers. No Spark packages or JVM were installed
in this session; PySpark remains an explicitly documented runtime prerequisite
rather than changing the existing application dependency pins.

Future authorized local usage:

```powershell
python batching_python_driver.py --output Output_Parquet_new --batch-size 25000
```

This command connects to local Neo4j; it was **not run** in the code-only session.
An existing output path is refused before connection. `_EXPORT_INCOMPLETE`
marks partial output; `_EXPORT_COMPLETE.json` appears only after all counts
pass. A failed export is left for inspection: choose a fresh path for a retry,
and obtain approval before deleting old output. Do not treat a partial export
as the T2 reconciliation oracle.

Fixture tests use a fake Neo4j reader and Spark-compatible in-memory sink,
including pagination, schemas, relationship flags, all-table orchestration,
count mismatches, output safety and cleanup. No live Neo4j reads or actual
Spark/Parquet engine execution have been verified. Status is
**code-complete (fixture-tested; not run against Neo4j)**, not live-verified.
