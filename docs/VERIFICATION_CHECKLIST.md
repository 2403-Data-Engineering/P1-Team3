# Repo-only verification checklist — T5

Scope: fixture-tested source code and documentation, not a deployed pipeline.
The user confirmed nothing was deployed. No live verification or teardown was
performed for T5; this checklist does not certify an AWS account inventory.
T5 completion does not upgrade other tasks' pending live-verification statuses.

## Evidence by task

| Task | Fixture/local evidence | Still unverified |
|---|---|---|
| T0.1 contract | `tests/test_txn_id.py`: deterministic IDs, row-specific duplicate-looking transactions, required fields/type/amount/label rejection, null destination balances for merchants, customer balances retained, risk_score naming | Real producer payloads, deployed consumers, identity preservation through live services |
| T0.2 eval sidecar | `tests/test_eval_sidecar.py`: unlabeled ops plus txn_id-keyed labels with a one-to-one join, including different labels on identical-looking source rows; operational path rejects labels | A full dataset split, deployed eval access controls, live reporting joins |
| T0.3 ingest | `tests/test_neo4j_ingest.py`: repeat load keeps counts using an in-memory MERGE/fake session; query uses MERGE/txn_id and declares unique account/relationship keys; environment-only connection arguments | Local server/edition compatibility, installed constraints via SHOW CONSTRAINTS, legacy edges, actual double-load counts, transaction semantics and database recovery |
| T0.4 detection | `tests/test_detection.py`: required mock-fixture flags/weighted scores, no similarity signal, query step order, fan_out_flag and risk_score writes; reset is distinguished from dense-community detection in the order test | GDS installation/procedure syntax/orientation, actual Louvain/degree outcomes, graph windows and scale. The in-memory community implementation is connected components, **not Louvain** |
| T0.5 baseline | `tests/test_star_schema.py`: fake read-only reader and Spark sink exercise four-table main, pagination, stored txn_id validation, relationship flags/risk_score, fixed type keys, nullable merchant balances, schemas, output refusal/count checks/partial markers; normalized fixture totals agree with T2 | Real Neo4j counts, actual Spark schema enforcement/Parquet partitions/JVM, quiescent graph reads, comparison with a live local baseline. The repaired Neo4j test stubs the module before import |
| T1 foundation | Terraform formatting/provider-schema validation passed in prior sessions; backend examples, scoped role, encryption/public blocks, budget and teardown design exist | Terraform plan remains pending runtime inputs; no apply, IAM/network/encryption/budget delivery test, provisioned resource or measured cost |
| T2 batch | `tests/test_pipeline.py`: reproducible bounded sample/IDs, label/malformed-record refusal, nulls/Decimal casts, persistent local replay without duplicate facts, count/amount reconciliation and mismatch exit; Glue orchestration uses fake S3/Glue/Spark; ZIP schema resource works | Actual Spark/Parquet, package installation/job startup in Glue, S3 replay/replacement recovery, catalog registration, full sample performance and bill |
| T3 graph results | `tests/test_graph_exports.py`: T0.5-normalized fixture agreement, exact ring aggregates, immutable two-run account history, explicit-run joins without fact multiplication, null unmatched signals, invalid scope/version/checksum rejection, fake Parquet input | Live completed detection provenance, atomic snapshot consistency, real exports/upload, scheduled orchestration, live graph-to-curated reconciliation |
| T4 reporting | `tests/test_reporting.py`: all 16 view definitions created and selected in local SQLite; KPI/time/type/ring totals, histogram/ranking, explicit/latest run selection, duplicate-signal quality counters, label coverage/isolation and threshold independence | Native Athena execution, Hive DDL/catalog/SerDe/partition semantics, arbitrary-precision query behavior, BI connector/refresh and deployed workgroup controls |
| T5 repo delivery | README/architecture/schema/runbook/source paths checked against code; names-only env example and ignore coverage checked; full local test suite rerun | No end-to-end live acceptance or teardown claim; live rollout requires the gates below |

## Repeatable fixture checks

- [x] Exact replay preserves one fact per txn_id. Distinct source rows that look
  identical still have different IDs; ingestion timestamps do not change IDs.
  Local repeated runs replace an authoritative snapshot, and fake Glue replay
  overwrites the same content-addressed table paths rather than appending facts.
- [x] Malformed fields, unknown transaction types, bad/forged identity metadata,
  operational fraud labels and configured batch/sample limit violations fail.
- [x] Merchant destination balances are null rather than zero; customer balances
  remain numeric. Fixture schema checks preserve destination nullability.
- [x] T2 shared facts and T0.5-normalized fake-reader facts agree by txn_id and
  by `(step, type)` counts and exact Decimal amount sums. Reconciliation rejects
  duplicate keys and returns a nonzero exit on mismatches. This is a **fixture
  baseline comparison**, not a comparison with generated Parquet or Neo4j.
- [x] T3 joins retain curated fact counts for a selected run. Two runs for the
  same account remain distinguishable and cannot multiply facts.
- [x] Evaluation labels change only evaluation rates; changing model thresholds
  changes only model metrics. Missing/duplicate labels lower coverage instead of
  being counted as legitimate. No operational SQL view reads eval_labels.
- [x] Future destructive actions, state retention and query-result cleanup have
  a documented approval-based runbook; no destructive action was necessary here.

The four-row replay fixture has **4 facts**, **5 accounts**, **5 fixed type rows**
and **2 observed step/date rows**, totaling **20363.00**:

| Step | Type | Count | Amount sum |
|---|---|---:|---:|
| 1 | PAYMENT | 2 | 362.00 |
| 1 | TRANSFER | 1 | 10000.50 |
| 2 | CASH_OUT | 1 | 10000.50 |

The reporting fixture explicitly assigns graph signals for testing: its selected
newer run has one high-risk account at threshold 30 and model-flagged volume
20001.00. Its constructed dense community has internal volume 20363.00. These
are controlled test expectations, **not observed fraud findings**. Sidecar ground
truth gives PAYMENT 50%, TRANSFER/CASH_OUT 100%, with no rate for unseen types.
Neither four-row labels nor these scores calibrate a defensible threshold.

## SQL portability boundary

**SQLite is not Athena's Trino/Presto SQL dialect and is not Hive DDL.** Running
the 16 view files unchanged in SQLite checks SQLite parsing and small expected
results; it does **not** verify Athena syntax, function support, coercion, reserved
words, aggregate types, optimization or catalog behavior. A local Python
`from_iso8601_timestamp` adapter simulates timestamp ordering; the actual Athena
function/type semantics remain untested. SQLite's fractional NUMERIC values use
floating arithmetic, unlike the intended Athena decimal arithmetic.

Neither `reporting/ddl/sources.sql` nor `reporting/ddl/register_run.sql` was
executed: SQLite has no Hive external tables, S3 locations, JSON/CSV SerDes or
Glue partitions. Those are unrendered Athena/Hive templates. Every view was
exercised locally, but **every view still needs native Athena validation**.

## Live acceptance gates (all pending)

- [ ] Configure a compatible local Neo4j/GDS and unlabeled CSV. Check legacy
  edges and installed keys; obtain approval before any data deletion or write.
- [ ] Double-load an approved sample, run scheduled/windowed detection, then
  export all four tables with ingestion/detection paused. Verify real row counts,
  nulls, identities, graph signals and actual Parquet/JVM behavior.
- [ ] Provide T1 runtime inputs and an external private/versioned state bucket;
  review an actual plan. Obtain deployment approval and confirm the budget
  notification before any paid job. Scope eval IAM separately from scoring.
- [ ] Upload approved immutable sample/artifacts, run the bounded Glue batch,
  replay it, test interrupted-run recovery, and reconcile against the live local
  baseline by step/type/count/amount. Measure runtime and cost.
- [ ] Export the completed graph run with accurate window metadata, verify
  hashes/counts, publish graph partitions before the registry, and reconcile joins.
- [ ] Render/validate all SQL in Athena engine 3 and the required Hive DDL; verify
  native decimals, timestamp parsing, partitions, source keys and workgroup scan
  controls. Reconcile the report totals and pass all quality gates.
- [ ] Justify the risk threshold on an appropriate population; build a BI report
  from the specification, pin one run for Import refresh and test refresh results.
- [ ] Only if resources were later deployed: follow approved teardown and retain
  evidence. No live resource removal or account-wide billing claim is signed off
  by this repo-only checklist.

Execution record and final sign-off: [AGENT_HANDOFF.md](../AGENT_HANDOFF.md).
